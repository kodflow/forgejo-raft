// Copyright 2026 The Forgejo Authors. All rights reserved.
// SPDX-License-Identifier: GPL-3.0-or-later

// Package nomadgate reads Nomad, read-only, for the JOIN fence decision
// (kitsunium ADR 0152 D17, R8-09, D20: "a read-only Nomad token for
// Forgejo"). A node is not let into the consensus while its Nomad node is
// draining, ineligible or not ready: a machine being emptied must not
// become a member. Only GET /v1/nodes and GET /v1/node/<id> are called,
// which the node:read capability allows. When Nomad does not answer, the
// gate refuses: a join is rare and an operator retries it.
package nomadgate

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"

	"forgejo.org/modules/json"
	"forgejo.org/modules/log"
)

// Gate is a read-only view of the Nomad nodes.
type Gate struct {
	// Addr is http(s)://host:port, or unix:///path for the task API socket.
	Addr string
	// TokenFile holds a token with node:read, read at each call (the
	// workload identity file Nomad rotates); empty sends none.
	TokenFile string
	// SlotMeta is the node meta key carrying the slot hosted there.
	SlotMeta string
	Client   *http.Client
}

// Node is what the gate reads of a Nomad node.
type Node struct {
	ID                    string            `json:"ID"`
	Name                  string            `json:"Name"`
	Status                string            `json:"Status"`
	SchedulingEligibility string            `json:"SchedulingEligibility"`
	Drain                 bool              `json:"Drain"`
	DrainStrategy         any               `json:"DrainStrategy"`
	HTTPAddr              string            `json:"HTTPAddr"`
	Meta                  map[string]string `json:"Meta"`
}

// Draining says whether a drain is under way on the node.
func (n Node) Draining() bool {
	return n.Drain || n.DrainStrategy != nil
}

// ErrRefused wraps every refusal of the gate.
var ErrRefused = errors.New("JOIN_REFUSED")

func (g *Gate) base() (string, *http.Client) {
	if path, ok := strings.CutPrefix(g.Addr, "unix://"); ok {
		c := g.Client
		if c == nil {
			c = &http.Client{Timeout: 10 * time.Second, Transport: &http.Transport{
				DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
					return (&net.Dialer{}).DialContext(ctx, "unix", path)
				},
			}}
		}
		return "http://nomad", c
	}
	c := g.Client
	if c == nil {
		c = &http.Client{Timeout: 10 * time.Second}
	}
	return strings.TrimRight(g.Addr, "/"), c
}

func (g *Gate) get(ctx context.Context, path string, v any) error {
	base, c := g.base()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, base+path, nil)
	if err != nil {
		return err
	}
	if g.TokenFile != "" {
		b, err := os.ReadFile(g.TokenFile)
		if err != nil {
			return fmt.Errorf("nomad token: %w", err)
		}
		req.Header.Set("X-Nomad-Token", strings.TrimSpace(string(b)))
	}
	resp, err := c.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		b, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		return fmt.Errorf("nomad %s %s: %s", path, resp.Status, strings.TrimSpace(string(b)))
	}
	return json.NewDecoder(io.LimitReader(resp.Body, 8<<20)).Decode(v)
}

// NodeOf returns the Nomad node hosting slot: the one whose meta names
// it. When several do (the old machine of a replacement, not yet purged),
// the one whose address host is the joiner's is taken; otherwise the
// answer is ambiguous and refused.
func (g *Gate) NodeOf(ctx context.Context, slot, joinerAddr string) (Node, error) {
	var stubs []struct {
		ID string `json:"ID"`
	}
	if err := g.get(ctx, "/v1/nodes", &stubs); err != nil {
		return Node{}, err
	}
	var found []Node
	for _, s := range stubs {
		var n Node
		if err := g.get(ctx, "/v1/node/"+url.PathEscape(s.ID), &n); err != nil {
			return Node{}, err
		}
		if n.Meta[g.SlotMeta] == slot {
			found = append(found, n)
		}
	}
	if len(found) > 1 {
		host, _, _ := net.SplitHostPort(joinerAddr)
		var same []Node
		for _, n := range found {
			if h, _, _ := net.SplitHostPort(n.HTTPAddr); host != "" && h == host {
				same = append(same, n)
			}
		}
		found = same
	}
	switch len(found) {
	case 0:
		return Node{}, fmt.Errorf("%w: no Nomad node carries %s=%s", ErrRefused, g.SlotMeta, slot)
	case 1:
		return found[0], nil
	default:
		return Node{}, fmt.Errorf("%w: several Nomad nodes carry %s=%s", ErrRefused, g.SlotMeta, slot)
	}
}

// CheckJoin refuses a join of slot while its Nomad node is draining,
// ineligible or not ready, and when Nomad cannot tell.
func (g *Gate) CheckJoin(ctx context.Context, slot, joinerAddr string) error {
	n, err := g.NodeOf(ctx, slot, joinerAddr)
	if errors.Is(err, ErrRefused) {
		return err
	}
	if err != nil {
		// The refusal reaches the joiner as is: the detail, which names
		// Nomad's address, stays in this member's log.
		log.Warn("cluster: JOIN drain check for slot %s: Nomad did not answer: %v", slot, err)
		return fmt.Errorf("%w: Nomad did not answer for slot %s", ErrRefused, slot)
	}
	var why string
	switch {
	case n.Draining():
		why = "draining"
	case n.SchedulingEligibility != "eligible":
		why = n.SchedulingEligibility
	case n.Status != "ready":
		why = n.Status
	default:
		return nil
	}
	// The reason reaches the joiner before it is admitted: it names the
	// slot and the state, and the node's name and id stay in this log.
	log.Warn("cluster: JOIN drain check for slot %s: Nomad node %s (%s) is %s", slot, n.Name, short(n.ID), why)
	return fmt.Errorf("%w: the Nomad node of slot %s is %s", ErrRefused, slot, why)
}

func short(id string) string {
	if len(id) > 8 {
		return id[:8]
	}
	return id
}
