// Copyright 2026 The Forgejo Authors. All rights reserved.
// SPDX-License-Identifier: GPL-3.0-or-later

package cluster

import (
	"context"
	"crypto/x509"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"slices"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"forgejo.org/modules/json"
	"forgejo.org/modules/log"
	"forgejo.org/modules/setting"
	"forgejo.org/services/cluster/consensus"
	"forgejo.org/services/cluster/reftable"
)

// The autonomous mode ([cluster] EXPECTED_NODES from 3): a node is told how
// many nodes the cluster has and sorts the rest out by itself, as Consul's
// bootstrap_expect does. PEERS names every other slot, the same list on
// every node; the node asks each who it is over the object channel's
// mutual TLS, and then:
//
//   - if any of them is already a member, it joins the cluster;
//   - otherwise the node of the lowest slot founds the cluster alone, once
//     every expected node answers as joining, and every other node joins
//     it. A majority is not enough: the silent node may hold the log.
//
// Only one slot is ever the lowest of the same list, so two nodes never
// found two clusters, provided every node is given the same PEERS.
// A node that already holds a cluster's log never founds again: a wiped
// founder finds a member among the others and joins instead, and waits
// while any of them is silent.
//
// Admission without an operator's grant rests on the cluster's CA: a node
// presenting a CA-signed certificate for an empty slot of this cluster is
// admitted as a learner (authorizeJoin), and the autopilot makes voters.
// A node the autopilot removed comes back the same way, under a new id
// (retire.go).

// autoMode is the autonomous mode's view of the settings, held by the
// replicator so tests run several nodes in one process.
type autoMode struct {
	expected int
	self     string
	listen   string
	// peers is PEERS, slot -> consensus address.
	peers map[string]string
	// pins is PEER_PINS: when set, a JOIN trusts these keys only instead
	// of every CA-signed member it finds.
	pins [][32]byte
	// probe asks a node who it is; consensusAddr is where its consensus
	// listens, slot the slot PEERS names there.
	probe func(ctx context.Context, consensusAddr, slot string) (hello, error)
	// tick paces the autopilot and the formation's rounds.
	tick time.Duration
	// now is the autopilot's clock, which tests replace.
	now func() time.Time
	// deadTimeout is DEAD_MEMBER_TIMEOUT: how long a member stays out of
	// the leader's reach before the autopilot removes it. downAfter is how
	// long without a word from a member makes it out of reach.
	deadTimeout, downAfter time.Duration
	// rejoin replaces this node's consensus identity once a quorum says
	// its id is retired (retire.go).
	rejoin func(ctx context.Context) error
	// lastOwn is the last membership this node's engine listed, which a
	// closed engine no longer does (retire.go). Only the autopilot's
	// goroutine touches it.
	lastOwn consensus.Membership
	// revoked reports a key this node's table holds revoked: a JOIN never
	// trusts an answer under such a key. Nil revokes nothing.
	revoked func(spki [32]byte) bool
	// stopNoted is set once a stopped engine's refusal to rejoin is logged.
	stopNoted atomic.Bool
	// disk is what the data directory held when the node read it.
	disk atomic.Int32
}

func newAutoMode(oc *objectChannel) *autoMode {
	return &autoMode{
		expected: setting.Cluster.ExpectedNodes, self: setting.Cluster.Slot, listen: setting.Cluster.ListenAddr,
		peers: setting.Cluster.Peers, pins: peerPins(), probe: oc.probeHello, tick: 2 * time.Second,
		now: time.Now, deadTimeout: setting.Cluster.DeadMemberTimeout, downAfter: 10 * time.Second,
	}
}

// autoMinVoters is the engine's floor for a Remove: three voters, the
// autopilot's own floor (voters.go), so no removal crosses it from any
// path. Only the founding list, of one voter, is checked against one.
const autoMinVoters = 3

// hello is what a node says of itself to any node of the cluster.
type hello struct {
	Slot     string `json:"slot"`
	State    string `json:"state"`
	Address  string `json:"address"`
	Expected int    `json:"expected"`
	// Node is the answering node's id; Index, Voters and Retired are its
	// committed membership: where it is, who votes in it, and the ids it
	// retired. A removed node counts them to know it was removed.
	Node    string   `json:"node,omitempty"`
	Index   uint64   `json:"index,omitempty"`
	Voters  []string `json:"voters,omitempty"`
	Retired []string `json:"retired,omitempty"`
	// Asked is the id the asking node named (?node=), and AskedRetired
	// whether the answering node's membership retired it: the answer to
	// a node that names itself, in place of the Retired list, which only
	// grows (#93). An older binary ignores the question and sends the
	// list, which a node reads all the same.
	Asked        string `json:"asked,omitempty"`
	AskedRetired bool   `json:"asked_retired,omitempty"`
	// Payload is the highest command version the answering binary
	// applies; absent from older binaries, which apply version 1 only.
	Payload int `json:"payload,omitempty"`
	// SPKI is the key the handshake proved, never read from the body.
	SPKI [32]byte `json:"-"`
}

// helloReadLimit bounds a hello read: far above any answer to a node that
// names itself, and room for about fifty thousand retired ids from an
// older binary that sends the whole list (#93).
const helloReadLimit = 1 << 20

// retires reports whether this answer says the node id is retired: the
// direct answer to the question when the node asked it, else the list.
func (h hello) retires(id string) bool {
	if h.Asked == id {
		return h.AskedRetired
	}
	return slices.Contains(h.Retired, id)
}

// answerFor turns a hello into the answer to a node that named itself:
// whether that id is retired, without the list.
func (h *hello) answerFor(id string) {
	h.Asked, h.AskedRetired, h.Retired = id, slices.Contains(h.Retired, id), nil
}

const (
	helloPath   = "/v1/hello"
	helloMember = "member"
	// helloJoining is a node whose data directory holds a joining stamp and
	// no member's log.
	helloJoining = "joining"
	// helloStarting is a node that has not read its data directory yet: it
	// may hold a member's log, so nothing is founded while it answers so.
	helloStarting = "starting"
)

// What a node's data directory held when it read it (autoMode.disk).
const (
	diskUnread int32 = iota
	diskJoining
	diskMember
)

// noteDisk records what the data directory holds: a member's stamp and
// log, or a joining stamp only. The hello reports it from then on, before
// the engine is open.
func (a *autoMode) noteDisk(member bool) {
	if member {
		a.disk.Store(diskMember)
		return
	}
	a.disk.Store(diskJoining)
}

// helloOf is this node's answer. A node reports what its data directory
// holds before its engine opens, so a restarting member is never taken for
// a fresh node, and what its committed membership says once it is open.
func (r *Replicator) helloOf() hello {
	h := hello{Slot: r.auto.self, State: helloStarting, Address: r.auto.listen, Expected: r.auto.expected, Payload: reftable.PayloadVersion}
	switch r.auto.disk.Load() {
	case diskMember:
		h.State = helloMember
	case diskJoining:
		h.State = helloJoining
	}
	eng := r.Engine()
	if eng == nil {
		return h
	}
	self := eng.Status().ID
	m := eng.Membership()
	h.Node, h.Index = self.String(), m.At.Index
	if Rank(m, self) > 0 {
		h.State = helloMember
	}
	for _, x := range m.Members {
		if x.Voter {
			h.Voters = append(h.Voters, x.ID.String())
		}
	}
	for _, id := range m.Retired {
		h.Retired = append(h.Retired, id.String())
	}
	return h
}

type target struct{ addr, slot string }

// targets lists the other slots PEERS names, with their consensus
// addresses, in slot order.
func (a *autoMode) targets() []target {
	slots := make([]string, 0, len(a.peers))
	for slot := range a.peers {
		if slot != a.self {
			slots = append(slots, slot)
		}
	}
	sort.Strings(slots)
	out := make([]target, 0, len(slots))
	for _, slot := range slots {
		out = append(out, target{addr: a.peers[slot], slot: slot})
	}
	return out
}

// round asks every target who it is, at once, and keeps the answers of
// other nodes. An answer from another slot than PEERS names at that
// address is dropped: the seeds and the certificates disagree.
func (a *autoMode) round(ctx context.Context) []hello {
	ts := a.targets()
	answers := make([]*hello, len(ts))
	var wg sync.WaitGroup
	for i, t := range ts {
		wg.Go(func() {
			pctx, cancel := context.WithTimeout(ctx, 5*time.Second)
			defer cancel()
			h, err := a.probe(pctx, t.addr, t.slot)
			if err != nil {
				log.Debug("cluster: discovery: %s did not answer: %v", t.addr, err)
				return
			}
			if h.Slot != t.slot {
				log.Warn("cluster: discovery: PEERS names slot %s at %s, whose certificate names %s; ignored", t.slot, t.addr, h.Slot)
				return
			}
			if h.Address == "" {
				h.Address = t.addr
			}
			answers[i] = &h
		})
	}
	wg.Wait()
	var out []hello
	bySlot := map[string]bool{a.self: true}
	for _, h := range answers {
		if h != nil && !bySlot[h.Slot] {
			bySlot[h.Slot] = true
			out = append(out, *h)
		}
	}
	return out
}

// formation is what a node with an empty log does next.
type formation uint8

const (
	formWait formation = iota
	formFound
	formJoin
)

// decide is the founding rule, pure so it is tested alone. self is this
// node's slot, peerSlots the slots PEERS names, seen the answers of a round.
func decide(self string, expected int, peerSlots []string, seen []hello) (formation, string) {
	for _, h := range seen {
		if h.State == helloMember {
			return formJoin, fmt.Sprintf("slot %s is a member of the cluster", h.Slot)
		}
	}
	for _, h := range seen {
		if h.State != helloJoining {
			// A node that has not read its data directory may hold a
			// member's log: nothing is founded until every answer is a
			// node with a joining stamp and no log.
			return formWait, fmt.Sprintf("slot %s has not said what its data directory holds yet", h.Slot)
		}
	}
	for _, h := range seen {
		if h.Expected != expected {
			return formWait, fmt.Sprintf("slot %s expects %d nodes, this one %d: nothing is founded until they agree", h.Slot, h.Expected, expected)
		}
	}
	known := map[string]bool{self: true}
	for _, s := range peerSlots {
		known[s] = true
	}
	for _, h := range seen {
		known[h.Slot] = true
	}
	if len(known) > expected {
		return formWait, fmt.Sprintf("%d slots answer or are named for %d expected nodes: nothing is founded", len(known), expected)
	}
	if len(known) < expected {
		return formWait, fmt.Sprintf("%d of the %d expected slots known so far", len(known), expected)
	}
	slots := make([]string, 0, len(known))
	for s := range known {
		slots = append(slots, s)
	}
	founder := slices.Min(slots)
	if founder != self {
		return formJoin, fmt.Sprintf("slot %s founds the cluster", founder)
	}
	// Every expected slot must answer, not a majority: a slot that stays
	// silent may be the one node still holding the cluster's log (the
	// others wiped), and founding without it would found a second cluster
	// under the same id, which its log can never rejoin.
	answered := map[string]bool{self: true}
	for _, h := range seen {
		answered[h.Slot] = true
	}
	if reached := len(answered); reached < expected {
		return formWait, fmt.Sprintf("this node founds the cluster once every expected node answers: %d of %d", reached, expected)
	}
	return formFound, "this node has the lowest slot and every expected node answers"
}

// form prepares the data directory of the autonomous mode before the node
// opens: stamped on the first start, then founded or left to join, by the
// rule above. A directory that is already a member's is left alone.
func (r *Replicator) form(ctx context.Context, driver consensus.Driver, cfg consensus.Config, cluster consensus.ClusterID, spki [32]byte) error {
	rep, err := driver.Inspect(cfg.DataDir)
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("[cluster] reading %s: %w", cfg.DataDir, err)
	}
	if err != nil || rep.Cluster.IsZero() {
		// A cluster a Recover superseded is never founded again under its
		// id: the witnesses, when configured, are asked first.
		if err := fenceClusterOrExit(ctx, cluster); err != nil {
			return err
		}
		id, inc, err := driver.Init(cfg.DataDir, cluster, cfg.Slot)
		if err != nil {
			return fmt.Errorf("[cluster] stamping %s: %w", cfg.DataDir, err)
		}
		log.Info("cluster: data directory stamped, node %s of cluster %s", id, cluster)
		rep = consensus.Report{Cluster: cluster, Node: id, Incarnation: inc, Slot: cfg.Slot, Joining: true}
	}
	if rep.BootstrapPending {
		// This node decided to found, and its Bootstrap stopped between the
		// pending stamp and the member stamp (#62): it is run again with
		// the same one-member list, which the SDK accepts only with the
		// same digest. hello does not say "member" before it succeeds.
		if rep.Cluster != cluster {
			return fmt.Errorf("%w: the data directory belongs to cluster %s, the certificate to %s", consensus.ErrMisconfigured, rep.Cluster, cluster)
		}
		self := consensus.Member{ID: rep.Node, Incarnation: rep.Incarnation, Slot: cfg.Slot, SPKI: spki, Address: cfg.ListenAddress, Voter: true}
		log.Warn("cluster: the founding of cluster %s was interrupted on this node; it resumes", cluster)
		if err := driver.Bootstrap(ctx, cfg, []consensus.Member{self}); err != nil {
			return fmt.Errorf("[cluster] resuming the founding: %w", err)
		}
		r.auto.noteDisk(true)
		return nil
	}
	r.auto.noteDisk(!rep.Joining)
	if !rep.Joining {
		return nil
	}
	if rep.Cluster != cluster {
		return fmt.Errorf("%w: the data directory belongs to cluster %s, the certificate to %s", consensus.ErrMisconfigured, rep.Cluster, cluster)
	}
	self := consensus.Member{ID: rep.Node, Incarnation: rep.Incarnation, Slot: cfg.Slot, SPKI: spki, Address: cfg.ListenAddress, Voter: true}
	return r.auto.formLoop(ctx, func(ctx context.Context) error {
		return driver.Bootstrap(ctx, cfg, []consensus.Member{self})
	})
}

// formLoop runs rounds until the node founds the cluster or knows it must
// join, logging what it waits for when that changes.
func (a *autoMode) formLoop(ctx context.Context, found func(context.Context) error) error {
	peerSlots := make([]string, 0, len(a.peers))
	for s := range a.peers {
		peerSlots = append(peerSlots, s)
	}
	last := ""
	for {
		what, why := decide(a.self, a.expected, peerSlots, a.round(ctx))
		switch what {
		case formFound:
			if err := found(ctx); err != nil {
				return fmt.Errorf("[cluster] founding the cluster: %w", err)
			}
			log.Warn("cluster: founded the cluster as its first voter: %s", why)
			return nil
		case formJoin:
			log.Info("cluster: joining: %s", why)
			return nil
		}
		if why != last {
			log.Info("cluster: waiting to form the cluster: %s", why)
			last = why
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(a.tick):
		}
	}
}

// discoverForJoin gives a joining node's JOIN its addresses and the keys an
// answer may come from: the members found, or PEER_PINS when set.
func (a *autoMode) discoverForJoin(ctx context.Context) ([]string, [][32]byte) {
	var addrs []string
	var pins [][32]byte
	for _, h := range a.round(ctx) {
		if h.State != helloMember || (a.revoked != nil && a.revoked(h.SPKI)) {
			// A revoked key's answer is no member to join through: it
			// would pin that key for the JOIN (#49).
			continue
		}
		addrs = append(addrs, h.Address)
		pins = append(pins, h.SPKI)
	}
	if len(a.pins) > 0 {
		pins = a.pins
	}
	return addrs, pins
}

// probeHello asks the node whose consensus listens at consensusAddr who it
// is, on its object channel, over mutual TLS with the cluster's CA.
func (oc *objectChannel) probeHello(ctx context.Context, consensusAddr, slot string) (hello, error) {
	addr := oc.objectAddr(consensusAddr, slot)
	var got peer
	cfg := oc.id.clientConfig(func(p peer) error {
		if oc.cluster != "" && p.Cluster != oc.cluster {
			return fmt.Errorf("CLUSTER_MISMATCH: %s", p.Cluster)
		}
		got = p
		return nil
	})
	c := &http.Client{Transport: &http.Transport{
		DialContext:       (&net.Dialer{Timeout: peerDialTimeout}).DialContext,
		TLSClientConfig:   cfg,
		DisableKeepAlives: true, TLSHandshakeTimeout: peerHandshakeTimeout,
	}}
	u := "https://" + addr + helloPath
	if oc.self != nil {
		if id := oc.self(); id != 0 {
			// Naming itself, the node gets whether its id is retired
			// rather than every retired id (#93).
			u += "?node=" + url.QueryEscape(id.String())
		}
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	if err != nil {
		return hello{}, err
	}
	resp, err := c.Do(req)
	if err != nil {
		return hello{}, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return hello{}, fmt.Errorf("%s answered %s", addr, resp.Status)
	}
	var h hello
	if err := json.NewDecoder(io.LimitReader(resp.Body, helloReadLimit)).Decode(&h); err != nil {
		return hello{}, err
	}
	// The slot and the key are the certificate's, whatever the body says.
	if h.Slot != got.Slot {
		return hello{}, fmt.Errorf("%s says slot %s, its certificate %s", addr, h.Slot, got.Slot)
	}
	h.SPKI = got.SPKI
	return h, nil
}

// objectAddr is the object channel of the node whose consensus listens at
// consensusAddr: OBJECT_PEERS for its slot, else its host and OBJECT_PORT.
func (oc *objectChannel) objectAddr(consensusAddr, slot string) string {
	if oc.resolve != nil && slot != "" {
		if a := oc.resolve(consensus.Member{Slot: slot, Address: consensusAddr}); a != "" {
			return a
		}
	}
	host, _, err := net.SplitHostPort(consensusAddr)
	if err != nil {
		return consensusAddr
	}
	return net.JoinHostPort(host, oc.port)
}

// clusterOf reads the cluster id a node certificate's URI SAN names.
func (id *identity) clusterOf() (consensus.ClusterID, error) {
	c, err := id.load()
	if err != nil {
		return consensus.ClusterID{}, err
	}
	if len(c.Certificate) == 0 {
		return consensus.ClusterID{}, errors.New("no certificate")
	}
	leaf, err := x509.ParseCertificate(c.Certificate[0])
	if err != nil {
		return consensus.ClusterID{}, err
	}
	p, err := peerFromChain(leaf)
	if err != nil {
		return consensus.ClusterID{}, err
	}
	return consensus.ParseClusterID(p.Cluster)
}

// autoClusterID is the cluster of the autonomous mode: CLUSTER_ID when set,
// which must match the certificate, else the certificate's.
func autoClusterID(id *identity) (consensus.ClusterID, error) {
	fromCert, err := id.clusterOf()
	if err != nil {
		return fromCert, fmt.Errorf("the node certificate: %w", err)
	}
	if setting.Cluster.ClusterID == "" {
		return fromCert, nil
	}
	cid, err := consensus.ParseClusterID(setting.Cluster.ClusterID)
	if err != nil {
		return cid, err
	}
	if cid != fromCert {
		return cid, fmt.Errorf("%w: CLUSTER_ID %s, the certificate names %s", consensus.ErrMisconfigured, cid, hex.EncodeToString(fromCert[:]))
	}
	return cid, nil
}
