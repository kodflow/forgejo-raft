// Copyright 2026 The Forgejo Authors. All rights reserved.
// SPDX-License-Identifier: GPL-3.0-or-later

package cluster

import (
	"context"
	"net"
	"net/http"
	"sync"
	"time"
)

// A renewed certificate reaches new handshakes at once: both the consensus
// transport (the SDK's reloadable PEM identity) and the object channel
// (identity.load) reread the PEM file when it changes. Connections already
// open are handled per channel (ADR 0152 D17, R7-25):
//
//   - The object channel rotates them. Its clients are dropped and their
//     idle connections closed; a request in flight finishes on its
//     connection, which then idles out of a transport nobody uses
//     (IdleConnTimeout). On the server side, idle connections handshaken
//     before the renewal are closed at once, busy ones as soon as their
//     request ends (connRotator).
//   - The consensus transport keeps them until their natural end, by
//     design of the SDK: a renewal keeps the key, so the members' SPKI pins
//     and the authenticated peer are unchanged, and the SDK caps every
//     admitted connection's read deadline at the NotAfter of the
//     certificate it was authenticated with (and at MaxConnectionAge). No
//     connection outlives the certificate that opened it, and cutting the
//     log's lanes on every renewal would only cost a reconnection.

// identityPoll is how often the node's PEM file is checked for a change,
// whoever renewed it: the in-process renewer, `forgejo cluster cert renew`,
// or an external agent.
var identityPoll = 15 * time.Second

// watchIdentity calls rotated, from a goroutine of its own until ctx ends,
// each time the node certificate on disk changes, once the new pair
// loaded. The certificate it starts from is read before it returns: read
// by the goroutine, a renewal that came first was its starting point, and
// was never noticed (#153).
func watchIdentity(ctx context.Context, id *identity, every time.Duration, rotated func()) {
	last := id.SPKIAndSerial()
	go func() {
		t := time.NewTicker(every)
		defer t.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
			}
			if cur := id.SPKIAndSerial(); cur != "" && cur != last {
				last = cur
				rotated()
			}
		}
	}()
}

// connRotator closes the object channel's server connections opened
// before the last rotation once they are idle.
type connRotator struct {
	mu    sync.Mutex
	gen   uint64
	conns map[net.Conn]*rotConn
}

type rotConn struct {
	gen  uint64
	idle bool
}

func newConnRotator() *connRotator { return &connRotator{conns: map[net.Conn]*rotConn{}} }

// ConnState is the http.Server hook.
func (r *connRotator) ConnState(c net.Conn, s http.ConnState) {
	r.mu.Lock()
	defer r.mu.Unlock()
	switch s {
	case http.StateNew:
		r.conns[c] = &rotConn{gen: r.gen}
	case http.StateActive:
		if rc := r.conns[c]; rc != nil {
			rc.idle = false
		}
	case http.StateIdle:
		rc := r.conns[c]
		if rc == nil {
			return
		}
		if rc.gen < r.gen {
			delete(r.conns, c)
			_ = c.Close()
			return
		}
		rc.idle = true
	case http.StateHijacked, http.StateClosed:
		delete(r.conns, c)
	}
}

// rotate closes the idle connections now; the busy ones close when their
// request ends. It returns how many were closed at once.
func (r *connRotator) rotate() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.gen++
	n := 0
	for c, rc := range r.conns {
		if rc.idle {
			delete(r.conns, c)
			_ = c.Close()
			n++
		}
	}
	return n
}

// open counts the connections tracked, for tests.
func (r *connRotator) open() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.conns)
}

// rotate drops the object channel's clients, so the next request
// handshakes with the renewed certificate.
func (oc *objectChannel) rotate() {
	oc.mu.Lock()
	old := oc.clients
	oc.clients = nil
	oc.mu.Unlock()
	for _, c := range old {
		c.CloseIdleConnections()
	}
}
