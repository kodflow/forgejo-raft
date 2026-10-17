// Copyright 2026 The Forgejo Authors. All rights reserved.
// SPDX-License-Identifier: GPL-3.0-or-later

package cluster

import (
	"errors"
	"sync"
	"time"
)

// txn is a native git transaction in flight: registered in "prepared",
// released in "committed" or "aborted" (ADR 0152 D19, R4-24, R5-04, R6-06).
type txn struct {
	nonce    string
	repo     string
	refs     []string
	pid      int
	deadline time.Time
	// proposed is set once the transaction's entry committed.
	proposed bool
}

// registry tracks native git transactions so the projector never certifies
// a repository's watermark while git may still publish a ref behind it.
// Registration and certification take the same lock.
type registry struct {
	mu      sync.Mutex
	byNonce map[string]*txn
	byRepo  map[string]map[string]*txn
	ttl     time.Duration
	perRepo int
	total   int
	alive   func(pid int) bool
	now     func() time.Time
}

func newRegistry(ttl time.Duration, perRepo, total int) *registry {
	return &registry{
		byNonce: map[string]*txn{}, byRepo: map[string]map[string]*txn{},
		ttl: ttl, perRepo: perRepo, total: total, alive: pidAlive, now: time.Now,
	}
}

var (
	errNoNonce     = errors.New("the transaction carries no cluster nonce; it was not started by Forgejo")
	errTooManyTxn  = errors.New("too many transactions in flight; retry")
	errNonceReused = errors.New("transaction nonce already in use")
)

// register records a transaction in "prepared". Above the caps it refuses
// before git publishes anything.
func (r *registry) register(nonce, repo string, refs []string, pid int) error {
	if nonce == "" {
		return errNoNonce
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if t, ok := r.byNonce[nonce]; ok {
		// "register" from serv, then "prepared" from the hook, share a
		// nonce: the second call completes the first.
		if t.repo != repo {
			return errNonceReused
		}
		t.refs, t.deadline = refs, r.now().Add(r.ttl)
		if pid > 0 {
			t.pid = pid
		}
		return nil
	}
	if len(r.byNonce) >= r.total || len(r.byRepo[repo]) >= r.perRepo {
		return errTooManyTxn
	}
	t := &txn{nonce: nonce, repo: repo, refs: refs, pid: pid, deadline: r.now().Add(r.ttl)}
	r.byNonce[nonce] = t
	if r.byRepo[repo] == nil {
		r.byRepo[repo] = map[string]*txn{}
	}
	r.byRepo[repo][nonce] = t
	return nil
}

// release removes a transaction, explicitly (committed, aborted, a hook
// refusal) or after its process is gone.
func (r *registry) release(nonce string) *txn {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.releaseLocked(nonce)
}

func (r *registry) releaseLocked(nonce string) *txn {
	t := r.byNonce[nonce]
	if t == nil {
		return nil
	}
	delete(r.byNonce, nonce)
	if m := r.byRepo[t.repo]; m != nil {
		delete(m, nonce)
		if len(m) == 0 {
			delete(r.byRepo, t.repo)
		}
	}
	return t
}

func (r *registry) markProposed(nonce string) {
	r.mu.Lock()
	if t := r.byNonce[nonce]; t != nil {
		t.proposed = true
	}
	r.mu.Unlock()
}

// inFlight reports whether the repository has a transaction that may still
// publish. An expired registration whose process is gone is dropped; one
// whose process still runs keeps the repository FENCED (R6-06).
func (r *registry) inFlight(repo string) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	now := r.now()
	for nonce, t := range r.byRepo[repo] {
		if now.Before(t.deadline) {
			return true
		}
		if t.pid > 0 && r.alive(t.pid) {
			return true
		}
		r.releaseLocked(nonce)
	}
	return false
}

// certify runs f while no transaction can register for repo, and only when
// none is in flight; it reports whether f ran.
func (r *registry) certify(repo string, f func()) bool {
	if r.inFlight(repo) {
		return false
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if len(r.byRepo[repo]) > 0 {
		return false
	}
	f()
	return true
}

// count returns the transactions in flight, for the status page.
func (r *registry) count() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.byNonce)
}
