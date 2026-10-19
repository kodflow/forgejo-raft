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
	// prepared is set once git reached the hook's "prepared" phase. A
	// reservation alone (SSH's serv, before git runs) can publish nothing:
	// git publishes only after "prepared", which waits for a certification
	// of the repository. It counts against the caps, never as a fence, so
	// a long SSH upload does not hold the repository's projection, and the
	// consumer's queue behind it.
	prepared bool
	// proposed is set once the transaction's entry committed.
	proposed bool
}

// registry tracks native git transactions so the projector never certifies
// a repository's watermark while git may still publish a ref behind it.
// A registration waits while its repository is being certified.
type registry struct {
	mu      sync.Mutex
	byNonce map[string]*txn
	byRepo  map[string]map[string]*txn
	// certifying holds the repositories being certified, each with the
	// channel closed when its certification ends. The certification runs
	// git outside mu: only a transaction of THAT repository waits for it.
	certifying map[string]chan struct{}
	ttl        time.Duration
	perRepo    int
	total      int
	alive      func(pid int) bool
	now        func() time.Time
}

func newRegistry(ttl time.Duration, perRepo, total int) *registry {
	return &registry{
		byNonce: map[string]*txn{}, byRepo: map[string]map[string]*txn{}, certifying: map[string]chan struct{}{},
		ttl: ttl, perRepo: perRepo, total: total, alive: pidAlive, now: time.Now,
	}
}

var (
	errNoNonce     = errors.New("the transaction carries no cluster nonce; it was not started by Forgejo")
	errTooManyTxn  = errors.New("too many transactions in flight; retry")
	errNonceReused = errors.New("transaction nonce already in use")
)

// reserve records the nonce of a transaction git has not started yet (the
// SSH path, before receive-pack runs). It counts against the caps and
// fences nothing: "prepared" completes it, and only then does the
// repository stay uncertified.
func (r *registry) reserve(nonce, repo string, pid int) error {
	if nonce == "" {
		return errNoNonce
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if t, ok := r.byNonce[nonce]; ok {
		if t.repo != repo {
			return errNonceReused
		}
		return nil
	}
	return r.addLocked(&txn{nonce: nonce, repo: repo, pid: pid, deadline: r.now().Add(r.ttl)})
}

// register records a transaction in "prepared". Above the caps it refuses
// before git publishes anything.
func (r *registry) register(nonce, repo string, refs []string, pid int) error {
	if nonce == "" {
		return errNoNonce
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	// A certification of this repository reads git now: registering would
	// let git publish behind the watermark it is about to set. It runs two
	// git commands; past the registration TTL the push is turned away.
	var expired *time.Timer
	defer func() {
		if expired != nil {
			expired.Stop()
		}
	}()
	for {
		done, busy := r.certifying[repo]
		if !busy {
			break
		}
		if expired == nil {
			expired = time.NewTimer(r.ttl)
		}
		r.mu.Unlock()
		select {
		case <-done:
			r.mu.Lock()
		case <-expired.C:
			r.mu.Lock()
			return errTooManyTxn
		}
	}
	if t, ok := r.byNonce[nonce]; ok {
		// "register" from serv, then "prepared" from the hook, share a
		// nonce: the second call completes the first.
		if t.repo != repo {
			return errNonceReused
		}
		t.refs, t.deadline, t.prepared = refs, r.now().Add(r.ttl), true
		if pid > 0 {
			t.pid = pid
		}
		return nil
	}
	return r.addLocked(&txn{nonce: nonce, repo: repo, refs: refs, pid: pid, deadline: r.now().Add(r.ttl), prepared: true})
}

func (r *registry) addLocked(t *txn) error {
	if len(r.byNonce) >= r.total || len(r.byRepo[t.repo]) >= r.perRepo {
		return errTooManyTxn
	}
	r.byNonce[t.nonce] = t
	if r.byRepo[t.repo] == nil {
		r.byRepo[t.repo] = map[string]*txn{}
	}
	r.byRepo[t.repo][t.nonce] = t
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
// whose process still runs keeps the repository FENCED (R6-06). A
// reservation is never a fence: git has published nothing yet, and will
// not before its "prepared" waits out any certification.
func (r *registry) inFlight(repo string) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.liveLocked(repo, false)
}

// busy reports whether a git process may still write under the
// repository's directory, a reservation included: a receive-pack still
// uploading writes its quarantine there, and git gc waits for it. A
// certification and a rename's directory move do not: whatever the upload
// becomes is decided by Apply, which refuses a push into a key renamed
// away.
func (r *registry) busy(repo string) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.liveLocked(repo, true)
}

func (r *registry) liveLocked(repo string, reserved bool) bool {
	now := r.now()
	live := false
	for nonce, t := range r.byRepo[repo] {
		if now.Before(t.deadline) || (t.pid > 0 && r.alive(t.pid)) {
			live = live || t.prepared || reserved
			continue
		}
		r.releaseLocked(nonce)
	}
	return live
}

// certify runs f while no transaction can register for repo, and only when
// none is in flight; it reports whether f ran. f runs git outside the
// registry's lock, so a slow disk under one repository holds no other
// repository's push.
func (r *registry) certify(repo string, f func()) bool {
	if r.inFlight(repo) {
		return false
	}
	r.mu.Lock()
	if _, busy := r.certifying[repo]; busy || r.liveLocked(repo, false) {
		r.mu.Unlock()
		return false
	}
	done := make(chan struct{})
	r.certifying[repo] = done
	r.mu.Unlock()
	defer func() {
		r.mu.Lock()
		delete(r.certifying, repo)
		r.mu.Unlock()
		close(done)
	}()
	f()
	return true
}

// count returns the transactions in flight, for the status page.
func (r *registry) count() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.byNonce)
}
