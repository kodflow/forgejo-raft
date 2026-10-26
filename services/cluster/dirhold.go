// Copyright 2026 The Forgejo Authors. All rights reserved.
// SPDX-License-Identifier: GPL-3.0-or-later

package cluster

import (
	"context"
	"slices"
	"strings"
	"sync"
)

// dirHolds keeps the cluster's own writers out of the directories Forgejo
// moves on this node: an owner's, which its rename moves whole, or a
// repository's and its wiki's, which their rename or transfer moves.
// Forgejo moves them before the entry is proposed, and back when the
// cluster refuses it; until the entry is applied, the table still names
// the repositories under their old keys. A projection, a fetch of missing
// objects or a shipment of a push's objects that wrote there meanwhile
// created a repository's directory again under the old name: left there
// once the rename was applied, it refused the next rename to that name,
// and the move back of a refused rename (#257); under a repository's old
// key, it held the moved repository's objects, which a repository created
// later under the freed name adopted and served under its own rights
// (#274).
//
// A hold names owners, or repository keys: an owner's covers every key
// under it. A writer enters the keys it writes under, and is refused while
// one of them, or its owner, is held; a hold waits for the writers
// already in there to leave. Holds nest.
type dirHolds struct {
	mu sync.Mutex
	// held counts the holds of each owner and of each key.
	held map[string]int
	// writing counts the writers under each key.
	writing map[string]int
	// left is closed, and replaced, whenever a writer leaves.
	left chan struct{}
}

func newDirHolds() *dirHolds {
	return &dirHolds{held: map[string]int{}, writing: map[string]int{}, left: make(chan struct{})}
}

// keyOwner is the owner whose directory holds the key's.
func keyOwner(key string) string {
	owner, _, _ := strings.Cut(key, "/")
	return owner
}

// dirCovered reports whether a hold of names covers key: it names the key,
// or its owner. A key always has its owner and a slash; an owner has none.
func dirCovered(names map[string]bool, key string) bool {
	return names[key] || names[keyOwner(key)]
}

// heldLocked reports whether key, or its owner, is held. h.mu is held.
func (h *dirHolds) heldLocked(key string) bool {
	return h.held[key] > 0 || h.held[keyOwner(key)] > 0
}

// enter admits a writer under the directories of keys, and reports false,
// admitting none, when one of them, or its owner, is held. leave ends it.
func (h *dirHolds) enter(keys ...string) (leave func(), ok bool) {
	if h == nil {
		return func() {}, true
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	if !h.admitLocked(keys) {
		return nil, false
	}
	var once sync.Once
	return func() { once.Do(func() { h.leave(keys) }) }, true
}

// admitLocked counts a writer under keys, unless one of them is held.
// h.mu is held.
func (h *dirHolds) admitLocked(keys []string) bool {
	if slices.ContainsFunc(keys, h.heldLocked) {
		return false
	}
	for _, k := range keys {
		h.writing[k]++
	}
	return true
}

// leave uncounts a writer under keys, and wakes the holds waiting for it.
func (h *dirHolds) leave(keys []string) {
	h.mu.Lock()
	defer h.mu.Unlock()
	for _, k := range keys {
		if h.writing[k]--; h.writing[k] <= 0 {
			delete(h.writing, k)
		}
	}
	close(h.left)
	h.left = make(chan struct{})
}

// hold refuses every new writer under the directories names covers, owners
// or repository keys, and returns once none is left in them, or ctx's
// error, the hold then undone. stop is called once they are held, before
// the wait, with what the hold covers: it ends what would keep writing
// there for long.
func (h *dirHolds) hold(ctx context.Context, names []string, stop func(covers func(key string) bool)) (release func(), err error) {
	in := make(map[string]bool, len(names))
	for _, n := range names {
		in[n] = true
	}
	covers := func(key string) bool { return dirCovered(in, key) }
	h.mu.Lock()
	for _, n := range names {
		h.held[n]++
	}
	h.mu.Unlock()
	var once sync.Once
	release = func() {
		once.Do(func() {
			h.mu.Lock()
			defer h.mu.Unlock()
			for _, n := range names {
				if h.held[n]--; h.held[n] <= 0 {
					delete(h.held, n)
				}
			}
		})
	}
	if stop != nil {
		stop(covers)
	}
	for {
		h.mu.Lock()
		busy := false
		for k := range h.writing {
			if covers(k) {
				busy = true
				break
			}
		}
		left := h.left
		h.mu.Unlock()
		if !busy {
			return release, nil
		}
		select {
		case <-left:
		case <-ctx.Done():
			release()
			return nil, ctx.Err()
		}
	}
}
