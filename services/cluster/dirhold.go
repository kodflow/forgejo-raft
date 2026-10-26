// Copyright 2026 The Forgejo Authors. All rights reserved.
// SPDX-License-Identifier: GPL-3.0-or-later

package cluster

import (
	"context"
	"strings"
	"sync"
)

// dirHolds keeps the cluster's own writers out of the directories of the
// owners Forgejo moves on this node. An owner's rename moves the owner's
// directory before its entry is proposed, and moves it back when the
// cluster refuses it; until the entry is applied, the table still names
// the owner's repositories under the old owner. A projection, a fetch of
// missing objects or a shipment of a push's objects that wrote there
// meanwhile created a repository's directory again under the old owner:
// left there once the rename was applied, it refused the next rename to
// that name, and the move back of a refused rename (#257).
//
// A writer enters the owner of every key it writes under, and is refused
// while that owner is held; a hold waits for the writers already in to
// leave. Holds nest.
type dirHolds struct {
	mu sync.Mutex
	// held counts the holds of each owner.
	held map[string]int
	// writing counts the writers in each owner's directory.
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

// enter admits a writer under the directories of keys, and reports false,
// admitting none, when the owner of one is held. leave ends it.
func (h *dirHolds) enter(keys ...string) (leave func(), ok bool) {
	if h == nil {
		return func() {}, true
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	for _, k := range keys {
		if h.held[keyOwner(k)] > 0 {
			return nil, false
		}
	}
	for _, k := range keys {
		h.writing[keyOwner(k)]++
	}
	var once sync.Once
	return func() {
		once.Do(func() {
			h.mu.Lock()
			defer h.mu.Unlock()
			for _, k := range keys {
				o := keyOwner(k)
				if h.writing[o]--; h.writing[o] <= 0 {
					delete(h.writing, o)
				}
			}
			close(h.left)
			h.left = make(chan struct{})
		})
	}, true
}

// hold refuses every new writer under the owners' directories, and returns
// once none is left in them, or ctx's error, the hold then undone. stop is
// called once the owners are held, before the wait: it ends what would
// keep writing there for long.
func (h *dirHolds) hold(ctx context.Context, owners []string, stop func()) (release func(), err error) {
	h.mu.Lock()
	for _, o := range owners {
		h.held[o]++
	}
	h.mu.Unlock()
	var once sync.Once
	release = func() {
		once.Do(func() {
			h.mu.Lock()
			defer h.mu.Unlock()
			for _, o := range owners {
				if h.held[o]--; h.held[o] <= 0 {
					delete(h.held, o)
				}
			}
		})
	}
	if stop != nil {
		stop()
	}
	for {
		h.mu.Lock()
		busy := false
		for _, o := range owners {
			busy = busy || h.writing[o] > 0
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
