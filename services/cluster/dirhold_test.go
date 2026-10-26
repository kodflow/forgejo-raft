// Copyright 2026 The Forgejo Authors. All rights reserved.
// SPDX-License-Identifier: GPL-3.0-or-later

package cluster

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// A hold of owners' directories refuses their writers from the moment it
// is taken, returns once the writers already in left, nests, and leaves
// the other owners alone; one whose wait ends in ctx's error holds nothing
// (#257).
func TestAnOwnerDirectoryHoldWaitsForTheWritersAlreadyIn(t *testing.T) {
	t.Parallel()
	h := newDirHolds()
	admitted := func(key string) bool {
		leave, ok := h.enter(key)
		if ok {
			leave()
		}
		return ok
	}
	leave, ok := h.enter("alice/r.git")
	require.True(t, ok)
	type held struct {
		release func()
		err     error
	}
	got := make(chan held, 1)
	go func() {
		release, err := h.hold(context.Background(), []string{"alice", "bob"}, nil)
		got <- held{release, err}
	}()
	require.Eventually(t, func() bool { return !admitted("bob/s.git") }, 5*time.Second, time.Millisecond, "refused once held")
	assert.False(t, admitted("alice/q.git"))
	assert.True(t, admitted("carol/t.git"), "another owner is not held")
	select {
	case <-got:
		t.Fatal("the hold returned while a writer was still in")
	case <-time.After(100 * time.Millisecond):
	}
	leave()
	var first held
	select {
	case first = <-got:
	case <-time.After(5 * time.Second):
		t.Fatal("the hold did not return once the writer left")
	}
	require.NoError(t, first.err)

	second, err := h.hold(t.Context(), []string{"alice"}, nil)
	require.NoError(t, err, "nothing is in")
	second()
	assert.False(t, admitted("alice/r.git"), "the first hold still holds")
	first.release()
	first.release()
	assert.True(t, admitted("alice/r.git"))
	assert.True(t, admitted("bob/s.git"))

	leave, ok = h.enter("dave/z.git")
	require.True(t, ok)
	ctx, cancel := context.WithTimeout(t.Context(), 50*time.Millisecond)
	defer cancel()
	_, err = h.hold(ctx, []string{"dave"}, nil)
	require.ErrorIs(t, err, context.DeadlineExceeded)
	assert.True(t, admitted("dave/w.git"), "a hold that failed holds nothing")
	leave()
}

// A fetch of missing objects runs beside the pass and writes under its
// repository's directory once the pass moved on: a hold of its owner
// cancels it and waits for its end, the pass then creates nothing under
// the held owner, whose directory may be gone, and projects every other
// one; released, the repository is projected again (#257).
func TestAnOwnerDirectoryHoldEndsTheFetchesUnderIt(t *testing.T) {
	t.Parallel()
	b := newFetchBench(t, "alice/r.git", "bob/s.git")
	gate := b.hold("alice/r.git")
	b.p.unfence()
	b.p.pass(t.Context())
	require.Eventually(t, func() bool {
		b.mu.Lock()
		defer b.mu.Unlock()
		return b.inFlight["alice/r.git"] == 1
	}, 10*time.Second, time.Millisecond, "alice's fetch runs")

	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	release, err := b.p.holdDirs(ctx, []string{"alice", "heir"})
	require.NoError(t, err, "the fetch was cancelled, not waited for")
	assert.False(t, b.p.isFetching("alice/r.git"))
	b.mu.Lock()
	assert.Zero(t, b.inFlight["alice/r.git"], "the fetch ended")
	b.mu.Unlock()

	// Forgejo moves the owner's directory meanwhile.
	root := b.p.root
	require.NoError(t, os.Rename(filepath.Join(root, "alice"), filepath.Join(root, "heir")))
	require.Eventually(t, func() bool {
		b.p.pass(t.Context())
		return b.p.Projected("bob/s.git") > 0
	}, 10*time.Second, 10*time.Millisecond, "another owner is projected")
	_, err = os.Stat(filepath.Join(root, "alice"))
	assert.True(t, os.IsNotExist(err), "nothing is created under the held owner: %v", err)
	assert.Zero(t, b.p.Projected("alice/r.git"))

	// The rename refused, Forgejo moves it back, then releases.
	require.NoError(t, os.Rename(filepath.Join(root, "heir"), filepath.Join(root, "alice")))
	close(gate)
	release()
	require.Eventually(t, func() bool {
		b.p.pass(t.Context())
		return b.p.Projected("alice/r.git") > 0
	}, 10*time.Second, 10*time.Millisecond, "projected again once released")
}

// A hold of repository keys, a repository's rename or transfer, refuses
// the writers of those keys from the moment it is taken, and not the other
// keys of their owners; it returns once the writers of its keys already in
// left, whatever the other keys' writers do, and nests with a hold of the
// owner (#274).
func TestARepositoryKeyHoldWaitsForTheWritersOfItsKeys(t *testing.T) {
	t.Parallel()
	h := newDirHolds()
	admitted := func(key string) bool {
		leave, ok := h.enter(key)
		if ok {
			leave()
		}
		return ok
	}
	leave, ok := h.enter("alice/r.git")
	require.True(t, ok)
	sibling, ok := h.enter("alice/q.git")
	require.True(t, ok)
	type held struct {
		release func()
		err     error
	}
	got := make(chan held, 1)
	go func() {
		release, err := h.hold(context.Background(), []string{"alice/r.git", "alice/s.git"}, nil)
		got <- held{release, err}
	}()
	require.Eventually(t, func() bool { return !admitted("alice/s.git") }, 5*time.Second, time.Millisecond, "refused once held")
	assert.False(t, admitted("alice/r.git"))
	assert.True(t, admitted("alice/q.git"), "another key of the owner is not held")
	assert.True(t, admitted("alice/r.wiki.git"), "a key the hold does not name is not held")
	sibling()
	select {
	case <-got:
		t.Fatal("the hold returned while a writer of its key was still in")
	case <-time.After(100 * time.Millisecond):
	}
	leave()
	var first held
	select {
	case first = <-got:
	case <-time.After(5 * time.Second):
		t.Fatal("the hold did not return once the writer left")
	}
	require.NoError(t, first.err)

	owner, err := h.hold(t.Context(), []string{"alice"}, nil)
	require.NoError(t, err, "nothing is in")
	first.release()
	assert.False(t, admitted("alice/r.git"), "the owner's hold still holds the key")
	owner()
	assert.True(t, admitted("alice/r.git"))
	assert.True(t, admitted("alice/s.git"))
}

// A hold of a repository's key cancels the fetch in flight under that key
// and waits for its end, while the fetch of another key of the same owner
// goes on (#274).
func TestARepositoryKeyHoldEndsTheFetchOfItsKeyAlone(t *testing.T) {
	t.Parallel()
	b := newFetchBench(t, "alice/r.git", "alice/q.git")
	gateR := b.hold("alice/r.git")
	gateQ := b.hold("alice/q.git")
	b.p.unfence()
	b.p.pass(t.Context())
	require.Eventually(t, func() bool {
		b.mu.Lock()
		defer b.mu.Unlock()
		return b.inFlight["alice/r.git"] == 1 && b.inFlight["alice/q.git"] == 1
	}, 10*time.Second, time.Millisecond, "both fetches run")

	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	release, err := b.p.holdDirs(ctx, []string{"alice/r.git", "alice/s.git"})
	require.NoError(t, err, "the fetch was cancelled, not waited for")
	assert.False(t, b.p.isFetching("alice/r.git"))
	assert.True(t, b.p.isFetching("alice/q.git"), "the fetch of another key of the owner goes on")

	close(gateQ)
	close(gateR)
	release()
	require.Eventually(t, func() bool {
		b.p.pass(t.Context())
		return b.p.Projected("alice/r.git") > 0 && b.p.Projected("alice/q.git") > 0
	}, 10*time.Second, 10*time.Millisecond, "projected again once released")
}
