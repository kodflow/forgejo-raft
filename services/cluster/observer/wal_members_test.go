// Copyright 2026 The Forgejo Authors. All rights reserved.
// SPDX-License-Identifier: GPL-3.0-or-later

package observer

import (
	"context"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Only the PostgreSQL primary's archive advances, and its host need not
// lead Raft: the observer takes the WAL from whichever member lists it
// (#57).
func TestObserverTakesTheWALFromTheMemberThatHoldsIt(t *testing.T) {
	ctx := context.Background()
	c := newFakeCluster(t)
	c.walFrom = map[string]bool{"node-b": true}
	o := newTestObserver(t, c, &hookLog{})
	seed(t, c, o)
	require.NoError(t, o.Step(ctx))
	st := o.State()
	require.Len(t, st.Checkpoints, 1)
	assert.Equal(t, StateProtected, st.Checkpoints[0].State)
	assert.Len(t, st.Bases, 1)
}

// A base taken because the ones held cover nothing replaces them: the
// bases no checkpoint restores from never pile up (#57).
func TestObserverKeepsOneIdleBase(t *testing.T) {
	ctx := context.Background()
	c := newFakeCluster(t)
	o := newTestObserver(t, c, &hookLog{})
	// No archive is reachable, and every base is older than BaseEvery:
	// each pass takes another, which covers nothing either.
	c.walFrom = map[string]bool{}
	o.cfg.BaseEvery = 1
	c.push("alice/demo.git", "refs/heads/main", c.commit("one"))
	c.checkpoint()
	require.ErrorIs(t, o.Step(ctx), ErrNoCoverage)
	first := o.State().Bases[0].Label
	for range 3 {
		c.checkpoint()
		require.ErrorIs(t, o.Step(ctx), ErrNoCoverage)
	}
	st := o.State()
	require.Len(t, st.Bases, 1)
	assert.NotEqual(t, first, st.Bases[0].Label)
	entries, err := os.ReadDir(o.store.path("pg", "base"))
	require.NoError(t, err)
	assert.Len(t, entries, 1)
}
