// Copyright 2026 The Forgejo Authors. All rights reserved.
// SPDX-License-Identifier: GPL-3.0-or-later

package cluster

import (
	"context"
	"strings"
	"testing"
	"time"

	"forgejo.org/modules/clusterhook"
	"forgejo.org/services/cluster/consensus"
	"forgejo.org/services/cluster/reftable"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// catchUpOnBarrier is an engine whose node applies the log only once a
// commit boundary is asked of it: a node behind the log that still reaches
// the others.
type catchUpOnBarrier struct {
	consensus.Engine
	up func()
}

func (e catchUpOnBarrier) Barrier(ctx context.Context) (consensus.Token, error) {
	e.up()
	return e.Engine.Barrier(ctx)
}

// A node behind repositories created through another one listed the
// owner's keys on its stale table: the early check of an owner's rename
// measured an entry short of the one proposed, and let one too large
// through, which only the proposal refused, once the SQL was written and
// the directory moved (#182). The check now confirms the applied log
// first, and a node that cannot confirm refuses the rename, both before
// anything is written.
func TestTheEarlyCheckOfAnOwnerRenameConfirmsTheTableFirst(t *testing.T) {
	tc := newTestCluster(t, "node-a", "node-b", "node-c")
	raisePayload(t, tc)
	work := t.TempDir()
	run(t, work, "init", "-q")
	c1 := commit(t, work, "one")
	zero := strings.Repeat("0", 40)
	origin, lag := tc.nodes[0], tc.nodes[2]
	for _, name := range []string{"a", "b", "c"} {
		require.True(t, tc.pushVia(origin, work, "big/"+name+".git", "refs/heads/main", zero, c1).OK)
	}
	waitAll(t, tc, func(n *testNode) bool { return gitRef(t, n, "big/c.git", "refs/heads/main") == c1 })

	// lag misses two repositories created through origin.
	tc.mem.SetDown(lag.node, true)
	for _, name := range []string{"d", "e"} {
		require.True(t, tc.pushVia(origin, work, "big/"+name+".git", "refs/heads/main", zero, c1).OK)
	}
	owner := [2]string{"big", "huge"}
	require.Len(t, origin.r.ownerMoves("big", "huge"), 5)
	require.Len(t, lag.r.ownerMoves("big", "huge"), 3, "lag is behind")
	v, rn := origin.r.renameCommand(origin.r.ownerMoves("big", "huge"), owner)
	data, err := reftable.EncodeVersion(v, reftable.KindRepoRename, rn)
	require.NoError(t, err)
	lag.r.maxEntry = len(data) - 1
	before, _ := origin.r.table.Applied()

	// Cut off, lag cannot confirm: it refuses, and measures nothing.
	ctx, cancel := context.WithTimeout(t.Context(), 300*time.Millisecond)
	err = lag.r.ownerRenameRefusal(ctx, "big", "huge")
	cancel()
	require.Error(t, err)
	require.NotErrorIs(t, err, clusterhook.ErrTooLarge)
	assert.Contains(t, err.Error(), "could not confirm it applied the log")

	// Behind, but in reach: the confirmation brings the two repositories,
	// and the rename is refused as too large before anything is written.
	lag.r.setEngine(catchUpOnBarrier{Engine: lag.node, up: func() { tc.mem.SetDown(lag.node, false) }})
	require.Len(t, lag.r.ownerMoves("big", "huge"), 3, "lag is still behind")
	err = lag.r.ownerRenameRefusal(t.Context(), "big", "huge")
	require.ErrorIs(t, err, clusterhook.ErrTooLarge)
	assert.Contains(t, err.Error(), "renaming the owner big moves its 5 repositories")
	require.Len(t, lag.r.ownerMoves("big", "huge"), 5, "lag caught up")

	after, _ := origin.r.table.Applied()
	assert.Equal(t, before.Index, after.Index, "nothing was proposed")
	for _, n := range tc.nodes {
		_, _, renamed := n.r.table.RenamedFrom("huge/a.git")
		assert.False(t, renamed, n.slot)
		assert.True(t, dirExists(n, "big/a.git"), "%s: the owner's directory did not move", n.slot)
		assert.False(t, dirExists(n, "huge/a.git"), n.slot)
	}

	// Within the bound, the check passes.
	lag.r.maxEntry = len(data) + 64
	require.NoError(t, lag.r.ownerRenameRefusal(t.Context(), "big", "huge"))
}
