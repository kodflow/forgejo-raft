// Copyright 2026 The Forgejo Authors. All rights reserved.
// SPDX-License-Identifier: GPL-3.0-or-later

package cluster

import (
	"context"
	"strings"
	"testing"
	"time"

	"forgejo.org/services/cluster/consensus"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// A new leader commits a no-op of its term, which the engine applies
// without the table: on a quiet forge, the barrier's index then stays above
// the table's forever. The fresh wait of every git request must still
// succeed at once, not wait READ_AFTER_WRITE_TIMEOUT and report the node
// behind (#69).
func TestHuntR3ProveFreshAfterLeaderChange(t *testing.T) {
	tc := newTestClusterOn(t, true, "node-a", "node-b", "node-c")
	work := t.TempDir()
	run(t, work, "init", "-q")
	c1 := commit(t, work, "one")
	zero := strings.Repeat("0", 40)
	require.Eventually(t, func() bool {
		return tc.pushVia(tc.nodes[0], work, "s/f.git", "refs/heads/main", zero, c1).OK
	}, 10*time.Second, 50*time.Millisecond)
	for _, n := range tc.nodes {
		require.Eventually(t, func() bool { return gitRef(t, n, "s/f.git", "refs/heads/main") == c1 },
			10*time.Second, 20*time.Millisecond, "projection on %s", n.slot)
	}

	freshWithin := func(n *testNode) (bool, time.Duration) {
		start := time.Now()
		ok := n.r.WaitFresh(context.Background(), "s/f.git")
		return ok, time.Since(start)
	}
	for _, n := range tc.nodes {
		ok, took := freshWithin(n)
		require.True(t, ok, "baseline on %s", n.slot)
		require.Less(t, took, time.Second, "baseline on %s", n.slot)
	}

	var leader, target *testNode
	require.Eventually(t, func() bool {
		leader, target = nil, nil
		for _, n := range tc.nodes {
			if n.r.Engine().Status().Role == consensus.RoleLeader {
				leader = n
			} else if target == nil {
				target = n
			}
		}
		return leader != nil && target != nil
	}, 5*time.Second, 10*time.Millisecond)
	// The engine's own transfer: an operation through Operate would write
	// audit commands, which would hide the defect.
	require.NoError(t, leader.r.Engine().TransferLeadership(context.Background(), consensus.Operator("test"), target.r.Engine().Status().ID))
	require.Eventually(t, func() bool { return target.r.Engine().Status().Role == consensus.RoleLeader }, 5*time.Second, 10*time.Millisecond)
	// Every node applied the new term's no-op, which the table never sees.
	for _, n := range tc.nodes {
		require.Eventually(t, func() bool {
			pos, _ := n.r.table.Applied()
			return n.r.Engine().Status().Applied > pos.Index
		}, 5*time.Second, 10*time.Millisecond, "the no-op applied on %s", n.slot)
	}
	for _, n := range tc.nodes {
		ok, took := freshWithin(n)
		assert.True(t, ok, "WaitFresh on %s must succeed after a leader change on a quiet forge", n.slot)
		assert.Less(t, took, time.Second, "WaitFresh on %s must not wait for an entry no repository holds", n.slot)
	}
}
