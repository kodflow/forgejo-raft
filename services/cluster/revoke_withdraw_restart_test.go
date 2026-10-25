// Copyright 2026 The Forgejo Authors. All rights reserved.
// SPDX-License-Identifier: GPL-3.0-or-later

package cluster

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"testing"

	"forgejo.org/services/cluster/consensus"
	"forgejo.org/services/cluster/consensus/memraft"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// restartTable does to r's table what the engine does when the node opens
// again (raft load, restoreLatest) or installs a snapshot sent by the
// leader: Restore from a snapshot of the table, cut at its applied index.
func restartTable(t *testing.T, r *Replicator) {
	t.Helper()
	snap, err := r.table.Snapshot()
	require.NoError(t, err)
	var buf bytes.Buffer
	_, err = snap.WriteTo(&buf)
	require.NoError(t, err)
	require.NoError(t, r.table.Restore(bytes.NewReader(buf.Bytes()), consensus.SnapshotMeta{}))
}

// TestARefusedRemoveLeavesNoRevocationOnceTheMemberLeavesAnotherWay (#171)
// with one restart of the leader between the refusal and the member's
// departure: the withdrawn request came back from the snapshot, and the
// key of a member whose only Remove was refused was revoked for good, as
// before #171's fix (#223).
func TestAWithdrawnRequestStaysWithdrawnAcrossARestart(t *testing.T) {
	mem, rs := NewForTesting(t.TempDir(), "node-a", "node-b", "node-c", "node-d", "node-e")
	ctx := context.Background()
	leader := rs[0]
	require.NoError(t, leader.RaisePayloadVersion(ctx, "op"))
	mem.SetLeader(leader.Engine().(*memraft.Node))
	idB, idC := slotID(t, leader, "node-b"), slotID(t, leader, "node-c")
	keyB, keyC := sha256.Sum256([]byte("node-b")), sha256.Sum256([]byte("node-c"))
	spkiB := hex.EncodeToString(keyB[:])

	// 1. The operator's Remove of node-b, refused by the engine for
	// certain: its request is withdrawn on every member.
	real := leader.Engine()
	leader.setEngine(refusingEngine{real})
	err := leader.Operate(ctx, OperatorFor(1), Operation{Action: ActionRemove, Target: idB.String()})
	leader.setEngine(real)
	require.ErrorIs(t, err, consensus.ErrMembershipInvalid)
	for _, r := range rs {
		require.Empty(t, r.table.PendingRevocations(), "the refused change left its request")
	}

	// 2. The leader restarts (or any member installs a snapshot cut since):
	// its table is the one the others replayed.
	restartTable(t, leader)
	t.Logf("after the leader's restart: pending on the leader %v (requests %v); on node-b's own table %v",
		leader.table.PendingRevocations(), leader.table.RevocationRequests(spkiB), rs[1].table.PendingRevocations())
	for _, r := range rs[1:] {
		assert.Equal(t, r.table.PendingRevocations(), leader.table.PendingRevocations(), "restored or replayed, the same requests")
		assert.Equal(t, r.table.RevocationRequests(spkiB), leader.table.RevocationRequests(spkiB))
	}

	// 3. Later node-b and node-c are out of reach past DEAD_MEMBER_TIMEOUT:
	// the autopilot removes them, then flushRevocations at a later pass.
	require.True(t, leader.autoChange(ctx, ActionRemove, idB, "out of reach for DEAD_MEMBER_TIMEOUT", real.Remove))
	require.True(t, leader.autoChange(ctx, ActionRemove, idC, "out of reach for DEAD_MEMBER_TIMEOUT", real.Remove))
	require.NoError(t, leader.flushRevocations(ctx))
	for _, r := range rs {
		t.Logf("%s: key of node-b revoked %t, key of node-c revoked %t", r.Engine().Status().Slot,
			r.table.KeyRevoked(spkiB), r.table.KeyRevoked(hex.EncodeToString(keyC[:])))
	}

	// 4. Both come back, an operator grants their slots.
	require.NoError(t, leader.grantJoin(ctx, OperatorFor(1), "node-b"))
	require.NoError(t, leader.grantJoin(ctx, OperatorFor(1), "node-c"))
	_, errC := leader.authorizeJoin(ctx, consensus.JoinRequest{Slot: "node-c", SPKI: keyC})
	_, errB := leader.authorizeJoin(ctx, consensus.JoinRequest{Slot: "node-b", SPKI: keyB})
	t.Logf("JOIN node-c: %v", errC)
	t.Logf("JOIN node-b: %v", errB)
	require.NoError(t, errC)
	assert.False(t, leader.table.KeyRevoked(spkiB), "a refused Remove revoked nothing, then or later")
	assert.NoError(t, errB, "node-b is treated as node-c: its only Remove was refused")
}

// The same with a learner whose Remove was refused: once the leader
// restarted, the autopilot removed it as a learner whose key is asked for
// revocation (revokedLearner), then wrote the revocation (#223).
func TestALearnerWhoseRemoveWasRefusedIsKeptAcrossARestart(t *testing.T) {
	mem, rs := NewForTesting(t.TempDir(), "node-a", "node-b", "node-c")
	ctx := context.Background()
	leader := rs[0]
	require.NoError(t, leader.RaisePayloadVersion(ctx, "op"))
	mem.SetLeader(leader.Engine().(*memraft.Node))
	keyL := sha256.Sum256([]byte("node-l"))
	learner := consensus.Member{ID: 0x1e, Slot: "node-l", SPKI: keyL, Address: "127.0.0.1:1"}
	require.NoError(t, leader.Engine().AddLearner(ctx, "op", learner))

	real := leader.Engine()
	leader.setEngine(refusingEngine{real})
	err := leader.Operate(ctx, OperatorFor(1), Operation{Action: ActionRemove, Target: learner.ID.String()})
	leader.setEngine(real)
	require.ErrorIs(t, err, consensus.ErrMembershipInvalid)
	_, before := leader.revokedLearner(leader.Engine().Membership())
	require.False(t, before, "the refused Remove left no effect")

	restartTable(t, leader)
	id, after := leader.revokedLearner(leader.Engine().Membership())
	t.Logf("after the leader's restart: revokedLearner names %s: %t", id, after)
	assert.False(t, after, "a learner whose only Remove was refused is not removed after a restart")
}
