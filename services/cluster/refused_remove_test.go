// Copyright 2026 The Forgejo Authors. All rights reserved.
// SPDX-License-Identifier: GPL-3.0-or-later

package cluster

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"strconv"
	"testing"

	"forgejo.org/services/cluster/consensus"
	"forgejo.org/services/cluster/reftable"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// slotID is the id of the member of slot in r's membership.
func slotID(t *testing.T, r *Replicator, slot string) consensus.NodeID {
	t.Helper()
	for _, m := range r.Engine().Membership().Members {
		if m.Slot == slot {
			return m.ID
		}
	}
	t.Fatalf("no member of slot %s", slot)
	return 0
}

// A Remove the engine would refuse, below the floor of three voters, is
// refused before the key is revoked: the member keeps its place and its
// object channel (#64). At a version that cannot write a revocation, no
// pending revocation is left for the next raise to write.
func TestHuntR3ProveRefusedRemove(t *testing.T) {
	for _, raised := range []bool{true, false} {
		mem, rs := NewForTesting(t.TempDir(), "node-a")
		r := rs[0]
		ctx := context.Background()
		keyB := sha256.Sum256([]byte("node-b"))
		mem.Add("node-b", keyB, reftable.New())
		mem.Add("node-c", sha256.Sum256([]byte("node-c")), reftable.New())
		if raised {
			require.NoError(t, r.RaisePayloadVersion(ctx, "op"))
		}
		idB := slotID(t, r, "node-b")
		err := r.Operate(ctx, OperatorFor(1), Operation{Action: ActionRemove, Target: idB.String()})
		require.ErrorIs(t, err, consensus.ErrMembershipInvalid)
		assert.Equal(t, 2, Rank(r.Engine().Membership(), idB), "node-b is still a member")
		assert.False(t, r.table.KeyRevoked(hex.EncodeToString(keyB[:])), "raised %t: the key of a member the engine kept", raised)
		assert.Empty(t, r.table.PendingRevocations(), "raised %t", raised)
		if !raised {
			require.NoError(t, r.Operate(ctx, OperatorFor(1), Operation{Action: ActionRaisePayload, Target: strconv.Itoa(reftable.PayloadVersion)}))
			assert.False(t, r.table.KeyRevoked(hex.EncodeToString(keyB[:])), "the raise wrote no revocation of a member")
		}
		oc := &objectChannel{engine: r.Engine, revoked: r.table.KeyRevoked}
		_, err = oc.admit(peer{Slot: "node-b", SPKI: keyB})
		require.NoError(t, err)
	}
}

// The leader is never removed, so its key is not revoked: the operator
// transfers the leadership first (#64).
func TestHuntR3ProveRemoveLeader(t *testing.T) {
	mem, rs := NewForTesting(t.TempDir(), "node-a")
	r := rs[0]
	ctx := context.Background()
	for _, s := range []string{"node-b", "node-c", "node-d"} {
		mem.Add(s, sha256.Sum256([]byte(s)), reftable.New())
	}
	require.NoError(t, r.RaisePayloadVersion(ctx, "op"))
	leader := r.Engine().Status().Leader
	require.NotZero(t, leader)
	err := r.Operate(ctx, OperatorFor(1), Operation{Action: ActionRemove, Target: leader.String()})
	require.ErrorContains(t, err, "transfer the leadership")
	keyA := sha256.Sum256([]byte("node-a"))
	assert.False(t, r.table.KeyRevoked(hex.EncodeToString(keyA[:])))
	assert.Positive(t, Rank(r.Engine().Membership(), leader))
}

// A pending revocation of a key a member still holds is not written by the
// raise: the member would be cut off its peers' object channels (#64).
func TestPendingRevocationOfAHeldKeyIsNotWritten(t *testing.T) {
	mem, rs := NewForTesting(t.TempDir(), "node-a")
	r := rs[0]
	ctx := context.Background()
	keyB := sha256.Sum256([]byte("node-b"))
	mem.Add("node-b", keyB, reftable.New())
	mem.Add("node-c", sha256.Sum256([]byte("node-c")), reftable.New())
	// An older binary recorded it before a Remove the engine refused.
	require.NoError(t, r.recordPendingRevocation(ctx, reftable.RevokeKey{SPKI: hex.EncodeToString(keyB[:]), Slot: "node-b", Operator: "op", Time: 1}))
	require.NoError(t, r.RaisePayloadVersion(ctx, "op"))
	r.flushRevocations(ctx)
	assert.False(t, r.table.KeyRevoked(hex.EncodeToString(keyB[:])))
	assert.Len(t, r.table.PendingRevocations(), 1, "kept for when the member is gone")
}
