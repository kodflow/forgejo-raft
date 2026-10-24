// Copyright 2026 The Forgejo Authors. All rights reserved.
// SPDX-License-Identifier: GPL-3.0-or-later

package cluster

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"testing"

	"forgejo.org/services/cluster/consensus"
	"forgejo.org/services/cluster/consensus/memraft"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// refusingEngine answers Remove and DenyPin as the SDK's leader does when
// it refuses them: the removal of a member that may strand a voter
// (kitsunium/sdk#603), the denial of a pin without which the voters that
// answer are no majority. memraft models neither.
type refusingEngine struct{ consensus.Engine }

func (refusingEngine) Remove(context.Context, consensus.Operator, consensus.NodeID) error {
	return consensus.ErrMembershipInvalid
}

func (refusingEngine) DenyPin(context.Context, consensus.Operator, consensus.NodeID) error {
	return consensus.ErrMembershipInvalid
}

// keyState reports, on r, whether the key of slot is revoked and whether
// the object channel admits it.
func keyState(t *testing.T, r *Replicator, slot string) (revoked bool, admitted error) {
	t.Helper()
	key := sha256.Sum256([]byte(slot))
	oc := &objectChannel{engine: r.Engine, revoked: r.table.KeyRevoked}
	_, admitted = oc.admit(peer{Slot: slot, SPKI: key})
	return r.table.KeyRevoked(hex.EncodeToString(key[:])), admitted
}

// A Remove submitted on a follower, where the check of a stranded voter
// sees no peers, and refused by the leader: the member keeps its key and
// its object channel (#157). Refused for certain, the Remove leaves no
// request either (#171); the one a Remove that is made records is written
// once the member left.
func TestARemoveTheLeaderRefusesLeavesTheKeyUsable(t *testing.T) {
	t.Parallel()
	mem, rs := NewForTesting(t.TempDir(), "node-a", "node-b", "node-c", "node-d")
	ctx := context.Background()
	require.NoError(t, rs[0].RaisePayloadVersion(ctx, "op"))
	leader, follower := rs[3], rs[2]
	mem.SetLeader(leader.Engine().(*memraft.Node))
	mem.SetDown(rs[0].Engine().(*memraft.Node), true)
	idB := slotID(t, leader, "node-b")
	real := follower.Engine()
	follower.setEngine(refusingEngine{real})
	require.NoError(t, follower.removable(follower.Engine(), idB), "the follower's check passes")

	err := follower.Operate(ctx, OperatorFor(1), Operation{Action: ActionRemove, Target: idB.String()})
	require.ErrorIs(t, err, consensus.ErrMembershipInvalid)
	assert.Positive(t, Rank(leader.Engine().Membership(), idB), "node-b is still a member")
	for _, r := range rs[1:] {
		revoked, admitted := keyState(t, r, "node-b")
		assert.False(t, revoked, "the key of a member the engine keeps")
		require.NoError(t, admitted)
	}
	for _, r := range rs[1:] {
		assert.Empty(t, r.table.PendingRevocations(), "the refused Remove's request is withdrawn")
	}
	require.NoError(t, leader.flushRevocations(ctx))
	revoked, _ := keyState(t, leader, "node-b")
	assert.False(t, revoked, "and nothing is written while node-b holds the key")

	follower.setEngine(real)
	mem.SetDown(rs[0].Engine().(*memraft.Node), false)
	require.NoError(t, follower.Operate(ctx, OperatorFor(1), Operation{Action: ActionRemove, Target: idB.String()}))
	assert.Zero(t, Rank(leader.Engine().Membership(), idB))
	revoked, _ = keyState(t, leader, "node-b")
	assert.True(t, revoked, "written once node-b left")
	assert.Empty(t, leader.table.PendingRevocations())
}

// A DenyPin the engine refuses leaves the key usable, on the leader as
// elsewhere; the leader's own pin is refused before anything is written
// (#158).
func TestADenyPinTheEngineRefusesLeavesTheKeyUsable(t *testing.T) {
	t.Parallel()
	mem, rs := NewForTesting(t.TempDir(), "node-a", "node-b", "node-c")
	ctx := context.Background()
	require.NoError(t, rs[0].RaisePayloadVersion(ctx, "op"))
	leader := rs[0]
	mem.SetLeader(leader.Engine().(*memraft.Node))
	idA, idB := slotID(t, leader, "node-a"), slotID(t, leader, "node-b")

	for _, r := range rs {
		err := r.Operate(ctx, OperatorFor(1), Operation{Action: ActionDenyPin, Target: idA.String()})
		require.ErrorContains(t, err, "transfer the leadership")
	}
	revoked, admitted := keyState(t, rs[1], "node-a")
	assert.False(t, revoked, "the leader's key")
	require.NoError(t, admitted)
	assert.Empty(t, leader.table.PendingRevocations())

	real := leader.Engine()
	leader.setEngine(refusingEngine{real})
	err := leader.Operate(ctx, OperatorFor(1), Operation{Action: ActionDenyPin, Target: idB.String()})
	require.ErrorIs(t, err, consensus.ErrMembershipInvalid)
	for _, r := range rs {
		revoked, admitted := keyState(t, r, "node-b")
		assert.False(t, revoked, "the key of a member whose pin the engine kept")
		require.NoError(t, admitted)
	}

	leader.setEngine(real)
	require.NoError(t, leader.Operate(ctx, OperatorFor(1), Operation{Action: ActionDenyPin, Target: idB.String()}))
	revoked, admitted = keyState(t, leader, "node-b")
	assert.True(t, revoked, "written once the pin is denied")
	require.Error(t, admitted)
}

// The revocation asked for refuses the key at the JOIN as soon as it
// commits, before it is written: a removed node does not join again in
// between, nor when the node that removed it stopped before writing it
// (the guarantee #64's order gave). The next flush writes it.
func TestAnAskedRevocationRefusesTheJoinBeforeItIsWritten(t *testing.T) {
	t.Parallel()
	mem, rs := NewForTesting(t.TempDir(), "node-a", "node-b", "node-c", "node-d")
	ctx := context.Background()
	r := rs[0]
	require.NoError(t, r.RaisePayloadVersion(ctx, "op"))
	mem.SetLeader(r.Engine().(*memraft.Node))
	idB := slotID(t, r, "node-b")
	keyB := sha256.Sum256([]byte("node-b"))

	_, asked, _, err := r.revokeMember(ctx, r.Engine(), OperatorFor(1), idB)
	require.NoError(t, err)
	require.True(t, asked)
	require.NoError(t, r.Engine().Remove(ctx, OperatorFor(1), idB), "the change commits, the node stops there")
	require.NoError(t, r.grantJoin(ctx, OperatorFor(1), "node-b"))
	revoked, _ := keyState(t, r, "node-b")
	require.False(t, revoked)

	_, err = r.authorizeJoin(ctx, consensus.JoinRequest{Slot: "node-b", SPKI: keyB})
	require.ErrorContains(t, err, "revoked")
	err = r.Operate(ctx, OperatorFor(1), Operation{Action: ActionAddLearner, Target: "00000000000000aa:000102030405060708090a0b0c0d0e0f:node-b:" + hex.EncodeToString(keyB[:]) + ":127.0.0.1:1"})
	require.ErrorContains(t, err, "revoked")

	require.NoError(t, r.flushRevocations(ctx))
	revoked, _ = keyState(t, r, "node-b")
	assert.True(t, revoked)
}

// laggingMembership is an engine whose last appended membership is past
// the barrier: a change a new leader may still drop.
type laggingMembership struct{ consensus.Engine }

func (e laggingMembership) Membership() consensus.Membership {
	m := e.Engine.Membership()
	m.At.Index = 1 << 40
	return m
}

// A flush writes nothing on a membership not yet committed: a removal
// appended and dropped by a change of leader would leave its member with a
// revoked key (#157).
func TestAFlushWaitsForACommittedMembership(t *testing.T) {
	t.Parallel()
	_, rs := NewForTesting(t.TempDir(), "node-a", "node-b", "node-c", "node-d")
	ctx := context.Background()
	r := rs[0]
	require.NoError(t, r.RaisePayloadVersion(ctx, "op"))
	idB := slotID(t, r, "node-b")
	_, _, _, err := r.revokeMember(ctx, r.Engine(), OperatorFor(1), idB)
	require.NoError(t, err)
	require.NoError(t, r.Engine().Remove(ctx, OperatorFor(1), idB))

	real := r.Engine()
	r.setEngine(laggingMembership{real})
	require.Error(t, r.flushRevocations(ctx))
	revoked, _ := keyState(t, r, "node-b")
	assert.False(t, revoked)

	r.setEngine(real)
	require.NoError(t, r.flushRevocations(ctx))
	revoked, _ = keyState(t, r, "node-b")
	assert.True(t, revoked)
}
