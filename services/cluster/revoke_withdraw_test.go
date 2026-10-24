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
	"forgejo.org/services/cluster/consensus/memraft"
	"forgejo.org/services/cluster/reftable"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// An operator's Remove or DenyPin that the engine refuses for certain
// (refusingEngine, as the SDK's leader refuses one that may strand a voter,
// or that leaves no answering majority) left its revocation asked for
// (#165), and nothing withdrew it. When the member left later by a path
// that revokes nothing, the autopilot's removal of a dead member, which by
// design lets it come back, flushRevocations wrote the revocation and the
// member's JOIN was refused for good, while a member removed the same way
// without a refused request joined again (#171). The refused change now
// leaves no effect: its request is withdrawn.
func TestARefusedRemoveLeavesNoRevocationOnceTheMemberLeavesAnotherWay(t *testing.T) {
	for _, action := range []Action{ActionRemove, ActionDenyPin} {
		t.Run(string(action), func(t *testing.T) {
			mem, rs := NewForTesting(t.TempDir(), "node-a", "node-b", "node-c", "node-d", "node-e")
			ctx := context.Background()
			leader := rs[0]
			require.NoError(t, leader.RaisePayloadVersion(ctx, "op"))
			mem.SetLeader(leader.Engine().(*memraft.Node))
			idB, idC := slotID(t, leader, "node-b"), slotID(t, leader, "node-c")
			keyB, keyC := sha256.Sum256([]byte("node-b")), sha256.Sum256([]byte("node-c"))

			// 1. The operator's change of node-b, refused by the engine.
			real := leader.Engine()
			leader.setEngine(refusingEngine{real})
			err := leader.Operate(ctx, OperatorFor(1), Operation{Action: action, Target: idB.String()})
			leader.setEngine(real)
			require.ErrorIs(t, err, consensus.ErrMembershipInvalid)
			assert.Equal(t, 2, Rank(leader.Engine().Membership(), idB), "node-b is still a member")
			for _, r := range rs {
				assert.Empty(t, r.table.PendingRevocations(), "%s: the refused change left its request", r.Engine().Status().Slot)
			}

			// 2. Later node-b and node-c are out of reach past
			// DEAD_MEMBER_TIMEOUT: the autopilot removes them (autoChange,
			// then flushRevocations at the end of a later pass).
			require.True(t, leader.autoChange(ctx, ActionRemove, idB, "out of reach for DEAD_MEMBER_TIMEOUT", real.Remove))
			require.True(t, leader.autoChange(ctx, ActionRemove, idC, "out of reach for DEAD_MEMBER_TIMEOUT", real.Remove))
			require.NoError(t, leader.flushRevocations(ctx))

			// 3. Both come back, an operator grants their slots, they join
			// again.
			require.NoError(t, leader.grantJoin(ctx, OperatorFor(1), "node-b"))
			require.NoError(t, leader.grantJoin(ctx, OperatorFor(1), "node-c"))
			_, errC := leader.authorizeJoin(ctx, consensus.JoinRequest{Slot: "node-c", SPKI: keyC})
			_, errB := leader.authorizeJoin(ctx, consensus.JoinRequest{Slot: "node-b", SPKI: keyB})
			require.NoError(t, errC, "a dead member the autopilot removed joins again")
			assert.False(t, leader.table.KeyRevoked(hex.EncodeToString(keyB[:])), "a refused %s revoked nothing, then or later", action)
			require.NoError(t, errB, "node-b is treated as node-c: its only %s was refused", action)
		})
	}
}

// uncertainChangeEngine answers Remove and DenyPin with an unknown outcome, as the
// engine does when the change may still commit.
type uncertainChangeEngine struct{ consensus.Engine }

func (uncertainChangeEngine) Remove(context.Context, consensus.Operator, consensus.NodeID) error {
	return consensus.ErrOutcomeUnknown
}

func (uncertainChangeEngine) DenyPin(context.Context, consensus.Operator, consensus.NodeID) error {
	return consensus.ErrOutcomeUnknown
}

// A Remove whose outcome is unknown may have been made: its request stays,
// and is written once the member left, by any path (#64, #165). A second
// Remove of the same key, refused for certain, withdraws its own request
// only, never the first one: each is named by the index of its commit.
func TestARefusedRemoveWithdrawsItsOwnRequestOnly(t *testing.T) {
	mem, rs := NewForTesting(t.TempDir(), "node-a", "node-b", "node-c", "node-d", "node-e")
	ctx := context.Background()
	leader := rs[0]
	require.NoError(t, leader.RaisePayloadVersion(ctx, "op"))
	mem.SetLeader(leader.Engine().(*memraft.Node))
	idB := slotID(t, leader, "node-b")
	keyB := sha256.Sum256([]byte("node-b"))
	spki := hex.EncodeToString(keyB[:])
	real := leader.Engine()

	leader.setEngine(uncertainChangeEngine{real})
	err := leader.Operate(ctx, OperatorFor(1), Operation{Action: ActionRemove, Target: idB.String()})
	require.ErrorIs(t, err, consensus.ErrOutcomeUnknown)
	open := leader.table.RevocationRequests(spki)
	require.Len(t, open, 1, "the uncertain Remove's request stays")
	require.NotZero(t, open[0], "numbered by its commit")

	leader.setEngine(refusingEngine{real})
	err = leader.Operate(ctx, OperatorFor(2), Operation{Action: ActionRemove, Target: idB.String()})
	leader.setEngine(real)
	require.ErrorIs(t, err, consensus.ErrMembershipInvalid)
	for _, r := range rs {
		assert.Equal(t, open, r.table.RevocationRequests(spki), "the second request is withdrawn, the first stays")
		assert.Len(t, r.table.PendingRevocations(), 1)
	}

	// The member leaves another way: the request the uncertain Remove left
	// is written, and the key does not join again.
	require.True(t, leader.autoChange(ctx, ActionRemove, idB, "out of reach for DEAD_MEMBER_TIMEOUT", real.Remove))
	require.NoError(t, leader.flushRevocations(ctx))
	assert.True(t, leader.table.KeyRevoked(spki))
	require.NoError(t, leader.grantJoin(ctx, OperatorFor(1), "node-b"))
	_, err = leader.authorizeJoin(ctx, consensus.JoinRequest{Slot: "node-b", SPKI: keyB})
	require.ErrorContains(t, err, "revoked")
}

// A learner whose key is asked for revocation, by an operator's Remove or
// DenyPin whose outcome is unknown, is not promoted: the autopilot removes
// it, as a learner whose key is revoked (#70). Before, promotable read the
// revocation alone and made it a voter (#171).
func TestALearnerWhoseKeyIsAskedForRevocationIsNotPromoted(t *testing.T) {
	r, e, _ := newLeaderBench(t, 3)
	e.join(consensus.Member{ID: 0xb, Slot: "node-b", SPKI: [32]byte{0xb}, Voter: true})
	e.join(consensus.Member{ID: 0xc, Slot: "node-c", SPKI: [32]byte{0xc}})
	learner := e.Membership().Members[2]
	spki := hex.EncodeToString(learner.SPKI[:])
	data, err := reftable.Encode(reftable.KindAudit, &reftable.Audit{
		ID: "asked", Operator: "op", Action: reftable.AuditRevokeKey, Target: "node-c:" + spki, Outcome: reftable.AuditPending, Time: 1,
	})
	require.NoError(t, err)
	res := r.table.Apply(consensus.Entry{Term: 1, Index: 1, Kind: consensus.EntryCommand, Data: data, Token: consensus.Token{Cluster: consensus.ClusterID{0x71}, Epoch: 1, Term: 1, Index: 1}})
	require.True(t, res.(reftable.Result).OK)
	require.True(t, r.revocationAsked(spki))

	assert.False(t, r.promotable(t.Context(), e)(learner), "a learner whose key is asked for revocation")
	id, ok := r.revokedLearner(e.Membership())
	require.True(t, ok)
	assert.Equal(t, learner.ID, id)
	r.autopilotStep(t.Context())
	assert.Equal(t, []consensus.NodeID{learner.ID}, e.removes, "removed, as a learner whose key is revoked")
	assert.Empty(t, e.promotes, "never promoted")
}

// A mixed cluster: while one member's binary applies version 6 only, the
// raise to WithdrawVersion waits for it, and a refused Remove keeps its
// request as before, numbered 0 on every member alike: nothing is withdrawn,
// and nothing an older binary cannot apply is written. Once every member
// applies it and the version is raised, that request still stays, and a
// refused Remove of another member withdraws its own (#171).
func TestAWithdrawalWaitsForEveryMemberToApplyIt(t *testing.T) {
	m := consensus.Membership{Members: []consensus.Member{{ID: 1}, {ID: 2}, {ID: 3}}}
	mixed := []hello{{Node: consensus.NodeID(2).String(), Payload: reftable.PayloadVersion}, {Node: consensus.NodeID(3).String(), Payload: reftable.WithdrawVersion - 1}}
	assert.False(t, everyMemberApplies(m, 1, mixed, anyAnswer), "a member applying version 6 holds the raise")

	mem, rs := NewForTesting(t.TempDir(), "node-a", "node-b", "node-c", "node-d", "node-e")
	ctx := context.Background()
	leader := rs[0]
	mem.SetLeader(leader.Engine().(*memraft.Node))
	raise, err := reftable.Encode(reftable.KindAudit, &reftable.Audit{
		ID: "raise-6", Operator: "op", Action: reftable.AuditPayloadVersion, Target: strconv.Itoa(reftable.WithdrawVersion - 1), Outcome: reftable.AuditRaised,
	})
	require.NoError(t, err)
	_, res, err := leader.proposeCommand(ctx, raise, nil)
	require.NoError(t, err)
	require.True(t, res.OK)
	probe := leader.memberProbe
	older := true
	leader.memberProbe = func(ctx context.Context, addr, slot string) (hello, error) {
		h, err := probe(ctx, addr, slot)
		if slot == "node-e" && older {
			h.Payload = reftable.WithdrawVersion - 1
		}
		return h, err
	}
	err = leader.Operate(ctx, OperatorFor(1), Operation{Action: ActionRaisePayload, Target: strconv.Itoa(reftable.PayloadVersion)})
	require.ErrorContains(t, err, "not raised")
	require.Equal(t, uint8(reftable.WithdrawVersion-1), leader.table.WriteVersion())

	idB, idC := slotID(t, leader, "node-b"), slotID(t, leader, "node-c")
	keyB, keyC := sha256.Sum256([]byte("node-b")), sha256.Sum256([]byte("node-c"))
	real := leader.Engine()
	leader.setEngine(refusingEngine{real})
	require.ErrorIs(t, leader.Operate(ctx, OperatorFor(1), Operation{Action: ActionRemove, Target: idB.String()}), consensus.ErrMembershipInvalid)
	leader.setEngine(real)
	applied, _ := leader.table.Applied()
	for _, r := range rs {
		at, _ := r.table.Applied()
		assert.Equal(t, applied.Index, at.Index)
		assert.Equal(t, []uint64{0}, r.table.RevocationRequests(hex.EncodeToString(keyB[:])), "kept, as before, and numbered by none")
	}
	entries, err := real.(*memraft.Node).ReadCommitted(ctx, 1, applied.Index, 1<<20)
	require.NoError(t, err)
	for _, e := range entries {
		require.Less(t, e.Data[0], uint8(reftable.WithdrawVersion), "entry %d is written at a version node-e cannot apply", e.Index)
	}

	// node-e runs this binary now: the version is raised.
	older = false
	require.NoError(t, leader.Operate(ctx, OperatorFor(1), Operation{Action: ActionRaisePayload, Target: strconv.Itoa(reftable.PayloadVersion)}))
	require.Equal(t, uint8(reftable.PayloadVersion), leader.table.WriteVersion())
	leader.setEngine(refusingEngine{real})
	require.ErrorIs(t, leader.Operate(ctx, OperatorFor(1), Operation{Action: ActionRemove, Target: idC.String()}), consensus.ErrMembershipInvalid)
	leader.setEngine(real)
	for _, r := range rs {
		assert.Equal(t, []uint64{0}, r.table.RevocationRequests(hex.EncodeToString(keyB[:])), "the request of before the raise stays")
		assert.Empty(t, r.table.RevocationRequests(hex.EncodeToString(keyC[:])), "the one after it is withdrawn")
	}
}
