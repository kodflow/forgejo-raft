// Copyright 2026 The Forgejo Authors. All rights reserved.
// SPDX-License-Identifier: GPL-3.0-or-later

package cluster

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"sync"
	"testing"
	"time"

	"forgejo.org/services/cluster/consensus"
	"forgejo.org/services/cluster/reftable"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// behindView is a follower that has not appended the configuration that
// added member missing yet: its Membership, the configuration it last
// appended (what the SDK hands as the node's membership), omits it. With
// catchUp, it appends it once it applied a commit boundary, as a follower
// catching up with the leader's commit does; without, it stays behind
// through the boundary, as for an addition not committed when the boundary
// was taken. Every change is forwarded to the leader, which decides on its
// own configuration: the in-memory cluster's.
type behindView struct {
	consensus.Engine
	missing consensus.NodeID
	catchUp bool

	mu       sync.Mutex
	caughtUp bool
}

func (e *behindView) Barrier(ctx context.Context) (consensus.Token, error) {
	tok, err := e.Engine.Barrier(ctx)
	if err == nil && e.catchUp {
		e.mu.Lock()
		e.caughtUp = true
		e.mu.Unlock()
	}
	return tok, err
}

func (e *behindView) Membership() consensus.Membership {
	m := e.Engine.Membership()
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.caughtUp {
		return m
	}
	all := m.Members
	m.Members = nil
	for _, x := range all {
		if x.ID != e.missing {
			m.Members = append(m.Members, x)
		}
	}
	return m
}

// An operator's Remove or Deny pin served by a node whose membership does
// not list the target yet (a follower catching up after a restart or a
// snapshot, the confirmation routed there by the load balancer) was
// forwarded and made by the leader, while revokeMember found no member to
// read a key from: the operator was told "done", the audit said ok, no
// revocation was asked nor written, and the key joined again on the CA
// under a new id (#337). The node now applies a confirmed commit boundary
// first, which lists every member whose addition committed before the
// request, and refuses MEMBERSHIP_INVALID, changing nothing, a target it
// still does not list then. The same change from a node that lists the
// target asks and writes the revocation, as before.
func TestRemoveOrDenyPinFromANodeBehindTheMembership(t *testing.T) {
	for _, action := range []Action{ActionRemove, ActionDenyPin} {
		for _, catchUp := range []bool{true, false} {
			name := string(action) + "/behind-for-good"
			if catchUp {
				name = string(action) + "/caught-up-at-the-boundary"
			}
			t.Run(name, func(t *testing.T) {
				mem, rs := NewForTesting(t.TempDir(), "node-a", "node-c")
				a, c := rs[0], rs[1]
				ctx := context.Background()
				keyB, keyD := sha256.Sum256([]byte("node-b")), sha256.Sum256([]byte("node-d"))
				mem.Add("node-d", keyD, reftable.New())
				mem.Add("node-e", sha256.Sum256([]byte("node-e")), reftable.New())
				// node-b joined last: node-c has not appended its addition.
				mem.Add("node-b", keyB, reftable.New())
				idB, idD := slotID(t, a, "node-b"), slotID(t, a, "node-d")
				require.NoError(t, a.RaisePayloadVersion(ctx, "op"), "every member applies RevokeVersion and WithdrawVersion")
				require.GreaterOrEqual(t, int(a.table.WriteVersion()), int(reftable.WithdrawVersion))
				a.auto = &autoMode{expected: 5, self: "node-a", peers: map[string]string{
					"node-a": "a:7480", "node-b": "b:7480", "node-c": "c:7480", "node-d": "d:7480", "node-e": "e:7480",
				}}
				hexB := hex.EncodeToString(keyB[:])

				// Control: node-c lists node-d, the change asks and writes
				// its revocation, and node-d's key is refused at the JOIN.
				require.NoError(t, c.Operate(ctx, OperatorFor(1), Operation{Action: action, Target: idD.String()}))
				_, err := a.authorizeJoin(ctx, consensus.JoinRequest{Slot: "node-d", SPKI: keyD})
				require.ErrorContains(t, err, "revoked", "control: a %s from a node that lists the member revokes its key", action)

				real := c.Engine()
				c.setEngine(&behindView{Engine: real, missing: idB, catchUp: catchUp})
				err = c.Operate(ctx, OperatorFor(1), Operation{Action: action, Target: idB.String()})
				t.Logf("%s of %s (node-b) submitted on node-c behind the membership: err=%v", action, idB, err)
				if !catchUp {
					require.ErrorIs(t, err, consensus.ErrMembershipInvalid, "a %s whose key this node cannot read is never made", action)
					require.ErrorContains(t, err, "not a member on this node")
					assert.Positive(t, Rank(a.Engine().Membership(), idB), "node-b is still a member")
					assert.NotEqual(t, [32]byte{}, a.Engine().Membership().Members[Rank(a.Engine().Membership(), idB)-1].SPKI, "node-b keeps its pin")
					assert.Empty(t, a.table.RevocationRequests(hexB), "nothing is asked")
					assert.Equal(t, "failed", a.table.Audit()[0].Outcome, "the audit says the %s failed", action)
					// node-c catches up: the operator's retry is made.
					c.setEngine(real)
					require.NoError(t, c.Operate(ctx, OperatorFor(1), Operation{Action: action, Target: idB.String()}))
				} else {
					require.NoError(t, err, "the node lists node-b once it applied the boundary")
				}
				c.setEngine(real)
				if action == ActionRemove {
					require.Zero(t, Rank(a.Engine().Membership(), idB), "the leader removed node-b")
					require.Contains(t, a.Engine().Membership().Retired, idB)
				} else {
					require.Equal(t, [32]byte{}, a.Engine().Membership().Members[Rank(a.Engine().Membership(), idB)-1].SPKI, "the leader denied node-b's pin")
				}

				t.Logf("node-b's key after its %s: revoked=%v asked=%v requests=%v pending=%d",
					action, a.table.KeyRevoked(hexB), a.revocationAsked(hexB), a.table.RevocationRequests(hexB), len(a.table.PendingRevocations()))
				require.NoError(t, a.flushRevocations(ctx))
				assert.True(t, a.table.KeyRevoked(hexB), "node-b's key is revoked once the %s is made", action)
				_, err = a.authorizeJoin(ctx, consensus.JoinRequest{Slot: "node-b", SPKI: keyB})
				require.ErrorContains(t, err, "revoked", "node-b's key, after an operator's %s, joins again on the CA under a new id", action)
			})
		}
	}
}

// revokeMember refuses an id the node's membership does not list, whoever
// asks: no key is read, so nothing is asked and the change is not made
// (#337). The autopilot, which removes from the leader a member of the
// leader's own membership, still removes a dead member and marks its key
// (#209), and a member whose pin was denied, which holds no key any more,
// is still removed.
func TestRevokeMemberRefusesAnIDTheMembershipDoesNotList(t *testing.T) {
	mem, rs := NewForTesting(t.TempDir(), "node-a")
	r := rs[0]
	ctx := context.Background()
	keyC, keyD := sha256.Sum256([]byte("node-c")), sha256.Sum256([]byte("node-d"))
	mem.Add("node-b", sha256.Sum256([]byte("node-b")), reftable.New())
	mem.Add("node-c", keyC, reftable.New())
	mem.Add("node-d", keyD, reftable.New())
	mem.Add("node-e", sha256.Sum256([]byte("node-e")), reftable.New())
	mem.Add("node-f", sha256.Sum256([]byte("node-f")), reftable.New())
	idB, idC, idD := slotID(t, r, "node-b"), slotID(t, r, "node-c"), slotID(t, r, "node-d")
	require.NoError(t, r.RaisePayloadVersion(ctx, "op"))
	absent := consensus.NodeID(0xabcdef)
	requests := func() int {
		n := 0
		for _, x := range r.table.Audit() {
			if x.Action == reftable.AuditRevokeKey {
				n++
			}
		}
		return n
	}

	// An id that was never a member.
	_, asked, _, err := r.revokeMember(ctx, r.Engine(), OperatorFor(1), absent)
	require.ErrorIs(t, err, consensus.ErrMembershipInvalid)
	require.ErrorContains(t, err, "not a member on this node")
	assert.False(t, asked)
	require.ErrorIs(t, r.Operate(ctx, OperatorFor(1), Operation{Action: ActionDenyPin, Target: absent.String()}), consensus.ErrMembershipInvalid)
	assert.False(t, r.removeDead(ctx, r.Engine(), absent), "the autopilot removes no id its leader does not list")
	assert.Zero(t, requests(), "no request for an id without a key")

	// A removed one: the operator is told so, and nothing is asked again.
	require.NoError(t, r.Operate(ctx, OperatorFor(1), Operation{Action: ActionRemove, Target: idB.String()}))
	before := requests()
	err = r.Operate(ctx, OperatorFor(1), Operation{Action: ActionRemove, Target: idB.String()})
	require.ErrorIs(t, err, consensus.ErrMembershipInvalid)
	require.ErrorContains(t, err, "removed already")
	assert.Equal(t, before, requests())

	// The autopilot still removes a dead member of its membership, and
	// marks its key.
	require.True(t, r.removeDead(ctx, r.Engine(), idC))
	assert.Zero(t, Rank(r.Engine().Membership(), idC))
	assert.True(t, r.table.KeyRevoked(hex.EncodeToString(keyC[:])), "the dead member's key is marked")
	_, err = r.authorizeJoin(ctx, consensus.JoinRequest{Slot: "node-c", SPKI: keyC})
	require.ErrorContains(t, err, "revoked")

	// A member whose pin was denied holds no key any more: its Remove is
	// made, and asks nothing.
	require.NoError(t, r.Operate(ctx, OperatorFor(1), Operation{Action: ActionDenyPin, Target: idD.String()}))
	assert.True(t, r.table.KeyRevoked(hex.EncodeToString(keyD[:])))
	before = requests()
	require.NoError(t, r.Operate(ctx, OperatorFor(1), Operation{Action: ActionRemove, Target: idD.String()}))
	assert.Zero(t, Rank(r.Engine().Membership(), idD))
	assert.Equal(t, before, requests())
}

// The same Remove on the SDK's real engine: a learner joins, and an
// operator removes it from a follower whose view (behindView) does not list
// it until it applied a commit boundary. The follower's boundary is the
// engine's ReadIndex through the leader, its Remove is forwarded to the
// leader, and the key is revoked and refused at the JOIN (#337).
func TestRemoveOfALearnerFromAFollowerBehindTheMembershipOnTheSDKEngine(t *testing.T) {
	b := newJoinBench(t, "node-d")
	ctx := context.Background()
	require.NoError(t, b.founders[0].RaisePayloadVersion(ctx, "op"))
	require.Eventually(t, func() bool {
		for _, r := range b.founders {
			if r.table.WriteVersion() < reftable.WithdrawVersion {
				return false
			}
		}
		return true
	}, 5*time.Second, 10*time.Millisecond)
	b.grant(t, "node-d")
	require.Eventually(t, func() bool { return b.join() == nil }, 5*time.Second, 50*time.Millisecond)
	require.Eventually(t, func() bool { return b.learner("node-d") }, 5*time.Second, 10*time.Millisecond, "admitted as a learner")

	var leader, follower *Replicator
	require.Eventually(t, func() bool {
		leader, follower = nil, nil
		for _, r := range b.founders {
			if r.Engine().Status().Role == consensus.RoleLeader {
				leader = r
			} else if follower == nil {
				follower = r
			}
		}
		return leader != nil && follower != nil
	}, 5*time.Second, 10*time.Millisecond)
	var learner consensus.Member
	for _, m := range leader.Engine().Membership().Members {
		if m.Slot == "node-d" {
			learner = m
		}
	}
	require.NotZero(t, learner.ID)
	require.Eventually(t, func() bool { return Rank(follower.Engine().Membership(), learner.ID) > 0 }, 5*time.Second, 10*time.Millisecond)

	real := follower.Engine()
	follower.setEngine(&behindView{Engine: real, missing: learner.ID, catchUp: true})
	err := follower.Operate(ctx, OperatorFor(1), Operation{Action: ActionRemove, Target: learner.ID.String()})
	follower.setEngine(real)
	require.NoError(t, err)
	spki := hex.EncodeToString(learner.SPKI[:])
	require.Eventually(t, func() bool {
		return Rank(leader.Engine().Membership(), learner.ID) == 0 && leader.table.KeyRevoked(spki)
	}, 5*time.Second, 10*time.Millisecond, "the leader removed node-d, and its key is revoked")
	_, err = leader.authorizeJoin(ctx, consensus.JoinRequest{Slot: "node-d", SPKI: learner.SPKI})
	require.ErrorContains(t, err, "revoked")
}
