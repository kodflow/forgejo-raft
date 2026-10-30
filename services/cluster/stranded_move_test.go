// Copyright 2026 The Forgejo Authors. All rights reserved.
// SPDX-License-Identifier: GPL-3.0-or-later

package cluster

import (
	"context"
	"testing"

	"forgejo.org/services/cluster/consensus"
	"forgejo.org/services/cluster/consensus/memraft"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// The engine's leader refuses MEMBERSHIP_INVALID the move of a voter that
// answers when a voter that may know it only at its old address could be
// left with no one to vouch for the leader (kitsunium/sdk#761). The
// operator reads which voter to bring back; once it is back, the move goes
// through. The leader's own move is never refused on this ground.
func TestMoveThatMayStrandASilentVoterIsRefusedReadably(t *testing.T) {
	t.Parallel()
	mem, rs := NewForTesting(t.TempDir(), "node-a", "node-b", "node-c", "node-d")
	ctx := context.Background()
	leader := rs[3]
	mem.SetLeader(leader.Engine().(*memraft.Node))
	mem.SetDown(rs[0].Engine().(*memraft.Node), true)
	idA, idB, idD := slotID(t, leader, "node-a"), slotID(t, leader, "node-b"), slotID(t, leader, "node-d")
	move := func(id consensus.NodeID, addr string) error {
		return leader.Operate(ctx, OperatorFor(1), Operation{Action: ActionUpdateAddress, Target: id.String(), Address: addr})
	}
	address := func(id consensus.NodeID) string {
		m := leader.Engine().Membership()
		return m.Members[Rank(m, id)-1].Address
	}
	before := address(idB)

	err := move(idB, "10.2.0.2:7480")
	require.ErrorIs(t, err, consensus.ErrMembershipInvalid)
	require.ErrorContains(t, err, "bring "+idA.String()+" back")
	assert.Equal(t, before, address(idB), "a refused move changes nothing")

	require.NoError(t, move(idD, "10.2.0.4:7480"), "the leader's own move")
	assert.Equal(t, "10.2.0.4:7480", address(idD))

	mem.SetDown(rs[0].Engine().(*memraft.Node), false)
	require.NoError(t, move(idB, "10.2.0.2:7480"))
	assert.Equal(t, "10.2.0.2:7480", address(idB))
}

// The engine's own refusal of a move carries no cause: the operator reads
// the ones it has.
func TestMoveRefusedByTheEngineSaysWhy(t *testing.T) {
	t.Parallel()
	mem, rs := NewForTesting(t.TempDir(), "node-a", "node-b", "node-c")
	leader := rs[0]
	mem.SetLeader(leader.Engine().(*memraft.Node))
	err := leader.Operate(context.Background(), OperatorFor(1), Operation{Action: ActionUpdateAddress, Target: consensus.NodeID(0xdead).String(), Address: "10.2.0.9:7480"})
	require.ErrorIs(t, err, consensus.ErrMembershipInvalid)
	require.ErrorContains(t, err, "bring that voter back")
}
