// Copyright 2026 The Forgejo Authors. All rights reserved.
// SPDX-License-Identifier: GPL-3.0-or-later

package cluster

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"io"
	"path/filepath"
	"testing"
	"time"

	"forgejo.org/services/cluster/consensus"
	"forgejo.org/services/cluster/consensus/memraft"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// The engine's leader refuses MEMBERSHIP_INVALID the removal of a member
// that answers when a voter of the new configuration might know neither
// the leader nor a voter that stays (kitsunium/sdk#603). strandedBy sees
// it first, so the key of a member the engine keeps is never revoked (#64).
func TestStrandedBy(t *testing.T) {
	t.Parallel()
	m := consensus.Membership{At: consensus.Position{Index: 40}, Members: []consensus.Member{
		{ID: 1, Voter: true}, {ID: 2, Voter: true}, {ID: 3, Voter: true}, {ID: 4, Voter: true}, {ID: 5},
	}}
	up := func(id consensus.NodeID) consensus.PeerHealth {
		return consensus.PeerHealth{ID: id, Match: 50, LastContact: 100 * time.Millisecond}
	}
	down := func(id consensus.NodeID, match uint64) consensus.PeerHealth {
		return consensus.PeerHealth{ID: id, Match: match, LastContact: time.Minute, Unreachable: true}
	}
	cases := []struct {
		name           string
		leader, target consensus.NodeID
		peers          []consensus.PeerHealth
		want           consensus.NodeID
	}{
		{"every voter answers", 1, 2, []consensus.PeerHealth{up(2), up(3), up(4), up(5)}, 0},
		{"not the leader: the engine decides", 1, 2, nil, 0},
		{"a silent voter caught up, listed after the leader", 1, 2, []consensus.PeerHealth{up(2), down(3, 45), up(4), up(5)}, 0},
		{"a silent voter behind the membership", 1, 2, []consensus.PeerHealth{up(2), down(3, 30), up(4), up(5)}, 3},
		{"a silent voter listed before the leader", 4, 2, []consensus.PeerHealth{down(1, 45), up(2), up(3), up(5)}, 1},
		{"an answering voter behind the membership", 1, 2, []consensus.PeerHealth{up(2), {ID: 3, Match: 39}, up(4), up(5)}, 3},
		{"a silent target is always removable", 4, 2, []consensus.PeerHealth{down(1, 30), down(2, 30), up(3), up(5)}, 0},
		{"a target heard too long ago is silent", 4, 2, []consensus.PeerHealth{down(1, 30), {ID: 2, Match: 50, LastContact: 2 * time.Second}, up(3), up(5)}, 0},
		{"a voter the leader has no word of", 1, 2, []consensus.PeerHealth{up(2), up(4), up(5)}, 3},
		{"a silent learner strands no one", 1, 2, []consensus.PeerHealth{up(2), up(3), up(4), down(5, 1)}, 0},
	}
	for _, c := range cases {
		got, ok := strandedBy(m, consensus.Health{Peers: c.peers}, c.leader, c.target)
		assert.Equal(t, c.want, got, c.name)
		assert.Equal(t, c.want != 0, ok, c.name)
	}
}

// The operator's removal of a voter that answers, while a voter listed
// before the leader is silent, is refused before the key is revoked; once
// the silent voter is back, it goes through (kitsunium/sdk#603, #64).
func TestRemoveThatMayStrandASilentVoterIsRefusedBeforeTheRevocation(t *testing.T) {
	t.Parallel()
	mem, rs := NewForTesting(t.TempDir(), "node-a", "node-b", "node-c", "node-d")
	ctx := context.Background()
	require.NoError(t, rs[0].RaisePayloadVersion(ctx, "op"))
	leader := rs[3]
	mem.SetLeader(leader.Engine().(*memraft.Node))
	mem.SetDown(rs[0].Engine().(*memraft.Node), true)
	keyB := sha256.Sum256([]byte("node-b"))
	idB := slotID(t, leader, "node-b")
	err := leader.Operate(ctx, OperatorFor(1), Operation{Action: ActionRemove, Target: idB.String()})
	require.ErrorIs(t, err, consensus.ErrMembershipInvalid)
	require.ErrorContains(t, err, "bring "+slotID(t, leader, "node-a").String()+" back")
	assert.Positive(t, Rank(leader.Engine().Membership(), idB), "node-b is still a member")
	assert.False(t, leader.table.KeyRevoked(hex.EncodeToString(keyB[:])), "the key of a member the engine keeps")
	assert.Empty(t, leader.table.PendingRevocations())

	mem.SetDown(rs[0].Engine().(*memraft.Node), false)
	require.NoError(t, leader.Operate(ctx, OperatorFor(1), Operation{Action: ActionRemove, Target: idB.String()}))
	assert.Zero(t, Rank(leader.Engine().Membership(), idB))
	assert.True(t, leader.table.KeyRevoked(hex.EncodeToString(keyB[:])))
}

// A silent voter is removed while another is silent too: the engine never
// refuses the removal of a member that does not answer, and neither does
// the operator's check, so the autopilot's and the operator's removal of a
// dead voter stay possible (kitsunium/sdk#603).
func TestRemoveOfASilentVoterIsNotRefusedForAnotherSilentOne(t *testing.T) {
	t.Parallel()
	mem, rs := NewForTesting(t.TempDir(), "node-a", "node-b", "node-c", "node-d", "node-e")
	ctx := context.Background()
	require.NoError(t, rs[0].RaisePayloadVersion(ctx, "op"))
	leader := rs[4]
	mem.SetLeader(leader.Engine().(*memraft.Node))
	mem.SetDown(rs[0].Engine().(*memraft.Node), true)
	mem.SetDown(rs[1].Engine().(*memraft.Node), true)
	idB := slotID(t, leader, "node-b")
	require.NoError(t, leader.Operate(ctx, OperatorFor(1), Operation{Action: ActionRemove, Target: idB.String()}))
	assert.Zero(t, Rank(leader.Engine().Membership(), idB))
}

// The removal of a learner that answers is not held while a voter listed
// before the leader is silent: a learner vouches for no voter, and the
// engine's guard admits it as well since kitsunium/sdk#614. The removal of
// a voter that answers there stays refused (kitsunium/sdk#603).
func TestRemovableAdmitsAnAnsweringLearnerWhileAVoterIsStranded(t *testing.T) {
	t.Parallel()
	mem, rs := NewForTesting(t.TempDir(), "node-a", "node-b", "node-c", "node-d", "node-e")
	leader := rs[3]
	mem.SetLeader(leader.Engine().(*memraft.Node))
	mem.SetDown(rs[0].Engine().(*memraft.Node), true)
	idB, idE := slotID(t, leader, "node-b"), slotID(t, leader, "node-e")
	eng := learnerView{Engine: leader.Engine(), learner: idE}
	require.NoError(t, leader.removable(eng, idE))
	require.ErrorIs(t, leader.removable(eng, idB), consensus.ErrMembershipInvalid)
}

// learnerView reports one member as a learner in the engine's membership.
type learnerView struct {
	consensus.Engine
	learner consensus.NodeID
}

func (l learnerView) Membership() consensus.Membership {
	m := l.Engine.Membership()
	members := make([]consensus.Member, len(m.Members))
	copy(members, m.Members)
	for i := range members {
		if members[i].ID == l.learner {
			members[i].Voter = false
		}
	}
	m.Members = members
	return m
}

// SaveSnapshot of the engine returns at its context's end while a write to
// the writer already blocked goes on, on a goroutine of the engine's
// (kitsunium/sdk#601). The file it wrote into is closed and removed: the
// late write fails on the closed file, and nothing is left at the path.
func TestSaveSnapshotOutlivedByALateWrite(t *testing.T) {
	t.Parallel()
	_, rs := NewForTesting(t.TempDir(), "node-a")
	r := rs[0]
	wrote := make(chan error, 1)
	release := make(chan struct{})
	r.setEngine(lateSaver{Engine: r.Engine(), release: release, wrote: wrote})
	path := filepath.Join(t.TempDir(), "refs.snap")
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := r.SaveSnapshot(ctx, "op", path)
	require.ErrorIs(t, err, context.Canceled)
	close(release)
	require.Error(t, <-wrote, "the late write reached a closed file")
	for _, p := range []string{path, path + ".partial", path + ".sha256", path + ".meta"} {
		assert.NoFileExists(t, p)
	}
}

// lateSaver answers SaveSnapshot at its context's end, and writes to w
// once released after the caller's SaveSnapshot returned, as the engine's
// copy goroutine may.
type lateSaver struct {
	consensus.Engine
	release <-chan struct{}
	wrote   chan<- error
}

func (l lateSaver) SaveSnapshot(ctx context.Context, _ consensus.Operator, w io.Writer) (consensus.SnapshotMeta, error) {
	go func() {
		<-l.release
		_, err := w.Write([]byte("late"))
		l.wrote <- err
	}()
	<-ctx.Done()
	return consensus.SnapshotMeta{}, ctx.Err()
}
