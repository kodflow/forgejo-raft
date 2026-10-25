// Copyright 2026 The Forgejo Authors. All rights reserved.
// SPDX-License-Identifier: GPL-3.0-or-later

package cluster

import (
	"context"
	"strings"
	"sync"
	"testing"
	"time"

	cluster_model "forgejo.org/models/cluster"
	"forgejo.org/services/cluster/consensus"
	"forgejo.org/services/cluster/reftable"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// moveOnCut moves the leadership once, while the coordinator takes the
// cluster's cut: after coordinator.run read its role, before take proposes
// its barrier, as a leadership transfer that completes during fenceAndCut
// does.
type moveOnCut struct {
	cutLock
	once sync.Once
	move func()
}

func (m *moveOnCut) TryExclusive(ctx context.Context) (func(), bool, error) {
	m.once.Do(m.move)
	return m.cutLock.TryExclusive(ctx)
}

// proposeHook is a node's engine that counts the entries the node proposes,
// by kind, and moves the leadership once, right before the node proposes an
// entry of kind moveAt: after the coordinator checked that it leads, as a
// transfer that completes between that check and the proposal does. The
// stand-in then appends the proposal in the new leader's term, as the
// engine does with a proposal a follower forwards to its leader.
type proposeHook struct {
	consensus.Engine
	moveAt reftable.Kind
	move   func()
	once   sync.Once

	mu       sync.Mutex
	proposed map[reftable.Kind]int
}

func (h *proposeHook) Propose(ctx context.Context, data []byte) (consensus.Token, any, error) {
	if len(data) > 1 {
		kind := reftable.Kind(data[1])
		h.mu.Lock()
		h.proposed[kind]++
		h.mu.Unlock()
		if kind == h.moveAt && h.move != nil {
			h.once.Do(h.move)
		}
	}
	return h.Engine.Propose(ctx, data)
}

func (h *proposeHook) count(kind reftable.Kind) int {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.proposed[kind]
}

// A checkpoint is a causal cut only because the consumer that derives the
// SQL is held at B by the coordinator's own node, the leader (checkpoint.go,
// step 3). A coordinator whose node lost the leadership after
// coordinator.run read its role proposed its barrier anyway: the engine
// forwarded it to the new leader, where it committed in the new leader's
// term, and so did the position, which applyCheckpointPosition accepted,
// both in one term. Meanwhile the new leader's consumer, which nothing
// held, derived the entries past B: the SQL at L held a push the refs at B
// did not, and the checkpoint was certified anyway (#218). The coordinator
// now closes its checkpoint only on a barrier and a position that commit
// in the term it led when it started, while it still leads that term: it
// gives the period up otherwise. The witness, a coordinator that leads
// from the start to the end, still closes its checkpoint, and its cut
// holds.
func TestADeposedCoordinatorClosesNoCheckpoint(t *testing.T) {
	zero := strings.Repeat("0", 40)
	repo := "alice/cut.git"
	key := repo + " refs/heads/main"

	for _, c := range []struct {
		name string
		// move is when the leadership moves from node-a, the
		// coordinator's, to node-b: never for the witness.
		move string
	}{
		{name: "the coordinator leads"},
		{name: "the coordinator lost the leadership before its barrier", move: "cut"},
		{name: "the leadership moved as the barrier was proposed", move: "barrier"},
		{name: "the coordinator lost the leadership while it read PostgreSQL's position", move: "mark"},
	} {
		t.Run(c.name, func(t *testing.T) {
			ctx := context.Background()
			tc := newTestCluster(t, "node-a", "node-b", "node-c")
			pg := newFakePG()
			coord := withCheckpoints(t, tc, pg)
			a, b := tc.nodes[0], tc.nodes[1]
			work := t.TempDir()
			run(t, work, "init", "-q")
			one := commit(t, work, "one")
			require.True(t, tc.pushVia(a, work, repo, "refs/heads/main", zero, one).OK)
			require.Eventually(t, func() bool { return pg.stateAt(1 << 62)[key] == one }, 10*time.Second, 10*time.Millisecond)
			two := commit(t, work, "two")

			moveToB := func() { tc.mem.SetLeader(b.node) }
			hook := &proposeHook{Engine: a.node, proposed: map[reftable.Kind]int{}}
			a.r.setEngine(hook)
			switch c.move {
			case "cut":
				a.r.cut = &moveOnCut{cutLock: pg.cut, move: moveToB}
			case "barrier":
				hook.moveAt, hook.move = reftable.KindCheckpointBarrier, moveToB
			}
			derived := false
			coord.wal = &markHook{fakePG: pg, during: func() {
				if c.move == "mark" {
					moveToB()
				}
				// A push lands after B, through node-b, while the
				// coordinator is about to read PostgreSQL's position.
				resp, err := tc.tryPushVia(b, work, repo, "refs/heads/main", one, two)
				require.NoError(t, err)
				require.True(t, resp.OK, resp.Message)
				for deadline := time.Now().Add(2 * time.Second); !derived && time.Now().Before(deadline); time.Sleep(10 * time.Millisecond) {
					derived = pg.stateAt(1 << 62)[key] == two
				}
			}}

			rec, err := coord.take(ctx)
			if err == nil {
				refs := refsAtBarrier(t, coord, rec)
				sql := pg.stateAt(parseFakeLSN(t, rec.LSN))
				t.Logf("checkpoint %s: barrier at %d in term %d, node-a leads: %v; SQL of the push after B derived before L: %v",
					rec.Name(), rec.Index, rec.Term, a.node.Status().Role == consensus.RoleLeader, derived)
				assert.Equal(t, refs, sql, "checkpoint %s: the refs at the barrier and the SQL at %s differ", rec.Name(), rec.LSN)
			}
			if c.move == "" {
				require.NoError(t, err, "a coordinator that leads from the start to the end closes its checkpoint")
				assert.False(t, derived, "the SQL of the push after B waits for PostgreSQL's position")
				assert.Len(t, a.r.table.Checkpoints(), 1)
				return
			}
			require.Error(t, err, "a coordinator that lost the leadership closes no checkpoint")
			t.Logf("given up: %v", err)
			require.ErrorContains(t, err, "term")
			for _, n := range tc.nodes {
				assert.Empty(t, n.r.table.Checkpoints(), "%s holds a checkpoint closed by a coordinator that lost the leadership", n.slot)
			}
			assert.Zero(t, hook.count(reftable.KindCheckpointPosition), "no position is proposed once the coordinator no longer leads its term")
			bk, _ := tc.cursors.get(ctx, cluster_model.CursorBackup)
			assert.Empty(t, bk.Lineage, "no backup cursor is recorded for a checkpoint given up")
		})
	}
}

// On the SDK's engine, which forwards a follower's proposal to its leader,
// a leadership transfer completes while the coordinator takes its cut. Its
// barrier used to be forwarded to the new leader and to commit in its
// term, and the position after it: the checkpoint closed on a cut no
// consumer held (#218). The deposed coordinator now gives it up, and the
// new leader's coordinator closes one, in its own term.
func TestADeposedCoordinatorClosesNoCheckpointOnTheSDKEngine(t *testing.T) {
	ctx := context.Background()
	tc := newTestClusterOn(t, true, "node-a", "node-b", "node-c")
	pg := newFakePG()
	for _, n := range tc.nodes {
		n.r.consumer.work = pg.work
		n.r.cut = pg.cut
	}
	leads := func(n *testNode) bool { return n.r.Engine().Status().Role == consensus.RoleLeader }
	var from, to *testNode
	require.Eventually(t, func() bool {
		for _, n := range tc.nodes {
			if leads(n) {
				from = n
				return true
			}
		}
		return false
	}, 10*time.Second, 10*time.Millisecond)
	for _, n := range tc.nodes {
		if n != from {
			to = n
			break
		}
	}
	coordOn := func(n *testNode) *coordinator {
		c := newCoordinator(n.r, time.Hour, t.TempDir(), pg)
		c.files = func() (*storedListing, error) { return nil, nil }
		c.backup = tc.cursors.set
		return c
	}
	coord := coordOn(from)
	from.r.cut = &moveOnCut{cutLock: pg.cut, move: func() {
		require.NoError(t, from.r.Engine().TransferLeadership(ctx, consensus.Operator("test"), to.r.Engine().Status().ID))
		require.Eventually(t, func() bool { return leads(to) && !leads(from) }, 10*time.Second, 10*time.Millisecond,
			"the leadership moves from %s to %s", from.slot, to.slot)
	}}
	_, err := coord.take(ctx)
	require.Error(t, err, "a coordinator that lost the leadership closes no checkpoint")
	t.Logf("given up: %v", err)
	require.ErrorContains(t, err, "term")
	for _, n := range tc.nodes {
		_, err := n.r.confirm(ctx)
		require.NoError(t, err)
		assert.Empty(t, n.r.table.Checkpoints(), "%s holds a checkpoint closed by a coordinator that lost the leadership", n.slot)
	}

	rec, err := coordOn(to).take(ctx)
	require.NoError(t, err, "the new leader's coordinator closes its checkpoint")
	assert.Equal(t, to.r.Engine().Status().Term, rec.Term, "in the new leader's term")
}
