// Copyright 2026 The Forgejo Authors. All rights reserved.
// SPDX-License-Identifier: GPL-3.0-or-later

package cluster

import (
	"bytes"
	"context"
	"strings"
	"sync"
	"testing"
	"time"

	cluster_model "forgejo.org/models/cluster"
	"forgejo.org/modules/json"
	"forgejo.org/services/cluster/consensus"
	"forgejo.org/services/cluster/reftable"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// A checkpoint says whether every event the table holds of an earlier
// lineage was behind that lineage's cursor: dispatched, or parked with its
// row. The current lineage's events are drained's.
func TestEarlierLineagesDrained(t *testing.T) {
	store, lg, parked := newSQLStore(), &memLog{}, &memParked{}
	for i := uint64(1); i <= 10; i++ {
		lg.events = append(lg.events, reftable.Event{Token: oldToken(i), Kind: reftable.EventRef, Repo: "o/old.git"})
	}
	for i := uint64(11); i <= 20; i++ {
		lg.events = append(lg.events, reftable.Event{Token: newToken(i), Kind: reftable.EventRef, Repo: "o/new.git"})
	}
	c := unitConsumer(store, lg, parked, nil)
	ctx := context.Background()
	current := newToken(1).Identity()
	old := oldToken(1).Identity().String()

	assert.False(t, c.earlierDrained(ctx, current), "no cursor of the old lineage")
	store.primary[cursorName(old)] = cluster_model.Cursor{Lineage: old, Position: 9}
	assert.False(t, c.earlierDrained(ctx, current), "its last event waits for its work")
	store.primary[cursorName(old)] = cluster_model.Cursor{Lineage: "other", Position: 10}
	assert.False(t, c.earlierDrained(ctx, current), "a cursor of another lineage under its name")
	store.primary[cursorName(old)] = cluster_model.Cursor{Lineage: old, Position: 10}
	assert.True(t, c.earlierDrained(ctx, current), "every event of it dispatched; the current lineage's are drained's")

	lg.events = lg.events[10:]
	assert.True(t, c.earlierDrained(ctx, current), "no earlier lineage")
}

// withEarlierEvents puts events of an earlier lineage at the head of a
// table's queue, as the table a Recover starts from holds them: through
// its snapshot, the field Restore reads.
func withEarlierEvents(t *testing.T, tbl *reftable.Table, evs ...reftable.Event) {
	t.Helper()
	snap, err := tbl.Snapshot()
	require.NoError(t, err)
	var buf bytes.Buffer
	_, err = snap.WriteTo(&buf)
	require.NoError(t, err)
	snap.Release()
	b, err := json.Marshal(evs)
	require.NoError(t, err)
	head := strings.TrimSuffix(string(b), "]")
	s := buf.String()
	switch {
	case strings.Contains(s, `"events":null`):
		s = strings.Replace(s, `"events":null`, `"events":`+head+"]", 1)
	case strings.Contains(s, `"events":[]`):
		s = strings.Replace(s, `"events":[]`, `"events":`+head+"]", 1)
	default:
		require.Contains(t, s, `"events":[`)
		s = strings.Replace(s, `"events":[`, `"events":`+head+",", 1)
	}
	require.NoError(t, tbl.Restore(strings.NewReader(s), consensus.SnapshotMeta{}))
}

// endedCluster is a cluster right after a Recover: every member's table
// holds an event of the old lineage, whose work waits until released.
type endedCluster struct {
	tc      *testCluster
	pg      *fakePG
	coord   *coordinator
	leader  *testNode
	oldID   consensus.LogIdentity
	release chan struct{}
}

func newEndedCluster(t *testing.T) *endedCluster {
	tc := newTestCluster(t, "node-a", "node-b", "node-c")
	pg := newFakePG()
	e := &endedCluster{
		tc: tc, pg: pg, coord: withCheckpoints(t, tc, pg), leader: tc.nodes[0],
		oldID:   consensus.LogIdentity{Cluster: consensus.ClusterID{0xde, 0xad}, Epoch: 1},
		release: make(chan struct{}),
	}
	for _, n := range tc.nodes {
		n.r.consumer.work = func(ctx context.Context, evs []reftable.Event) error {
			if evs[0].Repo == "o/old.git" {
				select {
				case <-e.release:
				case <-ctx.Done():
					return ctx.Err()
				}
			}
			return pg.work(ctx, evs)
		}
	}
	t.Cleanup(e.dispatchOld)
	ev := reftable.Event{
		Token: consensus.Token{Cluster: e.oldID.Cluster, Epoch: e.oldID.Epoch, Term: 1, Index: 1},
		Kind:  reftable.EventRef, Repo: "o/old.git", Ref: "refs/heads/main", PusherID: 1,
		Old: strings.Repeat("1", 40), New: strings.Repeat("2", 40),
	}
	for _, n := range tc.nodes {
		withEarlierEvents(t, n.r.table, ev)
	}
	require.True(t, e.holds(e.leader))
	require.NotEmpty(t, e.leader.r.table.PinnedObjects("o/old.git"))
	e.leader.r.signalConsumer()
	return e
}

// holds reports whether a member's table still holds the old lineage's
// event.
func (e *endedCluster) holds(n *testNode) bool {
	for _, ev := range n.r.table.Events() {
		if ev.Token.Identity() == e.oldID {
			return true
		}
	}
	return false
}

// dispatchOld lets the old lineage's event's work run.
func (e *endedCluster) dispatchOld() {
	select {
	case <-e.release:
	default:
		close(e.release)
	}
}

// drainOld lets the old lineage's event's work run, and waits for its
// cursor to reach the lineage's end.
func (e *endedCluster) drainOld(t *testing.T) {
	e.dispatchOld()
	old := e.oldID.String()
	require.Eventually(t, func() bool {
		cur, _ := e.tc.cursors.get(context.Background(), cursorName(old))
		return cur.Lineage == old && cur.Position == 1
	}, 10*time.Second, 20*time.Millisecond, "the old lineage's event is dispatched")
}

// backup is the backup cursor, checked to be of the current lineage.
func (e *endedCluster) backup(t *testing.T) *cluster_model.Cursor {
	_, current := e.leader.r.table.Applied()
	require.NotEqual(t, e.oldID, current)
	b, _ := e.tc.cursors.get(context.Background(), cluster_model.CursorBackup)
	require.Equal(t, current.String(), b.Lineage)
	return b
}

// releasedBy takes a checkpoint that reads the old lineage drained: every
// member trims it to its end, by the existing Ack, and its objects are no
// longer pinned against gc.
func (e *endedCluster) releasedBy(t *testing.T) {
	_, err := e.coord.take(context.Background())
	require.NoError(t, err)
	assert.True(t, e.backup(t).Closed, "the checkpoint read the old lineage drained")
	for _, n := range e.tc.nodes {
		require.Eventually(t, func() bool { return !e.holds(n) }, 10*time.Second, 20*time.Millisecond,
			"%s trims the ended lineage to its end", n.slot)
		assert.Empty(t, n.r.table.PinnedObjects("o/old.git"), "%s: git gc may run on the repository again", n.slot)
	}
}

// After a Recover, a checkpoint of the new lineage taken while the old
// lineage's tail still waits for its work holds that lineage's cursor
// behind its end: a PostgreSQL restored to it replays the tail from the
// log, so the tail is not trimmed to its end on that backup. The next
// checkpoint reads the old lineage drained, says so with the backup
// cursor, and the tail is trimmed to its end, by the existing Ack, on
// every member: its objects are no longer pinned against gc (#205).
func TestCheckpointReleasesTheEndedLineageOnceItReadItDrained(t *testing.T) {
	e := newEndedCluster(t)
	_, err := e.coord.take(context.Background())
	require.NoError(t, err)
	assert.False(t, e.backup(t).Closed, "the checkpoint read the old lineage behind its end")

	e.drainOld(t)
	assert.Never(t, func() bool { return !e.holds(e.leader) }, 2500*time.Millisecond, 50*time.Millisecond,
		"a PostgreSQL restored to that backup replays the old lineage's event from the log")
	e.releasedBy(t)
}

// The first checkpoint after a Recover, no backup cursor before it, reads
// the old lineage behind its end and PostgreSQL's position; the old
// lineage drains while it is still in flight. With no backup cursor yet,
// nothing said the tail was needed: it was trimmed to its end, and the
// backup the checkpoint recorded next could not replay it. A checkpoint
// in flight holds the margin until its backup cursor is read.
func TestAnEndedLineageIsNotTrimmedUnderACheckpointInFlight(t *testing.T) {
	e := newEndedCluster(t)
	marked, proceed := make(chan struct{}), make(chan struct{})
	e.coord.wal = &markHook{fakePG: e.pg, after: func() {
		close(marked)
		<-proceed
	}}
	done := make(chan error, 1)
	go func() {
		_, err := e.coord.take(context.Background())
		done <- err
	}()
	unblock := sync.OnceFunc(func() { close(proceed) })
	t.Cleanup(unblock)
	select {
	case <-marked:
	case err := <-done:
		require.FailNow(t, "the checkpoint ended before PostgreSQL's position", "%v", err)
	case <-time.After(10 * time.Second):
		require.FailNow(t, "the checkpoint did not read PostgreSQL's position")
	}
	b, _ := e.tc.cursors.get(context.Background(), cluster_model.CursorBackup)
	require.Empty(t, b.Lineage, "no backup cursor before this checkpoint")
	assert.False(t, e.leader.r.backupSettled(b), "a checkpoint is in flight")

	e.drainOld(t)
	assert.Never(t, func() bool { return !e.holds(e.leader) }, 2500*time.Millisecond, 50*time.Millisecond,
		"the checkpoint in flight read PostgreSQL's position with the old lineage behind its end")

	unblock()
	require.NoError(t, <-done)
	b = e.backup(t)
	assert.False(t, b.Closed, "the checkpoint read the old lineage behind its end")
	assert.True(t, e.leader.r.backupSettled(b), "the backup cursor of the newest checkpoint")
	assert.False(t, e.leader.r.backupSettled(&cluster_model.Cursor{}), "a checkpoint closed whose backup cursor was not read")
	assert.Never(t, func() bool { return !e.holds(e.leader) }, 1500*time.Millisecond, 50*time.Millisecond,
		"its backup replays the old lineage's event from the log")
	e.releasedBy(t)
}

// Right after a Recover, the coordinator loses the leadership during its
// cut. Its barrier and its position used to be forwarded to the new
// leader, and the checkpoint closed in the new leader's term. That leader,
// with no checkpoint in flight of its own, an entry of its term applied
// and no checkpoint closed, took the backup as settled and trimmed the old
// lineage to its end while the deposed coordinator had read PostgreSQL's
// position with that lineage behind its end: the backup cursor it recorded
// next replays a tail the log no longer holds (#218, #214). A coordinator
// that no longer leads its term now closes no checkpoint, so none of an
// earlier leader's can close in the current term: the new leader's count
// of its own checkpoints in flight, and the guard of its term, settle the
// backup, and it trims the ended lineage once it applied an entry of its
// term.
func TestADeposedCoordinatorLeavesTheEndedLineageToTheNewLeader(t *testing.T) {
	e := newEndedCluster(t)
	ctx := context.Background()
	a, b := e.leader, e.tc.nodes[1]
	a.r.cut = &moveOnCut{cutLock: e.pg.cut, move: func() { e.tc.mem.SetLeader(b.node) }}
	e.coord.wal = &markHook{fakePG: e.pg, after: func() {
		// PostgreSQL's position is read with the old lineage behind its
		// end; it drains now, and the new leader trims it if it takes its
		// backup as settled.
		e.drainOld(t)
		for deadline := time.Now().Add(3 * time.Second); e.holds(b) && time.Now().Before(deadline); time.Sleep(20 * time.Millisecond) {
		}
	}}
	_, err := e.coord.take(ctx)
	if err == nil {
		bk := e.backup(t)
		for _, n := range e.tc.nodes {
			assert.True(t, bk.Closed || e.holds(n),
				"%s trimmed the old lineage to its end, and a checkpoint that read it behind its end closed: a restore to it cannot replay its tail", n.slot)
		}
	}
	require.Error(t, err, "a coordinator that lost the leadership during its cut closes no checkpoint")
	require.ErrorContains(t, err, "term")
	for _, n := range e.tc.nodes {
		assert.Empty(t, n.r.table.Checkpoints(), "%s holds a checkpoint of a coordinator that lost the leadership", n.slot)
	}
	bk, _ := e.tc.cursors.get(ctx, cluster_model.CursorBackup)
	require.Empty(t, bk.Lineage, "no backup cursor before the new leader's")
	assert.False(t, b.r.backupSettled(bk), "the new leader applied no entry of its term yet")

	// An entry of the new leader's term commits, and the old lineage
	// drains: no checkpoint can record a backup that replays the old
	// lineage's tail any more, and the new leader trims it to its end.
	work := t.TempDir()
	run(t, work, "init", "-q")
	sha := commit(t, work, "after the move")
	require.True(t, e.tc.pushVia(b, work, "alice/after.git", "refs/heads/main", strings.Repeat("0", 40), sha).OK)
	e.drainOld(t)
	require.Eventually(t, func() bool { return b.r.backupSettled(bk) }, 10*time.Second, 20*time.Millisecond,
		"the new leader applied an entry of its term, and takes no checkpoint")
	for _, n := range e.tc.nodes {
		require.Eventually(t, func() bool { return !e.holds(n) }, 10*time.Second, 20*time.Millisecond,
			"%s trims the ended lineage to its end", n.slot)
		assert.Empty(t, n.r.table.Checkpoints(), "%s", n.slot)
	}
}
