// Copyright 2026 The Forgejo Authors. All rights reserved.
// SPDX-License-Identifier: GPL-3.0-or-later

package cluster

import (
	"bytes"
	"context"
	"strings"
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

// After a Recover, a checkpoint of the new lineage taken while the old
// lineage's tail still waits for its work holds that lineage's cursor
// behind its end: a PostgreSQL restored to it replays the tail from the
// log, so the tail is not trimmed to its end on that backup. The next
// checkpoint reads the old lineage drained, says so with the backup
// cursor, and the tail is trimmed to its end, by the existing Ack, on
// every member: its objects are no longer pinned against gc (#205).
func TestCheckpointReleasesTheEndedLineageOnceItReadItDrained(t *testing.T) {
	ctx := context.Background()
	tc := newTestCluster(t, "node-a", "node-b", "node-c")
	pg := newFakePG()
	coord := withCheckpoints(t, tc, pg)
	leader := tc.nodes[0]

	// The old lineage's last event waits for its work until released.
	release := make(chan struct{})
	for _, n := range tc.nodes {
		n.r.consumer.work = func(ctx context.Context, evs []reftable.Event) error {
			if evs[0].Repo == "o/old.git" {
				select {
				case <-release:
				case <-ctx.Done():
					return ctx.Err()
				}
			}
			return pg.work(ctx, evs)
		}
	}
	defer func() {
		select {
		case <-release:
		default:
			close(release)
		}
	}()
	oldID := consensus.LogIdentity{Cluster: consensus.ClusterID{0xde, 0xad}, Epoch: 1}
	old := oldID.String()
	ev := reftable.Event{
		Token: consensus.Token{Cluster: oldID.Cluster, Epoch: oldID.Epoch, Term: 1, Index: 1},
		Kind:  reftable.EventRef, Repo: "o/old.git", Ref: "refs/heads/main", PusherID: 1,
		Old: strings.Repeat("1", 40), New: strings.Repeat("2", 40),
	}
	for _, n := range tc.nodes {
		withEarlierEvents(t, n.r.table, ev)
	}
	holds := func(n *testNode) bool {
		for _, e := range n.r.table.Events() {
			if e.Token.Identity() == oldID {
				return true
			}
		}
		return false
	}
	require.True(t, holds(leader))
	require.NotEmpty(t, leader.r.table.PinnedObjects("o/old.git"))
	leader.r.signalConsumer()

	// A checkpoint while the old lineage's event waits for its work.
	_, err := coord.take(ctx)
	require.NoError(t, err)
	_, current := leader.r.table.Applied()
	require.NotEqual(t, oldID, current)
	b, _ := tc.cursors.get(ctx, cluster_model.CursorBackup)
	require.Equal(t, current.String(), b.Lineage)
	assert.False(t, b.Closed, "the checkpoint read the old lineage behind its end")

	close(release)
	require.Eventually(t, func() bool {
		cur, _ := tc.cursors.get(ctx, cursorName(old))
		return cur.Lineage == old && cur.Position == 1
	}, 10*time.Second, 20*time.Millisecond, "the old lineage's event is dispatched")
	assert.Never(t, func() bool { return !holds(leader) }, 2500*time.Millisecond, 50*time.Millisecond,
		"a PostgreSQL restored to that backup replays the old lineage's event from the log")

	// The next checkpoint reads it drained, and releases it.
	_, err = coord.take(ctx)
	require.NoError(t, err)
	b, _ = tc.cursors.get(ctx, cluster_model.CursorBackup)
	assert.True(t, b.Closed, "the checkpoint read the old lineage drained")
	for _, n := range tc.nodes {
		require.Eventually(t, func() bool { return !holds(n) }, 10*time.Second, 20*time.Millisecond,
			"%s trims the ended lineage to its end", n.slot)
		assert.Empty(t, n.r.table.PinnedObjects("o/old.git"), "%s: git gc may run on the repository again", n.slot)
	}
}
