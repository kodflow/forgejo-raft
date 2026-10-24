// Copyright 2026 The Forgejo Authors. All rights reserved.
// SPDX-License-Identifier: GPL-3.0-or-later

package cluster

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	cluster_model "forgejo.org/models/cluster"
	"forgejo.org/modules/setting"
	"forgejo.org/modules/test"
	"forgejo.org/services/cluster/reftable"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Once one checkpoint closed, the backup cursor stays in SQL, and the trim
// never passes it (R8-23, R9-14). When no checkpoint closes any more
// (CHECKPOINT_INTERVAL set back to 0, or the leadership on a member whose
// CHECKPOINT_INTERVAL is 0), every event pushed since stays in the queue
// although its SQL is derived, until pushes are refused at nine tenths of
// MaxEventBytes. The alarm counted entries only, and came after the
// refusal: it counts the bytes the backup holds too, and fires long before
// it; the leader says it takes no checkpoint (#176).
func TestTrimHeldByTheBackupIsAlarmedBeforePushesAreRefused(t *testing.T) {
	// Set before the replicators run and restored after they stopped: the
	// cleanups run in reverse order.
	t.Cleanup(test.MockVariableValue(&setting.Cluster.AckMargin, 0))
	t.Cleanup(test.MockVariableValue(&ackEvery, 1))
	// The bound within reach of a few pushes; the production one is
	// checked against the refusal below.
	t.Cleanup(test.MockVariableValue(&backupMaxHeld, 1024))
	ctx := context.Background()
	tc := newTestCluster(t, "node-a", "node-b", "node-c")
	pg := newFakePG()
	coord := withCheckpoints(t, tc, pg)
	leader := tc.nodes[0]

	work := t.TempDir()
	run(t, work, "init", "-q")
	old := strings.Repeat("0", 40)
	push := func(i int) {
		sha := commit(t, work, fmt.Sprintf("c%d", i))
		resp := tc.pushVia(tc.nodes[1], work, "alice/busy.git", "refs/heads/main", old, sha)
		require.True(t, resp.OK, resp.Message)
		old = sha
	}
	var lin string
	lastEvent := func() uint64 {
		var last uint64
		for _, ev := range leader.r.table.Events() {
			if ev.Token.Identity().String() == lin && ev.Token.Index > last {
				last = ev.Token.Index
			}
		}
		return last
	}
	dispatchedTo := func(index uint64) func() bool {
		return func() bool {
			cur, _ := tc.cursors.get(ctx, cursorName(lin))
			return cur.Lineage == lin && uint64(cur.Position) >= index
		}
	}

	for i := range 3 {
		push(i)
	}
	_, ident := leader.r.table.Applied()
	lin = ident.String()
	rec, err := coord.take(ctx)
	require.NoError(t, err)
	b, _ := tc.cursors.get(ctx, cluster_model.CursorBackup)
	require.EqualValues(t, rec.Index, b.Position, "the checkpoint recorded the backup cursor at its barrier")

	// No coordinator takes another checkpoint: CHECKPOINT_INTERVAL = 0 on
	// the leader.
	leader.r.checkpoints.Store(nil)
	const more = 8
	for i := 3; i < 3+more; i++ {
		push(i)
	}
	held := lastEvent()
	require.Eventually(t, dispatchedTo(held), 20*time.Second, 20*time.Millisecond, "the consumer derived every push")

	// The alarm fires once the backup holds the bound, and says how much;
	// the leader says it takes no checkpoint.
	require.Eventually(t, func() bool { return leader.r.consumer.BackupLag() > 0 }, 10*time.Second, 20*time.Millisecond,
		"the backup holds %d bytes of the queue behind it", leader.r.table.EventBytesBetween(lin, rec.Index, held))
	assert.GreaterOrEqual(t, leader.r.consumer.BackupHeld(), backupMaxHeld)
	assert.Equal(t, leader.r.table.EventBytesBetween(lin, rec.Index, held), leader.r.consumer.BackupHeld())
	require.Eventually(t, func() bool { return leader.r.ConsumerHealth(ctx).CheckpointsOff }, 15*time.Second, 50*time.Millisecond,
		"the leader says it takes no checkpoint while the backup cursor exists")
	v := leader.r.ConsumerHealth(ctx)
	assert.True(t, v.HasBackup)
	assert.Equal(t, rec.Index, v.Backup)
	after := 0
	for _, ev := range leader.r.table.Events() {
		if ev.Token.Identity().String() == lin && ev.Token.Index > rec.Index {
			after++
		}
	}
	assert.Equal(t, more, after, "the trim is still held: the replay from the checkpoint is kept")

	// The production bound comes long before the refusal, at the size of
	// the events of this queue.
	avg := float64(leader.r.table.EventBytes()) / float64(leader.r.table.EventCount())
	atAlarm, atRefusal := float64(reftable.MaxEventBytes/4)/avg, float64(pushRefusedBytes)/avg
	t.Logf("average event %.0f bytes: the alarm after about %.0f events, pushes refused after about %.0f", avg, atAlarm, atRefusal)
	assert.Less(t, atAlarm, atRefusal/3, "the alarm fires long before pushes are refused")
	assert.Less(t, atAlarm, float64(backupMaxLag), "and before the bound in entries")

	// A checkpoint that closes releases the trim and the alarm.
	leader.r.checkpoints.Store(coord)
	_, err = coord.take(ctx)
	require.NoError(t, err)
	push(1000)
	require.Eventually(t, dispatchedTo(lastEvent()), 20*time.Second, 20*time.Millisecond)
	require.Eventually(t, func() bool {
		for _, ev := range leader.r.table.Events() {
			if ev.Token.Identity().String() == lin && ev.Token.Index <= held {
				return false
			}
		}
		return leader.r.consumer.BackupLag() == 0
	}, 10*time.Second, 50*time.Millisecond, "a checkpoint that closes releases the trim and the alarm")
	assert.False(t, leader.r.ConsumerHealth(ctx).CheckpointsOff)
}
