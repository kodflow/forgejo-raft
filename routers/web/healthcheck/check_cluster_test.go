// Copyright 2026 The Forgejo Authors. All rights reserved.
// SPDX-License-Identifier: GPL-3.0-or-later

package healthcheck

import (
	"testing"
	"time"

	"forgejo.org/services/cluster"
	"forgejo.org/services/cluster/consensus/memraft"

	"github.com/stretchr/testify/assert"
)

// A healthy follower does not warn: the rolling update reads its gate on the
// leader, which warns when no further node may go down.
func TestCheckClusterFailureTolerance(t *testing.T) {
	mem, reps := cluster.NewForTesting(t.TempDir(), "node-a", "node-b", "node-c")
	leader, follower := reps[0], reps[1]

	c := checks{}
	checkCluster(follower, c)
	assert.Equal(t, Pass, c["cluster:failure_tolerance"][0].Status, c["cluster:failure_tolerance"][0].Output)

	c = checks{}
	checkCluster(leader, c)
	assert.Equal(t, Pass, c["cluster:failure_tolerance"][0].Status, c["cluster:failure_tolerance"][0].Output)

	// One voter down: the leader warns, nothing more may fail.
	mem.SetDown(reps[2].Engine().(*memraft.Node), true)
	c = checks{}
	checkCluster(leader, c)
	assert.Equal(t, warn, c["cluster:failure_tolerance"][0].Status, c["cluster:failure_tolerance"][0].Output)
}

// SQL that a failover of the database could roll back is a warning: the
// event queue is not trimmed while it lasts (finding 4).
func TestCheckClusterSQLDurability(t *testing.T) {
	_, reps := cluster.NewForTesting(t.TempDir(), "node-a")
	c := checks{}
	checkCluster(reps[0], c)
	assert.Equal(t, Pass, c["cluster:sql_durability"][0].Status, "not checked yet")

	cluster.SetSQLDurabilityForTesting(reps[0], false, "synchronous_standby_names is empty")
	c = checks{}
	checkCluster(reps[0], c)
	assert.Equal(t, warn, c["cluster:sql_durability"][0].Status)
	assert.Contains(t, c["cluster:sql_durability"][0].Output, "synchronous_standby_names")

	cluster.SetSQLDurabilityForTesting(reps[0], true, "1 synchronous standbys hold every commit read")
	c = checks{}
	checkCluster(reps[0], c)
	assert.Equal(t, Pass, c["cluster:sql_durability"][0].Status)
}

// A backup that lags beyond its bound holds the trim, and says so.
func TestCheckClusterBackupLag(t *testing.T) {
	_, reps := cluster.NewForTesting(t.TempDir(), "node-a")
	c := checks{}
	checkCluster(reps[0], c)
	assert.Equal(t, Pass, c["cluster:backup_lag"][0].Status)
	assert.Equal(t, Pass, c["cluster:event_queue"][0].Status)

	cluster.SetBackupLagForTesting(reps[0], 2_000_000)
	c = checks{}
	checkCluster(reps[0], c)
	assert.Equal(t, warn, c["cluster:backup_lag"][0].Status)
	assert.Contains(t, c["cluster:backup_lag"][0].Output, "holds the trim: 2000000 entries")
	// This leader is not seen to lack the checkpoints: the way out is the
	// checkpoint that does not close, not CHECKPOINT_INTERVAL.
	assert.Contains(t, c["cluster:backup_lag"][0].Output, "no checkpoint closed since it")
	assert.NotContains(t, c["cluster:backup_lag"][0].Output, "transfer the leadership")
}

// A leader that takes no checkpoint while a backup cursor of its lineage
// exists says so, with the way out, before the backup holds anything; the
// event queue's warning names the backup as what holds the trim (#176).
func TestCheckClusterCheckpointsOff(t *testing.T) {
	_, reps := cluster.NewForTesting(t.TempDir(), "node-a")
	c := checks{}
	checkCluster(reps[0], c)
	assert.Equal(t, Pass, c["cluster:checkpoints"][0].Status, c["cluster:checkpoints"][0].Output)

	cluster.SetBackupCursorForTesting(reps[0], 10)
	c = checks{}
	checkCluster(reps[0], c)
	assert.Equal(t, warn, c["cluster:checkpoints"][0].Status)
	assert.Contains(t, c["cluster:checkpoints"][0].Output, "takes no checkpoint")
	assert.Contains(t, c["cluster:checkpoints"][0].Output, "CHECKPOINT_INTERVAL")

	cluster.SetBackupHoldForTesting(reps[0], 40, 17<<20)
	c = checks{}
	checkCluster(reps[0], c)
	assert.Equal(t, warn, c["cluster:backup_lag"][0].Status)
	assert.Contains(t, c["cluster:backup_lag"][0].Output, "40 entries and 17825792 bytes")
	assert.Contains(t, c["cluster:backup_lag"][0].Output, "transfer the leadership")
	assert.NotContains(t, c["cluster:backup_lag"][0].Output, "take a checkpoint")
}

// The leader says who may take the checkpoints: a voter that takes them
// may lead instead, and why the leader does not hand it the leadership for
// now, or that no voter takes them, and every write is refused once the
// queue is full (#176).
func TestCheckClusterNoVoterTakesCheckpoints(t *testing.T) {
	_, reps := cluster.NewForTesting(t.TempDir(), "node-a", "node-b", "node-c")
	leader := reps[0]
	cluster.SetBackupCursorForTesting(leader, 10)

	cluster.SetCheckpointViewForTesting(leader, []string{"node-c"}, nil)
	c := checks{}
	checkCluster(leader, c)
	assert.Equal(t, warn, c["cluster:checkpoints"][0].Status)
	assert.Contains(t, c["cluster:checkpoints"][0].Output, "the leadership may go to a voter that takes them (node-c)")
	assert.NotContains(t, c["cluster:checkpoints"][0].Output, "not handed")

	// A taker the leader would not hand the leadership to: it says why.
	cluster.SetCheckpointViewForTesting(leader, []string{"node-c"}, nil, "node-c: 65 entries behind, at most 64")
	c = checks{}
	checkCluster(leader, c)
	assert.Contains(t, c["cluster:checkpoints"][0].Output, "the leadership may go to a voter that takes them (node-c); not handed to it for now: node-c: 65 entries behind, at most 64")

	cluster.SetCheckpointViewForTesting(leader, nil, []string{"node-b"})
	c = checks{}
	checkCluster(leader, c)
	assert.Contains(t, c["cluster:checkpoints"][0].Output, "no voter that answered takes them, and node-b did not say")

	cluster.SetCheckpointViewForTesting(leader, nil, nil)
	c = checks{}
	checkCluster(leader, c)
	assert.Contains(t, c["cluster:checkpoints"][0].Output, "no voter takes them")
	assert.Contains(t, c["cluster:checkpoints"][0].Output, "every write is refused once the queue is full")

	// A follower reports nothing of the leader's.
	c = checks{}
	checkCluster(reps[1], c)
	assert.Equal(t, Pass, c["cluster:checkpoints"][0].Status)
}

// What holds the trim of a filling queue: SQL is said not durable only once
// its durability is checked, as cluster:sql_durability says; before that,
// it is said not checked yet, after what is known to hold the trim (#176).
func TestQueueHolderSaysSQLIsNotDurableOnlyOnceChecked(t *testing.T) {
	checked := time.Now()
	assert.Contains(t, queueHolder(cluster.View{}), "not checked yet")
	assert.NotContains(t, queueHolder(cluster.View{}), "SQL is not durable")
	assert.Contains(t, queueHolder(cluster.View{SQLDurabilityChecked: checked}), "SQL is not durable")
	assert.Contains(t, queueHolder(cluster.View{SQLDurabilityChecked: checked, SQLDurable: true}), "not derived yet")
	assert.Contains(t, queueHolder(cluster.View{CheckpointsOff: true}), "no checkpoint will move it")
	assert.Contains(t, queueHolder(cluster.View{ParkedEvents: 3}), "parked events")
}

// The status page and the health choose the way out of a held backup the
// same way: the leadership or CHECKPOINT_INTERVAL when this leader takes no
// checkpoint, the checkpoint that does not close otherwise (#176).
func TestTheWayOutOfAHeldBackupFollowsTheCheckpoints(t *testing.T) {
	for _, tc := range []struct {
		name    string
		v       cluster.View
		untaken bool
	}{
		{"a leader that takes none", cluster.View{HasBackup: true, CheckpointsOff: true}, true},
		{"a leader that takes them", cluster.View{HasBackup: true, CheckpointsHere: true}, false},
		{"a follower", cluster.View{}, false},
	} {
		assert.Equal(t, tc.untaken, tc.v.CheckpointsUntaken(), tc.name)
		if tc.untaken {
			assert.Contains(t, checkpointRemedy(tc.v), "CHECKPOINT_INTERVAL", tc.name)
		} else {
			assert.Contains(t, checkpointRemedy(tc.v), "no checkpoint closed since it", tc.name)
		}
	}
}
