// Copyright 2026 The Forgejo Authors. All rights reserved.
// SPDX-License-Identifier: GPL-3.0-or-later

package healthcheck

import (
	"testing"

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

// The leader says who may take the checkpoints: the voter it hands the
// leadership to, or that no voter takes them, and every write is refused
// once the queue is full (#176).
func TestCheckClusterNoVoterTakesCheckpoints(t *testing.T) {
	_, reps := cluster.NewForTesting(t.TempDir(), "node-a", "node-b", "node-c")
	leader := reps[0]
	cluster.SetBackupCursorForTesting(leader, 10)

	cluster.SetCheckpointViewForTesting(leader, []string{"node-c"}, nil)
	c := checks{}
	checkCluster(leader, c)
	assert.Equal(t, warn, c["cluster:checkpoints"][0].Status)
	assert.Contains(t, c["cluster:checkpoints"][0].Output, "the leadership goes to a voter that takes them (node-c)")

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
