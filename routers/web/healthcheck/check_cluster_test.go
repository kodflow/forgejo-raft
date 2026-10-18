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
	assert.Contains(t, c["cluster:backup_lag"][0].Output, "lags 2000000 entries, trim held")
}
