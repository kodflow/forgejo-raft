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
