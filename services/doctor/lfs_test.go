// Copyright 2026 The Forgejo Authors. All rights reserved.
// SPDX-License-Identifier: GPL-3.0-or-later

package doctor

import (
	"testing"

	"forgejo.org/modules/clusterhook"
	"forgejo.org/modules/log"
	"forgejo.org/modules/setting"
	"forgejo.org/modules/test"
	"forgejo.org/services/repository"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// `forgejo doctor check --run gc-lfs --fix` (or --all --fix) on a node of a
// cluster runs without the replicator: it judged LFS objects orphaned from
// that node's git alone, and deleted them for every member (#349). It is
// refused before anything is read, and the refusal names the gc_lfs cron
// task, which a running node gates.
func TestGarbageCollectLFSCheckIsRefusedOnANodeOfACluster(t *testing.T) {
	defer test.MockVariableValue(&setting.LFS.StartServer, true)()
	defer test.MockVariableValue(&setting.Cluster.Enabled, true)()
	require.False(t, clusterhook.Enabled(), "the doctor runs no replicator")

	err := garbageCollectLFSCheck(t.Context(), log.GetLogger(log.DEFAULT), true)
	require.ErrorIs(t, err, repository.ErrLFSCollectionWithoutReplicator)
	assert.Contains(t, err.Error(), "gc_lfs")
}
