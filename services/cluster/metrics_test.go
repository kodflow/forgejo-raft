// Copyright 2026 The Forgejo Authors. All rights reserved.
// SPDX-License-Identifier: GPL-3.0-or-later

package cluster

import (
	"strings"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// The names pass Prometheus' lint and the leader exposes its peers.
func TestMetricsExposeTheReplicator(t *testing.T) {
	_, reps := NewForTesting(t.TempDir(), "node-a", "node-b", "node-c")
	defer SetCurrentForTesting(reps[0])()
	reg := prometheus.NewPedanticRegistry()
	require.NoError(t, reg.Register(NewCollector()))
	problems, err := testutil.GatherAndLint(reg)
	require.NoError(t, err)
	assert.Empty(t, problems)
	mfs, err := reg.Gather()
	require.NoError(t, err)
	names := map[string]bool{}
	for _, mf := range mfs {
		names[mf.GetName()] = true
	}
	for _, want := range []string{"forgejo_cluster_failure_tolerance", "forgejo_cluster_applied_index", "forgejo_cluster_peer_lag_entries", "forgejo_cluster_role", "forgejo_cluster_ready", "forgejo_cluster_projection_failing_repositories"} {
		assert.True(t, names[want], want)
	}
	for n := range names {
		assert.True(t, strings.HasPrefix(n, "forgejo_cluster_"), n)
	}
}
