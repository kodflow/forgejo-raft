// Copyright 2026 The Forgejo Authors. All rights reserved.
// SPDX-License-Identifier: GPL-3.0-or-later

package setting

import (
	"testing"
	"time"

	"forgejo.org/modules/test"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const observerNode = `
ROLE = observer
OBSERVER_NAME = nas-paris
OBSERVER_DIR = /volume1/observer
OBSERVER_ENDPOINTS = node-a=forgejo-export@192.0.2.1:22,node-b=forgejo-export@192.0.2.2:22
OBSERVER_KNOWN_HOSTS = /run/observer/known_hosts
OBSERVER_SSH_KEY_FILE = /run/observer/id_ed25519
OBSERVER_SECRETS_KEY_FILE = /run/observer/secrets.key
OBSERVER_SNAPSHOT_HOOK = /usr/local/bin/snapshot-observer
`

// An observer never becomes a member: every key that would make it listen,
// hold a slot, be granted a join or found a cluster is refused, and the
// replicator stays off whatever else the section says.
func TestObserverSettingsRefuseListeners(t *testing.T) {
	defer test.MockVariableValue(&DisableGitHooks, true)()

	loadClusterSection(t, observerNode)
	require.NoError(t, Cluster.ConfigError)
	assert.False(t, Cluster.Enabled, "an observer runs no replicator")
	assert.Equal(t, ClusterRoleObserver, Cluster.Role)
	assert.Equal(t, "nas-paris", Cluster.Observer.Name)
	assert.Equal(t, "forgejo-export@192.0.2.2:22", Cluster.Observer.Endpoints["node-b"])
	assert.Equal(t, 15*time.Second, Cluster.Observer.PollInterval)
	assert.Equal(t, 30*time.Minute, Cluster.Observer.ExportSilence)
	loadClusterSection(t, observerNode+"OBSERVER_EXPORT_SILENCE = 2h\n")
	require.NoError(t, Cluster.ConfigError)
	assert.Equal(t, 2*time.Hour, Cluster.Observer.ExportSilence)
	loadClusterSection(t, observerNode+"OBSERVER_EXPORT_SILENCE = 10s\n")
	require.ErrorContains(t, Cluster.ConfigError, "OBSERVER_EXPORT_SILENCE")
	loadClusterSection(t, observerNode)
	assert.Equal(t, 7*24*time.Hour, Cluster.Observer.BaseEvery)
	loadClusterSection(t, observerNode+"OBSERVER_BASE_EVERY = 24h\n")
	require.NoError(t, Cluster.ConfigError)
	assert.Equal(t, 24*time.Hour, Cluster.Observer.BaseEvery)
	loadClusterSection(t, observerNode+"OBSERVER_BASE_EVERY = 10m\n")
	require.ErrorContains(t, Cluster.ConfigError, "OBSERVER_BASE_EVERY")

	for _, line := range []string{
		"LISTEN_ADDR = 192.0.2.9:7480",
		"OBJECT_LISTEN_ADDR = 192.0.2.9:7481",
		"OBJECT_PORT = 7481",
		"OBJECT_PEERS = node-a=192.0.2.1:7481",
		"SLOT = observer-1",
		"PEERS = node-a=192.0.2.1:7480",
		"PEER_PINS = node-a=00",
		"EXPECTED_NODES = 3",
		"CLUSTER_ID = 0123456789abcdef0123456789abcdef",
		"JOIN_GRANT_TTL = 1h",
		"OPERATORS = root",
		"ENABLED = true",
		"CHECKPOINT_INTERVAL = 1m",
	} {
		loadClusterSection(t, observerNode+line+"\n")
		require.Error(t, Cluster.ConfigError, line)
		assert.Contains(t, Cluster.ConfigError.Error(), "refused with ROLE = observer", line)
		assert.False(t, Cluster.Enabled, "%s: the replicator never starts on an observer", line)
	}

	// What it needs to pull is required, and an endpoint never carries an
	// ssh option.
	loadClusterSection(t, "ROLE = observer\nOBSERVER_NAME = nas-paris\n")
	require.Error(t, Cluster.ConfigError)
	for _, want := range []string{"OBSERVER_ENDPOINTS", "OBSERVER_DIR", "OBSERVER_KNOWN_HOSTS", "OBSERVER_SSH_KEY_FILE", "OBSERVER_SECRETS_KEY_FILE", "OBSERVER_SNAPSHOT_HOOK"} {
		assert.Contains(t, Cluster.ConfigError.Error(), want)
	}
	loadClusterSection(t, observerNode+"OBSERVER_ENDPOINTS = node-a=-oProxyCommand=x@h:22\n")
	require.ErrorContains(t, Cluster.ConfigError, "OBSERVER_ENDPOINTS")

	// An unknown role fails closed.
	loadClusterSection(t, "ROLE = witness\n")
	require.ErrorContains(t, Cluster.ConfigError, "ROLE")
	assert.True(t, Cluster.Enabled, "refused, and nothing starts alone")

	// A member keeps its role by default.
	loadClusterSection(t, "ENABLED = true"+autonomousNode)
	require.NoError(t, Cluster.ConfigError)
	assert.Equal(t, ClusterRoleVoter, Cluster.Role)
}
