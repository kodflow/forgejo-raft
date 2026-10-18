// Copyright 2026 The Forgejo Authors. All rights reserved.
// SPDX-License-Identifier: GPL-3.0-or-later

package setting

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"forgejo.org/modules/test"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// A PEER_PINS entry that is not slot=<64 hex> makes the section invalid
// instead of being dropped.
func TestClusterPeerPinsAreAllValidated(t *testing.T) {
	defer test.MockVariableValue(&DisableGitHooks, true)()
	pin := strings.Repeat("ab", 32)
	load := func(pins string) error {
		cfg, err := NewConfigProviderFromData(`
[cluster]
ENABLED = true
SLOT = node-a
LISTEN_ADDR = 192.0.2.1:7480
TLS_CERT_FILE = /tmp/n.pem
TLS_CA_FILE = /tmp/ca.pem
PEER_PINS = ` + pins + `
`)
		require.NoError(t, err)
		loadClusterFrom(cfg)
		return Cluster.ConfigError
	}
	require.NoError(t, load("node-b="+pin+",node-c="+pin))
	assert.Len(t, Cluster.PeerPins, 2)
	for _, bad := range []string{"node-b=" + pin + ",garbage", "node-b=" + pin[:10], "=" + pin} {
		err := load(bad)
		require.Error(t, err, bad)
		assert.Contains(t, err.Error(), "PEER_PINS", bad)
	}
}

func loadClusterSection(t *testing.T, body string) {
	t.Helper()
	cfg, err := NewConfigProviderFromData("[cluster]\n" + body)
	require.NoError(t, err)
	loadClusterFrom(cfg)
}

const autonomousNode = `
SLOT = node-a
LISTEN_ADDR = 192.0.2.1:7480
TLS_CERT_FILE = /tmp/n.pem
TLS_CA_FILE = /tmp/ca.pem
HOOK_SOCKET = /tmp/forgejo-hook.sock
`

func TestClusterExpectedNodes(t *testing.T) {
	defer test.MockVariableValue(&DisableGitHooks, true)()
	defer test.MockVariableValue(&AppDataPath, t.TempDir())()

	// 1: the standard product, whatever else the section says.
	loadClusterSection(t, "ENABLED = true\nEXPECTED_NODES = 1\n")
	assert.False(t, Cluster.Enabled)
	require.NoError(t, Cluster.ConfigError)

	// 3 to 5 turn the replicator on by themselves, PEERS naming every
	// other slot.
	peers := func(n int) string {
		var out []string
		for i := 1; i < n; i++ {
			out = append(out, fmt.Sprintf("node-%c=192.0.2.%d:7480", 'a'+i, i+1))
		}
		return strings.Join(out, ",")
	}
	for n := 3; n <= 5; n++ {
		loadClusterSection(t, fmt.Sprintf("EXPECTED_NODES = %d\nPEERS = %s", n, peers(n))+autonomousNode)
		assert.True(t, Cluster.Enabled, n)
		require.NoError(t, Cluster.ConfigError, n)
	}

	// DEAD_MEMBER_TIMEOUT: five minutes by default, one at least.
	assert.Equal(t, 5*time.Minute, Cluster.DeadMemberTimeout)
	loadClusterSection(t, "EXPECTED_NODES = 3\nDEAD_MEMBER_TIMEOUT = 2m\nPEERS = "+peers(3)+autonomousNode)
	require.NoError(t, Cluster.ConfigError)
	assert.Equal(t, 2*time.Minute, Cluster.DeadMemberTimeout)
	loadClusterSection(t, "EXPECTED_NODES = 3\nDEAD_MEMBER_TIMEOUT = 30s\nPEERS = "+peers(3)+autonomousNode)
	require.ErrorContains(t, Cluster.ConfigError, "DEAD_MEMBER_TIMEOUT")

	// 2 is refused, plainly: no safe failover without a third vote.
	loadClusterSection(t, "EXPECTED_NODES = 2\nPEERS = "+peers(2)+autonomousNode)
	assert.True(t, Cluster.Enabled, "refused, and nothing starts alone")
	require.ErrorContains(t, Cluster.ConfigError, "two nodes cannot fail over safely without a third vote")
	require.ErrorContains(t, Cluster.ConfigError, "choose 1 (the standard product, one node) or 3 and more")

	// Out of range: refused, and the section stays on so nothing starts
	// alone; an explicit ENABLED = false is obeyed.
	for _, n := range []string{"0", "6", "-1", "three"} {
		loadClusterSection(t, "EXPECTED_NODES = "+n+"\n")
		assert.True(t, Cluster.Enabled, n)
		require.ErrorContains(t, Cluster.ConfigError, "EXPECTED_NODES", n)
	}
	loadClusterSection(t, "ENABLED = false\nEXPECTED_NODES = 9\n")
	assert.False(t, Cluster.Enabled)

	// PEERS must name exactly the other slots.
	loadClusterSection(t, "EXPECTED_NODES = 3"+autonomousNode)
	require.ErrorContains(t, Cluster.ConfigError, "needs exactly 2")
	loadClusterSection(t, "EXPECTED_NODES = 3\nPEERS = "+peers(2)+autonomousNode)
	require.ErrorContains(t, Cluster.ConfigError, "needs exactly 2")
	loadClusterSection(t, "EXPECTED_NODES = 3\nPEERS = node-a=192.0.2.1:7480,"+peers(4)+autonomousNode)
	require.ErrorContains(t, Cluster.ConfigError, "needs exactly 2")
	loadClusterSection(t, "EXPECTED_NODES = 3\nPEERS = node-a=192.0.2.1:7480,"+peers(3)+autonomousNode)
	require.NoError(t, Cluster.ConfigError, "this node's own slot in PEERS is allowed")

	// The manual mode is unchanged: no EXPECTED_NODES, no requirement.
	loadClusterSection(t, "ENABLED = true"+autonomousNode)
	require.NoError(t, Cluster.ConfigError)
	assert.Zero(t, Cluster.ExpectedNodes)
}

// A node that holds a cluster's log is never turned standalone by
// EXPECTED_NODES = 1: it would fork the repositories.
func TestClusterSizeOneRefusesAMembersDataDir(t *testing.T) {
	defer test.MockVariableValue(&DisableGitHooks, true)()
	dir := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(dir, "stamp"), []byte("x"), 0o600))
	loadClusterSection(t, "EXPECTED_NODES = 1\nDATA_DIR = "+dir+"\n")
	assert.True(t, Cluster.Enabled)
	require.ErrorContains(t, Cluster.ConfigError, "fork")

	loadClusterSection(t, "ENABLED = false\nEXPECTED_NODES = 1\nDATA_DIR = "+dir+"\n")
	assert.False(t, Cluster.Enabled, "the operator said so")
	require.NoError(t, Cluster.ConfigError)
}

// Each witness signs its own fence receipts: a key listed twice in
// WITNESS_KEYS would count one witness for two.
func TestClusterWitnessKeysAreDistinct(t *testing.T) {
	defer test.MockVariableValue(&DisableGitHooks, true)()
	k1, k2 := strings.Repeat("ab", 32), strings.Repeat("cd", 32)
	base := `ENABLED = true
SLOT = node-a
LISTEN_ADDR = 192.0.2.1:7480
TLS_CERT_FILE = /tmp/n.pem
TLS_CA_FILE = /tmp/ca.pem
WITNESSES = https://w1.example,https://w2.example
`
	loadClusterSection(t, base+"WITNESS_KEYS = "+k1+","+k2+"\n")
	require.NoError(t, Cluster.ConfigError)
	loadClusterSection(t, base+"WITNESS_KEYS = "+k1+","+strings.ToUpper(k1)+"\n")
	require.Error(t, Cluster.ConfigError)
	assert.Contains(t, Cluster.ConfigError.Error(), "WITNESS_KEYS")
}
