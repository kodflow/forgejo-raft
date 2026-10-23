// Copyright 2026 The Forgejo Authors. All rights reserved.
// SPDX-License-Identifier: GPL-3.0-or-later

package cluster

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"forgejo.org/services/cluster/consensus"
	"forgejo.org/services/cluster/reftable"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// A snapshot whose keys climb out of the repository root never reaches the
// projector, and the projector itself refuses such a key: nothing outside
// the root is removed or moved (#81). The keys climb one level only, to
// witnesses the test creates beside the root, inside its own directory.
func TestHuntR4ProveSnapKeyEscape(t *testing.T) {
	base := t.TempDir()
	root := filepath.Join(base, "repos")
	witness := filepath.Join(base, "raft-data")
	moved := filepath.Join(base, "tls")
	for _, d := range []string{root, witness, moved} {
		require.NoError(t, os.MkdirAll(d, 0o755))
	}
	require.NoError(t, os.WriteFile(filepath.Join(witness, "log.000001"), []byte("log"), 0o600))
	require.NoError(t, os.WriteFile(filepath.Join(moved, "node.key"), []byte("key"), 0o600))

	table := reftable.New()
	p := newProjector(root, table, newRegistry(time.Second, 8, 64))
	snap := `{"format":1,"repos":{"../raft-data":{},"../tls":{},"evil/stolen.git":{}},"last":{},"deleted":{"../raft-data":5},"renamed":{"evil/stolen.git":{"from":"../tls","i":6}}}`
	require.Error(t, table.Restore(strings.NewReader(snap), consensus.SnapshotMeta{}), "Restore must refuse a repository key that Apply refuses")
	p.unfence()
	p.pass(t.Context())

	// The projector's own guard, for a key that would come another way.
	for _, key := range []string{"../raft-data", "../tls", "/abs.git"} {
		require.Error(t, p.project(t.Context(), t.Context(), key), key)
	}
	assert.FileExists(t, filepath.Join(witness, "log.000001"), "the projector removed a directory outside the repository root")
	assert.FileExists(t, filepath.Join(moved, "node.key"), "the projector moved a directory from outside the repository root")
	entries, err := os.ReadDir(base)
	require.NoError(t, err)
	assert.Len(t, entries, 3, "nothing was set aside beside the root")
}
