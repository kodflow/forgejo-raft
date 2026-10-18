// Copyright 2026 The Forgejo Authors. All rights reserved.
// SPDX-License-Identifier: GPL-3.0-or-later

package cluster

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"forgejo.org/modules/clusterhook"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// A branch created on one node, then created again on another whose git has
// not projected it yet: receive-pack on the second node sees no such branch
// and sends a zero old value, which means "must not exist". The table holds
// the first branch: the second push must be refused, not turned into an
// overwrite that drops the first branch's commits.
func TestStaleCreateDoesNotOverwriteABranch(t *testing.T) {
	tc := newTestCluster(t, "node-a", "node-b", "node-c")
	work := t.TempDir()
	run(t, work, "init", "-q")
	c1 := commit(t, work, "one")
	c2 := commit(t, work, "two")
	zero := strings.Repeat("0", 40)
	require.True(t, tc.pushVia(tc.nodes[0], work, "a/r.git", "refs/heads/topic", zero, c1).OK)
	nodeC := tc.nodes[2]
	require.Eventually(t, func() bool {
		v, ok := nodeC.r.table.Get("a/r.git", "refs/heads/topic")
		return ok && v.SHA == c1
	}, 10*time.Second, 10*time.Millisecond)

	resp := tc.pushVia(nodeC, work, "a/r.git", "refs/heads/topic", zero, c2)
	assert.False(t, resp.OK, "a creation over a live branch must be refused")
	assert.Contains(t, resp.Message, "already exists")
	for _, n := range tc.nodes {
		v, _ := n.r.table.Get("a/r.git", "refs/heads/topic")
		assert.Equal(t, c1, v.SHA, n.slot)
	}
}

// A rename replicated while a push is in flight on the old key of another
// node: the projector there must not move the directory from under the git
// process writing into it. The registration fences the old key, and the
// move waits until the transaction ends.
func TestRenameWaitsForATransactionInFlightOnTheOldKey(t *testing.T) {
	tc := newTestCluster(t, "node-a", "node-b", "node-c")
	work := t.TempDir()
	run(t, work, "init", "-q")
	c1 := commit(t, work, "one")
	zero := strings.Repeat("0", 40)
	require.True(t, tc.pushVia(tc.nodes[0], work, "o/old.git", "refs/heads/main", zero, c1).OK)
	for _, n := range tc.nodes {
		require.Eventually(t, func() bool { return gitRef(t, n, "o/old.git", "refs/heads/main") == c1 }, 10*time.Second, 20*time.Millisecond)
	}

	nodeB := tc.nodes[1]
	nonce := clusterhook.NewNonce()
	require.NoError(t, nodeB.r.registry.register(nonce, "o/old.git", []string{"refs/heads/main"}, os.Getpid()))

	origin := tc.nodes[0]
	require.NoError(t, os.Rename(filepath.Join(origin.r.root, "o", "old.git"), filepath.Join(origin.r.root, "o", "new.git")))
	require.NoError(t, origin.r.proposeRepoRename(context.Background(), "o/old.git", "o/new.git"))
	require.Eventually(t, func() bool { _, _, ok := nodeB.r.table.RenamedFrom("o/new.git"); return ok }, 10*time.Second, 10*time.Millisecond)

	oldDir := filepath.Join(nodeB.r.root, "o", "old.git")
	require.Never(t, func() bool {
		_, err := os.Stat(oldDir)
		return os.IsNotExist(err)
	}, 3*time.Second, 50*time.Millisecond, "the directory moved under a transaction in flight")

	nodeB.r.registry.release(nonce)
	nodeB.r.projector.markDirty("o/new.git")
	require.Eventually(t, func() bool { return gitRef(t, nodeB, "o/new.git", "refs/heads/main") == c1 }, 10*time.Second, 20*time.Millisecond)
	_, err := os.Stat(oldDir)
	assert.True(t, os.IsNotExist(err))
}
