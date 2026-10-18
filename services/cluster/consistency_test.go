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

// A push still running on one node when the repository is deleted or renamed
// on another: its new branch must not bring the old key back to life in the
// table, under a name the database no longer has, where its push would be
// reported a success and then be lost.
func TestPushToADeletedOrRenamedKeyIsRefused(t *testing.T) {
	tc := newTestCluster(t, "node-a", "node-b", "node-c")
	work := t.TempDir()
	run(t, work, "init", "-q")
	c1 := commit(t, work, "one")
	c2 := commit(t, work, "two")
	zero := strings.Repeat("0", 40)
	ctx := context.Background()
	origin, other := tc.nodes[0], tc.nodes[2]

	require.True(t, tc.pushVia(origin, work, "p/del.git", "refs/heads/main", zero, c1).OK)
	require.True(t, tc.pushVia(origin, work, "p/old.git", "refs/heads/main", zero, c1).OK)
	for _, n := range tc.nodes {
		require.Eventually(t, func() bool {
			return gitRef(t, n, "p/del.git", "refs/heads/main") == c1 && gitRef(t, n, "p/old.git", "refs/heads/main") == c1
		}, 10*time.Second, 20*time.Millisecond)
	}

	require.NoError(t, os.RemoveAll(filepath.Join(origin.r.root, "p", "del.git")))
	require.NoError(t, origin.r.proposeRepoDelete(ctx, "p/del.git", 1))
	require.NoError(t, os.Rename(filepath.Join(origin.r.root, "p", "old.git"), filepath.Join(origin.r.root, "p", "new.git")))
	require.NoError(t, origin.r.proposeRepoRename(ctx, "p/old.git", "p/new.git"))
	require.Eventually(t, func() bool {
		_, deleted := other.r.table.Deleted("p/del.git")
		_, _, renamed := other.r.table.RenamedFrom("p/new.git")
		return deleted && renamed
	}, 10*time.Second, 10*time.Millisecond)
	for _, repo := range []string{"del.git", "old.git"} {
		require.Eventually(t, func() bool {
			_, err := os.Stat(filepath.Join(other.r.root, "p", repo))
			return os.IsNotExist(err)
		}, 10*time.Second, 10*time.Millisecond, repo)
	}

	for _, repo := range []string{"p/del.git", "p/old.git"} {
		resp := tc.pushVia(other, work, repo, "refs/heads/late", zero, c2)
		assert.False(t, resp.OK, repo)
		_, ok := other.r.table.Get(repo, "refs/heads/late")
		assert.False(t, ok, repo)
	}
	_, deleted := other.r.table.Deleted("p/del.git")
	assert.True(t, deleted, "the deleted key stays deleted")

	// Forgejo creating a repository again under a freed name is no push:
	// its HEAD is written, and the name lives again.
	dir := filepath.Join(other.r.root, "p", "del.git")
	resp := other.r.Handle(ctx, &clusterhook.Request{
		Op: clusterhook.OpPrepared, Nonce: clusterhook.NewNonce(), GitDir: dir,
		Lines: []clusterhook.Line{{Old: zero, New: "ref:refs/heads/main", Ref: "HEAD"}},
	})
	require.True(t, resp.OK, resp.Message)
}
