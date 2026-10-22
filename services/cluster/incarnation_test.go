// Copyright 2026 The Forgejo Authors. All rights reserved.
// SPDX-License-Identifier: GPL-3.0-or-later

package cluster

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"forgejo.org/modules/clusterhook"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// createVia plays Forgejo's creation of repo on node n: git init, whose
// HEAD the hook proposes without a pusher, as services/repository does.
func (tc *testCluster) createVia(n *testNode, repo string) {
	t := tc.t
	dir := filepath.Join(n.r.root, filepath.FromSlash(repo))
	run(t, n.r.root, "init", "-q", "--bare", dir)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	req := &clusterhook.Request{
		Op: clusterhook.OpPrepared, Nonce: clusterhook.NewNonce(), GitDir: dir,
		Lines: []clusterhook.Line{{Old: strings.Repeat("0", 40), New: "ref:refs/heads/main", Ref: "HEAD"}},
	}
	resp := n.r.Handle(ctx, req)
	require.True(t, resp.OK, resp.Message)
	req.Op = clusterhook.OpCommitted
	n.r.Handle(ctx, req)
}

// holdOn keeps n's projector off keys, as a transaction in flight does, and
// returns the release, which schedules them.
func holdOn(t *testing.T, n *testNode, keys ...string) func() {
	t.Helper()
	nonces := make([]string, len(keys))
	for i, k := range keys {
		nonces[i] = clusterhook.NewNonce()
		require.NoError(t, n.r.registry.register(nonces[i], k, nil, os.Getpid(), false))
	}
	return func() {
		for i, k := range keys {
			n.r.registry.release(nonces[i])
			n.r.projector.markDirty(k)
		}
	}
}

// hasObject reports whether n's repo holds the object sha.
func hasObject(n *testNode, repo, sha string) bool {
	dir := filepath.Join(n.r.root, filepath.FromSlash(repo))
	return exec.Command("git", "-C", dir, "cat-file", "-e", sha+"^{commit}").Run() == nil
}

func waitAll(t *testing.T, tc *testCluster, cond func(n *testNode) bool) {
	t.Helper()
	for _, n := range tc.nodes {
		require.Eventually(t, func() bool { return cond(n) }, 15*time.Second, 20*time.Millisecond, n.slot)
	}
}

// A repository deleted and created again under its name while a node did
// not project the deletion: that node reused the deleted repository's
// directory, which kept the deleted objects (#52). Its stamp predates the
// key's deletion, which the table keeps after the revival: it is set
// aside, and the new repository projected afresh.
func TestHuntR2ProveRecreateSameFormatKeepsDeletedObjects(t *testing.T) {
	tc := newTestCluster(t, "node-a", "node-b", "node-c")
	work := t.TempDir()
	run(t, work, "init", "-q")
	c1 := commit(t, work, "one")
	c2 := commit(t, work, "two")
	run(t, work, "checkout", "-q", "--orphan", "fresh")
	c3 := commit(t, work, "three")
	zero := strings.Repeat("0", 40)
	origin, lag := tc.nodes[0], tc.nodes[2]
	require.True(t, tc.pushVia(origin, work, "s/x.git", "refs/heads/main", zero, c1).OK)
	require.True(t, tc.pushVia(origin, work, "s/x.git", "refs/heads/main", c1, c2).OK)
	waitAll(t, tc, func(n *testNode) bool { return gitRef(t, n, "s/x.git", "refs/heads/main") == c2 })

	release := holdOn(t, lag, "s/x.git")
	ctx := context.Background()
	require.NoError(t, os.RemoveAll(filepath.Join(origin.r.root, "s", "x.git")))
	require.NoError(t, origin.r.proposeRepoDelete(ctx, "s/x.git", 1))
	tc.createVia(origin, "s/x.git")
	require.True(t, tc.pushVia(origin, work, "s/x.git", "refs/heads/main", zero, c3).OK)
	release()

	waitAll(t, tc, func(n *testNode) bool { return gitRef(t, n, "s/x.git", "refs/heads/main") == c3 })
	for _, n := range tc.nodes {
		assert.False(t, hasObject(n, "s/x.git", c1), "%s: the deleted commit is in the new s/x.git", n.slot)
	}
	entries, err := os.ReadDir(filepath.Join(lag.r.root, staleDirName))
	require.NoError(t, err)
	assert.Len(t, entries, 1, "the deleted repository's directory was set aside")
	assert.Empty(t, lag.r.projector.Lag())
}

// A node behind the log moved a deleted repository's directory under a
// live key: x renamed to z, z deleted and created again (form A), or a
// key deleted, created again, then renamed (form B) (#98). The deleted
// repository's directory is set aside, never moved.
func TestHuntR7ProveRmkRenameMark(t *testing.T) {
	t.Run("form A", func(t *testing.T) {
		tc := newTestCluster(t, "node-a", "node-b", "node-c")
		work := t.TempDir()
		run(t, work, "init", "-q")
		c1 := commit(t, work, "one")
		run(t, work, "checkout", "-q", "--orphan", "fresh")
		c2 := commit(t, work, "two")
		zero := strings.Repeat("0", 40)
		origin, lag := tc.nodes[0], tc.nodes[2]
		require.True(t, tc.pushVia(origin, work, "s/x.git", "refs/heads/main", zero, c1).OK)
		waitAll(t, tc, func(n *testNode) bool { return gitRef(t, n, "s/x.git", "refs/heads/main") == c1 })

		release := holdOn(t, lag, "s/x.git", "s/z.git")
		ctx := context.Background()
		root := origin.r.root
		require.NoError(t, os.Rename(filepath.Join(root, "s", "x.git"), filepath.Join(root, "s", "z.git")))
		require.NoError(t, origin.r.proposeRepoRename(ctx, "s/x.git", "s/z.git"))
		require.NoError(t, os.RemoveAll(filepath.Join(root, "s", "z.git")))
		require.NoError(t, origin.r.proposeRepoDelete(ctx, "s/z.git", 1))
		tc.createVia(origin, "s/z.git")
		require.True(t, tc.pushVia(origin, work, "s/z.git", "refs/heads/main", zero, c2).OK)
		release()

		waitAll(t, tc, func(n *testNode) bool { return gitRef(t, n, "s/z.git", "refs/heads/main") == c2 })
		require.Eventually(t, func() bool {
			_, err := os.Stat(filepath.Join(lag.r.root, "s", "x.git"))
			return os.IsNotExist(err)
		}, 15*time.Second, 20*time.Millisecond, "the deleted repository's directory left s/x.git")
		for _, n := range tc.nodes {
			assert.False(t, hasObject(n, "s/z.git", c1), "%s: the deleted commit is in the new s/z.git", n.slot)
		}
		entries, err := os.ReadDir(filepath.Join(lag.r.root, staleDirName))
		require.NoError(t, err)
		assert.Len(t, entries, 1)
	})

	t.Run("form B", func(t *testing.T) {
		tc := newTestCluster(t, "node-a", "node-b", "node-c")
		work := t.TempDir()
		run(t, work, "init", "-q")
		c1 := commit(t, work, "one")
		secret := commit(t, work, "secret")
		run(t, work, "checkout", "-q", "--orphan", "fresh")
		c2 := commit(t, work, "two")
		zero := strings.Repeat("0", 40)
		origin, lag := tc.nodes[0], tc.nodes[2]
		require.True(t, tc.pushVia(origin, work, "o/r.git", "refs/heads/main", zero, c1).OK)
		require.True(t, tc.pushVia(origin, work, "o/r.git", "refs/heads/secret", zero, secret).OK)
		waitAll(t, tc, func(n *testNode) bool { return gitRef(t, n, "o/r.git", "refs/heads/secret") == secret })

		release := holdOn(t, lag, "o/r.git", "o/new.git")
		ctx := context.Background()
		root := origin.r.root
		require.NoError(t, os.RemoveAll(filepath.Join(root, "o", "r.git")))
		require.NoError(t, origin.r.proposeRepoDelete(ctx, "o/r.git", 1))
		tc.createVia(origin, "o/r.git")
		require.True(t, tc.pushVia(origin, work, "o/r.git", "refs/heads/main", zero, c2).OK)
		require.NoError(t, os.Rename(filepath.Join(root, "o", "r.git"), filepath.Join(root, "o", "new.git")))
		require.NoError(t, origin.r.proposeRepoRename(ctx, "o/r.git", "o/new.git"))
		release()

		waitAll(t, tc, func(n *testNode) bool { return gitRef(t, n, "o/new.git", "refs/heads/main") == c2 })
		for _, n := range tc.nodes {
			assert.Empty(t, gitRef(t, n, "o/new.git", "refs/heads/secret"), "%s: the deleted repository's branch under o/new.git", n.slot)
			assert.False(t, hasObject(n, "o/new.git", c1), "%s: the deleted commit is in o/new.git", n.slot)
		}
		require.Eventually(t, func() bool {
			_, err := os.Stat(filepath.Join(lag.r.root, "o", "r.git"))
			return os.IsNotExist(err)
		}, 15*time.Second, 20*time.Millisecond, "the deleted repository's directory left o/r.git")
	})
}
