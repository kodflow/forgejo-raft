// Copyright 2026 The Forgejo Authors. All rights reserved.
// SPDX-License-Identifier: GPL-3.0-or-later

package cluster

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"forgejo.org/modules/clusterhook"
	"forgejo.org/modules/setting"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// heldGitInit starts Forgejo's git init of dir, as git.InitRepository runs
// it: the directory first, then git init in it without a template. Its
// reference-transaction hook holds it at "prepared", where Forgejo's hook
// asks the replicator: config, refs and HEAD.lock are written, HEAD is
// not. It returns once git is held; finish lets git go on and waits for
// it. Everything lies under dir and t.TempDir().
func heldGitInit(t *testing.T, dir string) (finish func() error) {
	t.Helper()
	gate := t.TempDir()
	hooks := filepath.Join(gate, "hooks")
	require.NoError(t, os.MkdirAll(hooks, 0o755))
	held, goOn := filepath.Join(gate, "held"), filepath.Join(gate, "go")
	script := fmt.Sprintf("#!/bin/sh\ncat >/dev/null\n[ \"$1\" = prepared ] || exit 0\n: > '%s'\ni=0\nwhile [ ! -e '%s' ] && [ $i -lt 600 ]; do sleep 0.05; i=$((i+1)); done\n", held, goOn)
	require.NoError(t, os.WriteFile(filepath.Join(hooks, "reference-transaction"), []byte(script), 0o755))

	require.NoError(t, os.MkdirAll(dir, 0o755))
	cmd := exec.Command("git", "-c", "core.hooksPath="+hooks, "-c", "init.defaultBranch=main", "init", "-q", "--bare", "--template", "")
	cmd.Dir = dir
	cmd.Env = append(os.Environ(), "HOME="+setting.Git.HomePath)
	var out bytes.Buffer
	cmd.Stdout, cmd.Stderr = &out, &out
	require.NoError(t, cmd.Start())
	done := make(chan error, 1)
	go func() { done <- cmd.Wait() }()
	var result error
	ended := false
	finish = func() error {
		if ended {
			return result
		}
		ended = true
		if err := os.WriteFile(goOn, nil, 0o644); err != nil {
			return err
		}
		if err := <-done; err != nil {
			result = fmt.Errorf("git init: %w: %s", err, out.String())
		}
		return result
	}
	t.Cleanup(func() { _ = finish() })
	require.Eventually(t, func() bool { _, err := os.Stat(held); return err == nil }, 10*time.Second, 10*time.Millisecond, "git init reaches its hook")
	return finish
}

// deleteOnOrigin deletes key through origin, as Forgejo does, while the
// origin's projector is held off the key: its pass of the deletion comes
// late, as under load. It returns the release of the hold.
func deleteOnOrigin(t *testing.T, origin *testNode, key string) func() {
	t.Helper()
	release := holdOn(t, origin, key)
	require.NoError(t, os.RemoveAll(filepath.Join(origin.r.root, filepath.FromSlash(key))))
	require.NoError(t, origin.r.proposeRepoDelete(context.Background(), key, 1))
	_, deleted := origin.r.table.Deleted(key)
	require.True(t, deleted, "the deletion is applied on the origin")
	return release
}

// Forgejo creates a repository again under a name it just deleted, on the
// node that deleted it, before that node's projector passed the deletion.
// git init writes config, refs and HEAD.lock, then waits in its hook at
// "prepared": until the hook registered the transaction, nothing fenced
// the key, and the pass of the deletion removed the directory under git
// init, which failed the creation (#391). git init is held in its hook
// here while the pass runs; the creation then goes through, and the
// deleted repository's objects are nowhere in the new one.
func TestTheDeletionsPassLeavesAGitInitUnderTheFreedName(t *testing.T) {
	tc := newTestCluster(t, "node-a", "node-b", "node-c")
	work := t.TempDir()
	run(t, work, "init", "-q")
	c1 := commit(t, work, "one")
	run(t, work, "checkout", "-q", "--orphan", "fresh")
	c2 := commit(t, work, "two")
	zero := strings.Repeat("0", 40)
	origin := tc.nodes[0]
	const key = "s/z.git"
	require.True(t, tc.pushVia(origin, work, key, "refs/heads/main", zero, c1).OK)
	waitAll(t, tc, func(n *testNode) bool { return gitRef(t, n, key, "refs/heads/main") == c1 })

	release := deleteOnOrigin(t, origin, key)
	dir := filepath.Join(origin.r.root, "s", "z.git")
	finish := heldGitInit(t, dir)
	release()
	ctx := context.Background()
	origin.r.projector.pass(ctx)
	require.DirExists(t, dir, "the pass of the deletion removed the directory git init writes under the freed name")

	// The hook proposes git init's HEAD, and git goes on.
	req := &clusterhook.Request{
		Op: clusterhook.OpPrepared, Nonce: clusterhook.NewNonce(), GitDir: dir,
		Lines: []clusterhook.Line{{Old: zero, New: "ref:refs/heads/main", Ref: "HEAD"}},
	}
	resp := origin.r.Handle(ctx, req)
	require.True(t, resp.OK, resp.Message)
	require.NoError(t, finish())
	req.Op = clusterhook.OpCommitted
	origin.r.Handle(ctx, req)

	require.True(t, tc.pushVia(origin, work, key, "refs/heads/main", zero, c2).OK)
	waitAll(t, tc, func(n *testNode) bool { return gitRef(t, n, key, "refs/heads/main") == c2 })
	for _, n := range tc.nodes {
		assert.False(t, hasObject(n, key, c1), "%s: the deleted commit is in the new %s", n.slot, key)
	}
	settle(t, origin)
}

// interruptedInit leaves under dir what git init leaves in its hook at
// "prepared", config, refs and HEAD.lock, changed at mtime.
func interruptedInit(t *testing.T, dir string, mtime time.Time) {
	t.Helper()
	require.NoError(t, os.MkdirAll(filepath.Join(dir, "refs", "heads"), 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(dir, "config"), []byte("[core]\n\tbare = true\n"), 0o644))
	require.NoError(t, os.WriteFile(filepath.Join(dir, "HEAD.lock"), []byte("ref: refs/heads/main\n"), 0o644))
	require.NoError(t, os.Chtimes(dir, mtime, mtime))
}

// The pass of a deletion leaves only a directory git init may still be
// writing. A git init that never reached its hook, killed or abandoned,
// is removed once it aged past the registration's TTL and staleLockAge,
// or when its time lies further ahead, and the deletion is projected. A
// whole repository under the deleted key goes at once, however fresh: it
// is no git init of Forgejo's, whose HEAD reaches the hook first.
func TestTheDeletionsPassRemovesAnythingButAGitInitInProgress(t *testing.T) {
	tc := newTestCluster(t, "node-a", "node-b", "node-c")
	work := t.TempDir()
	run(t, work, "init", "-q")
	c1 := commit(t, work, "one")
	zero := strings.Repeat("0", 40)
	origin := tc.nodes[0]
	const key = "s/z.git"
	require.True(t, tc.pushVia(origin, work, key, "refs/heads/main", zero, c1).OK)
	waitAll(t, tc, func(n *testNode) bool { return gitRef(t, n, key, "refs/heads/main") == c1 })

	release := deleteOnOrigin(t, origin, key)
	dir := filepath.Join(origin.r.root, "s", "z.git")
	interruptedInit(t, dir, time.Now())
	release()
	ctx := context.Background()
	origin.r.projector.pass(ctx)
	require.DirExists(t, dir, "a directory git init may still be writing is left")
	deletedAt, _ := origin.r.table.Deleted(key)
	assert.Less(t, origin.r.projector.Projected(key), deletedAt, "the deletion is not projected while the directory is left")

	window := max(origin.r.registry.ttl, staleLockAge)
	aged := time.Now().Add(-2 * window)
	require.NoError(t, os.Chtimes(dir, aged, aged))
	origin.r.projector.pass(ctx)
	require.NoDirExists(t, dir, "an interrupted git init is removed once it aged")
	settle(t, origin)
	for _, n := range tc.nodes {
		require.Eventually(t, func() bool {
			_, err := os.Stat(filepath.Join(n.r.root, "s", "z.git"))
			return os.IsNotExist(err)
		}, 10*time.Second, 20*time.Millisecond, "%s removed the deleted repository", n.slot)
	}

	interruptedInit(t, dir, time.Now().Add(2*window))
	origin.r.projector.markDirty(key)
	origin.r.projector.pass(ctx)
	require.NoDirExists(t, dir, "an interrupted git init whose time lies ahead is removed")

	run(t, origin.r.root, "init", "-q", "--bare", dir)
	origin.r.projector.markDirty(key)
	origin.r.projector.pass(ctx)
	require.NoDirExists(t, dir, "a whole repository under the deleted key is removed at once")
	settle(t, origin)
}
