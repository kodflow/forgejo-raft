// Copyright 2026 The Forgejo Authors. All rights reserved.
// SPDX-License-Identifier: GPL-3.0-or-later

package cluster

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"forgejo.org/modules/clusterhook"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// leaveLock writes what a git killed in the middle of a transaction leaves
// behind, an hour old.
func leaveLock(t *testing.T, path, content string) {
	t.Helper()
	require.NoError(t, os.MkdirAll(filepath.Dir(path), 0o755))
	require.NoError(t, os.WriteFile(path, []byte(content), 0o644))
	old := time.Now().Add(-time.Hour)
	require.NoError(t, os.Chtimes(path, old, old))
}

// A receive-pack killed in "prepared" leaves its ref's lock and HEAD's on
// one node. The projector must remove them once no git can own them, and
// the node converge and be ready again after a start (#48).
func TestALockLeftByAKilledGitDoesNotStopProjection(t *testing.T) {
	tc := newTestCluster(t, "node-a", "node-b", "node-c")
	work := t.TempDir()
	run(t, work, "init", "-q")
	c1 := commit(t, work, "one")
	c2 := commit(t, work, "two")
	zero := strings.Repeat("0", 40)
	origin, lag := tc.nodes[0], tc.nodes[2]
	require.True(t, tc.pushVia(origin, work, "alice/killed.git", "refs/heads/main", zero, c1).OK)
	require.Eventually(t, func() bool { return gitRef(t, lag, "alice/killed.git", "refs/heads/main") == c1 }, 10*time.Second, 20*time.Millisecond)

	dir := filepath.Join(lag.r.root, "alice", "killed.git")
	leaveLock(t, filepath.Join(dir, "refs", "heads", "main.lock"), c2+"\n")
	leaveLock(t, filepath.Join(dir, "HEAD.lock"), "")
	require.True(t, tc.pushVia(origin, work, "alice/killed.git", "refs/heads/main", c1, c2).OK)
	require.Eventually(t, func() bool { return gitRef(t, lag, "alice/killed.git", "refs/heads/main") == c2 }, 15*time.Second, 20*time.Millisecond, "the node converges on the table")
	for _, l := range []string{"HEAD.lock", "refs/heads/main.lock"} {
		_, err := os.Stat(filepath.Join(dir, filepath.FromSlash(l)))
		assert.True(t, os.IsNotExist(err), "%s removed", l)
	}
	lag.r.projector.unfence()
	require.Eventually(t, lag.r.projector.Ready, 10*time.Second, 20*time.Millisecond, "ready after a start")
}

// A pack-refs or update-ref killed while it rewrote packed-refs leaves
// packed-refs.lock and packed-refs.new: every deletion of a packed ref
// fails until both are gone (#48).
func TestAPackedRefsLockLeftBehindDoesNotStopADeletion(t *testing.T) {
	tc := newTestCluster(t, "node-a", "node-b", "node-c")
	work := t.TempDir()
	run(t, work, "init", "-q")
	c1 := commit(t, work, "one")
	zero := strings.Repeat("0", 40)
	origin, lag := tc.nodes[0], tc.nodes[2]
	require.True(t, tc.pushVia(origin, work, "alice/gc.git", "refs/heads/main", zero, c1).OK)
	require.True(t, tc.pushVia(origin, work, "alice/gc.git", "refs/heads/old", zero, c1).OK)
	require.Eventually(t, func() bool { return gitRef(t, lag, "alice/gc.git", "refs/heads/old") == c1 }, 10*time.Second, 20*time.Millisecond)

	dir := filepath.Join(lag.r.root, "alice", "gc.git")
	run(t, dir, "pack-refs", "--all")
	leaveLock(t, filepath.Join(dir, "packed-refs.lock"), "")
	leaveLock(t, filepath.Join(dir, "packed-refs.new"), "")

	// refs/heads/old deleted through node-a, as Forgejo deletes a branch.
	odir := filepath.Join(origin.r.root, "alice", "gc.git")
	req := &clusterhook.Request{
		Op: clusterhook.OpPrepared, Nonce: clusterhook.NewNonce(), GitDir: odir, RepoID: 1,
		Lines: []clusterhook.Line{{Old: c1, New: zero, Ref: "refs/heads/old"}},
	}
	resp := origin.r.Handle(t.Context(), req)
	require.True(t, resp.OK, resp.Message)
	run(t, odir, "update-ref", "-d", "refs/heads/old")
	req.Op = clusterhook.OpCommitted
	origin.r.Handle(t.Context(), req)

	require.Eventually(t, func() bool { return gitRef(t, lag, "alice/gc.git", "refs/heads/old") == "" }, 15*time.Second, 20*time.Millisecond, "the deletion reaches the node")
	lag.r.projector.unfence()
	require.Eventually(t, lag.r.projector.Ready, 10*time.Second, 20*time.Millisecond, "ready after a start")
}

// A lock younger than the bound, or one of a repository with a native
// transaction registered, may be git's own: it stays.
func TestAFreshLockIsLeftAlone(t *testing.T) {
	tc := newTestCluster(t, "node-a")
	n := tc.nodes[0]
	dir := filepath.Join(n.r.root, "alice", "fresh.git")
	run(t, n.r.root, "init", "-q", "--bare", dir)
	fresh := filepath.Join(dir, "packed-refs.lock")
	require.NoError(t, os.WriteFile(fresh, nil, 0o644))
	assert.Zero(t, n.r.projector.clearStaleLocks("alice/fresh.git", dir, nil, nil))
	assert.FileExists(t, fresh)

	leaveLock(t, fresh, "")
	require.NoError(t, n.r.registry.reserve("nonce", "alice/fresh.git", os.Getpid()))
	assert.Zero(t, n.r.projector.clearStaleLocks("alice/fresh.git", dir, nil, nil), "a git of this repository is registered")
	assert.FileExists(t, fresh)
	n.r.registry.release("nonce")
	assert.Equal(t, 1, n.r.projector.clearStaleLocks("alice/fresh.git", dir, nil, nil))
	assert.NoFileExists(t, fresh)
}
