// Copyright 2026 The Forgejo Authors. All rights reserved.
// SPDX-License-Identifier: GPL-3.0-or-later

package cluster

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"forgejo.org/models/db"
	git_model "forgejo.org/models/git"
	repo_model "forgejo.org/models/repo"
	"forgejo.org/models/unittest"
	"forgejo.org/modules/clusterhook"
	"forgejo.org/modules/lfs"
	"forgejo.org/modules/setting"
	"forgejo.org/modules/storage"
	"forgejo.org/modules/test"
	"forgejo.org/modules/timeutil"
	"forgejo.org/services/cluster/consensus/memraft"
	repo_service "forgejo.org/services/repository"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// The LFS garbage collection judges an LFS object orphaned when its pointer
// blob is missing from the git of the node that runs it, then deletes the
// object's row from the SQL and its file from the storage every member
// shares (#349). These tests run it as the gc_lfs cron task and its "Run"
// button do, with the stricter of their options (7 days, 3 days), on a
// node whose git lacks a pointer the cluster serves.

// lfsGCBench is an LFS storage of the test's own, and the object's row and
// file, uploaded eight days ago, in the repository laggingRepo (ID 1).
type lfsGCBench struct {
	t     *testing.T
	ctx   context.Context
	store *lfs.ContentStore
	repo  *repo_model.Repository
	opts  repo_service.GarbageCollectLFSMetaObjectsOptions
	old   timeutil.TimeStamp
}

func newLFSGCBench(t *testing.T) *lfsGCBench {
	t.Helper()
	lfsStorage := &setting.Storage{Type: setting.LocalStorageType, Path: filepath.Join(t.TempDir(), "lfs")}
	t.Cleanup(test.MockVariableValue(&setting.LFS.Storage, lfsStorage))
	t.Cleanup(test.MockVariableValue(&setting.LFS.StartServer, true))
	local, err := storage.NewStorage(setting.LocalStorageType, lfsStorage)
	require.NoError(t, err)
	t.Cleanup(test.MockVariableValue(&storage.LFS, local))
	ctx := db.DefaultContext
	repo, err := repo_model.GetRepositoryByID(ctx, 1)
	require.NoError(t, err)
	return &lfsGCBench{
		t: t, ctx: ctx, store: lfs.NewContentStore(), repo: repo,
		// services/cron/tasks_extended.go registerGCLFS: what the cron
		// task and the dashboard's "Run" button pass.
		opts: repo_service.GarbageCollectLFSMetaObjectsOptions{
			LogDetail:               t.Logf,
			AutoFix:                 true,
			OlderThan:               time.Now().Add(-24 * time.Hour * 7),
			UpdatedLessRecentlyThan: time.Now().Add(-24 * time.Hour * 3),
		},
		old: timeutil.TimeStamp(time.Now().Add(-8 * 24 * time.Hour).Unix()),
	}
}

// object stores content's file and row, eight days old.
func (b *lfsGCBench) object(content []byte) (lfs.Pointer, *git_model.LFSMetaObject) {
	b.t.Helper()
	pointer, err := lfs.GeneratePointer(bytes.NewReader(content))
	require.NoError(b.t, err)
	require.NoError(b.t, b.store.Put(pointer, bytes.NewReader(content)))
	meta, err := git_model.NewLFSMetaObject(b.ctx, b.repo.ID, pointer)
	require.NoError(b.t, err)
	b.backdate(meta)
	return pointer, meta
}

// backdate sets the row eight days back, in raw SQL: xorm leaves a
// `created` column out of an Update, and an aging through xorm would let
// the test pass for nothing.
func (b *lfsGCBench) backdate(meta *git_model.LFSMetaObject) {
	b.t.Helper()
	_, err := db.GetEngine(b.ctx).Exec("UPDATE lfs_meta_object SET created_unix = ?, updated_unix = ? WHERE id = ?", b.old, b.old, meta.ID)
	require.NoError(b.t, err)
	got := new(git_model.LFSMetaObject)
	has, err := db.GetEngine(b.ctx).ID(meta.ID).Get(got)
	require.NoError(b.t, err)
	require.True(b.t, has)
	require.Equal(b.t, b.old, got.CreatedUnix)
	require.Equal(b.t, b.old, got.UpdatedUnix)
}

// exists reports whether the object's row and file are still there.
func (b *lfsGCBench) exists(pointer lfs.Pointer, meta *git_model.LFSMetaObject) (row, file bool) {
	b.t.Helper()
	row, err := db.GetEngine(b.ctx).ID(meta.ID).Exist(new(git_model.LFSMetaObject))
	require.NoError(b.t, err)
	file, err = b.store.Exists(pointer)
	require.NoError(b.t, err)
	return row, file
}

// marked reports whether a collection examined the row and found its
// pointer (MarkLFSMetaObject).
func (b *lfsGCBench) marked(meta *git_model.LFSMetaObject) bool {
	b.t.Helper()
	got := new(git_model.LFSMetaObject)
	_, err := db.GetEngine(b.ctx).ID(meta.ID).Get(got)
	require.NoError(b.t, err)
	return got.UpdatedUnix > b.old
}

// collectOn runs the collection of the repository on node n, as the node
// that serves the cron task: its repository root, and its replicator's
// gates, which Init registers in the process.
func (b *lfsGCBench) collectOn(n *testNode) {
	b.t.Helper()
	defer test.MockVariableValue(&setting.RepoRootPath, n.r.root)()
	defer clusterhook.SetEnabled(clusterhook.Enabled())
	clusterhook.SetEnabled(true)
	clusterhook.SetGarbageCollectionGate(n.r.projector.MayCollectGarbage)
	clusterhook.SetFreshWaiter(n.r.WaitFresh)
	defer func() {
		clusterhook.SetGarbageCollectionGate(nil)
		clusterhook.SetFreshWaiter(nil)
	}()
	require.NoError(b.t, repo_service.GarbageCollectLFSMetaObjectsForRepo(b.ctx, b.repo, b.opts))
}

// trimFollowsCursor makes the trim follow the consumer's cursor, set
// before the replicators run and restored after they stopped: a push's
// event no longer pins its objects once its SQL is derived, and the gate
// opens on a node with no work pending on the repository. Production
// trims AckMargin entries behind, ackEvery at a time.
func trimFollowsCursor(t *testing.T) {
	t.Helper()
	t.Cleanup(test.MockVariableValue(&setting.Cluster.AckMargin, 0))
	t.Cleanup(test.MockVariableValue(&ackEvery, 1))
}

// pushPointer commits an LFS pointer of content in work and returns the
// commit and the pointer's blob.
func pushPointer(t *testing.T, work string, content []byte) (commitSHA, blob string) {
	t.Helper()
	pointer, err := lfs.GeneratePointer(bytes.NewReader(content))
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(filepath.Join(work, "asset.bin"), []byte(pointer.StringContent()), 0o644))
	run(t, work, "add", "asset.bin")
	commitSHA = commit(t, work, "an LFS pointer")
	return commitSHA, strings.TrimSpace(run(t, work, "rev-parse", commitSHA+":asset.bin"))
}

// A node that projects a repository again (new member, replaced disk,
// `forgejo cluster reproject`) inits its directory first and fetches its
// objects afterwards: the gc_lfs task served by that node meanwhile found
// the pointer missing, and deleted for the whole cluster the row and the
// file of an object every node serves again once the fetch ends (#349).
func TestLFSGarbageCollectionSkipsARepositoryANodeProjectsAgain(t *testing.T) {
	require.NoError(t, unittest.PrepareTestDatabase())
	bench := newLFSGCBench(t)
	trimFollowsCursor(t)

	gate := make(chan struct{})
	var gateClosed atomic.Bool
	var blocking atomic.Bool
	release := func() {
		if gateClosed.CompareAndSwap(false, true) {
			close(gate)
		}
	}
	t.Cleanup(release)
	tc := newTestClusterWith(t, memraft.New().ID(), func(tc *testCluster, pins map[string][32]byte) {
		for _, n := range tc.nodes {
			n.node = tc.mem.Add(n.slot, pins[n.slot], n.r.table)
			n.r.setEngine(n.node)
		}
		a := tc.nodes[0]
		inner := a.r.projector.fetch
		// node-a's fetch of the missing objects, held: one still in
		// flight, or queued behind maxFetches others.
		a.r.projector.fetch = func(ctx context.Context, repo, dir string, shas []string) error {
			if blocking.Load() {
				select {
				case <-gate:
				case <-ctx.Done():
					return ctx.Err()
				}
			}
			return inner(ctx, repo, dir, shas)
		}
	}, "node-a", "node-b", "node-c")
	a, b := tc.nodes[0], tc.nodes[1]

	// One LFS pointer, pushed through node-b, committed and projected on
	// every node.
	content := []byte("an LFS object whose pointer every node of the cluster serves\n")
	work := t.TempDir()
	run(t, work, "init", "-q")
	c1, pointerBlob := pushPointer(t, work, content)
	zero := strings.Repeat("0", 40)
	require.Equal(t, int64(1), tc.repoID(laggingRepo))
	require.True(t, tc.pushVia(b, work, laggingRepo, "refs/heads/main", zero, c1).OK)
	for _, m := range tc.nodes {
		require.Eventually(t, func() bool { return gitRef(t, m, laggingRepo, "refs/heads/main") == c1 },
			15*time.Second, 20*time.Millisecond, "%s: main at c1", m.slot)
	}
	pointer, meta := bench.object(content)

	// Control: on node-b, which holds the pointer, the row is examined and
	// kept. The run marks it; it is set back, as no node marks it when
	// gc_lfs is not scheduled (the default).
	require.Eventually(t, func() bool { return b.r.projector.MayCollectGarbage(laggingRepo) },
		15*time.Second, 20*time.Millisecond, "node-b has no work pending on the repository")
	bench.collectOn(b)
	row, file := bench.exists(pointer, meta)
	require.True(t, row && file, "control: node-b holds the pointer, the object is kept")
	require.True(t, bench.marked(meta), "control: node-b examined the row and found its pointer")
	bench.backdate(meta)

	// node-a projects the repository again: init first, fetch after.
	blocking.Store(true)
	dir := filepath.Join(a.r.root, filepath.FromSlash(laggingRepo))
	require.NoError(t, os.RemoveAll(dir))
	matched, err := a.r.projector.Verify(bench.ctx, laggingRepo)
	require.NoError(t, err)
	require.False(t, matched)
	require.Eventually(t, func() bool {
		_, err := os.Stat(filepath.Join(dir, "HEAD"))
		return err == nil && a.r.projector.isFetching(laggingRepo)
	}, 15*time.Second, 20*time.Millisecond, "node-a inits the directory and fetches its objects")
	t.Logf("node-a: %s initialised, fetch in flight, main=%q, MayCollectGarbage=%v",
		laggingRepo, gitRef(t, a, laggingRepo, "refs/heads/main"), a.r.projector.MayCollectGarbage(laggingRepo))

	// The LFS collection, served by node-a.
	bench.collectOn(a)
	row, file = bench.exists(pointer, meta)
	t.Logf("after gc_lfs on node-a: lfs_meta_object row=%v, file in the shared LFS storage=%v", row, file)

	// node-a's fetch ends: every node serves the pointer again.
	blocking.Store(false)
	release()
	for _, m := range tc.nodes {
		require.Eventually(t, func() bool { return gitRef(t, m, laggingRepo, "refs/heads/main") == c1 },
			15*time.Second, 20*time.Millisecond, "%s: main at c1", m.slot)
		has := strings.TrimSpace(run(t, filepath.Join(m.r.root, filepath.FromSlash(laggingRepo)), "cat-file", "-t", pointerBlob))
		require.Equal(t, "blob", has, "%s holds the pointer blob", m.slot)
	}
	row, file = bench.exists(pointer, meta)
	t.Logf("every node holds pointer blob %.10s at main: row=%v, file=%v", pointerBlob, row, file)
	assert.True(t, row, "the row of an LFS object whose pointer every node serves was deleted for the whole cluster")
	assert.True(t, file, "the file of that LFS object was deleted from the storage every node shares")
}

// A node cut off from the cluster (stopped, partitioned) does not hold
// what the cluster committed meanwhile, and its projector has nothing
// pending: nothing applied, nothing dirty. The gc_lfs task served by that
// node found the pointer of a later commit missing, and deleted the
// object for the whole cluster (#349). Once the node serves the
// repository at the cluster's last writes, the collection runs again: a
// real orphan is collected, the live object kept.
func TestLFSGarbageCollectionSkipsARepositoryANodeIsBehindOn(t *testing.T) {
	require.NoError(t, unittest.PrepareTestDatabase())
	bench := newLFSGCBench(t)
	trimFollowsCursor(t)

	tc := newTestClusterWith(t, memraft.New().ID(), func(tc *testCluster, pins map[string][32]byte) {
		for _, n := range tc.nodes {
			n.node = tc.mem.Add(n.slot, pins[n.slot], n.r.table)
			n.r.setEngine(n.node)
		}
		// node-a takes no shipment: what it holds, it fetched once its
		// table applied the write, as a node that was away does.
		tc.nodes[0].r.objects.repoKnown = func(context.Context, string) (bool, error) { return false, nil }
	}, "node-a", "node-b", "node-c")
	a, b := tc.nodes[0], tc.nodes[1]
	zero := strings.Repeat("0", 40)

	// The repository on every node, without the pointer yet.
	work := t.TempDir()
	run(t, work, "init", "-q")
	c0 := commit(t, work, "before the LFS object")
	require.Equal(t, int64(1), tc.repoID(laggingRepo))
	require.True(t, tc.pushVia(b, work, laggingRepo, "refs/heads/main", zero, c0).OK)
	for _, m := range tc.nodes {
		require.Eventually(t, func() bool { return gitRef(t, m, laggingRepo, "refs/heads/main") == c0 },
			15*time.Second, 20*time.Millisecond, "%s: main at c0", m.slot)
	}

	// node-a has no work pending on the repository: its git gc would
	// run. It is cut off then; the pointer is pushed through node-b and
	// committed by node-b and node-c.
	require.Eventually(t, func() bool { return a.r.projector.MayCollectGarbage(laggingRepo) },
		15*time.Second, 20*time.Millisecond, "node-a has no work pending on the repository")
	tc.mem.SetDown(a.node, true)
	content := []byte("an LFS object pushed while node-a was away\n")
	c1, pointerBlob := pushPointer(t, work, content)
	require.True(t, tc.pushVia(b, work, laggingRepo, "refs/heads/main", c0, c1).OK)
	pointer, meta := bench.object(content)
	_, err := gitIn(filepath.Join(a.r.root, filepath.FromSlash(laggingRepo)), "cat-file", "-e", pointerBlob)
	require.Error(t, err, "node-a lacks the pointer blob")
	t.Logf("node-a: main=%.10s, pointer blob %.10s missing, MayCollectGarbage=%v",
		gitRef(t, a, laggingRepo, "refs/heads/main"), pointerBlob, a.r.projector.MayCollectGarbage(laggingRepo))

	// The LFS collection, served by node-a.
	bench.collectOn(a)
	row, file := bench.exists(pointer, meta)
	t.Logf("after gc_lfs on node-a: lfs_meta_object row=%v, file in the shared LFS storage=%v", row, file)

	// node-a comes back and projects c1.
	tc.mem.SetDown(a.node, false)
	require.Eventually(t, func() bool { return gitRef(t, a, laggingRepo, "refs/heads/main") == c1 },
		15*time.Second, 20*time.Millisecond, "node-a: main at c1")
	row, file = bench.exists(pointer, meta)
	assert.True(t, row, "the row of an LFS object the cluster serves was deleted for the whole cluster")
	assert.True(t, file, "the file of that LFS object was deleted from the storage every node shares")
	if !row || !file {
		return
	}

	// node-a serves the repository at the cluster's last writes: the
	// collection runs, a real orphan goes, the live object stays.
	orphanPointer, orphan := bench.object([]byte("an LFS object no commit names\n"))
	require.Eventually(t, func() bool { return a.r.projector.MayCollectGarbage(laggingRepo) },
		15*time.Second, 20*time.Millisecond, "node-a has no work pending on the repository")
	bench.collectOn(a)
	row, file = bench.exists(pointer, meta)
	assert.True(t, row && file, "the live object is kept")
	assert.True(t, bench.marked(meta), "node-a examined the live row and found its pointer")
	row, file = bench.exists(orphanPointer, orphan)
	assert.False(t, row, "the orphan's row is collected")
	assert.False(t, file, "the orphan's file is collected")
}
