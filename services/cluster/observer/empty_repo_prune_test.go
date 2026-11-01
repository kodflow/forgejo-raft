// Copyright 2026 The Forgejo Authors. All rights reserved.
// SPDX-License-Identifier: GPL-3.0-or-later

package observer

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"forgejo.org/services/cluster/reftable"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// createEmpty creates repo in the cluster as git init leaves it, its HEAD
// alone, in format: a repository created without "Initialize repository"
// and never pushed to.
func (c *fakeCluster) createEmpty(repo, format string) {
	c.t.Helper()
	c.mu.Lock()
	defer c.mu.Unlock()
	u := reftable.RefUpdate{Ref: "HEAD", Symbolic: "refs/heads/main"}
	if v, ok := c.table.Get(repo, "HEAD"); ok {
		u.ExpectRev = v.Rev
	}
	res := c.append(reftable.KindRefTxn, &reftable.RefTxn{Repo: repo, Format: format, Init: true, Updates: []reftable.RefUpdate{u}})
	require.True(c.t, res.OK, res.Reason)
}

// pruneOld makes every checkpoint the observer holds prunable, then runs
// passes with new checkpoints of repo, none of which may fail: each lets
// go every protected checkpoint but the newest, directory included.
func pruneOld(t *testing.T, c *fakeCluster, o *Observer, repo string) {
	t.Helper()
	ctx := context.Background()
	held := len(o.State().Checkpoints)
	o.cfg.LiveKeep = 1
	for i := range 3 {
		c.push(repo, "refs/heads/main", c.commit(fmt.Sprintf("after the empty repository %d", i)))
		c.checkpoint()
		require.NoError(t, o.Step(ctx), "pass %d", i)
	}
	st := o.State()
	require.Len(t, st.Checkpoints, held+3)
	last := len(st.Checkpoints) - 1
	for _, a := range st.Checkpoints[:last] {
		assert.Equal(t, StatePruned, a.State, "checkpoint %s, protected and past LiveKeep, is let go", a.Name)
		_, err := os.Stat(o.store.path("checkpoints", a.Name))
		assert.True(t, os.IsNotExist(err), "the directory of the pruned checkpoint %s stays: %v", a.Name, err)
	}
	assert.Equal(t, StateProtected, st.Checkpoints[last].State, "the newest protected checkpoint stays")
}

// An empty repository is in every manifest, its symbolic HEAD and no ref
// (#114), but the observer never creates its store: there is nothing to
// fetch. prune dropped the refs of a checkpoint it let go in the store of
// every repository of the manifest: git ran in a directory that does not
// exist, and the prune failed on the oldest prunable checkpoint at every
// pass. Nothing was pruned any more, and the base renewal after it never
// ran again (#179): the observer's disk grew without bound (#379).
func TestObserverPrunesACheckpointHoldingAnEmptyRepository(t *testing.T) {
	ctx := context.Background()
	c := newFakeCluster(t)
	o := newTestObserver(t, c, &hookLog{})
	seed(t, c, o)
	c.createEmpty("bob/empty.git", "sha1")
	c.checkpoint()
	require.NoError(t, o.Step(ctx))
	first := o.State().Checkpoints[0].Name
	m, err := o.store.ReadManifest(first)
	require.NoError(t, err)
	require.Contains(t, m.Repos, "bob/empty.git", "the empty repository is in the manifest (#114)")
	require.Empty(t, m.Repos["bob/empty.git"].Refs)
	_, err = os.Stat(o.store.git.dirFor("bob/empty.git", m.Repos["bob/empty.git"].Format))
	require.True(t, os.IsNotExist(err), "the empty repository has no store: %v", err)

	pruneOld(t, c, o, "alice/demo.git")
	refs, err := o.store.git.refs(ctx, o.store.git.dir("alice/demo.git"), first)
	require.NoError(t, err)
	assert.Empty(t, refs, "the refs of the pruned checkpoint go")
}

// A repository already held in one object format, deleted and created
// again empty in the other: its store in that format, under .<format>/
// (#84), is never created, and the prune of a checkpoint naming it failed
// the same way (#379).
func TestObserverPrunesARepositoryRecreatedEmptyInTheOtherFormat(t *testing.T) {
	ctx := context.Background()
	c := newFakeCluster(t)
	o := newTestObserver(t, c, &hookLog{})
	seed(t, c, o)
	require.NoError(t, o.Step(ctx))
	before := o.State().Checkpoints[0].Name

	require.True(t, c.append(reftable.KindRepoDelete, &reftable.RepoDelete{Repo: "alice/demo.git", RepoID: 1}).OK)
	c.createEmpty("alice/demo.git", "sha256")
	cp := c.checkpoint()
	require.NoError(t, o.Step(ctx))
	m, err := o.store.ReadManifest(cp.Name())
	require.NoError(t, err)
	rm := m.Repos["alice/demo.git"]
	require.Equal(t, "sha256", rm.Format)
	require.Empty(t, rm.Refs)
	dir := o.store.git.dirFor("alice/demo.git", rm.Format)
	require.NotEqual(t, o.store.git.dir("alice/demo.git"), dir, "the sha1 store keeps its key")
	_, err = os.Stat(dir)
	require.True(t, os.IsNotExist(err), "no sha256 store: %v", err)

	pruneOld(t, c, o, "bob/other.git")
	refs, err := o.store.git.refs(ctx, o.store.git.dir("alice/demo.git"), before)
	require.NoError(t, err)
	assert.Empty(t, refs, "the refs of the pruned sha1 checkpoint go")
}

// dropRefs has nothing to remove from a store that was never created, but
// a store it cannot read is an error, which the next pass retries: the
// refs of a pruned checkpoint are never left behind silently.
func TestDropRefsSkipsOnlyAStoreThatDoesNotExist(t *testing.T) {
	ctx := context.Background()
	c := newFakeCluster(t)
	o := newTestObserver(t, c, &hookLog{})
	g := o.store.git
	name := "c1050000000000000000000000000000-1-00000000000000000009"
	require.NoError(t, g.dropRefs(ctx, g.dirFor("nobody/never.git", "sha1"), name))
	require.NoError(t, g.dropRefs(ctx, g.dirFor("nobody/never.git", "sha256"), name))

	file := filepath.Join(t.TempDir(), "a-file")
	require.NoError(t, os.WriteFile(file, nil, 0o600))
	require.Error(t, g.dropRefs(ctx, filepath.Join(file, "repo.git"), name), "a store under a file is no missing store")

	if os.Geteuid() != 0 {
		locked := filepath.Join(t.TempDir(), "locked")
		require.NoError(t, os.Mkdir(locked, 0o700))
		require.NoError(t, os.Chmod(locked, 0))
		t.Cleanup(func() { _ = os.Chmod(locked, 0o700) })
		require.Error(t, g.dropRefs(ctx, filepath.Join(locked, "repo.git"), name), "an unreadable store is no missing store")
	}
}
