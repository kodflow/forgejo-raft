// Copyright 2026 The Forgejo Authors. All rights reserved.
// SPDX-License-Identifier: GPL-3.0-or-later

package observer

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"forgejo.org/services/cluster/reftable"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// recreate deletes repo from the cluster and creates it again in format,
// with one commit on main.
func (c *fakeCluster) recreate(repo, format string) string {
	c.t.Helper()
	require.True(c.t, c.append(reftable.KindRepoDelete, &reftable.RepoDelete{Repo: repo, RepoID: 1}).OK)
	dir := filepath.Join(c.repos, filepath.FromSlash(repo))
	require.NoError(c.t, os.RemoveAll(dir))
	c.git(c.repos, "init", "-q", "--bare", "--object-format="+format, dir)
	work := c.t.TempDir()
	c.git(work, "init", "-q", "--object-format="+format)
	require.NoError(c.t, os.WriteFile(filepath.Join(work, "f.txt"), []byte(format), 0o644))
	c.git(work, "add", ".")
	c.git(work, "commit", "-q", "-m", "in "+format)
	sha := c.git(work, "rev-parse", "HEAD")
	c.git(work, "push", "-q", dir, "HEAD:refs/heads/main")
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, u := range []reftable.RefUpdate{{Ref: "HEAD", Symbolic: "refs/heads/main"}, {Ref: "refs/heads/main", New: sha}} {
		if v, ok := c.table.Get(repo, u.Ref); ok {
			u.ExpectRev = v.Rev
		}
		res := c.append(reftable.KindRefTxn, &reftable.RefTxn{Repo: repo, Format: format, Updates: []reftable.RefUpdate{u}})
		require.True(c.t, res.OK, res.Reason)
	}
	c.pg.write("branch "+repo+" refs/heads/main", sha)
	return sha
}

// A repository recreated in the other object format gets a store of its
// own: checkpoints after it are ingested, and the ones before still
// restore from theirs (#84).
func TestObserverFollowsARepositoryRecreatedInTheOtherFormat(t *testing.T) {
	ctx := context.Background()
	c := newFakeCluster(t)
	o := newTestObserver(t, c, &hookLog{})
	seed(t, c, o)
	require.NoError(t, o.Step(ctx))
	before := o.State().Checkpoints[0].Name

	sha := c.recreate("alice/demo.git", "sha256")
	cp := c.checkpoint()
	require.NoError(t, o.Step(ctx))
	st := o.State()
	require.Len(t, st.Checkpoints, 2)
	assert.Equal(t, cp.Name(), st.Checkpoints[1].Name)
	m, err := o.store.ReadManifest(cp.Name())
	require.NoError(t, err)
	assert.Equal(t, "sha256", m.Repos["alice/demo.git"].Format)

	a := &Archive{store: o.store}
	require.NoError(t, a.Check(ctx, before))
	require.NoError(t, a.Check(ctx, cp.Name()))
	target := filepath.Join(t.TempDir(), "demo.git")
	require.NoError(t, o.store.git.Materialize(ctx, "alice/demo.git", cp.Name(), target, "sha256", true, m.Repos["alice/demo.git"].Symbolic))
	assert.Equal(t, sha, c.git(target, "rev-parse", "refs/heads/main"))
	old := filepath.Join(t.TempDir(), "old.git")
	mb, err := o.store.ReadManifest(before)
	require.NoError(t, err)
	require.NoError(t, o.store.git.Materialize(ctx, "alice/demo.git", before, old, "sha1", true, mb.Repos["alice/demo.git"].Symbolic))
	assert.Equal(t, mb.Repos["alice/demo.git"].Refs["refs/heads/main"], c.git(old, "rev-parse", "refs/heads/main"))
}
