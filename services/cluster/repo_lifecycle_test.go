// Copyright 2026 The Forgejo Authors. All rights reserved.
// SPDX-License-Identifier: GPL-3.0-or-later

package cluster

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"forgejo.org/models/db"
	repo_model "forgejo.org/models/repo"
	"forgejo.org/models/unittest"
	user_model "forgejo.org/models/user"
	"forgejo.org/modules/clusterhook"
	"forgejo.org/modules/setting"
	"forgejo.org/services/cluster/reftable"
	repo_service "forgejo.org/services/repository"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func dirExists(n *testNode, key string) bool {
	_, err := os.Stat(filepath.Join(n.r.root, filepath.FromSlash(key)))
	return err == nil
}

// requireGoneEverywhere waits until every member deleted key and removed
// its directory.
func requireGoneEverywhere(t *testing.T, tc *testCluster, key string) {
	t.Helper()
	for _, n := range tc.nodes {
		require.Eventually(t, func() bool { _, deleted := n.r.table.Deleted(key); return deleted }, 10*time.Second, 20*time.Millisecond,
			"%s: %s deleted in the table", n.slot, key)
		require.Eventually(t, func() bool { return !dirExists(n, key) }, 10*time.Second, 20*time.Millisecond,
			"%s: %s removed", n.slot, key)
	}
}

// requireLiveEverywhere waits until every member holds key alive.
func requireLiveEverywhere(t *testing.T, tc *testCluster, key string) {
	t.Helper()
	for _, n := range tc.nodes {
		require.Eventually(t, func() bool { _, ok := n.r.table.Get(key, "HEAD"); return ok && dirExists(n, key) }, 10*time.Second, 20*time.Millisecond,
			"%s: %s projected", n.slot, key)
		_, deleted := n.r.table.Deleted(key)
		require.False(t, deleted, n.slot)
	}
}

// A deletion goes into the table before the files go. When the cluster
// does not record it, the deletion is refused with the files kept on every
// member, matching the table: removed here only, they came back from the
// other members, and a repository created again under the name inherited
// their refs (R-D4). reconcile reports the key SQL no longer has, and
// only `reconcile --delete-orphan` deletes it.
func TestRepositoryDeletionTheClusterDoesNotRecord(t *testing.T) {
	require.NoError(t, unittest.PrepareTestDatabase())
	tc := newTestCluster(t, "node-a", "node-b", "node-c")
	origin := tc.nodes[0]
	withGitThroughReplicator(t, origin)
	ctx := db.DefaultContext
	owner := unittest.AssertExistsAndLoadBean(t, &user_model.User{ID: 4})

	// With a quorum, the deletion is recorded and every member follows.
	kept, err := repo_service.CreateRepositoryDirectly(ctx, owner, owner, repo_service.CreateRepoOptions{Name: "kept"})
	require.NoError(t, err)
	requireLiveEverywhere(t, tc, "user4/kept.git")
	require.NoError(t, repo_service.DeleteRepositoryDirectly(ctx, kept.ID, repo_service.DeleteRepositoryOpts{}))
	requireGoneEverywhere(t, tc, "user4/kept.git")

	doomed, err := repo_service.CreateRepositoryDirectly(ctx, owner, owner, repo_service.CreateRepoOptions{Name: "doomed"})
	require.NoError(t, err)
	key := "user4/doomed.git"
	requireLiveEverywhere(t, tc, key)

	// Without a quorum, the deletion is refused and nothing is removed.
	tc.mem.SetDown(tc.nodes[1].node, true)
	tc.mem.SetDown(tc.nodes[2].node, true)
	dctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	err = repo_service.DeleteRepositoryDirectly(dctx, doomed.ID, repo_service.DeleteRepositoryOpts{})
	cancel()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "--delete-orphan")
	unittest.AssertNotExistsBean(t, &repo_model.Repository{ID: doomed.ID})
	tc.mem.SetDown(tc.nodes[1].node, false)
	tc.mem.SetDown(tc.nodes[2].node, false)
	origin.r.projector.markDirty(key)
	time.Sleep(200 * time.Millisecond)
	requireLiveEverywhere(t, tc, key)

	// reconcile only reports it, even with apply: after a loss on the SQL
	// side the table is the repository's only copy.
	orphan := func() Mismatch {
		ms, err := origin.r.Reconcile(ctx, true)
		require.NoError(t, err)
		for _, m := range ms {
			if m.Repo == key {
				return m
			}
		}
		require.Fail(t, "not reported", "%v", ms)
		return Mismatch{}
	}
	m := orphan()
	assert.Equal(t, RepoLifecycle, m.Category)
	assert.False(t, m.Applied)
	assert.Contains(t, m.Action, "adopting it")
	assert.Contains(t, m.Action, "--delete-orphan")
	assert.NotZero(t, m.Index)
	assert.NotEmpty(t, m.Since)
	assert.Equal(t, m.Since, orphan().Since, "unchanged since")
	requireLiveEverywhere(t, tc, key)

	// The explicit deletion: refused at another index, without a reason,
	// then done on every member and kept in the audit.
	del := func(index int64, reason string) *clusterhook.Response {
		return origin.r.Handle(ctx, &clusterhook.Request{Op: clusterhook.OpReconcile, Repo: "user4/doomed", Index: index, Reason: reason, Operator: "tester"})
	}
	resp := del(int64(m.Index)-1, "deletion not recorded")
	require.False(t, resp.OK)
	assert.Contains(t, resp.Message, "changed at index")
	resp = del(int64(m.Index), " ")
	require.False(t, resp.OK)
	assert.Contains(t, resp.Message, "reason is required")
	requireLiveEverywhere(t, tc, key)
	resp = del(int64(m.Index), "deletion not recorded")
	require.True(t, resp.OK, resp.Message)
	requireGoneEverywhere(t, tc, key)
	found := false
	for _, a := range origin.r.table.Audit() {
		if a.Action == string(ActionDeleteOrphan) && a.Target == key && a.Operator == "tester" {
			found = true
			assert.Equal(t, "deleted", a.Outcome)
			assert.Contains(t, a.Detail, "deletion not recorded")
		}
	}
	assert.True(t, found, "audited")
}

// A repository SQL lost (a PostgreSQL failover, a restore) looks like an
// orphan: it is restored by adoption, from the files every member kept,
// and reconcile no longer reports it. DeleteOrphan refuses a repository
// SQL knows.
func TestOrphanIsRestoredByAdoption(t *testing.T) {
	require.NoError(t, unittest.PrepareTestDatabase())
	tc := newTestCluster(t, "node-a", "node-b", "node-c")
	origin := tc.nodes[0]
	withGitThroughReplicator(t, origin)
	ctx := db.DefaultContext
	owner := unittest.AssertExistsAndLoadBean(t, &user_model.User{ID: 4})
	admin := unittest.AssertExistsAndLoadBean(t, &user_model.User{ID: 1})

	repo, err := repo_service.CreateRepositoryDirectly(ctx, owner, owner, repo_service.CreateRepoOptions{Name: "lost", AutoInit: true, Readme: "Default", DefaultBranch: "main"})
	require.NoError(t, err)
	key := "user4/lost.git"
	requireLiveEverywhere(t, tc, key)
	main, ok := origin.r.table.Get(key, "refs/heads/main")
	require.True(t, ok)

	// SQL loses the row, the files stay.
	_, err = db.GetEngine(ctx).ID(repo.ID).Delete(new(repo_model.Repository))
	require.NoError(t, err)
	ms, err := origin.r.Reconcile(ctx, true)
	require.NoError(t, err)
	var index uint64
	for _, m := range ms {
		if m.Repo == key {
			index = m.Index
		}
	}
	require.NotZero(t, index, "reported: %v", ms)
	requireLiveEverywhere(t, tc, key)

	adopted, err := repo_service.AdoptRepository(ctx, admin, owner, repo_service.CreateRepoOptions{Name: "lost"})
	require.NoError(t, err)
	ms, err = origin.r.Reconcile(ctx, true)
	require.NoError(t, err)
	for _, m := range ms {
		assert.NotEqual(t, RepoLifecycle, m.Category, "%+v", m)
	}
	for _, n := range tc.nodes {
		v, ok := n.r.table.Get(key, "refs/heads/main")
		require.True(t, ok, n.slot)
		assert.Equal(t, main.SHA, v.SHA, n.slot)
	}
	assert.Equal(t, "main", adopted.DefaultBranch)

	resp := origin.r.Handle(ctx, &clusterhook.Request{Op: clusterhook.OpReconcile, Repo: "user4/lost", Index: int64(index), Reason: "x", Operator: "tester"})
	require.False(t, resp.OK)
	requireLiveEverywhere(t, tc, key)
}

// A creation that fails after `git init` takes its key out of the table:
// every member had created the directory from HEAD, and kept the name
// taken (R-D5).
func TestFailedCreationLeavesNoLiveKey(t *testing.T) {
	require.NoError(t, unittest.PrepareTestDatabase())
	tc := newTestCluster(t, "node-a", "node-b", "node-c")
	withGitThroughReplicator(t, tc.nodes[0])
	owner := unittest.AssertExistsAndLoadBean(t, &user_model.User{ID: 4})

	_, err := repo_service.CreateRepositoryDirectly(db.DefaultContext, owner, owner, repo_service.CreateRepoOptions{
		Name: "half", AutoInit: true, Readme: "no-such-readme",
	})
	require.Error(t, err)
	unittest.AssertNotExistsBean(t, &repo_model.Repository{OwnerID: 4, LowerName: "half"})
	requireGoneEverywhere(t, tc, "user4/half.git")

	// The name is free again, on every member.
	_, err = repo_service.CreateRepositoryDirectly(db.DefaultContext, owner, owner, repo_service.CreateRepoOptions{Name: "half"})
	require.NoError(t, err)
	requireLiveEverywhere(t, tc, "user4/half.git")
}

// A fork that fails after `git init` takes its key out of the table too.
func TestFailedForkLeavesNoLiveKey(t *testing.T) {
	require.NoError(t, unittest.PrepareTestDatabase())
	tc := newTestCluster(t, "node-a", "node-b", "node-c")
	for _, n := range tc.nodes {
		n.r.objects.repoKnown = n.r.repoKnown
	}
	origin := tc.nodes[0]
	require.NoError(t, unittest.CopyDir(filepath.Join(setting.RepoRootPath, "user2", "repo1.git"), filepath.Join(origin.r.root, "user2", "repo1.git")))
	withGitThroughReplicator(t, origin)
	ctx := db.DefaultContext
	base := unittest.AssertExistsAndLoadBean(t, &repo_model.Repository{ID: 1})
	require.NoError(t, base.LoadOwner(ctx))
	owner := unittest.AssertExistsAndLoadBean(t, &user_model.User{ID: 4})

	_, err := repo_service.ForkRepositoryIfNotExists(ctx, owner, owner, repo_service.ForkRepoOptions{BaseRepo: base, Name: "broken-fork", SingleBranch: "no-such-branch"})
	require.Error(t, err)
	requireGoneEverywhere(t, tc, "user4/broken-fork.git")
	unittest.AssertNotExistsBean(t, &repo_model.Repository{OwnerID: 4, LowerName: "broken-fork"})
	assert.Equal(t, base.NumForks, unittest.AssertExistsAndLoadBean(t, &repo_model.Repository{ID: 1}).NumForks, "the base does not count the failed fork (#177)")
}

// A wiki the table holds alone is an orphan of its repository's name when
// SQL has no such repository; a wiki of a known repository is not.
func TestReconcileFindsAnOrphanedWiki(t *testing.T) {
	require.NoError(t, unittest.PrepareTestDatabase())
	r := newReplicator(t.TempDir(), newRegistry(0, 1, 1))
	const sha = "65f1bf27bc3bf70f64657658635e66094edbcb4d"
	for i, key := range []string{"ghost/x.wiki.git", "user2/repo1.wiki.git"} {
		applyAt(t, r.table, uint64(i+1), reftable.KindRefTxn, &reftable.RefTxn{Repo: key, Format: "sha1", Updates: []reftable.RefUpdate{{Ref: "refs/heads/main", New: sha}}})
	}
	ms, err := r.Reconcile(db.DefaultContext, false)
	require.NoError(t, err)
	var orphans []string
	for _, m := range ms {
		if m.Category == RepoLifecycle {
			orphans = append(orphans, m.Repo)
			assert.Contains(t, m.Action, "SQL has no row")
			assert.False(t, m.Applied)
		}
	}
	assert.Equal(t, []string{"ghost/x.git"}, orphans)
}
