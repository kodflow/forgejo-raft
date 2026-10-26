// Copyright 2026 The Forgejo Authors. All rights reserved.
// SPDX-License-Identifier: GPL-3.0-or-later

package cmd

import (
	"bytes"
	"context"
	"errors"
	"maps"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"forgejo.org/models/db"
	git_model "forgejo.org/models/git"
	repo_model "forgejo.org/models/repo"
	"forgejo.org/models/unittest"
	"forgejo.org/modules/clusterhook"
	"forgejo.org/modules/gitrepo"
	repo_module "forgejo.org/modules/repository"
	"forgejo.org/services/cluster/observer"
	repo_service "forgejo.org/services/repository"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// checkpointOfSQL is a checkpoint whose refs are SQL's branch rows, but
// for user2/repo1, whose refs are its repository's on disk: the refs at B a
// mirror's synchronisation, a migration or a parked push left ahead of
// the branch rows at L (#159).
func checkpointOfSQL(t *testing.T) *observer.Manifest {
	t.Helper()
	ctx := db.DefaultContext
	rows, err := db.GetEngine(ctx).Query(`SELECT r.owner_name AS owner, r.lower_name AS repo, b.name AS branch, b.commit_id AS sha
		FROM branch b JOIN repository r ON r.id = b.repo_id WHERE b.is_deleted = ?`, false)
	require.NoError(t, err)
	m := &observer.Manifest{Repos: map[string]observer.RepoManifest{}}
	for _, r := range rows {
		key := strings.ToLower(string(r["owner"])) + "/" + string(r["repo"]) + ".git"
		rm := m.Repos[key]
		if rm.Refs == nil {
			rm.Refs = map[string]string{}
		}
		rm.Refs["refs/heads/"+string(r["branch"])] = string(r["sha"])
		m.Repos[key] = rm
	}
	repo := unittest.AssertExistsAndLoadBean(t, &repo_model.Repository{ID: 1})
	gitRepo, err := gitrepo.OpenRepository(ctx, repo)
	require.NoError(t, err)
	defer gitRepo.Close()
	branches, _, err := gitRepo.GetBranchNames(0, 0)
	require.NoError(t, err)
	refs := map[string]string{}
	for _, b := range branches {
		c, err := gitRepo.GetBranchCommit(b)
		require.NoError(t, err)
		refs["refs/heads/"+b] = c.ID.String()
	}
	require.Contains(t, refs, "refs/heads/master")
	m.Repos["user2/repo1.git"] = observer.RepoManifest{Format: "sha1", Refs: refs}
	return m
}

// Branch rows a checkpoint's SQL holds behind its refs, of a repository
// SQL holds and the checkpoint restored, derive from the restored refs:
// the verification derives them and passes. What does not derive, the
// refs of a repository renamed past L, is still refused (#88, #159).
func TestRecoverDerivesTheBranchRowsBehindTheRefs(t *testing.T) {
	require.NoError(t, unittest.PrepareTestDatabase())
	unittest.PrepareTestEnv(t)
	t.Cleanup(func() { unittest.PrepareTestEnv(t) })
	ctx := db.DefaultContext
	e := db.GetEngine(ctx)

	t.Run("behind the refs", func(t *testing.T) {
		m := checkpointOfSQL(t)
		// The SQL at L: master at an older commit, and one branch the
		// synchronisation had not written yet.
		_, err := e.Exec("UPDATE branch SET commit_id = ? WHERE repo_id = 1 AND name = 'master'", strings.Repeat("0", 40))
		require.NoError(t, err)
		_, err = e.Exec("DELETE FROM branch WHERE repo_id = 1 AND name <> 'master'")
		require.NoError(t, err)

		var out bytes.Buffer
		require.NoError(t, verifyBranches(ctx, m, &out, false))
		assert.Contains(t, out.String(), "reconciled: user2/repo1.git, its branch rows derived from its restored refs")
		assert.Contains(t, out.String(), "refs and SQL rows match")
		mismatch, _, _, err := branchMismatch(ctx, m)
		require.NoError(t, err)
		assert.Empty(t, mismatch)
	})

	t.Run("a rename cut in two", func(t *testing.T) {
		m := checkpointOfSQL(t)
		// The refs at B under the new name, the row at L under the old.
		m.Repos["user2/renamed.git"] = m.Repos["user2/repo1.git"]
		delete(m.Repos, "user2/repo1.git")
		repo := unittest.AssertExistsAndLoadBean(t, &repo_model.Repository{ID: 1})
		require.NoError(t, os.Rename(repo.RepoPath(), repo_model.RepoPath("user2", "renamed")))

		var out bytes.Buffer
		err := verifyBranches(ctx, m, &out, false)
		require.ErrorContains(t, err, "user2/renamed.git refs/heads/master: refs ")
		require.ErrorContains(t, err, "user2/repo1.git refs/heads/master: in SQL, not in the refs")
		assert.NotContains(t, out.String(), "reconciled")
	})
}

// A pull mirror whose upstream deleted a branch, then recreated it at the
// same commit (a branch restored after its pull request was merged): the
// synchronisation marked the row deleted, and the next one, git holding
// the branch again, makes it live, the commit being the same or not. A
// checkpoint whose SQL at L still holds the row deleted, taken before
// that synchronisation or by a binary that did not revive it, holds the
// branch in its refs and not in its live rows: step 6 derives the row
// from the restored refs, as for any row behind them, instead of refusing
// the checkpoint after the old cluster was fenced (#178). Nothing here
// touches the repository on disk: only SQL rows change.
func TestRecoverDerivesABranchRecreatedAtItsCommit(t *testing.T) {
	require.NoError(t, unittest.PrepareTestDatabase())
	unittest.PrepareTestEnv(t)
	t.Cleanup(func() { unittest.PrepareTestEnv(t) })
	ctx := db.DefaultContext

	repo := unittest.AssertExistsAndLoadBean(t, &repo_model.Repository{ID: 1})
	gitRepo, err := gitrepo.OpenRepository(ctx, repo)
	require.NoError(t, err)
	defer gitRepo.Close()

	// Mirror sync 1: the branch rows follow the refs.
	_, err = repo_module.SyncRepoBranchesWithRepo(ctx, repo, gitRepo, 0)
	require.NoError(t, err)
	b, err := git_model.GetBranch(ctx, repo.ID, "branch2")
	require.NoError(t, err)
	require.False(t, b.IsDeleted)
	commit := b.CommitID
	// Mirror sync 2, the upstream deleted branch2: what
	// SyncRepoBranchesWithRepo's toRemove does.
	deleteBranch2 := func(t *testing.T) {
		t.Helper()
		require.NoError(t, git_model.DeleteBranches(ctx, repo.ID, 0, []int64{b.ID}))
		b, err := git_model.GetBranch(ctx, repo.ID, "branch2")
		require.NoError(t, err)
		require.True(t, b.IsDeleted)
		require.Equal(t, commit, b.CommitID)
	}

	t.Run("the next synchronisation", func(t *testing.T) {
		deleteBranch2(t)
		// Mirror sync 3, the upstream recreated branch2 at the same commit.
		_, err := repo_module.SyncRepoBranchesWithRepo(ctx, repo, gitRepo, 0)
		require.NoError(t, err)
		b, err := git_model.GetBranch(ctx, repo.ID, "branch2")
		require.NoError(t, err)
		assert.False(t, b.IsDeleted, "git holds branch2 again: its row is live")
		assert.Equal(t, commit, b.CommitID)

		// A checkpoint now: the refs at B hold branch2, and so does the SQL
		// at L.
		m := checkpointOfSQL(t)
		require.Equal(t, commit, m.Repos["user2/repo1.git"].Refs["refs/heads/branch2"])
		var out bytes.Buffer
		require.NoError(t, verifyBranches(ctx, m, &out, false), out.String())
		assert.NotContains(t, out.String(), "reconciled")
		assert.Contains(t, out.String(), "refs and SQL rows match")
	})

	t.Run("a checkpoint whose SQL holds the row deleted", func(t *testing.T) {
		deleteBranch2(t)
		m := checkpointOfSQL(t)
		require.Equal(t, commit, m.Repos["user2/repo1.git"].Refs["refs/heads/branch2"])
		var out bytes.Buffer
		require.NoError(t, verifyBranches(ctx, m, &out, false), out.String())
		assert.Contains(t, out.String(), "reconciled: user2/repo1.git, its branch rows derived from its restored refs")
		assert.Contains(t, out.String(), "refs and SQL rows match")
		b, err := git_model.GetBranch(ctx, repo.ID, "branch2")
		require.NoError(t, err)
		assert.False(t, b.IsDeleted)
		mismatch, _, _, err := branchMismatch(ctx, m)
		require.NoError(t, err)
		assert.Empty(t, mismatch)
	})
}

// checkpointOfEveryRepository is checkpointOfSQL holding every repository
// SQL holds, as a checkpoint does: those without a branch row as empty
// repositories, their HEAD alone.
func checkpointOfEveryRepository(t *testing.T) *observer.Manifest {
	t.Helper()
	m := checkpointOfSQL(t)
	rows, err := db.GetEngine(db.DefaultContext).Query(`SELECT owner_name AS owner, lower_name AS repo, default_branch AS head FROM repository`)
	require.NoError(t, err)
	for _, r := range rows {
		key := strings.ToLower(string(r["owner"])) + "/" + string(r["repo"]) + ".git"
		if _, ok := m.Repos[key]; !ok {
			m.Repos[key] = observer.RepoManifest{Format: "sha1", Symbolic: map[string]string{"HEAD": "refs/heads/" + string(r["head"])}}
		}
	}
	return m
}

// emptyRepositoryOfSQL is a repository SQL holds without a live branch row.
func emptyRepositoryOfSQL(t *testing.T) string {
	t.Helper()
	rows, err := db.GetEngine(db.DefaultContext).Query(`SELECT owner_name AS owner, lower_name AS repo FROM repository
		WHERE id NOT IN (SELECT repo_id FROM branch WHERE is_deleted = ?) ORDER BY id`, false)
	require.NoError(t, err)
	require.NotEmpty(t, rows)
	return strings.ToLower(string(rows[0]["owner"])) + "/" + string(rows[0]["repo"]) + ".git"
}

// withoutRepo is m without repo, as a copy.
func withoutRepo(m *observer.Manifest, repo string) *observer.Manifest {
	c := *m
	c.Repos = maps.Clone(m.Repos)
	delete(c.Repos, repo)
	return &c
}

// An orphan, as contrib/cluster/README.md "Recovery" describes it and
// services/cluster TestRepositoryDeletionTheClusterDoesNotRecord makes it:
// a repository deleted while the cluster has no quorum. Its rows are gone
// (DeleteRepositoryDirectly commits them first), the cluster did not record
// the deletion, and every member keeps its files and its live key, refs
// included, until `forgejo cluster reconcile --delete-orphan`. Every
// checkpoint taken meanwhile holds the orphan's branches at B and no row
// at L, and the observer protects it: step 6 restores the orphan as the
// old cluster held it and names it, instead of refusing the checkpoint
// after the old cluster was fenced (#255). When SQL also holds a
// repository the checkpoint does not, a rename or a transfer cut in two
// leaves the same, and only the operator tells them apart. Only SQL rows
// change here; the repository's files are kept, as the cluster keeps them.
func TestRecoverRestoresAnOrphanOfTheTable(t *testing.T) {
	require.NoError(t, unittest.PrepareTestDatabase())
	unittest.PrepareTestEnv(t)
	t.Cleanup(func() { unittest.PrepareTestEnv(t) })
	ctx := db.DefaultContext

	// The checkpoint before the deletion: every repository SQL holds, and
	// user2/repo1's refs those of its directory, the table's.
	m := checkpointOfEveryRepository(t)
	var out bytes.Buffer
	require.NoError(t, verifyBranches(ctx, m, &out, false), out.String())
	assert.NotContains(t, out.String(), "orphan")

	// The deletion, the cluster not recording it (no quorum).
	clusterhook.SetRepoLifecycle(func(context.Context, string, int64) error {
		return errors.New("no quorum: the leader did not answer")
	}, nil)
	t.Cleanup(func() { clusterhook.SetRepoLifecycle(nil, nil) })
	repo := unittest.AssertExistsAndLoadBean(t, &repo_model.Repository{ID: 1})
	err := repo_service.DeleteRepositoryDirectly(ctx, repo.ID, repo_service.DeleteRepositoryOpts{})
	require.ErrorContains(t, err, "--delete-orphan")
	unittest.AssertNotExistsBean(t, &repo_model.Repository{ID: 1})
	_, err = os.Stat(filepath.Join(repo.RepoPath(), "HEAD"))
	require.NoError(t, err, "the cluster keeps the orphan's files")
	// Its wiki, which the table keeps alive with it.
	m.Repos["user2/repo1.wiki.git"] = observer.RepoManifest{Format: "sha1", Refs: map[string]string{"refs/heads/master": strings.Repeat("1", 40)}}

	t.Run("SQL holds no repository the checkpoint does not", func(t *testing.T) {
		out.Reset()
		require.NoError(t, verifyBranches(ctx, m, &out, false), out.String())
		t.Logf("step 6:\n%s", out.String())
		assert.Contains(t, out.String(), "WARNING: user2/repo1.git is an orphan")
		assert.Contains(t, out.String(), "WARNING: user2/repo1.wiki.git is an orphan")
		assert.Contains(t, out.String(), "reconcile --delete-orphan")
		assert.Contains(t, out.String(), "refs and SQL rows match")
		assert.NotContains(t, out.String(), "reconciled")
	})

	t.Run("SQL also holds a repository the checkpoint does not", func(t *testing.T) {
		unheld := emptyRepositoryOfSQL(t)
		cut := withoutRepo(m, unheld)
		out.Reset()
		err := verifyBranches(ctx, cut, &out, false)
		t.Logf("step 6 refuses: %v", err)
		require.ErrorContains(t, err, "user2/repo1.git refs/heads/master: refs ")
		require.ErrorContains(t, err, "user2/repo1.git has no repository row in SQL")
		require.ErrorContains(t, err, unheld)
		require.ErrorContains(t, err, "--accept-orphans")

		// The operator's decision, the same command run again.
		out.Reset()
		require.NoError(t, verifyBranches(ctx, cut, &out, true), out.String())
		assert.Contains(t, out.String(), "WARNING: user2/repo1.git is an orphan")
		assert.Contains(t, out.String(), "--accept-orphans")
		assert.Contains(t, out.String(), "refs and SQL rows match")
	})
}

// What a rename or a transfer cut in two leaves is still refused once
// orphans are restored (#255): the refs under one name and the rows under
// the other, --accept-orphans offered only when it would pass, also when
// the rows' side has no branch row left, and with --accept-orphans when
// SQL's rows are not the checkpoint's refs.
func TestRecoverStillRefusesARenameCutInTwo(t *testing.T) {
	require.NoError(t, unittest.PrepareTestDatabase())
	unittest.PrepareTestEnv(t)
	t.Cleanup(func() { unittest.PrepareTestEnv(t) })
	ctx := db.DefaultContext
	e := db.GetEngine(ctx)

	// The refs at B under the new name, restored there, and the rows at L
	// under the old.
	renamed := func(t *testing.T, m *observer.Manifest) *observer.Manifest {
		t.Helper()
		m.Repos["user2/renamed.git"] = m.Repos["user2/repo1.git"]
		delete(m.Repos, "user2/repo1.git")
		repo := unittest.AssertExistsAndLoadBean(t, &repo_model.Repository{ID: 1})
		require.NoError(t, os.Rename(repo.RepoPath(), repo_model.RepoPath("user2", "renamed")))
		t.Cleanup(func() { require.NoError(t, os.Rename(repo_model.RepoPath("user2", "renamed"), repo.RepoPath())) })
		return m
	}

	t.Run("both halves hold branches", func(t *testing.T) {
		m := renamed(t, checkpointOfEveryRepository(t))
		var out bytes.Buffer
		err := verifyBranches(ctx, m, &out, false)
		require.ErrorContains(t, err, "user2/renamed.git refs/heads/master: refs ")
		require.ErrorContains(t, err, "user2/repo1.git refs/heads/master: in SQL, not in the refs")
		// It would not pass the rows under the old name: not offered.
		assert.NotContains(t, err.Error(), "--accept-orphans")
		assert.NotContains(t, out.String(), "is an orphan")
	})

	t.Run("the operator accepts orphans", func(t *testing.T) {
		m := renamed(t, checkpointOfEveryRepository(t))
		var out bytes.Buffer
		err := verifyBranches(ctx, m, &out, true)
		require.ErrorContains(t, err, "user2/repo1.git refs/heads/master: in SQL, not in the refs")
		assert.NotContains(t, out.String(), "reconciled")
	})

	t.Run("the rows' side without a branch row", func(t *testing.T) {
		m := renamed(t, checkpointOfEveryRepository(t))
		_, err := e.Exec("DELETE FROM branch WHERE repo_id = 1")
		require.NoError(t, err)
		var out bytes.Buffer
		err = verifyBranches(ctx, m, &out, false)
		require.ErrorContains(t, err, "user2/renamed.git refs/heads/master: refs ")
		require.ErrorContains(t, err, "user2/renamed.git has no repository row in SQL")
		require.ErrorContains(t, err, "user2/repo1.git")
		assert.NotContains(t, out.String(), "is an orphan")
	})
}
