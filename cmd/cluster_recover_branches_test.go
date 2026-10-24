// Copyright 2026 The Forgejo Authors. All rights reserved.
// SPDX-License-Identifier: GPL-3.0-or-later

package cmd

import (
	"bytes"
	"os"
	"strings"
	"testing"

	"forgejo.org/models/db"
	git_model "forgejo.org/models/git"
	repo_model "forgejo.org/models/repo"
	"forgejo.org/models/unittest"
	"forgejo.org/modules/gitrepo"
	repo_module "forgejo.org/modules/repository"
	"forgejo.org/services/cluster/observer"

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
		require.NoError(t, verifyBranches(ctx, m, &out))
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
		err := verifyBranches(ctx, m, &out)
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
		require.NoError(t, verifyBranches(ctx, m, &out), out.String())
		assert.NotContains(t, out.String(), "reconciled")
		assert.Contains(t, out.String(), "refs and SQL rows match")
	})

	t.Run("a checkpoint whose SQL holds the row deleted", func(t *testing.T) {
		deleteBranch2(t)
		m := checkpointOfSQL(t)
		require.Equal(t, commit, m.Repos["user2/repo1.git"].Refs["refs/heads/branch2"])
		var out bytes.Buffer
		require.NoError(t, verifyBranches(ctx, m, &out), out.String())
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
