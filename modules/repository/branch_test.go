// Copyright 2024 The Gitea Authors. All rights reserved.
// SPDX-License-Identifier: MIT

package repository

import (
	"testing"

	"forgejo.org/models/db"
	git_model "forgejo.org/models/git"
	repo_model "forgejo.org/models/repo"
	"forgejo.org/models/unittest"
	"forgejo.org/modules/gitrepo"
	"forgejo.org/modules/optional"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSyncRepoBranches(t *testing.T) {
	require.NoError(t, unittest.PrepareTestDatabase())
	_, err := db.GetEngine(db.DefaultContext).ID(1).Update(&repo_model.Repository{ObjectFormatName: "bad-fmt"})
	require.NoError(t, db.TruncateBeans(db.DefaultContext, &git_model.Branch{}))
	require.NoError(t, err)
	repo := unittest.AssertExistsAndLoadBean(t, &repo_model.Repository{ID: 1})
	assert.Equal(t, "bad-fmt", repo.ObjectFormatName)
	_, err = SyncRepoBranches(db.DefaultContext, 1, 0)
	require.NoError(t, err)
	repo = unittest.AssertExistsAndLoadBean(t, &repo_model.Repository{ID: 1})
	assert.Equal(t, "sha1", repo.ObjectFormatName)
	branch, err := git_model.GetBranch(db.DefaultContext, 1, "master")
	require.NoError(t, err)
	assert.Equal(t, "master", branch.Name)
}

// A row marked deleted whose branch git holds again, at the very commit the
// row names (a branch deleted, then restored or pushed again), is live
// after the synchronisation, as it is at any other commit. A row whose
// branch git does not hold stays deleted, as it was.
func TestSyncRepoBranchesRevivesABranchGitHoldsAgain(t *testing.T) {
	require.NoError(t, unittest.PrepareTestDatabase())
	ctx := db.DefaultContext
	repo := unittest.AssertExistsAndLoadBean(t, &repo_model.Repository{ID: 1})
	gitRepo, err := gitrepo.OpenRepository(ctx, repo)
	require.NoError(t, err)
	defer gitRepo.Close()

	_, err = SyncRepoBranchesWithRepo(ctx, repo, gitRepo, 0)
	require.NoError(t, err)
	live := unittest.AssertExistsAndLoadBean(t, &git_model.Branch{RepoID: repo.ID, Name: "branch2"})
	require.False(t, live.IsDeleted)
	commit, err := gitRepo.GetBranchCommitID("branch2")
	require.NoError(t, err)
	require.Equal(t, commit, live.CommitID)

	// What a mirror's synchronisation leaves when its upstream deleted the
	// branch: the row marked deleted, at the commit it had.
	require.NoError(t, git_model.DeleteBranches(ctx, repo.ID, 2, []int64{live.ID}))
	require.True(t, unittest.AssertExistsAndLoadBean(t, &git_model.Branch{ID: live.ID}).IsDeleted)

	_, err = SyncRepoBranchesWithRepo(ctx, repo, gitRepo, 0)
	require.NoError(t, err)
	revived := unittest.AssertExistsAndLoadBean(t, &git_model.Branch{ID: live.ID})
	assert.False(t, revived.IsDeleted, "git holds branch2 again: its row is live")
	assert.Equal(t, commit, revived.CommitID)
	n, err := db.Count[git_model.Branch](ctx, git_model.FindBranchOptions{RepoID: repo.ID, IsDeletedBranch: optional.Some(false)})
	require.NoError(t, err)
	names, _, err := gitRepo.GetBranchNames(0, 0)
	require.NoError(t, err)
	assert.EqualValues(t, len(names), n, "one live row per branch git holds")

	// foo and bar are deleted rows of branches git does not hold.
	for _, name := range []string{"foo", "bar"} {
		gone := unittest.AssertExistsAndLoadBean(t, &git_model.Branch{RepoID: repo.ID, Name: name})
		assert.True(t, gone.IsDeleted, "%s is not in git: its row stays deleted", name)
		assert.NotZero(t, gone.DeletedUnix, name)
	}
}
