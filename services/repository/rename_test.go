// Copyright 2026 The Forgejo Authors. All rights reserved.
// SPDX-License-Identifier: GPL-3.0-or-later

package repository

import (
	"testing"

	"forgejo.org/models/db"
	repo_model "forgejo.org/models/repo"
	"forgejo.org/models/unittest"
	user_model "forgejo.org/models/user"
	"forgejo.org/modules/util"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// The rename writes the new name in the database itself, with the
// redirect: a caller that refuses another setting after it, and returns
// without UpdateRepository, no longer leaves the database under the old
// name and the directories, on every node of a cluster, under the new
// one (#135).
func TestChangeRepositoryNameWritesName(t *testing.T) {
	require.NoError(t, unittest.PrepareTestDatabase())
	ctx := db.DefaultContext
	doer := unittest.AssertExistsAndLoadBean(t, &user_model.User{ID: 2})
	repo := unittest.AssertExistsAndLoadBean(t, &repo_model.Repository{ID: 1})
	oldPath := repo.RepoPath()

	require.NoError(t, ChangeRepositoryName(ctx, doer, repo, "Renamed-Repo1"))

	got := unittest.AssertExistsAndLoadBean(t, &repo_model.Repository{ID: 1})
	assert.Equal(t, "Renamed-Repo1", got.Name)
	assert.Equal(t, "renamed-repo1", got.LowerName)
	exists, err := util.IsExist(got.RepoPath())
	require.NoError(t, err)
	assert.True(t, exists, "the directory is under the name the database holds")
	exists, err = util.IsExist(oldPath)
	require.NoError(t, err)
	assert.False(t, exists)
	unittest.AssertExistsAndLoadBean(t, &repo_model.Redirect{OwnerID: 2, LowerName: "repo1", RedirectRepoID: 1})

	require.NoError(t, ChangeRepositoryName(ctx, doer, got, "repo1"))
	got = unittest.AssertExistsAndLoadBean(t, &repo_model.Repository{ID: 1})
	assert.Equal(t, "repo1", got.Name)
}
