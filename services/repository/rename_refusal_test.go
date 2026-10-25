// Copyright 2026 The Forgejo Authors. All rights reserved.
// SPDX-License-Identifier: GPL-3.0-or-later

package repository

import (
	"context"
	"fmt"
	"testing"

	"forgejo.org/models/db"
	repo_model "forgejo.org/models/repo"
	"forgejo.org/models/unittest"
	user_model "forgejo.org/models/user"
	"forgejo.org/modules/clusterhook"
	"forgejo.org/modules/util"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// A repository whose key left the cluster's table, its owner renamed
// through another node after it was loaded, is refused before anything is
// written, renamed or transferred: no SQL, no directory moved, no
// checkpoint's cut held, nothing proposed, so nothing has to be undone
// (#220).
func TestARenameOrATransferRefusedByTheClusterChangesNothing(t *testing.T) {
	unittest.PrepareTestEnv(t)
	held, proposed := 0, 0
	clusterhook.SetCutHold(func(context.Context) (func(), error) {
		held++
		return func() {}, nil
	})
	clusterhook.SetRepoLifecycle(nil, func(context.Context, [][2]string) error {
		proposed++
		return nil
	})
	var asked []string
	clusterhook.SetRepoRenameCheck(func(_ context.Context, key string) error {
		asked = append(asked, key)
		return fmt.Errorf("the cluster refused it: the repository %s was renamed to elsewhere/x.git", key)
	})
	t.Cleanup(func() {
		clusterhook.SetCutHold(nil)
		clusterhook.SetRepoLifecycle(nil, nil)
		clusterhook.SetRepoRenameCheck(nil)
	})
	exists := func(path string) bool {
		t.Helper()
		ok, err := util.IsExist(path)
		require.NoError(t, err)
		return ok
	}
	doer := unittest.AssertExistsAndLoadBean(t, &user_model.User{ID: 2})

	t.Run("rename", func(t *testing.T) {
		repo := unittest.AssertExistsAndLoadBean(t, &repo_model.Repository{ID: 2})
		err := ChangeRepositoryName(db.DefaultContext, doer, repo, "Refused-Rename")
		require.ErrorContains(t, err, "the repository user2/repo2.git was renamed to elsewhere/x.git")
		assert.Equal(t, []string{"user2/repo2.git"}, asked)
		unittest.AssertExistsAndLoadBean(t, &repo_model.Repository{ID: 2, LowerName: "repo2"})
		unittest.AssertNotExistsBean(t, &repo_model.Redirect{OwnerID: 2, LowerName: "repo2"})
		assert.True(t, exists(repo_model.RepoPath("user2", "repo2")), "the directory stays")
		assert.False(t, exists(repo_model.RepoPath("user2", "refused-rename")))
	})

	t.Run("transfer", func(t *testing.T) {
		asked = nil
		repo := unittest.AssertExistsAndLoadBean(t, &repo_model.Repository{ID: 3})
		repo.Owner = unittest.AssertExistsAndLoadBean(t, &user_model.User{ID: repo.OwnerID})
		err := TransferOwnership(db.DefaultContext, doer, doer, repo, nil)
		require.ErrorContains(t, err, "the repository org3/repo3.git was renamed to elsewhere/x.git")
		assert.Equal(t, []string{"org3/repo3.git"}, asked)
		unittest.AssertExistsAndLoadBean(t, &repo_model.Repository{ID: 3, OwnerID: 3, OwnerName: "org3"})
		assert.True(t, exists(repo_model.RepoPath("org3", "repo3")), "the directory stays")
		assert.False(t, exists(repo_model.RepoPath("user2", "repo3")))
	})

	assert.Zero(t, held, "no checkpoint's cut held")
	assert.Zero(t, proposed, "nothing proposed")
}
