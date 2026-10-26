// Copyright 2026 The Forgejo Authors. All rights reserved.
// SPDX-License-Identifier: GPL-3.0-or-later

package repository

import (
	"context"
	"errors"
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

// A repository's rename and its transfer hold, on this node, the
// directories of the repository and of its wiki under both names from
// before Forgejo moves them until the move ends, its move back after a
// refusal of the cluster included: nothing of the cluster writes there in
// between, and no copy of the repository stays under the freed name (#274).
func TestARenameOrATransferHoldsTheRepositorysDirectoriesUntilTheyStopMoving(t *testing.T) {
	unittest.PrepareTestEnv(t)
	exists := func(path string) bool {
		t.Helper()
		ok, err := util.IsExist(path)
		require.NoError(t, err)
		return ok
	}
	doer := unittest.AssertExistsAndLoadBean(t, &user_model.User{ID: 2})

	for _, tt := range []struct {
		name     string
		oldDir   string
		newDir   string
		keys     []string
		move     func() error
		repoBack *repo_model.Repository
	}{
		{
			name:   "rename",
			oldDir: repo_model.RepoPath("user2", "repo2"), newDir: repo_model.RepoPath("user2", "refused-rename"),
			keys: []string{"user2/repo2.git", "user2/repo2.wiki.git", "user2/refused-rename.git", "user2/refused-rename.wiki.git"},
			move: func() error {
				repo := unittest.AssertExistsAndLoadBean(t, &repo_model.Repository{ID: 2})
				return ChangeRepositoryName(db.DefaultContext, doer, repo, "Refused-Rename")
			},
			repoBack: &repo_model.Repository{ID: 2, LowerName: "repo2"},
		},
		{
			name:   "transfer",
			oldDir: repo_model.RepoPath("org3", "repo3"), newDir: repo_model.RepoPath("user2", "repo3"),
			keys: []string{"org3/repo3.git", "org3/repo3.wiki.git", "user2/repo3.git", "user2/repo3.wiki.git"},
			move: func() error {
				repo := unittest.AssertExistsAndLoadBean(t, &repo_model.Repository{ID: 3})
				repo.Owner = unittest.AssertExistsAndLoadBean(t, &user_model.User{ID: repo.OwnerID})
				return TransferOwnership(db.DefaultContext, doer, doer, repo, nil)
			},
			repoBack: &repo_model.Repository{ID: 3, OwnerID: 3, OwnerName: "org3"},
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			require.True(t, exists(tt.oldDir), "the repository's directory")
			held := 0
			var names []string
			var backAtRelease []bool
			clusterhook.SetOwnerDirectoryHold(func(_ context.Context, n []string) (func(), error) {
				assert.True(t, exists(tt.oldDir), "held before the directory moves")
				held++
				names = n
				return func() {
					held--
					backAtRelease = append(backAtRelease, exists(tt.oldDir) && !exists(tt.newDir))
				}, nil
			})
			var proposedHeld []int
			clusterhook.SetRepoLifecycle(nil, func(context.Context, [][2]string) error {
				assert.False(t, exists(tt.oldDir), "proposed once the directory moved")
				proposedHeld = append(proposedHeld, held)
				return errors.New("the cluster refused it: test")
			})
			t.Cleanup(func() {
				clusterhook.SetOwnerDirectoryHold(nil)
				clusterhook.SetRepoLifecycle(nil, nil)
			})

			err := tt.move()
			require.ErrorContains(t, err, "the cluster refused it")
			assert.Equal(t, tt.keys, names)
			assert.Equal(t, []int{1}, proposedHeld, "proposed under the hold")
			assert.Zero(t, held, "released")
			assert.Equal(t, []bool{true}, backAtRelease, "released once the directory moved back")
			unittest.AssertExistsAndLoadBean(t, tt.repoBack)
		})
	}
}
