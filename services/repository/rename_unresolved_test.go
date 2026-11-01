// Copyright 2026 The Forgejo Authors. All rights reserved.
// SPDX-License-Identifier: GPL-3.0-or-later

package repository

import (
	"context"
	"fmt"
	"testing"
	"time"

	"forgejo.org/models/db"
	repo_model "forgejo.org/models/repo"
	"forgejo.org/models/unittest"
	user_model "forgejo.org/models/user"
	"forgejo.org/modules/clusterhook"
	"forgejo.org/modules/log"
	"forgejo.org/modules/test"
	"forgejo.org/modules/util"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// A repository's rename or transfer whose outcome the cluster cannot
// confirm leaves the directories under the new name, SQL under the old
// one, and releases the hold of their keys while the move may still apply:
// until this node applies it, its projector may fetch the whole repository
// again under the old name, which its table still names, and keep that
// copy, certified, for a repository created there once SQL is aligned on
// the move to serve. The hold stays as it is; the line Forgejo logs gives
// the operator the procedure that closes that window, naming the
// repository, both names and the directories under the old one: restart
// the node, or remove them, before aligning SQL (#282).
func TestAnUnresolvedRenameOrTransferLogsTheOperatorsProcedure(t *testing.T) {
	exists := func(t *testing.T, path string) bool {
		t.Helper()
		ok, err := util.IsExist(path)
		require.NoError(t, err)
		return ok
	}
	unresolved := fmt.Errorf("%w: test", clusterhook.ErrOutcomeUnresolved)

	for _, tt := range []struct {
		name      string
		repoID    int64
		freed, to string
		oldOwner  string
		oldName   string
		newDir    string
		move      func(t *testing.T, doer *user_model.User, repo *repo_model.Repository) error
	}{
		{
			name: "rename", repoID: 1,
			freed: "user2/repo1", to: "user2/unresolved-rename",
			oldOwner: "user2", oldName: "repo1",
			newDir: repo_model.RepoPath("user2", "unresolved-rename"),
			move: func(_ *testing.T, doer *user_model.User, repo *repo_model.Repository) error {
				return ChangeRepositoryName(db.DefaultContext, doer, repo, "Unresolved-Rename")
			},
		},
		{
			name: "transfer", repoID: 3,
			freed: "org3/repo3", to: "user2",
			oldOwner: "org3", oldName: "repo3",
			newDir: repo_model.RepoPath("user2", "repo3"),
			move: func(t *testing.T, doer *user_model.User, repo *repo_model.Repository) error {
				repo.Owner = unittest.AssertExistsAndLoadBean(t, &user_model.User{ID: repo.OwnerID})
				return TransferOwnership(db.DefaultContext, doer, doer, repo, nil)
			},
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			unittest.PrepareTestEnv(t)
			// The directories stay under the new name.
			t.Cleanup(func() { unittest.PrepareTestEnv(t) })
			doer := unittest.AssertExistsAndLoadBean(t, &user_model.User{ID: 2})
			repo := unittest.AssertExistsAndLoadBean(t, &repo_model.Repository{ID: tt.repoID})
			oldDir, oldWiki := repo_model.RepoPath(tt.oldOwner, tt.oldName), repo_model.WikiPath(tt.oldOwner, tt.oldName)
			require.True(t, exists(t, oldDir))

			held := 0
			clusterhook.SetOwnerDirectoryHold(func(context.Context, []string) (func(), error) {
				held++
				return func() { held-- }, nil
			})
			proposed := 0
			clusterhook.SetRepoLifecycle(nil, func(context.Context, [][2]string) error {
				proposed++
				return unresolved
			})
			t.Cleanup(func() {
				clusterhook.SetOwnerDirectoryHold(nil)
				clusterhook.SetRepoLifecycle(nil, nil)
			})

			lc, cleanup := test.NewLogChecker(log.DEFAULT, log.ERROR)
			defer cleanup()
			want := []string{
				tt.freed, tt.to, fmt.Sprintf("(ID %d)", tt.repoID), tt.newDir,
				"frees " + tt.freed,
				"restart this node, or remove " + oldDir + " and " + oldWiki,
				"#282",
			}
			lc.Filter(want...).StopMark(clusterhook.ErrOutcomeUnresolved.Error())

			err := tt.move(t, doer, repo)
			require.ErrorIs(t, err, clusterhook.ErrOutcomeUnresolved)

			filtered, stopped := lc.Check(5 * time.Second)
			require.True(t, stopped, "the line of the unresolved move")
			for i, part := range want {
				assert.True(t, filtered[i], "the line names %q", part)
			}

			// Unchanged: proposed once, never undone, the directories left
			// under the new name, the hold released, SQL under the old name.
			assert.Equal(t, 1, proposed)
			assert.Zero(t, held, "released")
			assert.False(t, exists(t, oldDir))
			assert.True(t, exists(t, tt.newDir))
			got := unittest.AssertExistsAndLoadBean(t, &repo_model.Repository{ID: tt.repoID})
			assert.Equal(t, tt.oldName, got.LowerName)
			assert.Equal(t, tt.oldOwner, got.OwnerName)
		})
	}
}
