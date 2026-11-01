// Copyright 2026 The Forgejo Authors. All rights reserved.
// SPDX-License-Identifier: GPL-3.0-or-later

package repository

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"forgejo.org/models/db"
	repo_model "forgejo.org/models/repo"
	"forgejo.org/models/unittest"
	user_model "forgejo.org/models/user"
	"forgejo.org/modules/clusterhook"
	"forgejo.org/modules/test"
	"forgejo.org/modules/util"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// A rename the cluster recorded and the database then failed to record (the
// redirect, the name, or the commit, rolled back) is undone in the cluster,
// then on this node's disk: the directories of the repository and of its
// wiki go back under the old name before the hold of their keys is
// released. Left under the new name, the projector of this node fetched the
// whole repository again under the old key, and kept this copy, certified,
// under the freed name, for a repository created there later to adopt and
// serve (#383). They go back whatever the undo answers: undone, the table
// names the old key again; not undone, or its outcome unknown, the
// projector moves the directory to the new key itself, its stamp naming
// the rename's source.
func TestARenameTheDatabaseFailsToRecordMovesTheDirectoriesBackUnderTheHold(t *testing.T) {
	const newName = "undone-rename"
	exists := func(t *testing.T, path string) bool {
		t.Helper()
		ok, err := util.IsExist(path)
		require.NoError(t, err)
		return ok
	}
	unresolved := fmt.Errorf("%w: test", clusterhook.ErrOutcomeUnresolved)

	for _, tt := range []struct {
		name string
		// repoID 1 is user2/repo1, with a wiki; 2 is user2/repo2, without.
		repoID  int64
		oldName string
		wiki    bool
		// before runs before the rename, afterRename once the cluster
		// recorded it, before the database is written.
		before, afterRename func(t *testing.T)
		undoErr             error
		// commitErr, when set, is what the commit of the new name returns.
		commitErr error
		failure   string
	}{
		{
			name: "redirect", repoID: 1, oldName: "repo1", wiki: true,
			// A redirect already holds the old name: NewRedirect fails.
			before: func(t *testing.T) {
				require.NoError(t, db.Insert(db.DefaultContext, &repo_model.Redirect{OwnerID: 2, LowerName: "repo1", RedirectRepoID: 2}))
			},
			failure: "UNIQUE",
		},
		{
			name: "name", repoID: 1, oldName: "repo1", wiki: true,
			// A repository created under the new name meanwhile, through
			// another node: UpdateRepositoryCols fails on lower_name.
			afterRename: createUnderNewName(newName),
			failure:     "UNIQUE",
		},
		{
			name: "commit", repoID: 1, oldName: "repo1", wiki: true,
			commitErr: errors.New("commit failed: test"),
			failure:   "commit failed: test",
		},
		{
			name: "name without a wiki", repoID: 2, oldName: "repo2",
			afterRename: createUnderNewName(newName),
			failure:     "UNIQUE",
		},
		{
			name: "undo unresolved", repoID: 1, oldName: "repo1", wiki: true,
			afterRename: createUnderNewName(newName),
			undoErr:     unresolved,
			failure:     "UNIQUE",
		},
		{
			name: "undo refused", repoID: 1, oldName: "repo1", wiki: true,
			afterRename: createUnderNewName(newName),
			undoErr:     errors.New("the cluster refused the undo: test"),
			failure:     "UNIQUE",
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			unittest.PrepareTestEnv(t)
			doer := unittest.AssertExistsAndLoadBean(t, &user_model.User{ID: 2})
			repo := unittest.AssertExistsAndLoadBean(t, &repo_model.Repository{ID: tt.repoID})
			oldDir, newDir := repo_model.RepoPath("user2", tt.oldName), repo_model.RepoPath("user2", newName)
			oldWiki, newWiki := repo_model.WikiPath("user2", tt.oldName), repo_model.WikiPath("user2", newName)
			require.True(t, exists(t, oldDir))
			require.Equal(t, tt.wiki, exists(t, oldWiki))
			if tt.before != nil {
				tt.before(t)
			}
			if tt.commitErr != nil {
				t.Cleanup(test.MockVariableValue(&commitRename, func(db.Committer) error { return tt.commitErr }))
			}

			// back is whether the directories are under the old name only.
			back := func() bool {
				return exists(t, oldDir) && !exists(t, newDir) && exists(t, oldWiki) == tt.wiki && !exists(t, newWiki)
			}
			moved := func() bool {
				return !exists(t, oldDir) && exists(t, newDir) && !exists(t, oldWiki) && exists(t, newWiki) == tt.wiki
			}
			held := 0
			var backAtRelease []bool
			clusterhook.SetOwnerDirectoryHold(func(context.Context, []string) (func(), error) {
				held++
				return func() {
					held--
					backAtRelease = append(backAtRelease, back())
				}, nil
			})
			var proposals [][][2]string
			var undoSaw []bool
			clusterhook.SetRepoLifecycle(nil, func(_ context.Context, moves [][2]string) error {
				proposals = append(proposals, moves)
				if len(proposals) == 1 {
					if tt.afterRename != nil {
						tt.afterRename(t)
					}
					return nil
				}
				// The undo: under the hold, and before the directories
				// move back, so that every member follows the database.
				undoSaw = append(undoSaw, held == 1 && moved())
				return tt.undoErr
			})
			t.Cleanup(func() {
				clusterhook.SetOwnerDirectoryHold(nil)
				clusterhook.SetRepoLifecycle(nil, nil)
			})

			err := ChangeRepositoryName(db.DefaultContext, doer, repo, "Undone-Rename")
			require.ErrorContains(t, err, tt.failure)

			old := func(wiki bool) string { return clusterhook.RepoKey("user2", tt.oldName, wiki) }
			renamed := func(wiki bool) string { return clusterhook.RepoKey("user2", newName, wiki) }
			assert.Equal(t, [][][2]string{
				{{old(false), renamed(false)}, {old(true), renamed(true)}},
				{{renamed(false), old(false)}, {renamed(true), old(true)}},
			}, proposals, "the rename, then its undo")
			assert.Equal(t, []bool{true}, undoSaw, "the undo is proposed under the hold, before the directories move back")
			assert.Zero(t, held, "released")
			assert.Equal(t, []bool{true}, backAtRelease, "released once the directories are back under the old name")
			assert.True(t, back(), "the directories are back under the old name")
			got := unittest.AssertExistsAndLoadBean(t, &repo_model.Repository{ID: tt.repoID})
			assert.Equal(t, tt.oldName, got.LowerName, "the database kept the old name")
			unittest.AssertNotExistsBean(t, &repo_model.Redirect{OwnerID: 2, LowerName: tt.oldName, RedirectRepoID: tt.repoID})
		})
	}
}

// A commit whose answer was lost after the database recorded it is no
// failure to undo: the rename stands in the database, the table and on
// disk, and an undo would leave the database naming a repository the
// table and the directories moved away.
func TestARenameWhoseCommitWasRecordedDespiteItsErrorStands(t *testing.T) {
	const newName = "recorded-rename"
	unittest.PrepareTestEnv(t)
	doer := unittest.AssertExistsAndLoadBean(t, &user_model.User{ID: 2})
	repo := unittest.AssertExistsAndLoadBean(t, &repo_model.Repository{ID: 1})
	t.Cleanup(test.MockVariableValue(&commitRename, func(c db.Committer) error {
		require.NoError(t, c.Commit())
		return errors.New("connection lost on the commit's answer: test")
	}))
	var proposals [][][2]string
	clusterhook.SetOwnerDirectoryHold(func(context.Context, []string) (func(), error) { return func() {}, nil })
	clusterhook.SetRepoLifecycle(nil, func(_ context.Context, moves [][2]string) error {
		proposals = append(proposals, moves)
		return nil
	})
	t.Cleanup(func() {
		clusterhook.SetOwnerDirectoryHold(nil)
		clusterhook.SetRepoLifecycle(nil, nil)
	})

	require.NoError(t, ChangeRepositoryName(db.DefaultContext, doer, repo, newName))
	assert.Len(t, proposals, 1, "the rename, never undone")
	for path, want := range map[string]bool{
		repo_model.RepoPath("user2", "repo1"): false, repo_model.RepoPath("user2", newName): true,
		repo_model.WikiPath("user2", "repo1"): false, repo_model.WikiPath("user2", newName): true,
	} {
		ok, err := util.IsExist(path)
		require.NoError(t, err)
		assert.Equal(t, want, ok, path)
	}
	got := unittest.AssertExistsAndLoadBean(t, &repo_model.Repository{ID: 1})
	assert.Equal(t, newName, got.LowerName)
	unittest.AssertExistsAndLoadBean(t, &repo_model.Redirect{OwnerID: 2, LowerName: "repo1", RedirectRepoID: 1})
}

// createUnderNewName creates, in the database alone, a repository of user2
// under name, as another node's creation does while the rename waits.
func createUnderNewName(name string) func(t *testing.T) {
	return func(t *testing.T) {
		require.NoError(t, db.Insert(db.DefaultContext, &repo_model.Repository{OwnerID: 2, OwnerName: "user2", Name: name, LowerName: name}))
	}
}
