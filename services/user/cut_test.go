// Copyright 2026 The Forgejo Authors. All rights reserved.
// SPDX-License-Identifier: GPL-3.0-or-later

package user

import (
	"context"
	"fmt"
	"os"
	"testing"

	"forgejo.org/models/db"
	repo_model "forgejo.org/models/repo"
	"forgejo.org/models/unittest"
	user_model "forgejo.org/models/user"
	"forgejo.org/modules/clusterhook"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// An owner's rename holds the checkpoint's cut from before its SQL until
// its entry and its SQL are both done (#88).
func TestRenameUserHoldsTheCheckpointCut(t *testing.T) {
	require.NoError(t, unittest.PrepareTestDatabase())
	held := 0
	var seen []int
	clusterhook.SetCutHold(func(context.Context) (func(), error) {
		held++
		return func() { held-- }, nil
	})
	clusterhook.SetOwnerLifecycle(func(context.Context, string, string) error {
		seen = append(seen, held)
		return nil
	})
	t.Cleanup(func() {
		clusterhook.SetCutHold(nil)
		clusterhook.SetOwnerLifecycle(nil)
	})

	u := unittest.AssertExistsAndLoadBean(t, &user_model.User{ID: 4})
	require.NoError(t, RenameUser(db.DefaultContext, u, "renamed-under-cut"))
	assert.Equal(t, []int{1}, seen, "the entry is proposed under the cut")
	assert.Zero(t, held, "released once the SQL committed")
	unittest.AssertExistsAndLoadBean(t, &user_model.User{ID: 4, LowerName: "renamed-under-cut"})
}

// An owner the cluster cannot rename, its repositories too many for one
// replicated entry, is refused before anything is written: no SQL, no
// directory moved, no checkpoint's cut held, nothing proposed, so nothing
// has to be undone (#182).
func TestRenameUserRefusedByTheClusterChangesNothing(t *testing.T) {
	require.NoError(t, unittest.PrepareTestDatabase())
	held, proposed := 0, 0
	clusterhook.SetCutHold(func(context.Context) (func(), error) {
		held++
		return func() {}, nil
	})
	clusterhook.SetOwnerLifecycle(func(context.Context, string, string) error {
		proposed++
		return nil
	})
	var asked [2]string
	clusterhook.SetOwnerRenameCheck(func(_ context.Context, oldOwner, newOwner string) error {
		asked = [2]string{oldOwner, newOwner}
		return fmt.Errorf("renaming the owner %s moves its 9000 repositories, wikis included, at once: %w", oldOwner, clusterhook.ErrTooLarge)
	})
	t.Cleanup(func() {
		clusterhook.SetCutHold(nil)
		clusterhook.SetOwnerLifecycle(nil)
		clusterhook.SetOwnerRenameCheck(nil)
	})

	u := unittest.AssertExistsAndLoadBean(t, &user_model.User{ID: 2})
	_, err := os.Stat(user_model.UserPath(u.Name))
	require.NoError(t, err, "the owner's directory")
	err = RenameUser(db.DefaultContext, u, "Too-Many-Repositories")
	require.ErrorIs(t, err, clusterhook.ErrTooLarge)
	assert.Equal(t, [2]string{"user2", "too-many-repositories"}, asked)
	assert.Zero(t, held, "no checkpoint's cut held")
	assert.Zero(t, proposed, "nothing proposed")
	assert.Equal(t, "user2", u.Name)
	unittest.AssertExistsAndLoadBean(t, &user_model.User{ID: 2, Name: "user2", LowerName: "user2"})
	unittest.AssertExistsAndLoadBean(t, &repo_model.Repository{OwnerID: 2, OwnerName: "user2"})
	unittest.AssertNotExistsBean(t, &user_model.Redirect{LowerName: "user2"})
	_, err = os.Stat(user_model.UserPath(u.Name))
	require.NoError(t, err, "the owner's directory stays")
	_, err = os.Stat(user_model.UserPath("Too-Many-Repositories"))
	assert.True(t, os.IsNotExist(err), "no directory under the new name")
}
