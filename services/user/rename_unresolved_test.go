// Copyright 2026 The Forgejo Authors. All rights reserved.
// SPDX-License-Identifier: GPL-3.0-or-later

package user

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"forgejo.org/models/db"
	"forgejo.org/models/unittest"
	user_model "forgejo.org/models/user"
	"forgejo.org/modules/clusterhook"
	"forgejo.org/modules/log"
	"forgejo.org/modules/test"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// An owner's rename whose outcome the cluster cannot confirm leaves the
// owner's directory under the new name, SQL under the old one, and
// releases the hold of both owners while the rename may still apply: until
// this node applies it, its projector may fetch the owner's repositories
// again under the old owner, which its table still names, for a
// repository created there once SQL is aligned on the rename to serve. The
// hold stays as it is; the line Forgejo logs gives the operator the
// procedure, naming the owner, both names and the directory under the old
// name: restart the node, or remove there the directories the rename
// moved, before aligning SQL (#282).
func TestAnUnresolvedOwnerRenameLogsTheOperatorsProcedure(t *testing.T) {
	unittest.PrepareTestEnv(t)
	// The directory stays under the new name.
	t.Cleanup(func() { unittest.PrepareTestEnv(t) })
	u := unittest.AssertExistsAndLoadBean(t, &user_model.User{ID: 2})
	oldDir, newDir := user_model.UserPath("user2"), user_model.UserPath("unresolved-owner")
	_, err := os.Stat(oldDir)
	require.NoError(t, err, "the owner's directory")

	held := 0
	clusterhook.SetOwnerDirectoryHold(func(context.Context, []string) (func(), error) {
		held++
		return func() { held-- }, nil
	})
	proposed := 0
	clusterhook.SetOwnerLifecycle(func(context.Context, string, string) error {
		proposed++
		return fmt.Errorf("%w: test", clusterhook.ErrOutcomeUnresolved)
	})
	t.Cleanup(func() {
		clusterhook.SetOwnerDirectoryHold(nil)
		clusterhook.SetOwnerLifecycle(nil)
	})

	lc, cleanup := test.NewLogChecker(log.DEFAULT, log.ERROR)
	defer cleanup()
	want := []string{
		"user user2 (ID 2) to Unresolved-Owner", newDir,
		"frees the owner name user2",
		"restart this node, or remove the directories the rename moved, of its repositories and their wikis, under " + oldDir,
		"#282",
	}
	lc.Filter(want...).StopMark(clusterhook.ErrOutcomeUnresolved.Error())

	err = RenameUser(db.DefaultContext, u, "Unresolved-Owner")
	require.ErrorIs(t, err, clusterhook.ErrOutcomeUnresolved)

	filtered, stopped := lc.Check(5 * time.Second)
	require.True(t, stopped, "the line of the unresolved rename")
	for i, part := range want {
		assert.True(t, filtered[i], "the line names %q", part)
	}

	// Unchanged: proposed once, the directory left under the new name, the
	// hold released, SQL under the old name.
	assert.Equal(t, 1, proposed)
	assert.Zero(t, held, "released")
	_, err = os.Stat(oldDir)
	assert.True(t, os.IsNotExist(err), "nothing under the old name")
	_, err = os.Stat(newDir)
	require.NoError(t, err, "the directory under the new name")
	unittest.AssertExistsAndLoadBean(t, &user_model.User{ID: 2, LowerName: "user2"})
}
