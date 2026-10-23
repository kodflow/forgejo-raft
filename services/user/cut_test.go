// Copyright 2026 The Forgejo Authors. All rights reserved.
// SPDX-License-Identifier: GPL-3.0-or-later

package user

import (
	"context"
	"testing"

	"forgejo.org/models/db"
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
