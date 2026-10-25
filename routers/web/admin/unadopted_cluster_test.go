// Copyright 2026 The Forgejo Authors. All rights reserved.
// SPDX-License-Identifier: GPL-3.0-or-later

package admin

import (
	"context"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"testing"

	"forgejo.org/models/unittest"
	user_model "forgejo.org/models/user"
	"forgejo.org/modules/clusterhook"
	"forgejo.org/modules/setting"
	"forgejo.org/modules/test"
	"forgejo.org/services/contexttest"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Site administration > Repositories > Unadopted > Delete, on a node of a
// cluster that holds the repository: refused with the procedure that
// deletes it from the cluster, shown to the administrator, not a server
// error, and the files stay (#245).
func TestDeleteUnadoptedRefusedWhileTheClusterHoldsIt(t *testing.T) {
	unittest.PrepareTestEnv(t)
	defer test.MockVariableValue(&setting.RepoRootPath, t.TempDir())()
	defer clusterhook.SetEnabled(clusterhook.Enabled())
	clusterhook.SetEnabled(true)
	var asked []string
	clusterhook.SetRepoHeldCheck(func(_ context.Context, keys []string) error {
		asked = append(asked, keys...)
		return fmt.Errorf("%w: user2/orphan.git; delete it with `forgejo cluster reconcile --delete-orphan user2/orphan --expect-index 7 --reason \"...\"`", clusterhook.ErrRepoHeld)
	})
	defer clusterhook.SetRepoHeldCheck(nil)
	dir := filepath.Join(setting.RepoRootPath, "user2", "orphan.git")
	require.NoError(t, os.MkdirAll(dir, 0o755))

	ctx, resp := contexttest.MockContext(t, "POST /admin/repos/unadopted")
	ctx.Doer = unittest.AssertExistsAndLoadBean(t, &user_model.User{ID: 1})
	ctx.Req.Form = url.Values{"id": {"user2/orphan"}, "action": {"delete"}}
	AdoptOrDeleteRepository(ctx)

	assert.Equal(t, http.StatusSeeOther, resp.Code)
	assert.Contains(t, ctx.Flash.ErrorMsg, "--delete-orphan user2/orphan")
	assert.Empty(t, ctx.Flash.SuccessMsg)
	assert.DirExists(t, dir)
	assert.Equal(t, []string{"user2/orphan.git"}, asked)
}
