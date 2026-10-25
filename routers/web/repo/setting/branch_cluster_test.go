// Copyright 2026 The Forgejo Authors. All rights reserved.
// SPDX-License-Identifier: GPL-3.0-or-later

package setting

import (
	"context"
	"net/http"
	"testing"

	"forgejo.org/models/unittest"
	"forgejo.org/modules/clusterhook"
	"forgejo.org/modules/web"
	"forgejo.org/services/contexttest"
	"forgejo.org/services/forms"

	"github.com/stretchr/testify/assert"
)

// A branch renamed through the web settings moves its SQL row and the head
// of its pull requests. On a node of the cluster whose rows are not
// derived yet from the cluster's last writes, it is refused with 503 and
// the reason, and nothing is renamed (#225).
func TestRenameBranchRefusedWhileItsRowLags(t *testing.T) {
	unittest.PrepareTestEnv(t)
	clusterhook.SetDerivedWaiter(func(context.Context, string) bool { return false })
	t.Cleanup(func() { clusterhook.SetDerivedWaiter(nil) })

	ctx, resp := contexttest.MockContext(t, "user2/repo1/settings/rename_branch")
	contexttest.LoadUser(t, ctx, 2)
	contexttest.LoadRepo(t, ctx, 1)
	contexttest.LoadGitRepo(t, ctx)
	defer ctx.Repo.GitRepo.Close()
	web.SetForm(ctx, &forms.RenameBranchForm{From: "branch2", To: "renamed-behind"})

	RenameBranchPost(ctx)

	assert.Equal(t, http.StatusServiceUnavailable, ctx.Resp.Status())
	assert.Contains(t, resp.Body.String(), "may be behind")
	assert.True(t, ctx.Repo.GitRepo.IsBranchExist("branch2"))
	assert.False(t, ctx.Repo.GitRepo.IsBranchExist("renamed-behind"))
}
