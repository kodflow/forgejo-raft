// Copyright 2026 The Forgejo Authors. All rights reserved.
// SPDX-License-Identifier: GPL-3.0-or-later

package setting

import (
	"context"
	"fmt"
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
	clusterhook.SetDerivedWaiter(func(context.Context, string) error { return clusterhook.ErrBehind })
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

// While a push to the repository is parked, its rows stay as they were
// before the push: the rename is refused with 503, the reason saying so
// and how the parked event is retried, and nothing is renamed (#239).
func TestRenameBranchRefusedWhileAPushIsParked(t *testing.T) {
	unittest.PrepareTestEnv(t)
	clusterhook.SetDerivedWaiter(func(context.Context, string) error {
		return fmt.Errorf("%w (it is retried by `forgejo cluster reconcile --apply`)", clusterhook.ErrParked)
	})
	t.Cleanup(func() { clusterhook.SetDerivedWaiter(nil) })

	ctx, resp := contexttest.MockContext(t, "user2/repo1/settings/rename_branch")
	contexttest.LoadUser(t, ctx, 2)
	contexttest.LoadRepo(t, ctx, 1)
	contexttest.LoadGitRepo(t, ctx)
	defer ctx.Repo.GitRepo.Close()
	web.SetForm(ctx, &forms.RenameBranchForm{From: "branch2", To: "renamed-parked"})

	RenameBranchPost(ctx)

	assert.Equal(t, http.StatusServiceUnavailable, ctx.Resp.Status())
	assert.Contains(t, resp.Body.String(), "is parked")
	assert.Contains(t, resp.Body.String(), "forgejo cluster reconcile --apply")
	assert.True(t, ctx.Repo.GitRepo.IsBranchExist("branch2"))
	assert.False(t, ctx.Repo.GitRepo.IsBranchExist("renamed-parked"))
}
