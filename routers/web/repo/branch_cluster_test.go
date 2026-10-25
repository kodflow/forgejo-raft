// Copyright 2026 The Forgejo Authors. All rights reserved.
// SPDX-License-Identifier: GPL-3.0-or-later

package repo

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
	"github.com/stretchr/testify/require"
)

// createBranchFromMaster runs the web form "create branch" from the
// branch master of repo1, viewed.
func createBranchFromMaster(t *testing.T, name string) (*http.Response, string, bool) {
	t.Helper()
	ctx, resp := contexttest.MockContext(t, "user2/repo1/branches/_new/branch/master")
	contexttest.LoadUser(t, ctx, 2)
	contexttest.LoadRepo(t, ctx, 1)
	contexttest.LoadGitRepo(t, ctx)
	defer ctx.Repo.GitRepo.Close()
	ctx.Repo.BranchName, ctx.Repo.IsViewBranch = "master", true
	web.SetForm(ctx, &forms.NewBranchForm{NewBranchName: name})

	CreateBranch(ctx)

	return resp.Result(), resp.Body.String(), ctx.Repo.GitRepo.IsBranchExist(name)
}

// The web form's "create branch from branch" reads the source in the
// branch's SQL row. On a node of the cluster whose rows are not derived
// yet from the cluster's last writes, it is refused with 503 and the
// reason, and nothing is written (#226).
func TestCreateBranchFromABranchRefusedWhileItsRowLags(t *testing.T) {
	unittest.PrepareTestEnv(t)
	clusterhook.SetDerivedWaiter(func(context.Context, string) bool { return false })
	t.Cleanup(func() { clusterhook.SetDerivedWaiter(nil) })

	resp, body, created := createBranchFromMaster(t, "refused-behind")

	assert.Equal(t, http.StatusServiceUnavailable, resp.StatusCode)
	assert.Contains(t, body, "may be behind")
	assert.False(t, created)
}

// Without a cluster the form creates the branch from the row, as before.
func TestCreateBranchFromABranchOutsideACluster(t *testing.T) {
	unittest.PrepareTestEnv(t)

	resp, _, created := createBranchFromMaster(t, "outside-a-cluster")

	require.Equal(t, http.StatusSeeOther, resp.StatusCode)
	assert.True(t, created)
}
