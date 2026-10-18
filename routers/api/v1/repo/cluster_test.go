// Copyright 2026 The Forgejo Authors. All rights reserved.
// SPDX-License-Identifier: GPL-3.0-or-later

package repo

import (
	"context"
	"errors"
	"net/http"
	"testing"

	"forgejo.org/models/unittest"
	api "forgejo.org/modules/structs"
	"forgejo.org/modules/test"
	"forgejo.org/modules/web"
	"forgejo.org/services/contexttest"

	"github.com/stretchr/testify/assert"
)

// A branch created through the API on a node of the cluster that reaches no
// majority is refused with 503 and the reason, not a 500 from deep in git,
// and nothing is written.
func TestCreateBranchRefusedByTheCluster(t *testing.T) {
	unittest.PrepareTestEnv(t)
	defer test.MockVariableValue(&clusterWriteRefusal, func(context.Context) error {
		return errors.New("this node of the cluster reaches no leader with a majority; nothing was written")
	})()

	ctx, resp := contexttest.MockAPIContext(t, "POST /api/v1/repos/user2/repo1/branches")
	contexttest.LoadRepo(t, ctx, 1)
	contexttest.LoadUser(t, ctx, 2)
	contexttest.LoadGitRepo(t, ctx)
	defer ctx.Repo().GitRepo.Close()
	web.SetForm(ctx, &api.CreateBranchRepoOption{BranchName: "refused-by-the-cluster", OldRefName: "master"})

	CreateBranch(ctx)

	assert.Equal(t, http.StatusServiceUnavailable, ctx.Resp.Status())
	assert.Contains(t, resp.Body.String(), "nothing was written")
	assert.False(t, ctx.Repo().GitRepo.IsBranchExist("refused-by-the-cluster"))
}
