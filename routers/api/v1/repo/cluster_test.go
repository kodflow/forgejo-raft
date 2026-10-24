// Copyright 2026 The Forgejo Authors. All rights reserved.
// SPDX-License-Identifier: GPL-3.0-or-later

package repo

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"testing"

	"forgejo.org/models/unittest"
	"forgejo.org/modules/clusterhook"
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

// behind is the refusal of a node of the cluster behind on the repository.
func behind(_ context.Context, key string) error {
	return fmt.Errorf("%w to %s; nothing was written, retry shortly or through another node", clusterhook.ErrBehind, key)
}

// A tag created through the API on a node of the cluster behind on the
// repository is refused with 503 before its target is read from the
// node's git, and nothing is written (#189).
func TestCreateTagRefusedOnANodeBehind(t *testing.T) {
	unittest.PrepareTestEnv(t)
	defer test.MockVariableValue(&clusterWriteRefusal, func(context.Context) error { return nil })()
	defer test.MockVariableValue(&clusterFresh, behind)()

	ctx, resp := contexttest.MockAPIContext(t, "POST /api/v1/repos/user2/repo1/tags")
	contexttest.LoadRepo(t, ctx, 1)
	contexttest.LoadUser(t, ctx, 2)
	contexttest.LoadGitRepo(t, ctx)
	defer ctx.Repo().GitRepo.Close()
	web.SetForm(ctx, &api.CreateTagOption{TagName: "refused-behind", Target: "master"})

	CreateTag(ctx)

	assert.Equal(t, http.StatusServiceUnavailable, ctx.Resp.Status())
	assert.Contains(t, resp.Body.String(), "may be behind")
	assert.False(t, ctx.Repo().GitRepo.IsTagExist("refused-behind"))
}

// A branch renamed through the API on a node of the cluster behind on the
// repository is refused with 503, and nothing is renamed (#189).
func TestRenameBranchRefusedOnANodeBehind(t *testing.T) {
	unittest.PrepareTestEnv(t)
	defer test.MockVariableValue(&clusterWriteRefusal, func(context.Context) error { return nil })()
	defer test.MockVariableValue(&clusterFresh, behind)()

	ctx, resp := contexttest.MockAPIContext(t, "PATCH /api/v1/repos/user2/repo1/branches/branch2")
	ctx.SetParams("*", "branch2")
	contexttest.LoadRepo(t, ctx, 1)
	contexttest.LoadUser(t, ctx, 2)
	contexttest.LoadGitRepo(t, ctx)
	defer ctx.Repo().GitRepo.Close()
	web.SetForm(ctx, &api.UpdateBranchRepoOption{Name: "renamed-behind"})

	UpdateBranch(ctx)

	assert.Equal(t, http.StatusServiceUnavailable, ctx.Resp.Status())
	assert.Contains(t, resp.Body.String(), "may be behind")
	assert.True(t, ctx.Repo().GitRepo.IsBranchExist("branch2"))
	assert.False(t, ctx.Repo().GitRepo.IsBranchExist("renamed-behind"))
}
