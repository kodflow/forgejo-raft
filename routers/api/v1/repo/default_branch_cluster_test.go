// Copyright 2026 The Forgejo Authors. All rights reserved.
// SPDX-License-Identifier: GPL-3.0-or-later

package repo

import (
	"context"
	"net/http"
	"testing"

	repo_model "forgejo.org/models/repo"
	"forgejo.org/models/unittest"
	"forgejo.org/modules/gitrepo"
	api "forgejo.org/modules/structs"
	"forgejo.org/modules/test"
	"forgejo.org/modules/web"
	"forgejo.org/services/contexttest"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// editRepo1 is PATCH /repos/user2/repo1 with opts.
func editRepo1(t *testing.T, opts api.EditRepoOption) (int, string) {
	t.Helper()
	ctx, resp := contexttest.MockAPIContext(t, "PATCH /api/v1/repos/user2/repo1")
	contexttest.LoadRepo(t, ctx, 1)
	contexttest.LoadUser(t, ctx, 2)
	ctx.Repo().Owner = ctx.Doer()
	web.SetForm(ctx, &opts)

	Edit(ctx)
	return ctx.Resp.Status(), resp.Body.String()
}

// repo1 as SQL has it, and its default branch as git's HEAD.
func repo1AndHead(t *testing.T) (*repo_model.Repository, string) {
	t.Helper()
	repo := unittest.AssertExistsAndLoadBean(t, &repo_model.Repository{ID: 1})
	head, err := gitrepo.GetDefaultBranch(t.Context(), repo)
	require.NoError(t, err)
	return repo, head
}

// A default branch changed through the API is looked up in this node's
// git: on a node of the cluster behind on the repository, a branch another
// node pushed was taken for absent, and the change dropped with a 200. It
// is refused with 503 and the reason before anything is written, the
// rename and the description of the same request included (#299).
func TestEditDefaultBranchRefusedOnANodeBehind(t *testing.T) {
	unittest.PrepareTestEnv(t)
	defer test.MockVariableValue(&clusterFresh, behind)()
	name, description, branch := "renamed-behind", "changed behind", "branch2"

	status, body := editRepo1(t, api.EditRepoOption{Name: &name, Description: &description, DefaultBranch: &branch})

	assert.Equal(t, http.StatusServiceUnavailable, status)
	assert.Contains(t, body, "may be behind")
	repo, head := repo1AndHead(t)
	assert.Equal(t, "repo1", repo.Name)
	assert.NotEqual(t, description, repo.Description)
	assert.Equal(t, "master", repo.DefaultBranch)
	assert.Equal(t, "master", head)
}

// A node of the cluster that serves the repository at the cluster's commit
// index changes the default branch, having asked once; an edit that does
// not change it does not ask.
func TestEditDefaultBranchOnAFreshNode(t *testing.T) {
	unittest.PrepareTestEnv(t)
	asked := 0
	defer test.MockVariableValue(&clusterFresh, func(_ context.Context, key string) error {
		assert.Equal(t, "user2/repo1.git", key)
		asked++
		return nil
	})()

	description, master := "unchanged default branch", "master"
	status, _ := editRepo1(t, api.EditRepoOption{Description: &description, DefaultBranch: &master})
	assert.Equal(t, http.StatusOK, status)
	assert.Zero(t, asked, "the default branch is not changed")

	branch := "branch2"
	status, _ = editRepo1(t, api.EditRepoOption{DefaultBranch: &branch})
	assert.Equal(t, http.StatusOK, status)
	assert.Equal(t, 1, asked)
	repo, head := repo1AndHead(t)
	assert.Equal(t, description, repo.Description)
	assert.Equal(t, "branch2", repo.DefaultBranch)
	assert.Equal(t, "branch2", head)
}
