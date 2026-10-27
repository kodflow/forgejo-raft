// Copyright 2026 The Forgejo Authors. All rights reserved.
// SPDX-License-Identifier: GPL-3.0-or-later

package setting

import (
	"context"
	"net/http"
	"sync/atomic"
	"testing"

	repo_model "forgejo.org/models/repo"
	"forgejo.org/models/unittest"
	"forgejo.org/modules/clusterhook"
	"forgejo.org/modules/gitrepo"
	"forgejo.org/services/contexttest"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// postDefaultBranch is the settings form "Default branch" of repo1 set to
// branch.
func postDefaultBranch(t *testing.T, branch string) (int, string) {
	t.Helper()
	ctx, resp := contexttest.MockContext(t, "user2/repo1/settings/branches")
	contexttest.LoadUser(t, ctx, 2)
	contexttest.LoadRepo(t, ctx, 1)
	contexttest.LoadGitRepo(t, ctx)
	defer ctx.Repo.GitRepo.Close()
	ctx.Req.Form.Set("action", "default_branch")
	ctx.Req.Form.Set("branch", branch)

	SetDefaultBranchPost(ctx)
	return ctx.Resp.Status(), resp.Body.String()
}

// defaultBranchOf is repo1's default branch, in SQL and as git's HEAD.
func defaultBranchOf(t *testing.T) (string, string) {
	t.Helper()
	repo := unittest.AssertExistsAndLoadBean(t, &repo_model.Repository{ID: 1})
	head, err := gitrepo.GetDefaultBranch(t.Context(), repo)
	require.NoError(t, err)
	return repo.DefaultBranch, head
}

// The default branch set through the web settings is looked up, and the
// scheduled workflows it holds are registered, from this node's git. On a
// node of the cluster behind on the repository, it is refused with 503 and
// the reason, and nothing is changed: neither the default branch nor HEAD
// (#299).
func TestSetDefaultBranchRefusedOnANodeBehind(t *testing.T) {
	unittest.PrepareTestEnv(t)
	clusterhook.SetFreshWaiter(func(context.Context, string) bool { return false })
	t.Cleanup(func() { clusterhook.SetFreshWaiter(nil) })

	status, body := postDefaultBranch(t, "branch2")

	assert.Equal(t, http.StatusServiceUnavailable, status)
	assert.Contains(t, body, "may be behind")
	sql, head := defaultBranchOf(t)
	assert.Equal(t, "master", sql)
	assert.Equal(t, "master", head)
}

// A node of the cluster that serves the repository at the cluster's commit
// index changes the default branch, having asked once.
func TestSetDefaultBranchOnAFreshNode(t *testing.T) {
	unittest.PrepareTestEnv(t)
	var asked atomic.Int32
	clusterhook.SetFreshWaiter(func(_ context.Context, key string) bool {
		assert.Equal(t, "user2/repo1.git", key)
		asked.Add(1)
		return true
	})
	t.Cleanup(func() { clusterhook.SetFreshWaiter(nil) })

	status, _ := postDefaultBranch(t, "branch2")

	assert.Equal(t, http.StatusSeeOther, status)
	assert.EqualValues(t, 1, asked.Load())
	sql, head := defaultBranchOf(t)
	assert.Equal(t, "branch2", sql)
	assert.Equal(t, "branch2", head)
}
