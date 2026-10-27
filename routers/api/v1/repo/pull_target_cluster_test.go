// Copyright 2026 The Forgejo Authors. All rights reserved.
// SPDX-License-Identifier: GPL-3.0-or-later

package repo

import (
	"context"
	"net/http"
	"testing"

	issues_model "forgejo.org/models/issues"
	"forgejo.org/models/unittest"
	"forgejo.org/modules/clusterhook"
	api "forgejo.org/modules/structs"
	"forgejo.org/modules/web"
	"forgejo.org/services/contexttest"

	"github.com/stretchr/testify/assert"
)

// editPull sends PATCH /repos/user2/repo1/pulls/3 (pull request 2, branch2
// into master) with form.
func editPull(t *testing.T, form *api.EditPullRequestOption) (int, string) {
	t.Helper()
	ctx, resp := contexttest.MockAPIContext(t, "PATCH /api/v1/repos/user2/repo1/pulls/3")
	contexttest.LoadUser(t, ctx, 2)
	contexttest.LoadRepo(t, ctx, 1)
	contexttest.LoadGitRepo(t, ctx)
	ctx.SetParams(":index", "3")
	web.SetForm(ctx, form)

	EditPullRequest(ctx)

	ctx.Repo().GitRepo.Close()
	body := resp.Body.String()
	if len(body) > 300 {
		body = body[:300] + "..."
	}
	return ctx.Resp.Status(), body
}

// An edit of a pull request that changes its base, sent to a node of the
// cluster behind on the pull request's repositories, is refused with 503
// and the reason before anything is written: the patch tested on the
// node's git would be that of the commit before a push another node
// acknowledged, and the merge checks trust what the change of the base
// writes (#300). The title the same edit carries is not changed either:
// the client retries the whole edit.
func TestEditPullRequestBaseRefusedOnANodeBehind(t *testing.T) {
	unittest.PrepareTestEnv(t)
	clusterhook.SetFreshWaiter(func(context.Context, string) bool { return false })
	t.Cleanup(func() { clusterhook.SetFreshWaiter(nil) })
	title := unittest.AssertExistsAndLoadBean(t, &issues_model.Issue{ID: 3}).Title

	code, body := editPull(t, &api.EditPullRequestOption{Title: "renamed on a node behind", Base: "develop"})

	assert.Equal(t, http.StatusServiceUnavailable, code, body)
	assert.Contains(t, body, "may be behind")
	assert.Equal(t, "master", unittest.AssertExistsAndLoadBean(t, &issues_model.PullRequest{ID: 2}).BaseBranch, "the base is unchanged")
	assert.Equal(t, title, unittest.AssertExistsAndLoadBean(t, &issues_model.Issue{ID: 3}).Title, "nothing written")

	// An edit that leaves the base alone does not wait.
	code, body = editPull(t, &api.EditPullRequestOption{Title: "renamed on a node behind", Base: "master"})
	assert.Equal(t, http.StatusCreated, code, body)
	assert.Equal(t, "renamed on a node behind", unittest.AssertExistsAndLoadBean(t, &issues_model.Issue{ID: 3}).Title)
}

// On a node of the cluster that serves the repositories, the base changes
// as before.
func TestEditPullRequestBaseOnAFreshNode(t *testing.T) {
	unittest.PrepareTestEnv(t)
	clusterhook.SetFreshWaiter(func(context.Context, string) bool { return true })
	t.Cleanup(func() { clusterhook.SetFreshWaiter(nil) })

	code, body := editPull(t, &api.EditPullRequestOption{Base: "develop"})

	assert.Equal(t, http.StatusCreated, code, body)
	assert.Equal(t, "develop", unittest.AssertExistsAndLoadBean(t, &issues_model.PullRequest{ID: 2}).BaseBranch)
}
