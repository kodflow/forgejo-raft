// Copyright 2026 The Forgejo Authors. All rights reserved.
// SPDX-License-Identifier: GPL-3.0-or-later

package repo

import (
	"context"
	"net/http"
	"testing"

	repo_model "forgejo.org/models/repo"
	"forgejo.org/models/unittest"
	"forgejo.org/modules/clusterhook"
	"forgejo.org/modules/web"
	"forgejo.org/services/contexttest"
	"forgejo.org/services/forms"

	"github.com/stretchr/testify/assert"
)

// The web form generates a repository from a template it loads by its ID,
// outside RepoAssignment. On a node of the cluster that does not serve the
// template yet, the generation is refused with 503 and the reason, before
// anything is written (#261).
func TestGenerateFromATemplateRefusedOnANodeBehind(t *testing.T) {
	unittest.PrepareTestEnv(t)
	clusterhook.SetFreshWaiter(func(context.Context, string) bool { return false })
	t.Cleanup(func() { clusterhook.SetFreshWaiter(nil) })

	ctx, resp := contexttest.MockContext(t, "repo/create")
	contexttest.LoadUser(t, ctx, 2)
	web.SetForm(ctx, &forms.CreateRepoForm{UID: 2, RepoName: "generated-behind", RepoTemplate: 44, GitContent: true})

	CreatePost(ctx)

	assert.Equal(t, http.StatusServiceUnavailable, resp.Result().StatusCode)
	assert.Contains(t, resp.Body.String(), "may be behind")
	unittest.AssertNotExistsBean(t, &repo_model.Repository{OwnerID: 2, LowerName: "generated-behind"})
}

// A fork through the web form on a node of the cluster that does not serve
// its base yet is refused with 503 and the reason, before anything is
// written (#283).
func TestForkRefusedOnANodeBehind(t *testing.T) {
	unittest.PrepareTestEnv(t)
	clusterhook.SetFreshWaiter(func(context.Context, string) bool { return false })
	t.Cleanup(func() { clusterhook.SetFreshWaiter(nil) })

	ctx, resp := contexttest.MockContext(t, "user2/repo1/fork")
	contexttest.LoadUser(t, ctx, 4)
	contexttest.LoadRepo(t, ctx, 1)
	web.SetForm(ctx, &forms.CreateRepoForm{UID: 4, RepoName: "forked-behind"})

	ForkPost(ctx)

	assert.Equal(t, http.StatusServiceUnavailable, resp.Result().StatusCode)
	assert.Contains(t, resp.Body.String(), "may be behind")
	unittest.AssertNotExistsBean(t, &repo_model.Repository{OwnerID: 4, LowerName: "forked-behind"})
}
