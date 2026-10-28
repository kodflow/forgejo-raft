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
	api "forgejo.org/modules/structs"
	"forgejo.org/modules/test"
	"forgejo.org/modules/web"
	app_context "forgejo.org/services/context"
	"forgejo.org/services/contexttest"

	"github.com/stretchr/testify/assert"
)

// rowsLagging makes this node of the cluster one whose SQL rows are not
// derived yet from the cluster's last writes to the repository.
func rowsLagging(t *testing.T) {
	t.Helper()
	clusterhook.SetDerivedWaiter(func(context.Context, string) error { return clusterhook.ErrBehind })
	t.Cleanup(func() { clusterhook.SetDerivedWaiter(nil) })
}

// releaseAPIContext is an API request of user2 on repo1, its git open.
func releaseAPIContext(t *testing.T, req string) (*app_context.APIContext, func() string) {
	t.Helper()
	ctx, resp := contexttest.MockAPIContext(t, req)
	contexttest.LoadRepo(t, ctx, 1)
	contexttest.LoadUser(t, ctx, 2)
	contexttest.LoadGitRepo(t, ctx)
	t.Cleanup(func() { ctx.Repo().GitRepo.Close() })
	return ctx, resp.Body.String
}

// A release created through the API on a node of the cluster whose
// release rows are not derived yet from the cluster's last writes is
// refused with 503 and the reason, before the tag's row is read: built on
// that row, the release would be overwritten by the derivation of a push
// that deleted the tag, or written on a row it removed. Nothing is
// written, no row and no tag, for a tag that has a row and for one that
// has none (#298).
func TestCreateReleaseRefusedWhileTheTagRowsLag(t *testing.T) {
	unittest.PrepareTestEnv(t)
	rowsLagging(t)

	t.Run("a tag with its row", func(t *testing.T) {
		ctx, body := releaseAPIContext(t, "POST /api/v1/repos/user2/repo1/releases")
		web.SetForm(ctx, &api.CreateReleaseOption{TagName: "delete-tag", Target: "master", Title: "refused"})

		CreateRelease(ctx)

		assert.Equal(t, http.StatusServiceUnavailable, ctx.Resp.Status())
		assert.Contains(t, body(), "may be behind")
		row := unittest.AssertExistsAndLoadBean(t, &repo_model.Release{ID: 3})
		assert.True(t, row.IsTag, "the tag row is left as it was")
		assert.NotEqual(t, "refused", row.Title)
		assert.False(t, ctx.Repo().GitRepo.IsTagExist("delete-tag"))
	})

	t.Run("a new tag", func(t *testing.T) {
		ctx, body := releaseAPIContext(t, "POST /api/v1/repos/user2/repo1/releases")
		web.SetForm(ctx, &api.CreateReleaseOption{TagName: "refused-release", Target: "master", Title: "refused"})

		CreateRelease(ctx)

		assert.Equal(t, http.StatusServiceUnavailable, ctx.Resp.Status())
		assert.Contains(t, body(), "may be behind")
		unittest.AssertNotExistsBean(t, &repo_model.Release{RepoID: 1, LowerTagName: "refused-release"})
		assert.False(t, ctx.Repo().GitRepo.IsTagExist("refused-release"))
	})
}

// On a node whose rows are derived, a release is created, and its tag.
func TestCreateReleaseOnANodeWhoseRowsAreDerived(t *testing.T) {
	unittest.PrepareTestEnv(t)
	waited := false
	clusterhook.SetDerivedWaiter(func(context.Context, string) error { waited = true; return nil })
	t.Cleanup(func() { clusterhook.SetDerivedWaiter(nil) })

	ctx, _ := releaseAPIContext(t, "POST /api/v1/repos/user2/repo1/releases")
	web.SetForm(ctx, &api.CreateReleaseOption{TagName: "derived-release", Target: "master", Title: "derived"})

	CreateRelease(ctx)

	assert.True(t, waited, "the release waited for the rows to be derived")
	assert.Equal(t, http.StatusCreated, ctx.Resp.Status())
	row := unittest.AssertExistsAndLoadBean(t, &repo_model.Release{RepoID: 1, LowerTagName: "derived-release"})
	assert.False(t, row.IsDraft)
	assert.False(t, row.IsTag)
	assert.True(t, ctx.Repo().GitRepo.IsTagExist("derived-release"))
}

// A release edited through the API on such a node is refused with 503
// before it is read, and is left as it was (#298).
func TestEditReleaseRefusedWhileTheRowsLag(t *testing.T) {
	unittest.PrepareTestEnv(t)
	rowsLagging(t)

	ctx, body := releaseAPIContext(t, "PATCH /api/v1/repos/user2/repo1/releases/1")
	ctx.SetParams(":id", "1")
	web.SetForm(ctx, &api.EditReleaseOption{Title: "refused"})

	EditRelease(ctx)

	assert.Equal(t, http.StatusServiceUnavailable, ctx.Resp.Status())
	assert.Contains(t, body(), "may be behind")
	row := unittest.AssertExistsAndLoadBean(t, &repo_model.Release{ID: 1})
	assert.NotEqual(t, "refused", row.Title)
}

// A tag deleted through the API on such a node is refused with 503 before
// its row is read: a tag whose push is not derived yet has no row, or the
// row of its value before the push, which the derivation would write again
// after the deletion. Nothing is deleted (#298).
func TestDeleteTagRefusedWhileItsRowLags(t *testing.T) {
	unittest.PrepareTestEnv(t)
	defer test.MockVariableValue(&clusterWriteRefusal, func(context.Context) error { return nil })()
	defer test.MockVariableValue(&clusterFresh, func(context.Context, string) error { return nil })()
	rowsLagging(t)

	ctx, body := releaseAPIContext(t, "DELETE /api/v1/repos/user2/repo1/tags/delete-tag")
	ctx.SetParams("*", "delete-tag")

	DeleteTag(ctx)

	assert.Equal(t, http.StatusServiceUnavailable, ctx.Resp.Status())
	assert.Contains(t, body(), "may be behind")
	row := unittest.AssertExistsAndLoadBean(t, &repo_model.Release{ID: 3})
	assert.True(t, row.IsTag)
}

// A tag created through the API on such a node is refused with 503, and
// nothing is written (#298).
func TestCreateTagRefusedWhileTheRowsLag(t *testing.T) {
	unittest.PrepareTestEnv(t)
	defer test.MockVariableValue(&clusterWriteRefusal, func(context.Context) error { return nil })()
	defer test.MockVariableValue(&clusterFresh, func(context.Context, string) error { return nil })()
	rowsLagging(t)

	ctx, body := releaseAPIContext(t, "POST /api/v1/repos/user2/repo1/tags")
	web.SetForm(ctx, &api.CreateTagOption{TagName: "refused-tag", Target: "master"})

	CreateTag(ctx)

	assert.Equal(t, http.StatusServiceUnavailable, ctx.Resp.Status())
	assert.Contains(t, body(), "may be behind")
	unittest.AssertNotExistsBean(t, &repo_model.Release{RepoID: 1, LowerTagName: "refused-tag"})
	assert.False(t, ctx.Repo().GitRepo.IsTagExist("refused-tag"))
}
