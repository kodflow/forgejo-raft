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

// releaseRowsLag makes this node of the cluster one whose SQL rows are not
// derived yet from the cluster's last writes to the repository.
func releaseRowsLag(t *testing.T) {
	t.Helper()
	clusterhook.SetDerivedWaiter(func(context.Context, string) error { return clusterhook.ErrBehind })
	t.Cleanup(func() { clusterhook.SetDerivedWaiter(nil) })
}

// A release created from the web form on a node of the cluster whose
// release rows are not derived yet from the cluster's last writes is
// refused with 503 and the reason, before the tag's row is read: nothing is
// written, no row and no tag, for a tag that has a row, a new release and
// a tag alone (#298).
func TestNewReleaseRefusedWhileTheTagRowsLag(t *testing.T) {
	for _, tc := range []struct {
		name string
		form forms.NewReleaseForm
	}{
		{"a tag with its row", forms.NewReleaseForm{TagName: "delete-tag", Target: "master", Title: "refused", Content: "refused"}},
		{"a new release", forms.NewReleaseForm{TagName: "refused-release", Target: "master", Title: "refused", Content: "refused"}},
		{"a tag alone", forms.NewReleaseForm{TagName: "refused-tag", Target: "master", TagOnly: "on"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			unittest.PrepareTestEnv(t)
			releaseRowsLag(t)
			ctx, resp := contexttest.MockContext(t, "user2/repo1/releases/new")
			contexttest.LoadUser(t, ctx, 2)
			contexttest.LoadRepo(t, ctx, 1)
			contexttest.LoadGitRepo(t, ctx)
			defer ctx.Repo.GitRepo.Close()
			web.SetForm(ctx, &tc.form)

			NewReleasePost(ctx)

			assert.Equal(t, http.StatusServiceUnavailable, resp.Code)
			assert.Contains(t, resp.Body.String(), "may be behind")
			assert.False(t, ctx.Repo.GitRepo.IsTagExist(tc.form.TagName), "a tag was created")
			if tc.form.TagName == "delete-tag" {
				row := unittest.AssertExistsAndLoadBean(t, &repo_model.Release{ID: 3})
				assert.True(t, row.IsTag, "the tag row is left as it was")
				assert.NotEqual(t, "refused", row.Title)
			} else {
				unittest.AssertNotExistsBean(t, &repo_model.Release{RepoID: 1, LowerTagName: tc.form.TagName})
			}
		})
	}
}

// A release edited from the web form on such a node is refused with 503
// before it is read, and is left as it was (#298).
func TestEditReleaseRefusedWhileTheRowsLag(t *testing.T) {
	unittest.PrepareTestEnv(t)
	releaseRowsLag(t)
	ctx, resp := contexttest.MockContext(t, "user2/repo1/releases/edit/v1.1")
	contexttest.LoadUser(t, ctx, 2)
	contexttest.LoadRepo(t, ctx, 1)
	contexttest.LoadGitRepo(t, ctx)
	defer ctx.Repo.GitRepo.Close()
	ctx.SetParams("*", "v1.1")
	web.SetForm(ctx, &forms.EditReleaseForm{Title: "refused", Content: "refused"})

	EditReleasePost(ctx)

	assert.Equal(t, http.StatusServiceUnavailable, resp.Code)
	assert.Contains(t, resp.Body.String(), "may be behind")
	row := unittest.AssertExistsAndLoadBean(t, &repo_model.Release{ID: 1})
	assert.NotEqual(t, "refused", row.Title)
}

// A tag deleted from the tag list on such a node is refused before its row
// is read, and the page says so where it reads the answer, the redirect's
// flash: nothing is deleted. A release deleted with its tag kept is not
// refused: the derivation of a push to its tag converges on the tag row it
// becomes (#298).
func TestDeleteTagRefusedWhileItsRowLags(t *testing.T) {
	unittest.PrepareTestEnv(t)
	releaseRowsLag(t)
	ctx, _ := contexttest.MockContext(t, "user2/repo1/tags/delete?id=3")
	contexttest.LoadUser(t, ctx, 2)
	contexttest.LoadRepo(t, ctx, 1)
	contexttest.LoadGitRepo(t, ctx)
	defer ctx.Repo.GitRepo.Close()

	DeleteTag(ctx)

	assert.Contains(t, ctx.Flash.ErrorMsg, "may be behind")
	assert.Empty(t, ctx.Flash.SuccessMsg)
	unittest.AssertExistsAndLoadBean(t, &repo_model.Release{ID: 3})

	ctx, _ = contexttest.MockContext(t, "user2/repo1/releases/delete?id=4")
	contexttest.LoadUser(t, ctx, 2)
	contexttest.LoadRepo(t, ctx, 1)
	contexttest.LoadGitRepo(t, ctx)
	defer ctx.Repo.GitRepo.Close()

	DeleteRelease(ctx)

	assert.Empty(t, ctx.Flash.ErrorMsg)
	assert.NotEmpty(t, ctx.Flash.SuccessMsg)
	row := unittest.AssertExistsAndLoadBean(t, &repo_model.Release{ID: 4})
	assert.True(t, row.IsTag, "the release is kept as a tag")
}
