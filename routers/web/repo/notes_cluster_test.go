// Copyright 2026 The Forgejo Authors. All rights reserved.
// SPDX-License-Identifier: GPL-3.0-or-later

package repo

import (
	gocontext "context"
	"net/http"
	"net/http/httptest"
	"testing"

	"forgejo.org/models/unittest"
	"forgejo.org/modules/clusterhook"
	"forgejo.org/modules/git"
	"forgejo.org/modules/web"
	"forgejo.org/services/context"
	"forgejo.org/services/contexttest"
	"forgejo.org/services/forms"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// notesForms are the web forms that set or remove the note of a commit of
// repo1, from its page or a pull request's: each runs on the commit the
// fixtures give a note, as user2, and returns the response and the note of
// the commit afterwards, "" when there is none.
func notesForms(t *testing.T) map[string]func() (*httptest.ResponseRecorder, string) {
	t.Helper()
	commitID := "65f1bf27bc3bf70f64657658635e66094edbcb4d"
	run := func(handler func(*context.Context), form *forms.CommitNotesForm) (*httptest.ResponseRecorder, string) {
		ctx, resp := contexttest.MockContext(t, "/user2/repo1/commit/"+commitID)
		ctx.SetParams(":sha", commitID)
		ctx.SetParams(":index", "5")
		contexttest.LoadUser(t, ctx, 2)
		contexttest.LoadRepo(t, ctx, 1)
		contexttest.LoadGitRepo(t, ctx)
		defer ctx.Repo.GitRepo.Close()
		if form != nil {
			web.SetForm(ctx, form)
		}

		handler(ctx)

		note, err := git.GetNote(ctx, ctx.Repo.GitRepo, commitID)
		if git.IsErrNotExist(err) {
			return resp, ""
		}
		require.NoError(t, err)
		return resp, string(note.Message)
	}
	return map[string]func() (*httptest.ResponseRecorder, string){
		"SetCommitNotes": func() (*httptest.ResponseRecorder, string) {
			return run(SetCommitNotes, &forms.CommitNotesForm{Notes: "set from the commit page"})
		},
		"RemoveCommitNotes": func() (*httptest.ResponseRecorder, string) {
			return run(RemoveCommitNotes, nil)
		},
		"SetCommitNotesPullRequest": func() (*httptest.ResponseRecorder, string) {
			return run(SetCommitNotesPullRequest, &forms.CommitNotesForm{Notes: "set from a pull request"})
		},
		"RemoveCommitNotesPullRequest": func() (*httptest.ResponseRecorder, string) {
			return run(RemoveCommitNotesPullRequest, nil)
		},
	}
}

// A note set or removed from the web is built on this node's notes: on a
// node of the cluster behind on the repository, the notes another node
// acknowledged were dropped for the whole cluster (#324). Each form is
// refused with 503 and the reason, as the other web writes are, and nothing
// is written: the user retries.
func TestNotesFormsRefusedOnANodeBehind(t *testing.T) {
	unittest.PrepareTestEnv(t)
	clusterhook.SetFreshWaiter(func(gocontext.Context, string) bool { return false })
	t.Cleanup(func() { clusterhook.SetFreshWaiter(nil) })

	for handler, submit := range notesForms(t) {
		resp, note := submit()

		assert.Equal(t, http.StatusServiceUnavailable, resp.Code, "%s: %s", handler, resp.Body.String())
		assert.Contains(t, resp.Body.String(), "may be behind", handler)
		assert.Equal(t, "This is a test note\n", note, "%s wrote the note", handler)
	}
}

// A node of the cluster that serves the repository at the cluster's commit
// index writes each form, having asked once, and redirects as before.
func TestNotesFormsOnAFreshNode(t *testing.T) {
	unittest.PrepareTestEnv(t)
	asked := 0
	clusterhook.SetFreshWaiter(func(_ gocontext.Context, key string) bool {
		assert.Equal(t, "user2/repo1.git", key)
		asked++
		return true
	})
	t.Cleanup(func() { clusterhook.SetFreshWaiter(nil) })
	submit := notesForms(t)

	for _, step := range []struct{ handler, note string }{
		{"SetCommitNotes", "set from the commit page\n"},
		{"RemoveCommitNotes", ""},
		{"SetCommitNotesPullRequest", "set from a pull request\n"},
		{"RemoveCommitNotesPullRequest", ""},
	} {
		before := asked
		resp, note := submit[step.handler]()

		assert.Equal(t, http.StatusSeeOther, resp.Code, "%s: %s", step.handler, resp.Body.String())
		assert.Equal(t, step.note, note, step.handler)
		assert.Equal(t, before+1, asked, step.handler)
	}
}
