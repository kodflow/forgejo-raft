// Copyright 2026 The Forgejo Authors. All rights reserved.
// SPDX-License-Identifier: GPL-3.0-or-later

package repo

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"forgejo.org/models/unittest"
	"forgejo.org/modules/git"
	api "forgejo.org/modules/structs"
	"forgejo.org/modules/test"
	"forgejo.org/modules/web"
	app_context "forgejo.org/services/context"
	"forgejo.org/services/contexttest"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// notedCommit is the commit of repo1 that the fixtures give a note.
const notedCommit = "65f1bf27bc3bf70f64657658635e66094edbcb4d"

// notesRequest runs handler on repo1's note of notedCommit, as user2, with
// form when it is not nil, and returns the response and the note of
// notedCommit afterwards, "" when there is none.
func notesRequest(t *testing.T, method string, handler func(*app_context.APIContext), form *api.NoteOptions) (*httptest.ResponseRecorder, string) {
	t.Helper()
	ctx, resp := contexttest.MockAPIContext(t, method+" /api/v1/repos/user2/repo1/git/notes/"+notedCommit)
	contexttest.LoadRepo(t, ctx, 1)
	contexttest.LoadUser(t, ctx, 2)
	contexttest.LoadGitRepo(t, ctx)
	defer ctx.Repo().GitRepo.Close()
	ctx.SetParams(":sha", notedCommit)
	if form != nil {
		web.SetForm(ctx, form)
	}

	handler(ctx)

	note, err := git.GetNote(ctx, ctx.Repo().GitRepo, notedCommit)
	if git.IsErrNotExist(err) {
		return resp, ""
	}
	require.NoError(t, err)
	return resp, string(note.Message)
}

// A note set or removed through the API is built on this node's notes: on a
// node of the cluster behind on the repository, the notes another node
// acknowledged were dropped for the whole cluster, with a 200 (#324). The
// request is refused with 503 and the reason, and nothing is written.
func TestNotesRefusedOnANodeBehind(t *testing.T) {
	unittest.PrepareTestEnv(t)
	defer test.MockVariableValue(&clusterWriteRefusal, func(context.Context) error { return nil })()
	defer test.MockVariableValue(&clusterFresh, behind)()

	resp, note := notesRequest(t, "POST", SetNote, &api.NoteOptions{Message: "set behind"})
	assert.Equal(t, http.StatusServiceUnavailable, resp.Code)
	assert.Contains(t, resp.Body.String(), "may be behind")
	assert.Equal(t, "This is a test note\n", note, "the note is left as it was")

	resp, note = notesRequest(t, "DELETE", RemoveNote, nil)
	assert.Equal(t, http.StatusServiceUnavailable, resp.Code)
	assert.Contains(t, resp.Body.String(), "may be behind")
	assert.Equal(t, "This is a test note\n", note, "the note is left as it was")
}

// A node of the cluster that serves the repository at the cluster's commit
// index sets and removes the note, having asked once for each.
func TestNotesOnAFreshNode(t *testing.T) {
	unittest.PrepareTestEnv(t)
	asked := 0
	defer test.MockVariableValue(&clusterWriteRefusal, func(context.Context) error { return nil })()
	defer test.MockVariableValue(&clusterFresh, func(_ context.Context, key string) error {
		assert.Equal(t, "user2/repo1.git", key)
		asked++
		return nil
	})()

	resp, note := notesRequest(t, "POST", SetNote, &api.NoteOptions{Message: "set on a fresh node"})
	assert.Equal(t, http.StatusOK, resp.Code, resp.Body.String())
	assert.Equal(t, "set on a fresh node\n", note)
	assert.Equal(t, 1, asked)

	resp, note = notesRequest(t, "DELETE", RemoveNote, nil)
	assert.Equal(t, http.StatusNoContent, resp.Code, resp.Body.String())
	assert.Empty(t, note, "the note is removed")
	assert.Equal(t, 2, asked)
}
