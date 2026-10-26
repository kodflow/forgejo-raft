// Copyright 2026 The Forgejo Authors. All rights reserved.
// SPDX-License-Identifier: GPL-3.0-or-later

package repo

import (
	gocontext "context"
	"net/http"
	"strconv"
	"testing"

	"forgejo.org/models/db"
	issues_model "forgejo.org/models/issues"
	"forgejo.org/models/unittest"
	user_model "forgejo.org/models/user"
	"forgejo.org/modules/clusterhook"
	api "forgejo.org/modules/structs"
	"forgejo.org/modules/test"
	"forgejo.org/modules/web"
	"forgejo.org/services/context"
	"forgejo.org/services/contexttest"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// reviewCounts counts the reviews and the comments of pull request 2's
// issue, index 3 of repo1.
func reviewCounts(t *testing.T) (reviews, comments int) {
	t.Helper()
	return unittest.GetCount(t, &issues_model.Review{IssueID: 3}), unittest.GetCount(t, &issues_model.Comment{IssueID: 3})
}

// apiReviewCtx is the API context of a review of pull request 2 by user2,
// repo1's owner.
func apiReviewCtx(t *testing.T, path string) (*context.APIContext, func() (int, string)) {
	t.Helper()
	ctx, resp := contexttest.MockAPIContext(t, path)
	ctx.SetParams(":index", "3")
	contexttest.LoadRepo(t, ctx, 1)
	contexttest.LoadUser(t, ctx, 2)
	contexttest.LoadGitRepo(t, ctx)
	t.Cleanup(func() { ctx.Repo().GitRepo.Close() })
	return ctx, func() (int, string) { return ctx.Resp.Status(), resp.Body.String() }
}

// An approval or a rejection created through the API on a node of the
// cluster behind on the pull request's repository is refused with 503
// before the head is read from the node's git, with the commit_id a page
// of the node showed as with none, which defaults to that head: nothing is
// recorded, its comments neither. A comment judges no head, and is
// recorded (#264).
func TestCreatePullReviewRefusedOnANodeBehind(t *testing.T) {
	unittest.PrepareTestEnv(t)
	defer test.MockVariableValue(&clusterFresh, behind)()
	reviews, comments := reviewCounts(t)

	for _, opts := range []*api.CreatePullReviewOptions{
		{Event: api.ReviewStateApproved},
		{Event: api.ReviewStateApproved, CommitID: "4a357436d925b5c974181ff12a994538ddc5a269"},
		{Event: api.ReviewStateRequestChanges, Body: "not this"},
	} {
		opts.Comments = []api.CreatePullReviewComment{{Path: "README.md", Body: "a line", NewLineNum: 1}}
		ctx, answer := apiReviewCtx(t, "POST /api/v1/repos/user2/repo1/pulls/3/reviews")
		web.SetForm(ctx, opts)
		CreatePullReview(ctx)
		code, body := answer()
		assert.Equal(t, http.StatusServiceUnavailable, code, "%s %.10q: %s", opts.Event, opts.CommitID, body)
		assert.Contains(t, body, "may be behind")
	}
	r, c := reviewCounts(t)
	assert.Equal(t, reviews, r, "no review recorded")
	assert.Equal(t, comments, c, "no comment recorded")

	ctx, answer := apiReviewCtx(t, "POST /api/v1/repos/user2/repo1/pulls/3/reviews")
	web.SetForm(ctx, &api.CreatePullReviewOptions{Event: api.ReviewStateComment, Body: "a comment"})
	CreatePullReview(ctx)
	code, body := answer()
	assert.Equal(t, http.StatusOK, code, body)
	unittest.AssertExistsAndLoadBean(t, &issues_model.Review{IssueID: 3, ReviewerID: 2, Type: issues_model.ReviewTypeComment, Content: "a comment"})
}

// A pending review submitted through the API as an approval or a
// rejection on a node of the cluster behind on the pull request's
// repository is refused with 503 before the head it judges is read from
// the node's git, and stays pending (#264). So is one refused by
// pull_service.SubmitReview's own wait, not a 500.
func TestSubmitPullReviewRefusedOnANodeBehind(t *testing.T) {
	unittest.PrepareTestEnv(t)
	ctx := db.DefaultContext
	issue := unittest.AssertExistsAndLoadBean(t, &issues_model.Issue{ID: 3})
	require.NotEqual(t, int64(2), issue.PosterID, "user2 may approve")
	reviewer := unittest.AssertExistsAndLoadBean(t, &user_model.User{ID: 2})
	pending, err := issues_model.CreateReview(ctx, issues_model.CreateReviewOptions{
		Type: issues_model.ReviewTypePending, Issue: issue, Reviewer: reviewer, CommitID: "4a357436d925b5c974181ff12a994538ddc5a269",
	})
	require.NoError(t, err)

	submit := func(event api.ReviewStateType) (int, string) {
		apiCtx, answer := apiReviewCtx(t, "POST /api/v1/repos/user2/repo1/pulls/3/reviews/"+strconv.FormatInt(pending.ID, 10))
		apiCtx.SetParams(":id", strconv.FormatInt(pending.ID, 10))
		web.SetForm(apiCtx, &api.SubmitPullReviewOptions{Event: event, Body: "submitted on a node behind"})
		SubmitPullReview(apiCtx)
		return answer()
	}

	t.Run("refused before the head is read", func(t *testing.T) {
		defer test.MockVariableValue(&clusterFresh, behind)()
		for _, event := range []api.ReviewStateType{api.ReviewStateApproved, api.ReviewStateRequestChanges} {
			code, body := submit(event)
			assert.Equal(t, http.StatusServiceUnavailable, code, "%s: %s", event, body)
			assert.Contains(t, body, "may be behind")
		}
	})
	t.Run("refused by SubmitReview", func(t *testing.T) {
		defer test.MockVariableValue(&clusterFresh, func(gocontext.Context, string) error { return nil })()
		clusterhook.SetFreshWaiter(func(gocontext.Context, string) bool { return false })
		t.Cleanup(func() { clusterhook.SetFreshWaiter(nil) })
		code, body := submit(api.ReviewStateApproved)
		assert.Equal(t, http.StatusServiceUnavailable, code, body)
		assert.Contains(t, body, "may be behind")
	})
	still := unittest.AssertExistsAndLoadBean(t, &issues_model.Review{ID: pending.ID})
	assert.Equal(t, issues_model.ReviewTypePending, still.Type, "the review stays pending")
}
