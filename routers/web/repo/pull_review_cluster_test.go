// Copyright 2026 The Forgejo Authors. All rights reserved.
// SPDX-License-Identifier: GPL-3.0-or-later

package repo

import (
	gocontext "context"
	"net/http"
	"testing"

	issues_model "forgejo.org/models/issues"
	repo_model "forgejo.org/models/repo"
	"forgejo.org/models/unittest"
	"forgejo.org/modules/clusterhook"
	"forgejo.org/modules/git"
	"forgejo.org/modules/web"
	"forgejo.org/services/contexttest"
	"forgejo.org/services/forms"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// The review form posted to a node of the cluster behind on the pull
// request's repository is refused with 503 and the reason for an approval
// or a rejection, of the head a page of that node rendered: the review
// would judge the head this node's git shows, before a push another node
// acknowledged, and nothing is recorded, the user retries. A comment
// judges no head, and is recorded (#264).
func TestSubmitReviewFormRefusedOnANodeBehind(t *testing.T) {
	unittest.PrepareTestEnv(t)
	clusterhook.SetFreshWaiter(func(gocontext.Context, string) bool { return false })
	t.Cleanup(func() { clusterhook.SetFreshWaiter(nil) })
	repoPath := unittest.AssertExistsAndLoadBean(t, &repo_model.Repository{ID: 1}).RepoPath()
	// What the page shows as the head: refs/pull/3/head in this node's git.
	shown, err := git.GetFullCommitID(t.Context(), repoPath, "refs/pull/3/head")
	require.NoError(t, err)

	submit := func(kind string) (int, string) {
		// Pull request 2, branch2 into master, is index 3 of repo1, whose
		// owner, user2, reviews it.
		ctx, resp := contexttest.MockContext(t, "user2/repo1/pulls/3/files/reviews/submit")
		contexttest.LoadUser(t, ctx, 2)
		contexttest.LoadRepo(t, ctx, 1)
		contexttest.LoadGitRepo(t, ctx)
		defer ctx.Repo.GitRepo.Close()
		ctx.SetParams(":index", "3")
		web.SetForm(ctx, &forms.SubmitReviewForm{Content: "reviewed on a node behind", Type: kind, CommitID: shown})
		SubmitReview(ctx)
		return resp.Code, resp.Body.String()
	}

	before := unittest.GetCount(t, &issues_model.Review{IssueID: 3})
	for _, kind := range []string{"approve", "reject"} {
		code, body := submit(kind)
		assert.Equal(t, http.StatusServiceUnavailable, code, "%s: %s", kind, body)
		assert.Contains(t, body, "may be behind", kind)
	}
	assert.Equal(t, before, unittest.GetCount(t, &issues_model.Review{IssueID: 3}), "nothing recorded")

	code, body := submit("comment")
	assert.NotEqual(t, http.StatusServiceUnavailable, code, body)
	unittest.AssertExistsAndLoadBean(t, &issues_model.Review{IssueID: 3, ReviewerID: 2, Type: issues_model.ReviewTypeComment, Content: "reviewed on a node behind"})
}
