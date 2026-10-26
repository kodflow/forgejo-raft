// Copyright 2026 The Forgejo Authors. All rights reserved.
// SPDX-License-Identifier: GPL-3.0-or-later

package cluster

import (
	"testing"

	"forgejo.org/models/db"
	git_model "forgejo.org/models/git"
	issues_model "forgejo.org/models/issues"
	repo_model "forgejo.org/models/repo"
	"forgejo.org/models/unittest"
	"forgejo.org/modules/clusterhook"
	"forgejo.org/modules/gitrepo"
	pull_service "forgejo.org/services/pull"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// A pull request's head, refs/pull/N/head, moves from f1 to f2 through
// node-b, acknowledged, while node-a does not project it. A reviewer
// approves, through node-a, the head a page of node-a shows, f1:
// pull_service.SubmitReview read the head from node-a's git, found f1,
// and recorded the approval as not stale, which branch protection with
// IgnoreStaleApprovals counted for a head no one approved; nothing marks
// it stale before the next push, MarkReviewsAsStale running in the push's
// own work only (#264). An approval or a rejection now waits for the node
// to serve the repository at the cluster's commit index before it reads
// the head, and is refused, recording nothing, while it does not; once
// the node serves f2, an approval of f1 is stale, as without a cluster.
func TestReviewOnALaggingNodeJudgesTheAcknowledgedHead(t *testing.T) {
	tc, a, b, work, _, _, hold := laggingCluster(t)
	ctx := db.DefaultContext
	lm := openLagMerge(t, tc, a, work, hold, issues_model.PullRequestFlowAGit)

	// The bench closed the issue for its merges; a review is of an open
	// one, and the reviewer, user2, owns the repository: an official
	// reviewer, who may not approve a pull request of their own.
	_, err := db.GetEngine(ctx).ID(lm.pr.IssueID).Cols("is_closed", "poster_id").Update(&issues_model.Issue{IsClosed: false, PosterID: 4})
	require.NoError(t, err)
	pb := &git_model.ProtectedBranch{RepoID: 1, RuleName: "main", RequiredApprovals: 1, IgnoreStaleApprovals: true}
	require.NoError(t, db.Insert(ctx, pb))

	hold2 := holdA(t, a)
	f2 := commit(t, work, "feat two")
	pushOnB(t, tc, a, b, work, lm.head, lm.f1, f2)

	sqlRepo := unittest.AssertExistsAndLoadBean(t, &repo_model.Repository{ID: 1})
	gitRepo, err := gitrepo.OpenRepository(ctx, sqlRepo)
	require.NoError(t, err)
	defer gitRepo.Close()
	reviewer := lm.doer

	load := func() (*issues_model.Issue, *issues_model.PullRequest) {
		issue := unittest.AssertExistsAndLoadBean(t, &issues_model.Issue{ID: lm.pr.IssueID})
		pr := unittest.AssertExistsAndLoadBean(t, &issues_model.PullRequest{ID: lm.pr.ID})
		issue.PullRequest = pr
		pr.Issue = issue
		require.NoError(t, issue.LoadRepo(ctx))
		return issue, pr
	}
	decided := func() int64 {
		n, err := db.GetEngine(ctx).Where("issue_id = ?", lm.pr.IssueID).
			In("type", issues_model.ReviewTypeApprove, issues_model.ReviewTypeReject).Count(new(issues_model.Review))
		require.NoError(t, err)
		return n
	}
	granted := func() (int64, error) {
		_, pr := load()
		_, gate := pull_service.CheckPullBranchProtections(ctx, pr, true)
		return issues_model.GetGrantedApprovalsCount(ctx, pb, pr), gate
	}

	// node-a behind: an approval of the head its page shows, f1, of the
	// head a page of node-b shows, f2, and a rejection are refused, and
	// nothing is recorded.
	for _, c := range []struct {
		kind   issues_model.ReviewType
		commit string
	}{{issues_model.ReviewTypeApprove, lm.f1}, {issues_model.ReviewTypeApprove, f2}, {issues_model.ReviewTypeReject, lm.f1}} {
		issue, _ := load()
		review, _, err := pull_service.SubmitReview(ctx, reviewer, gitRepo, issue, c.kind, "", c.commit, nil)
		if err == nil {
			t.Logf("review %v of %.10s on node-a behind: stale=%v official=%v; the cluster's %s is f2=%.10s, node-a's git %.10s",
				c.kind, c.commit, review.Stale, review.Official, lm.head, f2, gitRef(t, a, laggingRepo, lm.head))
		}
		require.ErrorIs(t, err, clusterhook.ErrBehind, "review %v of %.10s on node-a behind", c.kind, c.commit)
	}
	assert.Zero(t, decided(), "node-a behind recorded no approval nor rejection")
	assert.Equal(t, lm.f1, gitRef(t, a, laggingRepo, lm.head), "node-a is still behind")

	// A comment decides nothing on the head: it is recorded.
	issue, _ := load()
	_, _, err = pull_service.SubmitReview(ctx, reviewer, gitRepo, issue, issues_model.ReviewTypeComment, "a comment", lm.f1, nil)
	require.NoError(t, err)

	// node-a serves f2: nothing was approved, and the merge gate holds.
	caughtUp(t, a, hold2, lm.head, f2)
	n, gate := granted()
	t.Logf("node-a caught up: granted approvals=%d, merge gate err=%v", n, gate)
	assert.Zero(t, n)
	require.Error(t, gate, "no one approved f2")

	// An approval of f1 is stale, as without a cluster; one of f2 is not.
	issue, _ = load()
	stale, _, err := pull_service.SubmitReview(ctx, reviewer, gitRepo, issue, issues_model.ReviewTypeApprove, "", lm.f1, nil)
	require.NoError(t, err)
	n, gate = granted()
	t.Logf("approval of f1 on node-a serving f2: stale=%v, granted approvals=%d, merge gate err=%v", stale.Stale, n, gate)
	assert.True(t, stale.Stale, "an approval of f1, once the cluster acknowledged f2 as the head, is stale")
	assert.Zero(t, n)
	require.Error(t, gate, "a stale approval opens no merge gate")

	issue, _ = load()
	fresh, _, err := pull_service.SubmitReview(ctx, reviewer, gitRepo, issue, issues_model.ReviewTypeApprove, "", f2, nil)
	require.NoError(t, err)
	n, gate = granted()
	t.Logf("approval of f2 on node-a serving f2: stale=%v, granted approvals=%d, merge gate err=%v", fresh.Stale, n, gate)
	assert.False(t, fresh.Stale)
	assert.EqualValues(t, 1, n)
	assert.NoError(t, gate)
}
