// Copyright 2026 The Forgejo Authors. All rights reserved.
// SPDX-License-Identifier: GPL-3.0-or-later

package pull

import (
	"context"
	"testing"

	"forgejo.org/models/db"
	git_model "forgejo.org/models/git"
	issues_model "forgejo.org/models/issues"
	repo_model "forgejo.org/models/repo"
	"forgejo.org/models/unittest"
	user_model "forgejo.org/models/user"
	"forgejo.org/modules/clusterhook"
	"forgejo.org/modules/git"
	"forgejo.org/modules/timeutil"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// replacedHead is pull request 2 (branch2 into master of repository 1)
// and a push that moved branch2 from f1 to f2, published on this node.
// master's protection dismisses stale approvals.
type replacedHead struct {
	pr       *issues_model.PullRequest
	doer     *user_model.User
	dir      string
	f1, f2   string
	pushTime int64
	asked    func() []string
}

// newReplacedHead sets up a cluster's node that serves the repository
// fresh, unless fresh says otherwise, and the push, whose transaction was
// built at pushTime.
func newReplacedHead(t *testing.T, fresh bool) *replacedHead {
	t.Helper()
	unittest.PrepareTestEnv(t)
	ctx := db.DefaultContext
	patchChecker(t)
	was := clusterhook.Enabled()
	clusterhook.SetEnabled(true)
	t.Cleanup(func() { clusterhook.SetEnabled(was) })
	r := &replacedHead{
		doer:     unittest.AssertExistsAndLoadBean(t, &user_model.User{ID: 2}),
		pr:       unittest.AssertExistsAndLoadBean(t, &issues_model.PullRequest{ID: 2}),
		dir:      unittest.AssertExistsAndLoadBean(t, &repo_model.Repository{ID: 1}).RepoPath(),
		pushTime: int64(timeutil.TimeStampNanoNow()),
	}
	r.asked = freshWaiter(t, func(context.Context, string) bool { return fresh })
	require.NoError(t, r.pr.LoadIssue(ctx))
	require.NoError(t, db.Insert(ctx, &git_model.ProtectedBranch{RepoID: 1, RuleName: "master", DismissStaleApprovals: true}))
	r.f1 = r.ref(t, "refs/heads/branch2")
	r.f2 = commitProtectedFile(ctx, t, r.dir)
	return r
}

// openOn records the pull request as created after the push's
// transaction was built, on head, its refs/pull/N/head, with an official
// approval of head.
func (r *replacedHead) openOn(t *testing.T, head string) *issues_model.Review {
	t.Helper()
	ctx := db.DefaultContext
	_, _, runErr := git.NewCommand(ctx, "update-ref").AddDynamicArguments(r.pr.GetGitRefName(), head).RunStdString(&git.RunOpts{Dir: r.dir})
	require.NoError(t, runErr)
	r.pr.Issue.Created = timeutil.TimeStampNano(r.pushTime + 1)
	require.NoError(t, issues_model.UpdateIssueCols(ctx, r.pr.Issue, "created"))
	approval, err := issues_model.CreateReview(ctx, issues_model.CreateReviewOptions{
		Type: issues_model.ReviewTypeApprove, Issue: r.pr.Issue, Official: true, CommitID: head,
		Reviewer: unittest.AssertExistsAndLoadBean(t, &user_model.User{ID: 1}),
	})
	require.NoError(t, err)
	return approval
}

// derive runs the push's work, as the consumer of the cluster derives it:
// with the time its transaction was built.
func (r *replacedHead) derive() {
	TestPullRequest(db.DefaultContext, r.doer, 1, r.pushTime, "branch2", true, r.f1, r.f2)
}

func (r *replacedHead) ref(t *testing.T, name string) string {
	t.Helper()
	gitRepo, err := git.OpenRepository(db.DefaultContext, r.dir)
	require.NoError(t, err)
	defer gitRepo.Close()
	sha, err := gitRepo.GetRefCommitID(name)
	require.NoError(t, err)
	return sha
}

func (r *replacedHead) pushComments(t *testing.T) int64 {
	t.Helper()
	n, err := db.GetEngine(db.DefaultContext).Where("issue_id = ? AND type = ?", r.pr.IssueID, issues_model.CommentTypePullRequestPush).Count(new(issues_model.Comment))
	require.NoError(t, err)
	return n
}

// A pull request created while a push to its head branch was in flight,
// on the commit the push replaced: the consumer derives the push with the
// time its transaction was built, before the pull request existed, and
// TestPullRequest passed it over. It kept its head and an approval of the
// commit before the push for good (#366). In a cluster, it is now taken:
// its head follows the push, a push comment is written, it is handed to
// the patch checker, and the approval is dismissed. A second run of the
// same work takes it no more. Without a cluster nothing changes: the time
// is taken once git published the ref.
func TestTestPullRequestTakesAPullRequestOpenedOnTheHeadThePushReplaced(t *testing.T) {
	r := newReplacedHead(t, true)
	approval := r.openOn(t, r.f1)

	clusterhook.SetEnabled(false)
	r.derive()
	clusterhook.SetEnabled(true)
	assert.Equal(t, r.f1, r.ref(t, r.pr.GetGitRefName()), "without a cluster, passed over as upstream")
	assert.Zero(t, r.pushComments(t))

	r.derive()
	assert.Equal(t, r.f2, r.ref(t, r.pr.GetGitRefName()), "the pull request's head follows the push")
	assert.EqualValues(t, 1, r.pushComments(t))
	pr := unittest.AssertExistsAndLoadBean(t, &issues_model.PullRequest{ID: 2})
	assert.Equal(t, issues_model.PullRequestStatusChecking, pr.Status, "handed to the patch checker")
	got := unittest.AssertExistsAndLoadBean(t, &issues_model.Review{ID: approval.ID})
	assert.True(t, got.Dismissed, "the approval of the commit before the push is dismissed")
	assert.True(t, got.Stale)
	assert.Contains(t, r.asked(), "user2/repo1.git", "the head is read once this node serves the base repository")

	r.derive()
	assert.EqualValues(t, 1, r.pushComments(t), "a second run takes it no more")
}

// A pull request created after the push's transaction was built, but on
// the pushed commit: it has the push already, and the push's work leaves
// it, and its approval of that commit, as they are.
func TestTestPullRequestLeavesAPullRequestOpenedOnThePushedHead(t *testing.T) {
	r := newReplacedHead(t, true)
	approval := r.openOn(t, r.f2)

	r.derive()
	assert.Equal(t, r.f2, r.ref(t, r.pr.GetGitRefName()))
	assert.Zero(t, r.pushComments(t))
	pr := unittest.AssertExistsAndLoadBean(t, &issues_model.PullRequest{ID: 2})
	assert.Equal(t, issues_model.PullRequestStatusMergeable, pr.Status)
	got := unittest.AssertExistsAndLoadBean(t, &issues_model.Review{ID: approval.ID})
	assert.False(t, got.Dismissed, "an approval of the pushed commit stays")
	assert.False(t, got.Stale)
}

// On a node that may be behind the cluster on the base repository, the
// pull request's head is not read, and the pull request is left as it is.
func TestTestPullRequestLeavesAPullRequestCreatedAfterThePushOnANodeBehind(t *testing.T) {
	r := newReplacedHead(t, false)
	approval := r.openOn(t, r.f1)

	r.derive()
	assert.Equal(t, r.f1, r.ref(t, r.pr.GetGitRefName()))
	assert.Zero(t, r.pushComments(t))
	got := unittest.AssertExistsAndLoadBean(t, &issues_model.Review{ID: approval.ID})
	assert.False(t, got.Dismissed)
	assert.Contains(t, r.asked(), "user2/repo1.git")
}
