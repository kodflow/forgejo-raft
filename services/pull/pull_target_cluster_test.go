// Copyright 2026 The Forgejo Authors. All rights reserved.
// SPDX-License-Identifier: GPL-3.0-or-later

package pull

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"forgejo.org/models/db"
	git_model "forgejo.org/models/git"
	issues_model "forgejo.org/models/issues"
	repo_model "forgejo.org/models/repo"
	"forgejo.org/models/unittest"
	user_model "forgejo.org/models/user"
	"forgejo.org/modules/clusterhook"
	"forgejo.org/modules/git"
	"forgejo.org/modules/queue"
	"forgejo.org/modules/setting"
	"forgejo.org/modules/test"
	shared_automerge "forgejo.org/services/shared/automerge"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// targetPR loads pull request 2 (branch2 into master of repository 1) as
// the routes hand it to ChangeTargetBranch: its issue and the issue's
// repository loaded.
func targetPR(t *testing.T) *issues_model.PullRequest {
	t.Helper()
	pr := unittest.AssertExistsAndLoadBean(t, &issues_model.PullRequest{ID: 2})
	require.NoError(t, pr.LoadIssue(db.DefaultContext))
	require.NoError(t, pr.Issue.LoadRepo(db.DefaultContext))
	return pr
}

// retargetComments counts the comments recording a change of the target
// branch of pull request 2.
func retargetComments(t *testing.T) int64 {
	t.Helper()
	pr := unittest.AssertExistsAndLoadBean(t, &issues_model.PullRequest{ID: 2})
	n, err := db.GetEngine(db.DefaultContext).Where("issue_id = ? AND type = ?", pr.IssueID, issues_model.CommentTypeChangeTargetBranch).Count(new(issues_model.Comment))
	require.NoError(t, err)
	return n
}

// On a node of a cluster behind on the pull request's repositories, a
// change of its target branch is refused with ErrBehind before the patch
// is tested on the node's git: the head there may be the commit before a
// push another node acknowledged, and the status, merge base, changed
// protected files and divergence the change writes would be those of that
// commit, which the merge checks then trust (#300). Nothing is written.
func TestChangeTargetBranchRefusedOnANodeBehind(t *testing.T) {
	unittest.PrepareTestEnv(t)
	ctx := db.DefaultContext
	asked := freshWaiter(t, func(context.Context, string) bool { return false })
	doer := unittest.AssertExistsAndLoadBean(t, &user_model.User{ID: 2})
	before := unittest.AssertExistsAndLoadBean(t, &issues_model.PullRequest{ID: 2})

	err := ChangeTargetBranch(ctx, targetPR(t), doer, "develop")
	require.ErrorIs(t, err, clusterhook.ErrBehind)
	assert.Contains(t, err.Error(), "user2/repo1.git")

	after := unittest.AssertExistsAndLoadBean(t, &issues_model.PullRequest{ID: 2})
	assert.Equal(t, "master", after.BaseBranch, "the target is unchanged")
	assert.Equal(t, before.Status, after.Status)
	assert.Equal(t, before.MergeBase, after.MergeBase)
	assert.Equal(t, before.ChangedProtectedFiles, after.ChangedProtectedFiles)
	assert.Equal(t, before.CommitsAhead, after.CommitsAhead)
	assert.Equal(t, before.CommitsBehind, after.CommitsBehind)
	assert.Zero(t, retargetComments(t), "no retarget comment")
	assert.Contains(t, asked(), "user2/repo1.git")
}

// A push to the head that another node of the cluster acknowledged reaches
// this node's git while the change of the target branch waits for it to
// serve the repository: the patch is tested on the pushed commit, whose
// change to a file the new target protects is recorded, and the merge
// checks refuse it. A node that serves the cluster's writes changes the
// target as before.
func TestChangeTargetBranchTestsTheHeadOnceFresh(t *testing.T) {
	unittest.PrepareTestEnv(t)
	ctx := db.DefaultContext
	require.NoError(t, db.Insert(ctx, &git_model.ProtectedBranch{RepoID: 1, RuleName: "develop", ProtectedFilePatterns: "secret.txt"}))
	dir := unittest.AssertExistsAndLoadBean(t, &repo_model.Repository{ID: 1}).RepoPath()
	var pushed string
	asked := freshWaiter(t, func(ctx context.Context, _ string) bool {
		if pushed == "" {
			pushed = commitProtectedFile(ctx, t, dir)
		}
		return true
	})
	doer := unittest.AssertExistsAndLoadBean(t, &user_model.User{ID: 2})

	require.NoError(t, ChangeTargetBranch(ctx, targetPR(t), doer, "develop"))

	require.NotEmpty(t, pushed, "the change of the target waited for this node to serve the repository")
	assert.Contains(t, asked(), "user2/repo1.git")
	after := unittest.AssertExistsAndLoadBean(t, &issues_model.PullRequest{ID: 2})
	assert.Equal(t, "develop", after.BaseBranch)
	assert.Equal(t, issues_model.PullRequestStatusMergeable, after.Status)
	assert.Contains(t, after.ChangedProtectedFiles, "secret.txt", "the pushed commit changes a file develop protects")
	assert.EqualValues(t, 1, retargetComments(t))
	_, err := CheckPullBranchProtections(ctx, after, false)
	assert.ErrorContains(t, err, "Changed protected files")
}

// commitProtectedFile commits, on top of branch2 of repository 1, a commit
// adding secret.txt, and moves branch2 to it, as a push another node of
// the cluster acknowledged reaches this node's git once it projects it.
// The index it builds the tree in is a file of the test's own directory.
func commitProtectedFile(ctx context.Context, t *testing.T, dir string) string {
	t.Helper()
	env := append(os.Environ(), "GIT_INDEX_FILE="+filepath.Join(t.TempDir(), "index"),
		"GIT_AUTHOR_NAME=t", "GIT_AUTHOR_EMAIL=t@t", "GIT_COMMITTER_NAME=t", "GIT_COMMITTER_EMAIL=t@t")
	opts := &git.RunOpts{Dir: dir, Env: env}
	runGit := func(c *git.Command, stdin string) string {
		o := *opts
		if stdin != "" {
			o.Stdin = strings.NewReader(stdin)
		}
		out, _, err := c.RunStdString(&o)
		require.NoError(t, err)
		return strings.TrimSpace(out)
	}
	runGit(git.NewCommand(ctx, "read-tree", "refs/heads/branch2"), "")
	blob := runGit(git.NewCommand(ctx, "hash-object", "-w", "--stdin"), "changed\n")
	runGit(git.NewCommand(ctx, "update-index", "--add", "--cacheinfo").AddDynamicArguments("100644,"+blob+",secret.txt"), "")
	tree := runGit(git.NewCommand(ctx, "write-tree"), "")
	sha := runGit(git.NewCommand(ctx, "commit-tree", "-m", "touch secret", "-p", "refs/heads/branch2").AddDynamicArguments(tree), "")
	runGit(git.NewCommand(ctx, "update-ref", "refs/heads/branch2").AddDynamicArguments(sha), "")
	return sha
}

// patchChecker gives the test a pr_patch_checker queue, which
// checkAndUpdateStatus asks whether a pull request waits for another
// test, and stands for the auto-merge queue a mergeable pull request is
// handed to: this binary starts neither.
func patchChecker(t *testing.T) {
	t.Helper()
	cfg, err := setting.GetQueueSettings(setting.CfgProvider, "pr_patch_checker")
	require.NoError(t, err)
	q, err := queue.NewWorkerPoolQueueWithContext(t.Context(), "pr_patch_checker", cfg, func(...string) []string { return nil }, true)
	require.NoError(t, err)
	prPatchCheckerQueue = q
	go q.Run()
	t.Cleanup(func() {
		q.ShutdownWait(5 * time.Second)
		prPatchCheckerQueue = nil
	})
	t.Cleanup(test.MockVariableValue(&shared_automerge.AddToQueueIfMergeable, func(context.Context, *issues_model.PullRequest) {}))
}

// The patch checker queue tests a pull request on the git of the node
// that handles it: a reopened pull request, one whose base's protection
// changed, any node's queue at its start. On a node of a cluster behind
// on its repositories, the head there may be the commit before a push
// another node acknowledged, and the columns written from it are those
// the merge checks trust (#300). The test is now deferred, nothing
// written, the pull request left checking, which no merge passes, and
// handed back to the queue; once the node serves the repositories, the
// test runs and writes its result.
func TestPatchCheckDeferredOnANodeBehind(t *testing.T) {
	unittest.PrepareTestEnv(t)
	ctx := db.DefaultContext
	patchChecker(t)
	var fresh atomic.Bool
	asked := freshWaiter(t, func(context.Context, string) bool { return fresh.Load() })
	_, err := db.GetEngine(ctx).ID(2).Cols("status", "merge_base").Update(&issues_model.PullRequest{Status: issues_model.PullRequestStatusChecking, MergeBase: "not tested yet"})
	require.NoError(t, err)

	deferred := handler("2")
	assert.Equal(t, []string{"2"}, deferred, "the test is handed back to the queue, not dropped")
	pr := unittest.AssertExistsAndLoadBean(t, &issues_model.PullRequest{ID: 2})
	assert.Equal(t, issues_model.PullRequestStatusChecking, pr.Status, "the pull request is still checking")
	assert.Equal(t, "not tested yet", pr.MergeBase, "nothing written")
	assert.False(t, pr.HasMerged)
	assert.Contains(t, asked(), "user2/repo1.git")

	// The node serves the repositories: the test handed back runs.
	fresh.Store(true)
	assert.Empty(t, handler(deferred...), "nothing handed back")
	pr = unittest.AssertExistsAndLoadBean(t, &issues_model.PullRequest{ID: 2})
	assert.Equal(t, issues_model.PullRequestStatusMergeable, pr.Status)
	dir := unittest.AssertExistsAndLoadBean(t, &repo_model.Repository{ID: 1}).RepoPath()
	mergeBase, _, err := git.NewCommand(ctx, "merge-base", "refs/heads/master", "refs/heads/branch2").RunStdString(&git.RunOpts{Dir: dir})
	require.NoError(t, err)
	assert.Equal(t, strings.TrimSpace(mergeBase), pr.MergeBase, "the test wrote its merge base")
}

// secondChild opens, in repository 1, a pull request of
// home-md-img-check into branch2: with pull request 5 (pr-to-update into
// branch2), the second child of pull request 2 (branch2 into master).
func secondChild(t *testing.T) *issues_model.PullRequest {
	t.Helper()
	ctx := db.DefaultContext
	issue := &issues_model.Issue{RepoID: 1, Index: 1000, PosterID: 2, IsPull: true, Title: "second child of branch2"}
	require.NoError(t, db.Insert(ctx, issue))
	pr := &issues_model.PullRequest{
		IssueID: issue.ID, Index: issue.Index, HeadRepoID: 1, BaseRepoID: 1,
		HeadBranch: "home-md-img-check", BaseBranch: "branch2",
		Type: issues_model.PullRequestGitea, Status: issues_model.PullRequestStatusMergeable,
	}
	require.NoError(t, db.Insert(ctx, pr))
	return pr
}

// The pull requests based on a merged pull request's head branch are
// retargeted before the branch is deleted (RetargetChildrenOnMerge), and
// any child's error keeps the branch. Since #300 a child's retarget on a
// node of a cluster behind on its repositories is refused with ErrBehind,
// and the errors of the children hid it from errors.Is: the deletion of
// the branch after an auto-merge took it for a failure and dropped it for
// good (#279), the web for a failed deletion. The refusal now stays an
// ErrBehind, and the first one stops the loop: each child would wait for
// the node again, in the request that merged the parent. Nothing is
// retargeted; once the node serves the repositories, every child is.
func TestRetargetChildrenOnMergeRefusedOnANodeBehind(t *testing.T) {
	unittest.PrepareTestEnv(t)
	defer test.MockVariableValue(&setting.Repository.PullRequest.RetargetChildrenOnMerge, true)()
	ctx := db.DefaultContext
	var fresh atomic.Bool
	asked := freshWaiter(t, func(context.Context, string) bool { return fresh.Load() })
	doer := unittest.AssertExistsAndLoadBean(t, &user_model.User{ID: 2})
	// Pull request 2 (branch2 into master) is the parent of pull request 5
	// (pr-to-update into branch2) and of second.
	parent := unittest.AssertExistsAndLoadBean(t, &issues_model.PullRequest{ID: 2})
	second := secondChild(t)
	children := []int64{5, second.ID}
	retargets := func() int64 {
		n, err := db.GetEngine(ctx).Where("type = ?", issues_model.CommentTypeChangeTargetBranch).In("issue_id", []int64{11, second.IssueID}).Count(new(issues_model.Comment))
		require.NoError(t, err)
		return n
	}

	err := RetargetChildrenOnMerge(ctx, doer, parent)
	require.ErrorIs(t, err, clusterhook.ErrBehind, "the refusal of a child is a refusal to retry: %v", err)
	assert.Len(t, asked(), 1, "the first refusal stops the loop: %v", asked())
	for _, id := range children {
		assert.Equal(t, "branch2", unittest.AssertExistsAndLoadBean(t, &issues_model.PullRequest{ID: id}).BaseBranch, "child %d keeps its target", id)
	}
	assert.Zero(t, retargets(), "no retarget comment")

	fresh.Store(true)
	require.NoError(t, RetargetChildrenOnMerge(ctx, doer, parent))
	for _, id := range children {
		assert.Equal(t, "master", unittest.AssertExistsAndLoadBean(t, &issues_model.PullRequest{ID: id}).BaseBranch, "child %d is retargeted", id)
	}
	assert.EqualValues(t, 2, retargets())
}

// The other errors of a child's retarget keep their behaviour: returned,
// so that the branch is kept, and not taken for a node behind, which the
// callers would retry. A new target the repository does not hold is one.
func TestRetargetBranchPullsKeepsTheOtherRefusals(t *testing.T) {
	unittest.PrepareTestEnv(t)
	ctx := db.DefaultContext
	freshWaiter(t, func(context.Context, string) bool { return true })
	doer := unittest.AssertExistsAndLoadBean(t, &user_model.User{ID: 2})

	err := RetargetBranchPulls(ctx, doer, 1, "branch2", "no-such-branch")
	require.Error(t, err)
	require.NotErrorIs(t, err, clusterhook.ErrBehind, "%v", err)
	assert.Equal(t, "branch2", unittest.AssertExistsAndLoadBean(t, &issues_model.PullRequest{ID: 5}).BaseBranch)

	// The errors of the children read as before, and a refusal among
	// them is found.
	other := errors.New("other child")
	behind := fmt.Errorf("a child: %w", clusterhook.ErrBehind)
	assert.Equal(t, "other child, "+behind.Error(), errlist{other, behind}.Error())
	require.ErrorIs(t, errlist{other, behind}, clusterhook.ErrBehind)
	assert.NotErrorIs(t, errlist{other}, clusterhook.ErrBehind)
}
