// Copyright 2026 The Forgejo Authors. All rights reserved.
// SPDX-License-Identifier: GPL-3.0-or-later

package repo

import (
	gocontext "context"
	"net/http"
	"sync/atomic"
	"testing"

	"forgejo.org/models/db"
	git_model "forgejo.org/models/git"
	issues_model "forgejo.org/models/issues"
	repo_model "forgejo.org/models/repo"
	"forgejo.org/models/unittest"
	"forgejo.org/modules/clusterhook"
	"forgejo.org/modules/git"
	"forgejo.org/modules/setting"
	"forgejo.org/modules/test"
	"forgejo.org/services/contexttest"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// updateTarget posts, as the pull request page does, a new target branch
// for pull request 2 (branch2 into master, index 3 of repo1).
func updateTarget(t *testing.T, target string) (int, string) {
	t.Helper()
	ctx, resp := contexttest.MockContext(t, "POST user2/repo1/pulls/3/target_branch?target_branch="+target)
	contexttest.LoadUser(t, ctx, 2)
	ctx.IsSigned = true
	contexttest.LoadRepo(t, ctx, 1)
	ctx.SetParams(":index", "3")

	UpdatePullRequestTarget(ctx)

	return resp.Code, resp.Body.String()
}

// A change of the target branch posted to a node of the cluster behind on
// the pull request's repositories is refused with 503 and the reason: the
// patch tested on the node's git would be that of the commit before a push
// another node acknowledged, and the merge checks trust what it writes
// (#300). Nothing is written, the user retries.
func TestUpdatePullRequestTargetRefusedOnANodeBehind(t *testing.T) {
	unittest.PrepareTestEnv(t)
	clusterhook.SetFreshWaiter(func(gocontext.Context, string) bool { return false })
	t.Cleanup(func() { clusterhook.SetFreshWaiter(nil) })

	code, body := updateTarget(t, "develop")

	assert.Equal(t, http.StatusServiceUnavailable, code, body)
	assert.Contains(t, body, "may be behind")
	assert.Equal(t, "master", unittest.AssertExistsAndLoadBean(t, &issues_model.PullRequest{ID: 2}).BaseBranch, "the target is unchanged")
}

// On a node of the cluster that serves the repositories, the target
// changes as before.
func TestUpdatePullRequestTargetOnAFreshNode(t *testing.T) {
	unittest.PrepareTestEnv(t)
	clusterhook.SetFreshWaiter(func(gocontext.Context, string) bool { return true })
	t.Cleanup(func() { clusterhook.SetFreshWaiter(nil) })

	code, body := updateTarget(t, "develop")

	assert.Equal(t, http.StatusOK, code, body)
	assert.Equal(t, "develop", unittest.AssertExistsAndLoadBean(t, &issues_model.PullRequest{ID: 2}).BaseBranch)
}

// The "Delete branch" button of a merged pull request retargets the pull
// requests based on its head branch before it deletes the branch. Since
// #300, a node of the cluster behind on a child's repositories refuses
// the child's retarget: the branch is kept, the child keeps its target,
// and the page says that the node lags, for the user to retry, where it
// said that the deletion failed.
func TestCleanUpPullRequestKeepsTheBranchWhileAChildRetargetLags(t *testing.T) {
	unittest.PrepareTestEnv(t)
	defer test.MockVariableValue(&setting.Repository.PullRequest.RetargetChildrenOnMerge, true)()
	dir := unittest.AssertExistsAndLoadBean(t, &repo_model.Repository{ID: 1}).RepoPath()
	head, err := git.GetFullCommitID(t.Context(), dir, "branch2")
	require.NoError(t, err)
	// Pull request 2, index 3 of repo1, merged branch2 at its head; pull
	// request 5 (pr-to-update into branch2) is its child.
	_, _, err = git.NewCommand(t.Context(), "update-ref", "refs/pull/3/head").AddDynamicArguments(head).RunStdString(&git.RunOpts{Dir: dir})
	require.NoError(t, err)
	_, err = db.GetEngine(db.DefaultContext).ID(2).Cols("has_merged").Update(&issues_model.PullRequest{HasMerged: true})
	require.NoError(t, err)
	var asked atomic.Int32
	clusterhook.SetFreshWaiter(func(gocontext.Context, string) bool { asked.Add(1); return false })
	t.Cleanup(func() { clusterhook.SetFreshWaiter(nil) })

	ctx, _ := contexttest.MockContext(t, "user2/repo1/pulls/3/cleanup")
	contexttest.LoadUser(t, ctx, 2)
	contexttest.LoadRepo(t, ctx, 1)
	contexttest.LoadGitRepo(t, ctx)
	defer ctx.Repo.GitRepo.Close()
	ctx.SetParams(":index", "3")

	func() {
		// A deletion hands its notifications to the push queue, which
		// this test binary does not start: it panics after the deletion.
		defer func() { _ = recover() }()
		CleanUpPullRequest(ctx)
	}()

	assert.Positive(t, asked.Load(), "the child's retarget waited for the node")
	assert.EqualValues(t, ctx.Tr("repo.branch.deletion_behind", "user2/repo1:branch2"), ctx.Flash.ErrorMsg)
	assert.Empty(t, ctx.Flash.SuccessMsg)
	got, err := git.GetFullCommitID(t.Context(), dir, "refs/heads/branch2")
	require.NoError(t, err, "branch2 is kept")
	assert.Equal(t, head, got)
	assert.False(t, unittest.AssertExistsAndLoadBean(t, &git_model.Branch{RepoID: 1, Name: "branch2"}).IsDeleted)
	assert.Equal(t, "branch2", unittest.AssertExistsAndLoadBean(t, &issues_model.PullRequest{ID: 5}).BaseBranch, "the child keeps its target")
}
