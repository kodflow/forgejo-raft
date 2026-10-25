// Copyright 2026 The Forgejo Authors. All rights reserved.
// SPDX-License-Identifier: GPL-3.0-or-later

package repo

import (
	"context"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"

	"forgejo.org/models/db"
	git_model "forgejo.org/models/git"
	issues_model "forgejo.org/models/issues"
	repo_model "forgejo.org/models/repo"
	"forgejo.org/models/unit"
	"forgejo.org/models/unittest"
	user_model "forgejo.org/models/user"
	"forgejo.org/modules/clusterhook"
	"forgejo.org/modules/git"
	api "forgejo.org/modules/structs"
	"forgejo.org/modules/web"
	"forgejo.org/services/contexttest"
	"forgejo.org/services/forms"
	notify_service "forgejo.org/services/notify"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// benchMerges stands for what a running Forgejo does once a merge is
// pushed, so that a merge this test does not expect fails the test, not
// the binary: its notifiers load the pull request's base repository, which
// Merge's deferred AddTestPullRequestTask reads from the reloaded pull
// request, and the post-receive hook marks the pull request merged, which
// keeps it out of the patch checker queue this binary does not start. It
// also lets the pull requests into repository 1 be merged in every style,
// the manual one included.
func benchMerges(t *testing.T) {
	t.Helper()
	n := &mergedNotifier{}
	notify_service.RegisterNotifier(n)
	t.Cleanup(func() { notify_service.UnregisterNotifier(n) })
	repo := unittest.AssertExistsAndLoadBean(t, &repo_model.Repository{ID: 1})
	u, err := repo.GetUnit(db.DefaultContext, unit.TypePullRequests)
	require.NoError(t, err)
	cfg := u.PullRequestsConfig()
	cfg.AllowMerge, cfg.AllowRebase, cfg.AllowRebaseMerge, cfg.AllowSquash = true, true, true, true
	cfg.AllowFastForwardOnly, cfg.AllowManualMerge = true, true
	require.NoError(t, repo_model.UpdateRepoUnit(db.DefaultContext, u))
}

type mergedNotifier struct{ notify_service.NullNotifier }

func (mergedNotifier) MergePullRequest(ctx context.Context, _ *user_model.User, pr *issues_model.PullRequest) {
	_ = pr.LoadBaseRepo(ctx)
	_, _ = db.GetEngine(ctx).ID(pr.ID).Cols("has_merged").Update(&issues_model.PullRequest{HasMerged: true})
}

// A pull request merged through the API on a node of the cluster behind on
// its repositories is refused with 503 and the reason, without
// head_commit_id as with it, in any style, and marked merged manually
// neither: the merge would read the head from the node's git, the commit
// before a push another node acknowledged. Nothing is merged, the client
// retries (#247).
func TestMergePullRequestRefusedOnANodeBehind(t *testing.T) {
	unittest.PrepareTestEnv(t)
	benchMerges(t)
	clusterhook.SetFreshWaiter(func(context.Context, string) bool { return false })
	t.Cleanup(func() { clusterhook.SetFreshWaiter(nil) })
	repoPath := unittest.AssertExistsAndLoadBean(t, &repo_model.Repository{ID: 1}).RepoPath()
	master, err := git.GetFullCommitID(t.Context(), repoPath, "master")
	require.NoError(t, err)
	head, err := git.GetFullCommitID(t.Context(), repoPath, "branch2")
	require.NoError(t, err)

	for _, form := range []*forms.MergePullRequestForm{
		{Do: string(repo_model.MergeStyleMerge)},
		{Do: string(repo_model.MergeStyleMerge), HeadCommitID: head},
		{Do: string(repo_model.MergeStyleSquash)},
		{Do: string(repo_model.MergeStyleManuallyMerged), MergeCommitID: master},
	} {
		// Pull request 2, branch2 into master, is index 3 of repo1.
		ctx, resp := contexttest.MockAPIContext(t, "POST /api/v1/repos/user2/repo1/pulls/3/merge")
		contexttest.LoadUser(t, ctx, 2)
		contexttest.LoadRepo(t, ctx, 1)
		contexttest.LoadGitRepo(t, ctx)
		ctx.SetParams(":index", "3")
		web.SetForm(ctx, form)

		MergePullRequest(ctx)

		ctx.Repo().GitRepo.Close()
		assert.Equal(t, http.StatusServiceUnavailable, ctx.Resp.Status(), "%s, head_commit_id %q: %s", form.Do, form.HeadCommitID, resp.Body.String())
		assert.Contains(t, resp.Body.String(), "may be behind", form.Do)
	}

	after, err := git.GetFullCommitID(t.Context(), repoPath, "master")
	require.NoError(t, err)
	assert.Equal(t, master, after, "nothing merged")
	assert.False(t, unittest.AssertExistsAndLoadBean(t, &issues_model.PullRequest{ID: 2}).HasMerged)
}

// The merge's checks judge the head as this node's git has it. A node
// behind a push another node acknowledged passed the checks on the commit
// before it, then the merge, waiting for the node to serve the push, took
// the pushed commit, which no check judged: here a required status check
// passed on branch2's commit and none ran on the push. An immediate merge
// now waits before its checks, which judge the pushed commit and refuse
// it: nothing is merged (#247).
func TestMergePullRequestChecksTheHeadItMerges(t *testing.T) {
	unittest.PrepareTestEnv(t)
	benchMerges(t)
	ctx := db.DefaultContext
	repo := unittest.AssertExistsAndLoadBean(t, &repo_model.Repository{ID: 1})
	doer := unittest.AssertExistsAndLoadBean(t, &user_model.User{ID: 2})
	dir := repo.RepoPath()
	master, err := git.GetFullCommitID(ctx, dir, "master")
	require.NoError(t, err)
	checked, err := git.GetFullCommitID(ctx, dir, "branch2")
	require.NoError(t, err)
	require.NoError(t, db.Insert(ctx, &git_model.ProtectedBranch{RepoID: 1, RuleName: "master", EnableStatusCheck: true, StatusCheckContexts: []string{"ci"}}))
	require.NoError(t, git_model.NewCommitStatus(ctx, git_model.NewCommitStatusOptions{
		Repo: repo, Creator: doer, SHA: git.MustIDFromString(checked),
		CommitStatus: &git_model.CommitStatus{State: api.CommitStatusSuccess, Context: "ci"},
	}))
	out, _, err := git.NewCommand(ctx, "commit-tree", "-m", "pushed through another node, not checked", "branch2^{tree}", "-p", "branch2").RunStdString(&git.RunOpts{Dir: dir})
	require.NoError(t, err)
	pushed := strings.TrimSpace(out)

	// The node serves the push once it waited for it, as a node behind it
	// does once it projected it.
	var waited atomic.Bool
	clusterhook.SetFreshWaiter(func(ctx context.Context, _ string) bool {
		if !waited.Swap(true) {
			_, _, err := git.NewCommand(ctx, "update-ref", "refs/heads/branch2").AddDynamicArguments(pushed).RunStdString(&git.RunOpts{Dir: dir})
			require.NoError(t, err)
		}
		return true
	})
	t.Cleanup(func() { clusterhook.SetFreshWaiter(nil) })

	// Pull request 2, branch2 into master, is index 3 of repo1.
	apiCtx, resp := contexttest.MockAPIContext(t, "POST /api/v1/repos/user2/repo1/pulls/3/merge")
	contexttest.LoadUser(t, apiCtx, 2)
	contexttest.LoadRepo(t, apiCtx, 1)
	contexttest.LoadGitRepo(t, apiCtx)
	apiCtx.SetParams(":index", "3")
	web.SetForm(apiCtx, &forms.MergePullRequestForm{Do: string(repo_model.MergeStyleMerge)})

	MergePullRequest(apiCtx)

	apiCtx.Repo().GitRepo.Close()
	require.True(t, waited.Load(), "the merge waited for this node to serve the head")
	after, err := git.GetFullCommitID(ctx, dir, "master")
	require.NoError(t, err)
	if after != master {
		merged, _, err := git.NewCommand(ctx, "rev-parse").AddDynamicArguments(after + "^2").RunStdString(&git.RunOpts{Dir: dir})
		require.NoError(t, err)
		assert.Failf(t, "the merge took a head no check judged",
			"master=%.10s merges %.10s; the required check passed on %.10s, the push brought %.10s", after, strings.TrimSpace(merged), checked, pushed)
	}
	assert.Equal(t, http.StatusMethodNotAllowed, apiCtx.Resp.Status(), "%s", resp.Body.String())
	assert.Contains(t, resp.Body.String(), "Not all required status checks successful")
	assert.False(t, unittest.AssertExistsAndLoadBean(t, &issues_model.PullRequest{ID: 2}).HasMerged)
}
