// Copyright 2026 The Forgejo Authors. All rights reserved.
// SPDX-License-Identifier: GPL-3.0-or-later

package repo

import (
	"context"
	"net/http"
	"testing"

	"forgejo.org/models/db"
	issues_model "forgejo.org/models/issues"
	repo_model "forgejo.org/models/repo"
	"forgejo.org/models/unit"
	"forgejo.org/models/unittest"
	user_model "forgejo.org/models/user"
	"forgejo.org/modules/clusterhook"
	"forgejo.org/modules/git"
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
