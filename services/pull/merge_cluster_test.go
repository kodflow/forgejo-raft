// Copyright 2026 The Forgejo Authors. All rights reserved.
// SPDX-License-Identifier: GPL-3.0-or-later

package pull

import (
	"context"
	"slices"
	"strings"
	"sync"
	"testing"

	"forgejo.org/models/db"
	issues_model "forgejo.org/models/issues"
	repo_model "forgejo.org/models/repo"
	"forgejo.org/models/unit"
	"forgejo.org/models/unittest"
	user_model "forgejo.org/models/user"
	"forgejo.org/modules/clusterhook"
	"forgejo.org/modules/git"
	"forgejo.org/modules/gitrepo"
	notify_service "forgejo.org/services/notify"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// freshWaiter registers, for the test, the cluster's wait for this node to
// serve a repository at the cluster's commit index, answered by fresh. It
// returns the keys asked so far, in order.
func freshWaiter(t *testing.T, fresh func(ctx context.Context, key string) bool) func() []string {
	t.Helper()
	var mu sync.Mutex
	var asked []string
	clusterhook.SetFreshWaiter(func(ctx context.Context, key string) bool {
		mu.Lock()
		asked = append(asked, key)
		mu.Unlock()
		return fresh(ctx, key)
	})
	t.Cleanup(func() { clusterhook.SetFreshWaiter(nil) })
	return func() []string {
		mu.Lock()
		defer mu.Unlock()
		return slices.Clone(asked)
	}
}

// allowEveryMergeStyle lets the pull requests into repository 1 be merged
// in every style, the manual one included.
func allowEveryMergeStyle(t *testing.T) {
	t.Helper()
	repo := unittest.AssertExistsAndLoadBean(t, &repo_model.Repository{ID: 1})
	u, err := repo.GetUnit(db.DefaultContext, unit.TypePullRequests)
	require.NoError(t, err)
	cfg := u.PullRequestsConfig()
	cfg.AllowMerge, cfg.AllowRebase, cfg.AllowRebaseMerge, cfg.AllowSquash = true, true, true, true
	cfg.AllowFastForwardOnly, cfg.AllowManualMerge = true, true
	require.NoError(t, repo_model.UpdateRepoUnit(db.DefaultContext, u))
}

// setFlow makes pull request 2 (branch2 into master of repository 1) a
// branch's pull request or an AGit one, whose head is refs/pull/3/head.
func setFlow(t *testing.T, flow issues_model.PullRequestFlow) *issues_model.PullRequest {
	t.Helper()
	_, err := db.GetEngine(db.DefaultContext).ID(2).Cols("flow").Update(&issues_model.PullRequest{Flow: flow})
	require.NoError(t, err)
	return unittest.AssertExistsAndLoadBean(t, &issues_model.PullRequest{ID: 2})
}

// headRef is the ref a merge of pull request 2 reads its head from.
func headRef(flow issues_model.PullRequestFlow) string {
	if flow == issues_model.PullRequestFlowAGit {
		return "refs/pull/3/head"
	}
	return git.BranchPrefix + "branch2"
}

var flows = []issues_model.PullRequestFlow{issues_model.PullRequestFlowGithub, issues_model.PullRequestFlowAGit}

// mergedNotifier stands for the notifiers a running Forgejo registers
// (webhook, feed, actions), which load the merged pull request's base
// repository: Merge's deferred AddTestPullRequestTask reads it from the
// reloaded pull request, nil in a test binary with no notifier.
type mergedNotifier struct{ notify_service.NullNotifier }

func (mergedNotifier) MergePullRequest(ctx context.Context, _ *user_model.User, pr *issues_model.PullRequest) {
	_ = pr.LoadBaseRepo(ctx)
}

// benchMerges lets a merge of pr run to its end in this test binary, so
// that a merge a test does not expect fails the test, not the binary: the
// notifiers are stood for, and the pull request's issue is closed, since
// the merge's deferred AddTestPullRequestTask would hand an open pull
// request on its base branch to the patch checker queue, which this binary
// does not start. Merge does not look at the issue.
func benchMerges(t *testing.T, pr *issues_model.PullRequest) {
	t.Helper()
	n := &mergedNotifier{}
	notify_service.RegisterNotifier(n)
	t.Cleanup(func() { notify_service.UnregisterNotifier(n) })
	_, err := db.GetEngine(db.DefaultContext).ID(pr.IssueID).Cols("is_closed").Update(&issues_model.Issue{IsClosed: true})
	require.NoError(t, err)
}

// On a node of a cluster behind on the repository, every merge style is
// refused with ErrBehind before the head is read from the node's git,
// whatever head the caller expects and whatever the pull request's flow,
// and so is the manual merge: nothing is merged or marked, the caller
// retries (#247).
func TestMergeRefusedOnANodeBehind(t *testing.T) {
	unittest.PrepareTestEnv(t)
	ctx := db.DefaultContext
	allowEveryMergeStyle(t)
	asked := freshWaiter(t, func(context.Context, string) bool { return false })
	doer := unittest.AssertExistsAndLoadBean(t, &user_model.User{ID: 2})
	repo := unittest.AssertExistsAndLoadBean(t, &repo_model.Repository{ID: 1})
	gitRepo, err := gitrepo.OpenRepository(ctx, repo)
	require.NoError(t, err)
	defer gitRepo.Close()
	master, err := gitRepo.GetBranchCommitID("master")
	require.NoError(t, err)

	for _, flow := range flows {
		pr := setFlow(t, flow)
		benchMerges(t, pr)
		head, err := gitRepo.GetRefCommitID(headRef(flow))
		require.NoError(t, err)
		for _, style := range []repo_model.MergeStyle{
			repo_model.MergeStyleMerge, repo_model.MergeStyleRebase, repo_model.MergeStyleRebaseMerge,
			repo_model.MergeStyleSquash, repo_model.MergeStyleFastForwardOnly,
		} {
			for _, expected := range []string{"", head} {
				err := Merge(ctx, pr, doer, gitRepo, style, expected, "refused", false)
				require.ErrorIs(t, err, clusterhook.ErrBehind, "flow %d, %s, head_commit_id %q", flow, style, expected)
			}
		}
		require.ErrorIs(t, MergedManually(ctx, pr, doer, gitRepo, master), clusterhook.ErrBehind, "flow %d, manually merged", flow)
	}

	after, err := git.GetFullCommitID(ctx, repo.RepoPath(), "master")
	require.NoError(t, err)
	assert.Equal(t, master, after, "nothing merged")
	pr := unittest.AssertExistsAndLoadBean(t, &issues_model.PullRequest{ID: 2})
	assert.False(t, pr.HasMerged, "nothing marked merged")
	require.NotEmpty(t, asked())
	for _, key := range asked() {
		assert.Equal(t, "user2/repo1.git", key)
	}
}

// A merge waits for the head repository and for the base repository: a
// fork's pull request is refused while this node is behind on either.
func TestMergeWaitsForTheHeadAndTheBaseRepositories(t *testing.T) {
	unittest.PrepareTestEnv(t)
	ctx := db.DefaultContext
	doer := unittest.AssertExistsAndLoadBean(t, &user_model.User{ID: 2})
	pr := unittest.AssertExistsAndLoadBean(t, &issues_model.PullRequest{ID: 3})
	require.NoError(t, pr.LoadBaseRepo(ctx))
	require.NoError(t, pr.LoadHeadRepo(ctx))
	baseKey := clusterhook.RepoKey(pr.BaseRepo.OwnerName, pr.BaseRepo.Name, false)
	headKey := clusterhook.RepoKey(pr.HeadRepo.OwnerName, pr.HeadRepo.Name, false)
	require.NotEqual(t, baseKey, headKey)
	benchMerges(t, pr)
	gitRepo, err := gitrepo.OpenRepository(ctx, pr.BaseRepo)
	require.NoError(t, err)
	defer gitRepo.Close()

	for _, behind := range []string{headKey, baseKey} {
		asked := freshWaiter(t, func(_ context.Context, key string) bool { return key != behind })
		err := Merge(ctx, pr, doer, gitRepo, repo_model.MergeStyleMerge, "", "refused", false)
		if assert.ErrorIs(t, err, clusterhook.ErrBehind, "behind on %s", behind) {
			assert.Contains(t, err.Error(), behind)
		}
		assert.Contains(t, asked(), behind)
	}
}

// mergeAndRead merges pull request 2 in flow, its head moved by fresh
// while the merge waits for this node to serve the repository (no cluster
// when fresh is nil), and returns the head the merge commit merged.
func mergeAndRead(t *testing.T, flow issues_model.PullRequestFlow, fresh func(ctx context.Context, key string) bool) string {
	t.Helper()
	unittest.PrepareTestEnv(t)
	ctx := db.DefaultContext
	if fresh != nil {
		freshWaiter(t, fresh)
	}
	pr := setFlow(t, flow)
	benchMerges(t, pr)
	doer := unittest.AssertExistsAndLoadBean(t, &user_model.User{ID: 2})
	repo := unittest.AssertExistsAndLoadBean(t, &repo_model.Repository{ID: 1})
	gitRepo, err := gitrepo.OpenRepository(ctx, repo)
	require.NoError(t, err)
	defer gitRepo.Close()
	master, err := gitRepo.GetBranchCommitID("master")
	require.NoError(t, err)

	require.NoError(t, Merge(ctx, pr, doer, gitRepo, repo_model.MergeStyleMerge, "", "merged", false))

	after, err := git.GetFullCommitID(ctx, repo.RepoPath(), "master")
	require.NoError(t, err)
	require.NotEqual(t, master, after, "master moved")
	merged, _, err := git.NewCommand(ctx, "rev-parse").AddDynamicArguments(after + "^2").RunStdString(&git.RunOpts{Dir: repo.RepoPath()})
	require.NoError(t, err)
	return strings.TrimSpace(merged)
}

// A push to the head that another node of the cluster acknowledged reaches
// this node's git while the merge waits for it to serve the repository:
// the merge holds the pushed commit, not the one before it, for a branch's
// pull request and for an AGit one (#247).
func TestMergeReadsTheHeadOnceFresh(t *testing.T) {
	for _, flow := range flows {
		var pushed string
		merged := mergeAndRead(t, flow, func(ctx context.Context, _ string) bool {
			dir := unittest.AssertExistsAndLoadBean(t, &repo_model.Repository{ID: 1}).RepoPath()
			ref := headRef(flow)
			if pushed == "" {
				out, _, err := git.NewCommand(ctx, "commit-tree", "-m", "pushed through another node").
					AddDynamicArguments(ref + "^{tree}").AddArguments("-p").AddDynamicArguments(ref).
					RunStdString(&git.RunOpts{Dir: dir})
				require.NoError(t, err)
				pushed = strings.TrimSpace(out)
			}
			_, _, err := git.NewCommand(ctx, "update-ref").AddDynamicArguments(ref, pushed).RunStdString(&git.RunOpts{Dir: dir})
			require.NoError(t, err)
			return true
		})
		require.NotEmpty(t, pushed, "flow %d: the merge waited", flow)
		assert.Equal(t, pushed, merged, "flow %d: the merge holds the acknowledged push", flow)
	}
}

// Without a cluster nothing waits: the merge holds the head as this node's
// git has it, as before.
func TestMergeOutsideACluster(t *testing.T) {
	for _, flow := range flows {
		unittest.PrepareTestEnv(t)
		repo := unittest.AssertExistsAndLoadBean(t, &repo_model.Repository{ID: 1})
		head, err := git.GetFullCommitID(db.DefaultContext, repo.RepoPath(), headRef(flow))
		require.NoError(t, err)

		assert.Equal(t, head, mergeAndRead(t, flow, nil), "flow %d", flow)
	}
}
