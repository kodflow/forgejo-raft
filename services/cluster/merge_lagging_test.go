// Copyright 2026 The Forgejo Authors. All rights reserved.
// SPDX-License-Identifier: GPL-3.0-or-later

package cluster

import (
	gocontext "context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"forgejo.org/models/db"
	issues_model "forgejo.org/models/issues"
	repo_model "forgejo.org/models/repo"
	"forgejo.org/models/unittest"
	user_model "forgejo.org/models/user"
	"forgejo.org/modules/clusterhook"
	"forgejo.org/modules/gitrepo"
	notify_service "forgejo.org/services/notify"
	pull_service "forgejo.org/services/pull"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// baseRepoNotifier stands for the notifiers a running Forgejo registers
// (webhook, feed, actions), which load the merged pull request's base
// repository: Merge's deferred AddTestPullRequestTask reads it from the
// reloaded pull request, nil in a test binary with no notifier.
type baseRepoNotifier struct{ notify_service.NullNotifier }

func (baseRepoNotifier) MergePullRequest(ctx gocontext.Context, _ *user_model.User, pr *issues_model.PullRequest) {
	_ = pr.LoadBaseRepo(ctx)
}

// lagMerge is a pull request of laggingRepo into main, opened while node-a
// serves the cluster's writes, and the pieces its merges need. Its head is
// head, at f1 on every node; a merge reads it from node-a's git.
type lagMerge struct {
	pr         *issues_model.PullRequest
	doer       *user_model.User
	head       string
	f1         string
	mainBefore string
}

// openLagMerge opens, through node-a, a pull request of laggingRepo into
// main whose head is f1 on every node: lag-feat for a branch's pull
// request, refs/pull/N/head of the base repository for an AGit one. The
// test bench closes its issue: the merge's deferred AddTestPullRequestTask
// would hand an open pull request on main to the patch checker queue,
// which this binary does not start. pull_service.Merge does not look at it.
func openLagMerge(t *testing.T, tc *testCluster, a *testNode, work, hold string, flow issues_model.PullRequestFlow) *lagMerge {
	t.Helper()
	ctx := db.DefaultContext
	zero := strings.Repeat("0", 40)
	a.r.registry.release(hold)
	run(t, work, "checkout", "-q", "-b", "lag-feat")
	f1 := commit(t, work, "feat one")
	branch := "refs/heads/lag-feat"
	require.True(t, tc.pushVia(a, work, laggingRepo, branch, zero, f1).OK)
	for _, m := range tc.nodes {
		require.Eventually(t, func() bool { return gitRef(t, m, laggingRepo, branch) == f1 }, 15*time.Second, 20*time.Millisecond, "%s", m.slot)
	}

	withGitThroughReplicator(t, a)
	freshFrom(t, a)
	// The cluster's repository has no master, the fixtures' default branch.
	_, err := db.GetEngine(ctx).ID(1).Cols("default_branch").Update(&repo_model.Repository{DefaultBranch: "main"})
	require.NoError(t, err)
	sqlRepo := unittest.AssertExistsAndLoadBean(t, &repo_model.Repository{ID: 1})
	doer := unittest.AssertExistsAndLoadBean(t, &user_model.User{ID: 2})

	issue := &issues_model.Issue{RepoID: 1, Title: "merge on a lagging node", PosterID: doer.ID, Poster: doer, IsPull: true}
	pr := &issues_model.PullRequest{
		HeadRepoID: 1, BaseRepoID: 1, HeadBranch: "lag-feat", BaseBranch: "main",
		HeadRepo: sqlRepo, BaseRepo: sqlRepo, Type: issues_model.PullRequestGitea, Flow: flow,
	}
	if flow == issues_model.PullRequestFlowAGit {
		// An AGit pull request's head is the commit its push carries,
		// to no branch: refs/pull/N/head of the base repository.
		pr.HeadBranch = "user2/lag-topic"
		pr.HeadCommitID = f1
	}
	require.NoError(t, pull_service.NewPullRequest(ctx, sqlRepo, issue, nil, nil, pr, nil))
	head := branch
	if flow == issues_model.PullRequestFlowAGit {
		head = pr.GetGitRefName()
	}
	for _, m := range tc.nodes {
		require.Eventually(t, func() bool {
			v, _ := m.r.table.Get(laggingRepo, pr.GetGitRefName())
			return v.SHA == f1
		}, 10*time.Second, 20*time.Millisecond, "%s: the pull request's head is f1", m.slot)
		require.Eventually(t, func() bool { return gitRef(t, m, laggingRepo, head) == f1 }, 15*time.Second, 20*time.Millisecond, "%s", m.slot)
	}
	_, err = db.GetEngine(ctx).ID(pr.IssueID).Cols("is_closed").Update(&issues_model.Issue{IsClosed: true})
	require.NoError(t, err)

	n := &baseRepoNotifier{}
	notify_service.RegisterNotifier(n)
	t.Cleanup(func() { notify_service.UnregisterNotifier(n) })

	mainBefore, _ := a.r.table.Get(laggingRepo, "refs/heads/main")
	return &lagMerge{pr: pr, doer: doer, head: head, f1: f1, mainBefore: mainBefore.SHA}
}

// mergeRefusedThenTaken moves the pull request's head from f1 to f2
// through node-b, acknowledged, while node-a does not project it. A merge
// served by node-a reads the head from node-a's git: without a wait, it
// merged f1, which the cluster accepted (the base's Old matches the
// table), and f2, acknowledged before the merge was asked, was left out
// of it; the post-receive hook of the node then marks the pull request
// merged (hook_post_receive.go handlePullRequestMerging), and the push's
// own work later passes over a merged pull request: f2 is never merged
// (#247). Every merge node-a serves while it is behind is refused with
// ErrBehind, with or without the head the client expects, and nothing is
// written; once node-a serves f2, the merge holds f2.
func mergeRefusedThenTaken(t *testing.T, tc *testCluster, a, b *testNode, work string, lm *lagMerge) {
	t.Helper()
	ctx := db.DefaultContext
	hold := clusterhook.NewNonce()
	require.NoError(t, a.r.registry.register(hold, laggingRepo, nil, os.Getpid(), false))
	f2 := commit(t, work, "feat two")
	pushOnB(t, tc, a, b, work, lm.head, lm.f1, f2)

	sqlRepo := unittest.AssertExistsAndLoadBean(t, &repo_model.Repository{ID: 1})
	gitRepo, err := gitrepo.OpenRepository(ctx, sqlRepo)
	require.NoError(t, err)
	defer gitRepo.Close()

	dirA := filepath.Join(a.r.root, filepath.FromSlash(laggingRepo))
	// mergedHead waits until every node's table moved main from before,
	// and returns main and the head its merge commit merged.
	mergedHead := func(before string) (string, string) {
		for _, m := range tc.nodes {
			require.Eventually(t, func() bool {
				v, ok := m.r.table.Get(laggingRepo, "refs/heads/main")
				return ok && v.SHA != before
			}, 10*time.Second, 20*time.Millisecond, "%s: main moved", m.slot)
		}
		v, _ := a.r.table.Get(laggingRepo, "refs/heads/main")
		return v.SHA, strings.TrimSpace(run(t, dirA, "rev-parse", v.SHA+"^2"))
	}

	// No head_commit_id (the API's default, the scheduled auto-merge), the
	// head a page rendered by node-a shows, the head a fresh page shows.
	for _, expected := range []string{"", lm.f1, f2} {
		err := pull_service.Merge(ctx, lm.pr, lm.doer, gitRepo, repo_model.MergeStyleMerge, expected, "merge on a lagging node", false)
		t.Logf("merge on node-a behind, head_commit_id %.10q: err=%v", expected, err)
		if err == nil {
			main, merged := mergedHead(lm.mainBefore)
			require.Failf(t, "node-a merged from a head it does not serve",
				"head_commit_id %.10q: main=%.10s merges %.10s; the acknowledged head is f2=%.10s, f1=%.10s", expected, main, merged, f2, lm.f1)
		}
		require.ErrorIs(t, err, clusterhook.ErrBehind, "head_commit_id %q", expected)
	}
	for _, m := range tc.nodes {
		v, _ := m.r.table.Get(laggingRepo, "refs/heads/main")
		assert.Equal(t, lm.mainBefore, v.SHA, "%s: nothing merged", m.slot)
	}
	assert.Equal(t, lm.mainBefore, gitRef(t, a, laggingRepo, "refs/heads/main"), "node-a: nothing merged")

	// Once node-a serves f2, the same merge holds it.
	caughtUp(t, a, hold, lm.head, f2)
	require.NoError(t, pull_service.Merge(ctx, lm.pr, lm.doer, gitRepo, repo_model.MergeStyleMerge, "", "merge on a fresh node", false))
	main, merged := mergedHead(lm.mainBefore)
	t.Logf("node-a fresh: main=%.10s merges %.10s; f1=%.10s f2=%.10s", main, merged, lm.f1, f2)
	assert.Equal(t, f2, merged, "the merge holds the acknowledged push")
}

// A pull request's head branch lag-feat moves from f1 to f2 through
// node-b while node-a does not project it: a merge served by node-a, as
// POST /repos/{o}/{r}/pulls/{n}/merge without head_commit_id, the
// scheduled auto-merge or the web form a page of node-a rendered ask, is
// refused, then takes f2 once node-a serves it (#247).
func TestAMergeOnALaggingNodeTakesTheAcknowledgedHead(t *testing.T) {
	tc, a, b, work, _, _, hold := laggingCluster(t)
	lm := openLagMerge(t, tc, a, work, hold, issues_model.PullRequestFlowGithub)
	mergeRefusedThenTaken(t, tc, a, b, work, lm)
}

// The head of an AGit pull request is refs/pull/N/head of the base
// repository, which an AGit push through node-b moves from f1 to f2 while
// node-a does not project it: the merge node-a serves is refused as well,
// then takes f2 once node-a serves it (#247).
func TestAnAGitMergeOnALaggingNodeTakesTheAcknowledgedHead(t *testing.T) {
	tc, a, b, work, _, _, hold := laggingCluster(t)
	lm := openLagMerge(t, tc, a, work, hold, issues_model.PullRequestFlowAGit)
	mergeRefusedThenTaken(t, tc, a, b, work, lm)
}
