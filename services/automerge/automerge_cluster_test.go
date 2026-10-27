// Copyright 2026 The Forgejo Authors. All rights reserved.
// SPDX-License-Identifier: GPL-3.0-or-later

package automerge

import (
	"context"
	"strings"
	"sync/atomic"
	"testing"

	"forgejo.org/models/db"
	issues_model "forgejo.org/models/issues"
	pull_model "forgejo.org/models/pull"
	repo_model "forgejo.org/models/repo"
	"forgejo.org/models/unittest"
	user_model "forgejo.org/models/user"
	"forgejo.org/modules/clusterhook"
	"forgejo.org/modules/git"
	"forgejo.org/modules/setting"
	"forgejo.org/modules/test"
	notify_service "forgejo.org/services/notify"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// mergedNotifier stands for what a running Forgejo does once a merge is
// pushed: its notifiers load the pull request's base repository, which
// Merge's deferred AddTestPullRequestTask reads from the reloaded pull
// request, and the post-receive hook marks the pull request merged, which
// keeps it out of the patch checker queue this binary does not start. It
// lets a merge this test does not expect fail the test, not the binary.
type mergedNotifier struct{ notify_service.NullNotifier }

func (mergedNotifier) AutoMergePullRequest(ctx context.Context, _ *user_model.User, pr *issues_model.PullRequest) {
	_ = pr.LoadBaseRepo(ctx)
	_, _ = db.GetEngine(ctx).ID(pr.ID).Cols("has_merged").Update(&issues_model.PullRequest{HasMerged: true})
}

// scheduleMerge schedules pull request 2 (branch2 into master of
// repository 1) to be merged by user2 once its checks succeed, its
// refs/pull/3/head at branch2's commit, as the pull request's work leaves
// it after a push. It returns the queue item of its head and master's
// commit.
func scheduleMerge(t *testing.T) (item, master string) {
	t.Helper()
	return scheduleMergeWith(t, &mergedNotifier{}, false)
}

// scheduleMergeWith is scheduleMerge with the notifier that stands for a
// running Forgejo, and the head branch deleted after the merge when
// deleteBranch.
func scheduleMergeWith(t *testing.T, n notify_service.Notifier, deleteBranch bool) (item, master string) {
	t.Helper()
	unittest.PrepareTestEnv(t)
	ctx := db.DefaultContext
	notify_service.RegisterNotifier(n)
	t.Cleanup(func() { notify_service.UnregisterNotifier(n) })
	doer := unittest.AssertExistsAndLoadBean(t, &user_model.User{ID: 2})
	require.NoError(t, pull_model.ScheduleAutoMerge(ctx, doer, 2, repo_model.MergeStyleMerge, "scheduled", deleteBranch))
	repo := unittest.AssertExistsAndLoadBean(t, &repo_model.Repository{ID: 1})
	sha, err := git.GetFullCommitID(ctx, repo.RepoPath(), "branch2")
	require.NoError(t, err)
	_, _, err = git.NewCommand(ctx, "update-ref", "refs/pull/3/head").AddDynamicArguments(sha).RunStdString(&git.RunOpts{Dir: repo.RepoPath()})
	require.NoError(t, err)
	master, err = git.GetFullCommitID(ctx, repo.RepoPath(), "master")
	require.NoError(t, err)
	return "2_" + sha, master
}

// nothingMerged checks that pull request 2 is still scheduled as
// scheduleMerge left it, and neither merged nor master moved.
func nothingMerged(t *testing.T, master string) {
	t.Helper()
	ctx := db.DefaultContext
	exists, scheduled, err := pull_model.GetScheduledMergeByPullID(ctx, 2)
	require.NoError(t, err)
	require.True(t, exists, "the schedule is kept")
	assert.Equal(t, int64(2), scheduled.DoerID)
	assert.Equal(t, repo_model.MergeStyleMerge, scheduled.MergeStyle)
	assert.Equal(t, "scheduled", scheduled.Message)
	assert.False(t, scheduled.DeleteBranchAfterMerge)
	repo := unittest.AssertExistsAndLoadBean(t, &repo_model.Repository{ID: 1})
	after, err := git.GetFullCommitID(ctx, repo.RepoPath(), "master")
	require.NoError(t, err)
	assert.Equal(t, master, after, "nothing merged")
	assert.False(t, unittest.AssertExistsAndLoadBean(t, &issues_model.PullRequest{ID: 2}).HasMerged)
}

// On a node of a cluster behind on the pull request's repositories, the
// scheduled merge is deferred before refs/pull/N/head is read from the
// node's git: the queue gets the item back, to retry it, the schedule is
// kept and nothing is merged (#247).
func TestAutoMergeDeferredOnANodeBehind(t *testing.T) {
	item, master := scheduleMerge(t)
	var calls atomic.Int32
	clusterhook.SetFreshWaiter(func(context.Context, string) bool { calls.Add(1); return false })
	t.Cleanup(func() { clusterhook.SetFreshWaiter(nil) })

	assert.Equal(t, []string{item}, handler(item), "the item is handed back to the queue")
	assert.Equal(t, int32(1), calls.Load())
	nothingMerged(t, master)
}

// Merge waits again, for the cluster's commit index at its own call: when
// it finds the node behind once the handler's wait passed and the handler
// deleted the schedule, the schedule is restored and the merge deferred,
// not lost (#247).
func TestAutoMergeKeepsItsScheduleWhenTheMergeFindsTheNodeBehind(t *testing.T) {
	item, master := scheduleMerge(t)
	var calls atomic.Int32
	clusterhook.SetFreshWaiter(func(context.Context, string) bool { return calls.Add(1) == 1 })
	t.Cleanup(func() { clusterhook.SetFreshWaiter(nil) })

	assert.Equal(t, []string{item}, handler(item), "the item is handed back to the queue")
	assert.Equal(t, int32(2), calls.Load(), "the handler waited, then Merge")
	nothingMerged(t, master)
}

// Without a cluster nothing is deferred: an item whose head moved is
// dropped, as before, the schedule kept for the item of the new head.
func TestAutoMergeOutsideACluster(t *testing.T) {
	_, master := scheduleMerge(t)

	assert.Empty(t, handler("2_"+strings.Repeat("0", 40)))
	nothingMerged(t, master)
}

// A push moves the head branch before the pull request's work moves
// refs/pull/N/head to it and its checks follow it. A scheduled merge whose
// checks passed on refs/pull/N/head merged the head branch, the pushed
// commit no check judged: it now merges only the head its checks passed,
// and keeps its schedule for the request the new head's checks queue
// (#247).
func TestAutoMergeTakesOnlyTheHeadItsChecksPassed(t *testing.T) {
	item, master := scheduleMerge(t)
	ctx := db.DefaultContext
	dir := unittest.AssertExistsAndLoadBean(t, &repo_model.Repository{ID: 1}).RepoPath()
	out, _, err := git.NewCommand(ctx, "commit-tree", "-m", "pushed, not checked yet", "branch2^{tree}", "-p", "branch2").RunStdString(&git.RunOpts{Dir: dir})
	require.NoError(t, err)
	pushed := strings.TrimSpace(out)
	_, _, err = git.NewCommand(ctx, "update-ref", "refs/heads/branch2").AddDynamicArguments(pushed).RunStdString(&git.RunOpts{Dir: dir})
	require.NoError(t, err)

	assert.Empty(t, handler(item), "the item is dropped, the new head's checks queue another")
	after, getErr := git.GetFullCommitID(ctx, dir, "master")
	require.NoError(t, getErr)
	if after != master {
		merged, _, err := git.NewCommand(ctx, "rev-parse").AddDynamicArguments(after + "^2").RunStdString(&git.RunOpts{Dir: dir})
		require.NoError(t, err)
		assert.Failf(t, "the scheduled merge took a head no check judged",
			"master=%.10s merges %.10s; the checks passed on %.10s, the push brought %.10s", after, strings.TrimSpace(merged), strings.TrimPrefix(item, "2_"), pushed)
	}
	nothingMerged(t, master)
}

// lagDerivation registers, for the test, the cluster's wait for the
// consumer to derive a repository's rows: refused while behind holds, as
// on a node whose consumer has not derived the cluster's last writes to
// the repository, or with one of its events parked.
func lagDerivation(t *testing.T, behind *atomic.Bool) {
	t.Helper()
	clusterhook.SetDerivedWaiter(func(context.Context, string) error {
		if behind.Load() {
			return clusterhook.ErrBehind
		}
		return nil
	})
	t.Cleanup(func() { clusterhook.SetDerivedWaiter(nil) })
}

// handled runs the queue's handler on items and returns those handed back
// to the queue. A branch deletion hands its notifications to the
// node-local push queue, which this test binary does not start: that call
// panics once the deletion is written, and the panic is recovered.
func handled(items ...string) (deferred []string) {
	defer func() { _ = recover() }()
	return handler(items...)
}

// branch2 returns the commit branch2 of repository 1 holds, "" once it is
// deleted.
func branch2(t *testing.T) string {
	t.Helper()
	dir := unittest.AssertExistsAndLoadBean(t, &repo_model.Repository{ID: 1}).RepoPath()
	out, _, err := git.NewCommand(db.DefaultContext, "for-each-ref", "--format=%(objectname)", "refs/heads/branch2").RunStdString(&git.RunOpts{Dir: dir})
	require.NoError(t, err)
	return strings.TrimSpace(out)
}

// pushToBranch2 stands for a push to branch2 acknowledged after the merge:
// it moves branch2 to a new commit on top of it, and returns it.
func pushToBranch2(t *testing.T) string {
	t.Helper()
	ctx := db.DefaultContext
	dir := unittest.AssertExistsAndLoadBean(t, &repo_model.Repository{ID: 1}).RepoPath()
	out, _, err := git.NewCommand(ctx, "commit-tree", "-m", "pushed after the merge", "branch2^{tree}", "-p", "branch2").RunStdString(&git.RunOpts{Dir: dir})
	require.NoError(t, err)
	pushed := strings.TrimSpace(out)
	_, _, err = git.NewCommand(ctx, "update-ref", "refs/heads/branch2").AddDynamicArguments(pushed).RunStdString(&git.RunOpts{Dir: dir})
	require.NoError(t, err)
	return pushed
}

// merged checks that the scheduled merge of pull request 2 merged it, its
// schedule spent.
func merged(t *testing.T, master string) {
	t.Helper()
	repo := unittest.AssertExistsAndLoadBean(t, &repo_model.Repository{ID: 1})
	after, err := git.GetFullCommitID(db.DefaultContext, repo.RepoPath(), "master")
	require.NoError(t, err)
	assert.NotEqual(t, master, after, "the scheduled merge merged")
	assert.True(t, unittest.AssertExistsAndLoadBean(t, &issues_model.PullRequest{ID: 2}).HasMerged)
	unittest.AssertNotExistsBean(t, &pull_model.AutoMerge{PullID: 2})
}

// A merge scheduled with the head branch deleted after it, on a node of a
// cluster whose consumer has not derived the head repository's last
// writes: the merge is done, and the deletion refused with ErrBehind
// (#267). The schedule, deleted before the merge, was gone and the refusal
// only logged: nothing ever deleted the branch (#279). The deletion is now
// handed back to the queue, which keeps handing it back while the node
// lags, and deletes the branch once the node derived the repository.
func TestAutoMergeDeletesItsHeadBranchOnceTheNodeDerivedIt(t *testing.T) {
	defer test.MockVariableValue(&setting.Repository.PullRequest.RetargetChildrenOnMerge, false)()
	item, master := scheduleMergeWith(t, &mergedNotifier{}, true)
	head := strings.TrimPrefix(item, "2_")
	var behind atomic.Bool
	behind.Store(true)
	lagDerivation(t, &behind)

	deferred := handled(item)
	merged(t, master)
	assert.Equal(t, head, branch2(t), "nothing is deleted while the node lags")
	require.Len(t, deferred, 1, "the deletion refused right after the merge is handed back to the queue, not dropped")
	assert.NotEqual(t, item, deferred[0], "the merge is not tried again")

	assert.Equal(t, deferred, handled(deferred...), "still behind: handed back again")
	assert.Equal(t, head, branch2(t), "nothing is deleted while the node lags")

	behind.Store(false)
	handled(deferred...)
	assert.Empty(t, branch2(t), "the head branch is deleted once the node derived the repository")
}

// A push to the head branch after the merge, while the deletion waits for
// the node: the branch then holds a commit the merge did not take, which
// its deletion would lose. The deletion is dropped, the branch kept.
func TestAutoMergeKeepsAHeadBranchPushedToAfterTheMerge(t *testing.T) {
	defer test.MockVariableValue(&setting.Repository.PullRequest.RetargetChildrenOnMerge, false)()
	item, master := scheduleMergeWith(t, &mergedNotifier{}, true)
	var behind atomic.Bool
	behind.Store(true)
	lagDerivation(t, &behind)

	deferred := handled(item)
	merged(t, master)
	require.Len(t, deferred, 1, "the deletion refused right after the merge is handed back to the queue, not dropped")

	pushed := pushToBranch2(t)
	behind.Store(false)
	assert.Empty(t, handled(deferred...), "the deletion is dropped")
	assert.Equal(t, pushed, branch2(t), "the commit pushed after the merge is kept")
}

// pushingNotifier is mergedNotifier, and a push to the head branch between
// the merge's push and the deletion of the branch.
type pushingNotifier struct {
	mergedNotifier
	t      *testing.T
	pushed string
}

func (n *pushingNotifier) AutoMergePullRequest(ctx context.Context, doer *user_model.User, pr *issues_model.PullRequest) {
	n.mergedNotifier.AutoMergePullRequest(ctx, doer, pr)
	n.pushed = pushToBranch2(n.t)
}

// The deletion right after the merge takes the branch only at the head the
// merge took: a push since, here between the merge's push and the
// deletion, is kept with its branch.
func TestAutoMergeKeepsAHeadBranchPushedToDuringTheMerge(t *testing.T) {
	defer test.MockVariableValue(&setting.Repository.PullRequest.RetargetChildrenOnMerge, false)()
	n := &pushingNotifier{t: t}
	item, master := scheduleMergeWith(t, n, true)

	assert.Empty(t, handled(item))
	merged(t, master)
	require.NotEmpty(t, n.pushed)
	assert.Equal(t, n.pushed, branch2(t), "the commit pushed after the merge is kept")
}

// Outside a cluster, or on a node that derived the head repository, the
// head branch is deleted right after the merge, as before.
func TestAutoMergeDeletesItsHeadBranchRightAfterTheMerge(t *testing.T) {
	defer test.MockVariableValue(&setting.Repository.PullRequest.RetargetChildrenOnMerge, false)()
	item, master := scheduleMergeWith(t, &mergedNotifier{}, true)

	handled(item)
	merged(t, master)
	assert.Empty(t, branch2(t), "the head branch is deleted")
}
