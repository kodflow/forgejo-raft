// Copyright 2026 The Forgejo Authors. All rights reserved.
// SPDX-License-Identifier: GPL-3.0-or-later

package cluster

import (
	gocontext "context"
	"fmt"
	"strings"
	"testing"
	"time"

	"forgejo.org/models/db"
	issues_model "forgejo.org/models/issues"
	pull_model "forgejo.org/models/pull"
	repo_model "forgejo.org/models/repo"
	"forgejo.org/models/unittest"
	user_model "forgejo.org/models/user"
	"forgejo.org/modules/setting"
	"forgejo.org/modules/test"
	"forgejo.org/services/automerge"
	notify_service "forgejo.org/services/notify"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// autoMergedNotifier stands for the post-receive hook of the merge's push,
// which this bench's git does not run: it marks the pull request merged
// (handlePullRequestMerging), which keeps it out of the patch checker
// queue this binary does not start. It also loads the base repository, as
// the notifiers of a running Forgejo do.
type autoMergedNotifier struct{ notify_service.NullNotifier }

func (autoMergedNotifier) AutoMergePullRequest(ctx gocontext.Context, _ *user_model.User, pr *issues_model.PullRequest) {
	_ = pr.LoadBaseRepo(ctx)
	_, _ = db.GetEngine(ctx).ID(pr.ID).Cols("has_merged").Update(&issues_model.PullRequest{HasMerged: true})
}

// autoMergeHandled runs the pr_auto_merge queue's handler of the node the
// test serves on items, and returns those handed back to the queue. A
// branch deletion hands its notifications to the node-local push queue,
// which this test binary does not start: that call panics once the
// deletion is written, and the panic is recovered.
func autoMergeHandled(items ...string) (deferred []string) {
	defer func() { _ = recover() }()
	return automerge.Handle(items...)
}

// untilHandled hands items to the queue's handler again, as the queue
// does with the items it hands back, until it hands back none.
func untilHandled(t *testing.T, items []string, msg string) {
	t.Helper()
	require.Eventually(t, func() bool { return len(autoMergeHandled(items...)) == 0 }, 15*time.Second, 50*time.Millisecond, msg)
}

// scheduleLagAutoMerge opens through node-a the pull request lag-feat into
// main of openLagMerge, and schedules its merge by user2 once its checks
// succeed, its head branch to be deleted after the merge. It returns the
// queue item of its head, f1.
func scheduleLagAutoMerge(t *testing.T, tc *testCluster, a *testNode, work, hold string) (*lagMerge, string) {
	t.Helper()
	lm := openLagMerge(t, tc, a, work, hold, issues_model.PullRequestFlowGithub)
	ctx := db.DefaultContext
	// openLagMerge closes the issue for pull_service.Merge, which does not
	// look at it; a scheduled merge merges only an open pull request, and
	// autoMergedNotifier keeps it out of the patch checker queue.
	_, err := db.GetEngine(ctx).ID(lm.pr.IssueID).Cols("is_closed").Update(&issues_model.Issue{IsClosed: false})
	require.NoError(t, err)
	_, err = db.GetEngine(ctx).ID(lm.pr.ID).Cols("status").Update(&issues_model.PullRequest{Status: issues_model.PullRequestStatusMergeable})
	require.NoError(t, err)
	n := &autoMergedNotifier{}
	notify_service.RegisterNotifier(n)
	t.Cleanup(func() { notify_service.UnregisterNotifier(n) })
	require.NoError(t, pull_model.ScheduleAutoMerge(ctx, lm.doer, lm.pr.ID, repo_model.MergeStyleMerge, "auto merge on a lagging node", true))
	return lm, fmt.Sprintf("%d_%s", lm.pr.ID, lm.f1)
}

// The scheduled merge of a pull request, its head branch to be deleted
// after it, is handled by node-a while the leader's consumer has not
// derived the repository's last writes (held on a push to lag-hold through
// node-b, acknowledged and projected on every node). node-a serves the
// repository, so the merge is done; the deletion is refused with
// ErrBehind, which keeps the branch row from staying live in SQL for good
// (#267). The schedule had been deleted before the merge and the refusal
// was only logged: the head branch stayed for good, on every node (#279).
// Now the deletion is handed back to the queue, handed back again while
// the consumer lags, and done once it derived the repository. The same
// deletion handled again never takes a branch that holds a commit the
// merge did not take: lag-feat, pushed again at f2 on top of f1, is kept.
func TestTheAutoMergedHeadBranchIsDeletedOnceTheLaggingNodeDerivedIt(t *testing.T) {
	tc, a, b, work, c1, _, hold := laggingCluster(t)
	defer test.MockVariableValue(&setting.Cluster.ReadAfterWriteTimeout, 500*time.Millisecond)()
	lm, item := scheduleLagAutoMerge(t, tc, a, work, hold)
	zero := strings.Repeat("0", 40)
	push := holdDerivation(t, tc, "refs/heads/lag-hold", c1, nil)
	require.True(t, tc.pushVia(b, work, laggingRepo, "refs/heads/lag-hold", zero, c1).OK)
	for _, m := range tc.nodes {
		require.Eventually(t, func() bool { return gitRef(t, m, laggingRepo, "refs/heads/lag-hold") == c1 }, 15*time.Second, 20*time.Millisecond, "%s projects the push", m.slot)
	}

	deferred := autoMergeHandled(item)
	for _, m := range tc.nodes {
		require.Eventually(t, func() bool {
			v, ok := m.r.table.Get(laggingRepo, "refs/heads/main")
			return ok && v.SHA != lm.mainBefore
		}, 10*time.Second, 20*time.Millisecond, "%s: the scheduled merge moved main", m.slot)
	}
	assert.True(t, unittest.AssertExistsAndLoadBean(t, &issues_model.PullRequest{ID: lm.pr.ID}).HasMerged)
	tableAt(t, tc, "refs/heads/lag-feat", lm.f1, "nothing is deleted while the consumer lags")
	t.Logf("merged through node-a, the consumer behind: items handed back %q", deferred)
	require.Len(t, deferred, 1, "the deletion refused right after the merge is handed back to the queue, not dropped")

	assert.Equal(t, deferred, autoMergeHandled(deferred...), "the consumer still lags: handed back again")
	tableAt(t, tc, "refs/heads/lag-feat", lm.f1, "nothing is deleted while the consumer lags")

	push.derived(t)
	untilHandled(t, deferred, "the deletion is done once the consumer derived the repository")
	for _, m := range tc.nodes {
		require.Eventually(t, func() bool {
			v, ok := m.r.table.Get(laggingRepo, "refs/heads/lag-feat")
			return !ok || !v.Live()
		}, 10*time.Second, 20*time.Millisecond, "%s: the head branch is deleted once the consumer derived the repository", m.slot)
	}

	f2 := commit(t, work, "feat two, pushed after the merge")
	require.True(t, tc.pushVia(b, work, laggingRepo, "refs/heads/lag-feat", zero, f2).OK)
	for _, m := range tc.nodes {
		require.Eventually(t, func() bool { return gitRef(t, m, laggingRepo, "refs/heads/lag-feat") == f2 }, 15*time.Second, 20*time.Millisecond, "%s projects the push", m.slot)
	}
	untilHandled(t, deferred, "the deletion is dropped")
	tableAt(t, tc, "refs/heads/lag-feat", f2, "a branch holding a commit the merge did not take is kept")
	assert.Equal(t, f2, gitRef(t, a, laggingRepo, "refs/heads/lag-feat"))
}
