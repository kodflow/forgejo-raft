// Copyright 2026 The Forgejo Authors. All rights reserved.
// SPDX-License-Identifier: GPL-3.0-or-later

package cluster

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	actions_model "forgejo.org/models/actions"
	"forgejo.org/models/db"
	issues_model "forgejo.org/models/issues"
	repo_model "forgejo.org/models/repo"
	"forgejo.org/models/unittest"
	user_model "forgejo.org/models/user"
	"forgejo.org/modules/cache"
	"forgejo.org/modules/clusterhook"
	"forgejo.org/modules/git"
	"forgejo.org/modules/log"
	"forgejo.org/modules/setting"
	"forgejo.org/modules/test"
	webhook_module "forgejo.org/modules/webhook"
	actions_service "forgejo.org/services/actions"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// chatOpsWorkflow deploys on a "/deploy" comment, or by hand, and cancels
// the deployment in progress of its group, as ChatOps deployment
// workflows do.
const chatOpsWorkflow = `on:
  workflow_dispatch:
  issue_comment:
    types: [created]
concurrency:
  group: deploy
  cancel-in-progress: true
jobs:
  deploy:
    runs-on: docker
    steps:
      - run: echo deploy
`

// commentCluster builds a three-node cluster holding laggingRepo with
// master, the fixtures' default branch of repository 1, at c1, which
// holds the ChatOps workflow, on every node. node-a then stops projecting
// the repository, and c2 is pushed to master through node-b, acknowledged:
// node-a's table applied c2, its git still shows c1 until release(hold).
func commentCluster(t *testing.T) (tc *testCluster, a, b *testNode, c1, c2, hold string) {
	t.Helper()
	require.NoError(t, unittest.PrepareTestDatabase())
	tc = newTestCluster(t, "node-a", "node-b", "node-c")
	a, b = tc.nodes[0], tc.nodes[1]
	work := t.TempDir()
	run(t, work, "init", "-q")
	require.NoError(t, os.MkdirAll(filepath.Join(work, ".forgejo", "workflows"), 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(work, ".forgejo", "workflows", "deploy.yml"), []byte(chatOpsWorkflow), 0o644))
	c1 = commit(t, work, "one")
	zero := strings.Repeat("0", 40)
	require.Equal(t, int64(1), tc.repoID(laggingRepo))
	require.True(t, tc.pushVia(a, work, laggingRepo, "refs/heads/master", zero, c1).OK)
	for _, m := range tc.nodes {
		require.Eventually(t, func() bool { return gitRef(t, m, laggingRepo, "refs/heads/master") == c1 },
			15*time.Second, 20*time.Millisecond, "%s: precondition", m.slot)
	}

	c2 = commit(t, work, "two")
	hold = clusterhook.NewNonce()
	require.NoError(t, a.r.registry.register(hold, laggingRepo, nil, os.Getpid(), false))
	t.Cleanup(func() { a.r.registry.release(hold) })
	pushOnB(t, tc, a, b, work, "refs/heads/master", c1, c2)
	tableAt(t, tc, "refs/heads/master", c2, "the push of c2")
	return tc, a, b, c1, c2, hold
}

// deployVia dispatches the ChatOps workflow on master through node n, as
// the API and the web's "Run workflow" do.
func deployVia(t *testing.T, n *testNode) *actions_model.ActionRun {
	t.Helper()
	ctx := db.DefaultContext
	repo := unittest.AssertExistsAndLoadBean(t, &repo_model.Repository{ID: 1})
	require.NoError(t, repo.LoadOwner(ctx))
	doer := unittest.AssertExistsAndLoadBean(t, &user_model.User{ID: 2})
	gitRepo, err := git.OpenRepository(ctx, filepath.Join(n.r.root, filepath.FromSlash(laggingRepo)))
	require.NoError(t, err)
	defer gitRepo.Close()
	freshFrom(t, n)
	wf, err := actions_service.GetWorkflowToDispatch(ctx, repo, gitRepo, "master", "deploy.yml")
	require.NoError(t, err)
	r, _, err := wf.Dispatch(ctx, func(string) string { return "" }, repo, doer)
	require.NoError(t, err)
	return r
}

// commentVia posts "/deploy" on issue 1 in a request served by node n:
// Forgejo's git reads n's directories and n answers its freshness
// checks. The actions notifier runs as issue_service.CreateIssueComment
// runs it, in the request's context, and the runs it wrote are returned
// with how long it took.
func commentVia(t *testing.T, n *testNode) ([]*actions_model.ActionRun, time.Duration) {
	t.Helper()
	withGitThroughReplicator(t, n)
	freshFrom(t, n)
	ctx := cache.WithCacheContext(db.DefaultContext)
	repo := unittest.AssertExistsAndLoadBean(t, &repo_model.Repository{ID: 1})
	require.NoError(t, repo.LoadOwner(ctx))
	require.Equal(t, "master", repo.DefaultBranch)
	doer := unittest.AssertExistsAndLoadBean(t, &user_model.User{ID: 2})
	issue := unittest.AssertExistsAndLoadBean(t, &issues_model.Issue{ID: 1})
	require.NoError(t, issue.LoadRepo(ctx))
	comment, err := issues_model.CreateComment(ctx, &issues_model.CreateCommentOptions{
		Type: issues_model.CommentTypeComment, Doer: doer, Repo: repo, Issue: issue, Content: "/deploy",
	})
	require.NoError(t, err)

	start := time.Now()
	actions_service.NewNotifier().CreateIssueComment(ctx, doer, repo, issue, comment, nil)
	took := time.Since(start)

	var runs []*actions_model.ActionRun
	require.NoError(t, db.GetEngine(ctx).Where("repo_id = ? AND event = ?", 1, webhook_module.HookEventIssueComment).Find(&runs))
	return runs, took
}

// pendingDeploys is the commits of the runs of the deploy group still
// waiting to run.
func pendingDeploys(t *testing.T) []string {
	t.Helper()
	var rows []*actions_model.ActionRun
	require.NoError(t, db.GetEngine(db.DefaultContext).Where("repo_id = ? AND concurrency_group = ? AND status = ?", 1, "deploy", actions_model.StatusWaiting).Find(&rows))
	commits := make([]string, 0, len(rows))
	for _, r := range rows {
		commits = append(commits, r.CommitSHA)
	}
	return commits
}

// master holds c1, with a ChatOps workflow, on every node. node-a stops
// projecting the repository. c2 is pushed to master through node-b,
// acknowledged, and its deployment dispatched through node-b, fresh, runs
// c2. A "/deploy" comment on issue 1 was then served by node-a: the
// actions notifier resolved master, issue_comment being a default
// branch's event, in node-a's git, at c1, and wrote a run at c1 whose
// concurrency group cancelled the run of c2 (#335). Now the notifier first
// waits, within READ_AFTER_WRITE_TIMEOUT, for node-a to serve the
// repository at the cluster's commit index: node-a catches up while it
// waits, and the comment runs c2, as it would on a single forge. Its run
// replaces the deployment of c2, by the same concurrency group, with
// another one of c2.
func TestACommentOnALaggingNodeRunsTheAcknowledgedHeadOnceCaughtUp(t *testing.T) {
	defer test.MockVariableValue(&setting.Cluster.ReadAfterWriteTimeout, 10*time.Second)()
	_, a, b, c1, c2, hold := commentCluster(t)
	deployed := deployVia(t, b)
	require.Equal(t, c2, deployed.CommitSHA)
	require.Equal(t, c1, gitRef(t, a, laggingRepo, "refs/heads/master"), "node-a has not projected c2")

	// node-a catches up a second after the comment, well within the bound.
	released := make(chan struct{})
	go func() {
		defer close(released)
		time.Sleep(time.Second)
		a.r.registry.release(hold)
		a.r.projector.signal()
	}()
	runs, took := commentVia(t, a)
	<-released

	for _, r := range runs {
		t.Logf("run %d (issue_comment through node-a, after %v): commit %.10s, status %s", r.ID, took, r.CommitSHA, r.Status)
	}
	t.Logf("run %d (dispatched through node-b at c2 = %.10s): %s; pending in the deploy group: %.10q",
		deployed.ID, c2, unittest.AssertExistsAndLoadBean(t, &actions_model.ActionRun{ID: deployed.ID}).Status, pendingDeploys(t))
	require.Len(t, runs, 1, "the comment triggers the workflow")
	assert.Equal(t, c2, runs[0].CommitSHA, "a comment made after the push of c2 was acknowledged runs c2")
	assert.Equal(t, []string{c2}, pendingDeploys(t), "the deployment left to run is the acknowledged head's")
	assert.Less(t, took, setting.Cluster.ReadAfterWriteTimeout, "node-a caught up within the bound")
}

// The same comment, served by node-a while it stays behind past
// READ_AFTER_WRITE_TIMEOUT: the notifier gives up waiting at the bound and
// keeps the behaviour it had, the workflow run at the head node-a holds,
// c1, with a warning that names the repository, the event and the wait.
func TestACommentOnANodeBehindPastTheBoundRunsItsHeadAndWarns(t *testing.T) {
	const bound = 500 * time.Millisecond
	defer test.MockVariableValue(&setting.Cluster.ReadAfterWriteTimeout, bound)()
	_, a, _, c1, c2, _ := commentCluster(t)

	lc, cleanup := test.NewLogChecker(log.DEFAULT, log.WARN)
	defer cleanup()
	want := []string{"issue_comment", "user2/repo1", "refs/heads/master", c1, "after waiting"}
	lc.Filter(want...).StopMark("may still be behind the cluster's last writes")

	runs, took := commentVia(t, a)
	for _, r := range runs {
		t.Logf("run %d (issue_comment through node-a, after %v): commit %.10s; the table: %.10s", r.ID, took, r.CommitSHA, c2)
	}
	filtered, stopped := lc.Check(5 * time.Second)
	require.True(t, stopped, "the warning of a workflow resolved on a node that may be behind")
	for i, part := range want {
		assert.True(t, filtered[i], "the warning names %q", part)
	}
	require.Len(t, runs, 1, "the comment triggers the workflow")
	assert.Equal(t, c1, runs[0].CommitSHA, "past the bound, the workflow runs the head of the node, as before")
	assert.GreaterOrEqual(t, took, bound, "the notifier waited for node-a up to the bound")
}
