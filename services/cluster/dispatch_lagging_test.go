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
	repo_model "forgejo.org/models/repo"
	"forgejo.org/models/unittest"
	user_model "forgejo.org/models/user"
	"forgejo.org/modules/clusterhook"
	"forgejo.org/modules/git"
	actions_service "forgejo.org/services/actions"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// deployWorkflow is dispatched by hand, and cancels the deployment in
// progress of its group, as deployment workflows do.
const deployWorkflow = `on:
  workflow_dispatch:
concurrency:
  group: deploy
  cancel-in-progress: true
jobs:
  deploy:
    runs-on: docker
    steps:
      - run: echo deploy
`

// dispatchCluster builds a three-node cluster holding laggingRepo with
// main at c1, which holds the deployment workflow, on every node, then
// pushes c2 to main through node-b, acknowledged. When holdA is true,
// node-a stops projecting the repository before that push and shows main
// at c1 still; its table applied c2.
func dispatchCluster(t *testing.T, holdA bool) (a, b *testNode, c1, c2, hold string) {
	t.Helper()
	require.NoError(t, unittest.PrepareTestDatabase())
	tc := newTestCluster(t, "node-a", "node-b", "node-c")
	a, b = tc.nodes[0], tc.nodes[1]
	work := t.TempDir()
	run(t, work, "init", "-q")
	require.NoError(t, os.MkdirAll(filepath.Join(work, ".forgejo", "workflows"), 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(work, ".forgejo", "workflows", "deploy.yml"), []byte(deployWorkflow), 0o644))
	c1 = commit(t, work, "one")
	zero := strings.Repeat("0", 40)
	require.Equal(t, int64(1), tc.repoID(laggingRepo))
	require.True(t, tc.pushVia(a, work, laggingRepo, "refs/heads/main", zero, c1).OK)
	for _, m := range tc.nodes {
		require.Eventually(t, func() bool { return gitRef(t, m, laggingRepo, "refs/heads/main") == c1 },
			15*time.Second, 20*time.Millisecond, "%s: precondition", m.slot)
	}

	c2 = commit(t, work, "two")
	if holdA {
		hold = clusterhook.NewNonce()
		require.NoError(t, a.r.registry.register(hold, laggingRepo, nil, os.Getpid(), false))
		pushOnB(t, tc, a, b, work, "refs/heads/main", c1, c2)
	} else {
		resp := tc.pushVia(b, work, laggingRepo, "refs/heads/main", c1, c2)
		require.True(t, resp.OK, resp.Message)
	}
	tableAt(t, tc, "refs/heads/main", c2, "the push of c2")
	return a, b, c1, c2, hold
}

// openOn opens laggingRepo in node n's git, as the request's middleware
// does before the handler runs, and reads main there.
func openOn(t *testing.T, n *testNode) (*git.Repository, string) {
	t.Helper()
	gitRepo, err := git.OpenRepository(db.DefaultContext, filepath.Join(n.r.root, filepath.FromSlash(laggingRepo)))
	require.NoError(t, err)
	t.Cleanup(func() { gitRepo.Close() })
	head, err := gitRepo.GetCommit("refs/heads/main")
	require.NoError(t, err)
	return gitRepo, head.ID.String()
}

// dispatchVia dispatches the deployment workflow on main as POST
// /repos/{owner}/{repo}/actions/workflows/deploy.yml/dispatches and the
// web's "Run workflow" do, served by node n with gitRepo, n's git: the
// workflow is looked up there (GetWorkflowToDispatch), then dispatched.
func dispatchVia(t *testing.T, n *testNode, gitRepo *git.Repository) (*actions_model.ActionRun, error) {
	t.Helper()
	ctx := db.DefaultContext
	repo := unittest.AssertExistsAndLoadBean(t, &repo_model.Repository{ID: 1})
	require.NoError(t, repo.LoadOwner(ctx))
	doer := unittest.AssertExistsAndLoadBean(t, &user_model.User{ID: 2})

	freshFrom(t, n)
	wf, err := actions_service.GetWorkflowToDispatch(ctx, repo, gitRepo, "main", "deploy.yml")
	if err != nil {
		return nil, err
	}
	r, _, err := wf.Dispatch(ctx, func(string) string { return "" }, repo, doer)
	return r, err
}

// repoRuns is the action runs of laggingRepo.
func repoRuns(t *testing.T) []*actions_model.ActionRun {
	t.Helper()
	var rows []*actions_model.ActionRun
	require.NoError(t, db.GetEngine(db.DefaultContext).Where("repo_id = ?", 1).OrderBy("id").Find(&rows))
	return rows
}

// main holds c1, with a dispatchable deployment workflow, on every node.
// node-a stops projecting the repository. c2 is pushed to main through
// node-b, acknowledged. The deployment is dispatched through node-b,
// fresh: its run is at c2. It was then dispatched through node-a: the
// workflow was looked up in node-a's git, main at c1, and Dispatch wrote
// an action_run at c1, which cancelled the run at c2 of the same group
// (#325). Now the dispatch waits for node-a to serve the repository at the
// cluster's commit index, and is refused while it does not, nothing
// written: no run is added, the run at c2 is not cancelled. Once node-a
// caught up, the same dispatch, through the git node-a opened and read
// while behind, runs c2.
func TestADispatchOnALaggingNodeIsRefused(t *testing.T) {
	a, b, c1, c2, hold := dispatchCluster(t, true)
	held := true
	t.Cleanup(func() {
		if held {
			a.r.registry.release(hold)
		}
	})

	onB, _ := openOn(t, b)
	fresh, err := dispatchVia(t, b, onB)
	require.NoError(t, err)
	require.Equal(t, c2, fresh.CommitSHA)
	before := len(repoRuns(t))

	onA, headOnA := openOn(t, a)
	require.Equal(t, c1, headOnA, "node-a has not projected c2")
	stale, err := dispatchVia(t, a, onA)
	staleSHA := ""
	if stale != nil {
		staleSHA = stale.CommitSHA
	}
	freshNow := unittest.AssertExistsAndLoadBean(t, &actions_model.ActionRun{ID: fresh.ID})
	t.Logf("dispatch through %s, behind on main (%.10s here, %.10s in the table): run at %.10q, %v; run %d (c2): %s",
		a.slot, headOnA, c2, staleSHA, err, freshNow.ID, freshNow.Status)
	require.ErrorIs(t, err, clusterhook.ErrBehind)
	assert.Nil(t, stale)
	assert.Len(t, repoRuns(t), before, "no run is written")
	assert.Equal(t, actions_model.StatusWaiting, freshNow.Status, "the run of the acknowledged head is not cancelled")

	caughtUp(t, a, hold, "refs/heads/main", c2)
	held = false
	again, err := dispatchVia(t, a, onA)
	require.NoError(t, err)
	assert.Equal(t, c2, again.CommitSHA, "a dispatch on main made after the push of c2 was acknowledged runs c2")
}

// A node that serves the repository at the cluster's commit index
// dispatches at once, at the head of main the cluster acknowledged.
func TestADispatchOnAFreshNodeRunsTheAcknowledgedHead(t *testing.T) {
	a, _, _, c2, _ := dispatchCluster(t, false)
	require.Eventually(t, func() bool { return gitRef(t, a, laggingRepo, "refs/heads/main") == c2 },
		15*time.Second, 20*time.Millisecond, "node-a projects c2")

	onA, _ := openOn(t, a)
	r, err := dispatchVia(t, a, onA)
	require.NoError(t, err)
	assert.Equal(t, c2, r.CommitSHA)
	assert.Equal(t, "refs/heads/main", r.Ref)
	assert.Equal(t, actions_model.StatusWaiting, unittest.AssertExistsAndLoadBean(t, &actions_model.ActionRun{ID: r.ID}).Status)
}
