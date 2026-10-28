// Copyright 2026 The Forgejo Authors. All rights reserved.
// SPDX-License-Identifier: GPL-3.0-or-later

package repo

import (
	"context"
	"net/http"
	"sync/atomic"
	"testing"

	actions_model "forgejo.org/models/actions"
	"forgejo.org/models/db"
	"forgejo.org/models/unittest"
	"forgejo.org/modules/clusterhook"
	"forgejo.org/modules/json"
	api "forgejo.org/modules/structs"
	"forgejo.org/modules/web"
	webhook_module "forgejo.org/modules/webhook"
	"forgejo.org/services/contexttest"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// runSearchMain is the head of main of user2/test_action_run_search,
// repository 63, which holds the workflow success.yaml.
const runSearchMain = "97f29ee599c373c729132a5c46a046978311e0ee"

// dispatchRunSearch is POST
// /repos/user2/test_action_run_search/actions/workflows/success.yaml/dispatches
// with ref main, the run's information asked.
func dispatchRunSearch(t *testing.T) (int, string) {
	t.Helper()
	// The fixtures hold runs of repository 63 but no run index: the next
	// run is numbered after them, as Forgejo numbers it.
	var last int64
	_, err := db.GetEngine(db.DefaultContext).Table("action_run").Where("repo_id = ?", 63).Select("MAX(`index`)").Get(&last)
	require.NoError(t, err)
	require.NoError(t, db.SyncMaxResourceIndex(db.DefaultContext, "action_run_index", 63, last))
	ctx, resp := contexttest.MockAPIContext(t, "POST /api/v1/repos/user2/test_action_run_search/actions/workflows/success.yaml/dispatches")
	contexttest.LoadRepo(t, ctx, 63)
	contexttest.LoadUser(t, ctx, 2)
	contexttest.LoadGitRepo(t, ctx)
	defer ctx.Repo().GitRepo.Close()
	ctx.SetParams(":workflowfilename", "success.yaml")
	web.SetForm(ctx, &api.DispatchWorkflowOption{Ref: "main", ReturnRunInfo: true})

	DispatchWorkflow(ctx)
	return ctx.Resp.Status(), resp.Body.String()
}

// dispatchedRuns is the number of dispatched runs of repository 63.
func dispatchedRuns(t *testing.T) int64 {
	t.Helper()
	n, err := db.GetEngine(db.DefaultContext).Where("repo_id = ? AND event = ?", 63, webhook_module.HookEventWorkflowDispatch).Count(new(actions_model.ActionRun))
	require.NoError(t, err)
	return n
}

// The ref of a workflow dispatched through the API is resolved in this
// node's git. On a node of the cluster behind on the repository, it is
// the head before the last push another node acknowledged, whose run
// would cancel the run of the acknowledged head through their concurrency
// group: the dispatch is refused with 503 and the reason, and no run is
// written (#325).
func TestDispatchWorkflowRefusedOnANodeBehind(t *testing.T) {
	unittest.PrepareTestEnv(t)
	clusterhook.SetFreshWaiter(func(context.Context, string) bool { return false })
	t.Cleanup(func() { clusterhook.SetFreshWaiter(nil) })
	before := dispatchedRuns(t)

	status, body := dispatchRunSearch(t)

	assert.Equal(t, http.StatusServiceUnavailable, status)
	assert.Contains(t, body, "may be behind")
	assert.Equal(t, before, dispatchedRuns(t), "no run is written")
}

// A node of the cluster that serves the repository at the cluster's
// commit index dispatches the workflow at the head of the ref, having
// asked once.
func TestDispatchWorkflowOnAFreshNode(t *testing.T) {
	unittest.PrepareTestEnv(t)
	var asked atomic.Int32
	clusterhook.SetFreshWaiter(func(_ context.Context, key string) bool {
		assert.Equal(t, "user2/test_action_run_search.git", key)
		asked.Add(1)
		return true
	})
	t.Cleanup(func() { clusterhook.SetFreshWaiter(nil) })
	before := dispatchedRuns(t)

	status, body := dispatchRunSearch(t)

	require.Equal(t, http.StatusCreated, status, body)
	assert.EqualValues(t, 1, asked.Load())
	var info api.DispatchWorkflowRun
	require.NoError(t, json.Unmarshal([]byte(body), &info))
	run := unittest.AssertExistsAndLoadBean(t, &actions_model.ActionRun{ID: info.ID})
	assert.Equal(t, runSearchMain, run.CommitSHA)
	assert.Equal(t, "refs/heads/main", run.Ref)
	assert.Equal(t, before+1, dispatchedRuns(t))
}
