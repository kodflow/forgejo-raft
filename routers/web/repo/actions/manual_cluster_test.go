// Copyright 2026 The Forgejo Authors. All rights reserved.
// SPDX-License-Identifier: GPL-3.0-or-later

package actions

import (
	"context"
	"net/http"
	"sync/atomic"
	"testing"

	actions_model "forgejo.org/models/actions"
	"forgejo.org/models/db"
	"forgejo.org/models/unittest"
	"forgejo.org/modules/clusterhook"
	webhook_module "forgejo.org/modules/webhook"
	"forgejo.org/services/contexttest"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// postManualRun is the web's "Run workflow" of success.yaml on main of
// user2/test_action_run_search, repository 63.
func postManualRun(t *testing.T) (int, string) {
	t.Helper()
	// The fixtures hold runs of repository 63 but no run index: the next
	// run is numbered after them, as Forgejo numbers it.
	var last int64
	_, err := db.GetEngine(db.DefaultContext).Table("action_run").Where("repo_id = ?", 63).Select("MAX(`index`)").Get(&last)
	require.NoError(t, err)
	require.NoError(t, db.SyncMaxResourceIndex(db.DefaultContext, "action_run_index", 63, last))
	ctx, resp := contexttest.MockContext(t, "POST /user2/test_action_run_search/actions/manual")
	contexttest.LoadUser(t, ctx, 2)
	contexttest.LoadRepo(t, ctx, 63)
	contexttest.LoadGitRepo(t, ctx)
	defer ctx.Repo.GitRepo.Close()
	ctx.SetFormString("workflow", "success.yaml")
	ctx.SetFormString("ref", "main")

	ManualRunWorkflow(ctx)
	return ctx.Resp.Status(), resp.Body.String()
}

// dispatchedRuns is the dispatched runs of repository 63.
func dispatchedRuns(t *testing.T) []*actions_model.ActionRun {
	t.Helper()
	var runs []*actions_model.ActionRun
	require.NoError(t, db.GetEngine(db.DefaultContext).Where("repo_id = ? AND event = ?", 63, webhook_module.HookEventWorkflowDispatch).OrderBy("id").Find(&runs))
	return runs
}

// The ref of a workflow run from the web is resolved in this node's git.
// On a node of the cluster behind on the repository, it is the head
// before the last push another node acknowledged, whose run would cancel
// the run of the acknowledged head through their concurrency group: it is
// refused with 503 and the reason, and no run is written (#325).
func TestManualRunWorkflowRefusedOnANodeBehind(t *testing.T) {
	unittest.PrepareTestEnv(t)
	clusterhook.SetFreshWaiter(func(context.Context, string) bool { return false })
	t.Cleanup(func() { clusterhook.SetFreshWaiter(nil) })
	before := dispatchedRuns(t)

	status, body := postManualRun(t)

	assert.Equal(t, http.StatusServiceUnavailable, status)
	assert.Contains(t, body, "may be behind")
	assert.Len(t, dispatchedRuns(t), len(before), "no run is written")
}

// A node of the cluster that serves the repository at the cluster's
// commit index runs the workflow at the head of the ref, having asked
// once.
func TestManualRunWorkflowOnAFreshNode(t *testing.T) {
	unittest.PrepareTestEnv(t)
	var asked atomic.Int32
	clusterhook.SetFreshWaiter(func(_ context.Context, key string) bool {
		assert.Equal(t, "user2/test_action_run_search.git", key)
		asked.Add(1)
		return true
	})
	t.Cleanup(func() { clusterhook.SetFreshWaiter(nil) })
	before := dispatchedRuns(t)

	status, _ := postManualRun(t)

	assert.Equal(t, http.StatusSeeOther, status)
	assert.EqualValues(t, 1, asked.Load())
	runs := dispatchedRuns(t)
	require.Len(t, runs, len(before)+1)
	run := runs[len(runs)-1]
	assert.Equal(t, "97f29ee599c373c729132a5c46a046978311e0ee", run.CommitSHA)
	assert.Equal(t, "refs/heads/main", run.Ref)
}
