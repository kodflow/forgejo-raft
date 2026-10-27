// Copyright 2026 The Forgejo Authors. All rights reserved.
// SPDX-License-Identifier: GPL-3.0-or-later

package cluster

import (
	"os"
	"path/filepath"
	"testing"

	"forgejo.org/models"
	"forgejo.org/models/db"
	git_model "forgejo.org/models/git"
	issues_model "forgejo.org/models/issues"
	"forgejo.org/models/unittest"
	"forgejo.org/modules/clusterhook"
	pull_service "forgejo.org/services/pull"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// A pull request lag-feat into main has its head at f1 on every node. The
// branch lag-from protects secret.txt (protected file patterns). The
// pull request's author pushes f2, which changes secret.txt, through
// node-b, acknowledged, while node-a does not project it, then changes
// the pull request's target to lag-from through node-a (the web
// UpdatePullRequestTarget, which the poster may call, or PATCH
// /pulls/{index} with base). ChangeTargetBranch tested the patch on
// node-a's git, head f1, and wrote status, merge_base,
// changed_protected_files and the divergence from it: no protected file
// changed. A merge check, on any node, reads those columns:
// CheckPullBranchProtections let f2 through, which the protected file
// patterns of lag-from forbid, until the next push to lag-feat or
// lag-from (#300). The retarget now waits for the node to serve the head
// and the base repositories at the cluster's commit index, and is refused
// while it does not, nothing written; once node-a serves f2, the same
// retarget records the protected file f2 changes, and the merge check
// refuses it.
func TestRetargetOnALaggingNodeKeepsTheProtectedFileCheck(t *testing.T) {
	tc, a, b, work, _, _, hold := laggingCluster(t)
	ctx := db.DefaultContext
	lm := openLagMerge(t, tc, a, work, hold, issues_model.PullRequestFlowGithub)
	// The bench closed the issue for its merges; a retarget is of an open
	// pull request.
	_, err := db.GetEngine(ctx).ID(lm.pr.IssueID).Cols("is_closed").Update(&issues_model.Issue{IsClosed: false})
	require.NoError(t, err)
	require.NoError(t, db.Insert(ctx, &git_model.ProtectedBranch{RepoID: 1, RuleName: "lag-from", ProtectedFilePatterns: "secret.txt"}))

	// f2 changes secret.txt, pushed to lag-feat through node-b and
	// acknowledged; node-a has not projected it.
	hold2 := holdA(t, a)
	require.NoError(t, os.WriteFile(filepath.Join(work, "secret.txt"), []byte("changed"), 0o644))
	run(t, work, "add", ".")
	run(t, work, "commit", "-q", "-m", "touch secret")
	f2 := run(t, work, "rev-parse", "HEAD")
	pushOnB(t, tc, a, b, work, lm.head, lm.f1, f2)

	load := func() *issues_model.PullRequest {
		pr := unittest.AssertExistsAndLoadBean(t, &issues_model.PullRequest{ID: lm.pr.ID})
		require.NoError(t, pr.LoadIssue(ctx))
		// The route's GetActionIssue loads the issue's repository.
		require.NoError(t, pr.Issue.LoadRepo(ctx))
		return pr
	}
	retargets := func() int64 {
		n, err := db.GetEngine(ctx).Where("issue_id = ? AND type = ?", lm.pr.IssueID, issues_model.CommentTypeChangeTargetBranch).Count(new(issues_model.Comment))
		require.NoError(t, err)
		return n
	}
	before := load()

	// node-a behind: the retarget is refused, and nothing is written.
	err = pull_service.ChangeTargetBranch(ctx, load(), lm.doer, "lag-from")
	row := load()
	t.Logf("retarget to lag-from on node-a behind (lag-feat %.10s here, %.10s in the table): err=%v; SQL: base=%s status=%v merge_base=%.10s changed_protected_files=%v commits_ahead=%d",
		gitRef(t, a, laggingRepo, lm.head), f2, err, row.BaseBranch, row.Status, row.MergeBase, row.ChangedProtectedFiles, row.CommitsAhead)
	if err == nil {
		_, gate := pull_service.CheckPullBranchProtections(ctx, row, false)
		t.Logf("CheckPullBranchProtections with the columns node-a wrote: %v", gate)
	}
	require.ErrorIs(t, err, clusterhook.ErrBehind, "the retarget served by a node behind on lag-feat is refused")
	assert.Equal(t, "main", row.BaseBranch, "the target is unchanged")
	assert.Equal(t, before.Status, row.Status)
	assert.Equal(t, before.MergeBase, row.MergeBase)
	assert.Equal(t, before.ChangedProtectedFiles, row.ChangedProtectedFiles)
	assert.Equal(t, before.CommitsAhead, row.CommitsAhead)
	assert.Equal(t, before.CommitsBehind, row.CommitsBehind)
	assert.Zero(t, retargets(), "no retarget comment")
	assert.Equal(t, lm.f1, gitRef(t, a, laggingRepo, lm.head), "node-a is still behind")

	// node-a serves f2: the same retarget records the protected file f2
	// changes, and the merge check refuses it.
	caughtUp(t, a, hold2, lm.head, f2)
	require.NoError(t, pull_service.ChangeTargetBranch(ctx, load(), lm.doer, "lag-from"))
	row = load()
	_, gate := pull_service.CheckPullBranchProtections(ctx, row, false)
	t.Logf("retarget to lag-from on node-a serving f2: base=%s status=%v changed_protected_files=%v commits_ahead=%d; CheckPullBranchProtections: %v",
		row.BaseBranch, row.Status, row.ChangedProtectedFiles, row.CommitsAhead, gate)
	assert.Equal(t, "lag-from", row.BaseBranch)
	assert.Contains(t, row.ChangedProtectedFiles, "secret.txt", "f2 changes a protected file of lag-from")
	assert.True(t, models.IsErrDisallowedToMerge(gate), "the merge check refuses f2: %v", gate)
	assert.EqualValues(t, 1, retargets())
}
