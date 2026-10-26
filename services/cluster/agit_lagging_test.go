// Copyright 2026 The Forgejo Authors. All rights reserved.
// SPDX-License-Identifier: GPL-3.0-or-later

package cluster

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"forgejo.org/models/db"
	issues_model "forgejo.org/models/issues"
	repo_model "forgejo.org/models/repo"
	"forgejo.org/models/unittest"
	"forgejo.org/modules/clusterhook"
	"forgejo.org/modules/git"
	"forgejo.org/modules/gitrepo"
	"forgejo.org/modules/private"
	"forgejo.org/services/agit"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// agitPush is one AGit push the proc-receive hook hands to Forgejo.
type agitPush struct {
	ref     git.RefName // refs/for/main, or refs/pull/N/head
	old     string      // the client's old value: zero for refs/for/main
	sha     string
	branch  string // the work branch holding sha
	options map[string]string
}

// agitResult is what agit.ProcReceive answered, and whether it panicked.
type agitResult struct {
	res      []private.HookProcReceiveRefResult
	err      error
	panicked any
}

// refused is the push's refusal, "" when it was taken.
func (r agitResult) refused() string {
	if len(r.res) != 1 {
		return ""
	}
	return r.res[0].Err
}

func (r agitResult) String() string {
	var s []string
	for _, x := range r.res {
		s = append(s, fmt.Sprintf("%s->%s err=%q", x.OriginalRef, x.Ref, x.Err))
	}
	return fmt.Sprintf("results [%s], err=%v, panicked=%v", strings.Join(s, "; "), r.err, r.panicked != nil)
}

// procReceiveOn runs, served by node n, the proc-receive work of an AGit
// push by user2, as routers/private/hook_proc_receive.go does once
// receive-pack migrated the pushed objects. The patch checker queue does
// not run in this binary: AddToTaskQueue, called once the pull request's
// ref was written, panics, and the panic is returned: it tells that the
// push was taken.
func procReceiveOn(t *testing.T, n *testNode, work string, p agitPush) (out agitResult) {
	t.Helper()
	ctx := db.DefaultContext
	dir := filepath.Join(n.r.root, filepath.FromSlash(laggingRepo))
	// The pushed objects, without any ref: FETCH_HEAD only.
	run(t, dir, "fetch", "-q", work, "refs/heads/"+p.branch)
	sqlRepo := unittest.AssertExistsAndLoadBean(t, &repo_model.Repository{ID: 1})
	gitRepo, err := gitrepo.OpenRepository(ctx, sqlRepo)
	require.NoError(t, err)
	defer gitRepo.Close()
	defer func() { out.panicked = recover() }()
	old := p.old
	if old == "" {
		old = strings.Repeat("0", 40)
	}
	out.res, out.err = agit.ProcReceive(ctx, sqlRepo, gitRepo, &private.HookOptions{
		OldCommitIDs:   []string{old},
		NewCommitIDs:   []string{p.sha},
		RefFullNames:   []git.RefName{p.ref},
		UserID:         2,
		UserName:       "user2",
		GitPushOptions: p.options,
	})
	return out
}

// openAGitLag opens, through node-a, the AGit pull request of user2's
// topic lag-topic into main, whose head refs/pull/N/head is f1 on every
// node, and opens its issue again: proc-receive updates an open one only.
func openAGitLag(t *testing.T) (tc *testCluster, a, b *testNode, work string, lm *lagMerge) {
	t.Helper()
	tc, a, b, work, _, _, hold := laggingCluster(t)
	lm = openLagMerge(t, tc, a, work, hold, issues_model.PullRequestFlowAGit)
	_, err := db.GetEngine(db.DefaultContext).ID(lm.pr.IssueID).Cols("is_closed").Update(&issues_model.Issue{IsClosed: false})
	require.NoError(t, err)
	return tc, a, b, work, lm
}

// holdA stops node-a's projection of laggingRepo again, until caughtUp.
func holdA(t *testing.T, a *testNode) string {
	t.Helper()
	hold := clusterhook.NewNonce()
	require.NoError(t, a.r.registry.register(hold, laggingRepo, nil, os.Getpid(), false))
	return hold
}

var lagTopic = map[string]string{"topic": "lag-topic"}

// settledAt waits until every node's table applied what any of them
// applied at the call, and checks that each holds ref at sha: a write the
// call made, acknowledged before it returned, is then on every node.
func settledAt(t *testing.T, tc *testCluster, ref, sha, msg string) {
	t.Helper()
	var target uint64
	for _, m := range tc.nodes {
		if pos, _ := m.r.table.Applied(); pos.Index > target {
			target = pos.Index
		}
	}
	for _, m := range tc.nodes {
		require.Eventually(t, func() bool {
			pos, _ := m.r.table.Applied()
			return pos.Index >= target
		}, 10*time.Second, 20*time.Millisecond, "%s applies index %d", m.slot, target)
	}
	tableAt(t, tc, ref, sha, msg)
}

// An AGit update of a pull request, a push to refs/for/main with its
// topic and without force-push, is decided against the head proc-receive
// reads from the git of the node that serves it (procReceivePullRequest):
// a node behind an AGit update another node acknowledged took a push
// that drops the acknowledged commits for a fast-forward, wrote it with
// update-ref without an old value, and the cluster accepted it whatever
// the head it holds: the acknowledged update was gone from the pull
// request on every node (#263). proc-receive now waits for the node to
// serve the repository at the cluster's commit index before it reads the
// head, and refuses the push, writing nothing, while it does not; once
// the node serves the acknowledged head, the push is refused as the
// force push it is, as without a cluster.
func TestProcReceiveOnALaggingNodeKeepsAnAcknowledgedAGitUpdate(t *testing.T) {
	tc, a, b, work, lm := openAGitLag(t)
	ref := lm.pr.GetGitRefName()

	// Control, node-a fresh: f2 (child of f1) acknowledged through node-b
	// and projected on node-a; a push of f3, from f1, is refused there.
	f2 := commit(t, work, "feat two")
	require.True(t, tc.pushVia(b, work, laggingRepo, ref, lm.f1, f2).OK)
	caughtUp(t, a, clusterhook.NewNonce(), ref, f2)
	run(t, work, "checkout", "-q", "-b", "agit-from-f1", lm.f1)
	f3 := commit(t, work, "feat three, from f1")
	out := procReceiveOn(t, a, work, agitPush{ref: "refs/for/main", sha: f3, branch: "agit-from-f1", options: lagTopic})
	t.Logf("control, node-a serves f2: %s", out)
	require.Nil(t, out.panicked)
	require.NoError(t, out.err)
	require.Contains(t, out.refused(), "behind its remote counterpart")
	settledAt(t, tc, ref, f2, "a force push without force-push is refused")

	// node-a behind: f4 (child of f2) acknowledged through node-b; a push
	// of f5, from f2, without force-push, through node-a.
	hold := holdA(t, a)
	run(t, work, "checkout", "-q", "lag-feat")
	f4 := commit(t, work, "feat four")
	pushOnB(t, tc, a, b, work, ref, f2, f4)
	run(t, work, "checkout", "-q", "-b", "agit-from-f2", f2)
	f5 := commit(t, work, "feat five, from f2")
	out = procReceiveOn(t, a, work, agitPush{ref: "refs/for/main", sha: f5, branch: "agit-from-f2", options: lagTopic})
	t.Logf("node-a behind: %s", out)
	assert.Nil(t, out.panicked, "the push was taken: proc-receive wrote the pull request's head")
	require.NoError(t, out.err)
	assert.Contains(t, out.refused(), clusterhook.ErrBehind.Error())
	settledAt(t, tc, ref, f4, "the acknowledged AGit update f4 is kept")
	assert.Equal(t, f2, gitRef(t, a, laggingRepo, ref), "node-a: nothing was written")

	// node-a serves f4: the same push is the force push it is.
	caughtUp(t, a, hold, ref, f4)
	out = procReceiveOn(t, a, work, agitPush{ref: "refs/for/main", sha: f5, branch: "agit-from-f2", options: lagTopic})
	t.Logf("node-a caught up: %s", out)
	require.Nil(t, out.panicked)
	require.NoError(t, out.err)
	assert.Contains(t, out.refused(), "behind its remote counterpart")
	settledAt(t, tc, ref, f4, "a force push without force-push is refused")
}

// A node whose wait for the repository passed may still not show a write
// the cluster acknowledged after the wait. proc-receive's update-ref now
// names the head it read as its old value, which git checks against this
// node's git and the cluster's Apply against the head it holds: the
// update decided against a head the cluster replaced is refused, and the
// acknowledged head is kept (#263). Before, update-ref named no old
// value, and the cluster took the write over the head it held.
func TestProcReceiveWritesOverTheHeadItReadOnly(t *testing.T) {
	tc, a, b, work, lm := openAGitLag(t)
	ref := lm.pr.GetGitRefName()
	// node-a's wait answers fresh, as for a write acknowledged after it.
	clusterhook.SetFreshWaiter(func(context.Context, string) bool { return true })

	holdA(t, a)
	f2 := commit(t, work, "feat two")
	pushOnB(t, tc, a, b, work, ref, lm.f1, f2)
	run(t, work, "checkout", "-q", "-b", "agit-from-f1", lm.f1)
	f3 := commit(t, work, "feat three, from f1")
	out := procReceiveOn(t, a, work, agitPush{ref: "refs/for/main", sha: f3, branch: "agit-from-f1", options: lagTopic})
	t.Logf("node-a behind, its wait passed: %s", out)
	assert.Nil(t, out.panicked, "the push was taken: proc-receive wrote the pull request's head")
	if out.err == nil {
		assert.NotEmpty(t, out.refused(), "the push was taken")
	}
	settledAt(t, tc, ref, f2, "the acknowledged AGit update f2 is kept")
	assert.Equal(t, lm.f1, gitRef(t, a, laggingRepo, ref), "node-a: nothing was written")
}

// A push straight to an AGit pull request's head, refs/pull/N/head, is
// taken as forced: git's client checked it is a fast-forward, or was
// told to force it, against the head the node advertised. A node behind
// an AGit update another node acknowledged advertised the head before
// it; once it caught up, proc-receive took the push over the acknowledged
// head, which the client never saw (#263). proc-receive now refuses a
// push whose old value is not the head it serves, as git refuses the
// update of any ref that moved since the client read it; a forced push
// from the head the node serves is still taken.
func TestDirectAGitPushFromAStaleHeadIsRefused(t *testing.T) {
	tc, a, b, work, lm := openAGitLag(t)
	ref := lm.pr.GetGitRefName()

	// node-a behind advertised f1; f2 (child of f1) is acknowledged
	// through node-b; node-a caught up before proc-receive ran.
	hold := holdA(t, a)
	f2 := commit(t, work, "feat two")
	pushOnB(t, tc, a, b, work, ref, lm.f1, f2)
	caughtUp(t, a, hold, ref, f2)
	run(t, work, "checkout", "-q", "-b", "agit-from-f1", lm.f1)
	f3 := commit(t, work, "feat three, from f1")
	out := procReceiveOn(t, a, work, agitPush{ref: git.RefName(ref), old: lm.f1, sha: f3, branch: "agit-from-f1"})
	t.Logf("push from the stale head f1: %s", out)
	assert.Nil(t, out.panicked, "the push was taken: proc-receive wrote the pull request's head")
	require.NoError(t, out.err)
	assert.NotEmpty(t, out.refused())
	settledAt(t, tc, ref, f2, "the acknowledged AGit update f2 is kept")

	// Control: forced from the head node-a serves, the push is taken.
	out = procReceiveOn(t, a, work, agitPush{ref: git.RefName(ref), old: f2, sha: f3, branch: "agit-from-f1"})
	t.Logf("forced push from the served head f2: %s", out)
	if out.panicked == nil {
		require.NoError(t, out.err)
		require.Empty(t, out.refused())
	}
	settledAt(t, tc, ref, f3, "a forced push from the head the node serves is taken")
}
