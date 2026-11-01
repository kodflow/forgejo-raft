// Copyright 2026 The Forgejo Authors. All rights reserved.
// SPDX-License-Identifier: GPL-3.0-or-later

package cluster

import (
	"context"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"forgejo.org/models/db"
	issues_model "forgejo.org/models/issues"
	repo_model "forgejo.org/models/repo"
	"forgejo.org/models/unittest"
	user_model "forgejo.org/models/user"
	"forgejo.org/modules/clusterhook"
	"forgejo.org/services/cluster/reftable"
	pull_service "forgejo.org/services/pull"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// inFlightCommit commits file=msg in work and returns the commit's id.
func inFlightCommit(t *testing.T, work, file, msg string) string {
	t.Helper()
	require.NoError(t, os.WriteFile(filepath.Join(work, file), []byte(msg), 0o644))
	run(t, work, "add", ".")
	run(t, work, "commit", "-q", "-m", msg)
	return run(t, work, "rev-parse", "HEAD")
}

// inFlightEvent finds the event of ref at sha in any node's table.
func inFlightEvent(tc *testCluster, ref, sha string) (reftable.Event, bool) {
	for _, n := range tc.nodes {
		for _, ev := range n.r.table.Events() {
			if ev.Repo == laggingRepo && ev.Ref == ref && ev.New == sha {
				return ev, true
			}
		}
	}
	return reftable.Event{}, false
}

// waitsToRegister reports whether a goroutine of this binary waits in
// registry.register: a "prepared" phase that built its transaction, and
// set the event's Time, then waits for the certification to end.
func waitsToRegister() bool {
	buf := make([]byte, 16<<20)
	buf = buf[:runtime.Stack(buf, true)]
	return strings.Contains(string(buf), "cluster.(*registry).register(")
}

// testPullRequestOf runs pull_service.TestPullRequest as the consumer's
// derivation of a push defers it (pushUpdates: AddTestPullRequestTask with
// TimeNano = the event's Time). The patch checker queue does not run in
// this binary: AddToTaskQueue, called once a pull request taken was moved
// to the push, panics, and the panic tells that a pull request was taken.
func testPullRequestOf(ctx context.Context, doer *user_model.User, branch string, olderThan int64, old, sha string) (taken bool) {
	defer func() { taken = recover() != nil }()
	pull_service.TestPullRequest(ctx, doer, 1, olderThan, branch, true, old, sha)
	return false
}

// A push to a pull request's head branch is in flight (its "prepared"
// phase built the transaction, so the event's Time is set, and it has not
// committed yet: shipping objects, waiting for a certification, an
// election) when the pull request is created. The creation waits for
// freshness at the call (#191), which does not include the push, so
// refs/pull/N/head is written at the head before it. The push then
// commits, and the consumer derives it with TimeNano = ev.Time:
// TestPullRequest took only the pull requests created at or before that
// time, so the pull request was passed over and never synchronised: it
// stayed broken, without a synchronize event, its approvals of the
// commit before the push still counted (#366). Now a pull request created
// after the event's time whose head is the very commit the push replaced
// is taken too; one created on the pushed head is not, and a second run
// of the push's work takes none again.
func TestAPullRequestCreatedDuringAnInFlightPushFollowsIt(t *testing.T) {
	tc, a, b, work, _, c2, hold := laggingCluster(t)
	ctx := db.DefaultContext
	zero := strings.Repeat("0", 40)
	const head = "refs/heads/lag-feat"
	a.r.registry.release(hold)

	// A feature branch off main (c2), at f1 on every node.
	run(t, work, "checkout", "-q", "-b", "feat", c2)
	f1 := inFlightCommit(t, work, "g.txt", "feature one")
	require.True(t, tc.pushVia(a, work, laggingRepo, head, zero, f1).OK)
	for _, m := range tc.nodes {
		require.Eventually(t, func() bool { return gitRef(t, m, laggingRepo, head) == f1 }, 15*time.Second, 20*time.Millisecond, "%s", m.slot)
	}
	f2 := inFlightCommit(t, work, "g.txt", "feature two")

	withGitThroughReplicator(t, a)
	freshFrom(t, a)
	_, err := db.GetEngine(ctx).ID(1).Cols("default_branch").Update(&repo_model.Repository{DefaultBranch: "main"})
	require.NoError(t, err)
	sqlRepo := unittest.AssertExistsAndLoadBean(t, &repo_model.Repository{ID: 1})
	doer := unittest.AssertExistsAndLoadBean(t, &user_model.User{ID: 2})
	open := func(title, base string) *issues_model.PullRequest {
		issue := &issues_model.Issue{RepoID: 1, Title: title, PosterID: doer.ID, Poster: doer, IsPull: true}
		pr := &issues_model.PullRequest{
			HeadRepoID: 1, BaseRepoID: 1, HeadBranch: "lag-feat", BaseBranch: base,
			HeadRepo: sqlRepo, BaseRepo: sqlRepo, Type: issues_model.PullRequestGitea,
		}
		require.NoError(t, pull_service.NewPullRequest(ctx, sqlRepo, issue, nil, nil, pr, nil))
		return pr
	}
	pullHead := func(pr *issues_model.PullRequest) string {
		v, _ := a.r.table.Get(laggingRepo, pr.GetGitRefName())
		return v.SHA
	}

	// node-b certifies the repository (registry.certify, two git commands,
	// a slow disk): a push registering meanwhile waits, within TXN_TTL
	// (30 s by default). Any delay of "prepared" after buildTxn does the
	// same: shipToMajority, a retry while a leader is elected.
	b.r.registry.mu.Lock()
	b.r.registry.ttl = 30 * time.Second
	b.r.registry.mu.Unlock()
	certifying, release := make(chan struct{}), make(chan struct{})
	ran := make(chan bool, 1)
	go func() { ran <- b.r.registry.certify(laggingRepo, func() { close(certifying); <-release }) }()
	select {
	case <-certifying:
	case ok := <-ran:
		t.Fatalf("the certification did not run: %v", ok)
	}
	released := false
	defer func() {
		if !released {
			close(release)
		}
	}()

	type pushResult struct {
		resp *clusterhook.Response
		err  error
	}
	pushed := make(chan pushResult, 1)
	handling := make(chan struct{}, 1)
	go func() {
		// tc.tryPushVia, telling when "prepared" is handed to node-b.
		dir := filepath.Join(b.r.root, filepath.FromSlash(laggingRepo))
		if _, err := gitIn(work, "push", "-q", dir, f2+":refs/tmp/incoming"); err != nil {
			pushed <- pushResult{nil, err}
			return
		}
		if _, err := gitIn(dir, "update-ref", "-d", "refs/tmp/incoming"); err != nil {
			pushed <- pushResult{nil, err}
			return
		}
		req := &clusterhook.Request{
			Op: clusterhook.OpPrepared, Nonce: clusterhook.NewNonce(), GitDir: dir, RepoID: 1, PusherID: 2,
			Lines: []clusterhook.Line{{Old: f1, New: f2, Ref: head}},
		}
		pctx, cancel := context.WithTimeout(context.Background(), time.Minute)
		defer cancel()
		unlock, err := holdRefLockIn(dir, head, f2)
		if err != nil {
			pushed <- pushResult{nil, err}
			return
		}
		handling <- struct{}{}
		resp := b.r.Handle(pctx, req)
		unlock()
		req.Op = clusterhook.OpAborted
		if resp.OK {
			if _, err = gitIn(dir, "update-ref", head, f2); err == nil {
				req.Op = clusterhook.OpCommitted
			}
		}
		b.r.Handle(pctx, req)
		pushed <- pushResult{resp, err}
	}()
	select {
	case <-handling:
	case res := <-pushed:
		t.Fatalf("the push ended before prepared: %v", res.err)
	case <-time.After(30 * time.Second):
		t.Fatal("prepared never handed to node-b")
	}
	require.Eventually(t, waitsToRegister, 30*time.Second, 20*time.Millisecond, "prepared built its transaction and waits for the certification")
	require.Equal(t, f1, gitRef(t, a, laggingRepo, head), "the push is not committed yet")

	inFlight := open("in-flight head", "main")
	created := int64(inFlight.Issue.Created)
	require.Equal(t, f1, pullHead(inFlight), "created while the push is in flight, on the head before it")

	close(release)
	released = true
	res := <-pushed
	require.NoError(t, res.err)
	require.True(t, res.resp.OK, res.resp.Message)
	for _, m := range tc.nodes {
		require.Eventually(t, func() bool { return gitRef(t, m, laggingRepo, head) == f2 }, 15*time.Second, 20*time.Millisecond, "%s projects the push", m.slot)
	}
	ev, ok := inFlightEvent(tc, head, f2)
	require.True(t, ok, "the push's event")
	require.Less(t, ev.Time, created, "the event's time precedes the pull request, which precedes the push's commit")

	// A pull request opened once the push is visible: its head is the
	// pushed commit already.
	after := open("pushed head", "lag-from")
	require.Equal(t, f2, pullHead(after))

	// The push's work, as the consumer derives it.
	taken := testPullRequestOf(ctx, doer, "lag-feat", ev.Time, f1, f2)
	t.Logf("pull request %d created at %d, the push's event at %d: taken=%v, %s at %.10s", inFlight.Index, created, ev.Time, taken, inFlight.GetGitRefName(), pullHead(inFlight))
	assert.True(t, taken, "the push's work takes the pull request created on the head it replaced")
	assert.Equal(t, f2, pullHead(inFlight), "the pull request's head follows the acknowledged push")
	assert.Equal(t, f2, pullHead(after))

	// Run again (an after-commit effect whose outcome was unknown, a
	// second derivation): neither pull request has the head the push
	// replaced any more, and none is taken.
	assert.False(t, testPullRequestOf(ctx, doer, "lag-feat", ev.Time, f1, f2), "a second run takes no pull request again")
	assert.Equal(t, f2, pullHead(inFlight))
	assert.Equal(t, f2, pullHead(after))
}
