// Copyright 2026 The Forgejo Authors. All rights reserved.
// SPDX-License-Identifier: GPL-3.0-or-later

package cluster

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"forgejo.org/models/db"
	git_model "forgejo.org/models/git"
	issues_model "forgejo.org/models/issues"
	repo_model "forgejo.org/models/repo"
	"forgejo.org/models/unittest"
	user_model "forgejo.org/models/user"
	"forgejo.org/modules/clusterhook"
	"forgejo.org/modules/gitrepo"
	"forgejo.org/modules/setting"
	"forgejo.org/modules/test"
	"forgejo.org/services/cluster/consensus"
	"forgejo.org/services/cluster/reftable"
	repo_service "forgejo.org/services/repository"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// derivation holds the leader's consumer on the push of a ref to a commit,
// then runs Forgejo's real post-receive work for it, as the consumer does
// once the leader projected the push: the branch rows, and the pull
// requests the push updates, on the test database. The test cluster's
// consumer records the other events, writing no SQL.
type derivation struct {
	open, stop, done chan struct{}
	release, end     func()
	err              error
}

func holdDerivation(t *testing.T, tc *testCluster, ref, sha string, after func(ctx context.Context, ev reftable.Event)) *derivation {
	t.Helper()
	var leader *testNode
	require.Eventually(t, func() bool {
		for _, n := range tc.nodes {
			if n.r.Engine().Status().Role == consensus.RoleLeader {
				leader = n
				return true
			}
		}
		return false
	}, 5*time.Second, 10*time.Millisecond, "a leader")
	d := &derivation{open: make(chan struct{}), stop: make(chan struct{}), done: make(chan struct{})}
	var opened, stopped, finished sync.Once
	d.release = func() { opened.Do(func() { close(d.open) }) }
	d.end = func() { stopped.Do(func() { close(d.stop) }) }
	leader.work.setThrough(func(ctx context.Context, evs []reftable.Event) (bool, error) {
		if evs[0].Ref != ref || evs[0].New != sha {
			return false, nil
		}
		select {
		case <-d.open:
		case <-d.stop:
			return true, errors.New("the test is over")
		case <-ctx.Done():
			return true, ctx.Err()
		}
		// Its side effects outside SQL (pull request tests, webhooks) are
		// collected and dropped, as a test of the consumer does.
		wctx, _ := clusterhook.WithAfterCommit(db.DefaultContext)
		err := pushWorkWith(wctx, evs, leader.r.table)
		if after != nil {
			after(wctx, evs[0])
		}
		finished.Do(func() {
			d.err = err
			close(d.done)
		})
		return true, err
	})
	t.Cleanup(func() {
		leader.work.setThrough(nil)
		d.end()
	})
	return d
}

// derived opens the derivation and waits until its work ran.
func (d *derivation) derived(t *testing.T) {
	t.Helper()
	d.release()
	select {
	case <-d.done:
	case <-time.After(15 * time.Second):
		require.FailNow(t, "the consumer did not derive the push")
	}
	require.NoError(t, d.err, "the push's post-receive work")
}

// A push to lag-from (c1 to c2) is acknowledged through node-b. Before the
// leader's consumer derives it (its next pass, up to a second later for a
// push a follower served), lag-from was renamed to lag-to through node-a,
// fresh: git_model.RenameBranch renamed the SQL row, still at c1, and the
// head of the pull requests. The consumer then derived the push under the
// event's name, lag-from: SyncBranchesToDB found no row and no branch, and
// the pull request update looked the pull requests up by head branch
// lag-from and found none. The row of lag-to kept c1 while the refs held
// c2, and its pull requests kept refs/pull/N/head at c1, until the next
// push to lag-to (#225). Now the rename waits for the consumer to derive
// every write the cluster confirmed, and is refused while it has not.
func TestABranchRenamedRightAfterAPushKeepsItsCommitInSQL(t *testing.T) {
	tc, a, b, work, c1, c2, hold := laggingCluster(t)
	a.r.registry.release(hold)
	ctx := db.DefaultContext
	require.NoError(t, db.Insert(ctx, &git_model.Branch{RepoID: 1, Name: "lag-from", CommitID: c1, CommitMessage: "one", PusherID: 2}))
	// The fixtures' open pull request 2 of repo1 has lag-from for head.
	_, err := db.GetEngine(ctx).Table("pull_request").Where("id = ?", 2).Update(map[string]any{"head_branch": "lag-from"})
	require.NoError(t, err)

	// What the push's update of the pull requests finds: TestPullRequest
	// looks them up by the pushed branch's name.
	found := -1
	push := holdDerivation(t, tc, "refs/heads/lag-from", c2, func(ctx context.Context, ev reftable.Event) {
		if prs, err := issues_model.GetUnmergedPullRequestsByHeadInfoMax(ctx, 1, ev.Time, "lag-from"); err == nil {
			found = len(prs)
		}
	})
	resp := tc.pushVia(b, work, laggingRepo, "refs/heads/lag-from", c1, c2)
	require.True(t, resp.OK, resp.Message)
	for _, m := range tc.nodes {
		require.Eventually(t, func() bool { return gitRef(t, m, laggingRepo, "refs/heads/lag-from") == c2 }, 15*time.Second, 20*time.Millisecond, "%s projects the push", m.slot)
	}

	withGitThroughReplicator(t, a)
	freshFrom(t, a)
	sqlRepo := unittest.AssertExistsAndLoadBean(t, &repo_model.Repository{ID: 1})
	doer := unittest.AssertExistsAndLoadBean(t, &user_model.User{ID: 2})
	gitRepo, err := gitrepo.OpenRepository(ctx, sqlRepo)
	require.NoError(t, err)
	defer gitRepo.Close()

	// node-a serves the push, the consumer has not derived it.
	msg, err := repo_service.RenameBranch(ctx, sqlRepo, doer, gitRepo, "lag-from", "lag-to")
	require.ErrorIs(t, err, clusterhook.ErrBehind, "the rename did not wait for the consumer to derive the push")
	assert.Empty(t, msg)
	row := unittest.AssertExistsAndLoadBean(t, &git_model.Branch{RepoID: 1, Name: "lag-from"})
	assert.Equal(t, c1, row.CommitID, "nothing was renamed")
	unittest.AssertNotExistsBean(t, &git_model.Branch{RepoID: 1, Name: "lag-to"})
	tableAt(t, tc, "refs/heads/lag-from", c2, "nothing was renamed")

	push.derived(t)
	msg, err = repo_service.RenameBranch(ctx, sqlRepo, doer, gitRepo, "lag-from", "lag-to")
	require.NoError(t, err)
	require.Empty(t, msg)
	tableAt(t, tc, "refs/heads/lag-to", c2, "the renamed branch holds the pushed commit")
	row = unittest.AssertExistsAndLoadBean(t, &git_model.Branch{RepoID: 1, Name: "lag-to"})
	t.Logf("SQL lag-to=%.8s (refs %.8s); the push's update found %d pull requests with lag-from for head", row.CommitID, c2, found)
	assert.Equal(t, c2, row.CommitID, "the SQL row of the renamed branch keeps the commit before the acknowledged push")
	assert.Positive(t, found, "the consumer's update of the pull requests of the pushed branch passes over pull request 2")
	pr := unittest.AssertExistsAndLoadBean(t, &issues_model.PullRequest{ID: 2})
	assert.Equal(t, "lag-to", pr.HeadBranch)
}

// A branch row at another commit than the table's (a push derived under a
// branch's old name, renamed meanwhile, #225) was never reported:
// reconcile compared the names of the branches only. Now it is reported
// regenerable, and rebuilt with apply.
func TestReconcileRebuildsABranchRowAtAnotherCommit(t *testing.T) {
	tc, a, b, work, c1, c2, hold := laggingCluster(t)
	a.r.registry.release(hold)
	ctx := db.DefaultContext
	// The row of lag-from from before the push; the test cluster's
	// consumer writes no SQL for the push.
	require.NoError(t, db.Insert(ctx, &git_model.Branch{RepoID: 1, Name: "lag-from", CommitID: c1, CommitMessage: "one", PusherID: 2}))
	resp := tc.pushVia(b, work, laggingRepo, "refs/heads/lag-from", c1, c2)
	require.True(t, resp.OK, resp.Message)
	require.Eventually(t, func() bool { return gitRef(t, a, laggingRepo, "refs/heads/lag-from") == c2 }, 15*time.Second, 20*time.Millisecond)
	defer test.MockVariableValue(&setting.RepoRootPath, a.r.root)()

	lagFrom := func(ms []Mismatch) (Mismatch, bool) {
		for _, m := range ms {
			if m.Repo == laggingRepo && m.Kind == "branch" && m.Ref == "lag-from" {
				return m, true
			}
		}
		return Mismatch{}, false
	}
	ms, err := a.r.Reconcile(ctx, false)
	require.NoError(t, err)
	m, ok := lagFrom(ms)
	require.True(t, ok, "the row of lag-from at c1, the table at c2, is reported: %+v", ms)
	assert.Equal(t, RefsAheadOfSQL, m.Category)
	assert.True(t, m.Regenerable)
	assert.False(t, m.Applied)

	ms, err = a.r.Reconcile(ctx, true)
	require.NoError(t, err)
	m, ok = lagFrom(ms)
	require.True(t, ok)
	assert.True(t, m.Applied)
	row := unittest.AssertExistsAndLoadBean(t, &git_model.Branch{RepoID: 1, Name: "lag-from"})
	assert.Equal(t, c2, row.CommitID, "the row is rebuilt from the table")

	ms, err = a.r.Reconcile(ctx, false)
	require.NoError(t, err)
	_, ok = lagFrom(ms)
	assert.False(t, ok, "nothing is left to rebuild")
}
