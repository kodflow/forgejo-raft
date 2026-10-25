// Copyright 2026 The Forgejo Authors. All rights reserved.
// SPDX-License-Identifier: GPL-3.0-or-later

package cluster

import (
	"context"
	"errors"
	"sync/atomic"
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
	"forgejo.org/modules/test"
	"forgejo.org/services/cluster/consensus"
	"forgejo.org/services/cluster/reftable"
	repo_service "forgejo.org/services/repository"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// A push to lag-from (c1 to c2) is acknowledged, and its work fails until
// the consumer parks it (a lock timeout under a long request, say): the
// dispatch moves the cursor past a parked event in the same transaction,
// its work not run. RenameBranch waited (RequireDerived) for the cursor
// only, and passed at once: it renamed the row, still at c1, and the head
// of the pull requests, and the parked push, retried later, derived under
// the old name and found neither. lag-to kept c1 in SQL while the refs
// held c2, and the pull request kept its old head, #225 again (#239). The
// form's "create branch from branch" read the row at c1 the same way. Now
// both are refused while the repository has a parked event, saying so,
// and pass once the retry applied it.
func TestABranchRenamedWhileItsPushIsParkedKeepsItsCommitInSQL(t *testing.T) {
	// Registered first, so restored after the cluster stopped.
	t.Cleanup(test.MockVariableValue(&parkedRetryEvery, 200*time.Millisecond))
	tc, a, b, work, c1, c2, hold := laggingCluster(t)
	a.r.registry.release(hold)
	ctx := db.DefaultContext
	require.NoError(t, db.Insert(ctx, &git_model.Branch{RepoID: 1, Name: "lag-from", CommitID: c1, CommitMessage: "one", PusherID: 2}))
	// The fixtures' open pull request 2 of repo1 has lag-from for head.
	_, err := db.GetEngine(ctx).Table("pull_request").Where("id = ?", 2).Update(map[string]any{"head_branch": "lag-from"})
	require.NoError(t, err)

	var leader *testNode
	require.Eventually(t, func() bool {
		for _, n := range tc.nodes {
			if n.r.Engine().Status().Role == consensus.RoleLeader {
				leader = n
				return true
			}
		}
		return false
	}, 5*time.Second, 10*time.Millisecond)
	// The push's work fails until the test lets it through: parked after
	// maxAttempts, then retried every parkedRetryEvery. Once through, it
	// is Forgejo's real post-receive work on the test database.
	var failing, derived atomic.Bool
	failing.Store(true)
	var found atomic.Int64
	found.Store(-1)
	leader.work.setThrough(func(ctx context.Context, evs []reftable.Event) (bool, error) {
		if evs[0].Ref != "refs/heads/lag-from" || evs[0].New != c2 {
			return false, nil
		}
		if failing.Load() {
			return true, errors.New("canceling statement due to lock timeout (SQLSTATE 55P03)")
		}
		wctx, _ := clusterhook.WithAfterCommit(db.DefaultContext)
		err := pushWorkWith(wctx, evs, leader.r.table)
		// What the push's update of the pull requests finds: TestPullRequest
		// looks them up by the pushed branch's name.
		if prs, perr := issues_model.GetUnmergedPullRequestsByHeadInfoMax(wctx, 1, evs[0].Time, "lag-from"); perr == nil {
			found.Store(int64(len(prs)))
		}
		derived.Store(err == nil)
		return true, err
	})
	t.Cleanup(func() { leader.work.setThrough(nil) })

	resp := tc.pushVia(b, work, laggingRepo, "refs/heads/lag-from", c1, c2)
	require.True(t, resp.OK, resp.Message)
	for _, m := range tc.nodes {
		require.Eventually(t, func() bool { return gitRef(t, m, laggingRepo, "refs/heads/lag-from") == c2 }, 15*time.Second, 20*time.Millisecond, "%s projects the push", m.slot)
	}
	require.Eventually(t, func() bool { return len(tc.parked.indexes()) > 0 }, 30*time.Second, 50*time.Millisecond, "the push is parked")
	t.Logf("parked: %v", tc.parked.indexes())

	withGitThroughReplicator(t, a)
	freshFrom(t, a)
	sqlRepo := unittest.AssertExistsAndLoadBean(t, &repo_model.Repository{ID: 1})
	doer := unittest.AssertExistsAndLoadBean(t, &user_model.User{ID: 2})
	gitRepo, err := gitrepo.OpenRepository(ctx, sqlRepo)
	require.NoError(t, err)
	defer gitRepo.Close()

	t.Run("while the push is parked", func(t *testing.T) {
		msg, err := repo_service.RenameBranch(ctx, sqlRepo, doer, gitRepo, "lag-from", "lag-to")
		t.Logf("rename while the push is parked: msg=%q err=%v", msg, err)
		require.ErrorIs(t, err, clusterhook.ErrBehind, "the rename passed while the push of the branch is parked")
		require.ErrorIs(t, err, clusterhook.ErrParked)
		assert.Contains(t, err.Error(), laggingRepo)
		assert.Contains(t, err.Error(), "is parked")
		assert.Contains(t, err.Error(), "forgejo cluster reconcile --apply")
		assert.Empty(t, msg)
		row := unittest.AssertExistsAndLoadBean(t, &git_model.Branch{RepoID: 1, Name: "lag-from"})
		assert.Equal(t, c1, row.CommitID, "nothing was renamed")
		unittest.AssertNotExistsBean(t, &git_model.Branch{RepoID: 1, Name: "lag-to"})
		tableAt(t, tc, "refs/heads/lag-from", c2, "nothing was renamed")
		pr := unittest.AssertExistsAndLoadBean(t, &issues_model.PullRequest{ID: 2})
		assert.Equal(t, "lag-from", pr.HeadBranch, "nothing was renamed")

		// The form's "create branch from branch" reads lag-from's row, at c1.
		err = repo_service.CreateNewBranch(ctx, doer, sqlRepo, gitRepo, "lag-from", "lag-new")
		require.ErrorIs(t, err, clusterhook.ErrParked, "the branch was created from lag-from's row, at c1")
		v, ok := a.r.table.Get(laggingRepo, "refs/heads/lag-new")
		assert.False(t, ok && v.Live(), "lag-new was written")
	})

	failing.Store(false)
	require.Eventually(t, derived.Load, 30*time.Second, 50*time.Millisecond, "the parked push is retried")
	require.Eventually(t, func() bool { return len(tc.parked.indexes()) == 0 }, 30*time.Second, 50*time.Millisecond, "nothing left parked")

	msg, err := repo_service.RenameBranch(ctx, sqlRepo, doer, gitRepo, "lag-from", "lag-to")
	require.NoError(t, err)
	require.Empty(t, msg)
	tableAt(t, tc, "refs/heads/lag-to", c2, "the renamed branch holds the pushed commit")
	row := unittest.AssertExistsAndLoadBean(t, &git_model.Branch{RepoID: 1, Name: "lag-to"})
	t.Logf("SQL lag-to=%.8s (push c2=%.8s); the parked push's update found %d pull requests with head lag-from", row.CommitID, c2, found.Load())
	assert.Equal(t, c2, row.CommitID, "the row of the branch renamed after its parked push keeps the commit before the push")
	assert.Positive(t, found.Load(), "the parked push's update of the pull requests passes over pull request 2")
	pr := unittest.AssertExistsAndLoadBean(t, &issues_model.PullRequest{ID: 2})
	assert.Equal(t, "lag-to", pr.HeadBranch)
}
