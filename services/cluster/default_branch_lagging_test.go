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
	"forgejo.org/modules/clusterhook"
	"forgejo.org/modules/gitrepo"
	actions_service "forgejo.org/services/actions"
	repo_service "forgejo.org/services/repository"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// nightlyWorkflow is a scheduled workflow, which actions registers only
// from the head of the default branch.
const nightlyWorkflow = `on:
  schedule:
    - cron: '*/5 * * * *'
jobs:
  nightly:
    runs-on: docker
    steps:
      - run: echo nightly
`

// repoSchedules is the scheduled workflows registered for laggingRepo.
func repoSchedules(t *testing.T) []*actions_model.ActionSchedule {
	t.Helper()
	var rows []*actions_model.ActionSchedule
	require.NoError(t, db.GetEngine(db.DefaultContext).Where("repo_id = ?", 1).Find(&rows))
	return rows
}

// defaultBranchCluster builds a three-node cluster holding laggingRepo
// with main and dev at c0 on every node, then pushes d1 to dev through a
// node other than the leader, acknowledged: d1 adds a scheduled workflow.
// When hold is true, b, the third node, stops projecting the repository
// before that push and shows dev at c0 still; its table applied d1.
func defaultBranchCluster(t *testing.T, holdB bool) (tc *testCluster, b *testNode, c0, d1, hold string) {
	t.Helper()
	require.NoError(t, unittest.PrepareTestDatabase())
	tc = newTestCluster(t, "node-a", "node-b", "node-c")
	work := t.TempDir()
	run(t, work, "init", "-q")
	c0 = commit(t, work, "zero")
	zero := strings.Repeat("0", 40)
	require.Equal(t, int64(1), tc.repoID(laggingRepo))

	leader := clusterLeader(t, tc)
	var f *testNode
	for _, n := range tc.nodes {
		switch {
		case n == leader:
		case f == nil:
			f = n
		default:
			b = n
		}
	}
	require.True(t, tc.pushVia(f, work, laggingRepo, "refs/heads/main", zero, c0).OK)
	require.True(t, tc.pushVia(f, work, laggingRepo, "refs/heads/dev", zero, c0).OK)
	for _, m := range tc.nodes {
		require.Eventually(t, func() bool { return gitRef(t, m, laggingRepo, "refs/heads/dev") == c0 }, 15*time.Second, 20*time.Millisecond, "%s projects dev", m.slot)
	}

	if holdB {
		hold = clusterhook.NewNonce()
		require.NoError(t, b.r.registry.register(hold, laggingRepo, nil, os.Getpid(), false))
	}

	require.NoError(t, os.MkdirAll(filepath.Join(work, ".forgejo", "workflows"), 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(work, ".forgejo", "workflows", "nightly.yml"), []byte(nightlyWorkflow), 0o644))
	run(t, work, "add", ".")
	run(t, work, "commit", "-q", "-m", "nightly")
	d1 = run(t, work, "rev-parse", "HEAD")
	resp := tc.pushVia(f, work, laggingRepo, "refs/heads/dev", c0, d1)
	require.True(t, resp.OK, resp.Message)
	for _, m := range []*testNode{leader, f} {
		require.Eventually(t, func() bool { return gitRef(t, m, laggingRepo, "refs/heads/dev") == d1 }, 15*time.Second, 20*time.Millisecond, "%s projects d1", m.slot)
	}
	tableAt(t, tc, "refs/heads/dev", d1, "the push of d1")
	return tc, b, c0, d1, hold
}

// setDefaultBranch is the settings form "Default branch" served by node n
// (SetRepoDefaultBranch), then what the actions notifier runs on the change
// (ChangeDefaultBranch, DetectAndHandleSchedules) when it succeeds.
func setDefaultBranch(t *testing.T, branch string) error {
	t.Helper()
	ctx := db.DefaultContext
	repo := unittest.AssertExistsAndLoadBean(t, &repo_model.Repository{ID: 1})
	gitRepo, err := gitrepo.OpenRepository(ctx, repo)
	require.NoError(t, err)
	defer gitRepo.Close()
	if err := repo_service.SetRepoDefaultBranch(ctx, repo, gitRepo, branch); err != nil {
		return err
	}
	repo = unittest.AssertExistsAndLoadBean(t, &repo_model.Repository{ID: 1})
	require.NoError(t, actions_service.DetectAndHandleSchedules(ctx, repo))
	return nil
}

// headIn waits until every node's table holds HEAD pointing to branch.
func headIn(t *testing.T, tc *testCluster, branch string) {
	t.Helper()
	for _, n := range tc.nodes {
		require.Eventually(t, func() bool {
			v, ok := n.r.table.Get(laggingRepo, "HEAD")
			return ok && v.Symbolic == "refs/heads/"+branch
		}, 10*time.Second, 20*time.Millisecond, "%s: HEAD refs/heads/%s in the table", n.slot, branch)
	}
}

// onlyNightlyAt checks that the scheduled workflows registered are the
// nightly workflow of dev at sha, and only it.
func onlyNightlyAt(t *testing.T, sha string) {
	t.Helper()
	got := repoSchedules(t)
	require.Len(t, got, 1, "the scheduled workflows of dev at %.10s", sha)
	assert.Equal(t, "nightly.yml", got[0].WorkflowID)
	assert.Equal(t, sha, got[0].CommitSHA)
	assert.Equal(t, "refs/heads/dev", got[0].Ref)
}

// dev is moved from c0 to d1, which adds a scheduled workflow, through a
// node other than the leader, acknowledged, while dev is not the default
// branch: the leader's consumer registers no schedule for it (handleSchedules
// skips a branch that is not the default one). Node b is behind on the
// repository. The default branch is set to dev through b. It read dev in
// b's git, at c0, wrote the change, and the actions notifier registered the
// scheduled workflows of c0, none: the nightly workflow of d1 never ran,
// nothing detecting it again before the next push to dev (#299). Now the
// change waits for b to serve the repository at the cluster's commit
// index, and is refused while it does not, nothing written: neither the
// default branch nor HEAD, and the schedules registered are kept. Once b
// caught up, the same change registers the workflow of d1.
func TestADefaultBranchSetOnALaggingNodeIsRefused(t *testing.T) {
	tc, b, c0, d1, hold := defaultBranchCluster(t, true)
	held := true
	t.Cleanup(func() {
		if held {
			b.r.registry.release(hold)
		}
	})
	require.Equal(t, c0, gitRef(t, b, laggingRepo, "refs/heads/dev"), "b has not projected d1")
	headBefore, _ := b.r.table.Get(laggingRepo, "HEAD")
	ctx := db.DefaultContext
	kept := &actions_model.ActionSchedule{RepoID: 1, OwnerID: 2, WorkflowID: "kept.yml", Ref: "refs/heads/master", CommitSHA: c0}
	require.NoError(t, db.Insert(ctx, kept))

	withGitThroughReplicator(t, b)
	freshFrom(t, b)
	err := setDefaultBranch(t, "dev")
	t.Logf("SetRepoDefaultBranch(dev) on %s, behind on dev (%.10s here, %.10s in the table): %v", b.slot, c0, d1, err)
	require.ErrorIs(t, err, clusterhook.ErrBehind)
	repo := unittest.AssertExistsAndLoadBean(t, &repo_model.Repository{ID: 1})
	assert.Equal(t, "master", repo.DefaultBranch, "the default branch is not changed")
	for _, n := range tc.nodes {
		v, _ := n.r.table.Get(laggingRepo, "HEAD")
		assert.Equal(t, headBefore.Symbolic, v.Symbolic, "%s: HEAD is not changed", n.slot)
	}
	unittest.AssertExistsAndLoadBean(t, &actions_model.ActionSchedule{ID: kept.ID})
	assert.Len(t, repoSchedules(t), 1, "no schedule is registered nor deleted")

	b.r.registry.release(hold)
	held = false
	require.Eventually(t, func() bool { return gitRef(t, b, laggingRepo, "refs/heads/dev") == d1 }, 15*time.Second, 20*time.Millisecond, "b catches up")
	require.NoError(t, setDefaultBranch(t, "dev"))
	repo = unittest.AssertExistsAndLoadBean(t, &repo_model.Repository{ID: 1})
	assert.Equal(t, "dev", repo.DefaultBranch)
	headIn(t, tc, "dev")
	onlyNightlyAt(t, d1)
}

// A node that serves the repository at the cluster's commit index changes
// the default branch at once, and the actions notifier registers the
// scheduled workflows of the branch's acknowledged head.
func TestADefaultBranchSetOnAFreshNodeRegistersItsSchedules(t *testing.T) {
	tc, b, _, d1, _ := defaultBranchCluster(t, false)
	require.Eventually(t, func() bool { return gitRef(t, b, laggingRepo, "refs/heads/dev") == d1 }, 15*time.Second, 20*time.Millisecond, "b projects d1")

	withGitThroughReplicator(t, b)
	freshFrom(t, b)
	require.NoError(t, setDefaultBranch(t, "dev"))
	repo := unittest.AssertExistsAndLoadBean(t, &repo_model.Repository{ID: 1})
	assert.Equal(t, "dev", repo.DefaultBranch)
	headIn(t, tc, "dev")
	onlyNightlyAt(t, d1)
}
