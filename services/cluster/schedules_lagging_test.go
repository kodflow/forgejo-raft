// Copyright 2026 The Forgejo Authors. All rights reserved.
// SPDX-License-Identifier: GPL-3.0-or-later

package cluster

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"forgejo.org/models/db"
	repo_model "forgejo.org/models/repo"
	"forgejo.org/models/unit"
	"forgejo.org/models/unittest"
	"forgejo.org/modules/clusterhook"
	"forgejo.org/modules/setting"
	"forgejo.org/modules/test"
	actions_service "forgejo.org/services/actions"
	repo_service "forgejo.org/services/repository"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// scheduleCluster builds a three-node cluster holding laggingRepo with
// master, its default branch, at c0 on every node, then pushes d1 to
// master through a node other than the leader, acknowledged: d1 adds a
// scheduled workflow. The workflow of d1 is then registered, as the push's
// actions notifier does where the consumer derives it: the test binary
// registers no notifier. When holdB is true, b, the third node, stops
// projecting the repository before that push and shows master at c0
// still; its table applied d1.
func scheduleCluster(t *testing.T, holdB bool) (tc *testCluster, b *testNode, c0, d1, hold string) {
	t.Helper()
	require.NoError(t, unittest.PrepareTestDatabase())
	tc = newTestCluster(t, "node-a", "node-b", "node-c")
	work := t.TempDir()
	run(t, work, "init", "-q")
	c0 = commit(t, work, "zero")
	zero := strings.Repeat("0", 40)
	require.Equal(t, int64(1), tc.repoID(laggingRepo))
	require.Equal(t, "master", unittest.AssertExistsAndLoadBean(t, &repo_model.Repository{ID: 1}).DefaultBranch)

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
	require.True(t, tc.pushVia(f, work, laggingRepo, "refs/heads/master", zero, c0).OK)
	for _, m := range tc.nodes {
		require.Eventually(t, func() bool { return gitRef(t, m, laggingRepo, "refs/heads/master") == c0 }, 15*time.Second, 20*time.Millisecond, "%s projects master", m.slot)
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
	resp := tc.pushVia(f, work, laggingRepo, "refs/heads/master", c0, d1)
	require.True(t, resp.OK, resp.Message)
	for _, m := range []*testNode{leader, f} {
		require.Eventually(t, func() bool { return gitRef(t, m, laggingRepo, "refs/heads/master") == d1 }, 15*time.Second, 20*time.Millisecond, "%s projects d1", m.slot)
	}
	tableAt(t, tc, "refs/heads/master", d1, "the push of d1")

	restore := test.MockVariableValue(&setting.RepoRootPath, leader.r.root)
	require.NoError(t, actions_service.DetectAndHandleSchedules(db.DefaultContext, unittest.AssertExistsAndLoadBean(t, &repo_model.Repository{ID: 1})))
	restore()
	masterNightlyAt(t, d1)
	return tc, b, c0, d1, hold
}

// masterNightlyAt checks that the scheduled workflows registered are the
// nightly workflow of master at sha, and only it.
func masterNightlyAt(t *testing.T, sha string) {
	t.Helper()
	got := repoSchedules(t)
	require.Len(t, got, 1, "the scheduled workflows of master at %.10s", sha)
	assert.Equal(t, "nightly.yml", got[0].WorkflowID)
	assert.Equal(t, sha, got[0].CommitSHA)
	assert.Equal(t, "refs/heads/master", got[0].Ref)
}

// enableActions saves the Actions unit of repo1, as the units settings
// and PATCH /repos/{owner}/{repo} with has_actions do: the scheduled
// workflows of the default branch are cleaned, then registered again from
// the branch's head (UpdateRepositoryUnits).
func enableActions(t *testing.T) error {
	t.Helper()
	repo := unittest.AssertExistsAndLoadBean(t, &repo_model.Repository{ID: 1})
	return repo_service.UpdateRepositoryUnits(db.DefaultContext, repo, []repo_model.RepoUnit{{RepoID: repo.ID, Type: unit.TypeActions}}, nil)
}

// archive archives repo1 as the settings and the API do, its scheduled
// workflows removed.
func archive(t *testing.T) {
	t.Helper()
	ctx := db.DefaultContext
	repo := unittest.AssertExistsAndLoadBean(t, &repo_model.Repository{ID: 1})
	require.NoError(t, repo_model.SetArchiveRepoState(ctx, repo, true))
	require.NoError(t, actions_service.CleanRepoScheduleTasks(ctx, repo, true))
	assert.Empty(t, repoSchedules(t))
}

// unarchive lifts the archiving of repo1 as the settings and the API do.
func unarchive(t *testing.T) error {
	t.Helper()
	repo := unittest.AssertExistsAndLoadBean(t, &repo_model.Repository{ID: 1})
	return repo_service.UnarchiveRepository(db.DefaultContext, repo)
}

// master is moved from c0 to d1, which adds a scheduled workflow, through
// a node other than the leader, acknowledged, and the workflow of d1 is
// registered. Node b is behind on the repository. The Actions unit is
// saved through b: UpdateRepositoryUnits cleaned the registered schedules,
// then registered those of master read in b's git, at c0, none: the
// nightly workflow of d1 never ran again, nothing detecting it before the
// next push to master (#313). Now the save waits for b to serve the
// repository at the cluster's commit index, and is refused while it does
// not, nothing written: the schedules registered are kept. Once b caught
// up, the same save registers the workflow of d1 again.
func TestActionsEnabledOnALaggingNodeIsRefused(t *testing.T) {
	_, b, c0, d1, hold := scheduleCluster(t, true)
	held := true
	t.Cleanup(func() {
		if held {
			b.r.registry.release(hold)
		}
	})
	require.Equal(t, c0, gitRef(t, b, laggingRepo, "refs/heads/master"), "b has not projected d1")

	withGitThroughReplicator(t, b)
	freshFrom(t, b)
	err := enableActions(t)
	t.Logf("UpdateRepositoryUnits(Actions) on %s, behind on master (%.10s here, %.10s in the table): %v; schedules after: %d", b.slot, c0, d1, err, len(repoSchedules(t)))
	require.ErrorIs(t, err, clusterhook.ErrBehind)
	masterNightlyAt(t, d1)

	b.r.registry.release(hold)
	held = false
	require.Eventually(t, func() bool { return gitRef(t, b, laggingRepo, "refs/heads/master") == d1 }, 15*time.Second, 20*time.Millisecond, "b catches up")
	require.NoError(t, enableActions(t))
	masterNightlyAt(t, d1)
}

// The same push, then the repository is archived, which removes its
// scheduled workflows. Lifting the archiving through b, behind on the
// repository, registered the scheduled workflows of master read in b's
// git, at c0, none (#313). It is refused now, nothing written: the
// repository stays archived. Once b caught up, it is lifted and the
// workflow of d1 registered.
func TestAnUnarchivingOnALaggingNodeIsRefused(t *testing.T) {
	_, b, c0, d1, hold := scheduleCluster(t, true)
	held := true
	t.Cleanup(func() {
		if held {
			b.r.registry.release(hold)
		}
	})
	require.Equal(t, c0, gitRef(t, b, laggingRepo, "refs/heads/master"), "b has not projected d1")
	archive(t)

	withGitThroughReplicator(t, b)
	freshFrom(t, b)
	err := unarchive(t)
	t.Logf("UnarchiveRepository on %s, behind on master (%.10s here, %.10s in the table): %v", b.slot, c0, d1, err)
	require.ErrorIs(t, err, clusterhook.ErrBehind)
	assert.True(t, unittest.AssertExistsAndLoadBean(t, &repo_model.Repository{ID: 1}).IsArchived, "the repository stays archived")
	assert.Empty(t, repoSchedules(t), "no schedule is registered")

	b.r.registry.release(hold)
	held = false
	require.Eventually(t, func() bool { return gitRef(t, b, laggingRepo, "refs/heads/master") == d1 }, 15*time.Second, 20*time.Millisecond, "b catches up")
	require.NoError(t, unarchive(t))
	assert.False(t, unittest.AssertExistsAndLoadBean(t, &repo_model.Repository{ID: 1}).IsArchived)
	masterNightlyAt(t, d1)
}

// A node that serves the repository at the cluster's commit index lifts
// the archiving at once, and registers the scheduled workflows of the
// default branch's acknowledged head; saving the Actions unit there
// registers them again.
func TestAnUnarchivingOnAFreshNodeRegistersItsSchedules(t *testing.T) {
	_, b, _, d1, _ := scheduleCluster(t, false)
	require.Eventually(t, func() bool { return gitRef(t, b, laggingRepo, "refs/heads/master") == d1 }, 15*time.Second, 20*time.Millisecond, "b projects d1")
	archive(t)

	withGitThroughReplicator(t, b)
	freshFrom(t, b)
	require.NoError(t, unarchive(t))
	assert.False(t, unittest.AssertExistsAndLoadBean(t, &repo_model.Repository{ID: 1}).IsArchived)
	masterNightlyAt(t, d1)

	require.NoError(t, enableActions(t))
	masterNightlyAt(t, d1)
}
