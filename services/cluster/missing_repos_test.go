// Copyright 2026 The Forgejo Authors. All rights reserved.
// SPDX-License-Identifier: GPL-3.0-or-later

package cluster

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"forgejo.org/models/db"
	repo_model "forgejo.org/models/repo"
	"forgejo.org/models/unittest"
	user_model "forgejo.org/models/user"
	"forgejo.org/modules/clusterhook"
	"forgejo.org/modules/setting"
	"forgejo.org/modules/test"
	repo_service "forgejo.org/services/repository"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// One replica of an acknowledged repository is lost on node-b alone (the
// projector is left running). The admin dashboard action "Delete all
// repositories missing their Git files", run on node-b, deleted it from
// SQL, the table and every member (#51): a node of a cluster refuses it.
func TestDeleteMissingRepositoriesIsRefusedInACluster(t *testing.T) {
	require.NoError(t, unittest.PrepareTestDatabase())
	fixtures := setting.RepoRootPath
	tc := newTestCluster(t, "node-a", "node-b", "node-c")
	a, b, c := tc.nodes[0], tc.nodes[1], tc.nodes[2]
	for _, n := range tc.nodes {
		require.NoError(t, unittest.CopyDir(fixtures, n.r.root))
	}
	withGitThroughReplicator(t, a)
	ctx := db.DefaultContext
	owner := unittest.AssertExistsAndLoadBean(t, &user_model.User{ID: 4})

	key := "user4/precious.git"
	repo, err := repo_service.CreateRepositoryDirectly(ctx, owner, owner, repo_service.CreateRepoOptions{Name: "precious", AutoInit: true, Readme: "Default", DefaultBranch: "main"})
	require.NoError(t, err)
	for _, n := range tc.nodes {
		require.Eventually(t, func() bool {
			v, ok := n.r.table.Get(key, "refs/heads/main")
			return ok && dirExists(n, key) && gitRef(t, n, key, "refs/heads/main") == v.SHA
		}, 10*time.Second, 20*time.Millisecond, n.slot)
	}
	require.True(t, b.r.Ready(), "node-b ready")

	require.NoError(t, os.RemoveAll(filepath.Join(b.r.root, "user4", "precious.git")))
	time.Sleep(3 * time.Second) // two projector ticks
	t.Logf("node-b dir after 3s with its projector running: %v (not restored)", dirExists(b, key))

	defer test.MockVariableValue(&setting.RepoRootPath, b.r.root)()
	clusterhook.SetRepoLifecycle(b.r.proposeRepoDelete, b.r.proposeRepoMoves)
	require.ErrorIs(t, repo_service.DeleteMissingRepositories(ctx), repo_service.ErrMissingReposInCluster)

	time.Sleep(2 * time.Second)
	exists, _ := repo_model.IsRepositoryModelExist(ctx, owner, "precious")
	_, delA := a.r.table.Deleted(key)
	_, delC := c.r.table.Deleted(key)
	t.Logf("after DeleteMissingRepositories on node-b: SQL row=%v table deleted on a=%v c=%v; files on a=%v c=%v", exists, delA, delC, dirExists(a, key), dirExists(c, key))
	require.True(t, exists, "repository %d deleted cluster-wide although node-a and node-c hold it", repo.ID)
	assert.False(t, delA || delC, "the repository's key was deleted in the table")
	assert.True(t, dirExists(a, key) && dirExists(c, key), "the other members lost their copy")
}

// Reinitialize on node-b (its copy lost, projector running and ready): the
// `git init` it ran rewrote the cluster's HEAD on every member (#51). A node
// of a cluster refuses it.
func TestReinitMissingRepositoriesIsRefusedInACluster(t *testing.T) {
	require.NoError(t, unittest.PrepareTestDatabase())
	fixtures := setting.RepoRootPath
	tc := newTestCluster(t, "node-a", "node-b", "node-c")
	a, b := tc.nodes[0], tc.nodes[1]
	for _, n := range tc.nodes {
		require.NoError(t, unittest.CopyDir(fixtures, n.r.root))
	}
	withGitThroughReplicator(t, b)
	ctx := db.DefaultContext
	owner := unittest.AssertExistsAndLoadBean(t, &user_model.User{ID: 4})

	key := "user4/precious.git"
	repo, err := repo_service.CreateRepositoryDirectly(ctx, owner, owner, repo_service.CreateRepoOptions{Name: "precious", AutoInit: true, Readme: "Default", DefaultBranch: "develop"})
	require.NoError(t, err)
	for _, n := range tc.nodes {
		require.Eventually(t, func() bool {
			v, ok := n.r.table.Get(key, "refs/heads/develop")
			return ok && gitRef(t, n, key, "refs/heads/develop") == v.SHA
		}, 10*time.Second, 20*time.Millisecond, n.slot)
	}
	before, _ := a.r.table.Get(key, "HEAD")
	require.NoError(t, os.RemoveAll(filepath.Join(b.r.root, "user4", "precious.git")))
	require.ErrorIs(t, repo_service.ReinitMissingRepositories(ctx), repo_service.ErrMissingReposInCluster)

	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if v, _ := a.r.table.Get(key, "HEAD"); v.Symbolic != before.Symbolic {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	time.Sleep(time.Second) // let node-a's projector follow
	after, _ := a.r.table.Get(key, "HEAD")
	refs, _ := readGitRefs(ctx, filepath.Join(a.r.root, "user4", "precious.git"))
	fresh := unittest.AssertExistsAndLoadBean(t, &repo_model.Repository{ID: repo.ID})
	t.Logf("table HEAD before=%+v after=%+v; git HEAD on node-a=%+v; SQL default_branch=%q", before, after, refs["HEAD"], fresh.DefaultBranch)
	require.Equal(t, before.Symbolic, after.Symbolic, "the cluster's HEAD was rewritten by node-b's local reinit")
}
