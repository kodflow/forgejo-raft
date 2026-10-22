// Copyright 2026 The Forgejo Authors. All rights reserved.
// SPDX-License-Identifier: GPL-3.0-or-later

package cluster

import (
	"path/filepath"
	"testing"
	"time"

	"forgejo.org/models/db"
	repo_model "forgejo.org/models/repo"
	"forgejo.org/models/unittest"
	user_model "forgejo.org/models/user"
	"forgejo.org/modules/git"
	"forgejo.org/modules/gitrepo"
	repo_service "forgejo.org/services/repository"

	"github.com/stretchr/testify/require"
)

// A node of a cluster runs git 2.48 or later (#83): HEAD reaches the table
// through the reference-transaction hook, with git init and symbolic-ref,
// and a push to the default branch reports no HEAD line. With an older git
// these failed: a creation with "Initialize repository" (its objects were
// shipped for a key the members did not know), a default branch change
// (never replicated), a push to the default branch (refused).

// Creation with "Initialize repository", the members' repoKnown on.
func TestHEADThroughTheHookAutoInit(t *testing.T) {
	t.Logf("GIT: %s", git.VersionInfo())
	require.NoError(t, unittest.PrepareTestDatabase())
	tc := newTestCluster(t, "node-a", "node-b", "node-c")
	for _, n := range tc.nodes {
		n.r.objects.repoKnown = n.r.repoKnown
	}
	withGitThroughReplicator(t, tc.nodes[0])
	owner := unittest.AssertExistsAndLoadBean(t, &user_model.User{ID: 4})
	_, err := repo_service.CreateRepositoryDirectly(db.DefaultContext, owner, owner, repo_service.CreateRepoOptions{
		Name: "autoinit", AutoInit: true, Readme: "Default", DefaultBranch: "main",
	})
	require.NoError(t, err, "auto-initialised creation in a cluster")
	for _, n := range tc.nodes {
		require.Eventually(t, func() bool {
			v, ok := n.r.table.Get("user4/autoinit.git", "HEAD")
			return ok && v.Symbolic == "refs/heads/main"
		},
			10*time.Second, 20*time.Millisecond, "%s: HEAD in the table", n.slot)
	}
}

// A change of default branch reaches the table and every member's git.
func TestHEADThroughTheHookDefaultBranch(t *testing.T) {
	t.Logf("GIT: %s", git.VersionInfo())
	require.NoError(t, unittest.PrepareTestDatabase())
	tc := newTestCluster(t, "node-a", "node-b", "node-c")
	origin := tc.nodes[0]
	withGitThroughReplicator(t, origin)
	owner := unittest.AssertExistsAndLoadBean(t, &user_model.User{ID: 4})
	repo, err := repo_service.CreateRepositoryDirectly(db.DefaultContext, owner, owner, repo_service.CreateRepoOptions{Name: "defbranch"})
	require.NoError(t, err)
	key := "user4/defbranch.git"
	_, initOK := origin.r.table.Get(key, "HEAD")
	t.Logf("after git init: HEAD in origin's table = %v", initOK)

	repo = unittest.AssertExistsAndLoadBean(t, &repo_model.Repository{ID: repo.ID})
	require.NoError(t, gitrepo.SetDefaultBranch(db.DefaultContext, repo, "dev"))
	head, _, err := git.NewCommand(db.DefaultContext, "symbolic-ref", "HEAD").RunStdString(&git.RunOpts{Dir: filepath.Join(origin.r.root, "user4", "defbranch.git")})
	require.NoError(t, err)
	t.Logf("origin's git HEAD after SetDefaultBranch: %s", head)
	for _, n := range tc.nodes {
		require.Eventually(t, func() bool { v, ok := n.r.table.Get(key, "HEAD"); return ok && v.Symbolic == "refs/heads/dev" },
			10*time.Second, 20*time.Millisecond, "%s: HEAD refs/heads/dev in the table", n.slot)
	}
}

// A push to the branch HEAD points to, on the node where git created the
// repository: git <= 2.47 hands the hook a log-only "HEAD <sha>" line.
func TestHEADThroughTheHookPushToTheDefaultBranch(t *testing.T) {
	t.Logf("GIT: %s", git.VersionInfo())
	require.NoError(t, unittest.PrepareTestDatabase())
	tc := newTestCluster(t, "node-a", "node-b", "node-c")
	origin := tc.nodes[0]
	withGitThroughReplicator(t, origin)
	ctx := db.DefaultContext
	owner := unittest.AssertExistsAndLoadBean(t, &user_model.User{ID: 4})
	_, err := repo_service.CreateRepositoryDirectly(ctx, owner, owner, repo_service.CreateRepoOptions{Name: "pushdefault", DefaultBranch: "main"})
	require.NoError(t, err)
	key := "user4/pushdefault.git"
	dir := filepath.Join(origin.r.root, "user4", "pushdefault.git")
	head, _, err := git.NewCommand(ctx, "symbolic-ref", "HEAD").RunStdString(&git.RunOpts{Dir: dir})
	require.NoError(t, err)
	t.Logf("origin's git HEAD: %s", head)
	work := t.TempDir()
	run(t, work, "init", "-q")
	c1 := commit(t, work, "one")
	stdout, stderr, err := git.NewCommand(ctx, "push").AddDynamicArguments(dir, "main:refs/heads/main").RunStdString(&git.RunOpts{Dir: work})
	require.NoError(t, err, "push to the default branch: %s %s", stdout, stderr)
	for _, n := range tc.nodes {
		require.Eventually(t, func() bool { v, ok := n.r.table.Get(key, "refs/heads/main"); return ok && v.SHA == c1 },
			10*time.Second, 20*time.Millisecond, "%s: refs/heads/main in the table", n.slot)
	}
}
