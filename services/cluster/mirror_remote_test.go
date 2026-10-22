// Copyright 2026 The Forgejo Authors. All rights reserved.
// SPDX-License-Identifier: GPL-3.0-or-later

package cluster

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"forgejo.org/models/db"
	repo_model "forgejo.org/models/repo"
	"forgejo.org/models/unittest"
	user_model "forgejo.org/models/user"
	"forgejo.org/modules/migration"
	"forgejo.org/modules/setting"
	"forgejo.org/modules/test"
	"forgejo.org/services/doctor"
	mirror_service "forgejo.org/services/mirror"
	repo_service "forgejo.org/services/repository"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// A mirror's remote lives in a node's git config, which the cluster does
// not replicate: it existed on the node that created the mirror only. A
// node that lacks it recreates it from SQL before each mirror operation,
// and doctor never removes a push mirror on a node's word (#79).
func TestHuntR4ProveMirrorRemote(t *testing.T) {
	require.NoError(t, unittest.PrepareTestDatabase())
	tc := newTestCluster(t, "node-a", "node-b", "node-c")
	for _, n := range tc.nodes {
		n.r.objects.repoKnown = n.r.repoKnown
	}
	origin := tc.nodes[0]
	baseDir := filepath.Join(t.TempDir(), "upstream.git")
	require.NoError(t, unittest.CopyDir(filepath.Join(setting.RepoRootPath, "user2", "repo1.git"), baseDir))
	withGitThroughReplicator(t, origin)
	ctx := db.DefaultContext

	mirror := unittest.AssertExistsAndLoadBean(t, &repo_model.Repository{ID: 5})
	owner := unittest.AssertExistsAndLoadBean(t, &user_model.User{ID: mirror.OwnerID})
	// The upstream is a local path, which its owner may mirror; the
	// fixture's mirror row, without an address, makes way for the one the
	// migration writes.
	defer test.MockVariableValue(&setting.ImportLocalPaths, true)()
	owner.AllowImportLocal = true
	_, err := db.GetEngine(ctx).ID(owner.ID).Cols("allow_import_local").Update(owner)
	require.NoError(t, err)
	_, err = db.DeleteByBean(ctx, &repo_model.Mirror{RepoID: mirror.ID})
	require.NoError(t, err)
	_, err = repo_service.MigrateRepositoryGitData(ctx, owner, mirror, migration.MigrateOptions{
		CloneAddr: baseDir, RepoName: mirror.Name, Mirror: true, Releases: true,
	}, nil)
	require.NoError(t, err)
	key := owner.LowerName + "/" + mirror.LowerName + ".git"
	requireLiveEverywhere(t, tc, key)
	remoteOn := func(n *testNode, name string) string {
		out, _ := gitOut(t, filepath.Join(n.r.root, filepath.FromSlash(key)), "config", "--get", "remote."+name+".url")
		return out
	}
	require.Equal(t, baseDir, remoteOn(origin, "origin"), "the node that migrated holds the remote")

	// A pull mirror syncs from a node that never held its remote.
	other := tc.nodes[1]
	require.Empty(t, remoteOn(other, "origin"))
	func() {
		defer test.MockVariableValue(&setting.RepoRootPath, other.r.root)()
		assert.True(t, mirror_service.SyncPullMirror(ctx, mirror.ID), "a pull mirror syncs on %s", other.slot)
	}()
	assert.Equal(t, baseDir, remoteOn(other, "origin"))
	fetch, _ := gitOut(t, filepath.Join(other.r.root, filepath.FromSlash(key)), "config", "--get-all", "remote.origin.fetch")
	assert.Contains(t, fetch, "+refs/*:refs/*")

	// Its address changes from a node that never held the remote.
	third := tc.nodes[2]
	func() {
		defer test.MockVariableValue(&setting.RepoRootPath, third.r.root)()
		m, err := repo_model.GetMirrorByRepoID(ctx, mirror.ID)
		require.NoError(t, err)
		assert.NoError(t, mirror_service.UpdateAddress(ctx, m, baseDir))
	}()
	assert.Equal(t, baseDir, remoteOn(third, "origin"))

	// A push mirror pushes from a node that never held its remote, and
	// doctor keeps its row there.
	target := filepath.Join(t.TempDir(), "target.git")
	run(t, filepath.Dir(target), "init", "-q", "--bare", target)
	pm := &repo_model.PushMirror{RepoID: mirror.ID, RemoteName: "remote_mirror_hunt", RemoteAddress: target}
	require.NoError(t, db.Insert(ctx, pm))
	func() {
		defer test.MockVariableValue(&setting.RepoRootPath, third.r.root)()
		require.NoError(t, doctor.FixPushMirrorsWithoutGitRemote(ctx, nil, true))
		unittest.AssertExistsAndLoadBean(t, &repo_model.PushMirror{ID: pm.ID})
		assert.True(t, mirror_service.SyncPushMirror(ctx, pm.ID), "a push mirror pushes from %s", third.slot)
	}()
	assert.Equal(t, target, remoteOn(third, pm.RemoteName))
	want := gitRef(t, third, key, "refs/heads/master")
	pushed, _ := gitOut(t, target, "rev-parse", "refs/heads/master")
	assert.Equal(t, want, pushed)
}

// gitOut runs git in dir, without the hook, and returns its trimmed
// output and whether it succeeded.
func gitOut(t *testing.T, dir string, args ...string) (string, bool) {
	t.Helper()
	cmd := exec.Command("git", append([]string{"-c", "core.hooksPath=/dev/null"}, args...)...)
	cmd.Dir = dir
	cmd.Env = append(os.Environ(), "HOME="+setting.Git.HomePath)
	out, err := cmd.Output()
	return strings.TrimSpace(string(out)), err == nil
}
