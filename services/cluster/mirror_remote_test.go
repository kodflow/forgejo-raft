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
	"forgejo.org/modules/git"
	"forgejo.org/modules/setting"
	"forgejo.org/modules/test"
	"forgejo.org/services/doctor"
	mirror_service "forgejo.org/services/mirror"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// A mirror's remote lives in a node's git config, which the cluster does
// not replicate: it existed on the node that created the mirror only. A
// node that lacks it recreates it from SQL before each mirror operation,
// and doctor never removes a push mirror on a node's word (#79). The
// mirror's key comes from `git init` alone, so every node holds the
// repository without any remote, as the nodes that did not migrate it do.
func TestHuntR4ProveMirrorRemote(t *testing.T) {
	require.NoError(t, unittest.PrepareTestDatabase())
	tc := newTestCluster(t, "node-a", "node-b", "node-c")
	origin := tc.nodes[0]
	upstream := filepath.Join(t.TempDir(), "upstream.git")
	require.NoError(t, unittest.CopyDir(filepath.Join(setting.RepoRootPath, "user2", "repo1.git"), upstream))
	withGitThroughReplicator(t, origin)
	ctx := db.DefaultContext

	mirror := unittest.AssertExistsAndLoadBean(t, &repo_model.Repository{ID: 5})
	owner := unittest.AssertExistsAndLoadBean(t, &user_model.User{ID: mirror.OwnerID})
	// The upstream is a local path, which its owner may mirror; the
	// fixture's mirror row, without an address, makes way for one that
	// holds it, as a migration writes.
	defer test.MockVariableValue(&setting.ImportLocalPaths, true)()
	owner.AllowImportLocal = true
	_, err := db.GetEngine(ctx).ID(owner.ID).Cols("allow_import_local").Update(owner)
	require.NoError(t, err)
	_, err = db.DeleteByBean(ctx, &repo_model.Mirror{RepoID: mirror.ID})
	require.NoError(t, err)
	require.NoError(t, (&repo_model.Mirror{RepoID: mirror.ID, Interval: setting.Mirror.DefaultInterval, EnablePrune: true}).InsertWithAddress(ctx, upstream))

	key := owner.LowerName + "/" + mirror.LowerName + ".git"
	wikiKey := owner.LowerName + "/" + mirror.LowerName + ".wiki.git"
	for _, k := range []string{key, wikiKey} {
		require.NoError(t, git.InitRepository(ctx, filepath.Join(origin.r.root, filepath.FromSlash(k)), true, git.Sha1ObjectFormat.Name()))
		requireLiveEverywhere(t, tc, k)
	}
	// The upstream serves a wiki with a page, which the mirror's wiki
	// fetches and its push mirror pushes.
	upstreamWiki := strings.TrimSuffix(upstream, ".git") + ".wiki.git"
	run(t, filepath.Dir(upstream), "init", "-q", "--bare", upstreamWiki)
	page := t.TempDir()
	run(t, page, "init", "-q")
	home := commit(t, page, "Home")
	run(t, page, "push", "-q", upstreamWiki, home+":refs/heads/main")
	repoDir := func(n *testNode) string { return filepath.Join(n.r.root, filepath.FromSlash(key)) }
	remoteIn := func(dir, name string) string {
		out, _ := gitOut(t, dir, "config", "--get", "remote."+name+".url")
		return out
	}
	remoteOn := func(n *testNode, name string) string { return remoteIn(repoDir(n), name) }
	wikiRemoteOn := func(n *testNode, name string) string {
		return remoteIn(filepath.Join(n.r.root, filepath.FromSlash(wikiKey)), name)
	}

	// A pull mirror syncs from a node that never held its remote.
	other := tc.nodes[1]
	require.Empty(t, remoteOn(other, "origin"))
	want, _ := gitOut(t, upstream, "rev-parse", "refs/heads/master")
	func() {
		defer test.MockVariableValue(&setting.RepoRootPath, other.r.root)()
		assert.True(t, mirror_service.SyncPullMirror(ctx, mirror.ID), "a pull mirror syncs on %s", other.slot)
	}()
	assert.Equal(t, upstream, remoteOn(other, "origin"))
	assert.Equal(t, strings.TrimSuffix(upstream, ".git")+".wiki.git", wikiRemoteOn(other, "origin"), "the wiki's remote is recreated too")
	fetch, _ := gitOut(t, repoDir(other), "config", "--get-all", "remote.origin.fetch")
	assert.Contains(t, fetch, "+refs/*:refs/*")
	got, _ := gitOut(t, repoDir(other), "rev-parse", "refs/heads/master")
	assert.Equal(t, want, got, "the mirror fetched the upstream")

	// Its address changes from a node that never held the remote.
	third := tc.nodes[2]
	func() {
		defer test.MockVariableValue(&setting.RepoRootPath, third.r.root)()
		m, err := repo_model.GetMirrorByRepoID(ctx, mirror.ID)
		require.NoError(t, err)
		assert.NoError(t, mirror_service.UpdateAddress(ctx, m, upstream))
	}()
	assert.Equal(t, upstream, remoteOn(third, "origin"))

	// A push mirror pushes from a node that never held its remote, and
	// doctor keeps its row there.
	target := filepath.Join(t.TempDir(), "target.git")
	run(t, filepath.Dir(target), "init", "-q", "--bare", target)
	run(t, filepath.Dir(target), "init", "-q", "--bare", strings.TrimSuffix(target, ".git")+".wiki.git")
	pm := &repo_model.PushMirror{RepoID: mirror.ID, RemoteName: "remote_mirror_hunt", RemoteAddress: target}
	require.NoError(t, db.Insert(ctx, pm))
	func() {
		defer test.MockVariableValue(&setting.RepoRootPath, other.r.root)()
		require.NoError(t, doctor.FixPushMirrorsWithoutGitRemote(ctx, nil, true))
		unittest.AssertExistsAndLoadBean(t, &repo_model.PushMirror{ID: pm.ID})
		assert.True(t, mirror_service.SyncPushMirror(ctx, pm.ID), "a push mirror pushes from %s", other.slot)
	}()
	assert.Equal(t, target, remoteOn(other, pm.RemoteName))
	// The wiki's remote is recreated before the check that skipped a wiki
	// without one (CodeRabbit on #123).
	assert.Equal(t, strings.TrimSuffix(target, ".git")+".wiki.git", wikiRemoteOn(other, pm.RemoteName))
	wikiPushed, _ := gitOut(t, strings.TrimSuffix(target, ".git")+".wiki.git", "rev-parse", "refs/heads/main")
	assert.Equal(t, home, wikiPushed, "the wiki is pushed too")
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
