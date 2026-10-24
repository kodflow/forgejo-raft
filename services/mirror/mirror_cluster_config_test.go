// Copyright 2026 The Forgejo Authors. All rights reserved.
// SPDX-License-Identifier: GPL-3.0-or-later

package mirror

import (
	"context"
	"net/http/cgi"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"forgejo.org/models/db"
	repo_model "forgejo.org/models/repo"
	"forgejo.org/models/unittest"
	"forgejo.org/modules/clusterhook"
	repo_module "forgejo.org/modules/repository"
	"forgejo.org/modules/setting"
	"forgejo.org/modules/test"
	migrations_allowlist "forgejo.org/services/migrations/allowlist"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// inCluster runs the test as a node of a cluster whose remotes are local
// git servers.
func inCluster(t *testing.T) {
	t.Helper()
	require.NoError(t, unittest.PrepareTestDatabase())
	was := clusterhook.Enabled()
	clusterhook.SetEnabled(true)
	t.Cleanup(func() { clusterhook.SetEnabled(was) })
	t.Cleanup(test.MockVariableValue(&setting.Migrations.AllowUnencrypted, true))
	t.Cleanup(test.MockVariableValue(&setting.Migrations.AllowedDomains, "127.0.0.1"))
	require.NoError(t, migrations_allowlist.Init())
	t.Cleanup(func() { _ = migrations_allowlist.Init() })
}

// openGitServer serves the bare repositories under root over HTTP to
// anyone.
func openGitServer(t *testing.T, root string) *httptest.Server {
	t.Helper()
	gitPath, err := exec.LookPath("git")
	require.NoError(t, err)
	backend := &cgi.Handler{
		Path: gitPath,
		Args: []string{"http-backend"},
		Env:  []string{"GIT_PROJECT_ROOT=" + root, "GIT_HTTP_EXPORT_ALL=1", "HOME=" + t.TempDir()},
	}
	srv := httptest.NewServer(backend)
	t.Cleanup(srv.Close)
	return srv
}

// pushRefSpecsIn returns the push refspecs of the remote name in the
// repository at dir.
func pushRefSpecsIn(dir, name string) string {
	return credGitTry(dir, "config", "--get-all", "remote."+name+".push")
}

// configState is a git config file as a node left it: its bytes and when
// it was last written.
func configState(t *testing.T, dir string) (string, time.Time) {
	t.Helper()
	path := filepath.Join(dir, "config")
	b, err := os.ReadFile(path)
	require.NoError(t, err)
	fi, err := os.Stat(path)
	require.NoError(t, err)
	return string(b), fi.ModTime()
}

// A push mirror's branch filter was written into the git config of the
// node that served the change only. A node that recreated the remote
// earlier (#79) kept the old push refspecs, and its syncs pushed every
// branch the owner took out of the mirror (#183). Every node now derives
// the push refspecs from SQL at each sync.
func TestABranchFilterChangeReachesEveryNode(t *testing.T) {
	inCluster(t)
	ctx := db.DefaultContext

	served := t.TempDir()
	credGit(t, served, "init", "-q", "--bare", "target.git")
	target := filepath.Join(served, "target.git")
	srv := credentialedGitServer(t, served)
	base := strings.Replace(srv.URL, "http://", "http://u:s3cret-pw@", 1)
	bare := srv.URL

	repo := unittest.AssertExistsAndLoadBean(t, &repo_model.Repository{ID: 1})
	fixture := filepath.Join(setting.RepoRootPath, "user2", "repo1.git")
	nodeA, nodeB := t.TempDir(), t.TempDir()
	key := filepath.Join(strings.ToLower(repo.OwnerName), strings.ToLower(repo.Name)+".git")
	for _, node := range []string{nodeA, nodeB} {
		require.NoError(t, os.MkdirAll(filepath.Dir(filepath.Join(node, key)), 0o755))
		require.NoError(t, unittest.CopyDir(fixture, filepath.Join(node, key)))
	}

	pm := &repo_model.PushMirror{RepoID: repo.ID, Repo: repo, RemoteName: "remote_mirror_filter", RemoteAddress: bare + "/target.git"}
	require.NoError(t, db.Insert(ctx, pm))
	onNode(nodeA, func() { require.NoError(t, AddPushMirrorRemote(ctx, pm, base+"/target.git")) })
	onNode(nodeB, func() { require.True(t, SyncPushMirror(ctx, pm.ID), "node B pushes, its remote recreated from SQL") })
	require.NotEmpty(t, credGitTry(target, "rev-parse", "--verify", "-q", "refs/heads/branch2"))

	// The owner restricts the mirror to master, the request served by node
	// A as the router does, then takes branch2 off the target.
	pm.BranchFilter = "master"
	require.NoError(t, repo_model.UpdatePushMirrorBranchFilter(ctx, pm))
	onNode(nodeA, func() { require.NoError(t, UpdatePushMirrorBranchFilter(ctx, pm)) })
	credGit(t, target, "update-ref", "-d", "refs/heads/branch2")

	onNode(nodeA, func() { require.True(t, SyncPushMirror(ctx, pm.ID)) })
	assert.Empty(t, credGitTry(target, "rev-parse", "--verify", "-q", "refs/heads/branch2"), "node A honours the filter")

	onNode(nodeB, func() { require.True(t, SyncPushMirror(ctx, pm.ID)) })
	assert.Empty(t, credGitTry(target, "rev-parse", "--verify", "-q", "refs/heads/branch2"), "node B pushed branch2, which the filter excludes")
	assert.Equal(t, pushRefSpecsIn(filepath.Join(nodeA, key), pm.RemoteName), pushRefSpecsIn(filepath.Join(nodeB, key), pm.RemoteName), "node B's push refspecs follow SQL")
	assert.Equal(t, "+refs/heads/master:refs/heads/master\n+refs/tags/*:refs/tags/*", pushRefSpecsIn(filepath.Join(nodeB, key), pm.RemoteName))

	// The filter taken off again: every branch comes back from node B.
	pm.BranchFilter = ""
	require.NoError(t, repo_model.UpdatePushMirrorBranchFilter(ctx, pm))
	onNode(nodeA, func() { require.NoError(t, UpdatePushMirrorBranchFilter(ctx, pm)) })
	onNode(nodeB, func() { require.True(t, SyncPushMirror(ctx, pm.ID)) })
	assert.NotEmpty(t, credGitTry(target, "rev-parse", "--verify", "-q", "refs/heads/branch2"), "node B follows the filter taken off")
}

// wikiUpstreams serves, under a fresh root, the repositories old.git and
// new.git, each with a wiki whose home page names it, and returns the root
// and each wiki's commit.
func wikiUpstreams(t *testing.T) (string, map[string]string) {
	t.Helper()
	served := t.TempDir()
	fixture := filepath.Join(setting.RepoRootPath, "user2", "repo1.git")
	wikiHome := map[string]string{}
	for _, up := range []string{"old", "new"} {
		require.NoError(t, unittest.CopyDir(fixture, filepath.Join(served, up+".git")))
		credGit(t, served, "init", "-q", "--bare", up+".wiki.git")
		page := t.TempDir()
		credGit(t, page, "init", "-q")
		require.NoError(t, os.WriteFile(filepath.Join(page, "Home.md"), []byte("Home of "+up), 0o644))
		credGit(t, page, "add", "Home.md")
		credGit(t, page, "-c", "user.name=t", "-c", "user.email=t@example.com", "commit", "-q", "-m", "Home of "+up)
		wikiHome[up] = credGit(t, page, "rev-parse", "HEAD")
		credGit(t, page, "push", "-q", filepath.Join(served, up+".wiki.git"), "HEAD:refs/heads/main")
	}
	return served, wikiHome
}

// pullMirrorNodes makes two nodes holding the repository and the wiki of
// the pull mirror repo, without any remote, and the mirror's row in SQL
// pointing at addr.
func pullMirrorNodes(t *testing.T, repo *repo_model.Repository, addr string) (nodeA, nodeB, key, wikiKey string) {
	t.Helper()
	nodeA, nodeB = t.TempDir(), t.TempDir()
	key = filepath.Join(strings.ToLower(repo.OwnerName), strings.ToLower(repo.Name)+".git")
	wikiKey = filepath.Join(strings.ToLower(repo.OwnerName), strings.ToLower(repo.Name)+".wiki.git")
	for _, node := range []string{nodeA, nodeB} {
		for _, dir := range []string{filepath.Join(node, key), filepath.Join(node, wikiKey)} {
			require.NoError(t, os.MkdirAll(filepath.Dir(dir), 0o755))
			credGit(t, filepath.Dir(dir), "init", "-q", "--bare", dir)
		}
	}
	_, err := db.DeleteByBean(db.DefaultContext, &repo_model.Mirror{RepoID: repo.ID})
	require.NoError(t, err)
	require.NoError(t, (&repo_model.Mirror{RepoID: repo.ID, Interval: setting.Mirror.DefaultInterval, EnablePrune: true}).InsertWithAddress(db.DefaultContext, addr))
	return nodeA, nodeB, key, wikiKey
}

// changeAddress points the pull mirror at addr, the request served by
// node, as the settings router does.
func changeAddress(t *testing.T, repo *repo_model.Repository, node, addr string) {
	t.Helper()
	ctx := db.DefaultContext
	m, err := repo_model.GetMirrorByRepoID(ctx, repo.ID)
	require.NoError(t, err)
	require.NoError(t, m.UpdateRemoteAddress(ctx, addr))
	maybe, err := m.SanitizedRemoteAddress()
	require.NoError(t, err)
	_, sanitized := maybe.Get()
	onNode(node, func() { require.NoError(t, UpdateAddress(ctx, m, sanitized)) })
}

// A pull mirror's new address was written into the git config of the node
// that served the change. The other nodes followed SQL for the
// repository's remote, not for the wiki's: ensurePullWikiRemote added a
// missing remote and never corrected one, so they kept fetching the wiki
// from the old address, and the two wikis replaced each other in the
// table at every sync (#183).
func TestAPullMirrorWikiFollowsTheAddressOnEveryNode(t *testing.T) {
	inCluster(t)
	ctx := db.DefaultContext
	served, wikiHome := wikiUpstreams(t)
	base := openGitServer(t, served).URL

	repo := unittest.AssertExistsAndLoadBean(t, &repo_model.Repository{ID: 5})
	nodeA, nodeB, key, wikiKey := pullMirrorNodes(t, repo, base+"/old.git")
	for _, node := range []string{nodeA, nodeB} {
		onNode(node, func() { require.True(t, SyncPullMirror(ctx, repo.ID)) })
		require.Equal(t, wikiHome["old"], credGitTry(filepath.Join(node, wikiKey), "rev-parse", "refs/heads/main"))
	}

	changeAddress(t, repo, nodeA, base+"/new.git")
	for name, node := range map[string]string{"A": nodeA, "B": nodeB} {
		onNode(node, func() { assert.True(t, SyncPullMirror(ctx, repo.ID), "node %s syncs", name) })
	}
	assert.Equal(t, base+"/new.git", credGitTry(filepath.Join(nodeB, key), "config", "--get", "remote.origin.url"))
	assert.Equal(t, base+"/new.wiki.git", credGitTry(filepath.Join(nodeB, wikiKey), "config", "--get", "remote.origin.url"), "node B's wiki remote follows SQL")
	assert.Equal(t, wikiHome["new"], credGitTry(filepath.Join(nodeA, wikiKey), "rev-parse", "refs/heads/main"), "node A")
	assert.Equal(t, wikiHome["new"], credGitTry(filepath.Join(nodeB, wikiKey), "rev-parse", "refs/heads/main"), "node B syncs the wiki from the old address")
}

// The same with a password in the address: the serving node, which asks
// for the new wiki without it, writes an empty wiki remote, as without a
// cluster; a later sync, which asks with it, points that remote at the
// wiki instead of adding a second one. No node's git config holds the
// password.
func TestAPullMirrorWikiFollowsTheAddressWithItsPasswordKeptOut(t *testing.T) {
	inCluster(t)
	ctx := db.DefaultContext
	served, wikiHome := wikiUpstreams(t)
	srv := credentialedGitServer(t, served)
	base := strings.Replace(srv.URL, "http://", "http://u:s3cret-pw@", 1)
	user := strings.Replace(srv.URL, "http://", "http://u@", 1)

	repo := unittest.AssertExistsAndLoadBean(t, &repo_model.Repository{ID: 5})
	nodeA, nodeB, key, wikiKey := pullMirrorNodes(t, repo, base+"/old.git")
	for _, node := range []string{nodeA, nodeB} {
		onNode(node, func() { require.True(t, SyncPullMirror(ctx, repo.ID)) })
		require.Equal(t, wikiHome["old"], credGitTry(filepath.Join(node, wikiKey), "rev-parse", "refs/heads/main"))
	}

	changeAddress(t, repo, nodeA, base+"/new.git")
	for name, node := range map[string]string{"A": nodeA, "B": nodeB} {
		onNode(node, func() { assert.True(t, SyncPullMirror(ctx, repo.ID), "node %s syncs", name) })
		dir := filepath.Join(node, wikiKey)
		assert.Equal(t, user+"/new.wiki.git", credGitTry(dir, "config", "--get", "remote.origin.url"), "node %s's wiki remote", name)
		assert.Equal(t, wikiHome["new"], credGitTry(dir, "rev-parse", "refs/heads/main"), "node %s's wiki", name)
		for _, d := range []string{dir, filepath.Join(node, key)} {
			cfg, _ := configState(t, d)
			assert.NotContains(t, cfg, "s3cret-pw", "node %s keeps the password out of %s", name, d)
		}
	}
}

// A node whose git config already follows SQL rewrites nothing at a sync,
// and asks no remote for the wiki: the comparison costs a local read.
func TestAMirrorSyncRewritesNothingWhenItsConfigFollowsSQL(t *testing.T) {
	t.Run("push mirror", func(t *testing.T) {
		inCluster(t)
		ctx := db.DefaultContext
		served := t.TempDir()
		credGit(t, served, "init", "-q", "--bare", "target.git")
		srv := credentialedGitServer(t, served)
		repo := unittest.AssertExistsAndLoadBean(t, &repo_model.Repository{ID: 1})
		node := t.TempDir()
		key := filepath.Join(strings.ToLower(repo.OwnerName), strings.ToLower(repo.Name)+".git")
		require.NoError(t, unittest.CopyDir(filepath.Join(setting.RepoRootPath, "user2", "repo1.git"), filepath.Join(node, key)))
		pm := &repo_model.PushMirror{RepoID: repo.ID, Repo: repo, RemoteName: "remote_mirror_same", RemoteAddress: srv.URL + "/target.git", BranchFilter: "master, branch2"}
		require.NoError(t, db.Insert(ctx, pm))
		require.NoError(t, pm.SetEncryptedRemoteAddress(ctx, strings.Replace(srv.URL, "http://", "http://u:s3cret-pw@", 1)+"/target.git"))
		onNode(node, func() { require.True(t, SyncPushMirror(ctx, pm.ID)) })
		dir := filepath.Join(node, key)
		require.Equal(t, "+refs/heads/master:refs/heads/master\n+refs/heads/branch2:refs/heads/branch2\n+refs/tags/*:refs/tags/*", pushRefSpecsIn(dir, pm.RemoteName))
		before, at := configState(t, dir)
		time.Sleep(20 * time.Millisecond)
		onNode(node, func() { require.True(t, SyncPushMirror(ctx, pm.ID)) })
		after, then := configState(t, dir)
		assert.Equal(t, before, after)
		assert.Equal(t, at, then, "the git config was written again")
	})

	t.Run("pull mirror", func(t *testing.T) {
		inCluster(t)
		ctx := db.DefaultContext
		served, wikiHome := wikiUpstreams(t)
		base := openGitServer(t, served).URL
		repo := unittest.AssertExistsAndLoadBean(t, &repo_model.Repository{ID: 5})
		node, _, key, wikiKey := pullMirrorNodes(t, repo, base+"/old.git")
		onNode(node, func() { require.True(t, SyncPullMirror(ctx, repo.ID)) })
		require.Equal(t, wikiHome["old"], credGitTry(filepath.Join(node, wikiKey), "rev-parse", "refs/heads/main"))
		var asked atomic.Int32
		defer test.MockVariableValue(&wikiRemoteURL, func(ctx context.Context, remote string) string {
			asked.Add(1)
			return repo_module.WikiRemoteURL(ctx, remote)
		})()
		repoBefore, repoAt := configState(t, filepath.Join(node, key))
		wikiBefore, wikiAt := configState(t, filepath.Join(node, wikiKey))
		time.Sleep(20 * time.Millisecond)
		onNode(node, func() { require.True(t, SyncPullMirror(ctx, repo.ID)) })
		assert.Zero(t, asked.Load(), "the remote was asked for the wiki again")
		repoAfter, repoThen := configState(t, filepath.Join(node, key))
		wikiAfter, wikiThen := configState(t, filepath.Join(node, wikiKey))
		assert.Equal(t, repoBefore, repoAfter)
		assert.Equal(t, repoAt, repoThen, "the repository's git config was written again")
		assert.Equal(t, wikiBefore, wikiAfter)
		assert.Equal(t, wikiAt, wikiThen, "the wiki's git config was written again")
	})
}

// The push refspecs every node derives from a branch filter are the ones
// the remote was always created with.
func TestPushRefSpecsOfABranchFilter(t *testing.T) {
	tags := "+refs/tags/*:refs/tags/*"
	for filter, want := range map[string][]string{
		"":                {"+refs/heads/*:refs/heads/*", tags},
		"master":          {"+refs/heads/master:refs/heads/master", tags},
		" master , ,dev ": {"+refs/heads/master:refs/heads/master", "+refs/heads/dev:refs/heads/dev", tags},
		"release/*":       {"+refs/heads/release/*:refs/heads/release/*", tags},
		" , ":             {tags},
	} {
		assert.Equal(t, want, pushRefSpecs(filter), "filter %q", filter)
	}
}
