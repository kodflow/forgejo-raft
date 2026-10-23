// Copyright 2026 The Forgejo Authors. All rights reserved.
// SPDX-License-Identifier: GPL-3.0-or-later

package mirror

import (
	"net/http"
	"net/http/cgi"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"forgejo.org/models/db"
	repo_model "forgejo.org/models/repo"
	"forgejo.org/models/unittest"
	"forgejo.org/modules/clusterhook"
	"forgejo.org/modules/setting"
	"forgejo.org/modules/test"
	migrations_allowlist "forgejo.org/services/migrations/allowlist"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// credentialedGitServer serves the bare repositories under root over HTTP,
// to the user u with the password pw only.
func credentialedGitServer(t *testing.T, root string) *httptest.Server {
	t.Helper()
	gitPath, err := exec.LookPath("git")
	require.NoError(t, err)
	backend := &cgi.Handler{
		Path: gitPath,
		Args: []string{"http-backend"},
		Env:  []string{"GIT_PROJECT_ROOT=" + root, "GIT_HTTP_EXPORT_ALL=1", "REMOTE_USER=u", "HOME=" + t.TempDir()},
	}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if u, pw, ok := r.BasicAuth(); !ok || u != "u" || pw != "s3cret-pw" {
			w.Header().Set("WWW-Authenticate", `Basic realm="test"`)
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		backend.ServeHTTP(w, r)
	}))
	t.Cleanup(srv.Close)
	return srv
}

func credGit(t *testing.T, dir string, args ...string) string {
	t.Helper()
	cmd := exec.Command("git", append([]string{"-c", "core.hooksPath=/dev/null"}, args...)...)
	cmd.Dir = dir
	cmd.Env = append(os.Environ(), "HOME="+setting.Git.HomePath)
	out, err := cmd.CombinedOutput()
	require.NoError(t, err, string(out))
	return strings.TrimSpace(string(out))
}

func credGitTry(dir string, args ...string) string {
	cmd := exec.Command("git", append([]string{"-c", "core.hooksPath=/dev/null"}, args...)...)
	cmd.Dir = dir
	cmd.Env = append(os.Environ(), "HOME="+setting.Git.HomePath)
	out, _ := cmd.Output()
	return strings.TrimSpace(string(out))
}

// A mirror's remote is a node's git config, which a cluster does not
// replicate: a node whose disk is new holds the repository's refs and no
// remote. It recreates the remote from SQL, without the password, which
// it passes through a credential helper. A push mirror authenticated by a
// password in its address pushes from every node, the wiki too, and a
// push mirror from before the encrypted address was kept stores it from
// the node that holds the remote, then pushes from every node (#79).
func TestMirrorCredentialsOnAFreshNode(t *testing.T) {
	require.NoError(t, unittest.PrepareTestDatabase())
	ctx := db.DefaultContext
	was := clusterhook.Enabled()
	clusterhook.SetEnabled(true)
	defer clusterhook.SetEnabled(was)
	defer test.MockVariableValue(&setting.Migrations.AllowUnencrypted, true)()
	defer test.MockVariableValue(&setting.Migrations.AllowedDomains, "127.0.0.1")()
	require.NoError(t, migrations_allowlist.Init())
	defer func() { _ = migrations_allowlist.Init() }()

	// The remote side, behind a password: the upstream of the pull mirror,
	// with its wiki, and the targets of two push mirrors.
	served := t.TempDir()
	fixture := filepath.Join(setting.RepoRootPath, "user2", "repo1.git")
	require.NoError(t, unittest.CopyDir(fixture, filepath.Join(served, "upstream.git")))
	for _, name := range []string{"upstream.wiki.git", "target.git", "target.wiki.git", "legacy.git"} {
		credGit(t, served, "init", "-q", "--bare", name)
	}
	page := t.TempDir()
	credGit(t, page, "init", "-q")
	require.NoError(t, os.WriteFile(filepath.Join(page, "Home.md"), []byte("Home"), 0o644))
	credGit(t, page, "add", "Home.md")
	credGit(t, page, "-c", "user.name=t", "-c", "user.email=t@example.com", "commit", "-q", "-m", "Home")
	home := credGit(t, page, "rev-parse", "HEAD")
	credGit(t, page, "push", "-q", filepath.Join(served, "upstream.wiki.git"), "HEAD:refs/heads/main")
	srv := credentialedGitServer(t, served)
	base := strings.Replace(srv.URL, "http://", "http://u:s3cret-pw@", 1)
	bare := srv.URL

	// Two nodes. The first created the mirrors and holds the refs; the
	// second's disk is new: the same repositories, without any remote.
	repo := unittest.AssertExistsAndLoadBean(t, &repo_model.Repository{ID: 5})
	nodeA, nodeB := t.TempDir(), t.TempDir()
	key := filepath.Join(strings.ToLower(repo.OwnerName), strings.ToLower(repo.Name)+".git")
	wikiKey := filepath.Join(strings.ToLower(repo.OwnerName), strings.ToLower(repo.Name)+".wiki.git")
	require.NoError(t, unittest.CopyDir(fixture, filepath.Join(nodeA, key)))
	for _, dir := range []string{filepath.Join(nodeA, wikiKey), filepath.Join(nodeB, key), filepath.Join(nodeB, wikiKey)} {
		require.NoError(t, os.MkdirAll(filepath.Dir(dir), 0o755))
		credGit(t, filepath.Dir(dir), "init", "-q", "--bare", dir)
	}
	on := func(node string, f func()) {
		defer test.MockVariableValue(&setting.RepoRootPath, node)()
		f()
	}
	configOf := func(dir string) string {
		b, err := os.ReadFile(filepath.Join(dir, "config"))
		require.NoError(t, err)
		return string(b)
	}
	keepsNoPassword := func(node string) {
		t.Helper()
		assert.NotContains(t, configOf(filepath.Join(node, key)), "s3cret-pw")
		assert.NotContains(t, configOf(filepath.Join(node, wikiKey)), "s3cret-pw")
	}

	// A pull mirror whose address carries a password syncs on the new disk.
	_, err := db.DeleteByBean(ctx, &repo_model.Mirror{RepoID: repo.ID})
	require.NoError(t, err)
	require.NoError(t, (&repo_model.Mirror{RepoID: repo.ID, Interval: setting.Mirror.DefaultInterval, EnablePrune: true}).InsertWithAddress(ctx, base+"/upstream.git"))
	on(nodeB, func() {
		assert.True(t, SyncPullMirror(ctx, repo.ID), "the pull mirror syncs on the new disk")
	})
	want := credGit(t, filepath.Join(served, "upstream.git"), "rev-parse", "refs/heads/master")
	assert.Equal(t, want, credGitTry(filepath.Join(nodeB, key), "rev-parse", "refs/heads/master"))
	assert.Equal(t, home, credGitTry(filepath.Join(nodeB, wikiKey), "rev-parse", "refs/heads/main"), "the wiki syncs too")
	keepsNoPassword(nodeB)

	// A push mirror whose address carries a password, created on the first
	// node, pushes from the new disk, its wiki too.
	pm := &repo_model.PushMirror{RepoID: repo.ID, Repo: repo, RemoteName: "remote_mirror_cred", RemoteAddress: bare + "/target.git"}
	require.NoError(t, db.Insert(ctx, pm))
	on(nodeA, func() {
		require.NoError(t, AddPushMirrorRemote(ctx, pm, base+"/target.git"))
	})
	on(nodeB, func() {
		assert.True(t, SyncPushMirror(ctx, pm.ID), "the push mirror pushes from the new disk")
	})
	assert.Equal(t, want, credGitTry(filepath.Join(served, "target.git"), "rev-parse", "refs/heads/master"))
	assert.Equal(t, home, credGitTry(filepath.Join(served, "target.wiki.git"), "rev-parse", "refs/heads/main"), "the wiki is pushed too")
	assert.Equal(t, bare+"/target.git", credGitTry(filepath.Join(nodeB, key), "config", "--get", "remote."+pm.RemoteName+".url"))
	keepsNoPassword(nodeB)

	// A push mirror from before its address was kept encrypted: only the
	// node that created it holds the password, in its git config. Its sync
	// there stores the address, and the new disk pushes.
	legacy := &repo_model.PushMirror{RepoID: repo.ID, Repo: repo, RemoteName: "remote_mirror_legacy", RemoteAddress: bare + "/legacy.git"}
	require.NoError(t, db.Insert(ctx, legacy))
	on(nodeA, func() {
		require.NoError(t, addPushRemoteAndConfig(ctx, legacy, base+"/legacy.git", filepath.Join(nodeA, key)))
	})
	on(nodeB, func() {
		assert.False(t, SyncPushMirror(ctx, legacy.ID), "no password is known to the new disk yet")
	})
	on(nodeA, func() {
		assert.True(t, SyncPushMirror(ctx, legacy.ID), "the creating node pushes")
	})
	stored := unittest.AssertExistsAndLoadBean(t, &repo_model.PushMirror{ID: legacy.ID})
	assert.NotEmpty(t, stored.EncryptedRemoteAddress, "the creating node stored the address")
	credGit(t, filepath.Join(served, "legacy.git"), "update-ref", "-d", "refs/heads/master")
	on(nodeB, func() {
		assert.True(t, SyncPushMirror(ctx, legacy.ID), "the new disk pushes once the address is stored")
	})
	assert.Equal(t, want, credGitTry(filepath.Join(served, "legacy.git"), "rev-parse", "refs/heads/master"))
	keepsNoPassword(nodeB)
}
