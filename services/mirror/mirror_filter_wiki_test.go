// Copyright 2026 The Forgejo Authors. All rights reserved.
// SPDX-License-Identifier: GPL-3.0-or-later

package mirror

import (
	gocontext "context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"forgejo.org/models/db"
	repo_model "forgejo.org/models/repo"
	"forgejo.org/models/unittest"
	"forgejo.org/modules/clusterhook"
	"forgejo.org/modules/setting"
	"forgejo.org/modules/timeutil"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// A push mirror created through node A; node B never synced it, so its git
// config has no remote for it (#79). The owner changed the branch filter
// through node B, as routers/web/repo/setting/setting.go does, SQL and the
// git config in one transaction: `git config --unset-all` of the missing
// push refspecs exited 5, which was not taken for "nothing to remove", and
// the change was refused with a 500 (#201). Node B now writes SQL only,
// and no push refspec without the remote: its next sync recreates the
// remote with the new filter.
func TestABranchFilterChangeThroughANodeWithoutTheRemote(t *testing.T) {
	inCluster(t)
	ctx := db.DefaultContext

	served := t.TempDir()
	credGit(t, served, "init", "-q", "--bare", "target.git")
	target := filepath.Join(served, "target.git")
	srv := credentialedGitServer(t, served)
	base := strings.Replace(srv.URL, "http://", "http://u:s3cret-pw@", 1)

	repo := unittest.AssertExistsAndLoadBean(t, &repo_model.Repository{ID: 1})
	fixture := filepath.Join(setting.RepoRootPath, "user2", "repo1.git")
	nodeA, nodeB := t.TempDir(), t.TempDir()
	key := filepath.Join(strings.ToLower(repo.OwnerName), strings.ToLower(repo.Name)+".git")
	for _, node := range []string{nodeA, nodeB} {
		require.NoError(t, os.MkdirAll(filepath.Dir(filepath.Join(node, key)), 0o755))
		require.NoError(t, unittest.CopyDir(fixture, filepath.Join(node, key)))
	}

	pm := &repo_model.PushMirror{RepoID: repo.ID, Repo: repo, RemoteName: "remote_mirror_nodeb", RemoteAddress: srv.URL + "/target.git"}
	require.NoError(t, db.Insert(ctx, pm))
	onNode(nodeA, func() { require.NoError(t, AddPushMirrorRemote(ctx, pm, base+"/target.git")) })

	pm.BranchFilter = "master"
	var err error
	onNode(nodeB, func() {
		err = db.WithTx(ctx, func(ctx gocontext.Context) error {
			if err := repo_model.UpdatePushMirrorBranchFilter(ctx, pm); err != nil {
				return err
			}
			return UpdatePushMirrorBranchFilter(ctx, pm)
		})
	})
	require.NoError(t, err, "the filter change through a node without the remote is refused")
	assert.Equal(t, "master", unittest.AssertExistsAndLoadBean(t, &repo_model.PushMirror{ID: pm.ID}).BranchFilter)
	dirB := filepath.Join(nodeB, key)
	cfg, _ := configState(t, dirB)
	assert.NotContains(t, cfg, pm.RemoteName, "node B wrote a remote section without the remote")

	// Node B's next sync recreates the remote, with the filter SQL holds.
	onNode(nodeB, func() { require.True(t, SyncPushMirror(ctx, pm.ID), "node B pushes, its remote recreated from SQL") })
	assert.Equal(t, srv.URL+"/target.git", credGitTry(dirB, "config", "--get", "remote."+pm.RemoteName+".url"))
	assert.Equal(t, "+refs/heads/master:refs/heads/master\n+refs/tags/*:refs/tags/*", pushRefSpecsIn(dirB, pm.RemoteName))
	assert.NotEmpty(t, credGitTry(target, "rev-parse", "--verify", "-q", "refs/heads/master"))
	assert.Empty(t, credGitTry(target, "rev-parse", "--verify", "-q", "refs/heads/branch2"), "the filter took branch2 out")

	// Through a node that holds the remote, the change still reaches its git
	// config at once.
	pm.BranchFilter = "master,branch2"
	onNode(nodeA, func() {
		require.NoError(t, db.WithTx(ctx, func(ctx gocontext.Context) error {
			if err := repo_model.UpdatePushMirrorBranchFilter(ctx, pm); err != nil {
				return err
			}
			return UpdatePushMirrorBranchFilter(ctx, pm)
		}))
	})
	assert.Equal(t, "+refs/heads/master:refs/heads/master\n+refs/heads/branch2:refs/heads/branch2\n+refs/tags/*:refs/tags/*", pushRefSpecsIn(filepath.Join(nodeA, key), pm.RemoteName))
}

// A pull mirror whose wiki the old address served, pointed through node A
// at an address that serves none: every node's wiki remote follows SQL
// (#183) and gets the empty URL UpdateAddress writes, and the wiki's update
// with it failed the whole sync, on every node, at every sync: its results
// were dropped and next_update_unix never moved (#201). A wiki without a
// remote to update from is skipped, as runPushSync skips one.
func TestAPullMirrorMovedToAnAddressWithoutAWiki(t *testing.T) {
	inCluster(t)
	ctx := db.DefaultContext
	served, wikiHome := wikiUpstreams(t)
	fixture := filepath.Join(setting.RepoRootPath, "user2", "repo1.git")
	require.NoError(t, unittest.CopyDir(fixture, filepath.Join(served, "nowiki.git")))
	base := openGitServer(t, served).URL

	repo := unittest.AssertExistsAndLoadBean(t, &repo_model.Repository{ID: 5})
	nodeA, nodeB, key, wikiKey := pullMirrorNodes(t, repo, base+"/old.git")
	for _, node := range []string{nodeA, nodeB} {
		onNode(node, func() { require.True(t, SyncPullMirror(ctx, repo.ID)) })
	}

	changeAddress(t, repo, nodeA, base+"/nowiki.git")
	// The mirror is due.
	_, err := db.GetEngine(ctx).Where("repo_id = ?", repo.ID).Cols("next_update_unix").Update(&repo_model.Mirror{NextUpdateUnix: timeutil.TimeStampNow().Add(-1)})
	require.NoError(t, err)
	for name, node := range map[string]string{"A": nodeA, "B": nodeB} {
		var ok bool
		onNode(node, func() { ok = SyncPullMirror(ctx, repo.ID) })
		assert.True(t, ok, "node %s syncs the mirror", name)
		assert.Equal(t, base+"/nowiki.git", credGitTry(filepath.Join(node, key), "config", "--get", "remote.origin.url"))
		assert.Empty(t, credGitTry(filepath.Join(node, wikiKey), "config", "--get", "remote.origin.url"), "node %s never fetches the wiki from the old address", name)
		assert.Equal(t, wikiHome["old"], credGitTry(filepath.Join(node, wikiKey), "rev-parse", "refs/heads/main"), "node %s keeps its wiki as it was", name)
	}
	m, err := repo_model.GetMirrorByRepoID(ctx, repo.ID)
	require.NoError(t, err)
	assert.Greater(t, m.NextUpdateUnix, timeutil.TimeStampNow(), "the next sync is scheduled, not due at every cron tick")
}

// Without a cluster, the node that serves the change writes the same empty
// URL, and every later sync failed the same way: the skip holds there too.
func TestAPullMirrorMovedToAnAddressWithoutAWikiOutsideACluster(t *testing.T) {
	inCluster(t)
	clusterhook.SetEnabled(false)
	ctx := db.DefaultContext
	served, _ := wikiUpstreams(t)
	fixture := filepath.Join(setting.RepoRootPath, "user2", "repo1.git")
	require.NoError(t, unittest.CopyDir(fixture, filepath.Join(served, "nowiki.git")))
	base := openGitServer(t, served).URL

	repo := unittest.AssertExistsAndLoadBean(t, &repo_model.Repository{ID: 5})
	node, _, key, wikiKey := pullMirrorNodes(t, repo, base+"/old.git")
	// The remotes the migration writes, without a cluster to recreate them.
	onNode(node, func() {
		for _, r := range []struct{ dir, addr string }{
			{filepath.Join(node, key), base + "/old.git"},
			{filepath.Join(node, wikiKey), base + "/old.wiki.git"},
		} {
			require.NoError(t, addPullRemote(ctx, r.dir, "origin", r.addr))
		}
		require.True(t, SyncPullMirror(ctx, repo.ID))
	})

	changeAddress(t, repo, node, base+"/nowiki.git")
	onNode(node, func() { assert.True(t, SyncPullMirror(ctx, repo.ID), "the mirror syncs") })
	assert.Empty(t, credGitTry(filepath.Join(node, wikiKey), "config", "--get", "remote.origin.url"))
}
