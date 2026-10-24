// Copyright 2026 The Forgejo Authors. All rights reserved.
// SPDX-License-Identifier: GPL-3.0-or-later

package mirror

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"forgejo.org/models/db"
	repo_model "forgejo.org/models/repo"
	"forgejo.org/models/unittest"
	"forgejo.org/modules/clusterhook"
	"forgejo.org/modules/setting"
	"forgejo.org/modules/test"
	"forgejo.org/modules/timeutil"
	migrations_allowlist "forgejo.org/services/migrations/allowlist"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// pushMirrorBench is a cluster of nodes, each a repository root of its
// own, and a remote behind a password that their push mirror targets.
type pushMirrorBench struct {
	served, base, bare string
	repo               *repo_model.Repository
	key, wikiKey       string
}

func newPushMirrorBench(t *testing.T, targets ...string) *pushMirrorBench {
	t.Helper()
	require.NoError(t, unittest.PrepareTestDatabase())
	was := clusterhook.Enabled()
	clusterhook.SetEnabled(true)
	t.Cleanup(func() { clusterhook.SetEnabled(was) })
	t.Cleanup(test.MockVariableValue(&setting.Migrations.AllowUnencrypted, true))
	t.Cleanup(test.MockVariableValue(&setting.Migrations.AllowedDomains, "127.0.0.1"))
	require.NoError(t, migrations_allowlist.Init())
	t.Cleanup(func() { _ = migrations_allowlist.Init() })
	t.Cleanup(func() {
		clusterhook.SetFreshWaiter(nil)
		clusterhook.SetReadGuard(nil)
	})

	b := &pushMirrorBench{served: t.TempDir()}
	for _, name := range append([]string{"target.git"}, targets...) {
		credGit(t, b.served, "init", "-q", "--bare", name)
	}
	srv := credentialedGitServer(t, b.served)
	b.base = strings.Replace(srv.URL, "http://", "http://u:s3cret-pw@", 1)
	b.bare = srv.URL
	b.repo = unittest.AssertExistsAndLoadBean(t, &repo_model.Repository{ID: 5})
	b.key = filepath.Join(strings.ToLower(b.repo.OwnerName), strings.ToLower(b.repo.Name)+".git")
	b.wikiKey = filepath.Join(strings.ToLower(b.repo.OwnerName), strings.ToLower(b.repo.Name)+".wiki.git")
	return b
}

// node returns a node's repository root holding the fixture repository.
func (b *pushMirrorBench) node(t *testing.T) string {
	t.Helper()
	node := t.TempDir()
	require.NoError(t, unittest.CopyDir(filepath.Join(setting.RepoRootPath, "user2", "repo1.git"), filepath.Join(node, b.key)))
	return node
}

// onNode runs f as the node whose repository root is node.
func onNode(node string, f func()) {
	defer test.MockVariableValue(&setting.RepoRootPath, node)()
	f()
}

// commitTo pushes a new commit on master, and a new branch at it, to the
// bare repository dir, as an acknowledged push a node projected, and
// returns it.
func commitTo(t *testing.T, dir, branch, content string) string {
	t.Helper()
	work := t.TempDir()
	credGit(t, work, "clone", "-q", dir, "w")
	wdir := filepath.Join(work, "w")
	require.NoError(t, os.WriteFile(filepath.Join(wdir, "new.txt"), []byte(content), 0o644))
	credGit(t, wdir, "add", "new.txt")
	credGit(t, wdir, "-c", "user.name=t", "-c", "user.email=t@example.com", "commit", "-q", "-m", content)
	credGit(t, wdir, "push", "-q", "origin", "HEAD:refs/heads/master", "HEAD:refs/heads/"+branch)
	return credGit(t, dir, "rev-parse", "refs/heads/master")
}

// initWiki makes dir a bare wiki holding one page with content.
func initWiki(t *testing.T, dir, content string) string {
	t.Helper()
	require.NoError(t, os.MkdirAll(filepath.Dir(dir), 0o755))
	credGit(t, filepath.Dir(dir), "init", "-q", "--bare", dir)
	page := t.TempDir()
	credGit(t, page, "init", "-q")
	require.NoError(t, os.WriteFile(filepath.Join(page, "Home.md"), []byte(content), 0o644))
	credGit(t, page, "add", "Home.md")
	credGit(t, page, "-c", "user.name=t", "-c", "user.email=t@example.com", "commit", "-q", "-m", content)
	credGit(t, page, "push", "-q", dir, "HEAD:refs/heads/main")
	return credGit(t, page, "rev-parse", "HEAD")
}

func pushMirrorRow(t *testing.T, id int64) *repo_model.PushMirror {
	t.Helper()
	return unittest.AssertExistsAndLoadBean(t, &repo_model.PushMirror{ID: id})
}

// setLastUpdate writes a push mirror's last_update as another node of the
// cluster does: a hold of its sync is the hold's expiry, in the future,
// and its end the sync's time with its error.
func setLastUpdate(t *testing.T, id int64, at timeutil.TimeStamp, lastError string) {
	t.Helper()
	_, err := db.GetEngine(db.DefaultContext).ID(id).Cols("last_update", "last_error").Update(&repo_model.PushMirror{LastUpdateUnix: at, LastError: lastError})
	require.NoError(t, err)
}

// A push mirror authenticated by a password pushes from every node since
// #146 (SSH ones since #79). A node whose projection of the repository is
// behind the cluster pushed `--mirror --force` from its own disk: the
// remote's branch was rewound and a branch created since was deleted there
// (#172). Its sync now waits for the node to be fresh, and fails without
// pushing when it is not, without writing last_update as a sync done: the
// next round of update_mirrors retries it.
func TestAPushMirrorDoesNotPushFromANodeBehind(t *testing.T) {
	b := newPushMirrorBench(t)
	ctx := db.DefaultContext
	nodeA, nodeB := b.node(t), b.node(t)
	old := credGit(t, filepath.Join(nodeB, b.key), "rev-parse", "refs/heads/master")
	// The acknowledged push, which node A projected and node B not yet: a
	// new commit on master and a new branch.
	fresh := commitTo(t, filepath.Join(nodeA, b.key), "fresh", "acknowledged")
	require.NotEqual(t, old, fresh)

	pm := &repo_model.PushMirror{RepoID: b.repo.ID, Repo: b.repo, RemoteName: "remote_mirror_behind", RemoteAddress: b.bare + "/target.git"}
	require.NoError(t, db.Insert(ctx, pm))
	onNode(nodeA, func() {
		require.NoError(t, AddPushMirrorRemote(ctx, pm, b.base+"/target.git"))
		require.True(t, SyncPushMirror(ctx, pm.ID), "the node that holds the push pushes it")
	})
	target := filepath.Join(b.served, "target.git")
	require.Equal(t, fresh, credGitTry(target, "rev-parse", "refs/heads/master"))
	require.Equal(t, fresh, credGitTry(target, "rev-parse", "refs/heads/fresh"))
	synced := pushMirrorRow(t, pm.ID)
	require.Empty(t, synced.LastError)

	// Node B says it may be behind for every repository; the
	// update_mirrors cron of node B (every node runs it) picks the mirror.
	var asked atomic.Int32
	clusterhook.SetFreshWaiter(func(context.Context, string) bool { asked.Add(1); return false })
	onNode(nodeB, func() {
		assert.False(t, SyncPushMirror(ctx, pm.ID), "a node behind fails the sync")
	})
	assert.Positive(t, asked.Load(), "the push mirror's sync asks whether the node is fresh")
	assert.Equal(t, fresh, credGitTry(target, "rev-parse", "refs/heads/master"), "the remote's master was rewound to node B's stale %s", old)
	assert.Equal(t, fresh, credGitTry(target, "rev-parse", "refs/heads/fresh"), "the branch the acknowledged push created was deleted from the remote")
	after := pushMirrorRow(t, pm.ID)
	assert.NotEmpty(t, after.LastError, "the refusal is the mirror's last error")
	assert.Equal(t, synced.LastUpdateUnix, after.LastUpdateUnix, "a refused sync is not written as done")
}

// The same sync never asked the read guard of #82 either: on a node whose
// directory under the key still holds another repository (renamed away or
// deleted, then the name taken again, not projected there yet), the guard
// refuses every read, and the push mirror pushed that other repository's
// history to the new repository's remote (#172).
func TestAPushMirrorDoesNotPushAnotherRepositorysHistory(t *testing.T) {
	b := newPushMirrorBench(t)
	ctx := db.DefaultContext
	nodeC := t.TempDir()
	// Under the key on node C: the previous repository of the name, with a
	// private commit.
	work := t.TempDir()
	credGit(t, work, "init", "-q", "w")
	wdir := filepath.Join(work, "w")
	require.NoError(t, os.WriteFile(filepath.Join(wdir, "secret.txt"), []byte("private history"), 0o644))
	credGit(t, wdir, "add", "secret.txt")
	credGit(t, wdir, "-c", "user.name=t", "-c", "user.email=t@example.com", "commit", "-q", "-m", "private")
	secret := credGit(t, wdir, "rev-parse", "HEAD")
	require.NoError(t, os.MkdirAll(filepath.Join(nodeC, filepath.Dir(b.key)), 0o755))
	credGit(t, nodeC, "init", "-q", "--bare", b.key)
	credGit(t, wdir, "push", "-q", filepath.Join(nodeC, b.key), "HEAD:refs/heads/master")

	var asked atomic.Int32
	clusterhook.SetReadGuard(func(string, int64) error {
		asked.Add(1)
		return assert.AnError
	})

	pm := &repo_model.PushMirror{RepoID: b.repo.ID, Repo: b.repo, RemoteName: "remote_mirror_other", RemoteAddress: b.bare + "/target.git"}
	require.NoError(t, db.Insert(ctx, pm))
	require.NoError(t, pm.SetEncryptedRemoteAddress(ctx, b.base+"/target.git"))
	onNode(nodeC, func() {
		assert.False(t, SyncPushMirror(ctx, pm.ID), "a directory holding another repository is never pushed")
	})
	assert.Positive(t, asked.Load(), "the push mirror's sync asks the read guard")
	assert.NotEqual(t, secret, credGitTry(filepath.Join(b.served, "target.git"), "rev-parse", "refs/heads/master"), "the other repository's private history reached the remote")
	after := pushMirrorRow(t, pm.ID)
	assert.NotEmpty(t, after.LastError)
	assert.Zero(t, after.LastUpdateUnix, "a refused sync is not written as done")
	// Nothing under the key was touched: no remote was added to the other
	// repository's config.
	assert.Empty(t, credGitTry(filepath.Join(nodeC, b.key), "config", "--get", "remote."+pm.RemoteName+".url"))
}

// The wiki is a repository of its own in the table, with its own
// projection: its push asks for its own key. A node fresh for the
// repository and behind for its wiki pushes the repository and not the
// wiki, whose remote keeps the newer pages.
func TestAPushMirrorDoesNotPushAWikiFromANodeBehind(t *testing.T) {
	b := newPushMirrorBench(t, "target.wiki.git")
	ctx := db.DefaultContext
	nodeA, nodeB := b.node(t), b.node(t)
	initWiki(t, filepath.Join(nodeB, b.wikiKey), "old page")
	newer := initWiki(t, filepath.Join(nodeA, b.wikiKey), "newer page")

	pm := &repo_model.PushMirror{RepoID: b.repo.ID, Repo: b.repo, RemoteName: "remote_mirror_wiki", RemoteAddress: b.bare + "/target.git"}
	require.NoError(t, db.Insert(ctx, pm))
	onNode(nodeA, func() {
		require.NoError(t, AddPushMirrorRemote(ctx, pm, b.base+"/target.git"))
		require.True(t, SyncPushMirror(ctx, pm.ID))
	})
	wikiTarget := filepath.Join(b.served, "target.wiki.git")
	require.Equal(t, newer, credGitTry(wikiTarget, "rev-parse", "refs/heads/main"))
	synced := pushMirrorRow(t, pm.ID)

	// Node B projected a push to the repository that node A did not yet,
	// and is behind for the wiki.
	ahead := commitTo(t, filepath.Join(nodeB, b.key), "ahead", "acknowledged")
	var mu sync.Mutex
	var asked []string
	clusterhook.SetFreshWaiter(func(_ context.Context, key string) bool {
		mu.Lock()
		defer mu.Unlock()
		asked = append(asked, key)
		return !strings.HasSuffix(key, ".wiki.git")
	})
	onNode(nodeB, func() {
		assert.False(t, SyncPushMirror(ctx, pm.ID), "a wiki behind fails the sync")
	})
	assert.Contains(t, asked, filepath.ToSlash(b.key), "the repository's push asks for the repository")
	assert.Contains(t, asked, filepath.ToSlash(b.wikiKey), "the wiki's push asks for the wiki")
	assert.Equal(t, newer, credGitTry(wikiTarget, "rev-parse", "refs/heads/main"), "the wiki's remote was rewound to node B's old page")
	assert.Equal(t, ahead, credGitTry(filepath.Join(b.served, "target.git"), "rev-parse", "refs/heads/master"), "the repository, fresh, is pushed")
	after := pushMirrorRow(t, pm.ID)
	assert.NotEmpty(t, after.LastError)
	assert.Equal(t, synced.LastUpdateUnix, after.LastUpdateUnix, "a refused sync is not written as done")
}

// Two fresh nodes that push the same mirror at once finished in any
// order: the node whose fresh wait came first, slower, overwrote the
// newer state the other pushed, until the next sync (#172). A node of a
// cluster holds a push mirror's sync in SQL, its fresh waits and pushes
// included: another node's sync waits for the hold to end, then waits for
// its own fresh state, so every push starts from a state at least as
// recent as the previous one.
func TestAPushMirrorSyncWaitsForAnotherNodesHold(t *testing.T) {
	b := newPushMirrorBench(t)
	ctx := db.DefaultContext
	nodeA, nodeB := b.node(t), b.node(t)
	older := credGit(t, filepath.Join(nodeA, b.key), "rev-parse", "refs/heads/master")
	newer := commitTo(t, filepath.Join(nodeB, b.key), "newer", "acknowledged")
	target := filepath.Join(b.served, "target.git")

	pm := &repo_model.PushMirror{RepoID: b.repo.ID, Repo: b.repo, RemoteName: "remote_mirror_hold", RemoteAddress: b.bare + "/target.git"}
	require.NoError(t, db.Insert(ctx, pm))
	require.NoError(t, pm.SetEncryptedRemoteAddress(ctx, b.base+"/target.git"))
	var asked atomic.Int32
	clusterhook.SetFreshWaiter(func(context.Context, string) bool { asked.Add(1); return true })

	syncOnB := func() <-chan bool {
		done := make(chan bool, 1)
		go func() {
			var ok bool
			onNode(nodeB, func() { ok = SyncPushMirror(ctx, pm.ID) })
			// Once node B's repository root is put back.
			done <- ok
		}()
		return done
	}
	// waitFor reads the outcome of node B's sync once it is done, if it
	// is not yet.
	waitFor := func(done <-chan bool, got, finished *bool) {
		t.Helper()
		if *finished {
			return
		}
		select {
		case *got = <-done:
			*finished = true
		case <-time.After(time.Minute):
			require.FailNow(t, "node B's sync never ended")
		}
	}
	quiet := func(done <-chan bool, got, finished *bool) {
		t.Helper()
		select {
		case *got = <-done:
			*finished = true
			assert.Fail(t, "node B synced while another node held the sync")
		case <-time.After(1500 * time.Millisecond):
		}
	}

	t.Run("waits for the hold to end", func(t *testing.T) {
		// Node A holds the sync: its fresh wait is done, its push of its
		// older state is under way.
		setLastUpdate(t, pm.ID, timeutil.TimeStampNow().Add(600), "")
		asked.Store(0)
		done := syncOnB()
		var got, finished bool
		quiet(done, &got, &finished)
		assert.Empty(t, credGitTry(target, "rev-parse", "--verify", "-q", "refs/heads/newer"), "node B pushed while node A held the sync")
		// Node A's push lands, then its hold ends.
		credGit(t, filepath.Join(nodeA, b.key), "push", "-q", "--mirror", "--force", b.base+"/target.git")
		setLastUpdate(t, pm.ID, timeutil.TimeStampNow(), "")
		waitFor(done, &got, &finished)
		assert.True(t, got)
		assert.Positive(t, asked.Load(), "node B waited for its own fresh state once it held the sync")
		assert.Equal(t, newer, credGitTry(target, "rev-parse", "refs/heads/master"), "node A's older %s, pushed last, overwrote node B's newer state", older)
		assert.Empty(t, pushMirrorRow(t, pm.ID).LastError)
	})

	t.Run("a later hold carries it", func(t *testing.T) {
		credGit(t, filepath.Join(nodeA, b.key), "push", "-q", "--mirror", "--force", b.base+"/target.git")
		setLastUpdate(t, pm.ID, timeutil.TimeStampNow().Add(600), "")
		asked.Store(0)
		done := syncOnB()
		var got, finished bool
		quiet(done, &got, &finished)
		// Node C takes the sync over after node B was asked for it: its
		// fresh wait comes after, and its push carries what node B holds.
		setLastUpdate(t, pm.ID, timeutil.TimeStampNow().Add(900), "")
		quiet(done, &got, &finished)
		quiet(done, &got, &finished)
		credGit(t, filepath.Join(nodeB, b.key), "push", "-q", "--mirror", "--force", b.base+"/target.git")
		setLastUpdate(t, pm.ID, timeutil.TimeStampNow(), "")
		waitFor(done, &got, &finished)
		assert.True(t, got, "the later sync succeeded")
		assert.Zero(t, asked.Load(), "node B relies on the later sync instead of pushing again")
		assert.Equal(t, newer, credGitTry(target, "rev-parse", "refs/heads/master"))
	})

	t.Run("a later hold that failed does not carry it", func(t *testing.T) {
		credGit(t, filepath.Join(nodeA, b.key), "push", "-q", "--mirror", "--force", b.base+"/target.git")
		before := timeutil.TimeStampNow().Add(-3600)
		setLastUpdate(t, pm.ID, timeutil.TimeStampNow().Add(600), "")
		asked.Store(0)
		done := syncOnB()
		var got, finished bool
		quiet(done, &got, &finished)
		// Node C takes the sync over, is refused, and puts last_update
		// back: node B pushes itself.
		setLastUpdate(t, pm.ID, timeutil.TimeStampNow().Add(900), "")
		quiet(done, &got, &finished)
		setLastUpdate(t, pm.ID, before, "this node of the cluster may be behind")
		waitFor(done, &got, &finished)
		assert.True(t, got)
		assert.Positive(t, asked.Load())
		assert.Equal(t, newer, credGitTry(target, "rev-parse", "refs/heads/master"))
		assert.Empty(t, pushMirrorRow(t, pm.ID).LastError)
	})

	t.Run("an expired hold is taken over", func(t *testing.T) {
		credGit(t, filepath.Join(nodeA, b.key), "push", "-q", "--mirror", "--force", b.base+"/target.git")
		// The node that held it stopped: its hold expired.
		setLastUpdate(t, pm.ID, timeutil.TimeStampNow().Add(-1), "")
		var got, finished bool
		waitFor(syncOnB(), &got, &finished)
		assert.True(t, got)
		assert.Equal(t, newer, credGitTry(target, "rev-parse", "refs/heads/master"))
	})
}
