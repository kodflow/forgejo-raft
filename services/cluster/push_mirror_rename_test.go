// Copyright 2026 The Forgejo Authors. All rights reserved.
// SPDX-License-Identifier: GPL-3.0-or-later

package cluster

import (
	"context"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"forgejo.org/models/db"
	repo_model "forgejo.org/models/repo"
	"forgejo.org/models/unittest"
	user_model "forgejo.org/models/user"
	"forgejo.org/modules/clusterhook"
	repo_module "forgejo.org/modules/repository"
	"forgejo.org/modules/setting"
	"forgejo.org/modules/test"
	"forgejo.org/modules/timeutil"
	mirror_service "forgejo.org/services/mirror"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// The private repository and the name the mirrored repository is renamed
// to, while the private one takes its name.
const (
	pmSwapPayroll = "user27/payroll.git"
	pmSwapTmp     = "user27/template-tmp.git"
)

// pushMirrorSwap is a cluster whose node-a serves the push mirror of the
// public repository 44, user27/template1, to a remote outside the forge,
// beside a private repository, user27/payroll, whose refs were written
// without a repository ID, as a fork's, a migration's or a pull mirror's
// sync write them: its directory's stamp carries no ID, and the read guard
// judges it by the replicated table alone (#294).
type pushMirrorSwap struct {
	a, b   *testNode
	pub    string // the public repository's work tree
	t1, p1 string // the public and the private repository's master
	target string // the remote, a bare repository
	pm     *repo_model.PushMirror
}

// newPushMirrorSwap builds the cluster and checks that the bench works: a
// first sync from node-a pushes the public history.
func newPushMirrorSwap(t *testing.T) *pushMirrorSwap {
	t.Helper()
	require.NoError(t, unittest.PrepareTestDatabase())
	tc := newTestCluster(t, "node-a", "node-b", "node-c")
	for _, n := range tc.nodes {
		n.r.objects.repoKnown = n.r.repoKnown
	}
	raisePayload(t, tc)
	s := &pushMirrorSwap{a: tc.nodes[0], b: tc.nodes[1]}
	zero := strings.Repeat("0", 40)
	ctx := db.DefaultContext

	tc.createVia(s.b, pmSwapPayroll)
	priv := t.TempDir()
	run(t, priv, "init", "-q")
	s.p1 = commit(t, priv, "PRIVATE: payroll of the whole company")
	require.True(t, tplGenPushVia(t, s.b, priv, pmSwapPayroll, "refs/heads/master", zero, s.p1, 0).OK)

	tc.createVia(s.b, tplGenTemplate)
	s.pub = t.TempDir()
	run(t, s.pub, "init", "-q")
	s.t1 = commit(t, s.pub, "public content")
	require.True(t, tplGenPushVia(t, s.b, s.pub, tplGenTemplate, "refs/heads/master", zero, s.t1, tplGenTemplateID).OK)
	waitApplied(t, s.b, s.a)
	require.Eventually(t, func() bool {
		return gitRef(t, s.a, tplGenTemplate, "refs/heads/master") == s.t1 && gitRef(t, s.a, pmSwapPayroll, "refs/heads/master") == s.p1
	}, 15*time.Second, 20*time.Millisecond, "node-a serves both")
	settle(t, s.a)
	stamp, known := readStamp(filepath.Join(s.a.r.root, filepath.FromSlash(pmSwapPayroll)))
	require.True(t, known)
	require.Zero(t, stamp.ID, "the private repository's directory carries no repository ID")

	tplGenServe(t, s.a)

	// The remote is a local bare repository its owner may push to.
	t.Cleanup(test.MockVariableValue(&setting.ImportLocalPaths, true))
	owner := unittest.AssertExistsAndLoadBean(t, &user_model.User{ID: 27})
	owner.AllowImportLocal = true
	_, err := db.GetEngine(ctx).ID(owner.ID).Cols("allow_import_local").Update(owner)
	require.NoError(t, err)
	s.target = filepath.Join(t.TempDir(), "target.git")
	run(t, filepath.Dir(s.target), "init", "-q", "--bare", s.target)
	s.pm = &repo_model.PushMirror{RepoID: tplGenTemplateID, RemoteName: "remote_mirror_swap", RemoteAddress: s.target, Interval: time.Hour}
	require.NoError(t, db.Insert(ctx, s.pm))

	require.True(t, mirror_service.SyncPushMirror(ctx, s.pm.ID), "the push mirror syncs from node-a")
	require.Equal(t, s.t1, s.remoteMaster(t), "the remote holds the public history")
	return s
}

// renames renames each pair in turn through node-b, then waits until
// node-a projected them all: its git serves sha as key's master.
func (s *pushMirrorSwap) renames(t *testing.T, key, sha string, pairs ...[2]string) {
	t.Helper()
	for _, p := range pairs {
		renameOn(t, s.b, p[0], p[1])
	}
	waitApplied(t, s.b, s.a)
	require.Eventually(t, func() bool { return gitRef(t, s.a, key, "refs/heads/master") == sha }, 15*time.Second, 20*time.Millisecond, "node-a projects the renames")
	settle(t, s.a)
}

// swap renames the mirrored repository away and the private repository
// under its name, both projected on node-a.
func (s *pushMirrorSwap) swap(t *testing.T) {
	t.Helper()
	s.renames(t, tplGenTemplate, s.p1, [2]string{tplGenTemplate, pmSwapTmp}, [2]string{pmSwapPayroll, tplGenTemplate})
	stamp, _ := readStamp(filepath.Join(s.a.r.root, filepath.FromSlash(tplGenTemplate)))
	t.Logf("node-a: %s holds the private repository, stamp repo=%s id=%d", tplGenTemplate, stamp.Repo, stamp.ID)
}

func (s *pushMirrorSwap) remoteMaster(t *testing.T) string {
	t.Helper()
	out, _ := gitOut(t, s.target, "rev-parse", "refs/heads/master")
	return out
}

// due makes the mirror due for the update_mirrors cron: its interval
// elapsed since its last sync.
func (s *pushMirrorSwap) due(t *testing.T) timeutil.TimeStamp {
	t.Helper()
	at := timeutil.TimeStampNow().Add(-2 * 3600)
	_, err := db.GetEngine(db.DefaultContext).ID(s.pm.ID).Cols("last_update").Update(&repo_model.PushMirror{LastUpdateUnix: at})
	require.NoError(t, err)
	return at
}

// picked reports whether the update_mirrors cron picks the mirror now.
func (s *pushMirrorSwap) picked(t *testing.T) bool {
	t.Helper()
	found := false
	require.NoError(t, repo_model.PushMirrorsIterate(db.DefaultContext, 0, func(_ int, bean any) error {
		found = found || bean.(*repo_model.PushMirror).ID == s.pm.ID
		return nil
	}))
	return found
}

// refused checks a sync that had to be refused: nothing of the private
// repository reached the remote, nothing was written into the git config
// of the directory that holds it, and the mirror stays due, with the
// refusal as its last error, for the next round of update_mirrors.
func (s *pushMirrorSwap) refused(t *testing.T, ok bool, dueAt timeutil.TimeStamp, want, dir string) {
	t.Helper()
	pushed := s.remoteMaster(t)
	t.Logf("sync ok=%v; remote master=%.10s (public %.10s, private %.10s)", ok, pushed, want, s.p1)
	assert.NotEqual(t, s.p1, pushed, "the push mirror of the public repository pushed the private repository's history to its remote, --mirror --force")
	assert.Equal(t, want, pushed, "the remote keeps the public history")
	assert.False(t, ok, "the sync is refused")
	url, _ := gitOut(t, dir, "config", "--get", "remote."+s.pm.RemoteName+".url")
	assert.Empty(t, url, "the push mirror's remote was written into the private repository's git config")
	row := unittest.AssertExistsAndLoadBean(t, &repo_model.PushMirror{ID: s.pm.ID})
	t.Logf("last error: %s", row.LastError)
	assert.NotEmpty(t, row.LastError, "the refusal is the mirror's last error")
	assert.Equal(t, dueAt, row.LastUpdateUnix, "a refused sync is not written as done")
	assert.True(t, s.picked(t), "the next round of update_mirrors retries the refused sync")
	noCopyLeft(t)
}

// noCopyLeft checks that the syncs removed their staging copies.
func noCopyLeft(t *testing.T) {
	t.Helper()
	left, err := filepath.Glob(filepath.Join(repo_module.LocalCopyPath(), "push-mirror.git*"))
	require.NoError(t, err)
	assert.Empty(t, left, "a push mirror's sync left its staging copy behind")
}

// pushedAgain checks that a sync from node-a, as the next round of
// update_mirrors runs it, pushes want, the public repository's master.
func (s *pushMirrorSwap) pushedAgain(t *testing.T, want string) {
	t.Helper()
	require.True(t, mirror_service.SyncPushMirror(db.DefaultContext, s.pm.ID), "the retried sync pushes")
	assert.Equal(t, want, s.remoteMaster(t))
	row := unittest.AssertExistsAndLoadBean(t, &repo_model.PushMirror{ID: s.pm.ID})
	assert.Empty(t, row.LastError)
	assert.False(t, s.picked(t), "a sync done is not due again before its interval")
	noCopyLeft(t)
}

// A push mirror's sync loaded its repository once, at its start, and read
// the directory under that name past one check: ReadRefusal with the name
// and the ID it loaded. A directory whose stamp carries no ID is judged by
// the replicated table alone, which rightly holds, under the name, the
// repository a rename put there: the check passed, the remote was
// recreated from SQL in that repository's git config (#79), and git pushed
// it `--mirror --force` to the remote (#294, after #172). The sync now
// reads the directory once, into a copy outside the repository root,
// between two checks that it holds the repository the sync loaded, and
// pushes from the copy only; a refused sync stays due, and the next one
// pushes.
func TestAPushMirrorNeverPushesTheRepositoryARenamePutUnderItsName(t *testing.T) {
	s := newPushMirrorSwap(t)
	ctx := db.DefaultContext
	dir := filepath.Join(s.a.r.root, filepath.FromSlash(tplGenTemplate))

	// The mirrored repository is renamed away and the private repository
	// under its name, through node-b, and both renames are projected on
	// node-a right before the sync's first check of the directory; the
	// database is left as it was, so that only the checks of the directory
	// can tell. The renames of its key that node-a applied while the
	// directory was read refuse the sync.
	t.Run("renames projected while the directory is read", func(t *testing.T) {
		dueAt := s.due(t)
		var checks atomic.Int32
		clusterhook.SetReadGuard(func(key string, repoID int64) error {
			if key == tplGenTemplate && checks.Add(1) == 1 {
				s.swap(t)
			}
			err := s.a.r.ReadRefusal(key, repoID)
			t.Logf("check %d of %s for repository %d: %v", checks.Load(), key, repoID, err)
			return err
		})
		ok := mirror_service.SyncPushMirror(ctx, s.pm.ID)
		clusterhook.SetReadGuard(s.a.r.ReadRefusal)
		s.refused(t, ok, dueAt, s.t1, dir)

		// The renames undone, and a push to the public repository since:
		// the retried sync pushes it.
		s.renames(t, tplGenTemplate, s.t1, [2]string{tplGenTemplate, pmSwapPayroll}, [2]string{pmSwapTmp, tplGenTemplate})
		t2 := commit(t, s.pub, "public, later")
		require.True(t, tplGenPushVia(t, s.b, s.pub, tplGenTemplate, "refs/heads/master", s.t1, t2, tplGenTemplateID).OK)
		waitApplied(t, s.b, s.a)
		require.Eventually(t, func() bool { return gitRef(t, s.a, tplGenTemplate, "refs/heads/master") == t2 }, 15*time.Second, 20*time.Millisecond)
		s.pushedAgain(t, t2)
		s.t1 = t2
	})

	// The same renames as Forgejo makes them, each recorded in the
	// database once the cluster applied its move (changeRepositoryName),
	// projected on node-a after the sync loaded its repository, while it
	// waited to serve it fresh: the window covers the wait for another
	// node's hold too, some minutes. The directory under the loaded name
	// and the replicated table both show the private repository, its stamp
	// without an ID: only the mirrored repository's row, read again by its
	// ID, tells that the name is no longer its own. The next sync loads the
	// repository under its new name and pushes it from there.
	t.Run("renames the database recorded before the directory is read", func(t *testing.T) {
		dueAt := s.due(t)
		var waits atomic.Int32
		clusterhook.SetFreshWaiter(func(ctx context.Context, key string) bool {
			if key == tplGenTemplate && waits.Add(1) == 1 {
				s.swap(t)
				_, err := db.GetEngine(db.DefaultContext).ID(tplGenTemplateID).Cols("name", "lower_name").Update(&repo_model.Repository{Name: "template-tmp", LowerName: "template-tmp"})
				require.NoError(t, err)
			}
			return s.a.r.WaitFresh(ctx, key)
		})
		ok := mirror_service.SyncPushMirror(ctx, s.pm.ID)
		clusterhook.SetFreshWaiter(s.a.r.WaitFresh)
		require.Positive(t, waits.Load(), "the sync waits to serve its repository fresh")
		s.refused(t, ok, dueAt, s.t1, dir)

		t3 := commit(t, s.pub, "public, under its new name")
		require.True(t, tplGenPushVia(t, s.b, s.pub, pmSwapTmp, "refs/heads/master", s.t1, t3, tplGenTemplateID).OK)
		waitApplied(t, s.b, s.a)
		require.Eventually(t, func() bool { return gitRef(t, s.a, pmSwapTmp, "refs/heads/master") == t3 }, 15*time.Second, 20*time.Millisecond)
		s.pushedAgain(t, t3)
	})
}
