// Copyright 2026 The Forgejo Authors. All rights reserved.
// SPDX-License-Identifier: GPL-3.0-or-later

package cluster

import (
	"fmt"
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
	user_model "forgejo.org/models/user"
	"forgejo.org/modules/clusterhook"
	"forgejo.org/modules/git"
	"forgejo.org/modules/setting"
	"forgejo.org/modules/test"
	"forgejo.org/modules/timeutil"
	mirror_service "forgejo.org/services/mirror"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// The pull mirror, the name it is renamed to while another repository
// takes its name, and the repositories a rename puts under its name: the
// private repository 3, its refs pushed with its ID, and a repository
// whose refs were written without one, as a fork's, a migration's or a
// mirror's sync wrote them.
const (
	pmPullMirror   = "org3/repo5.git"
	pmPullMirrorID = int64(5)
	pmPullTmp      = "org3/repo5-old.git"
	pmPullPrivate  = "org3/repo3.git"
	pmPullPrivID   = int64(3)
	pmPullNoID     = "org3/scratch.git"
)

// pullMirrorSwap is a cluster whose node-a syncs the pull mirror 5,
// org3/repo5, with --prune, from an upstream outside the forge, beside
// the repositories a rename puts under its name. git is a wrapper that
// can hold the sync's first git command in the mirror's directory while
// the renames are projected, as the tests of #283 and #294 do.
type pullMirrorSwap struct {
	a, b, c  *testNode
	gate     string            // the wrapper's files, a TempDir
	up       string            // the upstream's work tree
	upstream string            // the upstream, a bare repository
	master   string            // the upstream's master
	syncs    int               // the upstream's commits since
	victims  map[string]string // each victim's master, and its salaries branch under "<key> salaries"
}

// newPullMirrorSwap builds the cluster and checks that the bench works: a
// first sync from node-a fetches the upstream into the mirror.
func newPullMirrorSwap(t *testing.T) *pullMirrorSwap {
	t.Helper()
	require.NoError(t, unittest.PrepareTestDatabase())

	// The wrapper of git, set before the bench starts and restored after
	// it stopped. It only reads and writes files under gate, a TempDir:
	// armed names the directory whose `git config --get
	// http.followRedirects`, the sync's first git command there, waits for
	// go.
	s := &pullMirrorSwap{gate: t.TempDir(), victims: map[string]string{}}
	realGit, err := exec.LookPath("git")
	require.NoError(t, err)
	wrapper := filepath.Join(s.gate, "git-wrapper")
	script := fmt.Sprintf(`#!/bin/sh
case " $* " in
*" http.followRedirects"*)
  if [ -e '%[1]s/armed' ] && [ "$(pwd -P)" = "$(cat '%[1]s/armed')" ]; then
    rm -f '%[1]s/armed'
    : > '%[1]s/reached'
    i=0
    while [ ! -e '%[1]s/go' ] && [ $i -lt 3000 ]; do sleep 0.01; i=$((i+1)); done
  fi;;
esac
exec '%[2]s' "$@"
`, s.gate, realGit)
	require.NoError(t, os.WriteFile(wrapper, []byte(script), 0o755))
	t.Cleanup(test.MockVariableValue(&git.GitExecutable, wrapper))

	tc := newTestCluster(t, "node-a", "node-b", "node-c")
	for _, n := range tc.nodes {
		n.r.objects.repoKnown = n.r.repoKnown
	}
	raisePayload(t, tc)
	s.a, s.b, s.c = tc.nodes[0], tc.nodes[1], tc.nodes[2]
	zero := strings.Repeat("0", 40)
	ctx := db.DefaultContext

	// The victims, each with a master and a branch the upstream does not
	// hold, which the mirror's prune would remove.
	for _, v := range []struct {
		key string
		id  int64
	}{{pmPullPrivate, pmPullPrivID}, {pmPullNoID, 0}} {
		tc.createVia(s.b, v.key)
		work := t.TempDir()
		run(t, work, "init", "-q")
		master := commit(t, work, "PRIVATE master of "+v.key)
		require.True(t, tplGenPushVia(t, s.b, work, v.key, "refs/heads/master", zero, master, v.id).OK)
		salaries := commit(t, work, "PRIVATE salaries of "+v.key)
		require.True(t, tplGenPushVia(t, s.b, work, v.key, "refs/heads/salaries", zero, salaries, v.id).OK)
		s.victims[v.key] = master
		s.victims[v.key+" salaries"] = salaries
	}

	// The pull mirror 5: its key from git init alone, as on every node
	// that did not migrate it.
	tc.createVia(s.b, pmPullMirror)

	// Its upstream, a local bare repository its owner may mirror.
	s.upstream = filepath.Join(t.TempDir(), "upstream.git")
	run(t, filepath.Dir(s.upstream), "init", "-q", "--bare", s.upstream)
	s.up = t.TempDir()
	run(t, s.up, "init", "-q")
	s.master = commit(t, s.up, "upstream u1")
	run(t, s.up, "push", "-q", s.upstream, s.master+":refs/heads/master")
	f1 := commit(t, s.up, "upstream f1")
	run(t, s.up, "push", "-q", s.upstream, f1+":refs/heads/feature")

	t.Cleanup(test.MockVariableValue(&setting.ImportLocalPaths, true))
	owner := unittest.AssertExistsAndLoadBean(t, &user_model.User{ID: 3})
	owner.AllowImportLocal = true
	_, err = db.GetEngine(ctx).ID(owner.ID).Cols("allow_import_local").Update(owner)
	require.NoError(t, err)
	_, err = db.DeleteByBean(ctx, &repo_model.Mirror{RepoID: pmPullMirrorID})
	require.NoError(t, err)
	require.NoError(t, (&repo_model.Mirror{RepoID: pmPullMirrorID, Interval: setting.Mirror.DefaultInterval, EnablePrune: true}).InsertWithAddress(ctx, s.upstream))

	waitApplied(t, s.b, s.a)
	for _, key := range []string{pmPullPrivate, pmPullNoID} {
		require.Eventually(t, func() bool { return gitRef(t, s.a, key, "refs/heads/salaries") == s.victims[key+" salaries"] }, 15*time.Second, 20*time.Millisecond, "node-a serves %s", key)
	}
	settle(t, s.a)
	stamp, known := readStamp(filepath.Join(s.a.r.root, filepath.FromSlash(pmPullNoID)))
	require.True(t, known)
	require.Zero(t, stamp.ID, "the directory of %s carries no repository ID", pmPullNoID)
	tplGenServe(t, s.a)

	// The bench works: a sync from node-a fetches the upstream into the
	// mirror, and is not due again before its interval.
	require.True(t, mirror_service.SyncPullMirror(ctx, pmPullMirrorID), "the pull mirror syncs from node-a")
	s.synced(t, pmPullMirror, s.master)
	for _, key := range []string{pmPullPrivate, pmPullNoID} {
		s.untouched(t, key, key)
	}
	return s
}

// syncHeld runs a sync of the mirror from node-a, its first git command in
// the mirror's directory held while during runs, and returns its outcome.
func (s *pullMirrorSwap) syncHeld(t *testing.T, during func()) bool {
	t.Helper()
	mirrorDir, err := filepath.EvalSymlinks(filepath.Join(s.a.r.root, filepath.FromSlash(pmPullMirror)))
	require.NoError(t, err)
	for _, f := range []string{"reached", "go"} {
		require.NoError(t, os.RemoveAll(filepath.Join(s.gate, f)))
	}
	require.NoError(t, os.WriteFile(filepath.Join(s.gate, "armed"), []byte(mirrorDir), 0o644))
	done := make(chan bool, 1)
	go func() { done <- mirror_service.SyncPullMirror(db.DefaultContext, pmPullMirrorID) }()
	require.Eventually(t, func() bool {
		_, err := os.Stat(filepath.Join(s.gate, "reached"))
		return err == nil
	}, 30*time.Second, 10*time.Millisecond, "the sync reaches its first git command in the mirror's directory")
	during()
	require.NoError(t, os.WriteFile(filepath.Join(s.gate, "go"), nil, 0o644))
	select {
	case ok := <-done:
		return ok
	case <-time.After(2 * time.Minute):
		t.Fatal("the sync did not end")
		return false
	}
}

// renames renames each pair in turn through node-b, then waits until
// node-a projected them all and certified every key: its git serves sha
// as key's master.
func (s *pullMirrorSwap) renames(t *testing.T, key, sha string, pairs ...[2]string) {
	t.Helper()
	for _, p := range pairs {
		renameOn(t, s.b, p[0], p[1])
	}
	waitApplied(t, s.b, s.a)
	require.Eventually(t, func() bool { return gitRef(t, s.a, key, "refs/heads/master") == sha }, 15*time.Second, 20*time.Millisecond, "node-a projects the renames")
	settle(t, s.a)
}

// swap renames the mirror away and victim under its name.
func (s *pullMirrorSwap) swap(t *testing.T, victim string) {
	t.Helper()
	s.renames(t, pmPullMirror, s.victims[victim], [2]string{pmPullMirror, pmPullTmp}, [2]string{victim, pmPullMirror})
	stamp, _ := readStamp(filepath.Join(s.a.r.root, filepath.FromSlash(pmPullMirror)))
	t.Logf("node-a: %s holds %s, stamp repo=%s id=%d; ReadRefusal(%s, %d): %v", pmPullMirror, victim, stamp.Repo, stamp.ID, pmPullMirror, pmPullMirrorID, s.a.r.ReadRefusal(pmPullMirror, pmPullMirrorID))
}

// unswap undoes swap.
func (s *pullMirrorSwap) unswap(t *testing.T, victim string) {
	t.Helper()
	s.renames(t, pmPullMirror, s.master, [2]string{pmPullMirror, victim}, [2]string{pmPullTmp, pmPullMirror})
}

// due makes the mirror due for the update_mirrors cron.
func (s *pullMirrorSwap) due(t *testing.T) timeutil.TimeStamp {
	t.Helper()
	at := timeutil.TimeStampNow().Add(-3600)
	_, err := db.GetEngine(db.DefaultContext).Where("repo_id=?", pmPullMirrorID).Cols("next_update_unix").Update(&repo_model.Mirror{NextUpdateUnix: at})
	require.NoError(t, err)
	return at
}

// pullMirrorPicked reports whether the update_mirrors cron picks the
// mirror now.
func pullMirrorPicked(t *testing.T) bool {
	t.Helper()
	found := false
	require.NoError(t, repo_model.MirrorsIterate(db.DefaultContext, 0, func(_ int, bean any) error {
		found = found || bean.(*repo_model.Mirror).RepoID == pmPullMirrorID
		return nil
	}))
	return found
}

// untouched checks that the repository under key, whose refs were those
// of victim, holds them on every node, and nothing of the upstream.
func (s *pullMirrorSwap) untouched(t *testing.T, key, victim string) {
	t.Helper()
	waitApplied(t, s.a, s.c)
	waitApplied(t, s.a, s.b)
	for _, n := range []*testNode{s.a, s.b, s.c} {
		master, _ := n.r.table.Get(key, "refs/heads/master")
		salaries, held := n.r.table.Get(key, "refs/heads/salaries")
		feature, fetched := n.r.table.Get(key, "refs/heads/feature")
		assert.Equal(t, s.victims[victim], master.SHA, "%s: the master of %s, under %s, was rewritten by the pull mirror's sync", n.slot, victim, key)
		assert.True(t, held && salaries.Live(), "%s: the branch salaries of %s, under %s, was pruned by the pull mirror's sync", n.slot, victim, key)
		assert.Equal(t, s.victims[victim+" salaries"], salaries.SHA, "%s: the branch salaries of %s, under %s", n.slot, victim, key)
		assert.False(t, fetched && feature.Live(), "%s: the mirror's upstream was fetched into %s, under %s", n.slot, victim, key)
	}
}

// refused checks a sync that had to be refused: nothing of the mirror
// reached victim, under the mirror's name, on any node, nothing was
// written into the git config of the directory that holds it, and the
// mirror stays due, for the next round of update_mirrors.
func (s *pullMirrorSwap) refused(t *testing.T, ok bool, dueAt timeutil.TimeStamp, victim string) {
	t.Helper()
	assert.False(t, ok, "the sync is refused")
	s.untouched(t, pmPullMirror, victim)
	remotes, _ := gitOut(t, filepath.Join(s.a.r.root, filepath.FromSlash(pmPullMirror)), "config", "--get-regexp", `^remote\.`)
	assert.Empty(t, remotes, "the mirror's remote was written into the git config of %s", victim)
	row := unittest.AssertExistsAndLoadBean(t, &repo_model.Mirror{RepoID: pmPullMirrorID})
	assert.Equal(t, dueAt, row.NextUpdateUnix, "a refused sync is not written as done")
	assert.True(t, pullMirrorPicked(t), "the next round of update_mirrors retries the refused sync")
}

// synced checks that every node holds sha as the master of the mirror,
// and the upstream's feature, under key, and that the mirror is not due
// before its interval.
func (s *pullMirrorSwap) synced(t *testing.T, key, sha string) {
	t.Helper()
	waitApplied(t, s.a, s.c)
	waitApplied(t, s.a, s.b)
	for _, n := range []*testNode{s.a, s.b, s.c} {
		v, _ := n.r.table.Get(key, "refs/heads/master")
		assert.Equal(t, sha, v.SHA, "%s: the mirror's master under %s", n.slot, key)
		v, ok := n.r.table.Get(key, "refs/heads/feature")
		assert.True(t, ok && v.Live(), "%s: the upstream's feature under %s", n.slot, key)
	}
	assert.False(t, pullMirrorPicked(t), "a sync done is not due again before its interval")
}

// syncedAgain pushes a new commit to the upstream's master and checks that
// a sync from node-a, as the next round of update_mirrors runs it, fetches
// it into the mirror, under key.
func (s *pullMirrorSwap) syncedAgain(t *testing.T, key string) {
	t.Helper()
	s.syncs++
	s.master = commit(t, s.up, fmt.Sprintf("upstream, before sync %d", s.syncs))
	run(t, s.up, "push", "-q", s.upstream, s.master+":refs/heads/master")
	require.True(t, mirror_service.SyncPullMirror(db.DefaultContext, pmPullMirrorID), "the retried sync fetches")
	s.synced(t, key, s.master)
}

// A pull mirror's sync loaded its repository once, at its start, and every
// later step worked on the directory under that name, past no check: the
// mirror's remote, recreated from SQL into its git config, and `git fetch
// --prune --tags` with the mirror's refspec. A rename that put another
// repository under the name, projected on the node meanwhile, made the
// sync recreate the remote in that repository's git config, then rewrite
// and prune its branches, the transaction taken on every node: its old
// values matched the table, which rightly holds that repository under the
// name, and it carried no ID (#322, the pull side of #294). The sync now
// checks, before anything touches the directory, that it holds the
// mirror's repository and that the row still gives the name, then again
// right before each git command that writes there, and its ref
// transactions carry the mirror's ID. A refused sync stays due, and the
// next one fetches.
func TestAPullMirrorNeverRewritesTheRepositoryARenamePutUnderItsName(t *testing.T) {
	s := newPullMirrorSwap(t)
	ctx := db.DefaultContext

	// The issue's case: the mirror renamed away and the private repository
	// 3 under its name, through node-b, projected on node-a while the
	// sync's first git command in the directory was held. Its stamp names
	// another ID, and renames of the key were applied since the first
	// check: the sync is refused before it writes anything.
	t.Run("renames projected while the sync runs", func(t *testing.T) {
		dueAt := s.due(t)
		ok := s.syncHeld(t, func() { s.swap(t, pmPullPrivate) })
		s.refused(t, ok, dueAt, pmPullPrivate)
		s.unswap(t, pmPullPrivate)
		s.untouched(t, pmPullPrivate, pmPullPrivate)
		s.syncedAgain(t, pmPullMirror)
	})

	// The same with a repository whose refs were written without an ID:
	// its stamp names none to compare, and the table rightly holds it
	// under the name once node-a certified it. The renames of the key
	// applied since the sync's first check refuse it.
	t.Run("renames projected while the sync runs, the repository without an ID", func(t *testing.T) {
		dueAt := s.due(t)
		ok := s.syncHeld(t, func() {
			s.swap(t, pmPullNoID)
			require.NoError(t, s.a.r.ReadRefusal(pmPullMirror, pmPullMirrorID), "the directory's stamp and the table alone do not tell")
		})
		s.refused(t, ok, dueAt, pmPullNoID)
		s.unswap(t, pmPullNoID)
		s.untouched(t, pmPullNoID, pmPullNoID)
		s.syncedAgain(t, pmPullMirror)
	})

	// The same renames as Forgejo makes them, each recorded in the
	// database, projected and certified on node-a after the sync loaded
	// its repository and before its first check of the directories, the
	// repository under the name without an ID: neither the stamp, nor the
	// table, nor the watch of renames tells. Only the mirror's row, read
	// again by its ID, does. The next sync loads the repository under its
	// new name and fetches there.
	t.Run("renames the database recorded before the sync checks its directories", func(t *testing.T) {
		dueAt := s.due(t)
		var watches atomic.Int32
		clusterhook.SetRewriteWatch(func(key string) func() bool {
			if key == pmPullMirror && watches.Add(1) == 1 {
				s.swap(t, pmPullNoID)
				_, err := db.GetEngine(ctx).ID(pmPullMirrorID).Cols("name", "lower_name").Update(&repo_model.Repository{Name: "repo5-old", LowerName: "repo5-old"})
				require.NoError(t, err)
				require.NoError(t, s.a.r.ReadRefusal(pmPullMirror, pmPullMirrorID), "the directory's stamp and the table alone do not tell")
			}
			return s.a.r.watchRewrite(key)
		})
		ok := mirror_service.SyncPullMirror(ctx, pmPullMirrorID)
		clusterhook.SetRewriteWatch(s.a.r.watchRewrite)
		require.Positive(t, watches.Load(), "the sync checks its directory")
		s.refused(t, ok, dueAt, pmPullNoID)
		s.syncedAgain(t, pmPullTmp)
		s.untouched(t, pmPullMirror, pmPullNoID)
	})
}

// A ref transaction of Forgejo's own, without a pusher, whose writer names
// its repository by its ID, run in a directory stamped with another one,
// is refused before it is proposed: what a pull mirror's checks leave, the
// time from the last one to the start of its git command, which then
// works in the directory a rename put under the name (#322). A write
// without an ID, and HEAD written again under a new ID, as an adoption
// binds it, are not judged.
func TestAWriteNamingItsRepositoryIsRefusedInAnotherRepositorysDirectory(t *testing.T) {
	t.Parallel()
	tc := newTestCluster(t, "node-a", "node-b", "node-c")
	a := tc.nodes[0]
	const key = "alice/payroll.git"
	work := t.TempDir()
	run(t, work, "init", "-q")
	c1 := commit(t, work, "one")
	zero := strings.Repeat("0", 40)
	id := tc.repoID(key)
	require.True(t, tc.pushVia(a, work, key, "refs/heads/main", zero, c1).OK)
	dir := filepath.Join(a.r.root, filepath.FromSlash(key))
	require.Eventually(t, func() bool {
		s, ok := readStamp(dir)
		return ok && s.ID == id
	}, 10*time.Second, 20*time.Millisecond, "the directory is stamped with its ID")

	c2 := commit(t, work, "two")
	run(t, work, "push", "-q", dir, c2+":refs/tmp/in")
	run(t, dir, "update-ref", "-d", "refs/tmp/in")
	// write runs a transaction of one line as git does, ending it as git
	// does once the hook answered.
	write := func(repoID int64, line clusterhook.Line) *clusterhook.Response {
		req := &clusterhook.Request{
			Op: clusterhook.OpPrepared, Nonce: clusterhook.NewNonce(), GitDir: dir, RepoID: repoID,
			Lines: []clusterhook.Line{line},
		}
		resp := a.r.Handle(t.Context(), req)
		req.Op = clusterhook.OpAborted
		if resp.OK {
			if line.Ref != "HEAD" {
				run(t, dir, "update-ref", line.Ref, line.New)
			}
			req.Op = clusterhook.OpCommitted
		}
		a.r.Handle(t.Context(), req)
		return resp
	}

	// Another repository's sync, its fetch run in this directory: refused,
	// nothing proposed.
	resp := write(id+40, clusterhook.Line{Old: c1, New: c2, Ref: "refs/heads/main"})
	require.False(t, resp.OK, "a write naming another repository is proposed in its directory")
	t.Logf("refusal: %s", resp.Message)
	assert.Contains(t, resp.Message, "nothing was written")
	resp = write(id+40, clusterhook.Line{Old: zero, New: c2, Ref: "refs/heads/upstream"})
	require.False(t, resp.OK, "a creation naming another repository is proposed in its directory")
	for _, n := range tc.nodes {
		v, _ := n.r.table.Get(key, "refs/heads/main")
		assert.Equal(t, c1, v.SHA, n.slot)
		v, ok := n.r.table.Get(key, "refs/heads/upstream")
		assert.False(t, ok && v.Live(), n.slot)
	}

	// The repository's own ID, no ID at all, and HEAD under a new ID pass.
	assert.True(t, write(id, clusterhook.Line{Old: c1, New: c2, Ref: "refs/heads/main"}).OK, "the repository's own write")
	assert.True(t, write(0, clusterhook.Line{Old: zero, New: c2, Ref: "refs/heads/noid"}).OK, "a write without an ID")
	assert.True(t, write(id+40, clusterhook.Line{Old: zero, New: "ref:refs/heads/main", Ref: "HEAD"}).OK, "HEAD written again under a new ID, as an adoption binds it")
}
