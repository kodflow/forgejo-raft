// Copyright 2026 The Forgejo Authors. All rights reserved.
// SPDX-License-Identifier: GPL-3.0-or-later

package cluster

import (
	"fmt"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	"forgejo.org/models/db"
	repo_model "forgejo.org/models/repo"
	system_model "forgejo.org/models/system"
	"forgejo.org/models/unittest"
	mirror_service "forgejo.org/services/mirror"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// noticeNames reports whether a notice of the last ones names ref among
// the refs a sync could not update.
func noticeNames(t *testing.T, ref string) bool {
	t.Helper()
	notices, err := system_model.Notices(db.DefaultContext, 1, 20)
	require.NoError(t, err)
	for _, n := range notices {
		if strings.Contains(n.Description, " "+ref+" (") {
			return true
		}
	}
	return false
}

// A pull mirror whose owner keeps the branches the upstream deletes
// (EnablePrune off, a setting of the UI and of the API). The upstream
// replaces its branch feature by feature/x, then a branch topic/y by
// topic, and its master moves on each time. git 2.51 and later run the
// whole fetch as ONE transaction that tolerates a failure per ref, and
// hand every line to the reference-transaction hook, the ref git rejects
// included: the cluster refused the whole entry on the directory and file
// conflict, git aborted the whole fetch, and every later sync did the
// same, the mirror's master behind the upstream's for good, on every node
// (#365). Outside a cluster git writes master and rejects the conflicting
// ref alone, and reports the sync failed: so does the cluster now.
//
// Then prune is on again, which frees the names, and the upstream gains a
// ref whose name is not valid UTF-8: git takes it, the cluster cannot
// carry it, and the hook refuses the whole transaction that holds one
// (clusterhook.ParseLines): every sync was refused whole the same way.
// The sync leaves that name out, writes the rest, and fails naming it.
func TestPullMirrorSyncWritesAllButTheRefsItCannot(t *testing.T) {
	s := newPullMirrorSwap(t)
	ctx := db.DefaultContext
	// A local push runs the upstream's receive-pack without the pusher's
	// -c: the bench's hooks, in its git home, ran on each push.
	run(t, s.upstream, "config", "core.hooksPath", "/dev/null")

	_, err := db.GetEngine(ctx).Where("repo_id=?", pmPullMirrorID).Cols("enable_prune").Update(&repo_model.Mirror{EnablePrune: false})
	require.NoError(t, err)
	require.False(t, unittest.AssertExistsAndLoadBean(t, &repo_model.Mirror{RepoID: pmPullMirrorID}).EnablePrune)

	// Control: a mirror of the upstream as it is now, outside the cluster.
	plain := filepath.Join(t.TempDir(), "plain.git")
	run(t, filepath.Dir(plain), "clone", "-q", "--mirror", s.upstream, plain)

	f1, ok := s.a.r.table.Get(pmPullMirror, "refs/heads/feature")
	require.True(t, ok && f1.Live())

	// sync runs the update_mirrors cron's sync from node-a and checks that
	// every node took the upstream's master, holds kept at keptSHA, and
	// nothing under refused, which a notice names.
	sync := func(step, kept, keptSHA, refused string) {
		t.Helper()
		s.due(t)
		ok := mirror_service.SyncPullMirror(ctx, pmPullMirrorID)
		assert.False(t, ok, "%s: as git outside a cluster, the sync reports the ref it could not update", step)
		waitApplied(t, s.a, s.b)
		waitApplied(t, s.a, s.c)
		for _, n := range []*testNode{s.a, s.b, s.c} {
			v, _ := n.r.table.Get(pmPullMirror, "refs/heads/master")
			assert.Equal(t, s.master, v.SHA, "%s: %s: the mirror's master did not take the upstream's", step, n.slot)
			v, ok := n.r.table.Get(pmPullMirror, kept)
			assert.True(t, ok && v.Live(), "%s: %s: %s, which the upstream deleted, is kept without prune", step, n.slot, kept)
			assert.Equal(t, keptSHA, v.SHA, "%s: %s: %s", step, n.slot, kept)
			v, ok = n.r.table.Get(pmPullMirror, refused)
			assert.False(t, ok && v.Live(), "%s: %s: %s, which conflicts with %s, was written", step, n.slot, refused, kept)
		}
		assert.Equal(t, s.master, gitRef(t, s.a, pmPullMirror, "refs/heads/master"), "%s: node-a's git", step)
		assert.True(t, noticeNames(t, refused), "%s: a notice names %s", step, refused)
	}

	// feature gives way to feature/x, and master moves on, in one push.
	fx := commit(t, s.up, "upstream feature/x")
	s.master = commit(t, s.up, "upstream master after the swap")
	run(t, s.up, "push", "-q", s.upstream, ":refs/heads/feature", fx+":refs/heads/feature/x", s.master+":refs/heads/master")

	// Control: plain git, prune off, takes master and rejects feature/x alone.
	out, ferr := gitIn(plain, "fetch", "--tags", "origin")
	t.Logf("control, plain git fetch without prune: err=%v out=%s", ferr, out)
	plainMaster, _ := gitOut(t, plain, "rev-parse", "refs/heads/master")
	require.Equal(t, s.master, plainMaster, "control: without the cluster, git writes master and rejects feature/x alone")

	for i := 1; i <= 2; i++ {
		sync(fmt.Sprintf("a to a/b, sync %d", i), "refs/heads/feature", f1.SHA, "refs/heads/feature/x")
	}

	// The reverse: topic/y comes, then gives way to topic.
	ty := commit(t, s.up, "upstream topic/y")
	run(t, s.up, "push", "-q", s.upstream, ty+":refs/heads/topic/y")
	s.due(t)
	mirror_service.SyncPullMirror(ctx, pmPullMirrorID)
	waitApplied(t, s.a, s.b)
	waitApplied(t, s.a, s.c)
	for _, n := range []*testNode{s.a, s.b, s.c} {
		v, _ := n.r.table.Get(pmPullMirror, "refs/heads/topic/y")
		require.Equal(t, ty, v.SHA, "%s: the mirror took topic/y", n.slot)
	}
	tp := commit(t, s.up, "upstream topic")
	s.master = commit(t, s.up, "upstream master after the reverse swap")
	run(t, s.up, "push", "-q", s.upstream, ":refs/heads/topic/y", tp+":refs/heads/topic", s.master+":refs/heads/master")
	sync("a/b to a", "refs/heads/topic/y", ty, "refs/heads/topic")

	// Prune on, and a name that is not valid UTF-8 upstream.
	_, err = db.GetEngine(ctx).Where("repo_id=?", pmPullMirrorID).Cols("enable_prune").Update(&repo_model.Mirror{EnablePrune: true})
	require.NoError(t, err)
	bad := "refs/heads/caf\xe9"
	s.master = commit(t, s.up, "upstream master beside a name that is not UTF-8")
	run(t, s.up, "push", "-q", s.upstream, s.master+":refs/heads/master", s.master+":"+bad)
	for i := 1; i <= 2; i++ {
		step := fmt.Sprintf("a name that is not UTF-8, sync %d", i)
		s.due(t)
		ok := mirror_service.SyncPullMirror(ctx, pmPullMirrorID)
		assert.False(t, ok, "%s: the sync reports the ref it could not update", step)
		waitApplied(t, s.a, s.b)
		waitApplied(t, s.a, s.c)
		for _, n := range []*testNode{s.a, s.b, s.c} {
			want := map[string]string{"refs/heads/master": s.master, "refs/heads/feature/x": fx, "refs/heads/topic": tp}
			refs, _ := n.r.table.Refs(pmPullMirror)
			got := map[string]string{}
			for name, v := range refs {
				if v.Live() && strings.HasPrefix(name, "refs/heads/") {
					got[name] = v.SHA
				}
			}
			assert.Equal(t, want, got, "%s: %s: the mirror's branches, pruned, without the name that is not UTF-8", step, n.slot)
		}
		assert.Equal(t, s.master, gitRef(t, s.a, pmPullMirror, "refs/heads/master"), "%s: node-a's git", step)
		held, _ := gitOut(t, filepath.Join(s.a.r.root, filepath.FromSlash(pmPullMirror)), "for-each-ref", "--format=%(refname)", "refs/heads/")
		assert.NotContains(t, held, "caf", "%s: node-a's git holds the name the cluster does not", step)
		assert.True(t, noticeNames(t, strconv.Quote(bad)), "%s: a notice names %q", step, bad)
	}
}
