// Copyright 2026 The Forgejo Authors. All rights reserved.
// SPDX-License-Identifier: GPL-3.0-or-later

package cluster

import (
	"context"
	"errors"
	"os"
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
	repo_service "forgejo.org/services/repository"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// tplGenTemplate is the fixtures' repository 44, user27/template1: a public
// template whose default branch is master.
const (
	tplGenTemplate   = "user27/template1.git"
	tplGenTemplateID = int64(44)
)

// tplGenPushVia is tryPushVia with the repository ID given, as Forgejo's
// hook carries the database ID of the repository it pushes to.
func tplGenPushVia(t *testing.T, n *testNode, work, repo, ref, old, sha string, id int64) *clusterhook.Response {
	t.Helper()
	dir := filepath.Join(n.r.root, filepath.FromSlash(repo))
	if _, err := os.Stat(dir); os.IsNotExist(err) {
		run(t, n.r.root, "init", "-q", "--bare", dir)
	}
	run(t, work, "push", "-q", dir, sha+":refs/tmp/incoming")
	run(t, dir, "update-ref", "-d", "refs/tmp/incoming")
	req := &clusterhook.Request{
		Op: clusterhook.OpPrepared, Nonce: clusterhook.NewNonce(), GitDir: dir, RepoID: id, PusherID: 2,
		Lines: []clusterhook.Line{{Old: old, New: sha, Ref: ref}},
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	unlock := holdRefLock(t, dir, ref, sha)
	resp := n.r.Handle(ctx, req)
	unlock()
	req.Op = clusterhook.OpAborted
	if resp.OK {
		run(t, dir, "update-ref", ref, sha)
		req.Op = clusterhook.OpCommitted
	}
	n.r.Handle(ctx, req)
	return resp
}

// tplGenServe makes node n serve Forgejo's requests: its git, its
// freshness checks, its read guard and its watch of renames.
func tplGenServe(t *testing.T, n *testNode) {
	t.Helper()
	withGitThroughReplicator(t, n)
	freshFrom(t, n)
	clusterhook.SetReadGuard(n.r.ReadRefusal)
	clusterhook.SetRewriteWatch(n.r.watchRewrite)
	t.Cleanup(func() {
		clusterhook.SetReadGuard(nil)
		clusterhook.SetRewriteWatch(nil)
	})
}

// tplGenerate generates user2/<name> from the template as the web form
// /repo/create?template_id=44 does: the template is loaded by its ID,
// never through RepoAssignment.
func tplGenerate(t *testing.T, name string, opts repo_service.GenerateRepoOptions) (*repo_model.Repository, error) {
	t.Helper()
	ctx := db.DefaultContext
	tmpl := unittest.AssertExistsAndLoadBean(t, &repo_model.Repository{ID: tplGenTemplateID})
	require.True(t, tmpl.IsTemplate)
	require.False(t, tmpl.IsEmpty)
	require.NoError(t, tmpl.LoadOwner(ctx))
	doer := unittest.AssertExistsAndLoadBean(t, &user_model.User{ID: 1})
	owner := unittest.AssertExistsAndLoadBean(t, &user_model.User{ID: 2})
	opts.Name = name
	return repo_service.GenerateRepository(ctx, doer, owner, tmpl, opts)
}

// tplGenFile reads f.txt on master of key in n's git, "" when absent.
func tplGenFile(t *testing.T, n *testNode, key string) string {
	t.Helper()
	out, err := gitIn(filepath.Join(n.r.root, filepath.FromSlash(key)), "show", "refs/heads/master:f.txt")
	if err != nil {
		return ""
	}
	return out
}

// tplGenNothingWritten checks that a refused generation of user2/<name>
// left no row and no key on any member.
func tplGenNothingWritten(t *testing.T, tc *testCluster, name string) {
	t.Helper()
	unittest.AssertNotExistsBean(t, &repo_model.Repository{OwnerID: 2, LowerName: name})
	key := clusterhook.RepoKey("user2", name, false)
	for _, n := range tc.nodes {
		refs, _ := n.r.table.Refs(key)
		assert.False(t, anyLive(refs), "%s: %s holds live refs", n.slot, key)
		assert.False(t, dirExists(n, key), "%s: %s was created", n.slot, key)
	}
}

// tplGenServed logs what every member serves of a generation that was
// expected to be refused, gen, nil when it was.
func tplGenServed(t *testing.T, tc *testCluster, gen *repo_model.Repository) {
	t.Helper()
	if gen == nil {
		return
	}
	key := clusterhook.RepoKey(gen.OwnerName, gen.Name, false)
	for _, n := range tc.nodes {
		require.Eventually(t, func() bool { return tplGenFile(t, n, key) != "" }, 15*time.Second, 20*time.Millisecond, n.slot)
		t.Logf("%s: %s master:f.txt = %q", n.slot, key, tplGenFile(t, n, key))
	}
}

// tplGenGenerated waits until every member serves the generated
// repository's master and checks its f.txt is want.
func tplGenGenerated(t *testing.T, tc *testCluster, gen *repo_model.Repository, want string) {
	t.Helper()
	key := clusterhook.RepoKey(gen.OwnerName, gen.Name, false)
	waitAll(t, tc, func(n *testNode) bool { return tplGenFile(t, n, key) != "" })
	for _, n := range tc.nodes {
		assert.Equal(t, want, tplGenFile(t, n, key), "%s: %s", n.slot, key)
	}
}

// A private repository held the template's name, user27/template1, and
// was renamed away (to user27/payroll); the public template then took the
// name, through node-b. node-a has applied both entries but its projection
// of the name is behind (a transaction in flight, a long fetch, a pass
// busy elsewhere, as in #82): its user27/template1.git directory still
// holds the private repository, and a read of the template there is
// refused. The web form's generation loads the template by its ID, past
// RepoAssignment's guard: it cloned that directory, and committed the
// private tree into a new repository of the requester, replicated to every
// member (#261). It now waits for node-a to serve the template and is
// refused while node-a does not, before anything is written; once node-a
// caught up, it is built on the template.
func TestLaggingTemplateGenerationNeverCopiesAnotherRepository(t *testing.T) {
	require.NoError(t, unittest.PrepareTestDatabase())
	tc := newTestCluster(t, "node-a", "node-b", "node-c")
	raisePayload(t, tc)
	a, b := tc.nodes[0], tc.nodes[1]
	zero := strings.Repeat("0", 40)

	priv := t.TempDir()
	run(t, priv, "init", "-q")
	p1 := commit(t, priv, "PRIVATE: payroll of the whole company")
	require.True(t, tplGenPushVia(t, b, priv, tplGenTemplate, "refs/heads/master", zero, p1, 900).OK)
	waitAll(t, tc, func(n *testNode) bool { return gitRef(t, n, tplGenTemplate, "refs/heads/master") == p1 })
	settle(t, a)

	release := holdOn(t, a, tplGenTemplate)
	renameOn(t, b, tplGenTemplate, "user27/payroll.git")
	tc.createVia(b, tplGenTemplate)
	pub := t.TempDir()
	run(t, pub, "init", "-q")
	t1 := commit(t, pub, "template content")
	resp := tplGenPushVia(t, b, pub, tplGenTemplate, "refs/heads/master", zero, t1, tplGenTemplateID)
	require.True(t, resp.OK, resp.Message)
	waitApplied(t, b, a)
	require.Equal(t, p1, gitRef(t, a, tplGenTemplate, "refs/heads/master"), "node-a still holds the private repository under the template's name")
	require.Error(t, a.r.ReadRefusal(tplGenTemplate, tplGenTemplateID), "a read of the template is refused on node-a (#82)")

	tplGenServe(t, a)

	t.Run("node-a waits to serve the template", func(t *testing.T) {
		gen, err := tplGenerate(t, "tpl-gen-private", repo_service.GenerateRepoOptions{GitContent: true})
		tplGenServed(t, tc, gen)
		require.ErrorIs(t, err, clusterhook.ErrBehind)
		tplGenNothingWritten(t, tc, "tpl-gen-private")
	})

	// A node whose freshness check passes, its directory still holding
	// another repository, refuses the read as git's reads do.
	t.Run("node-a never reads another repository's directory", func(t *testing.T) {
		clusterhook.SetFreshWaiter(func(context.Context, string) bool { return true })
		defer clusterhook.SetFreshWaiter(a.r.WaitFresh)
		_, err := tplGenerate(t, "tpl-gen-private", repo_service.GenerateRepoOptions{GitContent: true})
		require.ErrorIs(t, err, clusterhook.ErrBehind)
		tplGenNothingWritten(t, tc, "tpl-gen-private")
		// The template's git hooks are read from the same directory.
		_, err = tplGenerate(t, "tpl-gen-hooks", repo_service.GenerateRepoOptions{GitHooks: true})
		require.ErrorIs(t, err, clusterhook.ErrBehind)
		tplGenNothingWritten(t, tc, "tpl-gen-hooks")
	})

	release()
	require.Eventually(t, func() bool { return gitRef(t, a, tplGenTemplate, "refs/heads/master") == t1 }, 15*time.Second, 20*time.Millisecond, "node-a catches up")

	// A rename of the template projected after the first check, while
	// `git init` of the new repository went through the cluster: the
	// directory is checked again right before git reads it, and the
	// repository is removed from every member as for a failed generation.
	t.Run("node-a checks the directory again right before reading it", func(t *testing.T) {
		var checks atomic.Int32
		clusterhook.SetReadGuard(func(key string, repoID int64) error {
			if key == tplGenTemplate && checks.Add(1) > 1 {
				return errors.New("this node of the cluster has not projected the last rename of " + key + " yet")
			}
			return a.r.ReadRefusal(key, repoID)
		})
		defer clusterhook.SetReadGuard(a.r.ReadRefusal)
		_, err := tplGenerate(t, "tpl-gen-late", repo_service.GenerateRepoOptions{GitContent: true})
		require.ErrorIs(t, err, clusterhook.ErrBehind)
		assert.EqualValues(t, 2, checks.Load(), "checked before anything is written, then right before the clone")
		unittest.AssertNotExistsBean(t, &repo_model.Repository{OwnerID: 2, LowerName: "tpl-gen-late"})
		requireGoneEverywhere(t, tc, clusterhook.RepoKey("user2", "tpl-gen-late", false))
	})

	gen, err := tplGenerate(t, "tpl-gen-private", repo_service.GenerateRepoOptions{GitContent: true})
	require.NoError(t, err)
	tplGenGenerated(t, tc, gen, "template content")
	for _, n := range tc.nodes {
		assert.Equal(t, p1, gitRef(t, n, "user27/payroll.git", "refs/heads/master"), "%s: the private repository is kept under its new name", n.slot)
	}

	// moves renames each pair in turn through node-b, then waits until
	// node-a projected them all: its git serves sha as key's master,
	// certified.
	moves := func(t *testing.T, key, sha string, pairs ...[2]string) {
		t.Helper()
		for _, p := range pairs {
			renameOn(t, b, p[0], p[1])
		}
		waitApplied(t, b, a)
		require.Eventually(t, func() bool { return gitRef(t, a, key, "refs/heads/master") == sha }, 15*time.Second, 20*time.Millisecond, "node-a projects the renames")
		settle(t, a)
	}
	// around counts node-a's checks of the template's directory, and runs
	// for the n-th one the function given in before right before it, and
	// the one given in after once it passed: renames projected between a
	// check and what precedes or follows it.
	around := func(t *testing.T, before, after map[int32]func()) *atomic.Int32 {
		t.Helper()
		var checks atomic.Int32
		clusterhook.SetReadGuard(func(key string, repoID int64) error {
			if key != tplGenTemplate {
				return a.r.ReadRefusal(key, repoID)
			}
			n := checks.Add(1)
			if f := before[n]; f != nil {
				f()
			}
			err := a.r.ReadRefusal(key, repoID)
			if f := after[n]; f != nil && err == nil {
				f()
			}
			return err
		})
		t.Cleanup(func() { clusterhook.SetReadGuard(a.r.ReadRefusal) })
		return &checks
	}
	const tmp, moved = "user27/template-tmp.git", "user27/template-moved.git"

	// The template is renamed away and the private repository takes its
	// name, both projected on node-a right after the check that precedes
	// git's read, and both undone, the template back under its name,
	// right before the check that follows it: git read the private
	// repository's directory, while the directory and the database show
	// the template before the read and after it. The renames of the key
	// node-a applied meanwhile tell: the generation is refused, and the
	// repository is removed from every member (#261).
	t.Run("node-a refuses a read the template's renames went through", func(t *testing.T) {
		checks := around(t, map[int32]func(){
			3: func() {
				moves(t, tplGenTemplate, t1, [2]string{tplGenTemplate, "user27/payroll.git"}, [2]string{tmp, tplGenTemplate})
			},
		}, map[int32]func(){
			2: func() {
				moves(t, tplGenTemplate, p1, [2]string{tplGenTemplate, tmp}, [2]string{"user27/payroll.git", tplGenTemplate})
			},
		})
		gen, err := tplGenerate(t, "tpl-gen-swapped", repo_service.GenerateRepoOptions{GitContent: true})
		tplGenServed(t, tc, gen)
		require.ErrorIs(t, err, clusterhook.ErrBehind)
		assert.EqualValues(t, 3, checks.Load(), "checked before anything is written, right before the clone, then right after it")
		unittest.AssertNotExistsBean(t, &repo_model.Repository{OwnerID: 2, LowerName: "tpl-gen-swapped"})
		requireGoneEverywhere(t, tc, clusterhook.RepoKey("user2", "tpl-gen-swapped", false))
		require.Equal(t, t1, gitRef(t, a, tplGenTemplate, "refs/heads/master"), "the template is back under its name")
	})

	// The same swap, for the template's git hooks: git read the private
	// repository's, and nothing of them is written into the new repository
	// before the check that follows the read, which refuses it.
	t.Run("node-a writes no hook of a read the template's renames went through", func(t *testing.T) {
		private := filepath.Join(a.r.root, "user27", "payroll.git", "hooks", "pre-receive.d", "pre-receive")
		require.NoError(t, os.MkdirAll(filepath.Dir(private), 0o755))
		require.NoError(t, os.WriteFile(private, []byte("#!/bin/sh\n# PRIVATE: the payroll's deploy key\n"), 0o644))
		generated := filepath.Join(a.r.root, "user2", "tpl-gen-hooks-swapped.git", "hooks", "pre-receive.d", "pre-receive")
		checks := around(t, map[int32]func(){
			3: func() {
				assert.NoFileExists(t, generated, "a hook git read through the swap was written before the check that follows the read")
				moves(t, tplGenTemplate, t1, [2]string{tplGenTemplate, "user27/payroll.git"}, [2]string{tmp, tplGenTemplate})
			},
		}, map[int32]func(){
			2: func() {
				moves(t, tplGenTemplate, p1, [2]string{tplGenTemplate, tmp}, [2]string{"user27/payroll.git", tplGenTemplate})
			},
		})
		_, err := tplGenerate(t, "tpl-gen-hooks-swapped", repo_service.GenerateRepoOptions{GitHooks: true})
		require.ErrorIs(t, err, clusterhook.ErrBehind)
		assert.EqualValues(t, 3, checks.Load(), "checked before anything is written, right before the hooks are read, then right after")
		unittest.AssertNotExistsBean(t, &repo_model.Repository{OwnerID: 2, LowerName: "tpl-gen-hooks-swapped"})
		requireGoneEverywhere(t, tc, clusterhook.RepoKey("user2", "tpl-gen-hooks-swapped", false))
		require.Equal(t, t1, gitRef(t, a, tplGenTemplate, "refs/heads/master"), "the template is back under its name")
		require.NoError(t, os.Remove(private), "the private repository's directory is back under its name")
	})

	// The same renames, not undone: the template's directory on node-a
	// holds the private repository when git reads it, and still does
	// after.
	t.Run("node-a refuses a read of the repository that took the template's name", func(t *testing.T) {
		checks := around(t, nil, map[int32]func(){
			2: func() {
				moves(t, tplGenTemplate, p1, [2]string{tplGenTemplate, moved}, [2]string{"user27/payroll.git", tplGenTemplate})
			},
		})
		gen, err := tplGenerate(t, "tpl-gen-taken", repo_service.GenerateRepoOptions{GitContent: true})
		tplGenServed(t, tc, gen)
		require.ErrorIs(t, err, clusterhook.ErrBehind)
		assert.EqualValues(t, 3, checks.Load(), "checked before anything is written, right before the clone, then right after it")
		unittest.AssertNotExistsBean(t, &repo_model.Repository{OwnerID: 2, LowerName: "tpl-gen-taken"})
		requireGoneEverywhere(t, tc, clusterhook.RepoKey("user2", "tpl-gen-taken", false))
	})
}

// A key deleted and created anew, both applied on node-a and not projected
// there yet when a read of the key's directory begins: the read's first
// check passes, the directory still holding the deleted repository under
// its stamp. The projection then sets that directory aside and makes the
// new repository's, which carries no stamp until it is certified, while
// git reads it. The read's watch counts from the index node-a had
// projected the key at, not from the one it had applied: it tells, as it
// tells a rename applied during the read. A push node-a had not projected
// yet is no rewrite: it refuses no read (#261).
func TestLaggingReadWatchSeesRewritesNotProjectedYet(t *testing.T) {
	tc := newTestCluster(t, "node-a", "node-b", "node-c")
	a, b := tc.nodes[0], tc.nodes[1]
	const key = "s/w.git"
	work := t.TempDir()
	run(t, work, "init", "-q")
	c1 := commit(t, work, "one")
	c2 := commit(t, work, "two")
	run(t, work, "checkout", "-q", "--orphan", "fresh")
	c3 := commit(t, work, "three")
	zero := strings.Repeat("0", 40)
	require.True(t, tc.pushVia(b, work, key, "refs/heads/main", zero, c1).OK)
	waitAll(t, tc, func(n *testNode) bool { return gitRef(t, n, key, "refs/heads/main") == c1 })
	settle(t, a)
	projects := func(sha string) {
		t.Helper()
		require.Eventually(t, func() bool { return gitRef(t, a, key, "refs/heads/main") == sha }, 15*time.Second, 20*time.Millisecond, "node-a projects %s", sha)
		settle(t, a)
	}

	release := holdOn(t, a, key)
	require.True(t, tc.pushVia(b, work, key, "refs/heads/main", c1, c2).OK)
	waitApplied(t, b, a)
	rewritten := a.r.watchRewrite(key)
	release()
	projects(c2)
	assert.False(t, rewritten(), "a push is no rewrite")

	release = holdOn(t, a, key)
	require.NoError(t, os.RemoveAll(filepath.Join(b.r.root, "s", "w.git")))
	require.NoError(t, b.r.proposeRepoDelete(t.Context(), key, 1))
	tc.createVia(b, key)
	require.True(t, tc.pushVia(b, work, key, "refs/heads/main", zero, c3).OK)
	waitApplied(t, b, a)
	require.Equal(t, c2, gitRef(t, a, key, "refs/heads/main"), "node-a has not projected the deletion")
	require.NoError(t, a.r.ReadRefusal(key, 0), "the read's first check passes")
	rewritten = a.r.watchRewrite(key)
	release()
	projects(c3)
	assert.True(t, rewritten(), "the deletion and the creation anew were not projected when the read began")
	assert.False(t, a.r.watchRewrite(key)(), "a read that begins once they are projected")
}

// The template's master moves from t1 to t2 through node-b, acknowledged.
// node-a has applied it, its git still shows t1. A repository generated
// from the template through node-a, right after, was built from t1 for
// good: the generation never waited for node-a to serve the template, as
// the branch, file and patch services wait since #226 and #239 (#261). It
// is now refused while node-a does not show t2, and built on t2 once it
// does. A generation that reads nothing from the template's directory is
// not refused.
func TestLaggingTemplateGenerationKeepsAnAcknowledgedPush(t *testing.T) {
	require.NoError(t, unittest.PrepareTestDatabase())
	tc := newTestCluster(t, "node-a", "node-b", "node-c")
	raisePayload(t, tc)
	a, b := tc.nodes[0], tc.nodes[1]
	zero := strings.Repeat("0", 40)

	tc.createVia(b, tplGenTemplate)
	pub := t.TempDir()
	run(t, pub, "init", "-q")
	t1 := commit(t, pub, "template v1")
	require.True(t, tplGenPushVia(t, b, pub, tplGenTemplate, "refs/heads/master", zero, t1, tplGenTemplateID).OK)
	waitAll(t, tc, func(n *testNode) bool { return gitRef(t, n, tplGenTemplate, "refs/heads/master") == t1 })
	settle(t, a)

	release := holdOn(t, a, tplGenTemplate)
	t2 := commit(t, pub, "template v2")
	resp := tplGenPushVia(t, b, pub, tplGenTemplate, "refs/heads/master", t1, t2, tplGenTemplateID)
	require.True(t, resp.OK, resp.Message)
	require.Eventually(t, func() bool {
		v, ok := a.r.table.Get(tplGenTemplate, "refs/heads/master")
		return ok && v.SHA == t2
	}, 10*time.Second, 20*time.Millisecond, "node-a applied the push")
	require.Equal(t, t1, gitRef(t, a, tplGenTemplate, "refs/heads/master"), "node-a has not projected it")

	tplGenServe(t, a)

	t.Run("node-a waits to serve the template", func(t *testing.T) {
		gen, err := tplGenerate(t, "tpl-gen-v", repo_service.GenerateRepoOptions{GitContent: true})
		tplGenServed(t, tc, gen)
		require.ErrorIs(t, err, clusterhook.ErrBehind)
		tplGenNothingWritten(t, tc, "tpl-gen-v")
	})

	t.Run("a generation that reads only SQL is served", func(t *testing.T) {
		gen, err := tplGenerate(t, "tpl-gen-topics", repo_service.GenerateRepoOptions{Topics: true, IssueLabels: true})
		require.NoError(t, err)
		requireLiveEverywhere(t, tc, clusterhook.RepoKey(gen.OwnerName, gen.Name, false))
	})

	release()
	require.Eventually(t, func() bool { return gitRef(t, a, tplGenTemplate, "refs/heads/master") == t2 }, 15*time.Second, 20*time.Millisecond, "node-a catches up")
	gen, err := tplGenerate(t, "tpl-gen-v", repo_service.GenerateRepoOptions{GitContent: true})
	require.NoError(t, err)
	tplGenGenerated(t, tc, gen, "template v2")
}
