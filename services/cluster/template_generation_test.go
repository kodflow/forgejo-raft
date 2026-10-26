// Copyright 2026 The Forgejo Authors. All rights reserved.
// SPDX-License-Identifier: GPL-3.0-or-later

package cluster

import (
	"context"
	"os"
	"path/filepath"
	"strings"
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
// freshness checks and its read guard.
func tplGenServe(t *testing.T, n *testNode) {
	t.Helper()
	withGitThroughReplicator(t, n)
	freshFrom(t, n)
	clusterhook.SetReadGuard(n.r.ReadRefusal)
	t.Cleanup(func() { clusterhook.SetReadGuard(nil) })
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
	gen, err := tplGenerate(t, "tpl-gen-private", repo_service.GenerateRepoOptions{GitContent: true})
	require.NoError(t, err)
	tplGenGenerated(t, tc, gen, "template content")
	for _, n := range tc.nodes {
		assert.Equal(t, p1, gitRef(t, n, "user27/payroll.git", "refs/heads/master"), "%s: the private repository is kept under its new name", n.slot)
	}
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
