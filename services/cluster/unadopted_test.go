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
	"forgejo.org/modules/test"
	repo_service "forgejo.org/services/repository"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// unadoptedOrphan leaves key an orphan, as the README's recovery section
// describes it: the repository's rows are deleted, the cluster did not
// record the deletion (no quorum), and every member keeps the files and
// the live key.
func unadoptedOrphan(t *testing.T, tc *testCluster, repoID int64, key string) {
	t.Helper()
	ctx := db.DefaultContext
	tc.mem.SetDown(tc.nodes[1].node, true)
	tc.mem.SetDown(tc.nodes[2].node, true)
	dctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	err := repo_service.DeleteRepositoryDirectly(dctx, repoID, repo_service.DeleteRepositoryOpts{})
	cancel()
	require.Error(t, err)
	require.Contains(t, err.Error(), "--delete-orphan")
	unittest.AssertNotExistsBean(t, &repo_model.Repository{ID: repoID})
	tc.mem.SetDown(tc.nodes[1].node, false)
	tc.mem.SetDown(tc.nodes[2].node, false)
	tc.nodes[0].r.projector.markDirty(key)
	time.Sleep(200 * time.Millisecond)
	requireLiveEverywhere(t, tc, key)
}

func unadoptedState(t *testing.T, tc *testCluster, key, ref, when string) {
	t.Helper()
	for _, n := range tc.nodes {
		v, ok := n.r.table.Get(key, ref)
		_, deleted := n.r.table.Deleted(key)
		_, id, _ := n.r.table.RefsWithID(key)
		t.Logf("%s %s: table %s live=%v sha=%s deleted=%v id=%d | dir=%v git=%q lag=%v",
			when, n.slot, ref, ok && v.Live(), v.SHA, deleted, id, dirExists(n, key), gitRef(t, n, key, ref), n.r.projector.Lag())
	}
}

// unadoptedWrite writes ref (old to sha) through n's replicator, its
// objects fetched into the repository first, as receive-pack's quarantine.
// A pull mirror's sync carries neither a pusher nor the repository's ID
// (services/mirror runs git without repo_module's pushing environment); a
// user's push through Forgejo's HTTP or SSH carries both (EnvRepoID,
// EnvPusherID), which the production hook relays (cmd/hook_cluster.go) and
// the test binary's hook does not (fork_test.go runTestHook).
func unadoptedWrite(t *testing.T, n *testNode, work, repo, ref, old, sha string, repoID, pusherID int64) {
	t.Helper()
	dir := filepath.Join(n.r.root, filepath.FromSlash(repo))
	run(t, dir, "fetch", "-q", work, sha+":refs/tmp/incoming")
	run(t, dir, "update-ref", "-d", "refs/tmp/incoming")
	req := &clusterhook.Request{
		Op: clusterhook.OpPrepared, Nonce: clusterhook.NewNonce(), GitDir: dir, RepoID: repoID, PusherID: pusherID,
		Lines: []clusterhook.Line{{Old: old, New: sha, Ref: ref}},
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	unlock := holdRefLock(t, dir, ref, sha)
	resp := n.r.Handle(ctx, req)
	unlock()
	require.True(t, resp.OK, resp.Message)
	run(t, dir, "update-ref", ref, sha)
	req.Op = clusterhook.OpCommitted
	n.r.Handle(ctx, req)
}

// unadoptedLostCopy removes n's copy of key, as "delete unadopted" did
// before #245 on the node that served it: the table keeps the key alive,
// the other members keep their copy.
func unadoptedLostCopy(t *testing.T, n *testNode, key string) {
	t.Helper()
	require.NoError(t, os.RemoveAll(filepath.Join(n.r.root, filepath.FromSlash(key))))
	require.False(t, dirExists(n, key))
}

// requireOrphanKept checks that every member still holds the orphan as it
// was: its main branch at sha, its ID, alive.
func requireOrphanKept(t *testing.T, tc *testCluster, key, sha string, id int64) {
	t.Helper()
	for _, n := range tc.nodes {
		v, ok := n.r.table.Get(key, "refs/heads/main")
		require.True(t, ok && v.Live(), "%s: %s keeps its main branch", n.slot, key)
		assert.Equal(t, sha, v.SHA, n.slot)
		_, deleted := n.r.table.Deleted(key)
		assert.False(t, deleted, "%s: %s deleted", n.slot, key)
		_, got, _ := n.r.table.RefsWithID(key)
		assert.Equal(t, id, got, "%s: %s keeps its ID", n.slot, key)
	}
}

// Site administration > Repositories > Unadopted > Delete (and DELETE
// /api/v1/admin/unadopted/{owner}/{repo}, and a user's own settings when
// ALLOW_DELETION_OF_UNADOPTED_REPOSITORIES) on an orphan removed the
// serving node's directory only. The README said "the projector brings
// them back"; it did not, nothing marked the key dirty. The name looked
// free on that node only, and a repository created there under it was
// accepted by the cluster into the orphan's live key: it inherited the
// deleted repository's refs and objects, and, the key keeping the deleted
// repository's ID, every node refused the new repository for good (#245).
// The deletion is now refused while the cluster holds the repository, a
// creation under a name the cluster holds is refused before anything is
// written, and `forgejo cluster reproject` gives a node that lost its copy
// the repository back.
func TestUnadoptedDeleteThenCreateWithID(t *testing.T) {
	require.NoError(t, unittest.PrepareTestDatabase())
	tc := newTestCluster(t, "node-a", "node-b", "node-c")
	origin := tc.nodes[0]
	withGitThroughReplicator(t, origin)
	ctx := db.DefaultContext
	owner := unittest.AssertExistsAndLoadBean(t, &user_model.User{ID: 4})
	admin := unittest.AssertExistsAndLoadBean(t, &user_model.User{ID: 1})

	old, err := repo_service.CreateRepositoryDirectly(ctx, owner, owner, repo_service.CreateRepoOptions{Name: "orphaned", AutoInit: true, Readme: "Default", DefaultBranch: "main", IsPrivate: true})
	require.NoError(t, err)
	key := "user4/orphaned.git"
	requireLiveEverywhere(t, tc, key)
	initMain, ok := origin.r.table.Get(key, "refs/heads/main")
	require.True(t, ok)
	// The owner pushes a private commit through Forgejo's HTTP or SSH path,
	// which carries the repository's ID into the table (the auto-init push
	// does too in production).
	work := t.TempDir()
	run(t, work, "init", "-q")
	private := commit(t, work, "private work of the deleted repository")
	unadoptedWrite(t, origin, work, key, "refs/heads/main", initMain.SHA, private, old.ID, 2)
	for _, n := range tc.nodes {
		require.Eventually(t, func() bool { return gitRef(t, n, key, "refs/heads/main") == private }, 15*time.Second, 20*time.Millisecond, n.slot)
	}
	_, oldID, _ := origin.r.table.RefsWithID(key)
	t.Logf("old repository id=%d, table id=%d, main=%s", old.ID, oldID, private)
	require.Equal(t, old.ID, oldID)

	unadoptedOrphan(t, tc, old.ID, key)
	unadoptedState(t, tc, key, "refs/heads/main", "orphan")

	// The admin deletes the unadopted files, through node-a: refused, the
	// cluster holds them on every member.
	err = repo_service.DeleteUnadoptedRepository(ctx, admin, owner, "orphaned")
	t.Logf("DeleteUnadoptedRepository through node-a: %v", err)
	require.ErrorContains(t, err, "--delete-orphan user4/orphaned", "the deletion of the files of a repository the cluster holds is refused, the right procedure named")
	unadoptedState(t, tc, key, "refs/heads/main", "after DeleteUnadoptedRepository on node-a")
	for _, n := range tc.nodes {
		assert.True(t, dirExists(n, key), "%s: its copy of the orphan was removed", n.slot)
	}
	requireOrphanKept(t, tc, key, private, old.ID)

	// A node that lost its copy, as node-a did through the deletion before
	// #245: the name looks free on its disk alone.
	if dirExists(origin, key) {
		unadoptedLostCopy(t, origin, key)
	}
	fresh, err := repo_service.CreateRepositoryDirectly(ctx, owner, owner, repo_service.CreateRepoOptions{Name: "orphaned"})
	if err == nil {
		time.Sleep(3 * time.Second)
		for _, n := range tc.nodes {
			settle(t, n)
		}
		unadoptedState(t, tc, key, "refs/heads/main", "after the creation on node-a")
		for _, n := range tc.nodes {
			if g := gitRef(t, n, key, "refs/heads/main"); g == private {
				t.Errorf("%s: the new repository %d holds the deleted repository's main %s", n.slot, fresh.ID, g)
			}
			if err := n.r.ReadRefusal(key, fresh.ID); err != nil {
				t.Errorf("%s: the new repository %d is refused: %v", n.slot, fresh.ID, err)
			}
		}
		t.Fatalf("a repository was created through node-a under the key the cluster holds for the orphan: id=%d", fresh.ID)
	}
	t.Logf("creation through node-a refused: %v", err)
	assert.True(t, repo_model.IsErrRepoFilesAlreadyExist(err), "the files exist in the cluster: %v", err)
	unittest.AssertNotExistsBean(t, &repo_model.Repository{OwnerID: owner.ID, LowerName: "orphaned"})
	requireOrphanKept(t, tc, key, private, old.ID)
	assert.False(t, dirExists(origin, key), "the refused creation wrote nothing on node-a")

	// `forgejo cluster reproject` on node-a gives it the orphan back, from
	// the other members, for the adoption or `reconcile --delete-orphan`.
	resp := origin.r.Handle(ctx, &clusterhook.Request{Op: clusterhook.OpReproject, Repo: key})
	require.True(t, resp.OK, "reproject of a repository node-a lost: %s", resp.Message)
	assert.JSONEq(t, `{"matched":false}`, string(resp.Data))
	require.Eventually(t, func() bool { return gitRef(t, origin, key, "refs/heads/main") == private }, 15*time.Second, 20*time.Millisecond, "node-a projects the orphan again")
	settle(t, origin)
	assert.True(t, hasObject(origin, key, private))
	resp = origin.r.Handle(ctx, &clusterhook.Request{Op: clusterhook.OpReproject, Repo: key})
	require.True(t, resp.OK, resp.Message)
	assert.JSONEq(t, `{"matched":true}`, string(resp.Data))
}

// The same with an orphan whose writes never carried the repository's ID,
// as a pull mirror's: the new repository was served everywhere with the
// deleted private repository's refs and objects (#245).
func TestUnadoptedDeleteThenCreateWithoutID(t *testing.T) {
	require.NoError(t, unittest.PrepareTestDatabase())
	tc := newTestCluster(t, "node-a", "node-b", "node-c")
	origin := tc.nodes[0]
	withGitThroughReplicator(t, origin)
	ctx := db.DefaultContext
	owner := unittest.AssertExistsAndLoadBean(t, &user_model.User{ID: 4})
	admin := unittest.AssertExistsAndLoadBean(t, &user_model.User{ID: 1})

	old, err := repo_service.CreateRepositoryDirectly(ctx, owner, owner, repo_service.CreateRepoOptions{Name: "mirrored", IsPrivate: true})
	require.NoError(t, err)
	key := "user4/mirrored.git"
	requireLiveEverywhere(t, tc, key)
	work := t.TempDir()
	run(t, work, "init", "-q")
	secret := commit(t, work, "private history of the mirrored repository")
	unadoptedWrite(t, origin, work, key, "refs/heads/main", strings.Repeat("0", 40), secret, 0, 0)
	for _, n := range tc.nodes {
		require.Eventually(t, func() bool { return gitRef(t, n, key, "refs/heads/main") == secret }, 15*time.Second, 20*time.Millisecond, n.slot)
	}
	_, oldID, _ := origin.r.table.RefsWithID(key)
	t.Logf("old repository id=%d, table id=%d, main=%s", old.ID, oldID, secret)
	require.Zero(t, oldID)

	unadoptedOrphan(t, tc, old.ID, key)
	err = repo_service.DeleteUnadoptedRepository(ctx, admin, owner, "mirrored")
	t.Logf("DeleteUnadoptedRepository through node-a: %v", err)
	require.ErrorContains(t, err, "--delete-orphan user4/mirrored")
	unadoptedState(t, tc, key, "refs/heads/main", "after DeleteUnadoptedRepository on node-a")
	if dirExists(origin, key) {
		unadoptedLostCopy(t, origin, key)
	}

	fresh, err := repo_service.CreateRepositoryDirectly(ctx, owner, owner, repo_service.CreateRepoOptions{Name: "mirrored", IsPrivate: false})
	if err == nil {
		time.Sleep(3 * time.Second)
		for _, n := range tc.nodes {
			settle(t, n)
		}
		unadoptedState(t, tc, key, "refs/heads/main", "after the creation on node-a")
		for _, n := range tc.nodes {
			if hasObject(n, key, secret) && n.r.ReadRefusal(key, fresh.ID) == nil {
				t.Errorf("%s: the new public repository %d serves the deleted private repository's commit %s (main=%q)", n.slot, fresh.ID, secret, gitRef(t, n, key, "refs/heads/main"))
			}
		}
		t.Fatalf("a public repository was created through node-a under the key the cluster holds for the private orphan: id=%d", fresh.ID)
	}
	t.Logf("creation through node-a refused: %v", err)
	assert.True(t, repo_model.IsErrRepoFilesAlreadyExist(err), "%v", err)
	unittest.AssertNotExistsBean(t, &repo_model.Repository{OwnerID: owner.ID, LowerName: "mirrored"})
	requireOrphanKept(t, tc, key, secret, 0)
}

// gitInitHEAD plays git init's HEAD in dir through n's hook, as `git init`
// writes it, and ends the transaction as git does.
func gitInitHEAD(t *testing.T, n *testNode, dir string) *clusterhook.Response {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	req := &clusterhook.Request{
		Op: clusterhook.OpPrepared, Nonce: clusterhook.NewNonce(), GitDir: dir,
		Lines: []clusterhook.Line{{Old: strings.Repeat("0", 40), New: "ref:refs/heads/main", Ref: "HEAD"}},
	}
	resp := n.r.Handle(ctx, req)
	req.Op = clusterhook.OpAborted
	if resp.OK {
		req.Op = clusterhook.OpCommitted
	}
	n.r.Handle(ctx, req)
	return resp
}

// git init's HEAD, written in a directory that holds no ref, under a key
// whose refs the table holds alive: a repository created over an orphan,
// or over a repository the node lost its copy of, by a path that did not
// ask the cluster first. The proposer took it as a creation, Apply
// accepted it into the live key, and the new repository inherited the
// refs and objects of another one (#245). It is refused before it is
// proposed. A key holding HEAD alone still takes it: a migration clones
// into the directory `git init` made, after removing it.
func TestUnadoptedGitInitOverTheRefsOfAKeyIsRefused(t *testing.T) {
	tc := newTestCluster(t, "node-a", "node-b", "node-c")
	a := tc.nodes[0]
	work := t.TempDir()
	run(t, work, "init", "-q")
	c1 := commit(t, work, "one")
	key := "user4/held.git"
	require.True(t, tc.pushVia(a, work, key, "refs/heads/main", strings.Repeat("0", 40), c1).OK)
	waitAll(t, tc, func(n *testNode) bool { return gitRef(t, n, key, "refs/heads/main") == c1 })
	settle(t, a)

	dir := filepath.Join(a.r.root, "user4", "held.git")
	unadoptedLostCopy(t, a, key)
	run(t, a.r.root, "init", "-q", "--bare", dir)
	before, _ := a.r.table.Applied()
	resp := gitInitHEAD(t, a, dir)
	t.Logf("git init's HEAD over %s through node-a: ok=%v %s", key, resp.OK, resp.Message)
	assert.False(t, resp.OK, "a git init took a key whose refs the table holds alive")
	assert.Contains(t, resp.Message, "refs/heads/main")
	after, _ := a.r.table.Applied()
	if !resp.OK {
		assert.Equal(t, before.Index, after.Index, "nothing was proposed")
	}
	for _, n := range tc.nodes {
		_, ok := n.r.table.Get(key, "HEAD")
		assert.False(t, ok, "%s: the key took git init's HEAD", n.slot)
	}

	fresh := "user4/fresh.git"
	fdir := filepath.Join(a.r.root, "user4", "fresh.git")
	run(t, a.r.root, "init", "-q", "--bare", fdir)
	resp = gitInitHEAD(t, a, fdir)
	require.True(t, resp.OK, resp.Message)
	require.NoError(t, os.RemoveAll(fdir))
	run(t, a.r.root, "init", "-q", "--bare", fdir)
	resp = gitInitHEAD(t, a, fdir)
	require.True(t, resp.OK, "git init's HEAD again, on a key holding HEAD alone: %s", resp.Message)
	v, ok := a.r.table.Get(fresh, "HEAD")
	require.True(t, ok)
	assert.Equal(t, "refs/heads/main", v.Symbolic)
}

// Files under a name the cluster does not hold, on one node's disk only
// (an init interrupted before the cluster recorded anything), are still
// deleted from the unadopted list: there is nothing to replicate, and no
// member projects them.
func TestUnadoptedDeleteOfFilesTheClusterDoesNotHold(t *testing.T) {
	require.NoError(t, unittest.PrepareTestDatabase())
	tc := newTestCluster(t, "node-a", "node-b", "node-c")
	origin := tc.nodes[0]
	withGitThroughReplicator(t, origin)
	ctx := db.DefaultContext
	owner := unittest.AssertExistsAndLoadBean(t, &user_model.User{ID: 4})
	admin := unittest.AssertExistsAndLoadBean(t, &user_model.User{ID: 1})

	key := "user4/debris.git"
	run(t, origin.r.root, "init", "-q", "--bare", filepath.Join(origin.r.root, "user4", "debris.git"))
	_, known := origin.r.table.Get(key, "HEAD")
	require.False(t, known)
	require.NoError(t, repo_service.DeleteUnadoptedRepository(ctx, admin, owner, "debris"))
	assert.False(t, dirExists(origin, key))
}

// A creation, a fork and a generation from a template under a name the
// table holds a repository under, its own or its wiki's, on a node whose
// disk lost the copy: refused before anything is written, as for files on
// the disk, and the key is left as it was. A name the table does not hold
// is created as before.
func TestUnadoptedNameRefusesEveryCreation(t *testing.T) {
	require.NoError(t, unittest.PrepareTestDatabase())
	tc := newTestCluster(t, "node-a", "node-b", "node-c")
	origin := tc.nodes[0]
	work := t.TempDir()
	run(t, work, "init", "-q")
	c1 := commit(t, work, "one")
	zero := strings.Repeat("0", 40)
	for _, key := range []string{"user4/taken.git", "user4/wikitaken.wiki.git"} {
		require.True(t, tc.pushVia(origin, work, key, "refs/heads/main", zero, c1).OK, key)
		waitAll(t, tc, func(n *testNode) bool { return gitRef(t, n, key, "refs/heads/main") == c1 })
	}
	settle(t, origin)
	unadoptedLostCopy(t, origin, "user4/taken.git")
	unadoptedLostCopy(t, origin, "user4/wikitaken.wiki.git")
	withGitThroughReplicator(t, origin)
	ctx := db.DefaultContext
	owner := unittest.AssertExistsAndLoadBean(t, &user_model.User{ID: 4})
	base := unittest.AssertExistsAndLoadBean(t, &repo_model.Repository{ID: 1})
	require.NoError(t, base.LoadOwner(ctx))
	tpl := unittest.AssertExistsAndLoadBean(t, &repo_model.Repository{ID: 44})
	require.True(t, tpl.IsTemplate)
	before, _ := origin.r.table.Applied()

	_, err := repo_service.CreateRepositoryDirectly(ctx, owner, owner, repo_service.CreateRepoOptions{Name: "taken"})
	assert.True(t, repo_model.IsErrRepoFilesAlreadyExist(err), "creation: %v", err)
	_, err = repo_service.ForkRepositoryIfNotExists(ctx, owner, owner, repo_service.ForkRepoOptions{BaseRepo: base, Name: "taken"})
	assert.True(t, repo_model.IsErrRepoFilesAlreadyExist(err), "fork: %v", err)
	_, err = repo_service.GenerateRepository(ctx, owner, owner, tpl, repo_service.GenerateRepoOptions{Name: "taken", GitContent: true})
	assert.True(t, repo_model.IsErrRepoFilesAlreadyExist(err), "generation: %v", err)
	_, err = repo_service.CreateRepositoryDirectly(ctx, owner, owner, repo_service.CreateRepoOptions{Name: "wikitaken"})
	assert.True(t, repo_model.IsErrRepoFilesAlreadyExist(err), "creation beside a wiki the table holds: %v", err)

	after, _ := origin.r.table.Applied()
	assert.Equal(t, before.Index, after.Index, "nothing was proposed")
	for _, name := range []string{"taken", "wikitaken"} {
		unittest.AssertNotExistsBean(t, &repo_model.Repository{OwnerID: owner.ID, LowerName: name})
		assert.False(t, dirExists(origin, "user4/"+name+".git"), "%s: a directory was written", name)
	}
	for _, n := range tc.nodes {
		for _, key := range []string{"user4/taken.git", "user4/wikitaken.wiki.git"} {
			v, ok := n.r.table.Get(key, "refs/heads/main")
			assert.True(t, ok && v.Live() && v.SHA == c1, "%s: %s changed", n.slot, key)
		}
	}

	_, err = repo_service.CreateRepositoryDirectly(ctx, owner, owner, repo_service.CreateRepoOptions{Name: "free"})
	require.NoError(t, err)
	requireLiveEverywhere(t, tc, "user4/free.git")
}

// A creation that reaches `git init` under a key whose refs the table
// holds, by a path that did not ask the cluster first: the proposer
// refuses git init's HEAD, and the failed creation's cleanup removes the
// directory `git init` began on this node only. It deleted the key from
// the table, the orphan's refs and its files on every member with it. The
// aborted transaction marks the key for a reconciliation, and the node
// projects the orphan again where `git init` began.
func TestUnadoptedCreationRefusedAtGitInitKeepsTheOrphan(t *testing.T) {
	require.NoError(t, unittest.PrepareTestDatabase())
	tc := newTestCluster(t, "node-a", "node-b", "node-c")
	origin := tc.nodes[0]
	withGitThroughReplicator(t, origin)
	ctx := db.DefaultContext
	owner := unittest.AssertExistsAndLoadBean(t, &user_model.User{ID: 4})

	old, err := repo_service.CreateRepositoryDirectly(ctx, owner, owner, repo_service.CreateRepoOptions{Name: "bypassed", AutoInit: true, Readme: "Default", DefaultBranch: "main", IsPrivate: true})
	require.NoError(t, err)
	key := "user4/bypassed.git"
	requireLiveEverywhere(t, tc, key)
	main, ok := origin.r.table.Get(key, "refs/heads/main")
	require.True(t, ok)
	head, ok := origin.r.table.Get(key, "HEAD")
	require.True(t, ok)
	unadoptedOrphan(t, tc, old.ID, key)
	_, id, _ := origin.r.table.RefsWithID(key)
	unadoptedLostCopy(t, origin, key)

	// A path that does not ask the cluster before it writes.
	clusterhook.SetRepoHeldCheck(nil)
	_, err = repo_service.CreateRepositoryDirectly(ctx, owner, owner, repo_service.CreateRepoOptions{Name: "bypassed"})
	t.Logf("creation through node-a, the cluster not asked first: %v", err)
	require.Error(t, err)
	assert.True(t, clusterhook.IsKeyHeld(err), "%v", err)
	unittest.AssertNotExistsBean(t, &repo_model.Repository{OwnerID: owner.ID, LowerName: "bypassed"})
	time.Sleep(time.Second)
	requireOrphanKept(t, tc, key, main.SHA, id)
	for _, n := range tc.nodes {
		v, _ := n.r.table.Get(key, "HEAD")
		assert.Equal(t, head, v, "%s: HEAD changed", n.slot)
	}
	for _, n := range tc.nodes {
		require.Eventually(t, func() bool { return gitRef(t, n, key, "refs/heads/main") == main.SHA }, 15*time.Second, 20*time.Millisecond, "%s holds the orphan", n.slot)
		settle(t, n)
		assert.Zero(t, setAsideCount(t, n), "%s: a directory was set aside", n.slot)
	}
}

// The check of a creation on a node that cannot confirm it applied the
// log refuses it, without naming the name held.
func TestUnadoptedCheckOnANodeThatCannotConfirm(t *testing.T) {
	defer test.MockVariableValue(&lifecycleResolveBudget, 300*time.Millisecond)()
	r := newReplicator(t.TempDir(), newRegistry(0, 1, 1))
	eng := &unknownEngine{table: r.table}
	r.setEngine(eng)
	err := r.heldRefusal(context.Background(), []string{"o/a.git", "o/a.wiki.git"})
	require.ErrorContains(t, err, "could not confirm it applied the log")
	require.NotErrorIs(t, err, clusterhook.ErrRepoHeld)
	assert.Zero(t, eng.index, "nothing was proposed")
}
