// Copyright 2026 The Forgejo Authors. All rights reserved.
// SPDX-License-Identifier: GPL-3.0-or-later

package cluster

import (
	"bufio"
	"context"
	"fmt"
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
	"forgejo.org/modules/git"
	"forgejo.org/modules/setting"
	"forgejo.org/modules/test"
	repo_service "forgejo.org/services/repository"

	"code.forgejo.org/xorm/xorm/contexts"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	testHookSocketEnv = "FORGEJO_CLUSTER_TEST_HOOK_SOCKET"
	testHookRootEnv   = "FORGEJO_CLUSTER_TEST_HOOK_ROOT"
)

// runTestHook is what `forgejo hook reference-transaction` does, run by the
// test binary: one phase of a git ref transaction handed to the replicator
// over its socket, failing closed in "prepared".
func runTestHook(sock string) int {
	if len(os.Args) < 2 || os.Args[1] == "preparing" {
		return 0
	}
	state := os.Args[1]
	gitDir := os.Getenv("GIT_DIR")
	wd, _ := os.Getwd()
	if gitDir == "" {
		gitDir = wd
	}
	if !filepath.IsAbs(gitDir) {
		gitDir = filepath.Join(wd, gitDir)
	}
	if real, err := filepath.EvalSymlinks(gitDir); err == nil {
		gitDir = real
	}
	if rel, err := filepath.Rel(os.Getenv(testHookRootEnv), gitDir); err != nil || strings.HasPrefix(rel, "..") {
		return 0
	}
	req := &clusterhook.Request{Op: state, Nonce: os.Getenv(clusterhook.EnvTxn), GitDir: gitDir, PID: os.Getppid(), CreateOnly: clusterhook.CreateOnlyFromEnv()}
	scanner := bufio.NewScanner(os.Stdin)
	for scanner.Scan() {
		if f := strings.Fields(scanner.Text()); len(f) == 3 {
			req.Lines = append(req.Lines, clusterhook.Line{Old: f[0], New: f[1], Ref: f[2]})
		}
	}
	if len(req.Lines) == 0 {
		return 0
	}
	ctx, cancel := context.WithTimeout(context.Background(), clusterhook.DefaultTimeout)
	defer cancel()
	resp, err := clusterhook.Call(ctx, sock, req)
	if state != clusterhook.OpPrepared {
		return 0
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, "cluster:", err)
		return 1
	}
	if !resp.OK {
		fmt.Fprintln(os.Stderr, "cluster:", resp.Message)
		return 1
	}
	return 0
}

// withGitThroughReplicator runs every git command of Forgejo through node
// n's replicator, as a node of a cluster does: the global hooks path gets
// the reference-transaction hook, Forgejo's git commands carry a nonce,
// deletions and renames go to the table, and the repository root is n's.
func withGitThroughReplicator(t *testing.T, n *testNode) {
	sockDir, err := os.MkdirTemp("", "fjh") //nolint:usetesting // a unix socket path must stay short: 104 bytes on macOS
	require.NoError(t, err)
	t.Cleanup(func() { _ = os.RemoveAll(sockDir) })
	sock := filepath.Join(sockDir, "s")
	require.NoError(t, n.r.serveHookSocket(sock))

	bin, err := os.Executable()
	require.NoError(t, err)
	hooks := filepath.Join(git.HomeDir(), "hooks")
	require.NoError(t, os.MkdirAll(hooks, 0o755))
	hook := filepath.Join(hooks, "reference-transaction")
	_, err = os.Stat(hook)
	require.True(t, os.IsNotExist(err), "a reference-transaction hook is already installed")
	script := fmt.Sprintf("#!/bin/sh\n%s='%s' %s='%s' exec '%s' \"$@\"\n", testHookSocketEnv, sock, testHookRootEnv, n.r.root, bin)
	require.NoError(t, os.WriteFile(hook, []byte(script), 0o755))
	t.Cleanup(func() { _ = os.Remove(hook) })

	clusterhook.SetEnabled(true)
	clusterhook.SetRepoLifecycle(n.r.proposeRepoDelete, n.r.proposeRepoMoves)
	clusterhook.SetRepoHeldCheck(n.r.heldRefusal)
	t.Cleanup(func() {
		clusterhook.SetEnabled(false)
		clusterhook.SetRepoLifecycle(nil, nil)
		clusterhook.SetRepoHeldCheck(nil)
	})
	t.Cleanup(test.MockVariableValue(&setting.RepoRootPath, n.r.root))
}

// A fork in a cluster: the fork's row is not committed while git writes
// its refs, so the other members know its key from the table alone. A
// `git clone` proposes every ref first, and the members refuse the objects
// of a key they do not know: the fork failed every time (R-D3). HEAD alone
// goes first, then the refs, and every member holds the fork.
func TestForkInACluster(t *testing.T) {
	require.NoError(t, unittest.PrepareTestDatabase())
	tc := newTestCluster(t, "node-a", "node-b", "node-c")
	for _, n := range tc.nodes {
		n.r.objects.repoKnown = n.r.repoKnown
	}
	origin := tc.nodes[0]
	// The base repository is on the origin, where the fixtures are.
	require.NoError(t, unittest.CopyDir(filepath.Join(setting.RepoRootPath, "user2", "repo1.git"), filepath.Join(origin.r.root, "user2", "repo1.git")))
	withGitThroughReplicator(t, origin)

	ctx := db.DefaultContext
	base := unittest.AssertExistsAndLoadBean(t, &repo_model.Repository{ID: 1})
	require.NoError(t, base.LoadOwner(ctx))
	owner := unittest.AssertExistsAndLoadBean(t, &user_model.User{ID: 4})
	_, err := repo_service.ForkRepositoryIfNotExists(ctx, owner, owner, repo_service.ForkRepoOptions{BaseRepo: base, Name: "repo1-fork", Description: "fork"})
	require.NoError(t, err)

	key := "user4/repo1-fork.git"
	want, err := readGitRefs(ctx, filepath.Join(origin.r.root, "user2", "repo1.git"))
	require.NoError(t, err)
	require.NotEmpty(t, want)
	for _, n := range tc.nodes {
		head, ok := n.r.table.Get(key, "HEAD")
		require.True(t, ok, "%s: HEAD in the table", n.slot)
		assert.Equal(t, "refs/heads/"+base.DefaultBranch, head.Symbolic, n.slot)
		for ref, v := range want {
			if !strings.HasPrefix(ref, "refs/heads/") && !strings.HasPrefix(ref, "refs/tags/") {
				continue
			}
			got, ok := n.r.table.Get(key, ref)
			require.True(t, ok, "%s: %s in the table", n.slot, ref)
			assert.Equal(t, v.SHA, got.SHA, "%s: %s", n.slot, ref)
			require.Eventually(t, func() bool { return gitRef(t, n, key, ref) == v.SHA }, 10*time.Second, 20*time.Millisecond,
				"%s: %s projected", n.slot, ref)
		}
	}
	_, err = os.Stat(filepath.Join(origin.r.root, "user4", "repo1-fork.git", "FETCH_HEAD"))
	assert.True(t, os.IsNotExist(err), "no FETCH_HEAD left behind")
}

// forkCountProbe runs seen when the fork count of a base repository is
// incremented, while on is set.
type forkCountProbe struct {
	on   atomic.Bool
	seen func()
}

func (p *forkCountProbe) BeforeProcess(c *contexts.ContextHook) (context.Context, error) {
	if p.on.Load() && strings.Contains(c.SQL, "num_forks=num_forks+1") {
		p.seen()
	}
	return c.Ctx, nil
}

func (p *forkCountProbe) AfterProcess(*contexts.ContextHook) error { return nil }

// A fork counts itself on its base only once its refs are copied: the
// update locks the base repository's row until the fork's transaction
// commits, and made first it held it during the whole copy, which in a
// cluster sends the fork's objects to a majority. A push of the base then
// waited on that row in the leader's log consumer, and the SQL of every
// repository behind it (#177).
func TestAForkCountsItselfOnItsBaseOnlyOnceCopied(t *testing.T) {
	require.NoError(t, unittest.PrepareTestDatabase())
	tc := newTestCluster(t, "node-a", "node-b", "node-c")
	for _, n := range tc.nodes {
		n.r.objects.repoKnown = n.r.repoKnown
	}
	origin := tc.nodes[0]
	require.NoError(t, unittest.CopyDir(filepath.Join(setting.RepoRootPath, "user2", "repo1.git"), filepath.Join(origin.r.root, "user2", "repo1.git")))
	withGitThroughReplicator(t, origin)

	ctx := db.DefaultContext
	base := unittest.AssertExistsAndLoadBean(t, &repo_model.Repository{ID: 1})
	require.NoError(t, base.LoadOwner(ctx))
	owner := unittest.AssertExistsAndLoadBean(t, &user_model.User{ID: 4})
	want, err := readGitRefs(ctx, filepath.Join(origin.r.root, "user2", "repo1.git"))
	require.NoError(t, err)
	key := "user4/repo1-fork.git"
	var copied []string
	counted := 0
	probe := &forkCountProbe{seen: func() {
		counted++
		for ref := range want {
			if !strings.HasPrefix(ref, "refs/heads/") && !strings.HasPrefix(ref, "refs/tags/") {
				continue
			}
			if v, ok := origin.r.table.Get(key, ref); ok && v.Live() {
				copied = append(copied, ref)
			}
		}
	}}
	eng, err := db.GetMasterEngine(db.GetEngine(ctx))
	require.NoError(t, err)
	eng.AddHook(probe)
	probe.on.Store(true)
	t.Cleanup(func() { probe.on.Store(false) })

	_, err = repo_service.ForkRepositoryIfNotExists(ctx, owner, owner, repo_service.ForkRepoOptions{BaseRepo: base, Name: "repo1-fork", Description: "fork"})
	require.NoError(t, err)
	probe.on.Store(false)
	require.Equal(t, 1, counted, "the base counts the fork once")
	var refs []string
	for ref := range want {
		if strings.HasPrefix(ref, "refs/heads/") || strings.HasPrefix(ref, "refs/tags/") {
			refs = append(refs, ref)
		}
	}
	require.NotEmpty(t, refs)
	assert.ElementsMatch(t, refs, copied, "every ref of the fork was copied when the base counted it")
	unittest.AssertExistsAndLoadBean(t, &repo_model.Repository{ID: 1, NumForks: base.NumForks + 1})
}

// forkBase loads the repository id as RepoAssignment does at the start of
// a fork's request, before the fork reads anything.
func forkBase(t *testing.T, id int64) *repo_model.Repository {
	t.Helper()
	base := unittest.AssertExistsAndLoadBean(t, &repo_model.Repository{ID: id})
	require.NoError(t, base.LoadOwner(db.DefaultContext))
	return base
}

// forkFor forks base for the user ownerID under name, as the web form and
// the API do once RepoAssignment loaded the base.
func forkFor(t *testing.T, base *repo_model.Repository, ownerID int64, name string) (*repo_model.Repository, error) {
	t.Helper()
	owner := unittest.AssertExistsAndLoadBean(t, &user_model.User{ID: ownerID})
	return repo_service.ForkRepositoryIfNotExists(db.DefaultContext, owner, owner, repo_service.ForkRepoOptions{BaseRepo: base, Name: name, Description: "fork"})
}

// forkNeverWritten checks that a refused fork left no row, and that no
// member ever held its key: no entry in the table, live or deleted, and no
// directory. The fork reads its base before anything of it is written, so
// a refusal has nothing to take back from the cluster (#283).
func forkNeverWritten(t *testing.T, tc *testCluster, owner *user_model.User, name string) {
	t.Helper()
	unittest.AssertNotExistsBean(t, &repo_model.Repository{OwnerID: owner.ID, LowerName: name})
	key := clusterhook.RepoKey(owner.Name, name, false)
	for _, n := range tc.nodes {
		refs, _ := n.r.table.Refs(key)
		assert.Empty(t, refs, "%s: %s has entries in the table", n.slot, key)
		_, deleted := n.r.table.Deleted(key)
		assert.False(t, deleted, "%s: %s was written, then deleted", n.slot, key)
		assert.False(t, dirExists(n, key), "%s: %s was created", n.slot, key)
	}
}

// forkServed logs what every member serves as ref of a fork that was
// expected to be refused, fork, nil when it was.
func forkServed(t *testing.T, tc *testCluster, fork *repo_model.Repository, ref string) {
	t.Helper()
	if fork == nil {
		return
	}
	key := clusterhook.RepoKey(fork.OwnerName, fork.Name, false)
	for _, n := range tc.nodes {
		require.Eventually(t, func() bool { return gitRef(t, n, key, ref) != "" }, 15*time.Second, 20*time.Millisecond, n.slot)
		t.Logf("%s: %s %s = %.10s", n.slot, key, ref, gitRef(t, n, key, ref))
	}
}

// The base's main moves from c2 to c3 through node-b, acknowledged.
// node-a has applied it, its git still shows c2. A fork of the base served
// by node-a listed and fetched the base's refs from node-a's git, without
// waiting for node-a to serve the base: the fork started from c2, on every
// member and for good (#283, as #261 for a generation). It now waits for
// node-a to serve the base, and is refused, before anything is written,
// while node-a does not; once node-a caught up, the fork holds c3.
func TestForkFromALaggingNodeKeepsAnAcknowledgedPush(t *testing.T) {
	tc, a, b, work, _, c2, hold := laggingCluster(t)
	for _, n := range tc.nodes {
		n.r.objects.repoKnown = n.r.repoKnown
	}
	c3 := commit(t, work, "three")
	pushOnB(t, tc, a, b, work, "refs/heads/main", c2, c3)
	tplGenServe(t, a)
	_, err := db.GetEngine(db.DefaultContext).ID(1).Cols("default_branch").Update(&repo_model.Repository{DefaultBranch: "main"})
	require.NoError(t, err)
	owner := unittest.AssertExistsAndLoadBean(t, &user_model.User{ID: 4})

	t.Run("node-a waits to serve the base", func(t *testing.T) {
		fork, err := forkFor(t, forkBase(t, 1), owner.ID, "lagging-fork")
		forkServed(t, tc, fork, "refs/heads/main")
		require.ErrorIs(t, err, clusterhook.ErrBehind)
		forkNeverWritten(t, tc, owner, "lagging-fork")
	})

	t.Logf("acknowledged c3=%.10s, c2=%.10s", c3, c2)
	caughtUp(t, a, hold, "refs/heads/main", c3)
	fork, err := forkFor(t, forkBase(t, 1), 5, "caught-up-fork")
	require.NoError(t, err)
	key := clusterhook.RepoKey(fork.OwnerName, fork.Name, false)
	for _, n := range tc.nodes {
		require.Eventually(t, func() bool { return gitRef(t, n, key, "refs/heads/main") == c3 }, 15*time.Second, 20*time.Millisecond,
			"%s: the fork holds the push acknowledged before it was asked", n.slot)
		v, ok := n.r.table.Get(key, "refs/heads/main")
		require.True(t, ok && v.Live(), n.slot)
		assert.Equal(t, c3, v.SHA, n.slot)
	}
}

// A private repository held the name user27/template1 and was renamed
// away (to user27/payroll); the public repository 44 then took the name,
// through node-b, and node-a serves it. A fork of the base read its
// directory on node-a by the name RepoAssignment loaded at the start of
// the request, past the one guard RepoAssignment made, with no check of
// its own: the base renamed away and the private repository renamed under
// its name, projected on node-a meanwhile, put the private history into
// the fork, public as its base, and replicated to every member (#283, as
// #261 for a generation). The fork now reads the base's directory between
// two checks that it holds the base, into a staging copy outside the
// repository root, before anything of the fork is written: a refusal
// leaves nothing on any member.
func TestForkFromALaggingNodeNeverCopiesAnotherRepository(t *testing.T) {
	require.NoError(t, unittest.PrepareTestDatabase())
	tc := newTestCluster(t, "node-a", "node-b", "node-c")
	for _, n := range tc.nodes {
		n.r.objects.repoKnown = n.r.repoKnown
	}
	raisePayload(t, tc)
	a, b := tc.nodes[0], tc.nodes[1]
	zero := strings.Repeat("0", 40)
	const payroll, tmp, moved = "user27/payroll.git", "user27/template-tmp.git", "user27/template-moved.git"

	priv := t.TempDir()
	run(t, priv, "init", "-q")
	p1 := commit(t, priv, "PRIVATE: payroll of the whole company")
	require.True(t, tplGenPushVia(t, b, priv, tplGenTemplate, "refs/heads/master", zero, p1, 900).OK)
	waitAll(t, tc, func(n *testNode) bool { return gitRef(t, n, tplGenTemplate, "refs/heads/master") == p1 })
	settle(t, a)
	renameOn(t, b, tplGenTemplate, payroll)
	tc.createVia(b, tplGenTemplate)
	pub := t.TempDir()
	run(t, pub, "init", "-q")
	t1 := commit(t, pub, "public content")
	resp := tplGenPushVia(t, b, pub, tplGenTemplate, "refs/heads/master", zero, t1, tplGenTemplateID)
	require.True(t, resp.OK, resp.Message)
	waitApplied(t, b, a)
	require.Eventually(t, func() bool { return gitRef(t, a, tplGenTemplate, "refs/heads/master") == t1 }, 15*time.Second, 20*time.Millisecond, "node-a serves the base")
	settle(t, a)

	tplGenServe(t, a)
	t.Logf("base t1=%.10s, private p1=%.10s", t1, p1)
	// Each fork is asked by its own user: a fork wrongly served does not
	// keep the next one from being asked.
	ownerOf := func(id int64) *user_model.User {
		return unittest.AssertExistsAndLoadBean(t, &user_model.User{ID: id})
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
	// around counts node-a's checks of the base's directory, and runs for
	// the n-th one the function given in before right before it, and the
	// one given in after once it passed: renames projected between a check
	// and the read that follows or precedes it.
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

	// RepoAssignment's guard passed, then the base was renamed away and the
	// private repository renamed under its name, both projected on node-a
	// before the fork reads anything.
	t.Run("node-a refuses the repository a rename put under the base's name", func(t *testing.T) {
		base := forkBase(t, tplGenTemplateID)
		require.NoError(t, clusterhook.ReadRefusal(tplGenTemplate, tplGenTemplateID), "RepoAssignment's guard passes")
		moves(t, tplGenTemplate, p1, [2]string{tplGenTemplate, tmp}, [2]string{payroll, tplGenTemplate})
		defer moves(t, tplGenTemplate, t1, [2]string{tplGenTemplate, payroll}, [2]string{tmp, tplGenTemplate})
		owner := ownerOf(4)
		fork, err := forkFor(t, base, owner.ID, "fork-taken")
		forkServed(t, tc, fork, "refs/heads/master")
		require.ErrorIs(t, err, clusterhook.ErrBehind)
		forkNeverWritten(t, tc, owner, "fork-taken")
	})

	// The same renames projected on node-a right after the check that
	// precedes git's read of the base, and undone, the base back under its
	// name, right before the check that follows it: git read the private
	// repository's directory, while the directory and the database show
	// the base before the read and after it. The renames of the key
	// node-a applied meanwhile tell, and nothing of the fork was written.
	t.Run("node-a refuses a read the base's renames went through", func(t *testing.T) {
		base := forkBase(t, tplGenTemplateID)
		swapped := false
		undo := func() {
			moves(t, tplGenTemplate, t1, [2]string{tplGenTemplate, payroll}, [2]string{tmp, tplGenTemplate})
			swapped = false
		}
		checks := around(t, map[int32]func(){
			2: undo,
		}, map[int32]func(){
			1: func() {
				moves(t, tplGenTemplate, p1, [2]string{tplGenTemplate, tmp}, [2]string{payroll, tplGenTemplate})
				swapped = true
			},
		})
		// A fork that never checks again leaves the swap in place.
		defer func() {
			if swapped {
				clusterhook.SetReadGuard(a.r.ReadRefusal)
				undo()
			}
		}()
		owner := ownerOf(5)
		fork, err := forkFor(t, base, owner.ID, "fork-swapped")
		forkServed(t, tc, fork, "refs/heads/master")
		require.ErrorIs(t, err, clusterhook.ErrBehind)
		assert.EqualValues(t, 2, checks.Load(), "checked right before the base is read, then right after")
		forkNeverWritten(t, tc, owner, "fork-swapped")
		require.Equal(t, t1, gitRef(t, a, tplGenTemplate, "refs/heads/master"), "the base is back under its name")
	})

	// The same renames, not undone: the base's directory on node-a holds
	// the private repository when git reads it, and still does after.
	t.Run("node-a refuses a read of the repository that took the base's name", func(t *testing.T) {
		base := forkBase(t, tplGenTemplateID)
		swapped := false
		checks := around(t, nil, map[int32]func(){
			1: func() {
				moves(t, tplGenTemplate, p1, [2]string{tplGenTemplate, moved}, [2]string{payroll, tplGenTemplate})
				swapped = true
			},
		})
		defer func() {
			if swapped {
				clusterhook.SetReadGuard(a.r.ReadRefusal)
				moves(t, tplGenTemplate, t1, [2]string{tplGenTemplate, payroll}, [2]string{moved, tplGenTemplate})
			}
		}()
		owner := ownerOf(8)
		fork, err := forkFor(t, base, owner.ID, "fork-moved")
		forkServed(t, tc, fork, "refs/heads/master")
		require.ErrorIs(t, err, clusterhook.ErrBehind)
		assert.EqualValues(t, 2, checks.Load(), "checked right before the base is read, then right after")
		forkNeverWritten(t, tc, owner, "fork-moved")
	})

	// The base's row no longer gives the name the request loaded: renamed
	// since, another repository may take the name, and then its directory
	// on this node.
	t.Run("node-a refuses a base renamed since it was loaded", func(t *testing.T) {
		base := forkBase(t, tplGenTemplateID)
		eng := db.GetEngine(db.DefaultContext)
		_, err := eng.ID(tplGenTemplateID).Cols("name", "lower_name").Update(&repo_model.Repository{Name: "template-renamed", LowerName: "template-renamed"})
		require.NoError(t, err)
		owner := ownerOf(10)
		fork, err := forkFor(t, base, owner.ID, "fork-renamed")
		_, rerr := eng.ID(tplGenTemplateID).Cols("name", "lower_name").Update(&repo_model.Repository{Name: "template1", LowerName: "template1"})
		require.NoError(t, rerr)
		forkServed(t, tc, fork, "refs/heads/master")
		require.ErrorIs(t, err, clusterhook.ErrBehind)
		forkNeverWritten(t, tc, owner, "fork-renamed")
	})

	fork, err := forkFor(t, forkBase(t, tplGenTemplateID), 2, "fork-public")
	require.NoError(t, err)
	assert.False(t, fork.IsPrivate)
	key := clusterhook.RepoKey(fork.OwnerName, fork.Name, false)
	for _, n := range tc.nodes {
		require.Eventually(t, func() bool { return gitRef(t, n, key, "refs/heads/master") == t1 }, 15*time.Second, 20*time.Millisecond,
			"%s: the fork holds the base", n.slot)
		assert.Equal(t, p1, gitRef(t, n, payroll, "refs/heads/master"), "%s: the private repository is kept under its name", n.slot)
	}
}
