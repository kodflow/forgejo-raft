// Copyright 2026 The Forgejo Authors. All rights reserved.
// SPDX-License-Identifier: GPL-3.0-or-later

package cluster

import (
	"context"
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
	"forgejo.org/modules/clusterhook"
	"forgejo.org/modules/git"
	"forgejo.org/modules/setting"
	repo_service "forgejo.org/services/repository"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Regression tests of git's own transactions through the cluster's hook,
// with a real git (lot F1: #37, #72, #38, #108).

func gitWithNonce(t *testing.T, dir string, args ...string) (string, error) {
	cmd := exec.Command("git", append([]string{"-c", "user.name=t", "-c", "user.email=t@t"}, args...)...)
	cmd.Dir = dir
	cmd.Env = append(os.Environ(), "HOME="+setting.Git.HomePath, clusterhook.EnvTxn+"="+clusterhook.NewNonce())
	out, err := cmd.CombinedOutput()
	return string(out), err
}

func logRefState(t *testing.T, tc *testCluster, repo string, refs ...string) {
	for _, m := range tc.nodes {
		for _, ref := range refs {
			v, ok := m.r.table.Get(repo, ref)
			t.Logf("%s %-22s table(ok=%v tomb=%v rev=%d sha=%.10s) git=%.10q", m.slot, ref, ok, v.Tombstone, v.Rev, v.SHA, gitRef(t, m, repo, ref))
		}
	}
}

// Deleting a packed branch by a real `git push :b` succeeds, as a loose one
// does. The files backend runs a nested transaction on packed-refs first,
// 0->0, which the cluster took for an unchecked deletion: the push was then
// reported failed yet applied on every member (#37).
func TestDeletingAPackedBranch(t *testing.T) {
	tc := newTestCluster(t, "node-a", "node-b", "node-c")
	n := tc.nodes[0]
	work := t.TempDir()
	run(t, work, "init", "-q")
	c1 := commit(t, work, "one")
	zero := strings.Repeat("0", 40)
	repo := "alice/pd.git"
	for _, b := range []string{"main", "packed"} {
		require.True(t, tc.pushVia(n, work, repo, "refs/heads/"+b, zero, c1).OK)
	}
	dir := filepath.Join(n.r.root, filepath.FromSlash(repo))
	run(t, dir, "pack-refs", "--all")
	require.True(t, tc.pushVia(n, work, repo, "refs/heads/loose", zero, c1).OK)
	packed, _ := os.ReadFile(filepath.Join(dir, "packed-refs"))
	t.Logf("packed-refs:\n%s", packed)
	withGitThroughReplicator(t, n)

	out, errLoose := gitWithNonce(t, work, "push", dir, ":refs/heads/loose")
	t.Logf("git push :loose: err=%v\n%s", errLoose, out)
	out, errPacked := gitWithNonce(t, work, "push", dir, ":refs/heads/packed")
	t.Logf("git push :packed: err=%v\n%s", errPacked, out)
	time.Sleep(3 * time.Second)
	logRefState(t, tc, repo, "refs/heads/loose", "refs/heads/packed")

	vl, _ := n.r.table.Get(repo, "refs/heads/loose")
	require.NoError(t, errLoose, "control: a loose deletion succeeds")
	assert.True(t, vl.Tombstone, "control: loose deleted")
	vp, _ := n.r.table.Get(repo, "refs/heads/packed")
	require.NoError(t, errPacked, "a packed deletion succeeds")
	assert.True(t, vp.Tombstone, "packed deleted")
	for _, m := range tc.nodes {
		require.Eventually(t, func() bool { return gitRef(t, m, repo, "refs/heads/packed") == "" }, 10*time.Second, 20*time.Millisecond, m.slot)
	}
}

// git push --atomic :keep c2:topic on packed refs is applied whole: it was
// refused to the client while keep was deleted everywhere (#37).
func TestAnAtomicPushDeletingAPackedBranch(t *testing.T) {
	tc := newTestCluster(t, "node-a", "node-b", "node-c")
	n := tc.nodes[0]
	work := t.TempDir()
	run(t, work, "init", "-q")
	c1 := commit(t, work, "one")
	c2 := commit(t, work, "two")
	zero := strings.Repeat("0", 40)
	repo := "alice/atomic.git"
	require.True(t, tc.pushVia(n, work, repo, "refs/heads/main", zero, c1).OK)
	require.True(t, tc.pushVia(n, work, repo, "refs/heads/keep", zero, c1).OK)
	dir := filepath.Join(n.r.root, filepath.FromSlash(repo))
	run(t, work, "push", "-q", dir, c2+":refs/tmp/incoming")
	run(t, dir, "update-ref", "-d", "refs/tmp/incoming")
	run(t, dir, "pack-refs", "--all")
	withGitThroughReplicator(t, n)

	out, err := gitWithNonce(t, work, "push", "--atomic", dir, ":refs/heads/keep", c2+":refs/heads/topic")
	t.Logf("git push --atomic :keep c2:topic: err=%v\n%s", err, out)
	time.Sleep(3 * time.Second)
	logRefState(t, tc, repo, "refs/heads/keep", "refs/heads/topic")
	v, _ := n.r.table.Get(repo, "refs/heads/keep")
	vt, topic := n.r.table.Get(repo, "refs/heads/topic")
	require.NoError(t, err)
	assert.True(t, v.Tombstone, "keep deleted")
	assert.True(t, topic && vt.SHA == c2, "topic created")
}

// A lagging member deletes a packed branch whose table value a push
// acknowledged elsewhere moved on: the deletion names the old value and is
// refused, the acknowledged value stays (#37).
func TestAPackedDeletionOnALaggingNode(t *testing.T) {
	tc := newTestCluster(t, "node-a", "node-b", "node-c")
	a, b := tc.nodes[0], tc.nodes[1]
	work := t.TempDir()
	run(t, work, "init", "-q")
	c1 := commit(t, work, "one")
	c2 := commit(t, work, "two")
	zero := strings.Repeat("0", 40)
	repo := "alice/lag.git"
	require.True(t, tc.pushVia(a, work, repo, "refs/heads/main", zero, c1).OK)
	require.True(t, tc.pushVia(a, work, repo, "refs/heads/keep", zero, c1).OK)
	dir := filepath.Join(a.r.root, filepath.FromSlash(repo))
	run(t, dir, "pack-refs", "--all")
	require.Eventually(t, func() bool { return gitRef(t, b, repo, "refs/heads/keep") == c1 }, 10*time.Second, 20*time.Millisecond)

	hold := clusterhook.NewNonce()
	require.NoError(t, a.r.registry.register(hold, repo, nil, os.Getpid(), false))
	resp := tc.pushVia(b, work, repo, "refs/heads/keep", c1, c2)
	require.True(t, resp.OK, resp.Message)
	require.Eventually(t, func() bool { v, _ := a.r.table.Get(repo, "refs/heads/keep"); return v.SHA == c2 }, 10*time.Second, 20*time.Millisecond)
	require.Equal(t, c1, gitRef(t, a, repo, "refs/heads/keep"), "node A lags")

	withGitThroughReplicator(t, a)
	out, err := gitWithNonce(t, work, "push", dir, ":refs/heads/keep")
	t.Logf("git push :keep on lagging node-a (git keep=c1, table keep=c2): err=%v\n%s", err, out)
	a.r.registry.release(hold)
	time.Sleep(3 * time.Second)
	logRefState(t, tc, repo, "refs/heads/keep")
	v, _ := a.r.table.Get(repo, "refs/heads/keep")
	require.Error(t, err, "a deletion based on c1 against keep=c2 is refused")
	assert.False(t, v.Tombstone, "a deletion based on c1 erased keep=c2, acknowledged to its pusher")
	assert.Equal(t, c2, v.SHA)
}

// Forgejo's DeleteBranch (`git branch -D`) and RenameBranch on a packed
// branch succeed, and every member holds the result (#37, #38).
func TestBranchDeleteAndRenameOfPackedBranches(t *testing.T) {
	tc := newTestCluster(t, "node-a", "node-b", "node-c")
	n := tc.nodes[0]
	work := t.TempDir()
	run(t, work, "init", "-q")
	c1 := commit(t, work, "one")
	zero := strings.Repeat("0", 40)
	repo := "alice/brd.git"
	for _, b := range []string{"main", "del", "ren"} {
		require.True(t, tc.pushVia(n, work, repo, "refs/heads/"+b, zero, c1).OK)
	}
	dir := filepath.Join(n.r.root, filepath.FromSlash(repo))
	run(t, dir, "pack-refs", "--all")
	for _, m := range tc.nodes {
		require.Eventually(t, func() bool { return gitRef(t, m, repo, "refs/heads/ren") == c1 }, 10*time.Second, 20*time.Millisecond)
	}
	withGitThroughReplicator(t, n)
	g, err := git.OpenRepository(context.Background(), dir)
	require.NoError(t, err)
	defer g.Close()
	errDel := g.DeleteBranch("del", git.DeleteBranchOptions{Force: true})
	t.Logf("DeleteBranch(del) packed: %v", errDel)
	errRen := g.RenameBranch("ren", "ren-new")
	t.Logf("RenameBranch(ren, ren-new) packed: %v", errRen)
	time.Sleep(3 * time.Second)
	logRefState(t, tc, repo, "refs/heads/del", "refs/heads/ren", "refs/heads/ren-new")
	require.NoError(t, errDel)
	require.NoError(t, errRen)
	vd, _ := n.r.table.Get(repo, "refs/heads/del")
	assert.True(t, vd.Tombstone, "del deleted")
	vr, _ := n.r.table.Get(repo, "refs/heads/ren")
	assert.True(t, vr.Tombstone, "ren renamed away")
	for _, m := range tc.nodes {
		require.Eventually(t, func() bool {
			return gitRef(t, m, repo, "refs/heads/ren-new") == c1 && gitRef(t, m, repo, "refs/heads/ren") == "" && gitRef(t, m, repo, "refs/heads/del") == ""
		}, 10*time.Second, 20*time.Millisecond, "%s: ren-new held, ren and del gone", m.slot)
	}
}

// The same order with a pusher named (production receive-pack), driven
// through Handle with the lines and order real git 2.54/2.56 emit: the
// nested 0->0 is answered without a proposal, the deletion is proposed with
// its old value (#37).
func TestThePackedRefsTransactionIsNotProposed(t *testing.T) {
	tc := newTestCluster(t, "node-a", "node-b", "node-c")
	n := tc.nodes[0]
	work := t.TempDir()
	run(t, work, "init", "-q")
	c1 := commit(t, work, "one")
	zero := strings.Repeat("0", 40)
	repo := "alice/pusher.git"
	require.True(t, tc.pushVia(n, work, repo, "refs/heads/main", zero, c1).OK)
	require.True(t, tc.pushVia(n, work, repo, "refs/heads/b", zero, c1).OK)
	dir := filepath.Join(n.r.root, filepath.FromSlash(repo))
	run(t, dir, "pack-refs", "--all")
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	nonce := clusterhook.NewNonce()
	events := n.r.table.EventCount()
	unlock := holdRefLock(t, dir, "refs/heads/b", "") // git's empty deletion lock
	_ = os.WriteFile(filepath.Join(dir, "refs/heads/b.lock"), nil, 0o644)
	nested := n.r.Handle(ctx, &clusterhook.Request{
		Op: clusterhook.OpPrepared, Nonce: nonce, GitDir: dir, RepoID: 1, PusherID: 2,
		Lines: []clusterhook.Line{{Old: zero, New: zero, Ref: "refs/heads/b"}},
	})
	main := n.r.Handle(ctx, &clusterhook.Request{
		Op: clusterhook.OpPrepared, Nonce: nonce, GitDir: dir, RepoID: 1, PusherID: 2,
		Lines: []clusterhook.Line{{Old: c1, New: zero, Ref: "refs/heads/b"}},
	})
	unlock()
	t.Logf("nested prepared (0->0 b): ok=%v %s", nested.OK, nested.Message)
	t.Logf("main prepared (c1->0 b): ok=%v %s", main.OK, main.Message)
	require.True(t, nested.OK, nested.Message)
	require.True(t, main.OK, main.Message)
	assert.True(t, n.r.registry.inFlight(repo), "the deletion is registered")
	// The nested transaction's "committed" comes first and releases nothing.
	n.r.Handle(ctx, &clusterhook.Request{Op: clusterhook.OpCommitted, Nonce: nonce, GitDir: dir, Lines: []clusterhook.Line{{Old: zero, New: zero, Ref: "refs/heads/b"}}})
	assert.True(t, n.r.registry.inFlight(repo), "the nested transaction's committed released the deletion before git published")
	n.r.Handle(ctx, &clusterhook.Request{Op: clusterhook.OpCommitted, Nonce: nonce, GitDir: dir, Lines: []clusterhook.Line{{Old: c1, New: zero, Ref: "refs/heads/b"}}})
	assert.False(t, n.r.registry.inFlight(repo))
	v, _ := n.r.table.Get(repo, "refs/heads/b")
	t.Logf("table b tombstone=%v rev=%d", v.Tombstone, v.Rev)
	assert.True(t, v.Tombstone)
	assert.Equal(t, events+1, n.r.table.EventCount(), "one event for the deletion, none for the nested transaction")
}

// Forgejo's housekeeping (`git gc`, the "Garbage collect all repositories"
// operation) runs `git pack-refs --all --prune`, which drives the
// reference-transaction hook: one transaction "0 -> sha" per packed ref,
// then one "sha -> 0" per pruned loose ref. Through the hook, every branch
// and tag was deleted on every member (#72): gc runs without it.
func TestGitGcKeepsTheRefs(t *testing.T) {
	require.NoError(t, unittest.PrepareTestDatabase())
	out, _ := exec.Command("git", "--version").CombinedOutput()
	t.Logf("%s", strings.TrimSpace(string(out)))
	tc := newTestCluster(t, "node-a", "node-b", "node-c")
	work := t.TempDir()
	run(t, work, "init", "-q")
	c1 := commit(t, work, "one")
	zero := strings.Repeat("0", 40)
	n := tc.nodes[0]
	key := "user2/repo1.git"
	refs := []string{"refs/heads/main", "refs/heads/feature", "refs/tags/v1"}
	for _, ref := range refs {
		require.True(t, tc.pushVia(n, work, key, ref, zero, c1).OK, ref)
	}
	for _, m := range tc.nodes {
		for _, ref := range refs {
			require.Eventually(t, func() bool { return gitRef(t, m, key, ref) == c1 }, 10*time.Second, 20*time.Millisecond, "%s %s", m.slot, ref)
		}
	}
	revs := map[string]uint64{}
	for _, ref := range refs {
		v, _ := n.r.table.Get(key, ref)
		revs[ref] = v.Rev
	}
	evBefore := n.r.table.EventCount()

	withGitThroughReplicator(t, n)
	repo := unittest.AssertExistsAndLoadBean(t, &repo_model.Repository{ID: 1})
	require.Equal(t, filepath.Join(n.r.root, "user2", "repo1.git"), repo.RepoPath())
	gcErr := repo_service.GitGcRepo(db.DefaultContext, repo, time.Minute, nil)
	t.Logf("GitGcRepo: %v", gcErr)
	require.NoError(t, gcErr)

	for _, ref := range refs {
		v, ok := n.r.table.Get(key, ref)
		t.Logf("table after gc: %s live=%v sha=%s rev %d -> %d", ref, v.Live(), v.SHA, revs[ref], v.Rev)
		assert.True(t, ok && v.Live() && v.SHA == c1, "%s: a git gc must not change the replicated table", ref)
	}
	t.Logf("events: %d -> %d", evBefore, n.r.table.EventCount())
	assert.Equal(t, evBefore, n.r.table.EventCount(), "a git gc proposes nothing")
	time.Sleep(3 * time.Second)
	for _, m := range tc.nodes {
		for _, ref := range refs {
			got := gitRef(t, m, key, ref)
			t.Logf("git on %s after gc: %s = %q", m.slot, ref, got)
			assert.Equal(t, c1, got, "%s: %s must survive a git gc on %s", m.slot, ref, n.slot)
		}
	}
}

// Forgejo's RenameBranch through the cluster's reference-transaction hook:
// `git branch -m` wrote the new name without a transaction, which no other
// member ever saw (#38). Both a loose and a packed ref are renamed.
func TestRenameBranchReachesEveryMember(t *testing.T) {
	tc := newTestCluster(t, "node-a", "node-b", "node-c")
	n := tc.nodes[0]
	work := t.TempDir()
	run(t, work, "init", "-q")
	c1 := commit(t, work, "one")
	zero := strings.Repeat("0", 40)
	repo := "alice/r1rename.git"
	for _, b := range []string{"main", "packed"} {
		require.True(t, tc.pushVia(n, work, repo, "refs/heads/"+b, zero, c1).OK)
	}
	dir := filepath.Join(n.r.root, filepath.FromSlash(repo))
	run(t, dir, "pack-refs", "--all")
	require.True(t, tc.pushVia(n, work, repo, "refs/heads/loose", zero, c1).OK)
	for _, m := range tc.nodes {
		require.Eventually(t, func() bool {
			return gitRef(t, m, repo, "refs/heads/loose") == c1 && gitRef(t, m, repo, "refs/heads/packed") == c1
		}, 15*time.Second, 20*time.Millisecond, "%s: precondition, the branches are on every member", m.slot)
	}

	withGitThroughReplicator(t, n)
	g, err := git.OpenRepository(context.Background(), dir)
	require.NoError(t, err)
	defer g.Close()
	errLoose := g.RenameBranch("loose", "loose-new")
	errPacked := g.RenameBranch("packed", "packed-new")
	t.Logf("RenameBranch loose -> loose-new: err=%v", errLoose)
	t.Logf("RenameBranch packed -> packed-new: err=%v", errPacked)
	require.NoError(t, errLoose)
	require.NoError(t, errPacked)

	// Let the projectors run: the old names must be gone everywhere first.
	for _, m := range tc.nodes {
		require.Eventually(t, func() bool { return gitRef(t, m, repo, "refs/heads/loose") == "" }, 15*time.Second, 20*time.Millisecond, "%s: old name gone", m.slot)
	}
	time.Sleep(3 * time.Second)
	for _, m := range tc.nodes {
		for _, b := range []string{"loose", "loose-new", "packed", "packed-new"} {
			v, ok := m.r.table.Get(repo, "refs/heads/"+b)
			t.Logf("%s %-10s table(ok=%v tombstone=%v live=%v) git=%q", m.slot, b, ok, ok && v.Tombstone, ok && v.Live(), gitRef(t, m, repo, "refs/heads/"+b))
		}
	}
	_, inTable := n.r.table.Get(repo, "refs/heads/loose-new")
	assert.True(t, inTable, "the renamed branch loose-new is not in the replicated table")
	for _, m := range tc.nodes {
		assert.Equal(t, c1, gitRef(t, m, repo, "refs/heads/loose-new"), "%s does not hold the renamed branch loose-new", m.slot)
		assert.Equal(t, c1, gitRef(t, m, repo, "refs/heads/packed-new"), "%s does not hold the renamed branch packed-new", m.slot)
		assert.Empty(t, gitRef(t, m, repo, "refs/heads/packed"), "%s still holds packed", m.slot)
	}
}

// Renaming the default branch, as services/repository.RenameBranch does:
// RenameBranch, then `git symbolic-ref HEAD refs/heads/<to>`
// (gitrepo.SetDefaultBranch). Every member serves the renamed branch (#38).
func TestRenameTheDefaultBranch(t *testing.T) {
	tc := newTestCluster(t, "node-a", "node-b", "node-c")
	n := tc.nodes[0]
	work := t.TempDir()
	run(t, work, "init", "-q")
	c1 := commit(t, work, "one")
	zero := strings.Repeat("0", 40)
	repo := "alice/r1default.git"
	require.True(t, tc.pushVia(n, work, repo, "refs/heads/main", zero, c1).OK)
	for _, m := range tc.nodes {
		require.Eventually(t, func() bool { return gitRef(t, m, repo, "refs/heads/main") == c1 }, 15*time.Second, 20*time.Millisecond)
	}
	dir := filepath.Join(n.r.root, filepath.FromSlash(repo))

	withGitThroughReplicator(t, n)
	ctx := context.Background()
	g, err := git.OpenRepository(ctx, dir)
	require.NoError(t, err)
	defer g.Close()
	require.NoError(t, g.RenameBranch("main", "trunk"))
	_, _, err = git.NewCommand(ctx, "symbolic-ref", "HEAD").AddDynamicArguments(git.BranchPrefix + "trunk").RunStdString(&git.RunOpts{Dir: dir})
	require.NoError(t, err)

	for _, m := range tc.nodes {
		require.Eventually(t, func() bool {
			v, ok := m.r.table.Get(repo, "HEAD")
			return ok && v.Symbolic == "refs/heads/trunk"
		}, 15*time.Second, 20*time.Millisecond, "%s: HEAD -> trunk in the table", m.slot)
	}
	time.Sleep(3 * time.Second)
	for _, m := range tc.nodes {
		mdir := filepath.Join(m.r.root, filepath.FromSlash(repo))
		cmd := exec.Command("git", "rev-parse", "--verify", "-q", "HEAD")
		cmd.Dir = mdir
		out, rerr := cmd.CombinedOutput()
		head := exec.Command("git", "symbolic-ref", "-q", "HEAD")
		head.Dir = mdir
		hout, _ := head.CombinedOutput()
		t.Logf("%s HEAD->%s rev-parse HEAD=%q err=%v main=%q trunk=%q", m.slot, strings.TrimSpace(string(hout)), strings.TrimSpace(string(out)), rerr, gitRef(t, m, repo, "refs/heads/main"), gitRef(t, m, repo, "refs/heads/trunk"))
	}
	for _, m := range tc.nodes {
		assert.Equal(t, c1, gitRef(t, m, repo, "refs/heads/trunk"), "%s: HEAD names trunk, which this member does not hold (the repository looks empty)", m.slot)
	}
}

// R6-06 / R3-12 end to end, through Handle on a three-node cluster: node-a's
// git passed "prepared" (main locked with c1) and has not published yet.
// The proposal outlived TXN_TTL (the registry's clock is moved past the
// deadline). node-b then takes an acknowledged write setting main back to
// c0 (ABA). With git's own PID, which the hook now sends (#108), alive
// until it published, the fence holds: node-a certifies nothing behind git.
func TestTheFenceHoldsUntilGitPublishes(t *testing.T) {
	fenceUntilGitPublishes(t, true)
}

func fenceUntilGitPublishes(t *testing.T, gitPID bool) {
	tc := newTestCluster(t, "node-a", "node-b", "node-c")
	a, b := tc.nodes[0], tc.nodes[1]
	var skew atomic.Int64
	a.r.registry.mu.Lock()
	a.r.registry.now = func() time.Time { return time.Now().Add(time.Duration(skew.Load())) }
	ttl := a.r.registry.ttl
	a.r.registry.mu.Unlock()

	repo, ref := "h/gpf.git", "refs/heads/main"
	work := t.TempDir()
	run(t, work, "init", "-q")
	c0 := commit(t, work, "zero")
	c1 := commit(t, work, "one")
	zero := strings.Repeat("0", 40)
	require.True(t, tc.pushVia(a, work, repo, ref, zero, c0).OK)
	for _, n := range []*testNode{a, b} {
		require.Eventually(t, func() bool { return gitRef(t, n, repo, ref) == c0 }, 10*time.Second, 20*time.Millisecond)
	}
	dirA := filepath.Join(a.r.root, filepath.FromSlash(repo))
	ctx := context.Background()

	// node-a: receive-pack wrote the objects, locked main with c1, and runs
	// the hook in "prepared".
	run(t, work, "push", "-q", dirA, c1+":refs/tmp/incoming")
	run(t, dirA, "update-ref", "-d", "refs/tmp/incoming")
	unlock := holdRefLock(t, dirA, ref, c1)
	defer unlock()
	pid := os.Getpid() // git-receive-pack: alive until it published
	if !gitPID {
		w := exec.Command("sh", "-c", "exit 0") // the generated bash wrapper: gone once the hook returned
		require.NoError(t, w.Run())
		pid = w.Process.Pid
	}
	req := &clusterhook.Request{
		Op: clusterhook.OpPrepared, Nonce: clusterhook.NewNonce(), GitDir: dirA, RepoID: 1, PusherID: 2, PID: pid,
		Lines: []clusterhook.Line{{Old: c0, New: c1, Ref: ref}},
	}
	resp := a.r.Handle(ctx, req)
	require.True(t, resp.OK, resp.Message)
	// The proposal took longer than TXN_TTL (slow majority, big push).
	skew.Store(int64(ttl + time.Second))

	// node-b projects main=c1, then an acknowledged write there sets it back to c0.
	require.Eventually(t, func() bool { return gitRef(t, b, repo, ref) == c1 }, 10*time.Second, 20*time.Millisecond)
	require.True(t, tc.pushVia(b, work, repo, ref, c1, c0).OK)
	require.Eventually(t, func() bool { v, _ := a.r.table.Get(repo, ref); return v.SHA == c0 }, 10*time.Second, 10*time.Millisecond)
	pos, _ := a.r.table.Applied()
	idx := pos.Index

	certified := false
	for end := time.Now().Add(6 * time.Second); time.Now().Before(end); time.Sleep(20 * time.Millisecond) {
		if a.r.projector.Projected(repo) >= idx {
			certified = true
			break
		}
	}
	t.Logf("before git published: node-a certified index %d (ABA entry) = %v; git main=%s, lock=%s, table main=%s",
		idx, certified, shortSHA(gitRef(t, a, repo, ref)), shortSHA(c1), shortSHA(c0))

	// git publishes what "prepared" allowed: the lock becomes the ref.
	lock := filepath.Join(dirA, "refs", "heads", "main.lock")
	require.NoError(t, os.Rename(lock, filepath.Join(dirA, "refs", "heads", "main")))
	gitA := gitRef(t, a, repo, ref)
	tabA, _ := a.r.table.Get(repo, ref)
	wctx, cancel := context.WithTimeout(ctx, 200*time.Millisecond)
	werr := a.r.WaitProjected(wctx, repo, idx)
	cancel()
	fresh := a.r.Handle(ctx, &clusterhook.Request{Op: clusterhook.OpFresh, Repo: repo})
	t.Logf("after git published: git main=%s table main=%s; WaitProjected(%d) err=%v; OpFresh ok=%v",
		shortSHA(gitA), shortSHA(tabA.SHA), idx, werr, fresh.OK)

	if !gitPID {
		// "committed" never reaches the replicator: does anything repair it?
		time.Sleep(5 * time.Second)
		t.Logf("5 s later, no \"committed\": git main=%s table main=%s Projected=%d",
			shortSHA(gitRef(t, a, repo, ref)), shortSHA(tabA.SHA), a.r.projector.Projected(repo))
	}
	assert.False(t, werr == nil && gitA != tabA.SHA,
		"node-a reports index %d projected while its git serves %s and the table at that index holds %s: the fence lapsed before git published",
		idx, shortSHA(gitA), shortSHA(tabA.SHA))

	req.Op = clusterhook.OpCommitted
	a.r.Handle(ctx, req)
	require.Eventually(t, func() bool { return gitRef(t, a, repo, ref) == c0 }, 10*time.Second, 20*time.Millisecond, "repair after committed")
}

func shortSHA(s string) string {
	if len(s) > 8 {
		return s[:8]
	}
	return s
}
