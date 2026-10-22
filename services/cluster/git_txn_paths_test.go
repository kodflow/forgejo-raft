// Copyright 2026 The Forgejo Authors. All rights reserved.
// SPDX-License-Identifier: GPL-3.0-or-later

package cluster

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"forgejo.org/modules/clusterhook"
	"forgejo.org/modules/setting"

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
