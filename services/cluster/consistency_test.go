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

	"forgejo.org/modules/clusterhook"
	"forgejo.org/modules/setting"
	"forgejo.org/modules/test"
	"forgejo.org/services/cluster/consensus"
	"forgejo.org/services/cluster/reftable"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// A branch created on one node, then created again on another whose git has
// not projected it yet: receive-pack on the second node sees no such branch
// and sends a zero old value, which means "must not exist". The table holds
// the first branch: the second push must be refused, not turned into an
// overwrite that drops the first branch's commits.
func TestStaleCreateDoesNotOverwriteABranch(t *testing.T) {
	tc := newTestCluster(t, "node-a", "node-b", "node-c")
	work := t.TempDir()
	run(t, work, "init", "-q")
	c1 := commit(t, work, "one")
	c2 := commit(t, work, "two")
	zero := strings.Repeat("0", 40)
	require.True(t, tc.pushVia(tc.nodes[0], work, "a/r.git", "refs/heads/topic", zero, c1).OK)
	nodeC := tc.nodes[2]
	require.Eventually(t, func() bool {
		v, ok := nodeC.r.table.Get("a/r.git", "refs/heads/topic")
		return ok && v.SHA == c1
	}, 10*time.Second, 10*time.Millisecond)

	resp := tc.pushVia(nodeC, work, "a/r.git", "refs/heads/topic", zero, c2)
	assert.False(t, resp.OK, "a creation over a live branch must be refused")
	assert.Contains(t, resp.Message, "already exists")
	for _, n := range tc.nodes {
		v, _ := n.r.table.Get("a/r.git", "refs/heads/topic")
		assert.Equal(t, c1, v.SHA, n.slot)
	}
}

// A rename replicated while a push is in flight on the old key of another
// node: the projector there must not move the directory from under the git
// process writing into it. The registration fences the old key, and the
// move waits until the transaction ends.
func TestRenameWaitsForATransactionInFlightOnTheOldKey(t *testing.T) {
	tc := newTestCluster(t, "node-a", "node-b", "node-c")
	work := t.TempDir()
	run(t, work, "init", "-q")
	c1 := commit(t, work, "one")
	zero := strings.Repeat("0", 40)
	require.True(t, tc.pushVia(tc.nodes[0], work, "o/old.git", "refs/heads/main", zero, c1).OK)
	for _, n := range tc.nodes {
		require.Eventually(t, func() bool { return gitRef(t, n, "o/old.git", "refs/heads/main") == c1 }, 10*time.Second, 20*time.Millisecond)
	}

	nodeB := tc.nodes[1]
	nonce := clusterhook.NewNonce()
	require.NoError(t, nodeB.r.registry.register(nonce, "o/old.git", []string{"refs/heads/main"}, os.Getpid(), false))

	origin := tc.nodes[0]
	require.NoError(t, os.Rename(filepath.Join(origin.r.root, "o", "old.git"), filepath.Join(origin.r.root, "o", "new.git")))
	require.NoError(t, origin.r.proposeRepoRename(context.Background(), "o/old.git", "o/new.git"))
	require.Eventually(t, func() bool { _, _, ok := nodeB.r.table.RenamedFrom("o/new.git"); return ok }, 10*time.Second, 10*time.Millisecond)

	oldDir := filepath.Join(nodeB.r.root, "o", "old.git")
	require.Never(t, func() bool {
		_, err := os.Stat(oldDir)
		return os.IsNotExist(err)
	}, 3*time.Second, 50*time.Millisecond, "the directory moved under a transaction in flight")

	nodeB.r.registry.release(nonce)
	nodeB.r.projector.markDirty("o/new.git")
	require.Eventually(t, func() bool { return gitRef(t, nodeB, "o/new.git", "refs/heads/main") == c1 }, 10*time.Second, 20*time.Millisecond)
	_, err := os.Stat(oldDir)
	assert.True(t, os.IsNotExist(err))
}

// A push still running on one node when the repository is deleted or renamed
// on another: its new branch must not bring the old key back to life in the
// table, under a name the database no longer has, where its push would be
// reported a success and then be lost.
func TestPushToADeletedOrRenamedKeyIsRefused(t *testing.T) {
	tc := newTestCluster(t, "node-a", "node-b", "node-c")
	work := t.TempDir()
	run(t, work, "init", "-q")
	c1 := commit(t, work, "one")
	c2 := commit(t, work, "two")
	zero := strings.Repeat("0", 40)
	ctx := context.Background()
	origin, other := tc.nodes[0], tc.nodes[2]

	require.True(t, tc.pushVia(origin, work, "p/del.git", "refs/heads/main", zero, c1).OK)
	require.True(t, tc.pushVia(origin, work, "p/old.git", "refs/heads/main", zero, c1).OK)
	for _, n := range tc.nodes {
		require.Eventually(t, func() bool {
			return gitRef(t, n, "p/del.git", "refs/heads/main") == c1 && gitRef(t, n, "p/old.git", "refs/heads/main") == c1
		}, 10*time.Second, 20*time.Millisecond)
	}

	require.NoError(t, os.RemoveAll(filepath.Join(origin.r.root, "p", "del.git")))
	require.NoError(t, origin.r.proposeRepoDelete(ctx, "p/del.git", 1))
	require.NoError(t, os.Rename(filepath.Join(origin.r.root, "p", "old.git"), filepath.Join(origin.r.root, "p", "new.git")))
	require.NoError(t, origin.r.proposeRepoRename(ctx, "p/old.git", "p/new.git"))
	require.Eventually(t, func() bool {
		_, deleted := other.r.table.Deleted("p/del.git")
		_, _, renamed := other.r.table.RenamedFrom("p/new.git")
		return deleted && renamed
	}, 10*time.Second, 10*time.Millisecond)
	for _, repo := range []string{"del.git", "old.git"} {
		require.Eventually(t, func() bool {
			_, err := os.Stat(filepath.Join(other.r.root, "p", repo))
			return os.IsNotExist(err)
		}, 10*time.Second, 10*time.Millisecond, repo)
	}

	for _, repo := range []string{"p/del.git", "p/old.git"} {
		resp := tc.pushVia(other, work, repo, "refs/heads/late", zero, c2)
		assert.False(t, resp.OK, repo)
		_, ok := other.r.table.Get(repo, "refs/heads/late")
		assert.False(t, ok, repo)
	}
	_, deleted := other.r.table.Deleted("p/del.git")
	assert.True(t, deleted, "the deleted key stays deleted")

	// Forgejo creating a repository again under a freed name is no push:
	// its HEAD is written, and the name lives again.
	dir := filepath.Join(other.r.root, "p", "del.git")
	resp := other.r.Handle(ctx, &clusterhook.Request{
		Op: clusterhook.OpPrepared, Nonce: clusterhook.NewNonce(), GitDir: dir,
		Lines: []clusterhook.Line{{Old: zero, New: "ref:refs/heads/main", Ref: "HEAD"}},
	})
	require.True(t, resp.OK, resp.Message)
}

// A proposal whose outcome was unknown is settled from the table: when a
// later push already moved the ref, the values no longer say it committed,
// and the push was reported refused although its entry was in the log.
// The nonce the table keeps answers instead.
func TestUnknownOutcomeSettledByTheNonceAfterALaterWrite(t *testing.T) {
	r := newReplicator(t.TempDir(), newRegistry(time.Second, 8, 64))
	a, b := strings.Repeat("a", 40), strings.Repeat("b", 40)
	apply := func(i uint64, tx *reftable.RefTxn) {
		data, err := reftable.Encode(reftable.KindRefTxn, tx)
		require.NoError(t, err)
		res := r.table.Apply(consensus.Entry{Term: 1, Index: i, Data: data}).(reftable.Result)
		require.True(t, res.OK, res.Reason)
	}
	mine := &reftable.RefTxn{Repo: "o/r.git", Format: "sha1", Nonce: "mine", Updates: []reftable.RefUpdate{{Ref: "refs/heads/main", New: a}}}
	apply(1, mine)
	apply(2, &reftable.RefTxn{Repo: "o/r.git", Format: "sha1", Nonce: "theirs", Updates: []reftable.RefUpdate{{Ref: "refs/heads/main", Old: a, ExpectRev: 1, New: b}}})
	assert.True(t, r.committed("o/r.git", mine))
	other := &reftable.RefTxn{Repo: "o/r.git", Format: "sha1", Nonce: "never", Updates: []reftable.RefUpdate{{Ref: "refs/heads/main", New: a}}}
	assert.False(t, r.committed("o/r.git", other))
}

// The written command version is raised once every member applies it, and
// a push then travels at the new version; a member that did not say so
// keeps the cluster at the old one.
func TestPayloadVersionRaisedOnlyWhenEveryMemberAppliesIt(t *testing.T) {
	m := consensus.Membership{Members: []consensus.Member{{ID: 1}, {ID: 2}, {ID: 3}}}
	v := reftable.PayloadVersion
	all := []hello{{Node: consensus.NodeID(2).String(), Payload: v}, {Node: consensus.NodeID(3).String(), Payload: v}}
	assert.True(t, everyMemberApplies(m, 1, all, anyAnswer))
	assert.False(t, everyMemberApplies(m, 1, all[:1], anyAnswer), "a member that did not answer")
	old := []hello{all[0], {Node: consensus.NodeID(3).String()}}
	assert.False(t, everyMemberApplies(m, 1, old, anyAnswer), "a member running an older binary")

	tc := newTestCluster(t, "node-a", "node-b", "node-c")
	require.NoError(t, tc.nodes[1].r.RaisePayloadVersion(context.Background(), "op"))
	for _, n := range tc.nodes {
		require.Eventually(t, func() bool { return n.r.table.WriteVersion() == reftable.PayloadVersion }, 10*time.Second, 10*time.Millisecond, n.slot)
	}
	work := t.TempDir()
	run(t, work, "init", "-q")
	c1 := commit(t, work, "one")
	require.True(t, tc.pushVia(tc.nodes[2], work, "v/r.git", "refs/heads/main", strings.Repeat("0", 40), c1).OK)
	for _, n := range tc.nodes {
		v, _ := n.r.table.Get("v/r.git", "refs/heads/main")
		assert.Equal(t, c1, v.SHA, n.slot)
	}
}

// A repository deleted, then its name taken by a rename, both applied on a
// node before its projector ran: the deleted repository's directory is
// still there under the new name. It must be set aside, not reused: the
// renamed repository moves in, and the old directory does not linger.
func TestRenameIntoAnUnprojectedDeletionSetsTheOldDirectoryAside(t *testing.T) {
	tc := newTestCluster(t, "node-a", "node-b", "node-c")
	work := t.TempDir()
	run(t, work, "init", "-q")
	c1 := commit(t, work, "one")
	c2 := commit(t, work, "two")
	zero := strings.Repeat("0", 40)
	origin, lag := tc.nodes[0], tc.nodes[2]
	require.True(t, tc.pushVia(origin, work, "s/a.git", "refs/heads/main", zero, c1).OK)
	require.True(t, tc.pushVia(origin, work, "s/b.git", "refs/heads/old", zero, c2).OK)
	for _, n := range tc.nodes {
		require.Eventually(t, func() bool {
			return gitRef(t, n, "s/a.git", "refs/heads/main") == c1 && gitRef(t, n, "s/b.git", "refs/heads/old") == c2
		}, 10*time.Second, 20*time.Millisecond)
	}

	// The lagging node does not project s/b.git while a transaction holds it.
	nonce := clusterhook.NewNonce()
	require.NoError(t, lag.r.registry.register(nonce, "s/b.git", nil, os.Getpid(), false))
	ctx := context.Background()
	require.NoError(t, os.RemoveAll(filepath.Join(origin.r.root, "s", "b.git")))
	require.NoError(t, origin.r.proposeRepoDelete(ctx, "s/b.git", 1))
	require.NoError(t, os.Rename(filepath.Join(origin.r.root, "s", "a.git"), filepath.Join(origin.r.root, "s", "b.git")))
	require.NoError(t, origin.r.proposeRepoRename(ctx, "s/a.git", "s/b.git"))
	require.Eventually(t, func() bool { _, _, ok := lag.r.table.RenamedFrom("s/b.git"); return ok }, 10*time.Second, 10*time.Millisecond)
	lag.r.registry.release(nonce)
	lag.r.projector.markDirty("s/b.git")

	srcDir := filepath.Join(lag.r.root, "s", "a.git")
	require.Eventually(t, func() bool {
		_, err := os.Stat(srcDir)
		return os.IsNotExist(err) && gitRef(t, lag, "s/b.git", "refs/heads/main") == c1
	}, 10*time.Second, 20*time.Millisecond, "the renamed repository moved in")
	entries, err := os.ReadDir(filepath.Join(lag.r.root, staleDirName))
	require.NoError(t, err)
	assert.Len(t, entries, 1, "the deleted repository's directory was set aside")
}

// A repository a peer creates to receive objects is acknowledged once the
// pack is synced: the directories leading to it must be synced too, or a
// power loss can take the whole repository the ack vouched for.
func TestInitBareSyncsTheDirectoriesItCreates(t *testing.T) {
	root := t.TempDir()
	var synced []string
	defer func(prev func(string) error) { syncDirFn = prev }(syncDirFn)
	syncDirFn = func(d string) error { synced = append(synced, d); return nil }
	var files []string
	defer func(prev func(string) error) { syncFileFn = prev }(syncFileFn)
	syncFileFn = func(f string) error { files = append(files, filepath.Base(f)); return nil }
	dir := filepath.Join(root, "o", "r.git")
	require.NoError(t, initBare(context.Background(), dir, "sha1"))
	// The repository is made and synced in a sibling, then renamed under
	// the key (#42): its own directories are synced under the sibling.
	require.Len(t, synced, 5, "%v", synced)
	tmp := synced[2]
	assert.True(t, strings.HasPrefix(filepath.Base(tmp), ".r.git.init-"), tmp)
	assert.Equal(t, []string{filepath.Join(tmp, "objects"), filepath.Join(tmp, "refs"), tmp, filepath.Join(root, "o"), root}, synced)
	assert.Equal(t, []string{"HEAD", "config"}, files)
	assert.True(t, validRepoDir(dir))
	assert.NoDirExists(t, tmp)
}

// Two renames applied on a node before its projector ran: s/a.git renamed
// to s/z.git, then s/b.git renamed to s/a.git (a user renames "app" to
// "app-old", then "app-new" to "app"). The node must move s/a.git's
// directory to s/z.git before it moves s/b.git's into s/a.git. The other
// order projects s/b.git's refs into the old s/a.git directory, then moves
// that directory to s/z.git, where s/b.git's branch stays as a ref the
// table never held there, and s/a.git is gone from the node.
func TestChainedRenamesOnALaggingNodeMoveInLogOrder(t *testing.T) {
	tc := newTestCluster(t, "node-a", "node-b", "node-c")
	work := t.TempDir()
	run(t, work, "init", "-q")
	c1 := commit(t, work, "one")
	c2 := commit(t, work, "two")
	zero := strings.Repeat("0", 40)
	origin, lag := tc.nodes[0], tc.nodes[2]
	require.True(t, tc.pushVia(origin, work, "s/a.git", "refs/heads/old", zero, c1).OK)
	require.True(t, tc.pushVia(origin, work, "s/b.git", "refs/heads/feature", zero, c2).OK)
	for _, n := range tc.nodes {
		require.Eventually(t, func() bool {
			return gitRef(t, n, "s/a.git", "refs/heads/old") == c1 && gitRef(t, n, "s/b.git", "refs/heads/feature") == c2
		}, 10*time.Second, 20*time.Millisecond)
	}

	// The lagging node projects neither repository while both renames apply.
	holds := []string{clusterhook.NewNonce(), clusterhook.NewNonce()}
	require.NoError(t, lag.r.registry.register(holds[0], "s/a.git", nil, os.Getpid(), false))
	require.NoError(t, lag.r.registry.register(holds[1], "s/b.git", nil, os.Getpid(), false))
	ctx := context.Background()
	root := origin.r.root
	require.NoError(t, os.Rename(filepath.Join(root, "s", "a.git"), filepath.Join(root, "s", "z.git")))
	require.NoError(t, origin.r.proposeRepoRename(ctx, "s/a.git", "s/z.git"))
	require.NoError(t, os.Rename(filepath.Join(root, "s", "b.git"), filepath.Join(root, "s", "a.git")))
	require.NoError(t, origin.r.proposeRepoRename(ctx, "s/b.git", "s/a.git"))
	require.Eventually(t, func() bool {
		f, _, ok := lag.r.table.RenamedFrom("s/a.git")
		return ok && f == "s/b.git"
	}, 10*time.Second, 10*time.Millisecond)
	for _, n := range holds {
		lag.r.registry.release(n)
	}
	for _, k := range []string{"s/a.git", "s/b.git", "s/z.git"} {
		lag.r.projector.markDirty(k)
	}

	require.Eventually(t, func() bool {
		return gitRef(t, lag, "s/z.git", "refs/heads/old") == c1 && gitRef(t, lag, "s/a.git", "refs/heads/feature") == c2
	}, 10*time.Second, 20*time.Millisecond, "each repository reached its new name on the lagging node")
	assert.Empty(t, gitRef(t, lag, "s/z.git", "refs/heads/feature"), "s/b.git's branch leaked into s/z.git")
	assert.Empty(t, gitRef(t, lag, "s/a.git", "refs/heads/old"))
}

// The same deletion then rename, after the node restarted (or installed a
// snapshot) between the two: the in-memory record of the deletion is gone,
// and the directory's stamp is what tells it held the deleted repository.
func TestRenameIntoAnUnprojectedDeletionAfterARestart(t *testing.T) {
	tc := newTestCluster(t, "node-a", "node-b", "node-c")
	work := t.TempDir()
	run(t, work, "init", "-q")
	c1 := commit(t, work, "one")
	c2 := commit(t, work, "two")
	zero := strings.Repeat("0", 40)
	origin, lag := tc.nodes[0], tc.nodes[2]
	require.True(t, tc.pushVia(origin, work, "s/a.git", "refs/heads/main", zero, c1).OK)
	require.True(t, tc.pushVia(origin, work, "s/b.git", "refs/heads/old", zero, c2).OK)
	for _, n := range tc.nodes {
		require.Eventually(t, func() bool {
			return gitRef(t, n, "s/a.git", "refs/heads/main") == c1 && gitRef(t, n, "s/b.git", "refs/heads/old") == c2
		}, 10*time.Second, 20*time.Millisecond)
	}

	nonce := clusterhook.NewNonce()
	require.NoError(t, lag.r.registry.register(nonce, "s/b.git", nil, os.Getpid(), false))
	ctx := context.Background()
	require.NoError(t, os.RemoveAll(filepath.Join(origin.r.root, "s", "b.git")))
	require.NoError(t, origin.r.proposeRepoDelete(ctx, "s/b.git", 1))
	require.NoError(t, os.Rename(filepath.Join(origin.r.root, "s", "a.git"), filepath.Join(origin.r.root, "s", "b.git")))
	require.NoError(t, origin.r.proposeRepoRename(ctx, "s/a.git", "s/b.git"))
	require.Eventually(t, func() bool { _, _, ok := lag.r.table.RenamedFrom("s/b.git"); return ok }, 10*time.Second, 10*time.Millisecond)
	// What a restart or a snapshot loses: the deletion seen by onChange.
	lag.r.projector.mu.Lock()
	clear(lag.r.projector.deletedAt)
	lag.r.projector.mu.Unlock()
	lag.r.registry.release(nonce)
	lag.r.projector.markDirty("s/b.git")

	srcDir := filepath.Join(lag.r.root, "s", "a.git")
	require.Eventually(t, func() bool {
		_, err := os.Stat(srcDir)
		return os.IsNotExist(err) && gitRef(t, lag, "s/b.git", "refs/heads/main") == c1
	}, 10*time.Second, 20*time.Millisecond, "the renamed repository moved in")
	entries, err := os.ReadDir(filepath.Join(lag.r.root, staleDirName))
	require.NoError(t, err)
	assert.Len(t, entries, 1, "the deleted repository's directory was set aside")
}

// s/x.git renamed to s/y.git, s/y.git renamed on to s/w.git, then a new
// repository created under the freed name s/x.git. At the next start every
// repository is reprojected: s/y.git's directory is gone (moved on), and
// its rename names s/x.git as the source. The new s/x.git must stay where
// it is: it is not the repository that rename moved.
func TestARecreatedRenameSourceStaysAtItsName(t *testing.T) {
	tc := newTestCluster(t, "node-a", "node-b", "node-c")
	work := t.TempDir()
	run(t, work, "init", "-q")
	c1 := commit(t, work, "one")
	c2 := commit(t, work, "two")
	zero := strings.Repeat("0", 40)
	origin, lag := tc.nodes[0], tc.nodes[2]
	require.True(t, tc.pushVia(origin, work, "s/x.git", "refs/heads/main", zero, c1).OK)
	settled := func(check func(n *testNode) bool) {
		for _, n := range tc.nodes {
			require.Eventually(t, func() bool { return check(n) }, 10*time.Second, 20*time.Millisecond)
		}
	}
	settled(func(n *testNode) bool { return gitRef(t, n, "s/x.git", "refs/heads/main") == c1 })
	ctx := context.Background()
	root := origin.r.root
	require.NoError(t, os.Rename(filepath.Join(root, "s", "x.git"), filepath.Join(root, "s", "y.git")))
	require.NoError(t, origin.r.proposeRepoRename(ctx, "s/x.git", "s/y.git"))
	settled(func(n *testNode) bool { return gitRef(t, n, "s/y.git", "refs/heads/main") == c1 })
	require.NoError(t, os.Rename(filepath.Join(root, "s", "y.git"), filepath.Join(root, "s", "w.git")))
	require.NoError(t, origin.r.proposeRepoRename(ctx, "s/y.git", "s/w.git"))
	settled(func(n *testNode) bool { return gitRef(t, n, "s/w.git", "refs/heads/main") == c1 })

	// Forgejo creates s/x.git anew: no pusher, as `git init` writes it.
	dir := filepath.Join(origin.r.root, "s", "x.git")
	run(t, origin.r.root, "init", "-q", "--bare", dir)
	run(t, work, "push", "-q", dir, c2+":refs/tmp/incoming")
	run(t, dir, "update-ref", "-d", "refs/tmp/incoming")
	resp := origin.r.Handle(ctx, &clusterhook.Request{
		Op: clusterhook.OpPrepared, Nonce: clusterhook.NewNonce(), GitDir: dir, RepoID: 3,
		Lines: []clusterhook.Line{{Old: zero, New: c2, Ref: "refs/heads/main"}},
	})
	require.True(t, resp.OK, resp.Message)
	run(t, dir, "update-ref", "refs/heads/main", c2)
	settled(func(n *testNode) bool {
		return gitRef(t, n, "s/x.git", "refs/heads/main") == c2 && n.r.projector.Projected("s/x.git") > 0
	})

	xDir := filepath.Join(lag.r.root, "s", "x.git")
	before, err := os.Stat(xDir)
	require.NoError(t, err)
	_, err = os.Stat(filepath.Join(lag.r.root, "s", "y.git"))
	require.True(t, os.IsNotExist(err), "s/y.git moved on before the start")
	// A start: every repository reconciled again.
	lag.r.projector.unfence()
	require.Eventually(t, lag.r.projector.Ready, 10*time.Second, 20*time.Millisecond)
	_, err = os.Stat(filepath.Join(lag.r.root, "s", "y.git"))
	assert.True(t, os.IsNotExist(err), "the new s/x.git was moved under s/y.git")
	after, err := os.Stat(xDir)
	require.NoError(t, err)
	assert.True(t, os.SameFile(before, after), "s/x.git's directory was replaced")
	assert.Equal(t, c2, gitRef(t, lag, "s/x.git", "refs/heads/main"))
	assert.Equal(t, c1, gitRef(t, lag, "s/w.git", "refs/heads/main"))
}

// A git client carries no read-after-write cookie: a fetch that follows an
// acknowledged push, from the pusher or from a CI job its webhook started,
// may land on a node whose projector has not caught up yet. Asked before
// git runs, the fresh wait holds the fetch until the node serves the push,
// and past READ_AFTER_WRITE_TIMEOUT says the node may be behind.
func TestFreshWaitServesAnAcknowledgedPush(t *testing.T) {
	tc := newTestCluster(t, "node-a", "node-b", "node-c")
	work := t.TempDir()
	run(t, work, "init", "-q")
	c1 := commit(t, work, "one")
	c2 := commit(t, work, "two")
	c3 := commit(t, work, "three")
	zero := strings.Repeat("0", 40)
	origin, lag := tc.nodes[0], tc.nodes[2]
	require.True(t, tc.pushVia(origin, work, "s/f.git", "refs/heads/main", zero, c1).OK)
	require.Eventually(t, func() bool { return gitRef(t, lag, "s/f.git", "refs/heads/main") == c1 }, 10*time.Second, 20*time.Millisecond)

	ctx := context.Background()
	fresh := &clusterhook.Request{Op: clusterhook.OpFresh, Repo: "s/f.git"}
	hold := clusterhook.NewNonce()
	require.NoError(t, lag.r.registry.register(hold, "s/f.git", nil, os.Getpid(), false))
	require.True(t, tc.pushVia(origin, work, "s/f.git", "refs/heads/main", c1, c2).OK)
	go func() {
		time.Sleep(300 * time.Millisecond)
		lag.r.registry.release(hold)
		lag.r.projector.markDirty("s/f.git")
	}()
	resp := lag.r.Handle(ctx, fresh)
	require.True(t, resp.OK, resp.Message)
	assert.Equal(t, c2, gitRef(t, lag, "s/f.git", "refs/heads/main"), "the fetch would read the ref from before the acknowledged push")

	defer test.MockVariableValue(&setting.Cluster.ReadAfterWriteTimeout, 300*time.Millisecond)()
	require.NoError(t, lag.r.registry.register(hold, "s/f.git", nil, os.Getpid(), false))
	require.True(t, tc.pushVia(origin, work, "s/f.git", "refs/heads/main", c2, c3).OK)
	assert.False(t, lag.r.Handle(ctx, fresh).OK, "a node that cannot catch up says it may be behind")
	lag.r.registry.release(hold)
}

// anyAnswer trusts every answer, for tests of the counting alone.
func anyAnswer(consensus.Member, hello) bool { return true }
