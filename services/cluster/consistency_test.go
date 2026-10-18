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
	require.NoError(t, nodeB.r.registry.register(nonce, "o/old.git", []string{"refs/heads/main"}, os.Getpid()))

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
	assert.True(t, everyMemberApplies(m, 1, all, v))
	assert.False(t, everyMemberApplies(m, 1, all[:1], v), "a member that did not answer")
	old := []hello{all[0], {Node: consensus.NodeID(3).String()}}
	assert.False(t, everyMemberApplies(m, 1, old, v), "a member running an older binary")

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
	require.NoError(t, lag.r.registry.register(nonce, "s/b.git", nil, os.Getpid()))
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
	dir := filepath.Join(root, "o", "r.git")
	require.NoError(t, initBare(context.Background(), dir, "sha1"))
	for _, d := range []string{filepath.Join(dir, "objects"), filepath.Join(dir, "refs"), dir, filepath.Join(root, "o"), root} {
		assert.Contains(t, synced, d)
	}
}
