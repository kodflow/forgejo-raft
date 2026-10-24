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
	"forgejo.org/services/cluster/reftable"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// raisePayload raises the payload version to reftable.PayloadVersion, as a
// production cluster runs once its autopilot raised it.
func raisePayload(t *testing.T, tc *testCluster) {
	t.Helper()
	require.NoError(t, tc.nodes[1].r.RaisePayloadVersion(context.Background(), "op"))
	for _, n := range tc.nodes {
		require.Eventually(t, func() bool { return n.r.table.WriteVersion() == reftable.PayloadVersion }, 10*time.Second, 10*time.Millisecond, n.slot)
	}
}

// waitApplied waits until lag applied what origin applied.
func waitApplied(t *testing.T, origin, lag *testNode) {
	t.Helper()
	applied, _ := origin.r.table.Applied()
	require.Eventually(t, func() bool {
		pos, _ := lag.r.table.Applied()
		return pos.Index >= applied.Index
	}, 10*time.Second, 20*time.Millisecond)
}

// catchUp waits until lag applied what origin applied, then lifts lag's
// fence: one pass then projects every key, as after a restart, and lag
// certifies them all.
func catchUp(t *testing.T, origin, lag *testNode) {
	t.Helper()
	waitApplied(t, origin, lag)
	lag.r.projector.unfence()
	require.Eventually(t, lag.r.projector.Ready, 15*time.Second, 20*time.Millisecond, "lag ready")
	require.Eventually(t, func() bool { return len(lag.r.projector.Lag()) == 0 }, 20*time.Second, 50*time.Millisecond, "lag certifies: %v", lag.r.projector.Lag())
}

// fetchableByID reports whether a client fetching sha by its id from n's
// repo gets it, as Forgejo's default uploadpack.allowAnySHA1InWant lets it.
func fetchableByID(t *testing.T, n *testNode, repo, sha string) bool {
	dst := t.TempDir()
	run(t, dst, "init", "-q", "--bare")
	cmd := exec.Command("git", "-c", "uploadpack.allowAnySHA1InWant=true", "-C", dst, "fetch", "-q", filepath.Join(n.r.root, filepath.FromSlash(repo)), sha)
	cmd.Env = append(os.Environ(), "HOME="+setting.Git.HomePath)
	return cmd.Run() == nil
}

// settle waits until n certified every key, as a user's next action on n
// comes after its projector caught up: Forgejo moves a directory on its own
// disk before the entry that moves the key is applied.
func settle(t *testing.T, n *testNode) {
	t.Helper()
	require.Eventually(t, func() bool { return len(n.r.projector.Lag()) == 0 }, 10*time.Second, 20*time.Millisecond, "%s certifies: %v", n.slot, n.r.projector.Lag())
}

// setAsideCount counts the directories n set aside.
func setAsideCount(t *testing.T, n *testNode) int {
	t.Helper()
	entries, err := os.ReadDir(filepath.Join(n.r.root, staleDirName))
	if os.IsNotExist(err) {
		return 0
	}
	require.NoError(t, err)
	return len(entries)
}

// createVia256 is createVia in the sha256 object format.
func (tc *testCluster) createVia256(n *testNode, repo string) {
	t := tc.t
	dir := filepath.Join(n.r.root, filepath.FromSlash(repo))
	run(t, n.r.root, "init", "-q", "--bare", "--object-format=sha256", dir)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	req := &clusterhook.Request{
		Op: clusterhook.OpPrepared, Nonce: clusterhook.NewNonce(), GitDir: dir,
		Lines: []clusterhook.Line{{Old: strings.Repeat("0", 64), New: "ref:refs/heads/main", Ref: "HEAD"}},
	}
	resp := n.r.Handle(ctx, req)
	require.True(t, resp.OK, resp.Message)
	req.Op = clusterhook.OpCommitted
	n.r.Handle(ctx, req)
}

// renamedAwayThenTaken plays, on origin, a repository o/r (main c1, a
// branch secret) renamed to o/q, o/q deleted, and a new o/r created and
// pushed, while lag is fenced: lag still holds the old o/r directory, the
// deleted o/q's. create creates the new o/r, push pushes its main.
func renamedAwayThenTaken(t *testing.T, tc *testCluster, create func(n *testNode, repo string), push func(n *testNode)) {
	t.Helper()
	ctx := context.Background()
	origin, peer, lag := tc.nodes[0], tc.nodes[1], tc.nodes[2]
	root := origin.r.root
	lag.r.projector.refence()
	settle(t, origin)
	require.NoError(t, os.Rename(filepath.Join(root, "o", "r.git"), filepath.Join(root, "o", "q.git")))
	require.NoError(t, origin.r.proposeRepoRename(ctx, "o/r.git", "o/q.git"))
	require.Eventually(t, func() bool { return dirExists(peer, "o/q.git") && !dirExists(peer, "o/r.git") }, 10*time.Second, 20*time.Millisecond)
	settle(t, origin)
	require.NoError(t, os.RemoveAll(filepath.Join(root, "o", "q.git")))
	require.NoError(t, origin.r.proposeRepoDelete(ctx, "o/q.git", 1))
	require.Eventually(t, func() bool { return !dirExists(peer, "o/q.git") }, 10*time.Second, 20*time.Millisecond)
	create(origin, "o/r.git")
	push(origin)
}

// A repository renamed away (o/r to o/q), its new name deleted, the old
// name taken by a new repository renamed in turn (o/r to o/new), while a
// node is behind: the old o/r directory that node still holds belongs to
// the deleted o/q, never to o/new. Moved and certified under o/new.git, it
// served the deleted repository's objects by their ids (#181).
func TestARepositoryRenamedAwayIsNotMovedUnderTheNameOfTheNextOne(t *testing.T) {
	tc := newTestCluster(t, "node-a", "node-b", "node-c")
	raisePayload(t, tc)
	work := t.TempDir()
	run(t, work, "init", "-q")
	c1 := commit(t, work, "one")
	secret := commit(t, work, "secret")
	run(t, work, "checkout", "-q", "--orphan", "fresh")
	c2 := commit(t, work, "two")
	zero := strings.Repeat("0", 40)
	origin, peer, lag := tc.nodes[0], tc.nodes[1], tc.nodes[2]
	require.True(t, tc.pushVia(origin, work, "o/r.git", "refs/heads/main", zero, c1).OK)
	require.True(t, tc.pushVia(origin, work, "o/r.git", "refs/heads/secret", zero, secret).OK)
	waitAll(t, tc, func(n *testNode) bool { return gitRef(t, n, "o/r.git", "refs/heads/secret") == secret })

	renamedAwayThenTaken(t, tc, tc.createVia, func(n *testNode) {
		require.True(t, tc.pushVia(n, work, "o/r.git", "refs/heads/main", zero, c2).OK)
	})
	root := origin.r.root
	settle(t, origin)
	require.NoError(t, os.Rename(filepath.Join(root, "o", "r.git"), filepath.Join(root, "o", "new.git")))
	require.NoError(t, origin.r.proposeRepoRename(context.Background(), "o/r.git", "o/new.git"))
	require.Eventually(t, func() bool { return gitRef(t, peer, "o/new.git", "refs/heads/main") == c2 }, 10*time.Second, 20*time.Millisecond)

	catchUp(t, origin, lag)
	for _, n := range tc.nodes {
		assert.Equal(t, c2, gitRef(t, n, "o/new.git", "refs/heads/main"), n.slot)
		assert.Empty(t, gitRef(t, n, "o/new.git", "refs/heads/secret"), "%s: the deleted o/q's branch is served under o/new.git", n.slot)
		assert.False(t, hasObject(n, "o/new.git", c1), "%s: the deleted o/q's commit is in o/new.git", n.slot)
		assert.False(t, fetchableByID(t, n, "o/new.git", secret), "%s: the deleted o/q's commit is fetched by its id from o/new.git", n.slot)
		assert.False(t, dirExists(n, "o/r.git"), "%s: a directory is left under the freed o/r.git", n.slot)
	}
	same, err := lag.r.projector.Verify(context.Background(), "o/new.git")
	require.NoError(t, err)
	assert.True(t, same)
	assert.Equal(t, 1, setAsideCount(t, lag), "the deleted o/q's directory is set aside on the lagging node")
}

// The same sequence, the new repository in the other object format: the
// lagging node moved the deleted sha1 repository under o/new.git, whose
// sha256 projection then failed at every pass, and kept serving the
// deleted repository (#181).
func TestARepositoryRenamedAwayIsNotMovedUnderTheNameOfOneInTheOtherObjectFormat(t *testing.T) {
	tc := newTestCluster(t, "node-a", "node-b", "node-c")
	raisePayload(t, tc)
	work := t.TempDir()
	run(t, work, "init", "-q")
	c1 := commit(t, work, "one")
	secret := commit(t, work, "secret")
	work256 := t.TempDir()
	run(t, work256, "init", "-q", "--object-format=sha256")
	c2 := commit(t, work256, "two")
	zero := strings.Repeat("0", 40)
	origin, peer, lag := tc.nodes[0], tc.nodes[1], tc.nodes[2]
	require.True(t, tc.pushVia(origin, work, "o/r.git", "refs/heads/main", zero, c1).OK)
	require.True(t, tc.pushVia(origin, work, "o/r.git", "refs/heads/secret", zero, secret).OK)
	waitAll(t, tc, func(n *testNode) bool { return gitRef(t, n, "o/r.git", "refs/heads/secret") == secret })

	renamedAwayThenTaken(t, tc, tc.createVia256, func(n *testNode) {
		require.True(t, tc.pushVia(n, work256, "o/r.git", "refs/heads/main", strings.Repeat("0", 64), c2).OK)
	})
	root := origin.r.root
	settle(t, origin)
	require.NoError(t, os.Rename(filepath.Join(root, "o", "r.git"), filepath.Join(root, "o", "new.git")))
	require.NoError(t, origin.r.proposeRepoRename(context.Background(), "o/r.git", "o/new.git"))
	require.Eventually(t, func() bool { return gitRef(t, peer, "o/new.git", "refs/heads/main") == c2 }, 10*time.Second, 20*time.Millisecond)

	waitApplied(t, origin, lag)
	lag.r.projector.unfence()
	ok := assert.Eventually(t, func() bool { return gitRef(t, lag, "o/new.git", "refs/heads/main") == c2 }, 20*time.Second, 100*time.Millisecond,
		"the lagging node never projects o/new.git")
	if !ok {
		for _, l := range lag.r.projector.Lag() {
			t.Logf("lag: %s watermark=%d target=%d pending=%v error=%q", l.Repo, l.Watermark, l.Target, l.Pending, l.Error)
		}
	}
	require.Eventually(t, func() bool { return len(lag.r.projector.Lag()) == 0 }, 20*time.Second, 50*time.Millisecond, "lag certifies: %v", lag.r.projector.Lag())
	assert.Empty(t, gitRef(t, lag, "o/new.git", "refs/heads/secret"))
	_, id, _ := origin.r.table.RefsWithID("o/new.git")
	require.NoError(t, lag.r.ReadRefusal("o/new.git", id))
	assert.Equal(t, 1, setAsideCount(t, lag), "the deleted sha1 repository's directory is set aside")
}

// A repository renamed away (o/r to o/q) and o/q deleted, then a new o/r
// created, while a node is behind: the old o/r directory that node still
// holds, the deleted o/q's, is not the new o/r's, and projecting the new
// repository into it kept the deleted one's objects there (#181).
func TestARepositoryRenamedAwayIsNotReusedByTheOneCreatedUnderItsName(t *testing.T) {
	tc := newTestCluster(t, "node-a", "node-b", "node-c")
	raisePayload(t, tc)
	work := t.TempDir()
	run(t, work, "init", "-q")
	c1 := commit(t, work, "one")
	secret := commit(t, work, "secret")
	run(t, work, "checkout", "-q", "--orphan", "fresh")
	c2 := commit(t, work, "two")
	zero := strings.Repeat("0", 40)
	origin, lag := tc.nodes[0], tc.nodes[2]
	require.True(t, tc.pushVia(origin, work, "o/r.git", "refs/heads/main", zero, c1).OK)
	require.True(t, tc.pushVia(origin, work, "o/r.git", "refs/heads/secret", zero, secret).OK)
	waitAll(t, tc, func(n *testNode) bool { return gitRef(t, n, "o/r.git", "refs/heads/secret") == secret })

	renamedAwayThenTaken(t, tc, tc.createVia, func(n *testNode) {
		require.True(t, tc.pushVia(n, work, "o/r.git", "refs/heads/main", zero, c2).OK)
	})

	catchUp(t, origin, lag)
	for _, n := range tc.nodes {
		assert.Equal(t, c2, gitRef(t, n, "o/r.git", "refs/heads/main"), n.slot)
		assert.Empty(t, gitRef(t, n, "o/r.git", "refs/heads/secret"), n.slot)
		assert.False(t, hasObject(n, "o/r.git", c1), "%s: the deleted o/q's commit is in the new o/r.git", n.slot)
		assert.False(t, fetchableByID(t, n, "o/r.git", secret), "%s: the deleted o/q's commit is fetched by its id from the new o/r.git", n.slot)
	}
	assert.Equal(t, 1, setAsideCount(t, lag), "the deleted o/q's directory is set aside on the lagging node")
}

// An owner renamed (alice to bob) after one of its repositories was renamed
// (x to y) and the name taken by a new repository, while a node is behind:
// both moves land at the same index, and the old x directory belongs to
// bob/y.git, never to bob/x.git (#181).
func TestAnOwnerRenameAfterARenameAndARecreationKeepsEachRepositoryInItsDirectory(t *testing.T) {
	tc := newTestCluster(t, "node-a", "node-b", "node-c")
	raisePayload(t, tc)
	work := t.TempDir()
	run(t, work, "init", "-q")
	c1 := commit(t, work, "one")
	secret := commit(t, work, "secret")
	run(t, work, "checkout", "-q", "--orphan", "fresh")
	c2 := commit(t, work, "two")
	zero := strings.Repeat("0", 40)
	origin, peer, lag := tc.nodes[0], tc.nodes[1], tc.nodes[2]
	require.True(t, tc.pushVia(origin, work, "alice/x.git", "refs/heads/main", zero, c1).OK)
	require.True(t, tc.pushVia(origin, work, "alice/x.git", "refs/heads/secret", zero, secret).OK)
	waitAll(t, tc, func(n *testNode) bool { return gitRef(t, n, "alice/x.git", "refs/heads/secret") == secret })
	before, err := os.Stat(filepath.Join(lag.r.root, "alice", "x.git"))
	require.NoError(t, err)

	lag.r.projector.refence()
	ctx := context.Background()
	root := origin.r.root
	settle(t, origin)
	require.NoError(t, os.Rename(filepath.Join(root, "alice", "x.git"), filepath.Join(root, "alice", "y.git")))
	require.NoError(t, origin.r.proposeRepoRename(ctx, "alice/x.git", "alice/y.git"))
	require.Eventually(t, func() bool { return gitRef(t, peer, "alice/y.git", "refs/heads/secret") == secret }, 10*time.Second, 20*time.Millisecond)
	tc.createVia(origin, "alice/x.git")
	require.True(t, tc.pushVia(origin, work, "alice/x.git", "refs/heads/main", zero, c2).OK)
	require.Eventually(t, func() bool { return gitRef(t, peer, "alice/x.git", "refs/heads/main") == c2 }, 10*time.Second, 20*time.Millisecond)
	settle(t, origin)
	require.NoError(t, os.Rename(filepath.Join(root, "alice"), filepath.Join(root, "bob")))
	require.NoError(t, origin.r.proposeOwnerRename(ctx, "alice", "bob"))
	require.Eventually(t, func() bool {
		return gitRef(t, peer, "bob/x.git", "refs/heads/main") == c2 && gitRef(t, peer, "bob/y.git", "refs/heads/secret") == secret
	}, 10*time.Second, 20*time.Millisecond)

	catchUp(t, origin, lag)
	for _, n := range tc.nodes {
		assert.Equal(t, c2, gitRef(t, n, "bob/x.git", "refs/heads/main"), n.slot)
		assert.Equal(t, secret, gitRef(t, n, "bob/y.git", "refs/heads/secret"), n.slot)
		assert.Empty(t, gitRef(t, n, "bob/x.git", "refs/heads/secret"), "%s: bob/y's branch is served under bob/x.git", n.slot)
		assert.False(t, hasObject(n, "bob/x.git", c1), "%s: bob/y's commit is in bob/x.git", n.slot)
		assert.False(t, fetchableByID(t, n, "bob/x.git", secret), "%s: bob/y's commit is fetched by its id from bob/x.git", n.slot)
	}
	after, err := os.Stat(filepath.Join(lag.r.root, "bob", "y.git"))
	require.NoError(t, err)
	assert.True(t, os.SameFile(before, after), "the old x directory reached bob/y.git on the lagging node")
	assert.Zero(t, setAsideCount(t, lag), "nothing set aside")
}

// An owner renamed (alice to bob) after one of its repositories was renamed
// (x to y), while a node is behind: the owner's rename moved the key x
// left, which holds no repository, and the node moved the old x directory
// to that key, bob/x.git, a name the database does not have, instead of
// bob/y.git (#181). The rename moves the keys holding a repository only.
func TestAnOwnerRenameMovesOnlyTheKeysThatHoldARepository(t *testing.T) {
	tc := newTestCluster(t, "node-a", "node-b", "node-c")
	raisePayload(t, tc)
	work := t.TempDir()
	run(t, work, "init", "-q")
	c1 := commit(t, work, "one")
	zero := strings.Repeat("0", 40)
	origin, peer, lag := tc.nodes[0], tc.nodes[1], tc.nodes[2]
	require.True(t, tc.pushVia(origin, work, "alice/x.git", "refs/heads/main", zero, c1).OK)
	waitAll(t, tc, func(n *testNode) bool { return gitRef(t, n, "alice/x.git", "refs/heads/main") == c1 })

	lag.r.projector.refence()
	ctx := context.Background()
	root := origin.r.root
	settle(t, origin)
	require.NoError(t, os.Rename(filepath.Join(root, "alice", "x.git"), filepath.Join(root, "alice", "y.git")))
	// As Forgejo does, the wiki's key moves with the repository's, though
	// no wiki was ever created.
	require.NoError(t, origin.r.proposeRepoMoves(ctx, [][2]string{{"alice/x.git", "alice/y.git"}, {"alice/x.wiki.git", "alice/y.wiki.git"}}))
	require.Eventually(t, func() bool { return gitRef(t, peer, "alice/y.git", "refs/heads/main") == c1 }, 10*time.Second, 20*time.Millisecond)
	settle(t, origin)
	require.NoError(t, os.Rename(filepath.Join(root, "alice"), filepath.Join(root, "bob")))
	require.NoError(t, origin.r.proposeOwnerRename(ctx, "alice", "bob"))
	require.Eventually(t, func() bool { return gitRef(t, peer, "bob/y.git", "refs/heads/main") == c1 }, 10*time.Second, 20*time.Millisecond)
	for _, key := range []string{"bob/x.git", "bob/y.wiki.git"} {
		from, idx, ok := origin.r.table.RenamedFrom(key)
		assert.False(t, ok, "the owner's rename moved %s into %s at %d", from, key, idx)
	}

	catchUp(t, origin, lag)
	for _, n := range tc.nodes {
		assert.Equal(t, c1, gitRef(t, n, "bob/y.git", "refs/heads/main"), n.slot)
		assert.False(t, dirExists(n, "bob/x.git"), "%s: a directory under bob/x.git, a name the database does not have", n.slot)
	}
	assert.Zero(t, setAsideCount(t, lag), "nothing set aside: the old x directory reached bob/y.git")
}

// A node behind a chain of renames that comes back (a to b, b to a) and
// then goes on (a to c) still moves the repository's own directory to c:
// the chain left a only to come back (#43).
func TestAChainOfRenamesThatComesBackStillTakesItsDirectoryOnALaggingNode(t *testing.T) {
	tc := newTestCluster(t, "node-a", "node-b", "node-c")
	raisePayload(t, tc)
	work := t.TempDir()
	run(t, work, "init", "-q")
	c1 := commit(t, work, "one")
	zero := strings.Repeat("0", 40)
	origin, lag := tc.nodes[0], tc.nodes[2]
	require.True(t, tc.pushVia(origin, work, "s/a.git", "refs/heads/main", zero, c1).OK)
	waitAll(t, tc, func(n *testNode) bool { return gitRef(t, n, "s/a.git", "refs/heads/main") == c1 })
	require.Eventually(t, func() bool { return lag.r.projector.Projected("s/a.git") > 0 }, 10*time.Second, 20*time.Millisecond)
	before, err := os.Stat(filepath.Join(lag.r.root, "s", "a.git"))
	require.NoError(t, err)

	lag.r.projector.refence()
	for _, m := range [][2]string{{"s/a.git", "s/b.git"}, {"s/b.git", "s/a.git"}, {"s/a.git", "s/c.git"}} {
		settle(t, origin)
		renameOn(t, origin, m[0], m[1])
	}

	catchUp(t, origin, lag)
	assert.Equal(t, c1, gitRef(t, lag, "s/c.git", "refs/heads/main"))
	after, err := os.Stat(filepath.Join(lag.r.root, "s", "c.git"))
	require.NoError(t, err)
	assert.True(t, os.SameFile(before, after), "the repository's own directory reached s/c.git")
	for _, freed := range []string{"s/a.git", "s/b.git"} {
		assert.False(t, dirExists(lag, freed), "%s holds a directory on the lagging node", freed)
	}
	assert.Zero(t, setAsideCount(t, lag), "nothing set aside")
}

// An owner renamed on a node whose projector had not yet moved the
// directory of one of its repositories renamed through another node (x to
// y): Forgejo moves the owner's whole directory, and the old x directory
// lands under bob/x.git, a key that holds nothing. A repository created
// later under bob/x through another node was projected into it on that
// node, and served bob/y's objects by their ids (#181).
func TestAnOwnerRenamedOnANodeBehindARenameLeavesNoDirectoryToTheNextRepository(t *testing.T) {
	tc := newTestCluster(t, "node-a", "node-b", "node-c")
	raisePayload(t, tc)
	work := t.TempDir()
	run(t, work, "init", "-q")
	c1 := commit(t, work, "one")
	secret := commit(t, work, "secret")
	run(t, work, "checkout", "-q", "--orphan", "fresh")
	c2 := commit(t, work, "two")
	zero := strings.Repeat("0", 40)
	origin, other := tc.nodes[0], tc.nodes[1]
	require.True(t, tc.pushVia(other, work, "alice/x.git", "refs/heads/main", zero, c1).OK)
	require.True(t, tc.pushVia(other, work, "alice/x.git", "refs/heads/secret", zero, secret).OK)
	waitAll(t, tc, func(n *testNode) bool {
		return gitRef(t, n, "alice/x.git", "refs/heads/secret") == secret && n.r.projector.Projected("alice/x.git") > 0
	})

	// The origin's projector does not move the directory yet, as while a
	// transaction is in flight there.
	release := holdOn(t, origin, "alice/x.git", "alice/y.git")
	ctx := context.Background()
	settle(t, other)
	require.NoError(t, os.Rename(filepath.Join(other.r.root, "alice", "x.git"), filepath.Join(other.r.root, "alice", "y.git")))
	require.NoError(t, other.r.proposeRepoMoves(ctx, [][2]string{{"alice/x.git", "alice/y.git"}, {"alice/x.wiki.git", "alice/y.wiki.git"}}))
	waitApplied(t, other, origin)
	require.True(t, dirExists(origin, "alice/x.git"), "the origin's projector did not move the directory")
	require.NoError(t, os.Rename(filepath.Join(origin.r.root, "alice"), filepath.Join(origin.r.root, "bob")))
	require.NoError(t, origin.r.proposeOwnerRename(ctx, "alice", "bob"))
	release()
	waitAll(t, tc, func(n *testNode) bool { return gitRef(t, n, "bob/y.git", "refs/heads/secret") == secret })

	tc.createVia(other, "bob/x.git")
	require.True(t, tc.pushVia(other, work, "bob/x.git", "refs/heads/main", zero, c2).OK)
	waitAll(t, tc, func(n *testNode) bool { return gitRef(t, n, "bob/x.git", "refs/heads/main") == c2 })
	require.Eventually(t, func() bool { return len(origin.r.projector.Lag()) == 0 }, 20*time.Second, 50*time.Millisecond, "origin certifies: %v", origin.r.projector.Lag())
	for _, n := range tc.nodes {
		assert.Empty(t, gitRef(t, n, "bob/x.git", "refs/heads/secret"), "%s: bob/y's branch is served under bob/x.git", n.slot)
		assert.False(t, hasObject(n, "bob/x.git", c1), "%s: bob/y's commit is in bob/x.git", n.slot)
		assert.False(t, fetchableByID(t, n, "bob/x.git", secret), "%s: bob/y's commit is fetched by its id from bob/x.git", n.slot)
		assert.Equal(t, secret, gitRef(t, n, "bob/y.git", "refs/heads/secret"), n.slot)
	}
}
