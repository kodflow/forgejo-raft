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

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// renamedOnSetup starts three nodes holding o/a (main c1, a branch secret).
func renamedOnSetup(t *testing.T) (tc *testCluster, c1, secret string) {
	t.Helper()
	tc = newTestCluster(t, "node-a", "node-b", "node-c")
	raisePayload(t, tc)
	work := t.TempDir()
	run(t, work, "init", "-q")
	c1 = commit(t, work, "one")
	secret = commit(t, work, "secret")
	zero := strings.Repeat("0", 40)
	origin := tc.nodes[0]
	require.True(t, tc.pushVia(origin, work, "o/a.git", "refs/heads/main", zero, c1).OK)
	require.True(t, tc.pushVia(origin, work, "o/a.git", "refs/heads/secret", zero, secret).OK)
	waitAll(t, tc, func(n *testNode) bool { return gitRef(t, n, "o/a.git", "refs/heads/secret") == secret })
	return tc, c1, secret
}

// renamedOnThenTaken plays, on origin, o/a renamed to o/b, o/b renamed on
// to o/c, then a new repository created under the freed o/b, while lag is
// fenced: lag still holds the old o/a directory, which belongs to o/c.
// create creates the new o/b.
func renamedOnThenTaken(t *testing.T, tc *testCluster, create func(n *testNode, repo string)) {
	t.Helper()
	ctx := context.Background()
	origin, peer, lag := tc.nodes[0], tc.nodes[1], tc.nodes[2]
	root := origin.r.root
	lag.r.projector.refence()
	settle(t, origin)
	require.NoError(t, os.Rename(filepath.Join(root, "o", "a.git"), filepath.Join(root, "o", "b.git")))
	require.NoError(t, origin.r.proposeRepoRename(ctx, "o/a.git", "o/b.git"))
	require.Eventually(t, func() bool { return dirExists(peer, "o/b.git") && !dirExists(peer, "o/a.git") }, 10*time.Second, 20*time.Millisecond)
	settle(t, origin)
	require.NoError(t, os.Rename(filepath.Join(root, "o", "b.git"), filepath.Join(root, "o", "c.git")))
	require.NoError(t, origin.r.proposeRepoRename(ctx, "o/b.git", "o/c.git"))
	require.Eventually(t, func() bool { return dirExists(peer, "o/c.git") && !dirExists(peer, "o/b.git") }, 10*time.Second, 20*time.Millisecond)
	settle(t, origin)
	create(origin, "o/b.git")
	require.Eventually(t, func() bool { return dirExists(peer, "o/b.git") }, 10*time.Second, 20*time.Millisecond)
	settle(t, origin)
}

// A repository renamed (o/a to o/b), renamed on (o/b to o/c), and a new,
// empty repository created under the freed o/b, while a node is behind:
// the rename into o/b still names o/a as its origin, but the repository it
// brought left o/b since. The lagging node moved the old o/a directory,
// o/c's private repository, under the new o/b, certified it, and served
// o/c's objects there by their ids, for good (#204). The directory is set
// aside instead, and o/c fetched from the members.
func TestARepositoryRenamedOnIsNotMovedUnderTheNameItLeft(t *testing.T) {
	tc, c1, secret := renamedOnSetup(t)
	origin, lag := tc.nodes[0], tc.nodes[2]
	renamedOnThenTaken(t, tc, tc.createVia)

	waitApplied(t, origin, lag)
	assert.False(t, lag.r.projector.awaitsRename("o/b.git"), "the object channel waits for the old o/a directory to move under the new o/b.git")

	catchUp(t, origin, lag)
	for _, n := range tc.nodes {
		assert.Equal(t, c1, gitRef(t, n, "o/c.git", "refs/heads/main"), n.slot)
		assert.Equal(t, secret, gitRef(t, n, "o/c.git", "refs/heads/secret"), n.slot)
		assert.Empty(t, gitRef(t, n, "o/b.git", "refs/heads/secret"), "%s: o/c's branch is served under the new o/b.git", n.slot)
		assert.False(t, hasObject(n, "o/b.git", c1), "%s: o/c's commit is in the new o/b.git", n.slot)
		assert.False(t, fetchableByID(t, n, "o/b.git", secret), "%s: o/c's secret commit is fetched by its id from the new o/b.git", n.slot)
		assert.False(t, dirExists(n, "o/a.git"), "%s: a directory is left under the freed o/a.git", n.slot)
	}
	same, err := lag.r.projector.Verify(context.Background(), "o/b.git")
	require.NoError(t, err)
	assert.True(t, same)
	assert.Equal(t, 1, setAsideCount(t, lag), "the old o/a directory is set aside on the lagging node")
}

// The same sequence, the new o/b in the other object format: the lagging
// node moved the sha1 directory under it and certified it (HEAD alone),
// and the first push of the new repository then never projected there,
// its sha256 objects refused by the sha1 repository at every pass (#204).
func TestARepositoryRenamedOnIsNotMovedUnderTheNameItLeftInTheOtherObjectFormat(t *testing.T) {
	tc, c1, _ := renamedOnSetup(t)
	origin, lag := tc.nodes[0], tc.nodes[2]
	renamedOnThenTaken(t, tc, tc.createVia256)

	catchUp(t, origin, lag)
	assert.False(t, hasObject(lag, "o/b.git", c1), "o/c's commit is in the new o/b.git")
	work256 := t.TempDir()
	run(t, work256, "init", "-q", "--object-format=sha256")
	c2 := commit(t, work256, "two")
	require.True(t, tc.pushVia(origin, work256, "o/b.git", "refs/heads/main", strings.Repeat("0", 64), c2).OK)
	ok := assert.Eventually(t, func() bool { return gitRef(t, lag, "o/b.git", "refs/heads/main") == c2 }, 20*time.Second, 100*time.Millisecond,
		"the lagging node never projects the new o/b.git")
	if !ok {
		for _, l := range lag.r.projector.Lag() {
			t.Logf("lag: %s watermark=%d target=%d pending=%v error=%q", l.Repo, l.Watermark, l.Target, l.Pending, l.Error)
		}
	}
	require.Eventually(t, func() bool { return len(lag.r.projector.Lag()) == 0 }, 20*time.Second, 50*time.Millisecond, "lag certifies: %v", lag.r.projector.Lag())
	_, id, _ := origin.r.table.RefsWithID("o/b.git")
	require.NoError(t, lag.r.ReadRefusal("o/b.git", id))
	assert.Equal(t, 1, setAsideCount(t, lag), "the old sha1 o/a directory is set aside")
}

// The same sequence, the new o/b then renamed in turn (o/b to o/d): the
// chain back from o/d goes through o/b to o/a, and the lagging node moved
// the old o/a directory, o/c's, under o/d (#204).
func TestARepositoryRenamedOnIsNotMovedUnderTheNextRenameOfTheNameItLeft(t *testing.T) {
	tc, c1, secret := renamedOnSetup(t)
	origin, peer, lag := tc.nodes[0], tc.nodes[1], tc.nodes[2]
	renamedOnThenTaken(t, tc, tc.createVia)
	root := origin.r.root
	require.NoError(t, os.Rename(filepath.Join(root, "o", "b.git"), filepath.Join(root, "o", "d.git")))
	require.NoError(t, origin.r.proposeRepoRename(context.Background(), "o/b.git", "o/d.git"))
	require.Eventually(t, func() bool { return dirExists(peer, "o/d.git") && !dirExists(peer, "o/b.git") }, 10*time.Second, 20*time.Millisecond)
	settle(t, origin)

	catchUp(t, origin, lag)
	for _, n := range tc.nodes {
		assert.Equal(t, secret, gitRef(t, n, "o/c.git", "refs/heads/secret"), n.slot)
		assert.Empty(t, gitRef(t, n, "o/d.git", "refs/heads/secret"), "%s: o/c's branch is served under o/d.git", n.slot)
		assert.False(t, hasObject(n, "o/d.git", c1), "%s: o/c's commit is in o/d.git", n.slot)
		assert.False(t, fetchableByID(t, n, "o/d.git", secret), "%s: o/c's secret commit is fetched by its id from o/d.git", n.slot)
		assert.False(t, dirExists(n, "o/a.git"), "%s: a directory is left under the freed o/a.git", n.slot)
		assert.False(t, dirExists(n, "o/b.git"), "%s: a directory is left under the freed o/b.git", n.slot)
	}
	assert.Equal(t, 1, setAsideCount(t, lag), "the old o/a directory is set aside on the lagging node")
}

// The natural form, through three accounts: alice/x renamed to alice/y,
// the owner alice renamed to bob (alice/y to bob/y), and a new account
// alice creating a repository y, while a node is behind: the lagging node
// moved the old alice/x directory, bob/y's private repository, under the
// new account's alice/y (#204).
func TestAnOwnerRenamedAfterARenameLeavesNoDirectoryToTheNextAccountOfItsName(t *testing.T) {
	tc := newTestCluster(t, "node-a", "node-b", "node-c")
	raisePayload(t, tc)
	work := t.TempDir()
	run(t, work, "init", "-q")
	c1 := commit(t, work, "one")
	secret := commit(t, work, "secret")
	zero := strings.Repeat("0", 40)
	origin, peer, lag := tc.nodes[0], tc.nodes[1], tc.nodes[2]
	require.True(t, tc.pushVia(origin, work, "alice/x.git", "refs/heads/main", zero, c1).OK)
	require.True(t, tc.pushVia(origin, work, "alice/x.git", "refs/heads/secret", zero, secret).OK)
	waitAll(t, tc, func(n *testNode) bool { return gitRef(t, n, "alice/x.git", "refs/heads/secret") == secret })

	ctx := context.Background()
	root := origin.r.root
	lag.r.projector.refence()
	settle(t, origin)
	require.NoError(t, os.Rename(filepath.Join(root, "alice", "x.git"), filepath.Join(root, "alice", "y.git")))
	require.NoError(t, origin.r.proposeRepoRename(ctx, "alice/x.git", "alice/y.git"))
	require.Eventually(t, func() bool { return dirExists(peer, "alice/y.git") && !dirExists(peer, "alice/x.git") }, 10*time.Second, 20*time.Millisecond)
	settle(t, origin)
	require.NoError(t, os.Rename(filepath.Join(root, "alice"), filepath.Join(root, "bob")))
	require.NoError(t, origin.r.proposeOwnerRename(ctx, "alice", "bob"))
	require.Eventually(t, func() bool { return gitRef(t, peer, "bob/y.git", "refs/heads/secret") == secret }, 10*time.Second, 20*time.Millisecond)
	settle(t, origin)
	tc.createVia(origin, "alice/y.git")
	require.Eventually(t, func() bool { return dirExists(peer, "alice/y.git") }, 10*time.Second, 20*time.Millisecond)
	settle(t, origin)

	catchUp(t, origin, lag)
	for _, n := range tc.nodes {
		assert.Equal(t, secret, gitRef(t, n, "bob/y.git", "refs/heads/secret"), n.slot)
		assert.Empty(t, gitRef(t, n, "alice/y.git", "refs/heads/secret"), "%s: bob/y's branch is served under the new account's alice/y.git", n.slot)
		assert.False(t, hasObject(n, "alice/y.git", c1), "%s: bob/y's commit is in the new account's alice/y.git", n.slot)
		assert.False(t, fetchableByID(t, n, "alice/y.git", secret), "%s: bob/y's secret commit is fetched by its id from alice/y.git", n.slot)
		assert.False(t, dirExists(n, "alice/x.git"), "%s: a directory is left under the freed alice/x.git", n.slot)
	}
	assert.Equal(t, 1, setAsideCount(t, lag), "the old alice/x directory is set aside on the lagging node")
}
