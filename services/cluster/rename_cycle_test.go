// Copyright 2026 The Forgejo Authors. All rights reserved.
// SPDX-License-Identifier: GPL-3.0-or-later

package cluster

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// parkedCount counts the directories n parked to break a cycle of renames.
func parkedCount(t *testing.T, n *testNode) int {
	t.Helper()
	entries, err := os.ReadDir(filepath.Join(n.r.root, staleDirName))
	if os.IsNotExist(err) {
		return 0
	}
	require.NoError(t, err)
	count := 0
	for _, e := range entries {
		if strings.HasPrefix(e.Name(), parkedPrefix) {
			count++
		}
	}
	return count
}

// renameOwnerOn renames an owner on node n as Forgejo does: the owner's
// directories held on n, the owner's directory moved, then the entry.
func renameOwnerOn(t *testing.T, n *testNode, from, to string) {
	t.Helper()
	release, err := n.r.holdOwnerDirectories(t.Context(), []string{from, to})
	require.NoError(t, err)
	defer release()
	require.NoError(t, os.Rename(filepath.Join(n.r.root, from), filepath.Join(n.r.root, to)))
	require.NoError(t, n.r.proposeOwnerRename(t.Context(), from, to))
}

// cycleRepos pushes one orphan commit to each key through origin, waits
// until every node projected them all, and returns the commits and lag's
// directories, in the order of keys.
func cycleRepos(t *testing.T, tc *testCluster, keys ...string) ([]string, []os.FileInfo) {
	t.Helper()
	work := t.TempDir()
	run(t, work, "init", "-q")
	zero := strings.Repeat("0", 40)
	origin, lag := tc.nodes[0], tc.nodes[2]
	commits := make([]string, len(keys))
	for i, key := range keys {
		run(t, work, "checkout", "-q", "--orphan", "b"+string(rune('a'+i)))
		commits[i] = commit(t, work, key)
		require.True(t, tc.pushVia(origin, work, key, "refs/heads/main", zero, commits[i]).OK)
	}
	waitAll(t, tc, func(n *testNode) bool {
		for i, key := range keys {
			if gitRef(t, n, key, "refs/heads/main") != commits[i] || n.r.projector.Projected(key) == 0 {
				return false
			}
		}
		return true
	})
	dirs := make([]os.FileInfo, len(keys))
	for i, key := range keys {
		st, err := os.Stat(filepath.Join(lag.r.root, filepath.FromSlash(key)))
		require.NoError(t, err)
		dirs[i] = st
	}
	return commits, dirs
}

// lagCertifies lifts lag's fence once it applied what origin applied, and
// waits until it certified every key, reporting where it stands if not.
func lagCertifies(t *testing.T, origin, lag *testNode, keys ...string) {
	t.Helper()
	waitApplied(t, origin, lag)
	lag.r.projector.unfence()
	if assert.Eventually(t, func() bool { return lag.r.projector.Ready() && len(lag.r.projector.Lag()) == 0 }, 20*time.Second, 50*time.Millisecond, "lag certifies") {
		return
	}
	t.Logf("lag Ready=%v Lag=%+v", lag.r.projector.Ready(), lag.r.projector.Lag())
	for _, k := range keys {
		s, known := readStamp(filepath.Join(lag.r.root, filepath.FromSlash(k)))
		t.Logf("lag %s main=%.8s stamp(known=%v)=%s@%d Writable=%v ReadRefusal=%v", k, gitRef(t, lag, k, "refs/heads/main"), known, s.Repo, s.Watermark, lag.r.projector.Writable(k), lag.r.ReadRefusal(k, 0))
	}
	t.FailNow()
}

// othersCertify waits until every node but lag applied what origin applied
// and certified every key. They are not fenced and project the renames as
// they come, but the last ones may still be on their way when lag
// certified: a check of their directories then finds one where a rename
// found it, as node-b's under tmp/x.git in a run of the CI.
func othersCertify(t *testing.T, tc *testCluster, origin, lag *testNode) {
	t.Helper()
	for _, n := range tc.nodes {
		if n == lag {
			continue
		}
		waitApplied(t, origin, n)
		settle(t, n)
	}
}

// sameDir asserts that key's directory on n is the directory want was.
func sameDir(t *testing.T, n *testNode, key string, want os.FileInfo) {
	t.Helper()
	st, err := os.Stat(filepath.Join(n.r.root, filepath.FromSlash(key)))
	require.NoError(t, err)
	assert.True(t, os.SameFile(want, st), "%s is not the directory its repository had on the lagging node", key)
}

// Two repositories swap their names through a temporary one (a to t, b to
// a, t to b) while a node is behind: each directory still holds its own
// repository, certified under its old key, and each projection waited for
// the other key's to take its directory first (movesOn), which never
// happened. Both keys stayed refused for good on that node, through every
// pass and every start (#392).
func TestASwapOfTwoNamesOnALaggingNodeIsProjected(t *testing.T) {
	tc := newTestCluster(t, "node-a", "node-b", "node-c")
	raisePayload(t, tc)
	origin, lag := tc.nodes[0], tc.nodes[2]
	c, dirs := cycleRepos(t, tc, "s/a.git", "s/b.git")

	lag.r.projector.refence()
	for _, m := range [][2]string{{"s/a.git", "s/t.git"}, {"s/b.git", "s/a.git"}, {"s/t.git", "s/b.git"}} {
		settle(t, origin)
		renameOn(t, origin, m[0], m[1])
	}
	lagCertifies(t, origin, lag, "s/a.git", "s/b.git")
	othersCertify(t, tc, origin, lag)

	for _, n := range tc.nodes {
		assert.Equal(t, c[1], gitRef(t, n, "s/a.git", "refs/heads/main"), n.slot)
		assert.Equal(t, c[0], gitRef(t, n, "s/b.git", "refs/heads/main"), n.slot)
		assert.False(t, hasObject(n, "s/a.git", c[0]), "%s: s/b's commit is in s/a.git", n.slot)
		assert.False(t, hasObject(n, "s/b.git", c[1]), "%s: s/a's commit is in s/b.git", n.slot)
		assert.False(t, dirExists(n, "s/t.git"), "%s: a directory under the waypoint", n.slot)
	}
	for _, k := range []string{"s/a.git", "s/b.git"} {
		assert.True(t, lag.r.projector.Writable(k), k)
		require.NoError(t, lag.r.ReadRefusal(k, 0), k)
	}
	sameDir(t, lag, "s/b.git", dirs[0])
	sameDir(t, lag, "s/a.git", dirs[1])
	assert.Zero(t, setAsideCount(t, lag), "nothing set aside nor left parked: no repository fetched again")
}

// The same swap through the owners (alice to tmp, bob to alice, tmp to
// bob), as a user or organization rename does: alice/x.git and bob/x.git
// wait for each other on a node behind, while alice's other repository,
// which no key waits for, goes on to bob.
func TestAnOwnerSwapOnALaggingNodeIsProjected(t *testing.T) {
	tc := newTestCluster(t, "node-a", "node-b", "node-c")
	raisePayload(t, tc)
	origin, lag := tc.nodes[0], tc.nodes[2]
	c, dirs := cycleRepos(t, tc, "alice/x.git", "bob/x.git", "alice/only.git")

	lag.r.projector.refence()
	for _, m := range [][2]string{{"alice", "tmp"}, {"bob", "alice"}, {"tmp", "bob"}} {
		settle(t, origin)
		renameOwnerOn(t, origin, m[0], m[1])
	}
	lagCertifies(t, origin, lag, "alice/x.git", "bob/x.git", "bob/only.git")
	othersCertify(t, tc, origin, lag)

	for _, n := range tc.nodes {
		assert.Equal(t, c[1], gitRef(t, n, "alice/x.git", "refs/heads/main"), n.slot)
		assert.Equal(t, c[0], gitRef(t, n, "bob/x.git", "refs/heads/main"), n.slot)
		assert.Equal(t, c[2], gitRef(t, n, "bob/only.git", "refs/heads/main"), n.slot)
		assert.False(t, hasObject(n, "alice/x.git", c[0]), "%s: bob/x's commit is in alice/x.git", n.slot)
		assert.False(t, hasObject(n, "bob/x.git", c[1]), "%s: alice/x's commit is in bob/x.git", n.slot)
		for _, freed := range []string{"alice/only.git", "tmp/x.git", "tmp/only.git"} {
			assert.False(t, dirExists(n, freed), "%s: a directory under %s", n.slot, freed)
		}
	}
	sameDir(t, lag, "bob/x.git", dirs[0])
	sameDir(t, lag, "alice/x.git", dirs[1])
	sameDir(t, lag, "bob/only.git", dirs[2])
	assert.Zero(t, setAsideCount(t, lag), "nothing set aside nor left parked: no repository fetched again")
}

// Three repositories rotate their names (a to t, b to a, c to b, t to c)
// while a node is behind: a waits for c, c for b and b for a.
func TestARotationOfThreeNamesOnALaggingNodeIsProjected(t *testing.T) {
	tc := newTestCluster(t, "node-a", "node-b", "node-c")
	raisePayload(t, tc)
	origin, lag := tc.nodes[0], tc.nodes[2]
	c, dirs := cycleRepos(t, tc, "s/a.git", "s/b.git", "s/c.git")

	lag.r.projector.refence()
	for _, m := range [][2]string{{"s/a.git", "s/t.git"}, {"s/b.git", "s/a.git"}, {"s/c.git", "s/b.git"}, {"s/t.git", "s/c.git"}} {
		settle(t, origin)
		renameOn(t, origin, m[0], m[1])
	}
	lagCertifies(t, origin, lag, "s/a.git", "s/b.git", "s/c.git")
	othersCertify(t, tc, origin, lag)

	for _, n := range tc.nodes {
		assert.Equal(t, c[1], gitRef(t, n, "s/a.git", "refs/heads/main"), n.slot)
		assert.Equal(t, c[2], gitRef(t, n, "s/b.git", "refs/heads/main"), n.slot)
		assert.Equal(t, c[0], gitRef(t, n, "s/c.git", "refs/heads/main"), n.slot)
		assert.False(t, dirExists(n, "s/t.git"), "%s: a directory under the waypoint", n.slot)
	}
	sameDir(t, lag, "s/c.git", dirs[0])
	sameDir(t, lag, "s/a.git", dirs[1])
	sameDir(t, lag, "s/b.git", dirs[2])
	assert.Zero(t, setAsideCount(t, lag), "nothing set aside nor left parked: no repository fetched again")
}

// A directory parked to break the cycle survives a start: b's directory
// cannot move into a yet (a transaction of b in flight), so the pass parks
// a's and stops there; the node then starts again, with nothing of the
// projector's state kept, and the parked directory still reaches b.
func TestAParkedDirectoryIsTakenAfterAStart(t *testing.T) {
	tc := newTestCluster(t, "node-a", "node-b", "node-c")
	raisePayload(t, tc)
	origin, lag := tc.nodes[0], tc.nodes[2]
	c, dirs := cycleRepos(t, tc, "s/a.git", "s/b.git")

	lag.r.projector.refence()
	for _, m := range [][2]string{{"s/a.git", "s/t.git"}, {"s/b.git", "s/a.git"}, {"s/t.git", "s/b.git"}} {
		settle(t, origin)
		renameOn(t, origin, m[0], m[1])
	}
	waitApplied(t, origin, lag)
	release := holdOn(t, lag, "s/b.git")
	lag.r.projector.unfence()
	require.Eventually(t, func() bool { return parkedCount(t, lag) == 1 && !dirExists(lag, "s/a.git") }, 10*time.Second, 20*time.Millisecond, "a's directory parked")
	// While b's directory waits to move, a's key takes no pack: created
	// empty, it would never take that directory (#100).
	assert.True(t, lag.r.projector.awaitsRename("s/a.git"))
	// The parked directory holds the repository under b now, and a's is
	// not that one.
	assert.NotEmpty(t, lag.r.projector.parked("s/b.git"))
	assert.Empty(t, lag.r.projector.parked("s/a.git"))
	assert.Empty(t, lag.r.projector.parked("s/t.git"))

	lag.r.projector.refence()
	release()
	lagCertifies(t, origin, lag, "s/a.git", "s/b.git")
	assert.Equal(t, c[1], gitRef(t, lag, "s/a.git", "refs/heads/main"))
	assert.Equal(t, c[0], gitRef(t, lag, "s/b.git", "refs/heads/main"))
	sameDir(t, lag, "s/b.git", dirs[0])
	sameDir(t, lag, "s/a.git", dirs[1])
	assert.Zero(t, setAsideCount(t, lag), "nothing set aside nor left parked: no repository fetched again")
}

// A parked directory never reaches a repository created anew under the key
// it waited for: b's directory cannot move into a yet, so a's stays parked,
// and meanwhile the repository under b (a's) is deleted and a new one is
// created there. The new b is projected afresh, and the parked directory,
// the deleted repository's, stays where it is, served by no key.
func TestAParkedDirectoryNeverReachesARepositoryCreatedAnewUnderItsKey(t *testing.T) {
	tc := newTestCluster(t, "node-a", "node-b", "node-c")
	raisePayload(t, tc)
	origin, lag := tc.nodes[0], tc.nodes[2]
	c, dirs := cycleRepos(t, tc, "s/a.git", "s/b.git")

	lag.r.projector.refence()
	for _, m := range [][2]string{{"s/a.git", "s/t.git"}, {"s/b.git", "s/a.git"}, {"s/t.git", "s/b.git"}} {
		settle(t, origin)
		renameOn(t, origin, m[0], m[1])
	}
	waitApplied(t, origin, lag)
	release := holdOn(t, lag, "s/b.git")
	lag.r.projector.unfence()
	require.Eventually(t, func() bool { return parkedCount(t, lag) == 1 && !dirExists(lag, "s/a.git") }, 10*time.Second, 20*time.Millisecond, "a's directory parked")

	settle(t, origin)
	require.NoError(t, os.RemoveAll(filepath.Join(origin.r.root, "s", "b.git")))
	require.NoError(t, origin.r.proposeRepoDelete(t.Context(), "s/b.git", 1))
	tc.createVia(origin, "s/b.git")
	work := t.TempDir()
	run(t, work, "init", "-q")
	fresh := commit(t, work, "fresh")
	require.True(t, tc.pushVia(origin, work, "s/b.git", "refs/heads/main", strings.Repeat("0", 40), fresh).OK)
	waitApplied(t, origin, lag)
	assert.Empty(t, lag.r.projector.parked("s/b.git"), "the parked directory is the deleted repository's")

	release()
	require.Eventually(t, func() bool { return len(lag.r.projector.Lag()) == 0 }, 20*time.Second, 50*time.Millisecond, "lag certifies: %v", lag.r.projector.Lag())
	assert.Equal(t, c[1], gitRef(t, lag, "s/a.git", "refs/heads/main"))
	sameDir(t, lag, "s/a.git", dirs[1])
	assert.Equal(t, fresh, gitRef(t, lag, "s/b.git", "refs/heads/main"))
	st, err := os.Stat(filepath.Join(lag.r.root, "s", "b.git"))
	require.NoError(t, err)
	assert.False(t, os.SameFile(dirs[0], st), "the deleted repository's directory is under the new s/b.git")
	assert.False(t, hasObject(lag, "s/b.git", c[0]), "the deleted repository's commit is in the new s/b.git")
	assert.Equal(t, 1, parkedCount(t, lag), "the parked directory is kept, never removed")
}
