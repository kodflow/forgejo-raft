// Copyright 2026 The Forgejo Authors. All rights reserved.
// SPDX-License-Identifier: GPL-3.0-or-later

package cluster

import (
	"path/filepath"
	"strings"
	"testing"
	"time"

	"forgejo.org/modules/clusterhook"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// alice/secret.git renamed to alice/secret2.git, then alice/secret.git
// created anew, while node-c has not projected the rename: node-c's
// alice/secret.git directory is still the renamed repository, and a read
// under the new repository's permissions must be refused there (#82),
// never on the members that hold the right directories.
func TestAReadOfANameARenameFreedWaitsForTheProjection(t *testing.T) {
	t.Parallel()
	tc := newTestCluster(t, "node-a", "node-b", "node-c")
	work := t.TempDir()
	run(t, work, "init", "-q")
	c1 := commit(t, work, "private")
	c2 := commit(t, work, "public")
	zero := strings.Repeat("0", 40)
	origin, lag := tc.nodes[0], tc.nodes[2]
	require.True(t, tc.pushVia(origin, work, "alice/secret.git", "refs/heads/main", zero, c1).OK)
	for _, n := range tc.nodes {
		require.Eventually(t, func() bool { return n.r.projector.Projected("alice/secret.git") > 0 }, 10*time.Second, 20*time.Millisecond)
		require.NoError(t, n.r.ReadRefusal("alice/secret.git", 0))
	}

	lag.r.projector.refence()
	renameOn(t, origin, "alice/secret.git", "alice/secret2.git")
	require.NoError(t, origin.r.ReadRefusal("alice/secret2.git", 0), "the renamed directory moved in on the origin")
	dir := filepath.Join(origin.r.root, "alice", "secret.git")
	run(t, origin.r.root, "init", "-q", "--bare", dir)
	run(t, work, "push", "-q", dir, c2+":refs/tmp/incoming")
	run(t, dir, "update-ref", "-d", "refs/tmp/incoming")
	req := &clusterhook.Request{
		Op: clusterhook.OpPrepared, Nonce: clusterhook.NewNonce(), GitDir: dir, RepoID: 2,
		Lines: []clusterhook.Line{{Old: zero, New: c2, Ref: "refs/heads/main"}},
	}
	resp := origin.r.Handle(t.Context(), req)
	require.True(t, resp.OK, resp.Message)
	run(t, dir, "update-ref", "refs/heads/main", c2)
	req.Op = clusterhook.OpCommitted
	origin.r.Handle(t.Context(), req)
	applied, _ := origin.r.table.Applied()
	require.Eventually(t, func() bool {
		pos, _ := lag.r.table.Applied()
		return pos.Index >= applied.Index
	}, 10*time.Second, 20*time.Millisecond)

	assert.Equal(t, c1, gitRef(t, lag, "alice/secret.git", "refs/heads/main"), "node-c still holds the renamed repository")
	err := lag.r.ReadRefusal("alice/secret.git", 0)
	require.Error(t, err, "node-c serves the renamed repository under the new one's name")
	assert.Contains(t, err.Error(), "has not projected the last rename")
	for _, n := range tc.nodes[:2] {
		// The rename and the push returned once acknowledged: node-b
		// holds the right directories once its projector moved them
		// (#231).
		settle(t, n)
		assert.NoError(t, n.r.ReadRefusal("alice/secret.git", 0), n.slot)
		assert.NoError(t, n.r.ReadRefusal("alice/secret2.git", 0), n.slot)
	}

	lag.r.projector.unfence()
	require.Eventually(t, lag.r.projector.Ready, 15*time.Second, 20*time.Millisecond, "node-c ready")
	assert.Equal(t, c2, gitRef(t, lag, "alice/secret.git", "refs/heads/main"))
	assert.Equal(t, c1, gitRef(t, lag, "alice/secret2.git", "refs/heads/main"))
	assert.NoError(t, lag.r.ReadRefusal("alice/secret.git", 0))
	assert.NoError(t, lag.r.ReadRefusal("alice/secret2.git", 0))
}

// The same rename and recreation while node-c is cut off from the others:
// its table never learns of the rename, and its alice/secret.git directory
// is still the renamed private repository. The database it shares with the
// others gives the name to the new repository, under another ID: the read
// is refused because the directory holds another repository by its ID, not
// by the table (#82).
func TestAReadOfANameARenameFreedIsRefusedOnAnIsolatedNode(t *testing.T) {
	t.Parallel()
	tc := newTestCluster(t, "node-a", "node-b", "node-c")
	work := t.TempDir()
	run(t, work, "init", "-q")
	c1 := commit(t, work, "private")
	c2 := commit(t, work, "public")
	zero := strings.Repeat("0", 40)
	origin, lag := tc.nodes[0], tc.nodes[2]
	oldID := tc.repoID("alice/secret.git")
	require.True(t, tc.pushVia(origin, work, "alice/secret.git", "refs/heads/main", zero, c1).OK)
	for _, n := range tc.nodes {
		require.Eventually(t, func() bool { return n.r.projector.Projected("alice/secret.git") > 0 }, 10*time.Second, 20*time.Millisecond)
		require.NoError(t, readRefusal(n, "alice/secret.git", oldID))
	}

	tc.mem.SetDown(lag.node, true)
	renameOn(t, origin, "alice/secret.git", "alice/secret2.git")
	newID := oldID + 100
	dir := filepath.Join(origin.r.root, "alice", "secret.git")
	run(t, origin.r.root, "init", "-q", "--bare", dir)
	run(t, work, "push", "-q", dir, c2+":refs/tmp/incoming")
	run(t, dir, "update-ref", "-d", "refs/tmp/incoming")
	req := &clusterhook.Request{
		Op: clusterhook.OpPrepared, Nonce: clusterhook.NewNonce(), GitDir: dir, RepoID: newID,
		Lines: []clusterhook.Line{{Old: zero, New: c2, Ref: "refs/heads/main"}},
	}
	resp := origin.r.Handle(t.Context(), req)
	require.True(t, resp.OK, resp.Message)
	run(t, dir, "update-ref", "refs/heads/main", c2)
	req.Op = clusterhook.OpCommitted
	origin.r.Handle(t.Context(), req)
	for _, n := range tc.nodes[:2] {
		require.Eventually(t, func() bool { return gitRef(t, n, "alice/secret.git", "refs/heads/main") == c2 }, 10*time.Second, 20*time.Millisecond, n.slot)
		assert.NoError(t, readRefusal(n, "alice/secret.git", newID), n.slot)
		assert.NoError(t, readRefusal(n, "alice/secret2.git", oldID), n.slot)
	}

	assert.Equal(t, c1, gitRef(t, lag, "alice/secret.git", "refs/heads/main"), "node-c still holds the renamed repository")
	_, renamed := lag.r.table.MovedAway("alice/secret.git")
	assert.False(t, renamed, "node-c's table never learnt of the rename")
	err := readRefusal(lag, "alice/secret.git", newID)
	require.Error(t, err, "node-c serves the renamed private repository under the new one's ID")
	assert.NoError(t, readRefusal(lag, "alice/secret.git", oldID), "the repository the directory holds")
	assert.NoError(t, readRefusal(lag, "alice/secret.git", 0), "no ID to compare")

	tc.mem.SetDown(lag.node, false)
	require.Eventually(t, func() bool { return gitRef(t, lag, "alice/secret.git", "refs/heads/main") == c2 }, 15*time.Second, 20*time.Millisecond)
	require.Eventually(t, func() bool { return readRefusal(lag, "alice/secret.git", newID) == nil }, 15*time.Second, 20*time.Millisecond)
	assert.Equal(t, c1, gitRef(t, lag, "alice/secret2.git", "refs/heads/main"))
	assert.NoError(t, readRefusal(lag, "alice/secret2.git", oldID))
}

func readRefusal(n *testNode, repo string, id int64) error { return n.r.ReadRefusal(repo, id) }
