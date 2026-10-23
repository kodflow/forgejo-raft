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

// SQL lost the row of a repository the table holds (failover, restore),
// and the operator adopted it as reconcile prescribes: the new row has a
// new ID, while the table and every stamp keep the old one, and a write
// without an ID (a mirror's sync) keeps them so. Every read, and the push
// that would carry the new ID, was refused on every node for good (#150).
// The adoption writes HEAD again under the new ID (bindAdoptedID in
// services/repository), which binds it on every node.
func TestAnAdoptedRepositoryIsServedUnderItsNewID(t *testing.T) {
	t.Parallel()
	tc := newTestCluster(t, "node-a", "node-b", "node-c")
	work := t.TempDir()
	run(t, work, "init", "-q")
	c1 := commit(t, work, "one")
	c2 := commit(t, work, "two")
	zero := strings.Repeat("0", 40)
	oldID := tc.repoID("alice/lost.git")
	require.True(t, tc.pushVia(tc.nodes[0], work, "alice/lost.git", "refs/heads/main", zero, c1).OK)
	newID := oldID + 41 // the adopted row
	dir := filepath.Join(tc.nodes[0].r.root, "alice", "lost.git")
	write := func(id int64, line clusterhook.Line) {
		req := &clusterhook.Request{
			Op: clusterhook.OpPrepared, Nonce: clusterhook.NewNonce(), GitDir: dir, RepoID: id,
			Lines: []clusterhook.Line{line},
		}
		resp := tc.nodes[0].r.Handle(t.Context(), req)
		require.True(t, resp.OK, resp.Message)
		req.Op = clusterhook.OpCommitted
		tc.nodes[0].r.Handle(t.Context(), req)
	}

	run(t, work, "push", "-q", dir, c2+":refs/tmp/in")
	run(t, dir, "update-ref", "-d", "refs/tmp/in")
	write(0, clusterhook.Line{Old: c1, New: c2, Ref: "refs/heads/main"})
	run(t, dir, "update-ref", "refs/heads/main", c2)
	waitAll(t, tc, func(n *testNode) bool { return gitRef(t, n, "alice/lost.git", "refs/heads/main") == c2 })
	for _, n := range tc.nodes {
		require.Eventually(t, func() bool { return len(n.r.projector.Lag()) == 0 }, 10*time.Second, 20*time.Millisecond, n.slot)
		require.Error(t, readRefusal(n, "alice/lost.git", newID), "%s: the stamp keeps the lost row's ID", n.slot)
	}

	// The adoption's HEAD, as it is, under the new ID.
	write(newID, clusterhook.Line{Old: zero, New: "ref:refs/heads/main", Ref: "HEAD"})
	for _, n := range tc.nodes {
		require.Eventually(t, func() bool {
			s, ok := readStamp(filepath.Join(n.r.root, "alice", "lost.git"))
			return ok && s.ID == newID
		}, 10*time.Second, 20*time.Millisecond, n.slot)
		require.NoError(t, readRefusal(n, "alice/lost.git", newID), n.slot)
		assert.Equal(t, c2, gitRef(t, n, "alice/lost.git", "refs/heads/main"), n.slot)
	}
}
