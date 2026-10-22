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
		require.NoError(t, n.r.ReadRefusal("alice/secret.git"))
	}

	lag.r.projector.refence()
	renameOn(t, origin, "alice/secret.git", "alice/secret2.git")
	require.NoError(t, origin.r.ReadRefusal("alice/secret2.git"), "the renamed directory moved in on the origin")
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
	err := lag.r.ReadRefusal("alice/secret.git")
	require.Error(t, err, "node-c serves the renamed repository under the new one's name")
	assert.Contains(t, err.Error(), "has not projected the last rename")
	for _, n := range tc.nodes[:2] {
		assert.NoError(t, n.r.ReadRefusal("alice/secret.git"), n.slot)
		assert.NoError(t, n.r.ReadRefusal("alice/secret2.git"), n.slot)
	}

	lag.r.projector.unfence()
	require.Eventually(t, lag.r.projector.Ready, 15*time.Second, 20*time.Millisecond, "node-c ready")
	assert.Equal(t, c2, gitRef(t, lag, "alice/secret.git", "refs/heads/main"))
	assert.Equal(t, c1, gitRef(t, lag, "alice/secret2.git", "refs/heads/main"))
	assert.NoError(t, lag.r.ReadRefusal("alice/secret.git"))
	assert.NoError(t, lag.r.ReadRefusal("alice/secret2.git"))
}
