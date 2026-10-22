// Copyright 2026 The Forgejo Authors. All rights reserved.
// SPDX-License-Identifier: GPL-3.0-or-later

package cluster

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"forgejo.org/modules/clusterhook"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// One repository whose projection keeps failing on a node, here a damaged
// packed-refs, must not keep that node from writing to every other one
// after a start (#39): only its own writes wait, and past the first pass's
// grace the node is ready again, the repository reported behind.
func TestARepositoryBehindDoesNotHoldTheOthersWrites(t *testing.T) {
	tc := newTestCluster(t, "node-a", "node-b", "node-c")
	work := t.TempDir()
	run(t, work, "init", "-q")
	c1 := commit(t, work, "one")
	c2 := commit(t, work, "two")
	zero := strings.Repeat("0", 40)
	origin, lag := tc.nodes[0], tc.nodes[2]
	require.True(t, tc.pushVia(origin, work, "a/broken.git", "refs/heads/main", zero, c1).OK)
	require.True(t, tc.pushVia(origin, work, "a/other.git", "refs/heads/main", zero, c1).OK)
	for _, n := range tc.nodes {
		require.Eventually(t, func() bool {
			return n.r.projector.Projected("a/broken.git") > 0 && n.r.projector.Projected("a/other.git") > 0
		}, 10*time.Second, 20*time.Millisecond)
	}
	broken := filepath.Join(lag.r.root, "a", "broken.git")
	require.NoError(t, os.WriteFile(filepath.Join(broken, "packed-refs"), []byte("garbage line\n"), 0o644))

	// A start: every repository reconciled again, a/broken.git fails.
	lag.r.projector.unfence()
	require.Eventually(t, func() bool { return lag.r.projector.Projected("a/other.git") > 0 }, 10*time.Second, 20*time.Millisecond)
	assert.False(t, lag.r.projector.Ready(), "the first pass is not over")
	assert.Zero(t, lag.r.projector.Projected("a/broken.git"))

	resp := tc.pushVia(lag, work, "a/other.git", "refs/heads/main", c1, c2)
	require.True(t, resp.OK, "a push to a healthy repository: %s", resp.Message)
	require.NoError(t, lag.r.WriteRefusal(t.Context()), "the node takes writes")
	// Git cannot even take a push into a/broken.git here: Forgejo's own
	// write reaches the proposal.
	resp = lag.r.Handle(t.Context(), &clusterhook.Request{
		Op: clusterhook.OpPrepared, Nonce: clusterhook.NewNonce(), GitDir: broken, RepoID: 1,
		Lines: []clusterhook.Line{{Old: c1, New: c2, Ref: "refs/heads/main"}},
	})
	assert.False(t, resp.OK, "a write to the repository behind")
	assert.Contains(t, resp.Message, "not reconciled on this node")

	var pending bool
	for _, l := range lag.r.projector.Lag() {
		if l.Repo == "a/broken.git" {
			pending = l.Pending && l.Error != ""
		}
	}
	assert.True(t, pending, "the repository behind is reported")

	defer func(g time.Duration) { firstPassGrace = g }(firstPassGrace)
	firstPassGrace = 0
	assert.True(t, lag.r.projector.Ready(), "past the grace, the node is ready")
	assert.True(t, lag.r.Ready(), "and healthy")
	assert.Zero(t, lag.r.projector.Projected("a/broken.git"), "the repository behind is still not served as projected")
}
