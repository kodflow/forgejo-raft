// Copyright 2026 The Forgejo Authors. All rights reserved.
// SPDX-License-Identifier: GPL-3.0-or-later

package cluster

import (
	"strings"
	"testing"
	"time"

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
