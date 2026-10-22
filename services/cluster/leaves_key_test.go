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

// s/b.git deleted, then s/a.git renamed to s/b.git, while node-c is down:
// node-c still holds the old s/b.git, which its projector sets aside. A
// push to the new s/b.git must not be acknowledged by node-c into that
// directory, where its objects would leave the key with it (#99); node-c
// gets them once it projects the new s/b.git.
func TestObjectsAreNotAcknowledgedIntoADirectoryAboutToBeSetAside(t *testing.T) {
	t.Parallel()
	tc := newTestCluster(t, "node-a", "node-b", "node-c")
	work := t.TempDir()
	run(t, work, "init", "-q")
	c0 := commit(t, work, "zero")
	c1 := commit(t, work, "one")
	c2 := commit(t, work, "two")
	zero := strings.Repeat("0", 40)
	origin, lag := tc.nodes[0], tc.nodes[2]
	require.True(t, tc.pushVia(origin, work, "s/b.git", "refs/heads/main", zero, c0).OK)
	require.True(t, tc.pushVia(origin, work, "s/a.git", "refs/heads/main", zero, c1).OK)
	for _, n := range tc.nodes {
		require.Eventually(t, func() bool {
			return n.r.projector.Projected("s/a.git") > 0 && n.r.projector.Projected("s/b.git") > 0
		}, 10*time.Second, 20*time.Millisecond)
	}

	lag.r.projector.refence()
	require.NoError(t, os.RemoveAll(filepath.Join(origin.r.root, "s", "b.git")))
	require.NoError(t, origin.r.proposeRepoDelete(t.Context(), "s/b.git", 1))
	renameOn(t, origin, "s/a.git", "s/b.git")
	require.Eventually(t, func() bool {
		f, _, ok := lag.r.table.RenamedFrom("s/b.git")
		return ok && f == "s/a.git"
	}, 10*time.Second, 10*time.Millisecond)
	assert.True(t, lag.r.projector.leavesKey("s/b.git"), "node-c's s/b.git is the deleted repository")
	assert.False(t, lag.r.projector.leavesKey("s/a.git"))

	require.True(t, tc.pushVia(origin, work, "s/b.git", "refs/heads/main", c1, c2).OK)
	old := filepath.Join(lag.r.root, "s", "b.git")
	missing, err := missingObjects(t.Context(), old, []string{c2})
	require.NoError(t, err)
	assert.Equal(t, []string{c2}, missing, "node-c acknowledged the push into the directory it sets aside")

	lag.r.projector.unfence()
	require.Eventually(t, lag.r.projector.Ready, 15*time.Second, 20*time.Millisecond, "node-c ready")
	assert.Equal(t, c2, gitRef(t, lag, "s/b.git", "refs/heads/main"))
	assert.False(t, lag.r.projector.leavesKey("s/b.git"), "the new s/b.git is the key's")
}
