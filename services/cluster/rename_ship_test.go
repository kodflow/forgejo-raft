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

// s/a.git renamed to s/b.git, then a push to s/b.git, while node-c has not
// moved s/a.git's directory yet: the push's objects must not create an
// empty s/b.git there, which would keep the renamed repository under the
// freed name for good (#100). node-c moves it in, and the old key is not
// known to the object channel any more.
func TestAPushToARenamedRepositoryWaitsForTheMove(t *testing.T) {
	t.Parallel()
	tc := newTestCluster(t, "node-a", "node-b", "node-c")
	work := t.TempDir()
	run(t, work, "init", "-q")
	c1 := commit(t, work, "one")
	c2 := commit(t, work, "two")
	zero := strings.Repeat("0", 40)
	origin, lag := tc.nodes[0], tc.nodes[2]
	require.True(t, tc.pushVia(origin, work, "s/a.git", "refs/heads/main", zero, c1).OK)
	require.Eventually(t, func() bool { return lag.r.projector.Projected("s/a.git") > 0 }, 10*time.Second, 20*time.Millisecond)
	before, err := os.Stat(filepath.Join(lag.r.root, "s", "a.git"))
	require.NoError(t, err)

	lag.r.projector.refence()
	renameOn(t, origin, "s/a.git", "s/b.git")
	require.Eventually(t, func() bool {
		f, _, ok := lag.r.table.RenamedFrom("s/b.git")
		return ok && f == "s/a.git"
	}, 10*time.Second, 10*time.Millisecond)
	assert.True(t, lag.r.projector.awaitsRename("s/b.git"))
	known, _ := lag.r.repoKnown(t.Context(), "s/a.git")
	assert.False(t, known, "a key renamed away is known to the object channel")

	require.True(t, tc.pushVia(origin, work, "s/b.git", "refs/heads/main", c1, c2).OK)
	// The shipment to node-c goes on in the background.
	assert.Never(t, func() bool {
		_, err := os.Stat(filepath.Join(lag.r.root, "s", "b.git"))
		return err == nil
	}, 3*time.Second, 50*time.Millisecond, "the shipment created s/b.git on node-c")

	lag.r.projector.unfence()
	require.Eventually(t, lag.r.projector.Ready, 15*time.Second, 20*time.Millisecond, "node-c ready")
	assert.Equal(t, c2, gitRef(t, lag, "s/b.git", "refs/heads/main"))
	after, err := os.Stat(filepath.Join(lag.r.root, "s", "b.git"))
	require.NoError(t, err)
	assert.True(t, os.SameFile(before, after), "the renamed repository's directory moved in")
	_, err = os.Stat(filepath.Join(lag.r.root, "s", "a.git"))
	assert.True(t, os.IsNotExist(err), "s/a.git left under the freed name")
}
