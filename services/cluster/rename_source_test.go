// Copyright 2026 The Forgejo Authors. All rights reserved.
// SPDX-License-Identifier: GPL-3.0-or-later

package cluster

import (
	"context"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"forgejo.org/modules/clusterhook"
	"forgejo.org/services/cluster/reftable"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// An owner's rename (alice to bob) commits through node A. A transfer of
// alice/x to org, loaded before it and served by node B, whose projector
// has not moved alice/x yet (its serial pass busy, here held as a
// transaction in flight holds it), moves B's own alice/x directory and
// proposes alice/x -> org/x after the owner's rename, as transferOwnership
// does once the row lock of the owner's rename is released
// (services/repository/transfer.go). Apply took that rename, whose source
// held nothing any more: the transfer was recorded, Forgejo committed its
// SQL under org/x, and no node served anything under org/x, the repository
// staying under bob/x, a key the database does not know (#220). The
// proposer now reads its source in its table once a commit boundary a
// quorum confirmed is applied, and refuses a source that left, for
// certain and before anything is proposed, at every payload version:
// Forgejo moves the directory back, and every node serves the repository
// where the database names it.
func TestATransferFromANodeBehindAnOwnerRenameIsRefused(t *testing.T) {
	for _, v := range []int{reftable.OwnerRenameVersion, reftable.PayloadVersion} {
		t.Run("payload "+strconv.Itoa(v), func(t *testing.T) {
			tc := newTestCluster(t, "node-a", "node-b", "node-c")
			raiseClusterTo(t, tc.nodes[1].r, v, replicatorsOf(tc)...)
			work := t.TempDir()
			run(t, work, "init", "-q")
			c1 := commit(t, work, "one")
			zero := strings.Repeat("0", 40)
			a, b := tc.nodes[0], tc.nodes[1]
			require.True(t, tc.pushVia(a, work, "alice/x.git", "refs/heads/main", zero, c1).OK)
			waitAll(t, tc, func(n *testNode) bool {
				return gitRef(t, n, "alice/x.git", "refs/heads/main") == c1 && n.r.projector.Projected("alice/x.git") > 0
			})
			settle(t, a)
			settle(t, b)

			// B's projector does not move alice/x yet.
			release := holdOn(t, b, "alice/x.git", "bob/x.git")
			ctx := context.Background()

			// The owner's rename, through A, as services/user/user.go does it.
			require.NoError(t, os.Rename(filepath.Join(a.r.root, "alice"), filepath.Join(a.r.root, "bob")))
			require.NoError(t, a.r.proposeOwnerRename(ctx, "alice", "bob"))
			waitApplied(t, a, b)
			require.True(t, dirExists(b, "alice/x.git"), "node B still holds alice/x on its disk")
			before, _ := a.r.table.Applied()

			// The transfer, through B, as transferOwnership does it: the local
			// directory moves, then RepoRenamed.
			require.NoError(t, os.MkdirAll(filepath.Join(b.r.root, "org"), 0o755))
			require.NoError(t, os.Rename(filepath.Join(b.r.root, "alice", "x.git"), filepath.Join(b.r.root, "org", "x.git")))
			err := b.r.proposeRepoMoves(ctx, [][2]string{{"alice/x.git", "org/x.git"}, {"alice/x.wiki.git", "org/x.wiki.git"}})
			t.Logf("the transfer's RepoRenamed through node B returned %v", err)
			require.Error(t, err, "the cluster recorded a transfer that moves nothing")
			assert.False(t, clusterhook.IsOutcomeUnresolved(err), "the refusal is certain: Forgejo moves the directories back")
			require.ErrorContains(t, err, "the repository alice/x.git was renamed to bob/x.git")
			after, _ := a.r.table.Applied()
			assert.Equal(t, before.Index, after.Index, "nothing was proposed")

			// Forgejo moves the directory back (transferOwnership's undo).
			require.NoError(t, os.Rename(filepath.Join(b.r.root, "org", "x.git"), filepath.Join(b.r.root, "alice", "x.git")))
			release()
			for _, n := range tc.nodes {
				_, _, marked := n.r.table.RenamedFrom("org/x.git")
				assert.False(t, marked, "%s: no rename into org/x", n.slot)
			}
			for _, n := range tc.nodes {
				require.Eventually(t, func() bool { return len(n.r.projector.Lag()) == 0 }, 20*time.Second, 50*time.Millisecond, "%s certifies: %v", n.slot, n.r.projector.Lag())
			}
			for _, n := range tc.nodes {
				assert.Equal(t, c1, gitRef(t, n, "bob/x.git", "refs/heads/main"), "%s serves the repository where the database names it", n.slot)
				assert.False(t, dirExists(n, "org/x.git"), n.slot)
				assert.False(t, dirExists(n, "alice/x.git"), n.slot)
				assert.Zero(t, setAsideCount(t, n), n.slot)
			}
		})
	}
}

// The control: the same owner's rename, node B's projector free. Once B
// projected it, B's disk no longer holds alice/x, and the transfer's own
// move of the directory fails before anything is proposed, as on a single
// node: only a node behind on its projection let it through.
func TestATransferFromANodeUpToDateFailsOnItsDisk(t *testing.T) {
	tc := newTestCluster(t, "node-a", "node-b", "node-c")
	raisePayload(t, tc)
	work := t.TempDir()
	run(t, work, "init", "-q")
	c1 := commit(t, work, "one")
	zero := strings.Repeat("0", 40)
	a, b := tc.nodes[0], tc.nodes[1]
	require.True(t, tc.pushVia(a, work, "alice/x.git", "refs/heads/main", zero, c1).OK)
	waitAll(t, tc, func(n *testNode) bool { return gitRef(t, n, "alice/x.git", "refs/heads/main") == c1 })
	settle(t, a)
	require.NoError(t, os.Rename(filepath.Join(a.r.root, "alice"), filepath.Join(a.r.root, "bob")))
	require.NoError(t, a.r.proposeOwnerRename(context.Background(), "alice", "bob"))
	waitApplied(t, a, b)
	settle(t, b)
	assert.False(t, dirExists(b, "alice/x.git"), "node B projected the owner's rename")
	require.NoError(t, os.MkdirAll(filepath.Join(b.r.root, "org"), 0o755))
	err := os.Rename(filepath.Join(b.r.root, "alice", "x.git"), filepath.Join(b.r.root, "org", "x.git"))
	t.Logf("the transfer's move of alice/x on an up-to-date node B: %v", err)
	assert.Error(t, err)
}
