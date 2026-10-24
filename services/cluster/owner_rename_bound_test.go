// Copyright 2026 The Forgejo Authors. All rights reserved.
// SPDX-License-Identifier: GPL-3.0-or-later

package cluster

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"forgejo.org/modules/clusterhook"
	"forgejo.org/services/cluster/consensus"
	"forgejo.org/services/cluster/reftable"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ownerRenameMoves is what an owner's rename moves for an owner of n
// repositories, each with a wiki, named as Forgejo allows.
func ownerRenameMoves(from, to string, n int) [][2]string {
	moves := make([][2]string, 0, 2*n)
	for i := range n {
		name := fmt.Sprintf("repository-%05d", i)
		moves = append(moves,
			[2]string{from + "/" + name + ".git", to + "/" + name + ".git"},
			[2]string{from + "/" + name + ".wiki.git", to + "/" + name + ".wiki.git"})
	}
	return moves
}

// An owner's rename is one entry listing every repository of the owner:
// past the SDK's default MaxEntryBytes of 1 MiB, about 6 600 repositories
// with wikis, it was refused as ENTRY_TOO_LARGE, a certain refusal, and the
// rename undone, at every attempt (#182). Forgejo opens the engine with
// consensus.MaxEntryBytes, which the SDK engine of this test runs with.
func TestAnOwnerRenameAboveTheSDKDefaultEntryBoundIsRecorded(t *testing.T) {
	for _, n := range []int{1000, 6000, 8000, 26000} {
		data, err := reftable.EncodeVersion(reftable.PayloadVersion, reftable.KindRepoRename, &reftable.RepoRename{
			From: "organisation/a.git", To: "organization/a.git", Also: ownerRenameMoves("organisation", "organization", n),
			Nonce: clusterhook.NewNonce(), Owner: [2]string{"organisation", "organization"},
		})
		require.NoError(t, err)
		t.Logf("owner of %d repositories with wikis: the rename entry is %d bytes (bound %d)", n, len(data), consensus.MaxEntryBytes)
		if n == 8000 {
			require.Greater(t, len(data), 1<<20, "the entry is above the SDK's default bound")
			require.Less(t, len(data), consensus.MaxEntryBytes)
		}
	}

	tc := newTestClusterOn(t, true, "node-a", "node-b", "node-c")
	raisePayload(t, tc)
	for _, n := range tc.nodes {
		moves := ownerRenameMoves("o-"+n.slot, "p-"+n.slot, 8000)
		require.NoError(t, n.r.proposeMoves(context.Background(), moves, [2]string{"o-" + n.slot, "p-" + n.slot}),
			"%s: an owner of 8000 repositories is renamed", n.slot)
		from, _, ok := n.r.table.RenamedFrom(moves[len(moves)-1][1])
		require.True(t, ok)
		assert.Equal(t, moves[len(moves)-1][0], from)
	}
}

// An owner's rename whose entry the engine would refuse is refused before
// it is proposed, with a message that says why, once Forgejo asks before
// writing anything and again when it is proposed: nothing reaches the
// table, so nothing has to be undone there (#182). One within the bound
// still passes.
func TestAnOwnerRenameTooLargeForOneEntryIsRefusedBeforeItIsProposed(t *testing.T) {
	tc := newTestCluster(t, "node-a", "node-b", "node-c")
	raisePayload(t, tc)
	work := t.TempDir()
	run(t, work, "init", "-q")
	c1 := commit(t, work, "one")
	zero := strings.Repeat("0", 40)
	origin := tc.nodes[0]
	for _, name := range []string{"a", "b", "c"} {
		require.True(t, tc.pushVia(origin, work, "big/"+name+".git", "refs/heads/main", zero, c1).OK)
	}
	v, rn := origin.r.renameCommand(origin.r.ownerMoves("big", "huge"), [2]string{"big", "huge"})
	data, err := reftable.EncodeVersion(v, reftable.KindRepoRename, rn)
	require.NoError(t, err)
	origin.r.maxEntry = len(data) - 1

	err = origin.r.ownerRenameRefusal("big", "huge")
	require.ErrorIs(t, err, clusterhook.ErrTooLarge)
	assert.Contains(t, err.Error(), "renaming the owner big moves its 3 repositories")
	assert.Contains(t, err.Error(), fmt.Sprintf("the bound is %d", len(data)-1))

	before, _ := origin.r.table.Applied()
	err = origin.r.proposeOwnerRename(context.Background(), "big", "huge")
	require.ErrorIs(t, err, clusterhook.ErrTooLarge)
	assert.False(t, clusterhook.IsOutcomeUnresolved(err), "a certain refusal: the caller moves the directory back")
	assert.Contains(t, err.Error(), "renaming the owner big moves its 3 repositories")
	after, _ := origin.r.table.Applied()
	assert.Equal(t, before.Index, after.Index, "nothing was proposed")
	for _, n := range tc.nodes {
		for _, name := range []string{"a", "b", "c"} {
			v, ok := n.r.table.Get("big/"+name+".git", "refs/heads/main")
			assert.True(t, ok && v.Live(), "%s: big/%s.git stays", n.slot, name)
			_, _, renamed := n.r.table.RenamedFrom("huge/" + name + ".git")
			assert.False(t, renamed, "%s: huge/%s.git", n.slot, name)
		}
	}

	origin.r.maxEntry = len(data) + 64
	require.NoError(t, origin.r.ownerRenameRefusal("big", "huge"))
	require.NoError(t, os.Rename(filepath.Join(origin.r.root, "big"), filepath.Join(origin.r.root, "huge")))
	require.NoError(t, origin.r.proposeOwnerRename(context.Background(), "big", "huge"))
	waitAll(t, tc, func(n *testNode) bool { return gitRef(t, n, "huge/c.git", "refs/heads/main") == c1 })
}
