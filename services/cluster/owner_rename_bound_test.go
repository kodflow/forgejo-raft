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

// renameEntrySize is the size of the entry r proposes for moves.
func renameEntrySize(t *testing.T, r *Replicator, moves [][2]string, owner [2]string) int {
	t.Helper()
	v, rn := r.renameCommand(moves, owner)
	data, err := reftable.EncodeVersion(v, reftable.KindRepoRename, rn)
	require.NoError(t, err)
	return len(data)
}

// Below reftable.OwnerRenameVersion, an owner's rename is one entry listing
// every repository of the owner, and the engine refuses one above
// consensus.MaxEntryBytes, the SDK's default of 1 MiB, as ENTRY_TOO_LARGE
// at every attempt: about 6 600 repositories with wikis. An owner of 8000
// was refused once its SQL was written and its directory moved, and the
// rename undone (#182). Such a rename is now refused before it is
// proposed, with certainty, and the engine agrees on the bound: the
// largest owner under it is recorded on the SDK's real engine, from a
// follower too, and one repository more is refused early, as the engine
// refuses that very entry.
func TestAnOwnerRenameAboveTheEntryBoundIsRefusedBeforeTheEngine(t *testing.T) {
	tc := newTestClusterOn(t, true, "node-a", "node-b", "node-c")
	raiseClusterTo(t, tc.nodes[1].r, reftable.OwnerRenameVersion-1, replicatorsOf(tc)...)
	ctx := context.Background()
	for _, n := range tc.nodes {
		from, to := "o-"+n.slot, "p-"+n.slot
		owner := [2]string{from, to}
		// The names have one length: the entry grows by the same bytes
		// with each repository.
		base := renameEntrySize(t, n.r, ownerRenameMoves(from, to, 6000), owner)
		each := renameEntrySize(t, n.r, ownerRenameMoves(from, to, 6001), owner) - base
		under := 6000 + (consensus.MaxEntryBytes-base)/each
		atBound, past := renameEntrySize(t, n.r, ownerRenameMoves(from, to, under), owner), renameEntrySize(t, n.r, ownerRenameMoves(from, to, under+1), owner)
		require.LessOrEqual(t, atBound, consensus.MaxEntryBytes)
		require.Greater(t, past, consensus.MaxEntryBytes)
		t.Logf("%s: an owner of %d repositories with wikis is %d bytes, of %d repositories %d bytes (bound %d)", n.slot,
			under, atBound, under+1, past, consensus.MaxEntryBytes)

		for _, count := range []int{under + 1, 8000} {
			moves := ownerRenameMoves(from, to, count)
			before, _ := n.r.table.Applied()
			err := n.r.proposeMoves(ctx, moves, owner)
			require.ErrorIs(t, err, clusterhook.ErrTooLarge, "%s: an owner of %d repositories", n.slot, count)
			assert.False(t, clusterhook.IsOutcomeUnresolved(err), "a certain refusal: the caller moves the directories back")
			after, _ := n.r.table.Applied()
			assert.Equal(t, before.Index, after.Index, "%s: nothing was proposed", n.slot)
			_, _, renamed := n.r.table.RenamedFrom(moves[len(moves)-1][1])
			assert.False(t, renamed, "%s: an owner of %d repositories", n.slot, count)

			// The very entry, proposed to the engine, is refused there.
			v, rn := n.r.renameCommand(moves, owner)
			data, err := reftable.EncodeVersion(v, reftable.KindRepoRename, rn)
			require.NoError(t, err)
			_, _, err = n.r.Engine().Propose(ctx, data)
			require.ErrorIs(t, err, consensus.ErrEntryTooLarge, "%s: the engine takes what the replicator refused", n.slot)
		}

		moves := ownerRenameMoves(from, to, under)
		require.NoError(t, n.r.proposeMoves(ctx, moves, owner), "%s: an owner of %d repositories is renamed", n.slot, under)
		from0, _, ok := n.r.table.RenamedFrom(moves[len(moves)-1][1])
		require.True(t, ok)
		assert.Equal(t, moves[len(moves)-1][0], from0)
	}
}

// Below reftable.OwnerRenameVersion, an owner's rename whose entry the
// engine would refuse is refused before it is proposed, with a message that
// says why, once Forgejo asks before writing anything and again when it is
// proposed: nothing reaches the table, so nothing has to be undone there
// (#182). One within the bound still passes. From OwnerRenameVersion the
// entry names the owner alone: past the same bound, the owner is renamed.
func TestAnOwnerRenameTooLargeForOneEntryIsRefusedBeforeItIsProposed(t *testing.T) {
	tc := newTestCluster(t, "node-a", "node-b", "node-c")
	raiseClusterTo(t, tc.nodes[1].r, reftable.OwnerRenameVersion-1, replicatorsOf(tc)...)
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

	err = origin.r.ownerRenameRefusal(t.Context(), "big", "huge")
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

	// As Forgejo renames an owner: checked, its two directories held while
	// the old one moves and the rename is proposed (#257).
	renameOwner := func(from, to string) {
		t.Helper()
		require.NoError(t, origin.r.ownerRenameRefusal(t.Context(), from, to))
		release, err := origin.r.holdOwnerDirectories(t.Context(), []string{from, to})
		require.NoError(t, err)
		defer release()
		require.NoError(t, os.Rename(filepath.Join(origin.r.root, from), filepath.Join(origin.r.root, to)))
		require.NoError(t, origin.r.proposeOwnerRename(context.Background(), from, to))
	}
	origin.r.maxEntry = len(data) + 64
	renameOwner("big", "huge")
	waitAll(t, tc, func(n *testNode) bool { return gitRef(t, n, "huge/c.git", "refs/heads/main") == c1 })

	// Every member applies OwnerRenameVersion: the owner goes back past the
	// bound that refused it.
	raisePayload(t, tc)
	origin.r.maxEntry = len(data) - 1
	settle(t, origin)
	renameOwner("huge", "big")
	assert.Equal(t, byte(reftable.KindOwnerRename), lastEntry(t, origin.node).Data[1])
	waitAll(t, tc, func(n *testNode) bool { return gitRef(t, n, "big/c.git", "refs/heads/main") == c1 })
}
