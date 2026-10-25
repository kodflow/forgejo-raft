// Copyright 2026 The Forgejo Authors. All rights reserved.
// SPDX-License-Identifier: GPL-3.0-or-later

package reftable

import (
	"strconv"
	"testing"

	"forgejo.org/services/cluster/consensus"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// The ring of nonces tells a command's absence only while it covers the
// command's proposal: below NoncesKept it never dropped one, and once full
// it covers what came after the index before its oldest nonce. Another
// lineage's indexes do not compare, and a restored table answers as the
// one its snapshot was cut from (#234).
func TestNoncesKeptSinceTellsWhetherTheRingCoversAProposal(t *testing.T) {
	h := newHarness(t)
	assert.True(t, h.table.NoncesKeptSince(consensus.LogIdentity{}, 0), "an empty table")
	h.raise(PayloadVersion)
	_, id := h.table.Applied()
	start := h.index
	pushes := func(n int) {
		for range n {
			// Spread over a few keys: Apply reads every live ref of the key.
			tx := push("o/busy"+strconv.FormatUint(h.index%64, 10)+".git", RefUpdate{Ref: "refs/heads/b" + strconv.FormatUint(h.index, 10), New: sha1A})
			tx.Nonce = "push-" + strconv.FormatUint(h.index, 10)
			require.True(t, h.at9(KindRefTxn, tx).OK)
		}
	}
	pushes(NoncesKept - 1)
	assert.True(t, h.table.NoncesKeptSince(id, 0), "not full: nothing was dropped")
	assert.True(t, h.table.NoncesKeptSince(consensus.LogIdentity{}, 0), "a proposal started on an empty table")
	assert.False(t, h.table.NoncesKeptSince(consensus.LogIdentity{Cluster: id.Cluster, Epoch: id.Epoch + 1}, 0), "another lineage")
	pushes(1)
	assert.True(t, h.table.NoncesKeptSince(id, start), "full, the oldest nonce the first after start")
	assert.True(t, h.table.NoncesKeptSince(id, start+1))
	pushes(1)
	assert.False(t, h.table.NoncesKeptSince(id, start), "the nonce right after start was dropped")
	assert.True(t, h.table.NoncesKeptSince(id, start+1))
	for _, from := range []uint64{start, start + 1, h.index} {
		assert.Equal(t, h.table.NoncesKeptSince(id, from), restored(t, h.table).NoncesKeptSince(id, from), from)
	}
}

// A rename is shown by its effect from an index on: each move the newest
// rename into its target, at one entry's index after it, and no target
// deleted or renamed away since. A round trip a to b and back, then a
// third a to b, shows only the third from an index between them (#74,
// #234).
func TestMovedSinceReadsTheNewestRenameOfEveryMove(t *testing.T) {
	h := newHarness(t)
	h.raise(PayloadVersion)
	for _, key := range []string{"o/a.git", "o/a.wiki.git"} {
		require.True(t, h.at9(KindRefTxn, push(key, RefUpdate{Ref: "HEAD", Symbolic: "refs/heads/main"}, RefUpdate{Ref: "refs/heads/main", New: sha1A})).OK, key)
	}
	there := [][2]string{{"o/a.git", "o/b.git"}, {"o/a.wiki.git", "o/b.wiki.git"}}
	rename := func(moves [][2]string, nonce string) {
		require.True(t, h.at9(KindRepoRename, &RepoRename{From: moves[0][0], To: moves[0][1], Also: moves[1:], Time: 5, Nonce: nonce}).OK, nonce)
	}
	before := h.index
	assert.False(t, h.table.MovedSince(there, 0), "nothing moved yet")
	rename(there, "first")
	first := h.index
	assert.True(t, h.table.MovedSince(there, before))
	assert.True(t, h.table.MovedSince(there, first-1))
	assert.False(t, h.table.MovedSince(there, first), "not after the rename")
	assert.False(t, h.table.MovedSince(there[:1], first))
	assert.False(t, h.table.MovedSince([][2]string{{"o/x.git", "o/b.git"}}, before), "another source")
	assert.False(t, h.table.MovedSince(nil, before))

	// Back, then a third a to b: from an index after the first, only the
	// third shows.
	rename([][2]string{{"o/b.git", "o/a.git"}, {"o/b.wiki.git", "o/a.wiki.git"}}, "back")
	assert.False(t, h.table.MovedSince(there, before), "o/b.git left since")
	between := h.index
	rename(there, "third")
	assert.True(t, h.table.MovedSince(there, between))
	assert.True(t, restored(t, h.table).MovedSince(there, between))

	// Moved on, or deleted: the table no longer shows the move.
	moved := newHarness(t)
	*moved = *h
	moved.table = restored(t, h.table)
	require.True(t, moved.at9(KindRepoRename, &RepoRename{From: "o/b.git", To: "o/c.git", Time: 6, Nonce: "on"}).OK)
	assert.False(t, moved.table.MovedSince(there, between))
	require.True(t, h.at9(KindRepoDelete, &RepoDelete{Repo: "o/b.wiki.git", RepoID: 7, Nonce: "gone"}).OK)
	assert.False(t, h.table.MovedSince(there, between))
}

// A deletion is shown by its effect from an index on while the key stays
// deleted (#234).
func TestDeletedSinceReadsTheDeletionAfterAnIndex(t *testing.T) {
	h := newHarness(t)
	h.raise(PayloadVersion)
	require.True(t, h.at9(KindRefTxn, push("o/a.git", RefUpdate{Ref: "refs/heads/main", New: sha1A})).OK)
	before := h.index
	assert.False(t, h.table.DeletedSince("o/a.git", 0))
	require.True(t, h.at9(KindRepoDelete, &RepoDelete{Repo: "o/a.git", RepoID: 7, Nonce: "d"}).OK)
	at := h.index
	assert.True(t, h.table.DeletedSince("o/a.git", before))
	assert.False(t, h.table.DeletedSince("o/a.git", at))
	assert.True(t, restored(t, h.table).DeletedSince("o/a.git", before))
	// Written again, the key is no longer deleted.
	init := h.gitInit("o/a.git", "sha1")
	init.Init, init.Nonce = true, "init"
	require.True(t, h.at9(KindRefTxn, init).OK)
	assert.False(t, h.table.DeletedSince("o/a.git", before))
}
