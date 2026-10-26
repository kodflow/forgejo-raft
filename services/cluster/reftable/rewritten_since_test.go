// Copyright 2026 The Forgejo Authors. All rights reserved.
// SPDX-License-Identifier: GPL-3.0-or-later

package reftable

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// A key is rewritten after an index by a deletion of its repository, or a
// rename, a transfer or an owner's rename from or into it, applied after
// that index: ref transactions do not count, nor commands on other keys.
// A deletion still counts once the key was created again, and a restored
// table answers as the one its snapshot was cut from (#252).
func TestRewrittenSinceSeesTheCommandsThatRewriteAKeysRefs(t *testing.T) {
	h := newHarness(t)
	h.raise(PayloadVersion)
	for _, key := range []string{"o/a.git", "o/a.wiki.git", "o/d.git", "o/x.git"} {
		require.True(t, h.at9(KindRefTxn, push(key, RefUpdate{Ref: "HEAD", Symbolic: "refs/heads/main"}, RefUpdate{Ref: "refs/heads/main", New: sha1A})).OK, key)
	}
	start := h.index
	require.True(t, h.at9(KindRefTxn, push("o/a.git", RefUpdate{Ref: "refs/heads/main", Old: sha1A, ExpectRev: 1, New: sha1B})).OK)
	for _, key := range []string{"o/a.git", "o/b.git", "o/d.git", "o/x.git", "p/a.git"} {
		assert.False(t, h.table.RewrittenSince(key, 0), "a ref transaction does not rewrite %s", key)
	}

	require.True(t, h.at9(KindRepoRename, &RepoRename{From: "o/a.git", To: "o/b.git", Also: [][2]string{{"o/a.wiki.git", "o/b.wiki.git"}}, Time: 2, Nonce: "ren"}).OK)
	renamed := h.index
	require.True(t, h.at9(KindRepoDelete, &RepoDelete{Repo: "o/d.git", RepoID: 7, Time: 3, Nonce: "del"}).OK)
	deleted := h.index
	for _, key := range []string{"o/a.git", "o/a.wiki.git", "o/b.git", "o/b.wiki.git"} {
		assert.True(t, h.table.RewrittenSince(key, start), "renamed after start: %s", key)
		assert.True(t, h.table.RewrittenSince(key, renamed-1), key)
		assert.False(t, h.table.RewrittenSince(key, renamed), "not after the rename: %s", key)
	}
	assert.True(t, h.table.RewrittenSince("o/d.git", deleted-1))
	assert.False(t, h.table.RewrittenSince("o/d.git", deleted))
	assert.False(t, h.table.RewrittenSince("o/x.git", start), "another key")

	// o/d.git created again: its deletion still rewrote it after start.
	require.True(t, h.at9(KindRefTxn, &RefTxn{Repo: "o/d.git", Format: "sha1", Init: true, Updates: []RefUpdate{{Ref: "HEAD", Symbolic: "refs/heads/main", ExpectRev: 2}}}).OK)
	_, stillDeleted := h.table.Deleted("o/d.git")
	require.False(t, stillDeleted)
	assert.True(t, h.table.RewrittenSince("o/d.git", start))

	// An owner's rename moves every key of the owner.
	require.True(t, h.at9(KindOwnerRename, &OwnerRename{From: "o", To: "p", Time: 4, Nonce: "own"}).OK)
	owner := h.index
	for _, key := range []string{"o/b.git", "o/x.git", "p/b.git", "p/x.git"} {
		assert.True(t, h.table.RewrittenSince(key, owner-1), key)
		assert.False(t, h.table.RewrittenSince(key, owner), key)
	}

	tb := restored(t, h.table)
	for _, key := range []string{"o/a.git", "o/b.git", "o/d.git", "o/x.git", "p/x.git", "q/none.git"} {
		for _, from := range []uint64{0, start, renamed, deleted, owner - 1, owner} {
			assert.Equal(t, h.table.RewrittenSince(key, from), tb.RewrittenSince(key, from), "%s from %d", key, from)
		}
	}
}
