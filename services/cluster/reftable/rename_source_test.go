// Copyright 2026 The Forgejo Authors. All rights reserved.
// SPDX-License-Identifier: GPL-3.0-or-later

package reftable

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// at9 applies a command at RenameSourceVersion.
func (h *harness) at9(kind Kind, body any) Result {
	h.t.Helper()
	return h.applyAt(RenameSourceVersion, kind, body)
}

// sourceTable is a table at RenameSourceVersion where alice/x left its key
// by its owner's rename to bob, carol/d was deleted, and dave/w lives
// without a wiki.
func sourceTable(t *testing.T) *harness {
	t.Helper()
	h := newHarness(t)
	h.raise(PayloadVersion)
	for _, key := range []string{"alice/x.git", "carol/d.git", "dave/w.git"} {
		require.True(t, h.at9(KindRefTxn, push(key, RefUpdate{Ref: "HEAD", Symbolic: "refs/heads/main"}, RefUpdate{Ref: "refs/heads/main", New: sha1A})).OK, key)
	}
	require.True(t, h.at9(KindOwnerRename, &OwnerRename{From: "alice", To: "bob", Nonce: "owner"}).OK)
	require.True(t, h.at9(KindRepoDelete, &RepoDelete{Repo: "carol/d.git", RepoID: 7, Nonce: "delete"}).OK)
	return h
}

// withWiki is the rename of a repository and its wiki, as RepoRenamed
// proposes it.
func withWiki(from, to string) *RepoRename {
	wiki := func(key string) string { return key[:len(key)-len(".git")] + ".wiki.git" }
	return &RepoRename{From: from, To: to, Also: [][2]string{{wiki(from), wiki(to)}}, Time: 5, Nonce: "rename-" + to}
}

// From RenameSourceVersion a repository's rename whose source left its
// key, renamed away by its owner's rename or deleted, is refused at its
// index and leaves the table as it was: it moved nothing, and its mark
// left the repository under a key the database no longer knows (#220).
// Below, it applies as before, on a binary of either version. The
// proposer reads the same reason in its own table.
func TestARenameFromAKeyThatLeftIsRefusedFromItsVersion(t *testing.T) {
	for _, c := range []struct{ from, why string }{
		{"alice/x.git", "the repository alice/x.git was renamed to bob/x.git"},
		{"carol/d.git", "the repository carol/d.git was deleted"},
	} {
		h := sourceTable(t)
		assert.Equal(t, c.why, h.table.RenameSourceRefusal(c.from))
		before := withoutFields(t, snapshotBytes(t, h.table), "applied")
		res := h.at9(KindRepoRename, withWiki(c.from, "org/x.git"))
		assert.False(t, res.OK, c.from)
		assert.Equal(t, c.why, res.Reason)
		assert.Equal(t, string(before), string(withoutFields(t, snapshotBytes(t, h.table), "applied")), "%s: the table is as it was", c.from)
		_, landed := h.table.CommittedNonce("rename-org/x.git")
		assert.False(t, landed, c.from)

		// Below the version, as before: the target is marked, nothing moves.
		old := sourceTable(t)
		res = old.applyAt(OwnerRenameVersion, KindRepoRename, withWiki(c.from, "org/x.git"))
		require.True(t, res.OK, res.Reason)
		from, _, marked := old.table.RenamedFrom("org/x.git")
		assert.True(t, marked && from == c.from, c.from)
		refs, _ := old.table.Refs("org/x.git")
		for _, v := range refs {
			assert.False(t, v.Live(), "%s: the rename moved nothing", c.from)
		}
	}
	v, ok := sourceTable(t).table.Get("bob/x.git", "refs/heads/main")
	require.True(t, ok)
	assert.Equal(t, sha1A, v.SHA, "the repository lives where its owner's rename moved it")
}

// What RenameSourceVersion does not refuse: a source with a live ref, a
// key the table never held, which has nothing to lose, a source written
// again after it left, whose wiki key still names an older rename, and an
// owner's rename, which ownerRenameRefuses checks.
func TestARenameFromAKeyThatHoldsItsRepositoryPassesItsVersion(t *testing.T) {
	h := sourceTable(t)
	for _, key := range []string{"dave/w.git", "nobody/n.git", "bob/x.git"} {
		assert.Empty(t, h.table.RenameSourceRefusal(key), key)
	}
	require.True(t, h.at9(KindRepoRename, withWiki("nobody/n.git", "nobody/m.git")).OK, "a key the table never held")
	require.True(t, h.at9(KindRepoRename, withWiki("dave/w.git", "dave/v.git")).OK)
	_, moved := h.table.MovedAway("dave/w.wiki.git")
	require.True(t, moved, "the wiki never created left a mark")

	// A new dave/w, created by git init, then renamed with its wiki.
	require.True(t, h.at9(KindRefTxn, &RefTxn{
		Repo: "dave/w.git", Format: "sha1", Init: true, Nonce: "init",
		Updates: []RefUpdate{{Ref: "HEAD", Symbolic: "refs/heads/main", ExpectRev: func() uint64 { v, _ := h.table.Get("dave/w.git", "HEAD"); return v.Rev }()}},
	}).OK)
	assert.Empty(t, h.table.RenameSourceRefusal("dave/w.git"))
	res := h.at9(KindRepoRename, withWiki("dave/w.git", "dave/u.git"))
	require.True(t, res.OK, res.Reason)
	from, _, _ := h.table.RenamedFrom("dave/u.git")
	assert.Equal(t, "dave/w.git", from)

	// An owner's rename by its keys, which names its owner.
	require.True(t, h.at9(KindRefTxn, push("eve/a.git", RefUpdate{Ref: "refs/heads/main", New: sha1B})).OK)
	res = h.at9(KindRepoRename, &RepoRename{From: "eve/a.git", To: "frank/a.git", Owner: [2]string{"eve", "frank"}, Nonce: "eve"})
	require.True(t, res.OK, res.Reason)
}

// A binary applying version 8 at most stops on a version 9 rename, never
// skips it (D18): it is written once every member applies it.
func TestAnOlderBinaryStopsOnARenameOfItsVersion(t *testing.T) {
	h := newHarness(t)
	h.table.limit = RenameSourceVersion - 1
	h.raise(RenameSourceVersion - 1)
	assert.Panics(t, func() { h.at9(KindRepoRename, withWiki("a/b.git", "a/c.git")) })
}
