// Copyright 2026 The Forgejo Authors. All rights reserved.
// SPDX-License-Identifier: GPL-3.0-or-later

package reftable

import (
	"bytes"
	"testing"

	"forgejo.org/services/cluster/consensus"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// a/a.git renamed to a/b.git at 2, a/b.git to a/c.git at 3, then a/x.git to
// the freed a/b.git at 5: the newest rename into a/b.git no longer hides
// the first one, and a walk from an event of a/a.git still finds its way
// forward, and back from a/c.git (#138).
func TestARenameIntoAFreedKeyKeepsTheEarlierOne(t *testing.T) {
	h := newHarness(t)
	require.True(t, h.apply(KindRefTxn, push("a/a.git", RefUpdate{Ref: "refs/heads/main", New: sha1A})).OK)
	require.True(t, h.apply(KindRepoRename, &RepoRename{From: "a/a.git", To: "a/b.git"}).OK)
	require.True(t, h.apply(KindRepoRename, &RepoRename{From: "a/b.git", To: "a/c.git"}).OK)
	require.True(t, h.apply(KindRefTxn, push("a/x.git", RefUpdate{Ref: "refs/heads/main", New: sha1B})).OK)
	require.True(t, h.apply(KindRepoRename, &RepoRename{From: "a/x.git", To: "a/b.git"}).OK)

	check := func(t *testing.T, tbl *Table) {
		to, at, ok := tbl.RenamedAfter("a/a.git", 1)
		require.True(t, ok, "the rename of a/a.git is still known")
		assert.Equal(t, "a/b.git", to)
		assert.EqualValues(t, 2, at)
		to, at, ok = tbl.RenamedAfter("a/b.git", at)
		require.True(t, ok)
		assert.Equal(t, "a/c.git", to)
		assert.EqualValues(t, 3, at)

		from, at, ok := tbl.RenamedFromBefore("a/c.git", ^uint64(0))
		require.True(t, ok)
		assert.Equal(t, "a/b.git", from)
		from, at, ok = tbl.RenamedFromBefore("a/b.git", at)
		require.True(t, ok, "the rename into a/b.git before 3")
		assert.Equal(t, "a/a.git", from)
		assert.EqualValues(t, 2, at)
		_, _, ok = tbl.RenamedFromBefore("a/a.git", at)
		assert.False(t, ok)

		from, at, ok = tbl.RenamedFrom("a/b.git")
		require.True(t, ok, "the newest rename stands")
		assert.Equal(t, "a/x.git", from)
		assert.EqualValues(t, 5, at)
	}
	check(t, h.table)

	snap, err := h.table.Snapshot()
	require.NoError(t, err)
	var buf bytes.Buffer
	_, err = snap.WriteTo(&buf)
	require.NoError(t, err)
	restored := New()
	require.NoError(t, restored.Restore(&buf, consensus.SnapshotMeta{}))
	check(t, restored)
}

// The table keeps the database ID of the repository each key holds: a ref
// transaction carrying one sets it, one without leaves it, a rename moves
// it, a deletion forgets it, and a snapshot keeps it (#82).
func TestTheTableKeepsEachKeysRepositoryID(t *testing.T) {
	h := newHarness(t)
	id := func(tbl *Table, repo string) int64 { _, id, _ := tbl.RefsWithID(repo); return id }
	require.True(t, h.apply(KindRefTxn, push("o/a.git", RefUpdate{Ref: "refs/heads/main", New: sha1A})).OK)
	assert.EqualValues(t, 7, id(h.table, "o/a.git"))
	noID := push("o/a.git", RefUpdate{Ref: "refs/heads/x", New: sha1B})
	noID.RepoID = 0
	require.True(t, h.apply(KindRefTxn, noID).OK)
	assert.EqualValues(t, 7, id(h.table, "o/a.git"), "a transaction without an ID leaves it")

	require.True(t, h.apply(KindRepoRename, &RepoRename{From: "o/a.git", To: "o/b.git"}).OK)
	assert.Zero(t, id(h.table, "o/a.git"), "the rename took the repository away")
	assert.EqualValues(t, 7, id(h.table, "o/b.git"))

	again := push("o/a.git", RefUpdate{Ref: "refs/heads/main", New: sha1C, ExpectRev: 2})
	again.RepoID = 9
	require.True(t, h.apply(KindRefTxn, again).OK, "a new repository under the freed name")
	assert.EqualValues(t, 9, id(h.table, "o/a.git"))

	snap, err := h.table.Snapshot()
	require.NoError(t, err)
	var buf bytes.Buffer
	_, err = snap.WriteTo(&buf)
	require.NoError(t, err)
	restored := New()
	require.NoError(t, restored.Restore(&buf, consensus.SnapshotMeta{}))
	assert.EqualValues(t, 9, id(restored, "o/a.git"))
	assert.EqualValues(t, 7, id(restored, "o/b.git"))

	require.True(t, h.apply(KindRepoDelete, &RepoDelete{Repo: "o/b.git"}).OK)
	assert.Zero(t, id(h.table, "o/b.git"), "a deletion forgets it")
}
