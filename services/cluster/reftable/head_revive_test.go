// Copyright 2026 The Forgejo Authors. All rights reserved.
// SPDX-License-Identifier: GPL-3.0-or-later

package reftable

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// A default-branch change of repository 7 (git symbolic-ref HEAD, no
// pusher) read on a node that applied 7's deletion but had not removed its
// directory yet was taken for git init and revived the key (#136). From
// InitVersion only a transaction marked Init creates.
func TestSymbolicHeadDoesNotReviveDeleted(t *testing.T) {
	h := newHarness(t)
	require.True(t, h.applyAt(InitVersion, KindRefTxn, push("z/m.git", RefUpdate{Ref: "HEAD", Symbolic: "refs/heads/main"}, RefUpdate{Ref: "refs/heads/main", New: sha1A})).OK)
	require.True(t, h.applyAt(InitVersion, KindRepoDelete, &RepoDelete{Repo: "z/m.git", RepoID: 7}).OK)
	readAt := h.index
	res := h.applyAt(InitVersion, KindRefTxn, &RefTxn{
		Repo: "z/m.git", RepoID: 7, Format: "sha1", ReadAt: readAt,
		Updates: []RefUpdate{{Ref: "HEAD", Symbolic: "refs/heads/dev", ExpectRev: revOf(h, "z/m.git", "HEAD")}},
	})
	_, deleted := h.table.Deleted("z/m.git")
	t.Logf("accepted=%v reason=%q deleted=%v", res.OK, res.Reason, deleted)
	assert.False(t, res.OK, "a HEAD change of the deleted repository 7 is no creation")
	assert.True(t, deleted, "the deletion of repository 7 holds")

	// git init's HEAD, marked Init, still creates the repository anew.
	create := h.gitInit("z/m.git", "sha1")
	create.Init = true
	require.True(t, h.applyAt(InitVersion, KindRefTxn, create).OK)
	_, deleted = h.table.Deleted("z/m.git")
	assert.False(t, deleted)
}

// Below InitVersion a transaction of HEAD alone is a creation, as an older
// binary applies it: a log written before the raise replays the same.
func TestSymbolicHeadCreatesBelowInitVersion(t *testing.T) {
	h := newHarness(t)
	require.True(t, h.v5(KindRefTxn, push("z/m.git", RefUpdate{Ref: "HEAD", Symbolic: "refs/heads/main"}, RefUpdate{Ref: "refs/heads/main", New: sha1A})).OK)
	require.True(t, h.v5(KindRepoDelete, &RepoDelete{Repo: "z/m.git", RepoID: 7}).OK)
	assert.True(t, h.v5(KindRefTxn, h.gitInit("z/m.git", "sha1")).OK)
}
