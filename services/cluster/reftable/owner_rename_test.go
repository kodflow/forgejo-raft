// Copyright 2026 The Forgejo Authors. All rights reserved.
// SPDX-License-Identifier: GPL-3.0-or-later

package reftable

import (
	"bytes"
	"fmt"
	"strings"
	"testing"
	"time"

	"forgejo.org/modules/json"
	"forgejo.org/services/cluster/consensus"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// at8 applies a command at OwnerRenameVersion.
func (h *harness) at8(kind Kind, body any) Result {
	h.t.Helper()
	return h.applyAt(OwnerRenameVersion, kind, body)
}

// snapshotBytes is the table's snapshot, as the engine stores it and a
// checkpoint keeps it (refs.snapshot).
func snapshotBytes(t *testing.T, tb *Table) []byte {
	t.Helper()
	snap, err := tb.Snapshot()
	require.NoError(t, err)
	var buf bytes.Buffer
	_, err = snap.WriteTo(&buf)
	require.NoError(t, err)
	return buf.Bytes()
}

// ownerTable is a table at OwnerRenameVersion whose owner alice holds what
// an owner's rename meets: repositories with and without a wiki, one whose
// name starts with a dot, one deleted, one renamed away to another key of
// alice, one whose refs were all deleted, and, beside them, an owner whose
// name starts with alice's, and a key of bob that a deleted repository
// left.
func ownerTable(t *testing.T) *harness {
	t.Helper()
	h := newHarness(t)
	h.raise(PayloadVersion)
	create := func(repo string, id int64, updates ...RefUpdate) {
		t.Helper()
		tx := push(repo, updates...)
		tx.RepoID = id
		tx.Nonce = "create-" + repo
		res := h.at8(KindRefTxn, tx)
		require.True(t, res.OK, "%s: %s", repo, res.Reason)
	}
	main := func(sha string) RefUpdate { return RefUpdate{Ref: "refs/heads/main", New: sha} }
	create("alice/a.git", 11, RefUpdate{Ref: "HEAD", Symbolic: "refs/heads/main"}, main(sha1A))
	create("alice/a.wiki.git", 11, main(sha1B))
	create("alice/.profile.git", 12, main(sha1C))
	create("alice/b.git", 13, main(sha1A))
	require.True(t, h.at8(KindRepoDelete, &RepoDelete{Repo: "alice/b.git", RepoID: 13, Nonce: "delete-b"}).OK)
	create("alice/c.git", 14, main(sha1B))
	require.True(t, h.at8(KindRepoRename, &RepoRename{From: "alice/c.git", To: "alice/d.git", Nonce: "rename-c"}).OK)
	create("alice/f.git", 15, RefUpdate{Ref: "refs/heads/x", New: sha1C})
	require.True(t, h.at8(KindRefTxn, push("alice/f.git", RefUpdate{Ref: "refs/heads/x", Old: sha1C, ExpectRev: 1, Delete: true})).OK)
	create("alicex/g.git", 16, main(sha1A))
	create("bob/a.git", 17, main(sha1C))
	require.True(t, h.at8(KindRepoDelete, &RepoDelete{Repo: "bob/a.git", RepoID: 17, Nonce: "delete-bob-a"}).OK)
	return h
}

// keyListRename is the RepoRename an owner's rename proposed below
// OwnerRenameVersion, listing the keys the table holds under from.
func keyListRename(tb *Table, from, to, nonce string) *RepoRename {
	var moves [][2]string
	for _, key := range tb.OwnerKeys(from) {
		moves = append(moves, [2]string{key, to + "/" + strings.TrimPrefix(key, from+"/")})
	}
	return &RepoRename{From: moves[0][0], To: moves[0][1], Time: 99, Also: moves[1:], Nonce: nonce, Owner: [2]string{from, to}}
}

// An owner's rename by its names leaves the table the rename listing its
// keys leaves, on the same table: the same moves in the same order, so the
// same refs and revisions, rename marks, departures, ids, events, nonce and
// checksums, a byte-identical snapshot, and the same change for the
// projector. So does the listing at WithdrawVersion, which a member wrote
// before the raise and which commits after it, and the prefix applied on
// a table restored from a snapshot, as a node that installs one or
// restores a checkpoint holds (#182).
func TestAnOwnerRenameByItsNamesLeavesTheTableOfTheRenameOfItsKeys(t *testing.T) {
	want := []string{"alice/.profile.git", "alice/a.git", "alice/a.wiki.git", "alice/d.git"}
	type result struct {
		res  Result
		ch   Change
		snap []byte
		root Sum
	}
	run := func(h *harness, apply func(h *harness) Result) result {
		t.Helper()
		var ch Change
		h.table.OnChange(func(c Change) { ch = c })
		res := apply(h)
		h.table.OnChange(nil)
		return result{res: res, ch: ch, snap: snapshotBytes(t, h.table), root: h.table.Root()}
	}

	h := ownerTable(t)
	require.Equal(t, want, h.table.OwnerKeys("alice"), "the keys holding a repository under alice")
	listed := run(h, func(h *harness) Result { return h.at8(KindRepoRename, keyListRename(h.table, "alice", "bob", "owner")) })
	require.True(t, listed.res.OK, listed.res.Reason)
	assert.Empty(t, h.table.OwnerKeys("alice"))
	assert.Equal(t, []string{"bob/.profile.git", "bob/a.git", "bob/a.wiki.git", "bob/d.git"}, h.table.OwnerKeys("bob"))
	assert.Equal(t, []string{"alicex/g.git"}, h.table.OwnerKeys("alicex"), "another owner whose name starts with alice's")

	h7 := ownerTable(t)
	older := run(h7, func(h *harness) Result {
		return h.applyAt(WithdrawVersion, KindRepoRename, keyListRename(h.table, "alice", "bob", "owner"))
	})

	hp := ownerTable(t)
	before := snapshotBytes(t, hp.table)
	prefix := run(hp, func(h *harness) Result {
		return h.at8(KindOwnerRename, &OwnerRename{From: "alice", To: "bob", Time: 99, Nonce: "owner"})
	})

	restored := &harness{t: t, table: New(), index: hp.index - 1}
	require.NoError(t, restored.table.Restore(bytes.NewReader(before), consensus.SnapshotMeta{}))
	fromSnapshot := run(restored, func(h *harness) Result {
		return h.at8(KindOwnerRename, &OwnerRename{From: "alice", To: "bob", Time: 99, Nonce: "owner"})
	})

	for name, got := range map[string]result{"the listing at version 7": older, "the owner's names": prefix, "the owner's names on a restored table": fromSnapshot} {
		assert.Equal(t, listed.res, got.res, name)
		assert.Equal(t, listed.ch, got.ch, name)
		assert.Equal(t, listed.root, got.root, name)
		assert.Equal(t, string(listed.snap), string(got.snap), "%s: the snapshot differs", name)
	}
	for _, key := range want {
		to := "bob/" + strings.TrimPrefix(key, "alice/")
		from, idx, ok := hp.table.RenamedFrom(to)
		assert.True(t, ok && from == key && idx == hp.index, "%s: renamed from %s at %d", to, from, idx)
	}
	for _, key := range []string{"bob/b.git", "bob/c.git", "bob/f.git"} {
		from, _, ok := hp.table.RenamedFrom(key)
		assert.False(t, ok, "%s holds no repository: no mark from %s", key, from)
	}
	at, ok := hp.table.CommittedNonce("owner")
	assert.True(t, ok && at == hp.index, "the nonce its proposer finds")
}

// A refusal is the same too: a key of the new owner that already holds a
// repository refuses both forms with the same reason, and neither moves
// anything (#182).
func TestAnOwnerRenameByItsNamesIsRefusedAsTheRenameOfItsKeys(t *testing.T) {
	tables := [2]*harness{ownerTable(t), ownerTable(t)}
	for _, h := range tables {
		tx := push("bob/a.wiki.git", RefUpdate{Ref: "refs/heads/main", New: sha1A})
		tx.Nonce = "taken"
		require.True(t, h.at8(KindRefTxn, tx).OK)
	}
	listed := tables[0].at8(KindRepoRename, keyListRename(tables[0].table, "alice", "bob", "owner"))
	prefix := tables[1].at8(KindOwnerRename, &OwnerRename{From: "alice", To: "bob", Time: 99, Nonce: "owner"})
	assert.False(t, listed.OK)
	assert.Equal(t, listed, prefix)
	assert.Equal(t, "repository bob/a.wiki.git already holds refs", prefix.Reason)
	assert.Equal(t, string(snapshotBytes(t, tables[0].table)), string(snapshotBytes(t, tables[1].table)))
	assert.Len(t, tables[1].table.OwnerKeys("alice"), 4, "nothing moved")
	_, ok := tables[1].table.CommittedNonce("owner")
	assert.False(t, ok, "a refused rename keeps no nonce")
}

// The names are checked as the keys are; an owner holding nothing any more
// is renamed with nothing to move; below OwnerRenameVersion the command is
// neither written nor applied; and a binary that applies an older version
// stops on it, never skips it (D18).
func TestAnOwnerRenameByItsNamesAtItsVersionOnly(t *testing.T) {
	h := ownerTable(t)
	for _, o := range []OwnerRename{
		{From: "", To: "bob"},
		{From: "alice", To: ""},
		{From: "alice", To: "alice"},
		{From: "Alice", To: "bob"},
		{From: "alice", To: "b/c"},
		{From: "alice", To: ".bob"},
		{From: "alice", To: ".."},
		{From: "alice", To: strings.Repeat("b", 510)},
	} {
		res := h.at8(KindOwnerRename, &o)
		assert.False(t, res.OK, "%q to %q", o.From, o.To)
	}
	assert.Len(t, h.table.OwnerKeys("alice"), 4)

	var ch Change
	h.table.OnChange(func(c Change) { ch = c })
	res := h.at8(KindOwnerRename, &OwnerRename{From: "carol", To: "dave", Nonce: "empty"})
	require.True(t, res.OK, res.Reason)
	assert.Empty(t, ch.Repos)
	at, ok := h.table.CommittedNonce("empty")
	assert.True(t, ok && at == h.index)

	_, err := EncodeVersion(OwnerRenameVersion-1, KindOwnerRename, &OwnerRename{From: "alice", To: "bob"})
	require.ErrorIs(t, err, ErrVersion)
	body, err := json.Marshal(&OwnerRename{From: "alice", To: "bob", Nonce: "below"})
	require.NoError(t, err)
	for _, v := range []uint8{BaseVersion, GenerationVersion, OwnerRenameVersion - 1} {
		h.index++
		res := h.table.Apply(consensus.Entry{
			Term: 1, Index: h.index, Kind: consensus.EntryCommand, Data: append([]byte{v, byte(KindOwnerRename)}, body...),
			Token: consensus.Token{Cluster: consensus.ClusterID{1}, Epoch: 1, Term: 1, Index: h.index},
		}).(Result)
		assert.False(t, res.OK, "version %d", v)
	}
	assert.Len(t, h.table.OwnerKeys("alice"), 4, "nothing moved below the version")

	old := newHarness(t)
	old.table.limit = OwnerRenameVersion - 1
	old.raise(OwnerRenameVersion)
	assert.Equal(t, uint8(BaseVersion), old.table.WriteVersion(), "an older binary does not adopt the raise")
	old.raise(OwnerRenameVersion - 1)
	require.True(t, old.applyAt(OwnerRenameVersion-1, KindRefTxn, push("alice/a.git", RefUpdate{Ref: "refs/heads/main", New: sha1A})).OK)
	assert.Panics(t, func() { old.at8(KindOwnerRename, &OwnerRename{From: "alice", To: "bob", Nonce: "n"}) })
}

// An owner of 8000 repositories with wikis, the proof of #182: the entry
// listing its keys is past the engine's bound, which refused it at every
// attempt, while the one naming the owner keeps the size of any other
// owner's. Apply expands it into the same 16 000 moves, to the same table.
func TestAnOwnerOfEightThousandRepositoriesIsRenamedByItsNames(t *testing.T) {
	const n = 8000
	listing := newHarness(t)
	listing.raise(PayloadVersion)
	for i := range n {
		for _, key := range []string{fmt.Sprintf("organisation/repository-%05d.git", i), fmt.Sprintf("organisation/repository-%05d.wiki.git", i)} {
			tx := push(key, RefUpdate{Ref: "refs/heads/main", New: sha1A})
			tx.RepoID = int64(i + 1)
			require.True(t, listing.at8(KindRefTxn, tx).OK)
		}
	}
	// The same table, as a member restored from a snapshot holds it.
	prefix := &harness{t: t, table: New(), index: listing.index}
	require.NoError(t, prefix.table.Restore(bytes.NewReader(snapshotBytes(t, listing.table)), consensus.SnapshotMeta{}))

	rn := keyListRename(listing.table, "organisation", "organization", "owner")
	data, err := EncodeVersion(PayloadVersion, KindRepoRename, rn)
	require.NoError(t, err)
	o := &OwnerRename{From: "organisation", To: "organization", Time: 99, Nonce: "owner"}
	small, err := EncodeVersion(PayloadVersion, KindOwnerRename, o)
	require.NoError(t, err)
	t.Logf("the rename listing %d keys is %d bytes, the one naming the owner %d bytes (bound %d)", 2*n, len(data), len(small), consensus.MaxEntryBytes)
	require.Greater(t, len(data), consensus.MaxEntryBytes)
	require.Less(t, len(small), 128)

	start := time.Now()
	require.True(t, listing.at8(KindRepoRename, rn).OK)
	t.Logf("Apply moved the %d keys listed in %v", 2*n, time.Since(start))
	start = time.Now()
	res := prefix.at8(KindOwnerRename, o)
	t.Logf("Apply expanded the owner's rename into %d moves in %v", 2*n, time.Since(start))
	require.True(t, res.OK, res.Reason)
	assert.Empty(t, prefix.table.OwnerKeys("organisation"))
	assert.Len(t, prefix.table.OwnerKeys("organization"), 2*n)
	assert.Equal(t, listing.table.Root(), prefix.table.Root())
	assert.True(t, bytes.Equal(snapshotBytes(t, listing.table), snapshotBytes(t, prefix.table)), "the snapshots differ")
}
