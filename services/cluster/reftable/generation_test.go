// Copyright 2026 The Forgejo Authors. All rights reserved.
// SPDX-License-Identifier: GPL-3.0-or-later

package reftable

import (
	"fmt"
	"strings"
	"testing"

	"forgejo.org/modules/json"
	"forgejo.org/services/cluster/consensus"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const sha256A = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"

// v5 is the harness at GenerationVersion.
func (h *harness) v5(kind Kind, body any) Result {
	h.t.Helper()
	return h.applyAt(GenerationVersion, kind, body)
}

// gitInit is the HEAD git init writes, with no pusher and no repository id,
// as Forgejo's creation proposes it, at the revision the table holds.
func (h *harness) gitInit(repo, format string) *RefTxn {
	v, _ := h.table.Get(repo, "HEAD")
	return &RefTxn{Repo: repo, Format: format, Updates: []RefUpdate{{Ref: "HEAD", Symbolic: "refs/heads/main", ExpectRev: v.Rev}}}
}

// A write without a pusher (a mirror sync, refs/pull, an API tag) in flight
// while its repository was deleted or renamed away: version 2 applied it
// and brought the repository back (#77). From version 5 it is refused,
// while git init's HEAD still creates a repository anew under the key.
func TestHuntR4ProveNonPushRevive(t *testing.T) {
	setup := func() *harness {
		h := newHarness(t)
		require.True(t, h.v5(KindRefTxn, push("z/m.git", RefUpdate{Ref: "HEAD", Symbolic: "refs/heads/main"}, RefUpdate{Ref: "refs/heads/main", New: sha1A})).OK)
		require.True(t, h.v5(KindRefTxn, push("z/r.git", RefUpdate{Ref: "refs/heads/main", New: sha1A})).OK)
		require.True(t, h.v5(KindRepoDelete, &RepoDelete{Repo: "z/m.git", RepoID: 7}).OK)
		require.True(t, h.v5(KindRepoRename, &RepoRename{From: "z/r.git", To: "z/s.git"}).OK)
		return h
	}
	unpushed := func(repo, ref string) *RefTxn {
		return &RefTxn{Repo: repo, Format: "sha1", Updates: []RefUpdate{{Ref: ref, New: sha1B}}}
	}

	for _, ref := range []string{"refs/heads/new", "refs/pull/1/head", "refs/tags/v1"} {
		h := setup()
		for _, repo := range []string{"z/m.git", "z/r.git"} {
			res := h.v5(KindRefTxn, unpushed(repo, ref))
			assert.False(t, res.OK, "%s %s", repo, ref)
			_, ok := h.table.Get(repo, ref)
			assert.False(t, ok, "%s %s", repo, ref)
		}
		_, deleted := h.table.Deleted("z/m.git")
		assert.True(t, deleted, "the deletion holds")
		to, moved := h.table.MovedAway("z/r.git")
		assert.True(t, moved)
		assert.Equal(t, "z/s.git", to)
	}

	// The creation under the freed names, then the first push.
	h := setup()
	for _, repo := range []string{"z/m.git", "z/r.git"} {
		require.True(t, h.v5(KindRefTxn, h.gitInit(repo, "sha1")).OK, repo)
		res := h.v5(KindRefTxn, &RefTxn{Repo: repo, RepoID: 9, Format: "sha1", PusherID: 3, Push: true, Updates: []RefUpdate{{Ref: "refs/heads/main", New: sha1C, ExpectRev: revOf(h, repo, "refs/heads/main")}}})
		assert.True(t, res.OK, "%s: %s", repo, res.Reason)
	}

	// Version 4 applies the write as before: a log written before the
	// raise replays to the same table.
	h = setup()
	assert.True(t, h.applyAt(4, KindRefTxn, unpushed("z/m.git", "refs/heads/new")).OK)
}

// The control: a push in the same interleaving was refused from version 2
// already, and still is.
func TestHuntR4ProveNonPushReviveControlPush(t *testing.T) {
	h := newHarness(t)
	require.True(t, h.v5(KindRefTxn, push("z/m.git", RefUpdate{Ref: "refs/heads/main", New: sha1A})).OK)
	require.True(t, h.v5(KindRepoDelete, &RepoDelete{Repo: "z/m.git", RepoID: 7}).OK)
	tx := push("z/m.git", RefUpdate{Ref: "refs/heads/new", New: sha1B})
	tx.Push = true
	res := h.v5(KindRefTxn, tx)
	assert.False(t, res.OK)
	assert.Contains(t, res.Reason, "was deleted")
}

func revOf(h *harness, repo, ref string) uint64 {
	v, _ := h.table.Get(repo, ref)
	return v.Rev
}

// A push prepared against repository 7 lands, from version 2 to 4, in the
// repository that took its name once 7 was deleted or renamed away (#91).
// From version 5 a transaction read before the key's repository left it is
// refused; so is a ref of another object format than the key's.
func TestHuntLcskStalePushLandsInRecreatedRepository(t *testing.T) {
	var readAt uint64
	stale := func() *RefTxn {
		return &RefTxn{Repo: "o/x.git", RepoID: 7, Format: "sha1", PusherID: 3, Push: true, ReadAt: readAt, Updates: []RefUpdate{{Ref: "refs/heads/feature", New: sha1B}}}
	}
	// o/x.git is repository 7, created before the raise.
	start := func(t *testing.T) *harness {
		h := newHarness(t)
		require.True(t, h.applyAt(4, KindRefTxn, &RefTxn{Repo: "o/x.git", Format: "sha1", Updates: []RefUpdate{{Ref: "HEAD", Symbolic: "refs/heads/main"}}}).OK)
		require.True(t, h.applyAt(4, KindRefTxn, push("o/x.git", RefUpdate{Ref: "refs/heads/main", New: sha1A})).OK)
		require.True(t, h.applyAt(4, KindRefTxn, push("o/other.git", RefUpdate{Ref: "refs/heads/main", New: sha1C})).OK)
		// The stale push reads the table here, then waits on its objects.
		readAt = h.index
		return h
	}
	recreate := func(h *harness, format string) {
		require.True(t, h.v5(KindRefTxn, h.gitInit("o/x.git", format)).OK)
		sha := sha1C
		if format == "sha256" {
			sha = sha256A
		}
		res := h.v5(KindRefTxn, &RefTxn{Repo: "o/x.git", RepoID: 9, Format: format, PusherID: 4, Push: true, ReadAt: h.index, Updates: []RefUpdate{{Ref: "refs/heads/main", New: sha, ExpectRev: revOf(h, "o/x.git", "refs/heads/main")}}})
		require.True(t, res.OK, res.Reason)
	}
	cases := map[string]func(t *testing.T, h *harness){
		"deleted-then-created": func(t *testing.T, h *harness) {
			require.True(t, h.v5(KindRepoDelete, &RepoDelete{Repo: "o/x.git", RepoID: 7}).OK)
			recreate(h, "sha1")
		},
		"renamed-then-created": func(t *testing.T, h *harness) {
			require.True(t, h.v5(KindRepoRename, &RepoRename{From: "o/x.git", To: "o/y.git"}).OK)
			recreate(h, "sha1")
		},
		"deleted-then-renamed-into": func(t *testing.T, h *harness) {
			require.True(t, h.v5(KindRepoDelete, &RepoDelete{Repo: "o/x.git", RepoID: 7}).OK)
			require.True(t, h.v5(KindRepoRename, &RepoRename{From: "o/other.git", To: "o/x.git"}).OK)
		},
		"deleted-then-created-sha256": func(t *testing.T, h *harness) {
			require.True(t, h.v5(KindRepoDelete, &RepoDelete{Repo: "o/x.git", RepoID: 7}).OK)
			recreate(h, "sha256")
		},
	}
	for name, run := range cases {
		t.Run(name, func(t *testing.T) {
			h := start(t)
			run(t, h)
			res := h.v5(KindRefTxn, stale())
			assert.False(t, res.OK, "the stale push of repository 7 was applied")
			v, ok := h.table.Get("o/x.git", "refs/heads/feature")
			assert.False(t, ok && v.Live(), "feature live in the new o/x.git")

			// The new repository's own pushes, read since, pass; so does
			// a write of the same repository id, which a wiki deleted
			// and created again keeps.
			own := stale()
			own.ReadAt = h.index
			if name == "deleted-then-created-sha256" {
				own.Format, own.Updates[0].New = "sha256", sha256A
			}
			assert.True(t, h.v5(KindRefTxn, own).OK, "the new repository's push")
		})
	}

	// Of another object format than the key's live refs: refused, whatever
	// it read.
	h := start(t)
	res := h.v5(KindRefTxn, &RefTxn{Repo: "o/x.git", Format: "sha256", ReadAt: h.index, Updates: []RefUpdate{{Ref: "refs/heads/f", New: sha256A}}})
	assert.False(t, res.OK)
	assert.Contains(t, res.Reason, "holds sha1 objects")

	// Version 4: unchanged, the stale push lands.
	h = start(t)
	require.True(t, h.applyAt(4, KindRepoDelete, &RepoDelete{Repo: "o/x.git", RepoID: 7}).OK)
	require.True(t, h.applyAt(4, KindRefTxn, h.gitInit("o/x.git", "sha1")).OK)
	assert.True(t, h.applyAt(4, KindRefTxn, stale()).OK)
}

// A round trip a to b and back keeps the repository writable under its
// key; a write read before the trip ended is refused, one read since
// passes.
func TestRoundTripRenameKeepsTheRepository(t *testing.T) {
	h := newHarness(t)
	require.True(t, h.v5(KindRefTxn, push("a/x.git", RefUpdate{Ref: "refs/heads/main", New: sha1A})).OK)
	before := h.index
	require.True(t, h.v5(KindRepoRename, &RepoRename{From: "a/x.git", To: "b/x.git"}).OK)
	require.True(t, h.v5(KindRepoRename, &RepoRename{From: "b/x.git", To: "a/x.git"}).OK)
	tx := push("a/x.git", RefUpdate{Ref: "refs/heads/main", Old: sha1A, ExpectRev: revOf(h, "a/x.git", "refs/heads/main"), New: sha1B})
	tx.ReadAt = before
	assert.False(t, h.v5(KindRefTxn, tx).OK, "read before the repository left a/x.git")
	tx.ReadAt = h.index
	res := h.v5(KindRefTxn, tx)
	assert.True(t, res.OK, res.Reason)
}

// A lifecycle command's nonce is kept from version 5, never below: its
// proposer tells THIS command from an older one that left the same state
// (#74).
func TestLifecycleNonceFromVersion5(t *testing.T) {
	h := newHarness(t)
	require.True(t, h.v5(KindRefTxn, push("a/x.git", RefUpdate{Ref: "refs/heads/main", New: sha1A})).OK)
	require.True(t, h.v5(KindRepoRename, &RepoRename{From: "a/x.git", To: "b/x.git", Nonce: "n1"}).OK)
	require.True(t, h.v5(KindRepoRename, &RepoRename{From: "b/x.git", To: "a/x.git", Nonce: "n2"}).OK)
	for _, n := range []string{"n1", "n2"} {
		_, ok := h.table.CommittedNonce(n)
		assert.True(t, ok, n)
	}
	_, ok := h.table.CommittedNonce("n3")
	assert.False(t, ok, "a third rename never applied")
	require.True(t, h.v5(KindRepoDelete, &RepoDelete{Repo: "a/x.git", RepoID: 7, Nonce: "d1"}).OK)
	_, ok = h.table.CommittedNonce("d1")
	assert.True(t, ok)

	require.True(t, h.applyAt(1, KindRepoDelete, &RepoDelete{Repo: "b/x.git", Nonce: "d0"}).OK)
	_, ok = h.table.CommittedNonce("d0")
	assert.False(t, ok, "below version 5 a lifecycle command keeps no nonce")
}

// An owner's rename lists its keys in the proposer's table, maybe behind
// the log: from version 5 Apply refuses a list that misses a live key of
// the owner (#56).
func TestHuntR2ProveOwnerLag(t *testing.T) {
	h := newHarness(t)
	require.True(t, h.v5(KindRefTxn, push("alice/old.git", RefUpdate{Ref: "refs/heads/main", New: sha1A})).OK)
	require.True(t, h.v5(KindRefTxn, push("alice/new.git", RefUpdate{Ref: "refs/heads/main", New: sha1B})).OK)
	require.True(t, h.v5(KindRefTxn, push("alicex/other.git", RefUpdate{Ref: "refs/heads/main", New: sha1B})).OK)
	owner := [2]string{"alice", "carol"}
	behind := &RepoRename{From: "alice/old.git", To: "carol/old.git", Owner: owner}
	res := h.v5(KindRepoRename, behind)
	assert.False(t, res.OK)
	assert.Contains(t, res.Reason, "alice/new.git")
	v, _ := h.table.Get("alice/old.git", "refs/heads/main")
	assert.True(t, v.Live(), "nothing moved")

	// A move out of the owner is refused too.
	assert.False(t, h.v5(KindRepoRename, &RepoRename{From: "alice/old.git", To: "dave/old.git", Also: [][2]string{{"alice/new.git", "carol/new.git"}}, Owner: owner}).OK)

	// The whole list passes; alicex is another owner.
	res = h.v5(KindRepoRename, &RepoRename{From: "alice/old.git", To: "carol/old.git", Also: [][2]string{{"alice/new.git", "carol/new.git"}}, Owner: owner})
	require.True(t, res.OK, res.Reason)
	v, _ = h.table.Get("carol/new.git", "refs/heads/main")
	assert.True(t, v.Live())

	// Version 4 ignores the owner, as an older binary does.
	h = newHarness(t)
	require.True(t, h.applyAt(4, KindRefTxn, push("alice/old.git", RefUpdate{Ref: "refs/heads/main", New: sha1A})).OK)
	require.True(t, h.applyAt(4, KindRefTxn, push("alice/new.git", RefUpdate{Ref: "refs/heads/main", New: sha1B})).OK)
	assert.True(t, h.apply(KindRepoRename, behind).OK)
}

// The control: an owner's rename that moves every key passes, with its
// wiki and with no key at all under another owner.
func TestHuntR2ProveOwnerLagControlUpToDate(t *testing.T) {
	h := newHarness(t)
	require.True(t, h.v5(KindRefTxn, push("alice/new.git", RefUpdate{Ref: "refs/heads/main", New: sha1B})).OK)
	require.True(t, h.v5(KindRefTxn, push("alice/new.wiki.git", RefUpdate{Ref: "refs/heads/main", New: sha1B})).OK)
	res := h.v5(KindRepoRename, &RepoRename{From: "alice/new.git", To: "carol/new.git", Also: [][2]string{{"alice/new.wiki.git", "carol/new.wiki.git"}}, Owner: [2]string{"alice", "carol"}})
	assert.True(t, res.OK, res.Reason)
}

// A repository whose name starts with a dot (".profile") is Forgejo's own;
// Apply takes it from version 5, never below (#54). "." and ".." stay
// refused, and so does an owner that starts with a dot.
func TestHuntR2ProveDotname(t *testing.T) {
	h := newHarness(t)
	tx := push("user4/.profile.git", RefUpdate{Ref: "refs/heads/main", New: sha1A})
	assert.False(t, h.applyAt(4, KindRefTxn, tx).OK, "version 4 refuses a leading dot")
	assert.True(t, h.v5(KindRefTxn, tx).OK)
	assert.True(t, h.v5(KindRefTxn, push("user4/.profile.wiki.git", RefUpdate{Ref: "refs/heads/main", New: sha1A})).OK)
	assert.True(t, h.v5(KindRepoRename, &RepoRename{From: "user4/.profile.git", To: "user4/.me.git"}).OK)
	assert.True(t, h.v5(KindRepoDelete, &RepoDelete{Repo: "user4/.me.git"}).OK)

	for _, key := range []string{"user4/..git", "user4/...git", ".cluster-stale/x.git", "./x.git", "user4/.git", "../x.git", "user4/../x.git"} {
		require.Error(t, ValidRepoKeyAt(key, GenerationVersion), key)
	}
	require.NoError(t, ValidRepoKeyAt("user4/.profile.git", GenerationVersion))
	require.Error(t, ValidRepoKeyAt("user4/.profile.git", 4))
	assert.NoError(t, ValidRepoKey("user4/.profile.git"), "a key the table may hold")
}

// Two members binding one grant to two joiners: from version 5 the second
// binding is refused, the first stands (#70); an operator's new grant, of
// another expiry, still replaces it.
func TestJoinGrantBindingIsACompareAndSet(t *testing.T) {
	k1, k2 := strings.Repeat("1", 64), strings.Repeat("2", 64)
	h := newHarness(t)
	require.True(t, h.apply(KindJoinGrant, &JoinGrant{Slot: "node-d", Expires: 100, Operator: "op"}).OK)
	require.True(t, h.v5(KindJoinGrant, &JoinGrant{Slot: "node-d", SPKI: k1, Expires: 100, Operator: "op"}).OK)
	res := h.v5(KindJoinGrant, &JoinGrant{Slot: "node-d", SPKI: k2, Expires: 100, Operator: "op"})
	assert.False(t, res.OK)
	g, _ := h.table.JoinGrant("node-d")
	assert.Equal(t, k1, g.SPKI)
	assert.True(t, h.v5(KindJoinGrant, &JoinGrant{Slot: "node-d", SPKI: k1, Expires: 100, Operator: "op"}).OK, "the same binding again")
	assert.True(t, h.v5(KindJoinGrant, &JoinGrant{Slot: "node-d", Expires: 200, Operator: "op"}).OK, "a new grant")

	// Below version 5, the last binding wins, as before.
	h = newHarness(t)
	require.True(t, h.apply(KindJoinGrant, &JoinGrant{Slot: "node-d", SPKI: k1, Expires: 100, Operator: "op"}).OK)
	assert.True(t, h.apply(KindJoinGrant, &JoinGrant{Slot: "node-d", SPKI: k2, Expires: 100, Operator: "op"}).OK)
}

// The deletions of a key outlive its revival, through a snapshot: the
// projector tells by them which repository a directory holds (#52, #98).
func TestDeletionsOutliveTheRevival(t *testing.T) {
	h := newHarness(t)
	require.True(t, h.apply(KindRefTxn, push("s/x.git", RefUpdate{Ref: "refs/heads/main", New: sha1A})).OK)
	require.True(t, h.apply(KindRepoDelete, &RepoDelete{Repo: "s/x.git"}).OK)
	del := h.index
	require.True(t, h.apply(KindRefTxn, h.gitInit("s/x.git", "sha1")).OK)
	_, deleted := h.table.Deleted("s/x.git")
	require.False(t, deleted)
	at, ok := h.table.DeletedBefore("s/x.git", ^uint64(0))
	require.True(t, ok)
	assert.Equal(t, del, at)
	_, ok = h.table.DeletedBefore("s/x.git", del)
	assert.False(t, ok, "none before the deletion itself")

	tb := restored(t, h.table)
	at, ok = tb.DeletedBefore("s/x.git", ^uint64(0))
	assert.True(t, ok)
	assert.Equal(t, del, at)
}

func restored(t *testing.T, from *Table) *Table {
	t.Helper()
	snap, err := from.Snapshot()
	require.NoError(t, err)
	var b strings.Builder
	_, err = snap.WriteTo(&b)
	require.NoError(t, err)
	tb := New()
	require.NoError(t, tb.Restore(strings.NewReader(b.String()), consensus.SnapshotMeta{}))
	return tb
}

// olderBinary is a table that applies payload versions up to 4, as the
// binaries before version 5 do.
func olderBinary() *Table {
	tb := New()
	tb.limit = 4
	return tb
}

func raiseTo(h *harness, v int) {
	h.t.Helper()
	require.True(h.t, h.apply(KindAudit, &Audit{Operator: "op", Action: AuditPayloadVersion, Target: fmt.Sprint(v), Outcome: AuditRaised}).OK)
}

func snapshotJSON(t *testing.T, tb *Table) map[string]any {
	t.Helper()
	snap, err := tb.Snapshot()
	require.NoError(t, err)
	var b strings.Builder
	_, err = snap.WriteTo(&b)
	require.NoError(t, err)
	var m map[string]any
	require.NoError(t, json.Unmarshal([]byte(b.String()), &m))
	return m
}

// The snapshot is written in format 1 until the cluster raised its version
// to 5, in format 2 from then on: a binary of version 4 refuses format 2,
// and stops, rather than restore it and lose what it cannot keep (#50).
func TestSnapshotFormat2FromVersion5(t *testing.T) {
	h := newHarness(t)
	raiseTo(h, 4)
	assert.InDelta(t, 1, snapshotJSON(t, h.table)["format"], 0, "format 1 while the cluster writes version 4")
	err := olderBinary().Restore(strings.NewReader(mustSnapshot(t, h.table)), consensus.SnapshotMeta{})
	require.NoError(t, err, "a version 4 member restores it")

	raiseTo(h, 5)
	require.Equal(t, uint8(5), h.table.WriteVersion())
	assert.InDelta(t, 2, snapshotJSON(t, h.table)["format"], 0)
	err = New().Restore(strings.NewReader(mustSnapshot(t, h.table)), consensus.SnapshotMeta{})
	require.NoError(t, err, "a version 5 member restores format 2")
	err = olderBinary().Restore(strings.NewReader(mustSnapshot(t, h.table)), consensus.SnapshotMeta{})
	require.ErrorIs(t, err, ErrVersion, "a version 4 member stops on format 2")
}

func mustSnapshot(t *testing.T, tb *Table) string {
	t.Helper()
	snap, err := tb.Snapshot()
	require.NoError(t, err)
	var b strings.Builder
	_, err = snap.WriteTo(&b)
	require.NoError(t, err)
	return b.String()
}

// An older leader re-cuts, in format 1 and without what it does not know,
// the state of a cluster that raised its version to 5: a current member
// that installs that snapshot refuses it rather than lose the revocations
// and the departed repositories without a word (#50).
func TestHuntR1ProveOldCutRestoredByCurrent(t *testing.T) {
	h := newHarness(t)
	raiseTo(h, 5)
	key := strings.Repeat("ab", 32)
	require.True(t, h.applyAt(RevokeVersion, KindRevokeKey, &RevokeKey{SPKI: key, Slot: "node-c", Operator: "op"}).OK)
	require.True(t, h.v5(KindRefTxn, push("o/x.git", RefUpdate{Ref: "refs/heads/main", New: sha1A})).OK)
	require.True(t, h.v5(KindRepoDelete, &RepoDelete{Repo: "o/x.git", RepoID: 7}).OK)

	var cut map[string]any
	require.NoError(t, json.Unmarshal([]byte(mustSnapshot(t, h.table)), &cut))
	// What an older binary writes back: format 1, no field it does not
	// know, its own payload version; the raise stays in the audit records.
	cut["format"], cut["payload"] = 1, 4
	for _, k := range []string{"revoked", "pending_revoke", "deletions", "departed_at"} {
		delete(cut, k)
	}
	b, err := json.Marshal(cut)
	require.NoError(t, err)

	tb := New()
	err = tb.Restore(strings.NewReader(string(b)), consensus.SnapshotMeta{})
	require.ErrorIs(t, err, ErrVersion, "KeyRevoked=%v", tb.KeyRevoked(key))
	assert.Equal(t, uint8(BaseVersion), tb.WriteVersion(), "the refused snapshot left the table as it was")

	// The same cut before the raise to 5 restores, as it always did.
	h = newHarness(t)
	raiseTo(h, 4)
	require.NoError(t, New().Restore(strings.NewReader(mustSnapshot(t, h.table)), consensus.SnapshotMeta{}))
}

// A mixed cluster, both ways. A member of version 4 applies the raise to 5
// as a plain record and keeps writing version 4, which a member of version
// 5 applies; on the first entry of version 5 it stops (D18), never skips.
func TestMixedVersion4And5(t *testing.T) {
	older, current := &harness{t: t, table: olderBinary()}, newHarness(t)
	for _, h := range []*harness{older, current} {
		raiseTo(h, 4)
		raiseTo(h, 5)
	}
	assert.Equal(t, uint8(4), older.table.WriteVersion(), "a version 4 binary does not adopt a raise it cannot apply")
	assert.Equal(t, uint8(5), current.table.WriteVersion())

	// Version 4 entries, written by the older member: both apply them alike.
	for _, h := range []*harness{older, current} {
		assert.True(t, h.applyAt(4, KindRefTxn, push("o/x.git", RefUpdate{Ref: "refs/heads/main", New: sha1A})).OK)
	}
	assert.Equal(t, older.table.Root(), current.table.Root())

	// A version 5 entry stops the older member.
	data, err := EncodeVersion(5, KindRepoDelete, &RepoDelete{Repo: "o/x.git", RepoID: 7})
	require.NoError(t, err)
	older.index++
	e := consensus.Entry{
		Term: 1, Index: older.index, Kind: consensus.EntryCommand, Data: data,
		Token: consensus.Token{Cluster: consensus.ClusterID{1}, Epoch: 1, Term: 1, Index: older.index},
	}
	assert.PanicsWithError(t, "VERSION_UNSUPPORTED: payload version 5", func() { older.table.Apply(e) })
	assert.True(t, current.v5(KindRepoDelete, &RepoDelete{Repo: "o/x.git", RepoID: 7}).OK)
}
