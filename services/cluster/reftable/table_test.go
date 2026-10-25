// Copyright 2026 The Forgejo Authors. All rights reserved.
// SPDX-License-Identifier: GPL-3.0-or-later

package reftable

import (
	"bytes"
	"strings"
	"testing"

	"forgejo.org/modules/json"
	"forgejo.org/services/cluster/consensus"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	sha1A = "1111111111111111111111111111111111111111"
	sha1B = "2222222222222222222222222222222222222222"
	sha1C = "3333333333333333333333333333333333333333"
)

type harness struct {
	t     *testing.T
	table *Table
	index uint64
}

func newHarness(t *testing.T) *harness { return &harness{t: t, table: New()} }

func (h *harness) apply(kind Kind, body any) Result {
	h.t.Helper()
	data, err := Encode(kind, body)
	require.NoError(h.t, err)
	h.index++
	e := consensus.Entry{
		Term: 1, Index: h.index, Kind: consensus.EntryCommand,
		Version: PayloadVersion, Data: data,
		Token: consensus.Token{Cluster: consensus.ClusterID{1}, Epoch: 1, Term: 1, Index: h.index},
	}
	return h.table.Apply(e).(Result)
}

func push(repo string, updates ...RefUpdate) *RefTxn {
	return &RefTxn{Repo: repo, RepoID: 7, Format: "sha1", Updates: updates, PusherID: 3, Time: 42}
}

func TestCreateUpdateDelete(t *testing.T) {
	h := newHarness(t)
	res := h.apply(KindRefTxn, push("alice/demo.git", RefUpdate{Ref: "refs/heads/main", New: sha1A}))
	require.True(t, res.OK, res.Reason)
	assert.Equal(t, []uint64{1}, res.Revs)

	v, ok := h.table.Get("alice/demo.git", "refs/heads/main")
	require.True(t, ok)
	assert.Equal(t, sha1A, v.SHA)

	res = h.apply(KindRefTxn, push("alice/demo.git", RefUpdate{Ref: "refs/heads/main", Old: sha1A, ExpectRev: 1, New: sha1B}))
	require.True(t, res.OK, res.Reason)
	res = h.apply(KindRefTxn, push("alice/demo.git", RefUpdate{Ref: "refs/heads/main", Old: sha1B, ExpectRev: 2, Delete: true}))
	require.True(t, res.OK, res.Reason)
	v, _ = h.table.Get("alice/demo.git", "refs/heads/main")
	assert.True(t, v.Tombstone)
	assert.EqualValues(t, 3, v.Rev)

	// Recreating continues the revision from the tombstone, never from 0.
	res = h.apply(KindRefTxn, push("alice/demo.git", RefUpdate{Ref: "refs/heads/main", ExpectRev: 3, New: sha1C}))
	require.True(t, res.OK, res.Reason)
	assert.Equal(t, []uint64{4}, res.Revs)
	assert.Len(t, h.table.Events(), 4)
}

func TestStaleRevisionIsRefusedAtomically(t *testing.T) {
	h := newHarness(t)
	require.True(t, h.apply(KindRefTxn, push("a/b.git", RefUpdate{Ref: "refs/heads/x", New: sha1A})).OK)
	before := h.table.Root()
	// The second update is stale: nothing of the transaction applies.
	res := h.apply(KindRefTxn, push("a/b.git",
		RefUpdate{Ref: "refs/heads/y", New: sha1B},
		RefUpdate{Ref: "refs/heads/x", ExpectRev: 0, New: sha1C}))
	assert.False(t, res.OK)
	assert.Contains(t, res.Reason, "changed since it was read")
	_, ok := h.table.Get("a/b.git", "refs/heads/y")
	assert.False(t, ok)
	assert.Equal(t, before, h.table.Root())
}

func TestABAIsRefused(t *testing.T) {
	h := newHarness(t)
	require.True(t, h.apply(KindRefTxn, push("a/b.git", RefUpdate{Ref: "refs/heads/x", New: sha1A})).OK)
	require.True(t, h.apply(KindRefTxn, push("a/b.git", RefUpdate{Ref: "refs/heads/x", ExpectRev: 1, New: sha1B})).OK)
	require.True(t, h.apply(KindRefTxn, push("a/b.git", RefUpdate{Ref: "refs/heads/x", ExpectRev: 2, New: sha1A})).OK)
	// Same SHA as at revision 1, but the revision moved: refused.
	res := h.apply(KindRefTxn, push("a/b.git", RefUpdate{Ref: "refs/heads/x", Old: sha1A, ExpectRev: 1, New: sha1C}))
	assert.False(t, res.OK)
}

func TestOldValueMismatch(t *testing.T) {
	h := newHarness(t)
	require.True(t, h.apply(KindRefTxn, push("a/b.git", RefUpdate{Ref: "refs/heads/x", New: sha1A})).OK)
	res := h.apply(KindRefTxn, push("a/b.git", RefUpdate{Ref: "refs/heads/x", Old: sha1B, ExpectRev: 1, New: sha1C}))
	assert.False(t, res.OK)
	assert.Contains(t, res.Reason, "git expected")
}

func TestValidation(t *testing.T) {
	cases := map[string]*RefTxn{
		"escape":       push("../etc.git", RefUpdate{Ref: "refs/heads/x", New: sha1A}),
		"upper":        push("Alice/demo.git", RefUpdate{Ref: "refs/heads/x", New: sha1A}),
		"no .git":      push("alice/demo", RefUpdate{Ref: "refs/heads/x", New: sha1A}),
		"bad ref":      push("a/b.git", RefUpdate{Ref: "refs/heads/a..b", New: sha1A}),
		"lock":         push("a/b.git", RefUpdate{Ref: "refs/heads/a.lock", New: sha1A}),
		"onelevel":     push("a/b.git", RefUpdate{Ref: "main", New: sha1A}),
		"bad sha":      push("a/b.git", RefUpdate{Ref: "refs/heads/x", New: "zz"}),
		"zero sha":     push("a/b.git", RefUpdate{Ref: "refs/heads/x", New: strings.Repeat("0", 40)}),
		"two modes":    push("a/b.git", RefUpdate{Ref: "refs/heads/x", New: sha1A, Delete: true}),
		"direct HEAD":  push("a/b.git", RefUpdate{Ref: "HEAD", New: sha1A}),
		"delete none":  push("a/b.git", RefUpdate{Ref: "refs/heads/x", Delete: true}),
		"duplicate":    push("a/b.git", RefUpdate{Ref: "refs/heads/x", New: sha1A}, RefUpdate{Ref: "refs/heads/x", New: sha1B}),
		"empty":        push("a/b.git"),
		"bad symbolic": push("a/b.git", RefUpdate{Ref: "HEAD", Symbolic: "main"}),
	}
	for name, tx := range cases {
		t.Run(name, func(t *testing.T) {
			h := newHarness(t)
			res := h.apply(KindRefTxn, tx)
			assert.False(t, res.OK)
			assert.NotEmpty(t, res.Reason)
			assert.True(t, h.table.Root().IsZero())
		})
	}
}

func TestDirectoryFileConflict(t *testing.T) {
	h := newHarness(t)
	require.True(t, h.apply(KindRefTxn, push("a/b.git", RefUpdate{Ref: "refs/heads/a/b", New: sha1A})).OK)
	assert.False(t, h.apply(KindRefTxn, push("a/b.git", RefUpdate{Ref: "refs/heads/a", New: sha1B})).OK)
	assert.False(t, h.apply(KindRefTxn, push("a/b.git", RefUpdate{Ref: "refs/heads/a/b/c", New: sha1B})).OK)
	// Deleting the blocker in the same transaction lifts the conflict.
	res := h.apply(KindRefTxn, push("a/b.git",
		RefUpdate{Ref: "refs/heads/a/b", ExpectRev: 1, Delete: true},
		RefUpdate{Ref: "refs/heads/a", New: sha1B}))
	assert.True(t, res.OK, res.Reason)
	// A tombstone does not count as existing: with refs/heads/a deleted,
	// refs/heads/a/b/c conflicts with tombstones only.
	require.True(t, h.apply(KindRefTxn, push("a/b.git", RefUpdate{Ref: "refs/heads/a", ExpectRev: 1, Delete: true})).OK)
	res = h.apply(KindRefTxn, push("a/b.git", RefUpdate{Ref: "refs/heads/a/b/c", ExpectRev: 0, New: sha1C}))
	assert.True(t, res.OK, res.Reason)
	// The smallest live descendant is the one reported.
	require.True(t, h.apply(KindRefTxn, push("a/b.git", RefUpdate{Ref: "refs/heads/z/m", New: sha1A}, RefUpdate{Ref: "refs/heads/z/b/q", New: sha1A})).OK)
	res = h.apply(KindRefTxn, push("a/b.git", RefUpdate{Ref: "refs/heads/z", New: sha1B}))
	require.False(t, res.OK)
	assert.Contains(t, res.Reason, "refs/heads/z/b/q")
}

func TestSymbolicHEAD(t *testing.T) {
	h := newHarness(t)
	res := h.apply(KindRefTxn, push("a/b.git", RefUpdate{Ref: "HEAD", Symbolic: "refs/heads/main"}))
	require.True(t, res.OK, res.Reason)
	v, _ := h.table.Get("a/b.git", "HEAD")
	assert.Equal(t, "refs/heads/main", v.Symbolic)
	assert.Equal(t, "ref:refs/heads/main", h.table.Events()[0].New)
}

func TestChecksumIsOrderIndependentAndIncremental(t *testing.T) {
	a, b := newHarness(t), newHarness(t)
	require.True(t, a.apply(KindRefTxn, push("a/x.git", RefUpdate{Ref: "refs/heads/m", New: sha1A})).OK)
	require.True(t, a.apply(KindRefTxn, push("b/y.git", RefUpdate{Ref: "refs/heads/m", New: sha1B})).OK)
	require.True(t, b.apply(KindRefTxn, push("b/y.git", RefUpdate{Ref: "refs/heads/m", New: sha1B})).OK)
	require.True(t, b.apply(KindRefTxn, push("a/x.git", RefUpdate{Ref: "refs/heads/m", New: sha1A})).OK)
	assert.Equal(t, a.table.Root(), b.table.Root())
	assert.False(t, a.table.Root().IsZero())

	// The incremental root equals the root recomputed from a snapshot.
	snap, err := a.table.Snapshot()
	require.NoError(t, err)
	var buf bytes.Buffer
	_, err = snap.WriteTo(&buf)
	require.NoError(t, err)
	c := New()
	require.NoError(t, c.Restore(&buf, consensus.SnapshotMeta{}))
	assert.Equal(t, a.table.Root(), c.Root())
	ha, _ := a.table.RepoHash("a/x.git")
	hc, _ := c.RepoHash("a/x.git")
	assert.Equal(t, ha, hc)
	pa, _ := a.table.Applied()
	pc, _ := c.Applied()
	assert.Equal(t, pa, pc)
}

func TestLeafKnownAnswer(t *testing.T) {
	// Pinned so an accidental change of the encoding cannot pass unnoticed.
	// Both values were cross-checked with an independent Python encoder.
	l := leaf("a/b.git", "refs/heads/main", &RefValue{SHA: sha1A, Format: "sha1", Rev: 1})
	assert.Equal(t, "699085d33b967a52ad96c8b21f1d864817ab67e95b405a44c1a3535c395d6b40", l.String())
	assert.Equal(t, "50bf8c5508c10607180ec711534a109d0e136e4d384ae5481ce5ba014a7114bf", rootTerm("a/b.git", Sum{1, 2, 3, 4}).String())
	// Adding then removing a leaf returns to zero.
	var s Sum
	assert.True(t, s.add(l).sub(l).IsZero())
	// Carry across limbs.
	maxLimb := Sum{^uint64(0), ^uint64(0), ^uint64(0), ^uint64(0)}
	assert.True(t, maxLimb.add(Sum{1}).IsZero())
}

func TestAckTrimsEvents(t *testing.T) {
	h := newHarness(t)
	require.True(t, h.apply(KindRefTxn, push("a/b.git", RefUpdate{Ref: "refs/heads/x", New: sha1A})).OK)
	require.True(t, h.apply(KindRefTxn, push("a/b.git", RefUpdate{Ref: "refs/heads/y", New: sha1B})).OK)
	id := consensus.LogIdentity{Cluster: consensus.ClusterID{1}, Epoch: 1}.String()
	require.True(t, h.apply(KindAck, &Ack{Identity: id, Index: 1}).OK)
	evs := h.table.Events()
	require.Len(t, evs, 1)
	assert.Equal(t, "refs/heads/y", evs[0].Ref)
	// An ack of another lineage trims nothing.
	h.apply(KindAck, &Ack{Identity: "other/1", Index: 100})
	assert.Len(t, h.table.Events(), 1)
	assert.Equal(t, []string{sha1B}, h.table.PinnedObjects("a/b.git"))
}

// LastEventOf names the newest pending event of a repository up to an index
// of the current lineage: what a read of the repository's SQL as of that
// index waits for the consumer to derive.
func TestLastEventOf(t *testing.T) {
	h := newHarness(t)
	id := consensus.LogIdentity{Cluster: consensus.ClusterID{1}, Epoch: 1}
	_, ok := h.table.LastEventOf("a/b.git", id, 10)
	assert.False(t, ok, "no event")
	require.True(t, h.apply(KindRefTxn, push("a/b.git", RefUpdate{Ref: "refs/heads/x", New: sha1A})).OK)
	require.True(t, h.apply(KindRefTxn, push("c/d.git", RefUpdate{Ref: "refs/heads/x", New: sha1A})).OK)
	require.True(t, h.apply(KindRefTxn, push("a/b.git", RefUpdate{Ref: "refs/heads/y", New: sha1B})).OK)

	tok, ok := h.table.LastEventOf("a/b.git", id, 10)
	require.True(t, ok)
	assert.EqualValues(t, 3, tok.Index)
	tok, ok = h.table.LastEventOf("a/b.git", id, 2)
	require.True(t, ok)
	assert.EqualValues(t, 1, tok.Index, "an event after the index is not waited for")
	_, ok = h.table.LastEventOf("a/b.git", id, 0)
	assert.False(t, ok)
	_, ok = h.table.LastEventOf("e/f.git", id, 10)
	assert.False(t, ok, "another repository's events are not waited for")
	// The events of an earlier lineage come before any index of a later
	// one.
	tok, ok = h.table.LastEventOf("a/b.git", consensus.LogIdentity{Cluster: consensus.ClusterID{1}, Epoch: 2}, 0)
	require.True(t, ok)
	assert.EqualValues(t, 3, tok.Index)
	// An event trimmed from the queue was dispatched: nothing waits for it.
	require.True(t, h.apply(KindAck, &Ack{Identity: id.String(), Index: 3}).OK)
	_, ok = h.table.LastEventOf("a/b.git", id, 10)
	assert.False(t, ok)
}

func TestAuditRecords(t *testing.T) {
	h := newHarness(t)
	assert.False(t, h.apply(KindAudit, &Audit{Operator: "has space", Action: "x", Outcome: "ok"}).OK)
	require.True(t, h.apply(KindAudit, &Audit{Operator: "forgejo-user:1", Action: "promote", Target: "n", Outcome: "ok", Time: 5}).OK)
	a := h.table.Audit()
	require.Len(t, a, 1)
	assert.Equal(t, "promote", a[0].Action)
	assert.EqualValues(t, 2, a[0].Token.Index)
}

func TestRepoDeleteAndRename(t *testing.T) {
	h := newHarness(t)
	require.True(t, h.apply(KindRefTxn, push("a/b.git", RefUpdate{Ref: "refs/heads/x", New: sha1A})).OK)
	require.True(t, h.apply(KindRepoRename, &RepoRename{From: "a/b.git", To: "a/c.git"}).OK)
	v, _ := h.table.Get("a/c.git", "refs/heads/x")
	assert.Equal(t, sha1A, v.SHA)
	v, _ = h.table.Get("a/b.git", "refs/heads/x")
	assert.True(t, v.Tombstone)
	from, _, ok := h.table.RenamedFrom("a/c.git")
	assert.True(t, ok)
	assert.Equal(t, "a/b.git", from)

	require.True(t, h.apply(KindRepoDelete, &RepoDelete{Repo: "a/c.git"}).OK)
	_, deleted := h.table.Deleted("a/c.git")
	assert.True(t, deleted)
	v, _ = h.table.Get("a/c.git", "refs/heads/x")
	assert.True(t, v.Tombstone)
}

func TestUnknownVersionStops(t *testing.T) {
	tbl := New()
	assert.Panics(t, func() {
		tbl.Apply(consensus.Entry{Data: []byte{PayloadVersion + 1, byte(KindRefTxn), '{', '}'}})
	})
	assert.Panics(t, func() {
		tbl.Apply(consensus.Entry{Data: []byte{PayloadVersion, 99, '{', '}'}})
	})
	// A malformed body of a known kind is a refusal, identical everywhere.
	res := tbl.Apply(consensus.Entry{Data: []byte{PayloadVersion, byte(KindRefTxn), '{'}}).(Result)
	assert.False(t, res.OK)
}

func TestEventQueueCap(t *testing.T) {
	h := newHarness(t)
	h.table.eventBytes = MaxEventBytes - 10
	res := h.apply(KindRefTxn, push("a/b.git", RefUpdate{Ref: "refs/heads/x", New: sha1A}))
	assert.False(t, res.OK)
	assert.Contains(t, res.Reason, "event queue is full")
}

func TestRefNames(t *testing.T) {
	for _, ok := range []string{"HEAD", "refs/heads/main", "refs/tags/v1.0", "refs/pull/1/head", "refs/heads/feat/x-y_z", "refs/heads/main\u00a0x", "refs/heads/\u3000"} {
		require.NoError(t, ValidRefName(ok), ok)
	}
	for _, bad := range []string{"", "refs/", "refs/heads/", "refs//x", "refs/heads/.x", "refs/heads/x.", "refs/heads/a@{b", "refs/heads/a b", "refs/heads/a:b", "refs/heads/a\\b", "refs/heads/a*", "refs/heads/a\x01", "FETCH_HEAD", "refs/heads/a\xffb"} {
		assert.Error(t, ValidRefName(bad), bad)
	}
}

func TestJoinGrants(t *testing.T) {
	h := newHarness(t)
	assert.False(t, h.apply(KindJoinGrant, &JoinGrant{Slot: "Bad Slot", Expires: 1, Operator: "op"}).OK)
	assert.False(t, h.apply(KindJoinGrant, &JoinGrant{Slot: "node-d", Expires: 1, Operator: "op", SPKI: "xyz"}).OK)
	assert.False(t, h.apply(KindJoinGrant, &JoinGrant{Slot: "node-d", Operator: "op"}).OK)
	require.True(t, h.apply(KindJoinGrant, &JoinGrant{Slot: "node-d", Expires: 10, Operator: "forgejo-user:1"}).OK)
	g, ok := h.table.JoinGrant("node-d")
	require.True(t, ok)
	assert.EqualValues(t, 10, g.Expires)

	snap, err := h.table.Snapshot()
	require.NoError(t, err)
	var buf bytes.Buffer
	_, err = snap.WriteTo(&buf)
	require.NoError(t, err)
	c := New()
	require.NoError(t, c.Restore(&buf, consensus.SnapshotMeta{}))
	g, ok = c.JoinGrant("node-d")
	require.True(t, ok)
	assert.Equal(t, "forgejo-user:1", g.Operator)
}

// A revoked key is kept for good, survives a snapshot, and voids a grant
// bound to it. It is written at RevokeVersion only: an older envelope is
// refused, never applied.
func TestRevokedKeys(t *testing.T) {
	h := newHarness(t)
	key := strings.Repeat("ab", 32)
	assert.False(t, h.applyAt(RevokeVersion, KindRevokeKey, &RevokeKey{SPKI: "xyz", Slot: "node-b", Operator: "forgejo-user:1"}).OK)
	assert.False(t, h.applyAt(RevokeVersion, KindRevokeKey, &RevokeKey{SPKI: key, Slot: "Bad Slot", Operator: "forgejo-user:1"}).OK)
	require.True(t, h.apply(KindJoinGrant, &JoinGrant{Slot: "node-b", SPKI: key, Expires: 10, Operator: "forgejo-user:1"}).OK)
	assert.False(t, h.apply(KindRevokeKey, &RevokeKey{SPKI: key, Slot: "node-b", Operator: "forgejo-user:1"}).OK, "below RevokeVersion")
	assert.False(t, h.table.KeyRevoked(key))
	require.True(t, h.applyAt(RevokeVersion, KindRevokeKey, &RevokeKey{SPKI: key, Slot: "node-b", Operator: "forgejo-user:1"}).OK)
	assert.True(t, h.table.KeyRevoked(key))
	_, ok := h.table.JoinGrant("node-b")
	assert.False(t, ok, "a grant bound to the revoked key is gone")

	snap, err := h.table.Snapshot()
	require.NoError(t, err)
	var buf bytes.Buffer
	_, err = snap.WriteTo(&buf)
	require.NoError(t, err)
	c := New()
	require.NoError(t, c.Restore(&buf, consensus.SnapshotMeta{}))
	assert.True(t, c.KeyRevoked(key))
}

// A revocation asked for before the cluster writes RevokeVersion waits as
// a version 1 audit record: it revokes nothing yet, an older member keeps
// it as a plain record, and a snapshot cut by such a member, without the
// field, reads it back from the audit kept. Writing it ends the wait.
func TestPendingRevocationWaitsAsAnAuditRecord(t *testing.T) {
	h := newHarness(t)
	key := strings.Repeat("cd", 32)
	require.True(t, h.apply(KindAudit, &Audit{ID: "r1", Operator: "forgejo-user:1", Action: AuditRevokeKey, Target: "node-b:" + key, Outcome: AuditPending}).OK)
	require.True(t, h.apply(KindAudit, &Audit{Operator: "forgejo-user:1", Action: AuditRevokeKey, Target: "node-b:junk", Outcome: AuditPending}).OK)
	assert.False(t, h.table.KeyRevoked(key))
	require.Len(t, h.table.PendingRevocations(), 1)
	assert.Equal(t, RevokeKey{SPKI: key, Slot: "node-b", Operator: "forgejo-user:1"}, h.table.PendingRevocations()[0])

	snap, err := h.table.Snapshot()
	require.NoError(t, err)
	var buf bytes.Buffer
	_, err = snap.WriteTo(&buf)
	require.NoError(t, err)
	var fields map[string]any
	require.NoError(t, json.Unmarshal(buf.Bytes(), &fields))
	require.Contains(t, fields, "pending_revoke")
	delete(fields, "pending_revoke")
	old, err := json.Marshal(fields)
	require.NoError(t, err)
	c := New()
	require.NoError(t, c.Restore(bytes.NewReader(old), consensus.SnapshotMeta{}))
	require.Len(t, c.PendingRevocations(), 1, "read back from the audit kept")

	require.True(t, h.applyAt(RevokeVersion, KindRevokeKey, &RevokeKey{SPKI: key, Slot: "node-b", Operator: "forgejo-user:1"}).OK)
	assert.True(t, h.table.KeyRevoked(key))
	assert.Empty(t, h.table.PendingRevocations())
}

func TestAuditRetryIsKeptOnce(t *testing.T) {
	h := newHarness(t)
	a := &Audit{ID: "abc", Operator: "forgejo-user:1", Action: "promote", Outcome: "ok"}
	require.True(t, h.apply(KindAudit, a).OK)
	require.True(t, h.apply(KindAudit, a).OK)
	assert.Len(t, h.table.Audit(), 1)
	assert.True(t, h.table.HasAudit("abc"))
}

// A repository and its wiki move in one command, all or none.
func TestRenameMovesSeveralRepositoriesAtomically(t *testing.T) {
	h := newHarness(t)
	require.True(t, h.apply(KindRefTxn, push("o/r.git", RefUpdate{Ref: "refs/heads/main", New: sha1A})).OK)
	require.True(t, h.apply(KindRefTxn, push("o/r.wiki.git", RefUpdate{Ref: "refs/heads/main", New: sha1B})).OK)
	require.True(t, h.apply(KindRefTxn, push("p/r.wiki.git", RefUpdate{Ref: "refs/heads/main", New: sha1C})).OK)
	before := h.table.Root()

	// The wiki's target holds refs: the repository does not move either.
	res := h.apply(KindRepoRename, &RepoRename{From: "o/r.git", To: "p/r.git", Also: [][2]string{{"o/r.wiki.git", "p/r.wiki.git"}}})
	require.False(t, res.OK)
	assert.Equal(t, before, h.table.Root())
	v, ok := h.table.Get("o/r.git", "refs/heads/main")
	require.True(t, ok)
	assert.False(t, v.Tombstone)

	// A chain through one of its own targets is refused.
	res = h.apply(KindRepoRename, &RepoRename{From: "o/r.git", To: "q/r.git", Also: [][2]string{{"q/r.git", "s/r.git"}}})
	require.False(t, res.OK)

	res = h.apply(KindRepoRename, &RepoRename{From: "o/r.git", To: "q/r.git", Also: [][2]string{{"o/r.wiki.git", "q/r.wiki.git"}}})
	require.True(t, res.OK, res.Reason)
	for _, k := range []string{"q/r.git", "q/r.wiki.git"} {
		v, ok := h.table.Get(k, "refs/heads/main")
		require.True(t, ok, k)
		assert.False(t, v.Tombstone, k)
	}
	from, _, ok := h.table.RenamedFrom("q/r.wiki.git")
	require.True(t, ok)
	assert.Equal(t, "o/r.wiki.git", from)
}

// RenamedAfter walks forward from an event: the oldest rename after it.
func TestRenamedAfter(t *testing.T) {
	h := newHarness(t)
	require.True(t, h.apply(KindRefTxn, push("a/b.git", RefUpdate{Ref: "refs/heads/x", New: sha1A})).OK)
	require.True(t, h.apply(KindRepoRename, &RepoRename{From: "a/b.git", To: "a/c.git"}).OK)
	require.True(t, h.apply(KindRepoRename, &RepoRename{From: "a/c.git", To: "a/d.git"}).OK)

	to, at, ok := h.table.RenamedAfter("a/b.git", 1)
	require.True(t, ok)
	assert.Equal(t, "a/c.git", to)
	assert.EqualValues(t, 2, at)
	to, _, ok = h.table.RenamedAfter("a/c.git", at)
	require.True(t, ok)
	assert.Equal(t, "a/d.git", to)
	_, _, ok = h.table.RenamedAfter("a/b.git", 2)
	assert.False(t, ok, "the rename is not after index 2")
	_, _, ok = h.table.RenamedAfter("a/d.git", 0)
	assert.False(t, ok)
}

// A node that rejoins under a new id replays the log on its table: an
// entry of the lineage already applied changes nothing, so a deletion
// followed by a recreation, or a rename and its undo, gives the same table
// (#41, TestHuntR1ProveRejoinReplay). Reset empties the table, and the
// replay from the start gives the same table again.
func TestReplayingAppliedEntriesKeepsTheTable(t *testing.T) {
	for name, cmds := range map[string][]struct {
		kind Kind
		body any
	}{
		"delete and recreate": {
			{KindRefTxn, push("o/x.git", RefUpdate{Ref: "refs/heads/main", New: sha1A})},
			{KindRepoDelete, &RepoDelete{Repo: "o/x.git"}},
			{KindRefTxn, push("o/x.git", RefUpdate{Ref: "refs/heads/main", New: sha1B, ExpectRev: 2})},
		},
		"rename and undo": {
			{KindRefTxn, push("p/a.git", RefUpdate{Ref: "refs/heads/main", New: sha1A})},
			{KindRepoRename, &RepoRename{From: "p/a.git", To: "p/b.git"}},
			{KindRepoRename, &RepoRename{From: "p/b.git", To: "p/a.git"}},
		},
	} {
		t.Run(name, func(t *testing.T) {
			entries := make([]consensus.Entry, 0, len(cmds))
			for i, c := range cmds {
				data, err := Encode(c.kind, c.body)
				require.NoError(t, err)
				idx := uint64(i + 1)
				entries = append(entries, consensus.Entry{
					Term: 1, Index: idx, Kind: consensus.EntryCommand, Version: PayloadVersion, Data: data,
					Token: consensus.Token{Cluster: consensus.ClusterID{1}, Epoch: 1, Term: 1, Index: idx},
				})
			}
			once, twice := New(), New()
			for _, e := range entries {
				require.True(t, once.Apply(e).(Result).OK)
				require.True(t, twice.Apply(e).(Result).OK)
			}
			for _, e := range entries {
				assert.False(t, twice.Apply(e).(Result).OK, "a replayed entry is not applied again")
			}
			assert.Equal(t, once.Root(), twice.Root())
			assert.Equal(t, once.Events(), twice.Events())

			twice.Reset()
			assert.Equal(t, Sum{}, twice.Root())
			pos, _ := twice.Applied()
			assert.Zero(t, pos.Index)
			assert.Empty(t, twice.Events())
			for _, e := range entries {
				require.True(t, twice.Apply(e).(Result).OK)
			}
			assert.Equal(t, once.Root(), twice.Root())
		})
	}
}
