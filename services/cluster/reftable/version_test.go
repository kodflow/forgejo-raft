// Copyright 2026 The Forgejo Authors. All rights reserved.
// SPDX-License-Identifier: GPL-3.0-or-later

package reftable

import (
	"bytes"
	"testing"

	"forgejo.org/modules/json"
	"forgejo.org/services/cluster/consensus"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func (h *harness) applyAt(v uint8, kind Kind, body any) Result {
	h.t.Helper()
	data, err := EncodeVersion(v, kind, body)
	require.NoError(h.t, err)
	h.index++
	e := consensus.Entry{
		Term: 1, Index: h.index, Kind: consensus.EntryCommand, Data: data,
		Token: consensus.Token{Cluster: consensus.ClusterID{1}, Epoch: 1, Term: 1, Index: h.index},
	}
	return h.table.Apply(e).(Result)
}

// A push still running when its repository was deleted or renamed on
// another member: from payload version 2, Apply refuses its new branch on
// the dead key; a version 1 entry applies exactly as it always did, so a
// log written before the upgrade replays to the same table.
func TestPushIntoAGoneKeyIsRefusedFromVersion2(t *testing.T) {
	setup := func() *harness {
		h := newHarness(t)
		require.True(t, h.apply(KindRefTxn, push("o/del.git", RefUpdate{Ref: "refs/heads/main", New: sha1A})).OK)
		require.True(t, h.apply(KindRefTxn, push("o/old.git", RefUpdate{Ref: "refs/heads/main", New: sha1A})).OK)
		require.True(t, h.apply(KindRepoDelete, &RepoDelete{Repo: "o/del.git", RepoID: 7}).OK)
		require.True(t, h.apply(KindRepoRename, &RepoRename{From: "o/old.git", To: "o/new.git"}).OK)
		return h
	}
	late := func(repo string, pushed bool) *RefTxn {
		tx := push(repo, RefUpdate{Ref: "refs/heads/late", New: sha1B})
		tx.Push = pushed
		return tx
	}

	h := setup()
	for _, repo := range []string{"o/del.git", "o/old.git"} {
		res := h.applyAt(2, KindRefTxn, late(repo, true))
		assert.False(t, res.OK, repo)
		_, ok := h.table.Get(repo, "refs/heads/late")
		assert.False(t, ok, repo)
	}
	_, deleted := h.table.Deleted("o/del.git")
	assert.True(t, deleted)
	// Forgejo creating a repository again under the freed name is no push.
	assert.True(t, h.applyAt(2, KindRefTxn, late("o/del.git", false)).OK)

	// Version 1: as before, whatever the Push field says.
	h = setup()
	for _, repo := range []string{"o/del.git", "o/old.git"} {
		assert.True(t, h.apply(KindRefTxn, late(repo, true)).OK, repo)
	}
}

// The written version is raised by a version 1 audit record, which an older
// member applies as a plain record; it survives a snapshot, and a snapshot
// cut by an older member, which lacks the field, reads it back from the
// audit it kept.
func TestWriteVersionIsRaisedByAnAuditRecord(t *testing.T) {
	h := newHarness(t)
	assert.Equal(t, uint8(BaseVersion), h.table.WriteVersion())
	require.True(t, h.apply(KindAudit, &Audit{Operator: "op", Action: AuditPayloadVersion, Target: "2", Outcome: "requested"}).OK)
	assert.Equal(t, uint8(BaseVersion), h.table.WriteVersion(), "only a raised outcome counts")
	require.True(t, h.apply(KindAudit, &Audit{Operator: "op", Action: AuditPayloadVersion, Target: "9", Outcome: AuditRaised}).OK)
	assert.Equal(t, uint8(BaseVersion), h.table.WriteVersion(), "a version this code cannot apply is not adopted")
	require.True(t, h.apply(KindAudit, &Audit{Operator: "op", Action: AuditPayloadVersion, Target: "2", Outcome: AuditRaised}).OK)
	assert.Equal(t, uint8(2), h.table.WriteVersion())

	snap, err := h.table.Snapshot()
	require.NoError(t, err)
	var buf bytes.Buffer
	_, err = snap.WriteTo(&buf)
	require.NoError(t, err)
	nt := New()
	require.NoError(t, nt.Restore(bytes.NewReader(buf.Bytes()), consensus.SnapshotMeta{}))
	assert.Equal(t, uint8(2), nt.WriteVersion())

	var fields map[string]json.RawMessage
	require.NoError(t, json.Unmarshal(buf.Bytes(), &fields))
	require.Contains(t, fields, "payload")
	delete(fields, "payload")
	old, err := json.Marshal(fields)
	require.NoError(t, err)
	nt = New()
	require.NoError(t, nt.Restore(bytes.NewReader(old), consensus.SnapshotMeta{}))
	assert.Equal(t, uint8(2), nt.WriteVersion())

	_, err = EncodeVersion(PayloadVersion+1, KindRefTxn, push("o/r.git"))
	require.ErrorIs(t, err, ErrVersion)
}

// A version 2 envelope carries a ref transaction only.
func TestVersion2OfAnotherKindStopsTheNode(t *testing.T) {
	h := newHarness(t)
	data, err := EncodeVersion(BaseVersion, KindAck, &Ack{Identity: "x", Index: 1})
	require.NoError(t, err)
	data[0] = 2
	assert.Panics(t, func() {
		h.table.Apply(consensus.Entry{Term: 1, Index: 1, Kind: consensus.EntryCommand, Data: data})
	})
}

// The nonce of an accepted transaction is kept: a proposer whose outcome
// was unknown finds its entry even once a later write moved the ref.
func TestCommittedNonceOutlivesALaterWrite(t *testing.T) {
	h := newHarness(t)
	tx := push("o/r.git", RefUpdate{Ref: "refs/heads/main", New: sha1A})
	tx.Nonce = "n1"
	require.True(t, h.apply(KindRefTxn, tx).OK)
	tx2 := push("o/r.git", RefUpdate{Ref: "refs/heads/main", Old: sha1A, ExpectRev: 1, New: sha1B})
	tx2.Nonce = "n2"
	require.True(t, h.apply(KindRefTxn, tx2).OK)
	// A copy of the first, refused, does not move its mark.
	assert.False(t, h.apply(KindRefTxn, tx).OK)
	i, ok := h.table.CommittedNonce("n1")
	assert.True(t, ok)
	assert.Equal(t, uint64(1), i)
	_, ok = h.table.CommittedNonce("n3")
	assert.False(t, ok)
}
