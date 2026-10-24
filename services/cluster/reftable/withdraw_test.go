// Copyright 2026 The Forgejo Authors. All rights reserved.
// SPDX-License-Identifier: GPL-3.0-or-later

package reftable

import (
	"bytes"
	"fmt"
	"strconv"
	"strings"
	"testing"

	"forgejo.org/modules/json"
	"forgejo.org/services/cluster/consensus"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func spkiOf(b byte) string { return strings.Repeat(fmt.Sprintf("%02x", b), 32) }

// ask applies an operator's revocation request of spki, a version 1
// audit record, and returns its index.
func (h *harness) ask(slot, spki string) uint64 {
	h.t.Helper()
	require.True(h.t, h.apply(KindAudit, &Audit{
		ID: fmt.Sprintf("ask-%d", h.index+1), Operator: "op", Action: AuditRevokeKey, Target: slot + ":" + spki, Outcome: AuditPending, Time: 1,
	}).OK)
	return h.index
}

// raise applies the audit record that raises the written version to v.
func (h *harness) raise(v int) {
	h.t.Helper()
	require.True(h.t, h.apply(KindAudit, &Audit{Operator: "op", Action: AuditPayloadVersion, Target: strconv.Itoa(v), Outcome: AuditRaised}).OK)
}

func (h *harness) withdraw(spki string, index uint64) Result {
	h.t.Helper()
	return h.applyAt(WithdrawVersion, KindWithdrawRevocation, &WithdrawRevocation{SPKI: spki, Index: index, Operator: "op", Time: 2})
}

// A revocation request is numbered by its commit from WithdrawVersion, 0
// before; a withdrawal removes the request it names and that one alone,
// the key staying asked for while another request of it is open, and
// touches neither a request numbered otherwise nor a key revoked (#171).
func TestAWithdrawalRemovesTheRequestItNames(t *testing.T) {
	h := newHarness(t)
	k1, k2, k3 := spkiOf(1), spkiOf(2), spkiOf(3)
	h.ask("node-b", k1)
	assert.Equal(t, []uint64{0}, h.table.RevocationRequests(k1), "asked before the raise: numbered by none")
	h.raise(PayloadVersion)
	second := h.ask("node-b", k1)
	assert.Equal(t, []uint64{0, second}, h.table.RevocationRequests(k1))
	other := h.ask("node-c", k2)
	assert.Equal(t, []uint64{other}, h.table.RevocationRequests(k2))

	assert.False(t, h.withdraw(k1, 0).OK, "a request numbered by none is not withdrawn")
	assert.False(t, h.withdraw(k1, other).OK, "another key's request")
	assert.False(t, h.withdraw(k2, second).OK, "another key's request")
	require.True(t, h.withdraw(k2, other).OK)
	assert.Empty(t, h.table.RevocationRequests(k2))
	require.True(t, h.withdraw(k1, second).OK)
	assert.Equal(t, []uint64{0}, h.table.RevocationRequests(k1), "the request of before the raise stays")
	assert.False(t, h.withdraw(k1, second).OK, "withdrawn once")
	pending := map[string]bool{}
	for _, k := range h.table.PendingRevocations() {
		pending[k.SPKI] = true
	}
	assert.Equal(t, map[string]bool{k1: true}, pending)

	// Two requests of one key: each withdrawal removes its own.
	a, b := h.ask("node-d", k3), h.ask("node-d", k3)
	require.True(t, h.withdraw(k3, a).OK)
	assert.Equal(t, []uint64{b}, h.table.RevocationRequests(k3), "the key stays asked for")
	require.True(t, h.withdraw(k3, b).OK)
	assert.Empty(t, h.table.RevocationRequests(k3))

	// A revocation written closes every request of its key.
	c := h.ask("node-d", k3)
	require.True(t, h.applyAt(RevokeVersion, KindRevokeKey, &RevokeKey{SPKI: k3, Slot: "node-d", Operator: "op", Time: 3}).OK)
	assert.False(t, h.withdraw(k3, c).OK)
	assert.True(t, h.table.KeyRevoked(k3))

	// Below WithdrawVersion a withdrawal is not written, and refused.
	_, err := EncodeVersion(WithdrawVersion-1, KindWithdrawRevocation, &WithdrawRevocation{SPKI: k1, Index: 1, Operator: "op"})
	require.ErrorIs(t, err, ErrVersion)
	body, err := json.Marshal(&WithdrawRevocation{SPKI: k2, Index: other, Operator: "op"})
	require.NoError(t, err)
	h.ask("node-c", k2)
	h.index++
	res := h.table.Apply(consensus.Entry{
		Term: 1, Index: h.index, Kind: consensus.EntryCommand, Data: append([]byte{WithdrawVersion - 1, byte(KindWithdrawRevocation)}, body...),
		Token: consensus.Token{Cluster: consensus.ClusterID{1}, Epoch: 1, Term: 1, Index: h.index},
	}).(Result)
	assert.False(t, res.OK)
	assert.Len(t, h.table.RevocationRequests(k2), 1)
}

// A binary applying version 6 only stops on a withdrawal, never skips it
// (D18): the cluster writes one only once every member applies it.
func TestAnOlderBinaryStopsOnAWithdrawal(t *testing.T) {
	h := newHarness(t)
	h.table.limit = WithdrawVersion - 1
	h.raise(WithdrawVersion - 1)
	n := h.ask("node-b", spkiOf(1))
	assert.Panics(t, func() { h.withdraw(spkiOf(1), n) })
}

// The numbering of the requests is the same on a table that replays the
// log and on one restored from a snapshot: one an older binary cut, which
// numbered nothing, gives every request it holds the 0 the log gives a
// request asked before the raise, and one cut after the raise keeps the
// numbers. The same withdrawals then apply alike on both (#171).
func TestRevocationRequestsAreNumberedAlikeAfterASnapshot(t *testing.T) {
	restore := func(t *testing.T, from *Table, strip bool) *Table {
		t.Helper()
		snap, err := from.Snapshot()
		require.NoError(t, err)
		var buf bytes.Buffer
		_, err = snap.WriteTo(&buf)
		require.NoError(t, err)
		data := buf.Bytes()
		if strip {
			var m map[string]any
			require.NoError(t, json.Unmarshal(data, &m))
			delete(m, "revoke_requests")
			data, err = json.Marshal(m)
			require.NoError(t, err)
		}
		tb := New()
		require.NoError(t, tb.Restore(bytes.NewReader(data), consensus.SnapshotMeta{}))
		return tb
	}
	k1, k2 := spkiOf(1), spkiOf(2)

	// Cut by an older binary before the raise.
	log := newHarness(t)
	log.ask("node-b", k1)
	log.ask("node-b", k1)
	restored := &harness{t: t, table: restore(t, log.table, true), index: log.index}
	assert.Equal(t, log.table.RevocationRequests(k1), restored.table.RevocationRequests(k1))
	assert.Equal(t, []uint64{0}, restored.table.RevocationRequests(k1))
	for _, h := range []*harness{log, restored} {
		h.raise(PayloadVersion)
		h.ask("node-c", k2)
		assert.False(t, h.withdraw(k1, 1).OK, "asked before the raise")
	}
	assert.Equal(t, log.table.RevocationRequests(k2), restored.table.RevocationRequests(k2))

	// Cut after the raise: the numbers travel.
	again := &harness{t: t, table: restore(t, log.table, false), index: log.index}
	n := log.table.RevocationRequests(k2)
	require.Len(t, n, 1)
	assert.Equal(t, n, again.table.RevocationRequests(k2))
	for _, h := range []*harness{log, restored, again} {
		require.True(t, h.withdraw(k2, n[0]).OK)
		assert.Empty(t, h.table.RevocationRequests(k2))
		assert.Equal(t, []uint64{0}, h.table.RevocationRequests(k1))
		assert.Len(t, h.table.PendingRevocations(), 1)
	}
}
