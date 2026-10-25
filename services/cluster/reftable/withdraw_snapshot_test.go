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

// restoreFrom is a new table restored from snap, as the engine restores
// one when the node opens again or installs a snapshot.
func restoreFrom(t *testing.T, snap []byte) *Table {
	t.Helper()
	tb := New()
	require.NoError(t, tb.Restore(bytes.NewReader(snap), consensus.SnapshotMeta{}))
	return tb
}

// withoutFields is snap with the named top-level fields removed, as a
// binary that did not write them cut it.
func withoutFields(t *testing.T, snap []byte, names ...string) []byte {
	t.Helper()
	var m map[string]any
	require.NoError(t, json.Unmarshal(snap, &m))
	for _, n := range names {
		delete(m, n)
	}
	out, err := json.Marshal(m)
	require.NoError(t, err)
	return out
}

// A revocation request withdrawn on the log (#171, payload version 7) does
// not come back on a table restored from a snapshot cut after the
// withdrawal (#223). Once the last open request was withdrawn the two maps
// were empty, which the snapshot left out; Restore read their absence as a
// snapshot of a binary older than the maps, and read the pending
// revocations back from the audit records kept, where the withdrawn
// request's record still is. The restored table held the key asked for,
// numbered 0, a request no withdrawal can ever name, where the table that
// replayed the log held nothing. The snapshot now writes both maps, even
// empty, and one an earlier binary cut without them, at the version of the
// withdrawal, restores alike.
func TestAWithdrawnRequestDoesNotComeBackFromASnapshot(t *testing.T) {
	h := newHarness(t)
	h.raise(PayloadVersion)
	k := spkiOf(9)
	n := h.ask("node-b", k)
	require.Equal(t, []uint64{n}, h.table.RevocationRequests(k))
	require.True(t, h.withdraw(k, n).OK, "the refused Remove withdraws its request")
	require.Empty(t, h.table.PendingRevocations())
	require.Empty(t, h.table.RevocationRequests(k))

	snap := snapshotBytes(t, h.table)
	var fields map[string]any
	require.NoError(t, json.Unmarshal(snap, &fields))
	_, hasPending := fields["pending_revoke"]
	_, hasRequests := fields["revoke_requests"]
	t.Logf("snapshot cut after the withdrawal: pending_revoke present=%t, revoke_requests present=%t, audit records=%d",
		hasPending, hasRequests, len(h.table.Audit()))
	assert.True(t, hasPending && hasRequests, "the snapshot tells no request from a binary older than the maps")

	// As this binary cuts it, and as an earlier one did, the empty maps
	// left out.
	for _, c := range []struct {
		name string
		snap []byte
	}{
		{"written", snap},
		{"left out", withoutFields(t, snap, "pending_revoke", "revoke_requests")},
	} {
		restored := restoreFrom(t, c.snap)
		assert.Equal(t, h.table.PendingRevocations(), restored.PendingRevocations(),
			"%s: the same log gives the same pending revocations, replayed or restored from a snapshot", c.name)
		assert.Empty(t, restored.RevocationRequests(k), "%s: a withdrawn request does not come back", c.name)
		assert.Equal(t, string(snap), string(snapshotBytes(t, restored)), "%s: the restored table is the one that replayed the log", c.name)

		r := &harness{t: t, table: restored, index: h.index}
		assert.False(t, r.withdraw(k, n).OK, "%s: withdrawn once, on the restored table as on the other", c.name)
	}
}

// A table restored from a snapshot holds the revocations of the table that
// replayed the log, at every point of a log that asks, withdraws and writes
// revocations: the same pending revocations, the same open requests and
// the same snapshot, whether the cut lands where the maps are empty or not,
// and the rest of the log applies alike on both (#223).
func TestATableRestoredAnywhereHoldsTheRevocationsOfTheLog(t *testing.T) {
	k1, k2, k3 := spkiOf(1), spkiOf(2), spkiOf(3)
	var steps []func(h *harness)
	var asked []uint64
	ask := func(slot, spki string) {
		steps = append(steps, func(h *harness) { asked = append(asked, h.ask(slot, spki)) })
	}
	withdraw := func(spki string, request int) {
		steps = append(steps, func(h *harness) { require.True(t, h.withdraw(spki, asked[request]).OK) })
	}
	steps = append(steps, func(h *harness) { h.raise(PayloadVersion) })
	ask("node-b", k1) // asked[0]
	withdraw(k1, 0)   // every map empty
	ask("node-b", k1) // asked[1]
	ask("node-c", k2) // asked[2]
	ask("node-b", k1) // asked[3]
	withdraw(k1, 1)   // k1 stays asked for by asked[3]
	withdraw(k2, 2)   // k2 no longer asked for
	ask("node-d", k3) // asked[4]
	steps = append(steps, func(h *harness) {
		require.True(t, h.applyAt(RevokeVersion, KindRevokeKey, &RevokeKey{SPKI: k3, Slot: "node-d", Operator: "op", Time: 3}).OK)
	})
	withdraw(k1, 3) // every map empty again

	replay := func(n int) *harness {
		asked = nil
		h := newHarness(t)
		for _, step := range steps[:n] {
			step(h)
		}
		return h
	}
	full := replay(len(steps))
	require.Empty(t, full.table.PendingRevocations())
	want := snapshotBytes(t, full.table)
	for cut := 1; cut <= len(steps); cut++ {
		log := replay(cut)
		restored := &harness{t: t, table: restoreFrom(t, snapshotBytes(t, log.table)), index: log.index}
		assert.Equal(t, log.table.PendingRevocations(), restored.table.PendingRevocations(), "cut after step %d", cut)
		for _, k := range []string{k1, k2, k3} {
			assert.Equal(t, log.table.RevocationRequests(k), restored.table.RevocationRequests(k), "cut after step %d, key %s", cut, k[:4])
		}
		assert.Equal(t, string(snapshotBytes(t, log.table)), string(snapshotBytes(t, restored.table)), "cut after step %d", cut)
		// The rest of the log, on the restored table.
		for _, step := range steps[cut:] {
			step(restored)
		}
		assert.Equal(t, string(want), string(snapshotBytes(t, restored.table)), "restored after step %d, then the rest of the log", cut)
	}
}

// A snapshot of a binary that applied no withdrawal, cut below
// WithdrawVersion, still has its pending revocations read back from the
// audit records kept when it holds none of the maps: no withdrawal could
// leave a record of a request that is not pending, and every request it
// asked for is numbered 0, as on the log (#50, #171). A request whose
// revocation is written stays written.
func TestASnapshotCutBeforeTheWithdrawalsReadsItsRequestsFromTheAudit(t *testing.T) {
	k1, k2 := spkiOf(1), spkiOf(2)
	h := newHarness(t)
	h.raise(WithdrawVersion - 1)
	h.ask("node-b", k1)
	h.ask("node-c", k2)
	require.True(t, h.applyAt(RevokeVersion, KindRevokeKey, &RevokeKey{SPKI: k2, Slot: "node-c", Operator: "op", Time: 3}).OK)
	require.Len(t, h.table.PendingRevocations(), 1)

	for _, strip := range [][]string{
		{"pending_revoke", "revoke_requests"}, // a binary older than both maps
		{"revoke_requests"},                   // a binary older than the numbering
	} {
		restored := restoreFrom(t, withoutFields(t, snapshotBytes(t, h.table), strip...))
		assert.Equal(t, h.table.PendingRevocations(), restored.PendingRevocations(), "without %v", strip)
		assert.Equal(t, []uint64{0}, restored.RevocationRequests(k1), "without %v", strip)
		assert.Empty(t, restored.RevocationRequests(k2), "without %v: revoked", strip)
		assert.True(t, restored.KeyRevoked(k2))
	}
}
