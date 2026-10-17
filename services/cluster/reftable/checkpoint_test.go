// Copyright 2026 The Forgejo Authors. All rights reserved.
// SPDX-License-Identifier: GPL-3.0-or-later

package reftable

import (
	"bytes"
	"strings"
	"testing"

	"forgejo.org/services/cluster/consensus"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func position(nonce string, index uint64) *CheckpointPosition {
	return &CheckpointPosition{
		Nonce: nonce, Index: index, LSN: "0/3000060", Timeline: 1, SystemID: "7400000000000000001",
		RestorePoint: "cp", WALSegmentSize: 16 << 20, RefsSHA256: strings.Repeat("ab", 32), FilesSHA256: strings.Repeat("cd", 32),
	}
}

// A checkpoint closes only the barrier it names, in the term that opened
// it; a newer barrier supersedes an open one, and the table at the barrier
// is captured exactly once, under the lock, at its index.
func TestCheckpointBarrierThenPosition(t *testing.T) {
	h := newHarness(t)
	require.True(t, h.apply(KindRefTxn, push("alice/demo.git", RefUpdate{Ref: "refs/heads/main", New: sha1A})).OK)
	nonce := strings.Repeat("1", 32)
	var captured []byte
	var capturedAt uint64
	h.table.OnCheckpoint(func(n string, index uint64, encode func() ([]byte, error)) {
		if n == nonce {
			captured, _ = encode()
			capturedAt = index
		}
	})
	require.True(t, h.apply(KindCheckpointBarrier, &CheckpointBarrier{Nonce: nonce, Time: 1}).OK)
	assert.EqualValues(t, 2, capturedAt)
	root := h.table.Root()

	// A push after the barrier is not in the capture.
	require.True(t, h.apply(KindRefTxn, push("alice/demo.git", RefUpdate{Ref: "refs/heads/main", Old: sha1A, ExpectRev: 1, New: sha1B})).OK)
	at := New()
	require.NoError(t, at.Restore(bytes.NewReader(captured), consensus.SnapshotMeta{}))
	assert.Equal(t, root, at.Root())
	v, _ := at.Get("alice/demo.git", "refs/heads/main")
	assert.Equal(t, sha1A, v.SHA)

	// Malformed or mismatched positions are refused, and leave it open.
	bad := position(nonce, 2)
	bad.LSN = "0/zz"
	assert.False(t, h.apply(KindCheckpointPosition, bad).OK)
	assert.False(t, h.apply(KindCheckpointPosition, position(nonce, 1)).OK)
	assert.False(t, h.apply(KindCheckpointPosition, position(strings.Repeat("2", 32), 2)).OK)
	require.True(t, h.apply(KindCheckpointPosition, position(nonce, 2)).OK)
	cps := h.table.Checkpoints()
	require.Len(t, cps, 1)
	assert.Equal(t, root.String(), cps[0].Root)
	assert.EqualValues(t, 2, cps[0].Index)
	assert.Equal(t, "01000000000000000000000000000000-1-00000000000000000002", cps[0].Name())
	assert.False(t, h.apply(KindCheckpointPosition, position(nonce, 2)).OK, "closed once")

	// A newer barrier supersedes an open one.
	n1, n2 := strings.Repeat("3", 32), strings.Repeat("4", 32)
	require.True(t, h.apply(KindCheckpointBarrier, &CheckpointBarrier{Nonce: n1}).OK)
	require.True(t, h.apply(KindCheckpointBarrier, &CheckpointBarrier{Nonce: n2}).OK)
	assert.False(t, h.apply(KindCheckpointPosition, position(n1, h.index-1)).OK)

	// The checkpoints survive a snapshot.
	snap, err := h.table.Snapshot()
	require.NoError(t, err)
	var buf bytes.Buffer
	_, err = snap.WriteTo(&buf)
	require.NoError(t, err)
	back := New()
	require.NoError(t, back.Restore(&buf, consensus.SnapshotMeta{}))
	assert.Equal(t, cps, back.Checkpoints())
}

// The leader that opened a barrier must close it in the same term: after
// a leader change, another dispatcher may have written SQL past it.
func TestCheckpointRefusedAcrossTerms(t *testing.T) {
	tb := New()
	nonce := strings.Repeat("5", 32)
	enc := func(kind Kind, body any) []byte {
		b, err := Encode(kind, body)
		require.NoError(t, err)
		return b
	}
	tok := func(term, index uint64) consensus.Token {
		return consensus.Token{Cluster: consensus.ClusterID{1}, Epoch: 1, Term: term, Index: index}
	}
	require.True(t, tb.Apply(consensus.Entry{Term: 3, Index: 1, Data: enc(KindCheckpointBarrier, &CheckpointBarrier{Nonce: nonce}), Token: tok(3, 1)}).(Result).OK)
	res := tb.Apply(consensus.Entry{Term: 4, Index: 2, Data: enc(KindCheckpointPosition, position(nonce, 1)), Token: tok(4, 2)}).(Result)
	assert.False(t, res.OK)
	assert.Contains(t, res.Reason, "term")
	assert.Empty(t, tb.Checkpoints())
}
