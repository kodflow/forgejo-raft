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

// The pusher's name is applied from PusherVersion only. Below it Apply
// drops the field, as an older binary does: a log written before the
// upgrade, or by a proposer that set it too early, replays to the same
// table on every binary.
func TestPusherNameOnlyFromVersion4(t *testing.T) {
	named := func(repo string) *RefTxn {
		tx := push(repo, RefUpdate{Ref: "refs/heads/main", New: sha1A})
		tx.PusherName = "alice"
		return tx
	}
	h := newHarness(t)
	require.True(t, h.applyAt(RevokeVersion, KindRefTxn, named("o/v3.git")).OK)
	require.True(t, h.applyAt(PusherVersion, KindRefTxn, named("o/v4.git")).OK)
	evs := h.table.Events()
	require.Len(t, evs, 2)
	assert.Empty(t, evs[0].PusherName, "dropped below version 4")
	assert.Equal(t, "alice", evs[1].PusherName)
	size := evs[0].size()
	assert.Equal(t, size, evs[1].size(), "the queue's size does not count the name")

	long := named("o/long.git")
	long.PusherName = strings.Repeat("a", MaxPusherName+1)
	assert.False(t, h.applyAt(PusherVersion, KindRefTxn, long).OK)
	long.PusherName = strings.Repeat("a", MaxPusherName+1)
	assert.True(t, h.applyAt(RevokeVersion, KindRefTxn, long).OK, "below version 4 the field is not read")

	// Replay: the same entries on a fresh table give the same snapshot,
	// and a snapshot restores with the names.
	replay := newHarness(t)
	require.True(t, replay.applyAt(RevokeVersion, KindRefTxn, named("o/v3.git")).OK)
	require.True(t, replay.applyAt(PusherVersion, KindRefTxn, named("o/v4.git")).OK)
	long.PusherName = strings.Repeat("a", MaxPusherName+1)
	require.False(t, replay.applyAt(PusherVersion, KindRefTxn, long).OK)
	require.True(t, replay.applyAt(RevokeVersion, KindRefTxn, long).OK)
	snap := func(tb *Table) []byte {
		s, err := tb.Snapshot()
		require.NoError(t, err)
		var buf bytes.Buffer
		_, err = s.WriteTo(&buf)
		require.NoError(t, err)
		return buf.Bytes()
	}
	a, b := snap(h.table), snap(replay.table)
	assert.Equal(t, string(a), string(b))
	nt := New()
	require.NoError(t, nt.Restore(bytes.NewReader(a), consensus.SnapshotMeta{}))
	assert.Equal(t, h.table.Events(), nt.Events())
	assert.Equal(t, h.table.EventBytes(), nt.EventBytes())
}

// A binary that applies version 3 stops on a version 4 envelope (D18):
// the name is never written before every member applies it.
func TestVersion4StopsAnOlderBinary(t *testing.T) {
	data, err := EncodeVersion(PusherVersion, KindRefTxn, push("o/r.git", RefUpdate{Ref: "refs/heads/main", New: sha1A}))
	require.NoError(t, err)
	assert.Equal(t, uint8(4), data[0])
	data[0] = PayloadVersion + 1
	assert.Panics(t, func() {
		New().Apply(consensus.Entry{Term: 1, Index: 1, Kind: consensus.EntryCommand, Data: data})
	})
	// Version 4 changes no other kind.
	ack, err := EncodeVersion(BaseVersion, KindAck, &Ack{Identity: "x", Index: 1})
	require.NoError(t, err)
	ack[0] = PusherVersion
	assert.Panics(t, func() {
		New().Apply(consensus.Entry{Term: 1, Index: 1, Kind: consensus.EntryCommand, Data: ack})
	})
}
