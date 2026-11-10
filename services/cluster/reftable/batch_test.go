// Copyright 2026 The Forgejo Authors. All rights reserved.
// SPDX-License-Identifier: GPL-3.0-or-later

package reftable

import (
	"encoding/json" //nolint:depguard // the state machine's own codec
	"fmt"
	"strings"
	"testing"

	"forgejo.org/services/cluster/consensus"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// UpdateBytes bounds the encoding of every update the hook may build of a
// ref, whatever its name holds that encoding/json escapes (#385).
func TestUpdateBytesBoundsAnUpdate(t *testing.T) {
	sha1, sha256 := strings.Repeat("a", 40), strings.Repeat("b", 64)
	names := []string{
		"refs/heads/main",
		"refs/heads/" + strings.Repeat("&", 200) + "/" + strings.Repeat("<>", 100),
		`refs/heads/"quoted"`,
		"refs/heads/line separator ",
		"refs/heads/café/\U0001F600",
		"refs/heads/" + strings.Repeat(strings.Repeat("&", 200)+"/", 5) + strings.Repeat("&", 8),
	}
	for _, name := range names {
		require.NoError(t, ValidRefName(name), name)
		bound := UpdateBytes(name, "")
		for _, u := range []RefUpdate{
			{Ref: name, New: sha1},
			{Ref: name, New: sha256, ExpectRev: 1<<64 - 1},
			{Ref: name, Old: sha256, New: sha256, ExpectRev: 1<<64 - 1},
			{Ref: name, Old: sha256, ExpectRev: 1<<64 - 1, Delete: true},
		} {
			b, err := json.Marshal(u)
			require.NoError(t, err)
			assert.LessOrEqual(t, len(b)+1, bound, "%+v", u)
		}
		b, err := json.Marshal(RefUpdate{Ref: "HEAD", Old: sha256, ExpectRev: 1<<64 - 1, Symbolic: name})
		require.NoError(t, err)
		assert.LessOrEqual(t, len(b)+1, UpdateBytes("HEAD", name))
		assert.Less(t, bound, BatchBytes/100, "one update is far within a batch")
	}
}

// Updates whose weights add up to BatchBytes at most make an entry within
// consensus.MaxEntryBytes, every other field of the transaction at its
// largest for a batch.
func TestBatchBytesLeavesRoomForTheEntry(t *testing.T) {
	tx := &RefTxn{
		Repo: strings.Repeat("o", 40) + "/" + strings.Repeat("r", 100) + ".wiki.git", RepoID: 1<<63 - 1, Format: "sha256",
		PusherID: 1<<63 - 1, Time: 1<<63 - 1, Nonce: strings.Repeat("a", 32) + "." + strings.Repeat("b", 64),
		Trigger: "cluster-import", PRID: 1<<63 - 1, IsWiki: true, Push: true, PusherName: strings.Repeat("p", MaxPusherName),
		ReadAt: 1<<64 - 1, Init: true,
	}
	weight := 0
	for i := 0; ; i++ {
		name := fmt.Sprintf("refs/heads/%s/%s/b%05d", strings.Repeat("&", 200), strings.Repeat(">", 200), i)
		w := UpdateBytes(name, "")
		if weight+w > BatchBytes {
			break
		}
		weight += w
		tx.Updates = append(tx.Updates, RefUpdate{Ref: name, Old: strings.Repeat("c", 64), ExpectRev: 1<<64 - 1, New: strings.Repeat("d", 64)})
	}
	data, err := EncodeVersion(PayloadVersion, KindRefTxn, tx)
	require.NoError(t, err)
	t.Logf("%d updates weighing %d bytes: an entry of %d bytes, the bound is %d", len(tx.Updates), weight, len(data), consensus.MaxEntryBytes)
	assert.LessOrEqual(t, len(data), consensus.MaxEntryBytes)
}

func TestBatchLen(t *testing.T) {
	const w = BatchBytes / 10
	each := func(int) int { return w }
	assert.Zero(t, BatchLen(0, 512, each))
	assert.Equal(t, 7, BatchLen(7, 512, each), "within both bounds")
	assert.Equal(t, 3, BatchLen(7, 3, each), "the count")
	assert.Equal(t, 10, BatchLen(50, 512, each), "the weight")
	assert.Equal(t, 1, BatchLen(5, 512, func(int) int { return BatchBytes + 1 }), "always one")
	assert.Equal(t, 2, BatchLen(5, 512, func(i int) int { return []int{1, BatchBytes - 1, 1, 1, 1}[i] }), "the weight, exactly")
}
