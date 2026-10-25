// Copyright 2026 The Forgejo Authors. All rights reserved.
// SPDX-License-Identifier: GPL-3.0-or-later

package kitsunium

import (
	"bytes"
	"context"
	"encoding/binary"
	"hash/crc32"
	"os"
	"path/filepath"
	"testing"

	"forgejo.org/services/cluster/consensus"

	sdk "github.com/kitsunium/sdk/pkg/v1/app/consensus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// newerMeta is a snapshot metadata of a version this build does not know,
// ending, as every version from 2 does, with the CRC-32C of every byte
// before it: what a newer build wrote, not a .meta that rotted.
func newerMeta() []byte {
	body := append([]byte{4}, "a layout of a later protocol"...)
	return binary.LittleEndian.AppendUint32(body, crc32.Checksum(body, crc32.MakeTable(crc32.Castagnoli)))
}

// A data directory that holds a snapshot metadata a newer build wrote is
// refused VERSION_UNSUPPORTED by everything that opens it, the node's start
// and the offline commands alike, with the directory and the remedy, and
// nothing in it is removed (kitsunium/sdk#665). Before, the open skipped
// such a .meta as one that rotted: a node rolled back past a raise of the
// cluster version started on an older snapshot, or on none, instead of
// refusing to start.
func TestADataDirectoryANewerBuildWroteIsRefused(t *testing.T) {
	dir := t.TempDir()
	id, inc, err := driver{}.Init(dir, consensus.ClusterID{0x66, 0x05}, "a")
	require.NoError(t, err)
	self := consensus.Member{ID: id, Incarnation: inc, Slot: "a", SPKI: [32]byte{1}, Address: "127.0.0.1:7001", Voter: true}
	require.NoError(t, driver{}.Bootstrap(context.Background(), consensus.Config{DataDir: dir, MinVoters: 1}, []consensus.Member{self}))
	meta := filepath.Join(dir, "snap", "00000000000000000009-00000000000000000002.meta")
	require.NoError(t, os.WriteFile(meta, newerMeta(), 0o600))

	refused := func(err error, what string) {
		t.Helper()
		require.ErrorIs(t, err, consensus.ErrVersionUnsupported, what)
		assert.ErrorContains(t, err, "the data directory "+dir+" holds what a newer release wrote", what)
	}
	_, err = driver{}.Inspect(dir)
	refused(err, "cluster inspect")
	newCluster := consensus.ClusterID{0x66, 0x06}
	refused(driver{}.Recover(dir, newCluster, []consensus.Member{self}, consensus.RecoverOptions{Confirm: newCluster.String(), Operator: "op:test"}), "cluster recover")
	expected := sdk.SnapshotMetaValue{Version: 1}
	_, err = driver{}.Restore(dir, bytes.NewReader(nil), newCluster, []consensus.Member{self},
		consensus.RestoreOptions{Meta: sdk.AppendSnapshotMeta(nil, &expected), Confirm: newCluster.String(), Operator: "op:test", MaxBytes: 1 << 20})
	refused(err, "cluster restore")
	_, err = driver{}.Open(context.Background(), consensus.Config{DataDir: dir, Slot: "a", ListenAddress: "127.0.0.1:0"})
	refused(err, "the node's start")

	b, err := os.ReadFile(meta)
	require.NoError(t, err, "the newer metadata is kept")
	assert.Equal(t, newerMeta(), b)
	// Without it, the directory opens on what it held: nothing else went.
	require.NoError(t, os.Remove(meta))
	rep, err := driver{}.Inspect(dir)
	require.NoError(t, err)
	assert.Equal(t, uint64(1), rep.LastIndex, "the founding entry")
}
