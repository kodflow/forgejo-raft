// Copyright 2026 The Forgejo Authors. All rights reserved.
// SPDX-License-Identifier: GPL-3.0-or-later

package observer

import (
	"compress/gzip"
	"context"
	"os"
	"testing"

	"forgejo.org/modules/clusterhook"
	"forgejo.org/modules/json"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// A flat archive written before #60 is read for its own system only, its
// histories included: another system never takes them.
func TestLegacyFlatArchiveBelongsToItsSystem(t *testing.T) {
	c := newFakeCluster(t)
	o := newTestObserver(t, c, &hookLog{})
	name := SegmentName(1, 2*fakeSegSize, fakeSegSize)
	f, err := os.Create(o.store.legacyWALPath(name))
	require.NoError(t, err)
	zw := gzip.NewWriter(f)
	_, err = zw.Write(c.pg.segmentHeader(1, 2*fakeSegSize))
	require.NoError(t, err)
	require.NoError(t, zw.Close())
	require.NoError(t, f.Close())
	require.NoError(t, os.WriteFile(o.store.legacyWALPath("00000002.history"), []byte("1\t0/3000000\tno recovery target specified\n"), 0o600))

	assert.True(t, o.store.haveWAL(c.pg.sysid, name))
	assert.True(t, o.store.haveWAL(c.pg.sysid, "00000002.history"))
	assert.Equal(t, name, o.store.lastSegment(c.pg.sysid))
	const other = "7425000000000000099"
	assert.False(t, o.store.haveWAL(other, name))
	assert.False(t, o.store.haveWAL(other, "00000002.history"))
	assert.Empty(t, o.store.lastSegment(other))
	h, err := o.store.histories(other)
	require.NoError(t, err)
	assert.Empty(t, h)
}

// A member that lists an invalid WAL name is passed over: the others'
// listings still bring the archive up to date.
func TestObserverPassesOverAMemberListingAnInvalidName(t *testing.T) {
	c := newFakeCluster(t)
	o := newTestObserver(t, c, &hookLog{})
	c.tamper = func(slot string, req *clusterhook.ExportRequest, data []byte) []byte {
		if req.Op == clusterhook.OpExportWALList && slot == "node-a" {
			b, _ := json.Marshal([]clusterhook.WALFile{{Name: "../../etc/passwd", Size: 1}})
			return b
		}
		return data
	}
	seed(t, c, o)
	require.NoError(t, o.Step(context.Background()))
	assert.Len(t, o.State().Checkpoints, 1)
}
