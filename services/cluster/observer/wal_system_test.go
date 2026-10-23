// Copyright 2026 The Forgejo Authors. All rights reserved.
// SPDX-License-Identifier: GPL-3.0-or-later

package observer

import (
	"context"
	"io"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// initdb gives the fake PostgreSQL a new system: segment names and
// timelines start over, and the members' archive is emptied for it.
func (p *fakePG) initdb(sysid string) {
	p.sysid, p.tli, p.insert = sysid, 1, fakeSegSize
	p.records, p.history = nil, nil
	p.archive = map[string][]byte{}
}

// After an initdb, the new system's segments reuse the old one's names:
// they are fetched again under the new system and cover its checkpoints,
// whose replay holds the new system's rows only (#60).
func TestObserverKeepsEachSystemsWALApart(t *testing.T) {
	ctx := context.Background()
	c := newFakeCluster(t)
	o := newTestObserver(t, c, &hookLog{})
	c.pg.write("old row", "old system")
	seed(t, c, o)
	require.NoError(t, o.Step(ctx))
	for i := range 3 {
		c.push("alice/demo.git", "refs/heads/main", c.commit(string(rune('a'+i))))
		c.checkpoint()
		require.NoError(t, o.Step(ctx))
	}

	c.pg.initdb("7425000000000000099")
	c.pg.write("new row", "new system")
	c.push("alice/demo.git", "refs/heads/main", c.commit("after initdb"))
	c.checkpoint()
	require.ErrorIs(t, o.Step(ctx), ErrNoCoverage)
	c.push("alice/demo.git", "refs/heads/main", c.commit("after initdb 2"))
	cp := c.checkpoint()
	require.NoError(t, o.Step(ctx))
	st := o.State()
	last := st.Checkpoints[len(st.Checkpoints)-1]
	require.Equal(t, cp.Name(), last.Name)
	m, err := o.store.ReadManifest(last.Name)
	require.NoError(t, err)
	assert.Equal(t, "7425000000000000099", m.PG.Base.SystemID)
	for _, seg := range m.PG.Segments {
		r, err := o.store.OpenWAL(m.Checkpoint.SystemID, seg)
		require.NoError(t, err)
		head := make([]byte, walHeaderSize)
		_, err = io.ReadFull(r, head)
		r.Close()
		require.NoError(t, err)
		assert.Equal(t, "7425000000000000099", headerSystem(head), "segment %s is the new system's", seg)
	}
	rows := replay(t, o, m)
	assert.Equal(t, "new system", rows["new row"])
	assert.NotContains(t, rows, "old row")
}

// A segment whose header names another system, another start or another
// size is never kept under its name (#60).
func TestCheckSegmentHeader(t *testing.T) {
	p := newFakePG()
	name := SegmentName(1, 3*fakeSegSize, fakeSegSize)
	head := p.segmentHeader(1, 3*fakeSegSize)
	require.NoError(t, checkSegmentHeader(head, name, p.sysid, fakeSegSize))
	require.ErrorContains(t, checkSegmentHeader(head, name, "7425000000000000099", fakeSegSize), "belongs to system")
	require.ErrorContains(t, checkSegmentHeader(p.segmentHeader(1, 4*fakeSegSize), name, p.sysid, fakeSegSize), "starts at")
	require.ErrorContains(t, checkSegmentHeader(p.segmentHeader(2, 3*fakeSegSize), name, p.sysid, fakeSegSize), "timeline")
	require.ErrorContains(t, checkSegmentHeader(head, name, p.sysid, 2*fakeSegSize), "")
	require.ErrorContains(t, checkSegmentHeader([]byte(`{"lsn":1}`), name, p.sysid, fakeSegSize), "shorter")
	// A promotion's copy of the switch segment keeps the old timeline.
	require.NoError(t, checkSegmentHeader(head, SegmentName(2, 3*fakeSegSize, fakeSegSize), p.sysid, fakeSegSize))
	// Histories and backup history files carry no header.
	require.NoError(t, checkSegmentHeader(nil, "00000002.history", p.sysid, fakeSegSize))
}
