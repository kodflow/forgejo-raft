// Copyright 2026 The Forgejo Authors. All rights reserved.
// SPDX-License-Identifier: GPL-3.0-or-later

package observer

import (
	"context"
	"fmt"
	"os"
	"slices"
	"strings"
	"testing"
	"time"

	"forgejo.org/modules/clusterhook"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// aDay is a day on the test's clock, and a minute: past a BaseEvery of a
// day since the base taken the day before, whose age is that of its
// directory's files, written just after the clock was read.
const aDay = 24*time.Hour + time.Minute

// requireRestorable checks that every published checkpoint restores from
// the observer directory: its base and its WAL are held, and its SQL
// replays to the branch rows of its refs.
func requireRestorable(t *testing.T, o *Observer) {
	t.Helper()
	a, err := OpenArchive(o.cfg.Dir, "")
	require.NoError(t, err)
	for _, name := range o.store.Published() {
		require.NoError(t, a.Check(context.Background(), name), name)
		m, err := o.store.ReadManifest(name)
		require.NoError(t, err)
		rows := replay(t, o, m)
		for ref, sha := range m.Repos["alice/demo.git"].Refs {
			assert.Equal(t, sha, rows["branch alice/demo.git "+ref], "%s: %s", name, ref)
		}
	}
}

// heldWAL counts the WAL files the observer holds for the cluster's system.
func heldWAL(t *testing.T, o *Observer, c *fakeCluster) int {
	t.Helper()
	entries, err := os.ReadDir(o.store.walDir(c.pg.sysid))
	require.NoError(t, err)
	return len(entries)
}

// The observer takes a new base backup once the newest is older than
// BaseEvery, though the bases it holds still cover the checkpoints: with an
// archive that has no gap, the first base would otherwise stay the base of
// every checkpoint, the prune would keep every segment since it, and each
// checkpoint's coverage, which RestorePG copies decompressed into the
// restore directory and PostgreSQL replays, would grow by the segments
// switched since, one per checkpoint at least (#179). Renewed, the bases,
// the WAL held and a restore's segments stay bounded, day after day, and
// every checkpoint the live directory holds still restores.
func TestObserverRenewsItsBaseEveryBaseEvery(t *testing.T) {
	ctx := context.Background()
	c := newFakeCluster(t)
	o := newTestObserver(t, c, &hookLog{})
	now := time.Now()
	o.cfg.Now = func() time.Time { return now }
	o.cfg.LiveKeep = time.Hour
	o.cfg.BaseEvery = 24 * time.Hour
	seed(t, c, o)
	require.NoError(t, o.Step(ctx))
	first := o.State().Bases[0].Label

	const perDay = 5
	var m *Manifest
	for day := 1; day <= 10; day++ {
		now = now.Add(aDay)
		for i := range perDay {
			c.push("alice/demo.git", "refs/heads/main", c.commit(fmt.Sprintf("day %d, %d", day, i)))
			c.checkpoint()
		}
		require.NoError(t, o.Step(ctx))
		st := o.State()
		newest, ok := st.newest()
		require.True(t, ok)
		require.Equal(t, StateProtected, newest.State)
		var err error
		m, err = o.store.ReadManifest(newest.Name)
		require.NoError(t, err)
		held := heldWAL(t, o, c)
		t.Logf("day %2d: %d base(s), checkpoint %s restores from base %s: %d segments to copy and replay; %d WAL files held",
			day, len(st.Bases), newest.Name[len(newest.Name)-6:], m.PG.Base.Label, len(m.PG.Segments), held)
		requireRestorable(t, o)
		if day < 2 {
			continue
		}
		assert.NotEqual(t, first, m.PG.Base.Label, "day %d: the newest checkpoint restores from a base renewed since", day)
		assert.LessOrEqual(t, len(st.Bases), 2, "day %d: the base the live checkpoint restores from, and the renewed one", day)
		assert.LessOrEqual(t, len(m.PG.Segments), perDay+2, "day %d: a restore replays one day of WAL", day)
		assert.LessOrEqual(t, held, perDay+2, "day %d: the WAL since the base the live checkpoint restores from", day)
		entries, err := os.ReadDir(o.store.path("pg", "base"))
		require.NoError(t, err)
		assert.Len(t, entries, len(st.Bases), "day %d: no base directory outlives its record", day)
	}
}

// A base is let go only once no live checkpoint restores from it, and the
// WAL from its start with it: a protected checkpoint that still restores
// from the older base keeps it, whatever base was renewed since.
func TestObserverKeepsTheBaseALiveCheckpointRestoresFrom(t *testing.T) {
	ctx := context.Background()
	c := newFakeCluster(t)
	o := newTestObserver(t, c, &hookLog{})
	now := time.Now()
	o.cfg.Now = func() time.Time { return now }
	o.cfg.LiveKeep = 36 * time.Hour
	o.cfg.BaseEvery = 24 * time.Hour
	seed(t, c, o)
	require.NoError(t, o.Step(ctx))
	b0 := o.State().Bases[0]
	require.Len(t, o.State().Bases, 1, "a base younger than BaseEvery is kept as the only one")

	day := func(n int) Manifest {
		t.Helper()
		now = now.Add(aDay)
		c.push("alice/demo.git", "refs/heads/main", c.commit(fmt.Sprintf("day %d", n)))
		c.checkpoint()
		require.NoError(t, o.Step(ctx))
		st := o.State()
		newest, ok := st.newest()
		require.True(t, ok)
		require.Equal(t, StateProtected, newest.State)
		m, err := o.store.ReadManifest(newest.Name)
		require.NoError(t, err)
		requireRestorable(t, o)
		return *m
	}

	// Day 1: the checkpoint restores from b0, then a base is renewed.
	m1 := day(1)
	assert.Equal(t, b0.Label, m1.PG.Base.Label)
	require.Len(t, o.State().Bases, 2)
	b1 := o.State().Bases[1]

	// Day 2: the checkpoint restores from b1; the one of day 1, still
	// live, restores from b0, which stays with the WAL from its start.
	m2 := day(2)
	assert.Equal(t, b1.Label, m2.PG.Base.Label)
	assert.Contains(t, o.store.Published(), m1.Checkpoint.Name)
	assert.True(t, slices.ContainsFunc(o.State().Bases, func(b BaseInfo) bool { return b.Label == b0.Label }))
	for _, seg := range m1.PG.Segments {
		assert.True(t, o.store.haveWAL(c.pg.sysid, seg), seg)
	}

	// Day 3: the checkpoint of day 1 is let go, and b0 and the WAL before
	// b1 with it; the checkpoints of days 2 and 3 still restore.
	m3 := day(3)
	assert.NotContains(t, o.store.Published(), m1.Checkpoint.Name)
	assert.Contains(t, o.store.Published(), m2.Checkpoint.Name)
	assert.Contains(t, o.store.Published(), m3.Checkpoint.Name)
	assert.False(t, slices.ContainsFunc(o.State().Bases, func(b BaseInfo) bool { return b.Label == b0.Label }))
	_, err := os.Stat(o.store.path("pg", "base", b0.Label))
	assert.True(t, os.IsNotExist(err), "b0 restores no live checkpoint")
	start, err := ParseLSN(b1.Start)
	require.NoError(t, err)
	entries, err := os.ReadDir(o.store.walDir(c.pg.sysid))
	require.NoError(t, err)
	for _, e := range entries {
		name := strings.TrimSuffix(e.Name(), ".gz")
		_, segno, ok := parseSegment(name, fakeSegSize)
		require.True(t, ok, name)
		assert.GreaterOrEqual(t, segno, uint64(start)/fakeSegSize, "%s is before b1, which the oldest live checkpoint restores from", name)
	}
}

// A base renewal that fails never fails the checkpoint of its pass: the
// checkpoint is published and protected from the base held, the failed
// base leaves nothing behind, and a pass renewRetry later takes the base
// again, not the passes in between.
func TestObserverKeepsItsCoverageWhenTheBaseRenewalFails(t *testing.T) {
	ctx := context.Background()
	c := newFakeCluster(t)
	o := newTestObserver(t, c, &hookLog{})
	now := time.Now()
	o.cfg.Now = func() time.Time { return now }
	o.cfg.LiveKeep = time.Hour
	o.cfg.BaseEvery = 24 * time.Hour
	seed(t, c, o)
	require.NoError(t, o.Step(ctx))
	first := o.State().Bases[0].Label

	refuse := true
	c.tamper = func(_ string, req *clusterhook.ExportRequest, data []byte) []byte {
		if req.Op == clusterhook.OpExportPGBase && refuse {
			return data[:600]
		}
		return data
	}
	for n := 1; n <= 2; n++ {
		now = now.Add(aDay)
		c.push("alice/demo.git", "refs/heads/main", c.commit(fmt.Sprintf("day %d", n)))
		c.checkpoint()
		calls := len(c.calls)
		require.NoError(t, o.Step(ctx), "day %d: the base renewal failed, not the checkpoint", n)
		assert.Contains(t, c.calls[calls:], clusterhook.OpExportPGBase, "day %d: the base is past BaseEvery: renewed", n)
		st := o.State()
		newest, ok := st.newest()
		require.True(t, ok)
		assert.Equal(t, StateProtected, newest.State)
		m, err := o.store.ReadManifest(newest.Name)
		require.NoError(t, err)
		assert.Equal(t, first, m.PG.Base.Label, "day %d: covered from the base held", n)
		require.Len(t, st.Bases, 1)
		assert.Equal(t, first, st.Bases[0].Label)
		staged, err := os.ReadDir(o.store.path("pg", "staging"))
		require.NoError(t, err)
		assert.Empty(t, staged, "day %d: the refused base leaves nothing in staging", n)
		requireRestorable(t, o)

		// A pass soon after does not stream another base to refuse.
		now = now.Add(10 * time.Minute)
		c.push("alice/demo.git", "refs/heads/main", c.commit(fmt.Sprintf("day %d, later", n)))
		c.checkpoint()
		calls = len(c.calls)
		require.NoError(t, o.Step(ctx))
		assert.NotContains(t, c.calls[calls:], clusterhook.OpExportPGBase, "day %d: tried again only renewRetry later", n)
		require.Len(t, o.State().Bases, 1)
	}

	// The base serves again: the next pass renews it, and the pass after
	// restores from the new one and lets the first go.
	refuse = false
	now = now.Add(aDay)
	c.push("alice/demo.git", "refs/heads/main", c.commit("day 3"))
	c.checkpoint()
	require.NoError(t, o.Step(ctx))
	require.Len(t, o.State().Bases, 2, "the renewal is taken again at a later pass")
	renewed := o.State().Bases[1].Label
	now = now.Add(time.Hour + time.Minute)
	c.push("alice/demo.git", "refs/heads/main", c.commit("day 3, later"))
	c.checkpoint()
	require.NoError(t, o.Step(ctx))
	st := o.State()
	newest, ok := st.newest()
	require.True(t, ok)
	m, err := o.store.ReadManifest(newest.Name)
	require.NoError(t, err)
	assert.Equal(t, renewed, m.PG.Base.Label)
	assert.False(t, slices.ContainsFunc(o.State().Bases, func(b BaseInfo) bool { return b.Label == first }), "the first base restores no live checkpoint")
	_, err = os.Stat(o.store.path("pg", "base", first))
	assert.True(t, os.IsNotExist(err))
	requireRestorable(t, o)
}
