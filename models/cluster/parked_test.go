// Copyright 2026 The Forgejo Authors. All rights reserved.
// SPDX-License-Identifier: GPL-3.0-or-later

package cluster

import (
	"context"
	"testing"

	"forgejo.org/models/db"
	"forgejo.org/models/unittest"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// A parked entry is removed by the one transaction that applies it: a
// second retrier finds fewer rows than it read and rolls back.
func TestParkedEntryUnparksOnce(t *testing.T) {
	require.NoError(t, unittest.PrepareTestDatabase())
	ctx := db.DefaultContext
	require.NoError(t, ParkEvents(ctx, []*ParkedEvent{
		{Lineage: "l", LogIndex: 7, Seq: 0, Repo: "o/a.git", Event: "{}"},
		{Lineage: "l", LogIndex: 7, Seq: 1, Repo: "o/a.git", Event: "{}"},
		{Lineage: "l", LogIndex: 9, Seq: 0, Repo: "o/b.git", Event: "{}"},
	}))
	n, err := CountParked(ctx)
	require.NoError(t, err)
	assert.EqualValues(t, 3, n)
	has, err := RepoHasParked(ctx, "o/a.git")
	require.NoError(t, err)
	assert.True(t, has)
	idx, ok, err := MinParkedIndex(ctx, "l")
	require.NoError(t, err)
	assert.True(t, ok)
	assert.EqualValues(t, 7, idx)

	require.NoError(t, NoteParkedFailure(ctx, "l", 7, "again"))
	rows, err := ListParked(ctx)
	require.NoError(t, err)
	require.Len(t, rows, 3)
	assert.Equal(t, 1, rows[0].Attempts)
	assert.Equal(t, "again", rows[0].Reason)

	// A rolled-back retry keeps the rows.
	err = db.WithTx(ctx, func(ctx context.Context) error {
		require.NoError(t, UnparkEntry(ctx, "l", 7, 2))
		return assert.AnError
	})
	require.ErrorIs(t, err, assert.AnError)
	n, _ = CountParked(ctx)
	assert.EqualValues(t, 3, n)

	require.NoError(t, db.WithTx(ctx, func(ctx context.Context) error { return UnparkEntry(ctx, "l", 7, 2) }))
	require.ErrorIs(t, db.WithTx(ctx, func(ctx context.Context) error { return UnparkEntry(ctx, "l", 7, 2) }), ErrParkedGone)
	idx, _, _ = MinParkedIndex(ctx, "l")
	assert.EqualValues(t, 9, idx)
}

// An operator gives up a parked entry only with a reason; the event, its
// last error, who and why are kept, and the trim is no longer held by it.
func TestResolveParkedKeepsTheEventAndItsReason(t *testing.T) {
	require.NoError(t, unittest.PrepareTestDatabase())
	ctx := db.DefaultContext
	require.NoError(t, ParkEvents(ctx, []*ParkedEvent{
		{Lineage: "l", LogIndex: 4, Seq: 0, Repo: "o/a.git", Event: `{"repo":"o/a.git"}`, Reason: "pusher ID reused", Attempts: 5},
		{Lineage: "l", LogIndex: 4, Seq: 1, Repo: "o/a.git", Event: `{"repo":"o/a.git","ref":"b"}`, Reason: "pusher ID reused", Attempts: 5},
		{Lineage: "m", LogIndex: 6, Repo: "o/b.git", Event: "{}"},
		{Lineage: "n", LogIndex: 6, Repo: "o/c.git", Event: "{}"},
	}))

	_, err := ResolveParked(ctx, "", 4, "local-uid:0", "  ")
	require.ErrorIs(t, err, ErrResolveNeedsReason)
	n, _ := CountParked(ctx)
	assert.EqualValues(t, 4, n, "nothing given up without a reason")

	_, err = ResolveParked(ctx, "", 6, "local-uid:0", "gone")
	require.ErrorIs(t, err, ErrParkedAmbiguous)
	_, err = ResolveParked(ctx, "", 99, "local-uid:0", "gone")
	require.ErrorIs(t, err, ErrParkedNotFound)

	res, err := ResolveParked(ctx, "", 4, "forgejo-user:1", "the pusher's account was recreated; work replayed by hand")
	require.NoError(t, err)
	require.Len(t, res, 2)
	_, ok, _ := MinParkedIndex(ctx, "l")
	assert.False(t, ok, "the trim is no longer held")

	kept, err := ListParkedResolutions(ctx, 10)
	require.NoError(t, err)
	require.Len(t, kept, 2)
	for _, k := range kept {
		assert.EqualValues(t, 4, k.LogIndex)
		assert.Equal(t, "forgejo-user:1", k.Operator)
		assert.Equal(t, "the pusher's account was recreated; work replayed by hand", k.Reason)
		assert.Equal(t, "pusher ID reused", k.LastError)
		assert.Contains(t, k.Event, `"repo":"o/a.git"`, "the payload is kept")
		assert.NotZero(t, k.ResolvedUnix)
	}
	_, err = ResolveParked(ctx, "m", 6, "local-uid:0", "gone")
	require.NoError(t, err, "named by its lineage")
}
