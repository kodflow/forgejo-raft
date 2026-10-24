// Copyright 2026 The Forgejo Authors. All rights reserved.
// SPDX-License-Identifier: GPL-3.0-or-later

package cluster

import (
	"context"
	"errors"
	"testing"

	cluster_model "forgejo.org/models/cluster"
	"forgejo.org/modules/setting"
	"forgejo.org/modules/test"
	"forgejo.org/services/cluster/consensus"
	"forgejo.org/services/cluster/reftable"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// oldToken is an entry of the lineage a Recover ended, newToken one of the
// lineage it opened: the indexes go on across the two.
func oldToken(i uint64) consensus.Token {
	return consensus.Token{Cluster: consensus.ClusterID{9}, Epoch: 1, Term: 1, Index: i}
}

func newToken(i uint64) consensus.Token {
	return consensus.Token{Cluster: consensus.ClusterID{10}, Epoch: 1, Term: 1, Index: i}
}

// countLineage counts the events of a lineage the queue holds, and the
// lowest index among them.
func countLineage(lg *memLog, tok consensus.Token) (n int, lowest uint64) {
	for _, ev := range lg.list() {
		if ev.Token.Identity() == tok.Identity() {
			if n == 0 || ev.Token.Index < lowest {
				lowest = ev.Token.Index
			}
			n++
		}
	}
	return n, lowest
}

// After a Recover the consumer dispatched every event of the old lineage,
// under its own cursor, durably; but it trimmed them only up to that
// cursor minus ACK_MARGIN, and that cursor never moves again: the last
// margin of the old lineage stayed in the table for good, and every
// repository it names kept pinned objects, so git gc never ran on them
// again, on any node (#205). The old lineage is trimmed to its end; the
// current one keeps its margin.
func TestEndedLineageTailIsTrimmedToItsEnd(t *testing.T) {
	store, lg, parked := newSQLStore(), &memLog{}, &memParked{}
	// The old lineage: 2000 pushes, one per entry, the last 300 to o/hot.
	for i := uint64(1); i <= 2000; i++ {
		repo := "o/cold.git"
		if i > 1700 {
			repo = "o/hot.git"
		}
		lg.events = append(lg.events, reftable.Event{Token: oldToken(i), Kind: reftable.EventRef, Repo: repo, PusherID: 1, New: "aa"})
	}
	c := unitConsumer(store, lg, parked, func(context.Context, []reftable.Event) error { return nil })
	c.lineageNow = func() string { return newToken(1).Identity().String() }
	ctx := context.Background()
	store.replicate()
	// The new lineage goes on: 20 000 entries, 100 per pass.
	next := uint64(2001)
	for range 200 {
		for range 100 {
			lg.events = append(lg.events, reftable.Event{Token: newToken(next), Kind: reftable.EventRef, Repo: "o/other.git", PusherID: 1, New: "bb"})
			next++
		}
		require.NoError(t, c.pass(ctx))
		store.replicate()
		require.NoError(t, c.pass(ctx))
	}

	oldLeft, hot, curLeft := 0, 0, 0
	for _, ev := range lg.list() {
		switch ev.Token.Identity() {
		case oldToken(1).Identity():
			oldLeft++
			if ev.Repo == "o/hot.git" {
				hot++
			}
		default:
			curLeft++
		}
	}
	oc, _ := store.get(ctx, cursorName(oldToken(1).Identity().String()))
	nc, _ := store.get(ctx, cursorName(newToken(1).Identity().String()))
	t.Logf("old lineage: cursor %d, %d events left (%d of o/hot.git); current lineage: cursor %d, %d events left", oc.Position, oldLeft, hot, nc.Position, curLeft)
	assert.EqualValues(t, 2000, oc.Position, "the old lineage's events are all dispatched")
	assert.Zero(t, oldLeft, "the old lineage's events, all dispatched and durable, are trimmed to its end")
	assert.EqualValues(t, 22000, nc.Position)
	assert.GreaterOrEqual(t, curLeft, int(setting.Cluster.AckMargin), "the current lineage keeps its margin")
}

// endedFixture is a consumer after a Recover: 100 events of the old
// lineage, then 100 of the new one, the table applying the new one.
type endedFixture struct {
	store  *sqlStore
	lg     *memLog
	parked *memParked
	c      *consumer
	ctx    context.Context
	// stuck is a repository the leader has not projected; failing one
	// whose work fails.
	stuck, failing map[string]bool
}

func newEndedFixture(t *testing.T, oldRepo func(i uint64) string) *endedFixture {
	t.Cleanup(test.MockVariableValue(&setting.Cluster.AckMargin, 16))
	t.Cleanup(test.MockVariableValue(&ackEvery, 4))
	f := &endedFixture{store: newSQLStore(), lg: &memLog{}, parked: &memParked{}, ctx: context.Background(), stuck: map[string]bool{}, failing: map[string]bool{}}
	for i := uint64(1); i <= 100; i++ {
		f.lg.events = append(f.lg.events, reftable.Event{Token: oldToken(i), Kind: reftable.EventRef, Repo: oldRepo(i), PusherID: 1, New: "aa"})
	}
	for i := uint64(101); i <= 200; i++ {
		f.lg.events = append(f.lg.events, reftable.Event{Token: newToken(i), Kind: reftable.EventRef, Repo: "o/new.git", PusherID: 1, New: "bb"})
	}
	f.c = unitConsumer(f.store, f.lg, f.parked, func(_ context.Context, evs []reftable.Event) error {
		if f.failing[evs[0].Repo] {
			return errors.New("broken")
		}
		return nil
	})
	f.c.projectedAt = func(repo string) uint64 {
		if f.stuck[repo] {
			return 0
		}
		return 1 << 62
	}
	f.c.lineageNow = func() string { return newToken(1).Identity().String() }
	return f
}

// passes runs the dispatcher with the standby caught up after each pass.
func (f *endedFixture) passes(t *testing.T, n int) {
	for range n {
		_ = f.c.pass(f.ctx)
		f.store.replicate()
	}
}

// backup records the backup cursor of a checkpoint at tok, which read the
// earlier lineages drained when closed.
func (f *endedFixture) backup(tok consensus.Token, closed bool) {
	f.store.mu.Lock()
	f.store.primary[cluster_model.CursorBackup] = cluster_model.Cursor{Lineage: tok.Identity().String(), Position: int64(tok.Index), Closed: closed}
	f.store.mu.Unlock()
	f.store.replicate()
}

// A lineage a Recover ended is trimmed to its end only once nothing can
// need its events: never past an event still waiting for its work, a
// parked event, or what a PostgreSQL restored to the last backup replays;
// never while the table still applies it; never before the trim is
// durable. The current lineage keeps its margin.
func TestEndedLineageIsNotTrimmedEarly(t *testing.T) {
	plain := func(uint64) string { return "o/old.git" }

	t.Run("durable and drained", func(t *testing.T) {
		f := newEndedFixture(t, plain)
		require.NoError(t, f.c.pass(f.ctx))
		n, _ := countLineage(f.lg, oldToken(1))
		assert.Equal(t, 100, n, "nothing is trimmed before the standby holds the cursor")
		f.passes(t, 2)
		n, _ = countLineage(f.lg, oldToken(1))
		assert.Zero(t, n, "the ended lineage is trimmed to its end")
		cur, _ := countLineage(f.lg, newToken(101))
		assert.GreaterOrEqual(t, cur, 16, "the current lineage keeps its margin")
	})

	t.Run("an event of it still waits for its work", func(t *testing.T) {
		f := newEndedFixture(t, func(i uint64) string {
			if i == 100 {
				return "o/stuck.git"
			}
			return "o/old.git"
		})
		f.stuck["o/stuck.git"] = true
		f.passes(t, 3)
		n, lowest := countLineage(f.lg, oldToken(1))
		assert.Equal(t, 17, n, "the cursor at 99 keeps its margin while the last event waits")
		assert.EqualValues(t, 84, lowest)

		f.stuck["o/stuck.git"] = false
		f.passes(t, 2)
		n, _ = countLineage(f.lg, oldToken(1))
		assert.Zero(t, n, "trimmed to its end once its last event is dispatched")
	})

	t.Run("the table still applies it", func(t *testing.T) {
		f := newEndedFixture(t, plain)
		f.lg.events = f.lg.events[:100]
		f.c.lineageNow = func() string { return oldToken(1).Identity().String() }
		f.passes(t, 3)
		n, lowest := countLineage(f.lg, oldToken(1))
		assert.Equal(t, 16, n, "a lineage the table applies keeps its margin")
		assert.EqualValues(t, 85, lowest)
	})

	t.Run("a parked event", func(t *testing.T) {
		f := newEndedFixture(t, func(i uint64) string {
			if i == 50 {
				return "o/bad.git"
			}
			return "o/old.git"
		})
		f.failing["o/bad.git"] = true
		f.passes(t, maxAttempts+3)
		require.Equal(t, []int64{50}, f.parked.indexes())
		n, lowest := countLineage(f.lg, oldToken(1))
		assert.Equal(t, 51, n, "never at or past a parked event")
		assert.EqualValues(t, 50, lowest)

		f.failing["o/bad.git"] = false
		applied, err := f.c.retryParked(f.ctx)
		require.NoError(t, err)
		require.Equal(t, 1, applied)
		f.passes(t, 2)
		n, _ = countLineage(f.lg, oldToken(1))
		assert.Zero(t, n, "trimmed to its end once the parked event is applied")
	})

	t.Run("a backup of the ended lineage", func(t *testing.T) {
		f := newEndedFixture(t, plain)
		f.backup(oldToken(60), false)
		f.passes(t, 3)
		n, lowest := countLineage(f.lg, oldToken(1))
		assert.Equal(t, 40, n, "never past the backup of the ended lineage")
		assert.EqualValues(t, 61, lowest)

		f.backup(newToken(150), true)
		f.passes(t, 2)
		n, _ = countLineage(f.lg, oldToken(1))
		assert.Zero(t, n, "a checkpoint of the current lineage that read it drained releases it")
	})

	t.Run("a backup a checkpoint may still replace", func(t *testing.T) {
		f := newEndedFixture(t, plain)
		settled := false
		f.c.settled = func(*cluster_model.Cursor) bool { return settled }
		f.passes(t, 3)
		n, lowest := countLineage(f.lg, oldToken(1))
		assert.Equal(t, 16, n, "a checkpoint in flight may record a backup that replays it: the margin stays")
		assert.EqualValues(t, 85, lowest)

		settled = true
		f.passes(t, 2)
		n, _ = countLineage(f.lg, oldToken(1))
		assert.Zero(t, n, "trimmed to its end once the backup cursor is the last one")
	})

	t.Run("a backup that did not read it drained", func(t *testing.T) {
		f := newEndedFixture(t, plain)
		f.backup(newToken(150), false)
		f.passes(t, 3)
		n, lowest := countLineage(f.lg, oldToken(1))
		assert.Equal(t, 16, n, "a PostgreSQL restored to that backup may replay the ended lineage: the margin stays")
		assert.EqualValues(t, 85, lowest)

		f.backup(newToken(190), true)
		f.passes(t, 2)
		n, _ = countLineage(f.lg, oldToken(1))
		assert.Zero(t, n, "released by the next checkpoint that reads it drained")
	})
}
