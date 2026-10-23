// Copyright 2026 The Forgejo Authors. All rights reserved.
// SPDX-License-Identifier: GPL-3.0-or-later

package cluster

import (
	"context"
	"errors"
	"maps"
	"sort"
	"sync"
	"testing"

	cluster_model "forgejo.org/models/cluster"
	"forgejo.org/modules/setting"
	"forgejo.org/modules/test"
	"forgejo.org/services/cluster/consensus"
	"forgejo.org/services/cluster/reftable"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// memParked is the parked events' table, in memory.
type memParked struct {
	mu   sync.Mutex
	rows []*cluster_model.ParkedEvent
}

func (p *memParked) park(_ context.Context, rows []*cluster_model.ParkedEvent) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	for _, r := range rows {
		cp := *r
		p.rows = append(p.rows, &cp)
	}
	return nil
}

func (p *memParked) hasParked(_ context.Context, repo string, repoID int64) (bool, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	for _, r := range p.rows {
		if repoID != 0 && r.RepoID == repoID || (repoID == 0 || r.RepoID == 0) && r.Repo == repo {
			return true, nil
		}
	}
	return false, nil
}

func (p *memParked) minParked(_ context.Context, lineage string) (int64, bool, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	var m int64
	ok := false
	for _, r := range p.rows {
		if r.Lineage == lineage && (!ok || r.LogIndex < m) {
			m, ok = r.LogIndex, true
		}
	}
	return m, ok, nil
}

func (p *memParked) list(context.Context) ([]*cluster_model.ParkedEvent, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	out := make([]*cluster_model.ParkedEvent, 0, len(p.rows))
	for _, r := range p.rows {
		cp := *r
		out = append(out, &cp)
	}
	sort.SliceStable(out, func(i, j int) bool {
		if out[i].Lineage != out[j].Lineage {
			return out[i].Lineage < out[j].Lineage
		}
		if out[i].LogIndex != out[j].LogIndex {
			return out[i].LogIndex < out[j].LogIndex
		}
		return out[i].Seq < out[j].Seq
	})
	return out, nil
}

func (p *memParked) unpark(_ context.Context, lineage string, index int64, want int) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	keep := p.rows[:0]
	n := 0
	for _, r := range p.rows {
		if r.Lineage == lineage && r.LogIndex == index {
			n++
			continue
		}
		keep = append(keep, r)
	}
	p.rows = keep
	if n != want {
		return cluster_model.ErrParkedGone
	}
	return nil
}

func (p *memParked) note(_ context.Context, lineage string, index int64, reason string) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	for _, r := range p.rows {
		if r.Lineage == lineage && r.LogIndex == index {
			r.Attempts++
			r.Reason = reason
		}
	}
	return nil
}

func (p *memParked) indexes() []int64 {
	p.mu.Lock()
	defer p.mu.Unlock()
	var out []int64
	for _, r := range p.rows {
		out = append(out, r.LogIndex)
	}
	return out
}

func (p *memParked) wire(c *consumer) {
	c.park, c.hasParked, c.minParked = p.park, p.hasParked, p.minParked
	c.listParked, c.unpark, c.noteParked = p.list, p.unpark, p.note
}

// sqlStore is a database with a primary and one standby: the dispatcher
// commits on the primary, the standby holds what was replicated to it, and
// a failover makes the standby the primary.
type sqlStore struct {
	mu      sync.Mutex
	primary map[string]cluster_model.Cursor
	standby map[string]cluster_model.Cursor
}

func newSQLStore() *sqlStore {
	return &sqlStore{primary: map[string]cluster_model.Cursor{}, standby: map[string]cluster_model.Cursor{}}
}

func (s *sqlStore) get(_ context.Context, name string) (*cluster_model.Cursor, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	v := s.primary[name]
	v.Name = name
	return &v, nil
}

func (s *sqlStore) advance(_ context.Context, name string, prev, next cluster_model.Cursor) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	cur := s.primary[name]
	if cur.Lineage != prev.Lineage || cur.Position != prev.Position {
		return cluster_model.ErrCursorMoved
	}
	s.primary[name] = next
	return nil
}

// write is a row the work writes, beside the cursor.
func (s *sqlStore) write(name string) {
	s.mu.Lock()
	s.primary[name] = cluster_model.Cursor{Lineage: "row"}
	s.mu.Unlock()
}

func (s *sqlStore) has(name string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	_, ok := s.primary[name]
	return ok
}

func (s *sqlStore) replicate() {
	s.mu.Lock()
	s.standby = maps.Clone(s.primary)
	s.mu.Unlock()
}

func (s *sqlStore) failover() {
	s.mu.Lock()
	s.primary = maps.Clone(s.standby)
	s.mu.Unlock()
}

// durable is the synchronous standby's check: it holds every commit.
func (s *sqlStore) durable(context.Context) (bool, string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if maps.Equal(s.primary, s.standby) {
		return true, "standby caught up"
	}
	return false, "standby behind"
}

// memLog is the table's event queue.
type memLog struct {
	mu     sync.Mutex
	events []reftable.Event
}

func (l *memLog) list() []reftable.Event {
	l.mu.Lock()
	defer l.mu.Unlock()
	return append([]reftable.Event(nil), l.events...)
}

func (l *memLog) ack(_ context.Context, lineage string, index uint64) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	keep := l.events[:0]
	for _, ev := range l.events {
		if ev.Token.Identity().String() == lineage && ev.Token.Index <= index {
			continue
		}
		keep = append(keep, ev)
	}
	l.events = keep
	return nil
}

func testToken(i uint64) consensus.Token {
	return consensus.Token{Cluster: consensus.ClusterID{9}, Epoch: 1, Term: 1, Index: i}
}

func unitConsumer(store *sqlStore, lg *memLog, parked *memParked, work func(ctx context.Context, evs []reftable.Event) error) *consumer {
	c := &consumer{
		work: work, dbReady: func() bool { return true },
		cursor: store.get, advance: store.advance,
		tx:     func(ctx context.Context, f func(context.Context) error) error { return f(ctx) },
		events: lg.list, ack: lg.ack, durable: store.durable,
		projectedAt: func(string) uint64 { return 1 << 62 },
		lastAck:     map[string]uint64{}, failures: map[uint64]int{}, reasons: map[uint64]string{}, seen: map[string]int64{},
	}
	parked.wire(c)
	return c
}

// Finding 3: the primary's cursor is lost by a failover to a standby that
// had not received it. The log never trimmed an event whose SQL was not
// confirmed on the standby, so every event is replayed and none is lost.
func TestTrimSurvivesACursorLostByFailover(t *testing.T) {
	defer test.MockVariableValue(&setting.Cluster.AckMargin, 0)()
	defer test.MockVariableValue(&ackEvery, 1)()
	store, lg, parked := newSQLStore(), &memLog{}, &memParked{}
	const n = 300
	for i := uint64(1); i <= n; i++ {
		lg.events = append(lg.events, reftable.Event{Token: testToken(i), Kind: reftable.EventRef, Repo: "o/r.git", PusherID: 1})
	}
	work := func(_ context.Context, evs []reftable.Event) error {
		for _, ev := range evs {
			store.write("work@" + ev.Token.String())
		}
		return nil
	}
	c := unitConsumer(store, lg, parked, work)
	ctx := context.Background()

	// The dispatcher commits everything on the primary; the standby has
	// none of it yet (an asynchronous one, or a synchronous one that lags).
	require.NoError(t, c.pass(ctx))
	lin := testToken(1).Identity().String()
	cur, _ := store.get(ctx, cursorName(lin))
	require.EqualValues(t, n, cur.Position)
	assert.Len(t, lg.list(), n, "nothing is trimmed while the standby lacks the cursor")
	assert.False(t, c.Durability().OK)

	// Failover: the cursor and the work go back to the standby's state.
	store.failover()
	require.NoError(t, c.pass(ctx))
	for i := uint64(1); i <= n; i++ {
		assert.True(t, store.has("work@"+testToken(i).String()), "the work of %d is in SQL after the failover", i)
	}

	// Once the standby holds it, the log is trimmed behind the cursor.
	store.replicate()
	require.NoError(t, c.pass(ctx))
	assert.Empty(t, lg.list(), "trimmed once durable")
	assert.True(t, c.Durability().OK)
}

// Finding 6: an event whose work keeps failing is parked, never dropped:
// its row commits with the cursor, the log keeps it, the later events of
// its repository wait behind it, and a retry applies each exactly once, in
// order, once the work succeeds; only then is the log trimmed past it.
func TestFailingWorkIsParkedNotTrimmedThenAppliedOnce(t *testing.T) {
	defer test.MockVariableValue(&setting.Cluster.AckMargin, 0)()
	defer test.MockVariableValue(&ackEvery, 1)()
	store, lg, parked := newSQLStore(), &memLog{}, &memParked{}
	lg.events = []reftable.Event{
		{Token: testToken(1), Kind: reftable.EventRef, Repo: "o/bad.git", Ref: "refs/heads/a", PusherID: 1},
		{Token: testToken(2), Kind: reftable.EventRef, Repo: "o/good.git", Ref: "refs/heads/a", PusherID: 1},
		{Token: testToken(3), Kind: reftable.EventRef, Repo: "o/bad.git", Ref: "refs/heads/b", PusherID: 1},
	}
	broken := true
	var mu sync.Mutex
	applied := map[string][]uint64{}
	work := func(_ context.Context, evs []reftable.Event) error {
		if evs[0].Repo == "o/bad.git" && broken {
			return errors.New("broken")
		}
		mu.Lock()
		defer mu.Unlock()
		for _, ev := range evs {
			applied[ev.Repo] = append(applied[ev.Repo], ev.Token.Index)
		}
		return nil
	}
	c := unitConsumer(store, lg, parked, work)
	store.replicate()
	ctx := context.Background()
	for range maxAttempts {
		require.Error(t, c.pass(ctx))
	}
	require.NoError(t, c.pass(ctx))
	store.replicate()
	require.NoError(t, c.pass(ctx))

	assert.Equal(t, []int64{1, 3}, parked.indexes(), "the failing event is parked, the later one of its repository behind it")
	assert.Equal(t, []uint64{2}, applied["o/good.git"], "another repository is not held back")
	assert.Empty(t, applied["o/bad.git"])
	assert.Len(t, lg.list(), 3, "the log keeps the events while one is parked")

	// A retry while the work still fails keeps them.
	n, err := c.retryParked(ctx)
	require.NoError(t, err)
	assert.Zero(t, n)
	assert.Equal(t, []int64{1, 3}, parked.indexes())

	broken = false
	n, err = c.retryParked(ctx)
	require.NoError(t, err)
	assert.Equal(t, 2, n)
	n, err = c.retryParked(ctx)
	require.NoError(t, err)
	assert.Zero(t, n, "nothing left to apply")
	assert.Equal(t, []uint64{1, 3}, applied["o/bad.git"], "each applied once, in order")
	assert.Empty(t, parked.indexes())

	store.replicate()
	c.maybeAck(ctx, testToken(1).Identity().String())
	assert.Empty(t, lg.list(), "trimmed once nothing is parked")
}

// The trim never passes the backup cursor a checkpoint recorded: a
// PostgreSQL restored to that checkpoint replays what follows from the log.
func TestTrimStopsAtTheBackupCursor(t *testing.T) {
	defer test.MockVariableValue(&setting.Cluster.AckMargin, 0)()
	defer test.MockVariableValue(&ackEvery, 1)()
	store, lg, parked := newSQLStore(), &memLog{}, &memParked{}
	for i := uint64(1); i <= 50; i++ {
		lg.events = append(lg.events, reftable.Event{Token: testToken(i), Kind: reftable.EventRef, Repo: "o/r.git", PusherID: 1})
	}
	c := unitConsumer(store, lg, parked, func(context.Context, []reftable.Event) error { return nil })
	lin := testToken(1).Identity().String()
	store.primary[cluster_model.CursorBackup] = cluster_model.Cursor{Lineage: lin, Position: 20}
	require.NoError(t, c.pass(context.Background()))
	store.replicate()
	c.maybeAck(context.Background(), lin)
	require.Len(t, lg.list(), 30)
	assert.EqualValues(t, 21, lg.list()[0].Token.Index)
}

// The trim never passes the backup cursor, however far the backup lags:
// it is held there, and the lag is reported.
func TestTrimHeldAtALaggingBackup(t *testing.T) {
	defer test.MockVariableValue(&setting.Cluster.AckMargin, 0)()
	defer test.MockVariableValue(&ackEvery, 1)()
	store, lg, parked := newSQLStore(), &memLog{}, &memParked{}
	const base = backupMaxLag + 100
	for i := uint64(base); i < base+20; i++ {
		lg.events = append(lg.events, reftable.Event{Token: testToken(i), Kind: reftable.EventRef, Repo: "o/r.git", PusherID: 1})
	}
	c := unitConsumer(store, lg, parked, func(context.Context, []reftable.Event) error { return nil })
	lin := testToken(1).Identity().String()
	store.primary[cluster_model.CursorBackup] = cluster_model.Cursor{Lineage: lin, Position: 10}
	require.NoError(t, c.pass(context.Background()))
	store.replicate()
	c.maybeAck(context.Background(), lin)
	assert.Len(t, lg.list(), 20, "nothing past the backup is trimmed")
	assert.EqualValues(t, base+19-10, c.BackupLag(), "the held trim is reported")
}
