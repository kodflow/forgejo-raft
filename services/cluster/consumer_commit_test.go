// Copyright 2026 The Forgejo Authors. All rights reserved.
// SPDX-License-Identifier: GPL-3.0-or-later

package cluster

import (
	"context"
	"errors"
	"maps"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	cluster_model "forgejo.org/models/cluster"
	"forgejo.org/modules/clusterhook"
	"forgejo.org/modules/test"
	"forgejo.org/services/cluster/reftable"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// A COMMIT whose outcome is unknown: the transaction ran, then the
// connection reported an error. landed says whether the database kept it.
func unknownCommitTx(landed bool) func(ctx context.Context, f func(context.Context) error) error {
	once := true
	return func(ctx context.Context, f func(context.Context) error) error {
		if !once {
			return f(ctx)
		}
		once = false
		if landed {
			if err := f(ctx); err != nil {
				return err
			}
		}
		return errors.New("driver: bad connection")
	}
}

// The effects after commit of a batch whose COMMIT committed but returned
// an error run exactly once, as those of a batch that rolled back and is
// replayed (#103, TestHuntR7ProveUcfx).
func TestEffectsOfACommitWithAnUnknownOutcomeRunOnce(t *testing.T) {
	for _, landed := range []bool{true, false} {
		store, lg, parked := newSQLStore(), &memLog{}, &memParked{}
		lg.events = []reftable.Event{{Token: testToken(1), Kind: reftable.EventRef, Repo: "o/r.git", Ref: "refs/heads/main", PusherID: 1}}
		effects := 0
		c := unitConsumer(store, lg, parked, func(ctx context.Context, _ []reftable.Event) error {
			clusterhook.Defer(ctx, func() { effects++ })
			return nil
		})
		c.tx = unknownCommitTx(landed)
		for range 3 {
			_ = c.pass(context.Background())
		}
		cur, _ := store.get(context.Background(), cursorName(testToken(1).Identity().String()))
		assert.EqualValues(t, 1, cur.Position, "landed %t", landed)
		assert.Equal(t, 1, effects, "landed %t: the effects run once", landed)
	}
}

// Same for a parked entry: its rows went with the work, so its effects run.
func TestEffectsOfARetriedParkedEntryWithAnUnknownOutcomeRunOnce(t *testing.T) {
	for _, landed := range []bool{true, false} {
		store, lg, parked := newSQLStore(), &memLog{}, &memParked{}
		effects := 0
		c := unitConsumer(store, lg, parked, func(ctx context.Context, _ []reftable.Event) error {
			clusterhook.Defer(ctx, func() { effects++ })
			return nil
		})
		lin := testToken(1).Identity().String()
		require.NoError(t, parked.park(context.Background(), []*cluster_model.ParkedEvent{
			{Lineage: lin, LogIndex: 4, Repo: "o/r.git", Event: `{"tok":{},"k":"ref","repo":"o/r.git"}`, Reason: "boom", Attempts: maxAttempts},
		}))
		// The memory transaction cannot roll back the unpark: one that did
		// not land never ran it.
		c.tx = unknownCommitTx(landed)
		for range 2 {
			_, err := c.retryParked(context.Background())
			require.NoError(t, err)
		}
		assert.Empty(t, parked.indexes(), "landed %t", landed)
		assert.Equal(t, 1, effects, "landed %t: the effects run once", landed)
	}
}

// An error of the work rolls the batch back: the outcome check runs only
// for an error after the callback's end, the COMMIT's. Another dispatcher
// that committed the same batch meanwhile must not make this attempt's
// deferred effects run (CodeRabbit on #120).
func TestEffectsOfAFailedWorkNeverRunWhenAnotherDispatcherCommitted(t *testing.T) {
	store, lg, parked := newSQLStore(), &memLog{}, &memParked{}
	ev := reftable.Event{Token: testToken(1), Kind: reftable.EventRef, Repo: "o/r.git", Ref: "refs/heads/main", PusherID: 1}
	lin := testToken(1).Identity().String()
	effects := 0
	c := unitConsumer(store, lg, parked, func(ctx context.Context, _ []reftable.Event) error {
		clusterhook.Defer(ctx, func() { effects++ })
		return errors.New("boom")
	})
	// The cursor already reads the batch's end: another dispatcher
	// committed it while this one failed.
	c.cursor = func(context.Context, string) (*cluster_model.Cursor, error) {
		return &cluster_model.Cursor{Lineage: lin, Position: 1}, nil
	}
	cur := &cluster_model.Cursor{Lineage: lin}
	require.Error(t, c.dispatch(context.Background(), lin, cur, []reftable.Event{ev}))
	assert.Zero(t, effects, "the effects of a rolled-back attempt do not run")
	assert.EqualValues(t, 0, cur.Position)
	assert.Equal(t, 1, c.failures[1], "the failure is counted")
}

// Same for a parked entry: another retrier that removed its rows while
// this one's work failed does not make this attempt's effects run.
func TestEffectsOfAFailedParkedRetryNeverRunWhenAnotherRetrierApplied(t *testing.T) {
	store, lg, parked := newSQLStore(), &memLog{}, &memParked{}
	lin := testToken(1).Identity().String()
	require.NoError(t, parked.park(context.Background(), []*cluster_model.ParkedEvent{
		{Lineage: lin, LogIndex: 4, Repo: "o/r.git", Event: `{"tok":{},"k":"ref","repo":"o/r.git"}`, Reason: "boom", Attempts: maxAttempts},
	}))
	effects := 0
	c := unitConsumer(store, lg, parked, func(ctx context.Context, _ []reftable.Event) error {
		clusterhook.Defer(ctx, func() { effects++ })
		// `forgejo cluster reconcile --apply` applied it meanwhile.
		require.NoError(t, parked.unpark(ctx, lin, 4, 1))
		return errors.New("boom")
	})
	applied, err := c.retryParked(context.Background())
	require.NoError(t, err)
	assert.Zero(t, applied)
	assert.Zero(t, effects, "the effects of a rolled-back retry do not run")
}

// failover is a database failover seen from the dispatcher: once the COMMIT
// of a batch failed with an unknown outcome, the old primary going down and
// the new one not promoted yet, the next reads find no primary. left is how
// many of them fail, -1 until up; reads counts those that failed.
type failover struct {
	mu     sync.Mutex
	failed bool
	left   int
	reads  int
}

func (f *failover) commitFailed() {
	f.mu.Lock()
	f.failed = true
	f.mu.Unlock()
}

func (f *failover) up() {
	f.mu.Lock()
	f.left = 0
	f.mu.Unlock()
}

func (f *failover) failedReads() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.reads
}

// read fails while no primary answers.
func (f *failover) read() error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if !f.failed || f.left == 0 {
		return nil
	}
	if f.left > 0 {
		f.left--
	}
	f.reads++
	return errors.New("failed to connect to `host=pg user=forgejo database=forgejo`: dial error (dial tcp 10.0.0.5:5432: connect: connection refused)")
}

// wire makes c's reads of the cursor and of the parked rows go through f.
func (f *failover) wire(c *consumer) {
	cursor, list := c.cursor, c.listParked
	c.cursor = func(ctx context.Context, name string) (*cluster_model.Cursor, error) {
		if err := f.read(); err != nil {
			return nil, err
		}
		return cursor(ctx, name)
	}
	c.listParked = func(ctx context.Context) ([]*cluster_model.ParkedEvent, error) {
		if err := f.read(); err != nil {
			return nil, err
		}
		return list(ctx)
	}
}

// failoverCommitTx is a first COMMIT whose outcome is unknown, cut by a
// failover: the transaction runs to its end, then its client gets the
// error of a backend terminated by the fast shutdown. landed says whether
// the new primary holds what it wrote: when not, the cursor and the parked
// rows are as before it.
func failoverCommitTx(store *sqlStore, parked *memParked, f *failover, landed bool) func(ctx context.Context, fn func(context.Context) error) error {
	once := true
	return func(ctx context.Context, fn func(context.Context) error) error {
		if !once {
			return fn(ctx)
		}
		once = false
		store.mu.Lock()
		cursors := maps.Clone(store.primary)
		store.mu.Unlock()
		parked.mu.Lock()
		rows := slices.Clone(parked.rows)
		parked.mu.Unlock()
		if err := fn(ctx); err != nil {
			return err
		}
		if !landed {
			store.mu.Lock()
			store.primary = cursors
			store.mu.Unlock()
			parked.mu.Lock()
			parked.rows = rows
			parked.mu.Unlock()
		}
		f.commitFailed()
		return errors.New("FATAL: terminating connection due to administrator command (SQLSTATE 57P01)")
	}
}

// A consumer whose batch's work defers one effect, counted.
func effectsConsumer(store *sqlStore, lg *memLog, parked *memParked, effects *int) *consumer {
	return unitConsumer(store, lg, parked, func(ctx context.Context, _ []reftable.Event) error {
		clusterhook.Defer(ctx, func() { *effects++ })
		return nil
	})
}

func oneEvent() []reftable.Event {
	return []reftable.Event{{Token: testToken(1), Kind: reftable.EventRef, Repo: "o/r.git", Ref: "refs/heads/main", PusherID: 1}}
}

// countingConsumer is effectsConsumer that also counts the runs of the
// work, a replay included.
func countingConsumer(store *sqlStore, lg *memLog, parked *memParked, effects, works *int) *consumer {
	return unitConsumer(store, lg, parked, func(ctx context.Context, _ []reftable.Event) error {
		*works++
		clusterhook.Defer(ctx, func() { *effects++ })
		return nil
	})
}

func parkOne(t *testing.T, parked *memParked) {
	t.Helper()
	require.NoError(t, parked.park(context.Background(), []*cluster_model.ParkedEvent{
		{Lineage: testToken(1).Identity().String(), LogIndex: 4, Repo: "o/r.git", Event: `{"tok":{},"k":"ref","repo":"o/r.git"}`, Reason: "boom", Attempts: maxAttempts},
	}))
}

// The read that settles a COMMIT with an unknown outcome finds no primary
// during the failover that cut it, and the reads after it too while the
// failover lasts. The batch's effects wait for a read that succeeds, and
// nothing is dispatched before it: a batch that committed runs them once,
// one that rolled back is replayed after that read and runs them once
// (#268).
func TestEffectsOfAnUnknownCommitWaitForAReadThatSettlesIt(t *testing.T) {
	ctx := context.Background()
	for _, landed := range []bool{true, false} {
		for _, failing := range []int{1, 3, 20} {
			store, lg, parked := newSQLStore(), &memLog{}, &memParked{}
			lg.events = oneEvent()
			effects, works := 0, 0
			c := countingConsumer(store, lg, parked, &effects, &works)
			fo := &failover{left: failing}
			fo.wire(c)
			c.tx = failoverCommitTx(store, parked, fo, landed)
			// The pass whose COMMIT fails, then one per read that fails.
			for i := range failing {
				require.Error(t, c.pass(ctx), "landed %t, %d failing reads: pass %d", landed, failing, i)
				assert.Zero(t, effects, "landed %t, %d failing reads: the effects wait for a read that succeeds", landed, failing)
				assert.Equal(t, 1, works, "landed %t, %d failing reads: nothing is dispatched before a read settles the batch", landed, failing)
			}
			for range 2 {
				require.NoError(t, c.pass(ctx), "landed %t, %d failing reads", landed, failing)
			}
			cur, _ := store.get(ctx, cursorName(testToken(1).Identity().String()))
			assert.EqualValues(t, 1, cur.Position, "landed %t, %d failing reads", landed, failing)
			assert.Equal(t, 1, effects, "landed %t, %d failing reads: the effects run once", landed, failing)
			assert.Equal(t, failing, fo.failedReads(), "landed %t, %d failing reads", landed, failing)
			replays := 0
			if !landed {
				replays = 1
			}
			assert.Equal(t, 1+replays, works, "landed %t, %d failing reads: replayed only if it rolled back", landed, failing)
			assert.False(t, c.hasUnknown(false), "landed %t, %d failing reads", landed, failing)
		}
	}
}

// Same for a parked entry: its rows went with the work only if the COMMIT
// landed, a failed read of them says nothing, and the entry is not retried
// before a read settles it.
func TestEffectsOfAnUnknownParkedRetryWaitForAReadThatSettlesIt(t *testing.T) {
	ctx := context.Background()
	for _, landed := range []bool{true, false} {
		for _, failing := range []int{1, 3} {
			store, lg, parked := newSQLStore(), &memLog{}, &memParked{}
			effects, works := 0, 0
			c := countingConsumer(store, lg, parked, &effects, &works)
			parkOne(t, parked)
			fo := &failover{left: failing}
			fo.wire(c)
			c.tx = failoverCommitTx(store, parked, fo, landed)
			// The retry whose COMMIT fails, then one per read that fails.
			for i := range failing {
				applied, err := c.retryParked(ctx)
				if i > 0 {
					require.Error(t, err, "landed %t, %d failing reads: retry %d", landed, failing, i)
				}
				assert.Zero(t, applied, "landed %t, %d failing reads: not known to be applied", landed, failing)
				assert.Zero(t, effects, "landed %t, %d failing reads: the effects wait for a read that succeeds", landed, failing)
				assert.Equal(t, 1, works, "landed %t, %d failing reads: the entry is not retried before a read settles it", landed, failing)
			}
			applied, err := c.retryParked(ctx)
			require.NoError(t, err, "landed %t, %d failing reads", landed, failing)
			assert.Equal(t, 1, applied, "landed %t, %d failing reads", landed, failing)
			assert.Empty(t, parked.indexes(), "landed %t, %d failing reads", landed, failing)
			assert.Equal(t, 1, effects, "landed %t, %d failing reads: the effects run once", landed, failing)
			assert.Equal(t, failing, fo.failedReads(), "landed %t, %d failing reads", landed, failing)
			retries := 0
			if !landed {
				retries = 1
			}
			assert.Equal(t, 1+retries, works, "landed %t, %d failing reads: retried only if it rolled back", landed, failing)
			assert.False(t, c.hasUnknown(true), "landed %t, %d failing reads", landed, failing)
		}
	}
}

// A parked entry left with an unknown outcome holds the dispatcher's pass
// too, the pass retrying the parked events itself, until a read settles
// it.
func TestAnUnknownParkedRetryHoldsThePassUntilARead(t *testing.T) {
	ctx := context.Background()
	store, lg, parked := newSQLStore(), &memLog{}, &memParked{}
	effects, works := 0, 0
	c := countingConsumer(store, lg, parked, &effects, &works)
	parkOne(t, parked)
	fo := &failover{left: 2}
	fo.wire(c)
	c.tx = failoverCommitTx(store, parked, fo, true)
	_, err := c.retryParked(ctx)
	require.NoError(t, err)
	lg.events = oneEvent()
	require.Error(t, c.pass(ctx), "the second read fails")
	assert.Equal(t, 1, works, "nothing is dispatched before a read settles the entry")
	assert.Zero(t, effects)
	require.NoError(t, c.pass(ctx))
	assert.Equal(t, 2, works, "the batch is dispatched once the entry is settled")
	assert.Equal(t, 2, effects, "the entry's effects, then the batch's")
}

// A read that succeeds may find the cursor past the batch: another
// dispatcher, the next leader's, went on from where this COMMIT left it.
// The batch committed, and its effects run.
func TestEffectsOfAnUnknownCommitRunWhenTheCursorWentPastIt(t *testing.T) {
	store, lg, parked := newSQLStore(), &memLog{}, &memParked{}
	lg.events = oneEvent()
	effects := 0
	c := effectsConsumer(store, lg, parked, &effects)
	fo := &failover{}
	inner := failoverCommitTx(store, parked, fo, true)
	name := cursorName(testToken(1).Identity().String())
	c.tx = func(ctx context.Context, fn func(context.Context) error) error {
		err := inner(ctx, fn)
		if err != nil {
			store.mu.Lock()
			cur := store.primary[name]
			cur.Position = 2
			store.primary[name] = cur
			store.mu.Unlock()
		}
		return err
	}
	for range 2 {
		_ = c.pass(context.Background())
	}
	assert.Equal(t, 1, effects, "the effects of the batch that committed run once")
}

// A read that hangs, on a connection to a primary going down, is bounded:
// each pass of the dispatcher returns, and the next one reads again.
func TestTheReadThatSettlesAnUnknownCommitIsBounded(t *testing.T) {
	defer test.MockVariableValue(&commitOutcomeReadMax, 20*time.Millisecond)()
	ctx := context.Background()
	store, lg, parked := newSQLStore(), &memLog{}, &memParked{}
	lg.events = oneEvent()
	effects := 0
	c := effectsConsumer(store, lg, parked, &effects)
	var hang atomic.Bool
	read := c.cursor
	c.cursor = func(ctx context.Context, name string) (*cluster_model.Cursor, error) {
		if hang.Load() {
			// The test's own bound, far past the read's.
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-time.After(time.Second):
				return nil, errors.New("the read hung")
			}
		}
		return read(ctx, name)
	}
	once := true
	c.tx = func(ctx context.Context, fn func(context.Context) error) error {
		if !once {
			return fn(ctx)
		}
		once = false
		if err := fn(ctx); err != nil {
			return err
		}
		hang.Store(true)
		return errors.New("FATAL: terminating connection due to administrator command (SQLSTATE 57P01)")
	}
	for i := range 3 {
		start := time.Now()
		require.Error(t, c.pass(ctx), "pass %d", i)
		assert.Less(t, time.Since(start), 500*time.Millisecond, "pass %d: the read is bounded", i)
	}
	assert.Zero(t, effects)
	hang.Store(false)
	require.NoError(t, c.pass(ctx))
	assert.Equal(t, 1, effects, "the effects of the batch that committed run once")
}

// The leadership may move while the database does not answer: the next
// leader replays a batch or retries an entry that rolled back, but not
// one that committed, whose effects only the node that held them runs.
// That node settles them as it no longer leads.
func TestANodeThatNoLongerLeadsSettlesWhatItLeftUnknown(t *testing.T) {
	ctx := context.Background()
	for _, landed := range []bool{true, false} {
		ran := 0
		if landed {
			ran = 1
		}

		store, lg, parked := newSQLStore(), &memLog{}, &memParked{}
		lg.events = oneEvent()
		effects := 0
		c := effectsConsumer(store, lg, parked, &effects)
		fo := &failover{left: 1}
		fo.wire(c)
		c.tx = failoverCommitTx(store, parked, fo, landed)
		require.Error(t, c.pass(ctx), "landed %t", landed)
		c.r = &Replicator{} // no engine: the node does not lead
		require.NoError(t, c.step(ctx), "landed %t", landed)
		assert.Equal(t, ran, effects, "landed %t: a batch that committed runs its effects, one that rolled back is the next leader's", landed)
		assert.False(t, c.hasUnknown(false), "landed %t", landed)

		store, lg, parked = newSQLStore(), &memLog{}, &memParked{}
		effects = 0
		c = effectsConsumer(store, lg, parked, &effects)
		parkOne(t, parked)
		fo = &failover{left: 1}
		fo.wire(c)
		c.tx = failoverCommitTx(store, parked, fo, landed)
		_, err := c.retryParked(ctx)
		require.NoError(t, err, "landed %t", landed)
		c.r = &Replicator{}
		require.NoError(t, c.step(ctx), "landed %t", landed)
		assert.Equal(t, ran, effects, "landed %t: a parked entry that committed runs its effects, one that rolled back is the next leader's", landed)
		assert.False(t, c.hasUnknown(true), "landed %t", landed)
	}
}

// The consumer that stops reads once more what it left unknown; what stays
// so runs its effects at least once, as no later read on this node settles
// it and a duplicate costs less than a loss.
func TestTheConsumerThatStopsSettlesWhatItLeftUnknown(t *testing.T) {
	for _, landed := range []bool{true, false} {
		for _, up := range []bool{true, false} {
			store, lg, parked := newSQLStore(), &memLog{}, &memParked{}
			lg.events = oneEvent()
			effects := 0
			c := effectsConsumer(store, lg, parked, &effects)
			fo := &failover{left: -1}
			fo.wire(c)
			c.tx = failoverCommitTx(store, parked, fo, landed)
			require.Error(t, c.pass(context.Background()), "landed %t, up %t", landed, up)
			if up {
				fo.up()
			}
			c.r = &Replicator{}
			ctx, cancel := context.WithCancel(context.Background())
			cancel()
			c.run(ctx)
			want := 1
			if up && !landed {
				want = 0 // read: it rolled back, its replay is the next leader's
			}
			assert.Equal(t, want, effects, "landed %t, the database answers at the stop %t", landed, up)
			assert.False(t, c.hasUnknown(false), "landed %t, up %t", landed, up)
		}
	}
}
