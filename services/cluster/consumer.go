// Copyright 2026 The Forgejo Authors. All rights reserved.
// SPDX-License-Identifier: GPL-3.0-or-later

package cluster

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"sync"
	"time"

	cluster_model "forgejo.org/models/cluster"
	"forgejo.org/models/db"
	git_model "forgejo.org/models/git"
	repo_model "forgejo.org/models/repo"
	user_model "forgejo.org/models/user"
	"forgejo.org/modules/clusterhook"
	"forgejo.org/modules/git"
	"forgejo.org/modules/gitrepo"
	"forgejo.org/modules/json"
	"forgejo.org/modules/log"
	repo_module "forgejo.org/modules/repository"
	"forgejo.org/modules/setting"
	"forgejo.org/services/cluster/consensus"
	"forgejo.org/services/cluster/reftable"
	repo_service "forgejo.org/services/repository"

	"github.com/jackc/pgx/v5/pgconn"
)

// consumer derives SQL from the log (ADR 0152 D19, R7-29). ONE dispatcher,
// on the current leader, runs the post-receive work of each event, keyed by
// its log position, and commits a per-lineage cursor as a compare-and-set.
// It rebuilds its queue from the table's events past the cursor whenever it
// gains leadership, so a failover loses nothing.
//
// No event is ever dropped. One whose work cannot run (it keeps failing,
// its repository is never projected) is PARKED: a row holding the event
// commits with the cursor that moves past it, the log keeps the event while
// the row exists, and the row goes only with the transaction that finally
// runs the work. The log is trimmed only behind SQL that survives any
// failover of the database (durability.go).
type consumer struct {
	r *Replicator
	// work runs the post-receive work of a batch of events of one repository;
	// tests replace it.
	work func(ctx context.Context, evs []reftable.Event) error
	// dbReady reports whether Forgejo's database is initialised.
	dbReady func() bool
	// cursor and advance are the SQL side; tests replace them.
	cursor  func(ctx context.Context, name string) (*cluster_model.Cursor, error)
	advance func(ctx context.Context, name string, prev, next cluster_model.Cursor) error
	// tx runs f in one SQL transaction; tests replace it. inTx reports
	// whether that transaction is still open, nil when tx is not SQL's.
	tx   func(ctx context.Context, f func(ctx context.Context) error) error
	inTx func(ctx context.Context) bool
	// events, projectedAt and lineageNow read the table and the projector;
	// ack proposes a trim. Tests replace them.
	events      func() []reftable.Event
	projectedAt func(repo string) uint64
	lineageNow  func() string
	ack         func(ctx context.Context, lineage string, index uint64) error
	// durable confirms that what SQL committed so far survives a failover
	// of the database; it is called after the values a trim depends on
	// were read.
	durable func(ctx context.Context) (bool, string)
	// The parked events' table; tests replace it.
	park       func(ctx context.Context, rows []*cluster_model.ParkedEvent) error
	hasParked  func(ctx context.Context, repo string, repoID int64) (bool, error)
	minParked  func(ctx context.Context, lineage string) (int64, bool, error)
	listParked func(ctx context.Context) ([]*cluster_model.ParkedEvent, error)
	unpark     func(ctx context.Context, lineage string, index int64, want int) error
	noteParked func(ctx context.Context, lineage string, index int64, reason string) error

	// gate holds the dispatch at an index while a checkpoint reads
	// PostgreSQL's position (checkpoint.go).
	gate sync.RWMutex
	hold uint64
	held bool

	durability durabilityState
	// retryMu lets one retry of the parked events run at a time.
	retryMu sync.Mutex

	mu       sync.Mutex
	lastAck  map[string]uint64
	failures map[uint64]int
	// reasons is why an event failed, for its parked row.
	reasons map[uint64]string
	// seen is the furthest cursor read per lineage: one read behind it went
	// back (a database failover or restore), and is replayed from the log.
	seen      map[string]int64
	lastRetry time.Time
	lag       uint64
	// backupLag is how far the backup cursor lags the dispatch cursor when
	// that lag is beyond backupMaxLag and holds the trim; 0 otherwise.
	backupLag uint64
	// stalledOut holds the repositories whose event was parked for want of
	// projection, until one of their events is projected.
	stalledOut map[string]bool
	// stall is the head event waiting for its repository's projection.
	stall struct {
		index uint64
		repo  string
		since time.Time
		// waited is how long the repository waited for its own
		// projection; last is when it was last counted.
		waited time.Duration
		last   time.Time
	}
	// waitsBehind reports a repository whose projection waits behind
	// another repository's in the projector's serial pass; nil in tests.
	waitsBehind func(repo string) bool
}

// projectionStallBound is how long the dispatcher waits for a repository
// to be projected before it parks that event: a repository that never
// projects (a fetch that keeps failing, a native git process that hangs)
// would otherwise hold back the SQL of every repository and grow the event
// queue until pushes are refused.
var projectionStallBound = 10 * time.Minute

// parkedRetryEvery is how often the leader retries the parked events.
var parkedRetryEvery = 30 * time.Second

// ackEvery is how far the trim must move before it is proposed again.
var ackEvery uint64 = 256

const (
	batchMax    = 64
	maxAttempts = 5
	// backupMaxLag is the backup lag past which the held trim is alarmed:
	// the trim never passes the backup, whatever its lag.
	backupMaxLag = 1 << 20
)

func newConsumer(r *Replicator) *consumer {
	c := &consumer{
		r: r, work: func(ctx context.Context, evs []reftable.Event) error {
			return pushWorkWith(ctx, evs, r.table)
		},
		dbReady: func() bool { return db.DefaultContext != nil },
		cursor:  cluster_model.GetCursor, advance: cluster_model.AdvanceCursor, tx: batchTx, inTx: db.InTransaction,
		events:      r.table.Events,
		projectedAt: r.projector.Projected,
		waitsBehind: r.projector.WaitsBehind,
		lineageNow: func() string {
			_, id := r.table.Applied()
			return id.String()
		},
		durable: sqlDurable,
		park:    cluster_model.ParkEvents, hasParked: cluster_model.RepoHasParked, minParked: cluster_model.MinParkedIndex,
		listParked: cluster_model.ListParked, unpark: cluster_model.UnparkEntry, noteParked: cluster_model.NoteParkedFailure,
		lastAck: map[string]uint64{}, failures: map[uint64]int{}, reasons: map[uint64]string{}, seen: map[string]int64{},
		stalledOut: map[string]bool{},
	}
	c.ack = func(ctx context.Context, lineage string, index uint64) error {
		data, err := reftable.Encode(reftable.KindAck, &reftable.Ack{Identity: lineage, Index: index})
		if err != nil {
			return err
		}
		_, _, err = r.proposeCommand(ctx, data, nil)
		return err
	}
	return c
}

func cursorName(lineage string) string { return "dispatch@" + lineage }

// batchTx is the transaction of a batch: a transaction nested in its work
// runs in a savepoint, so a nested failure the work logs and swallows
// rolls back that statement alone, never the batch before it nor the
// cursor after it (#104).
//
// On PostgreSQL its statements wait at most batchLockTimeout for a lock
// another transaction holds: a request's long transaction on a row the
// work updates (a fork copying a repository holds its base's row, the
// deletion of a repository its rows) kept the single dispatcher waiting,
// and the SQL of every repository behind it (#177). The work then fails,
// even when it swallowed that failure in a nested transaction (lockWaits),
// the batch is tried again, and the group is parked after maxAttempts
// while the rest goes on; the parked group is retried on its own.
func batchTx(ctx context.Context, f func(ctx context.Context) error) error {
	return db.WithTx(db.WithNestedSavepoints(ctx), func(ctx context.Context) error {
		if setting.Database.Type.IsPostgreSQL() {
			if _, err := db.Exec(ctx, fmt.Sprintf("SET LOCAL lock_timeout = %d", batchLockTimeout.Milliseconds())); err != nil {
				return fmt.Errorf("bounding the batch's lock waits: %w", err)
			}
		}
		return f(ctx)
	})
}

// batchLockTimeout bounds a lock wait of a batch's statement on
// PostgreSQL: far above what the requests' short transactions hold a row,
// far below the minutes a fork's copy took. A test lowers it.
var batchLockTimeout = 5 * time.Second

// lockWaits collects the lock waits a batch's work gave up in a nested
// transaction. Its caller may log and swallow that failure, as pushUpdates
// does updateIssuesCommit's: rolled back to its savepoint, the write the
// wait lost would be missing from a batch that commits, where a statement
// waiting without bound would have made it (#177). The group fails
// instead, so it is tried again, then parked.
type lockWaits struct {
	mu    sync.Mutex
	first error
}

func (w *lockWaits) see(err error) {
	if !isLockTimeout(err) {
		return
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.first == nil {
		w.first = err
	}
}

// take returns, as the failure of the work that ran since the last take,
// the first lock wait it gave up.
func (w *lockWaits) take() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	err := w.first
	w.first = nil
	if err == nil {
		return nil
	}
	return fmt.Errorf("a nested statement gave up waiting for a lock, its write rolled back: %w", err)
}

// pgLockNotAvailable is the SQLSTATE of a statement PostgreSQL cancelled
// at lock_timeout, or that asked NOWAIT for a held lock.
const pgLockNotAvailable = "55P03"

// isLockTimeout reports a statement PostgreSQL cancelled for want of a
// lock. Code on the way may have formatted the error with %v.
func isLockTimeout(err error) bool {
	if pgErr, ok := errors.AsType[*pgconn.PgError](err); ok {
		return pgErr.Code == pgLockNotAvailable
	}
	return strings.Contains(err.Error(), "(SQLSTATE "+pgLockNotAvailable+")")
}

// errBatchTxEnded is the work of a batch that ended its transaction: what
// it wrote before is rolled back, what follows would commit statement by
// statement, so nothing may advance past it.
var errBatchTxEnded = errors.New("the batch transaction ended inside the work")

// stillInTx refuses to go on once the work ended the batch's transaction.
func (c *consumer) stillInTx(ctx context.Context) error {
	if c.inTx != nil && !c.inTx(ctx) {
		return errBatchTxEnded
	}
	return nil
}

// forget drops what the consumer remembers of the table's events, for a
// node whose table is emptied to rejoin under a new id (#41).
func (c *consumer) forget() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.lastAck = map[string]uint64{}
	c.seen = map[string]int64{}
	c.failures = map[uint64]int{}
	c.reasons = map[uint64]string{}
	c.stalledOut = map[string]bool{}
	c.stall.index, c.stall.repo = 0, ""
}

// projected reports whether the events of one entry are projected on this
// node. When the head entry has waited longer than projectionStallBound,
// its events are let through marked as failed, so dispatch parks them and
// the queue moves on.
func (c *consumer) projected(evs []reftable.Event) bool {
	var waiting string
	for _, ev := range evs {
		if c.projectedAt(ev.Repo) < ev.Token.Index {
			waiting = ev.Repo
			break
		}
	}
	idx := evs[0].Token.Index
	// Read outside c.mu: it takes the projector's lock.
	behind := waiting != "" && c.waitsBehind != nil && c.waitsBehind(waiting)
	c.mu.Lock()
	defer c.mu.Unlock()
	if waiting == "" {
		for _, ev := range evs {
			delete(c.stalledOut, ev.Repo)
		}
		return true
	}
	if c.stalledOut[waiting] {
		// The bound is the repository's, not each event's: the later
		// events of a repository whose head event was parked unprojected
		// are parked at once, not after a bound each (#45).
		c.failures[idx] = maxAttempts
		c.reasons[idx] = fmt.Sprintf("%s is still not projected; parked behind its stalled event", waiting)
		return true
	}
	now := time.Now()
	if c.stall.index != idx || c.stall.repo != waiting {
		c.stall.index, c.stall.repo, c.stall.since = idx, waiting, now
		c.stall.waited, c.stall.last = 0, now
		return false
	}
	// The bound is the time the repository waited for its own projection:
	// one behind another repository's long projection in the serial pass
	// is not stalled, and parking it, then each one behind it, a bound
	// each, parked healthy repositories (#137).
	if !behind {
		c.stall.waited += now.Sub(c.stall.last)
	}
	c.stall.last = now
	if c.stall.waited < projectionStallBound {
		return false
	}
	log.Error("cluster consumer: %s is still not projected at %d after %s; parking its event", waiting, idx, projectionStallBound)
	c.failures[idx] = maxAttempts
	c.reasons[idx] = fmt.Sprintf("not projected after %s", projectionStallBound)
	if c.stalledOut == nil {
		c.stalledOut = map[string]bool{}
	}
	c.stalledOut[waiting] = true
	return true
}

func (c *consumer) clearStall() {
	c.mu.Lock()
	c.stall.repo = ""
	c.mu.Unlock()
}

// Stalled reports the repository the dispatcher is waiting on, and since
// when; empty when it is not waiting.
func (c *consumer) Stalled() (string, time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.stall.repo == "" {
		return "", 0
	}
	return c.stall.repo, time.Since(c.stall.since)
}

// Durability is the last verdict on SQL's durability.
func (c *consumer) Durability() Durability { return c.durability.get() }

// BackupLag is how far the last PostgreSQL backup lags the cursor when it
// holds the trim beyond its bound; 0 otherwise.
func (c *consumer) BackupLag() uint64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.backupLag
}

func (c *consumer) run(ctx context.Context) {
	tick := time.NewTicker(time.Second)
	defer tick.Stop()
	checked := false
	for {
		select {
		case <-ctx.Done():
			return
		case <-c.r.wake:
		case <-tick.C:
		}
		if !checked && c.dbReady() {
			// At start: the durability the cluster relies on, said once
			// loudly; every trim checks it again.
			checked = true
			c.checkDurability(ctx)
		}
		if err := c.step(ctx); err != nil && ctx.Err() == nil {
			log.Warn("cluster consumer: %v", err)
		}
	}
}

// checkDurability runs the durability check, keeps its verdict and logs a
// change of it.
func (c *consumer) checkDurability(ctx context.Context) bool {
	ok, why := c.durable(ctx)
	prev := c.durability.get()
	c.durability.set(ok, why)
	if prev.Checked.IsZero() || prev.OK != ok || prev.Reason != why && !ok {
		if ok {
			log.Info("cluster consumer: SQL is durable across failovers: %s", why)
		} else {
			log.Error("cluster consumer: SQL is NOT durable across failovers, the event queue is not trimmed: %s", why)
		}
	}
	return ok
}

// Lag is how many events wait for the dispatcher, as of its last pass.
func (c *consumer) Lag() uint64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.lag
}

// step dispatches what it can: only on the leader, only with a database,
// and only once the writer hook, when there is one, promoted the leader.
func (c *consumer) step(ctx context.Context) error {
	eng := c.r.Engine()
	if eng == nil || eng.Status().Role != consensus.RoleLeader {
		c.notePass(nil)
		return nil
	}
	if !c.dbReady() || c.r.writerGate(eng) != nil {
		return nil
	}
	err := c.pass(ctx)
	if ctx.Err() == nil {
		c.notePass(err)
	}
	return err
}

// notePass counts the failed passes in a row on the replicator, which the
// autopilot reads to make the leader yield (#47); a pass that went
// through, or a node that does not lead, starts over.
func (c *consumer) notePass(err error) {
	if err != nil {
		c.r.consumerFails.Add(1)
		return
	}
	c.r.consumerFails.Store(0)
}

// readCursor reads a lineage's cursor and notices when it went back.
func (c *consumer) readCursor(ctx context.Context, lineage string) (*cluster_model.Cursor, error) {
	cur, err := c.cursor(ctx, cursorName(lineage))
	if err != nil {
		return nil, err
	}
	pos := cur.Position
	if cur.Lineage != lineage {
		pos = 0
	}
	c.mu.Lock()
	seen, trimmed := c.seen[lineage], c.lastAck[lineage]
	c.seen[lineage] = pos
	c.mu.Unlock()
	if pos < seen {
		cursorRegressionsTotal.Inc()
		log.Warn("cluster consumer: the cursor of %s went back from %d to %d (a database failover or restore): the events past it are replayed from the log", lineage, seen, pos)
		if uint64(pos) < trimmed {
			log.Error("cluster consumer: the cursor of %s went back to %d, behind the trim at %d: the trim was confirmed durable, so the database lost a confirmed commit; run `forgejo cluster reconcile`", lineage, pos, trimmed)
		}
	}
	return cur, nil
}

// pass is one pass of the dispatcher, the leader's.
func (c *consumer) pass(ctx context.Context) error {
	events := c.events()
	cursors := map[string]*cluster_model.Cursor{}
	var pending []reftable.Event
	limit, held := c.limit()
	for _, ev := range events {
		if held && ev.Token.Index > limit {
			// A checkpoint is reading PostgreSQL's position: what derives
			// from later entries waits, so it lands after that position.
			break
		}
		lin := ev.Token.Identity().String()
		cur, ok := cursors[lin]
		if !ok {
			var err error
			if cur, err = c.readCursor(ctx, lin); err != nil {
				return err
			}
			cursors[lin] = cur
		}
		if cur.Lineage == "" || ev.Token.Index > uint64(cur.Position) {
			pending = append(pending, ev)
		}
	}
	c.mu.Lock()
	c.lag = uint64(len(pending))
	c.mu.Unlock()

	for len(pending) > 0 && ctx.Err() == nil {
		// A batch is consecutive events of one lineage, each one projected
		// on this node, the leader, before its SQL is written (R8-27). It
		// never splits the events of one entry: the cursor is an entry's
		// index, and the events of an index left behind would be skipped.
		lin := pending[0].Token.Identity().String()
		n := 0
		for n < len(pending) && pending[n].Token.Identity().String() == lin {
			end := n + 1
			for end < len(pending) && pending[end].Token == pending[n].Token {
				end++
			}
			if n > 0 && end > batchMax {
				break
			}
			if !c.projected(pending[n:end]) {
				break
			}
			n = end
		}
		if n == 0 {
			// The projector has not caught up; the next pass retries. The
			// parked events and the trim do not wait for it (#45).
			break
		}
		c.clearStall()
		batch := pending[:n]
		if err := c.dispatch(ctx, lin, cursors[lin], batch); err != nil {
			return err
		}
		pending = pending[n:]
	}
	c.mu.Lock()
	due := time.Since(c.lastRetry) >= parkedRetryEvery
	if due {
		c.lastRetry = time.Now()
	}
	c.mu.Unlock()
	if due {
		if _, err := c.retryParked(ctx); err != nil {
			log.Warn("cluster consumer: retrying the parked events: %v", err)
		}
	}
	for lin := range cursors {
		c.maybeAck(ctx, lin)
	}
	return nil
}

// dispatch runs a batch's work and moves the lineage's cursor past it in
// ONE SQL transaction (R7-29, R8-25): the work commits with its cursor, or
// neither does, and a replay after a failover finds the cursor where the
// committed work left it. A group that kept failing is parked after
// maxAttempts, in the same transaction, so it cannot stall every
// repository behind it and is never lost; the later groups of a repository
// with a parked event are parked behind it, so its work keeps its order.
func (c *consumer) dispatch(ctx context.Context, lineage string, cur *cluster_model.Cursor, batch []reftable.Event) error {
	next := cluster_model.Cursor{Lineage: lineage, Position: int64(batch[len(batch)-1].Token.Index)}
	// Side effects outside SQL (pull request tests, webhook deliveries) run
	// only once this transaction commits: a rolled-back attempt is replayed,
	// and its effects would otherwise happen twice.
	ctx, effects := clusterhook.WithAfterCommit(ctx)
	waits := &lockWaits{}
	ctx = db.OnSavepointRollback(ctx, waits.see)
	// seq is each event's place among the events of its entry.
	seq := make([]int, len(batch))
	for i := 1; i < len(batch); i++ {
		if batch[i].Token == batch[i-1].Token {
			seq[i] = seq[i-1] + 1
		}
	}
	parked := 0
	// ran says the callback reached its end: only then can an error come
	// from the COMMIT, whose outcome committedAnyway settles. An error of
	// the work rolled back, whatever another dispatcher committed since.
	ran := false
	err := c.tx(ctx, func(ctx context.Context) error {
		parked, ran = 0, false
		start := 0
		for i := 1; i <= len(batch); i++ {
			// One group per pusher too: the work attributes its branch rows to
			// the group's first pusher, and a group parked after maxAttempts
			// must not take another user's push down with it.
			// And one group per entry: two pushes of a ref grouped together
			// insert its new branch or tag twice, and a creation and its
			// deletion fail on the row the creation never wrote (#76, #44).
			if i < len(batch) && batch[i].Token == batch[start].Token && batch[i].Repo == batch[start].Repo && batch[i].Kind == batch[start].Kind &&
				batch[i].Trigger == batch[start].Trigger && batch[i].PusherID == batch[start].PusherID {
				continue
			}
			group, groupSeq := batch[start:i], seq[start:i]
			start = i
			if group[0].Trigger == TriggerImport {
				// An import describes refs that already existed: no push.
				continue
			}
			idx := group[0].Token.Index
			c.mu.Lock()
			attempts, reason := c.failures[idx], c.reasons[idx]
			c.mu.Unlock()
			behind := false
			if attempts < maxAttempts && c.hasParked != nil {
				var err error
				if behind, err = c.hasParked(ctx, group[0].Repo, group[0].RepoID); err != nil {
					return err
				}
				if behind {
					reason = "behind an earlier parked event of the repository"
				}
			}
			if attempts >= maxAttempts || behind {
				if attempts >= maxAttempts && reason == "" {
					reason = fmt.Sprintf("failed %d attempts", attempts)
				}
				if err := c.parkGroup(ctx, lineage, group, groupSeq, reason, attempts); err != nil {
					return err
				}
				parked += len(group)
				continue
			}
			err := c.work(ctx, group)
			if err == nil {
				err = waits.take()
			}
			if err == nil {
				err = c.stillInTx(ctx)
			}
			if err != nil {
				c.mu.Lock()
				c.failures[idx]++
				attempts = c.failures[idx]
				c.reasons[idx] = err.Error()
				c.mu.Unlock()
				return fmt.Errorf("event %s at %d: %w (attempt %d)", group[0].Repo, idx, err, attempts)
			}
		}
		if err := c.stillInTx(ctx); err != nil {
			return err
		}
		if err := c.advance(ctx, cursorName(lineage), *cur, next); err != nil {
			return err
		}
		ran = true
		return nil
	})
	if err != nil {
		if errors.Is(err, cluster_model.ErrCursorMoved) {
			return nil // another dispatcher moved it; its work stands, ours rolled back
		}
		if !ran || !c.committedAnyway(ctx, lineage, next, err) {
			dispatchFailuresTotal.Inc()
			return err
		}
	}
	effects.Run()
	dispatchedTotal.Add(float64(len(batch) - parked))
	parkedTotal.Add(float64(parked))
	cur.Lineage, cur.Position = next.Lineage, next.Position
	c.mu.Lock()
	for _, ev := range batch {
		delete(c.failures, ev.Token.Index)
		delete(c.reasons, ev.Token.Index)
	}
	if c.seen[lineage] < next.Position {
		c.seen[lineage] = next.Position
	}
	c.mu.Unlock()
	return nil
}

// committedAnyway tells a batch whose COMMIT failed with an unknown
// outcome (the connection cut after the database committed it, a
// synchronous standby's wait ended by a failover) from one that rolled
// back: the cursor is read again on a new connection, and a cursor at
// next means the work committed, so its effects run once, here, since no
// replay will collect them again (#103).
func (c *consumer) committedAnyway(ctx context.Context, lineage string, next cluster_model.Cursor, cause error) bool {
	if c.cursor == nil {
		return false
	}
	cur, err := c.cursor(ctx, cursorName(lineage))
	if err != nil || cur.Lineage != next.Lineage || cur.Position != next.Position {
		return false
	}
	log.Warn("cluster consumer: the commit of the batch up to %d of %s returned %v, but the cursor reads %d: it committed, its effects run", next.Position, lineage, cause, next.Position)
	return true
}

// parkedGone tells a retried parked entry whose COMMIT failed with an
// unknown outcome from one that rolled back: its rows are gone only if the
// transaction that removed them, with the work, committed (#103).
func (c *consumer) parkedGone(ctx context.Context, lineage string, index int64, cause error) bool {
	if c.listParked == nil {
		return false
	}
	rows, err := c.listParked(ctx)
	if err != nil {
		return false
	}
	for _, row := range rows {
		if row.Lineage == lineage && row.LogIndex == index {
			return false
		}
	}
	log.Warn("cluster consumer: the commit of the parked entry %d of %s returned %v, but its rows are gone: it committed, its effects run", index, lineage, cause)
	return true
}

// parkGroup writes the parked rows of a group, in the dispatch's
// transaction.
func (c *consumer) parkGroup(ctx context.Context, lineage string, group []reftable.Event, seq []int, reason string, attempts int) error {
	rows := make([]*cluster_model.ParkedEvent, 0, len(group))
	for i, ev := range group {
		b, err := json.Marshal(ev)
		if err != nil {
			return err
		}
		rows = append(rows, &cluster_model.ParkedEvent{
			Lineage: lineage, LogIndex: int64(ev.Token.Index), Seq: seq[i], Repo: ev.Repo, RepoID: ev.RepoID,
			Event: string(b), Reason: reason, Attempts: attempts,
		})
	}
	if err := c.park(ctx, rows); err != nil {
		return fmt.Errorf("parking the event of %s at %d: %w", group[0].Repo, group[0].Token.Index, err)
	}
	log.Error("cluster consumer: parked the event of %s at %d (%s); it is retried every %s and by `forgejo cluster reconcile --apply`", group[0].Repo, group[0].Token.Index, reason, parkedRetryEvery)
	return nil
}

// retryParked runs the work of the parked events, entry by entry, in
// order; an entry's work and the removal of its rows commit together, so
// it is applied exactly once. A repository whose entry fails or is not
// projected yet keeps its later entries parked, in order.
func (c *consumer) retryParked(ctx context.Context) (int, error) {
	c.retryMu.Lock()
	defer c.retryMu.Unlock()
	rows, err := c.listParked(ctx)
	if err != nil {
		return 0, err
	}
	// A repository whose entry stays parked blocks its later entries: by
	// id when known, so a rename does not let them pass and a new
	// repository under the old name is not held (#133).
	blocked := map[parkedRepo]bool{}
	applied := 0
	now := ""
	if c.lineageNow != nil {
		now = c.lineageNow()
	}
	for len(rows) > 0 && ctx.Err() == nil {
		n := 1
		for n < len(rows) && rows[n].Lineage == rows[0].Lineage && rows[n].LogIndex == rows[0].LogIndex {
			n++
		}
		entry := rows[:n]
		rows = rows[n:]
		lineage, index, repo := entry[0].Lineage, entry[0].LogIndex, entry[0].Repo
		if blocked[parkedRepoOf(entry[0])] {
			continue
		}
		evs := make([]reftable.Event, 0, len(entry))
		var derr error
		for _, row := range entry {
			var ev reftable.Event
			if derr = json.Unmarshal([]byte(row.Event), &ev); derr != nil {
				break
			}
			evs = append(evs, ev)
		}
		key := parkedRepoOf(entry[0])
		if derr == nil && key.id == 0 && len(evs) > 0 && evs[0].RepoID != 0 {
			// A row parked before its id had a column: the event has it.
			key = parkedRepo{id: evs[0].RepoID}
			if blocked[key] {
				continue
			}
		}
		if derr != nil {
			blocked[key] = true
			_ = c.noteParked(ctx, lineage, index, "decoding the parked event: "+derr.Error())
			continue
		}
		if lineage == now && c.projectedAt != nil && c.projectedAt(repo) < uint64(index) {
			blocked[key] = true
			continue
		}
		wctx, effects := clusterhook.WithAfterCommit(ctx)
		waits := &lockWaits{}
		wctx = db.OnSavepointRollback(wctx, waits.see)
		ran := false
		err := c.tx(wctx, func(ctx context.Context) error {
			ran = false
			if err := c.work(ctx, evs); err != nil {
				return err
			}
			if err := waits.take(); err != nil {
				return err
			}
			if err := c.stillInTx(ctx); err != nil {
				return err
			}
			// Another retrier that applied the entry first removed its
			// rows: this transaction, work included, rolls back.
			if err := c.unpark(ctx, lineage, index, len(entry)); err != nil {
				return err
			}
			ran = true
			return nil
		})
		if errors.Is(err, cluster_model.ErrParkedGone) {
			continue
		}
		// Only an error after the callback's end may hide a COMMIT that
		// landed: one of the work rolled back, even when another retrier
		// removed the rows meanwhile.
		if err != nil && ran && c.parkedGone(ctx, lineage, index, err) {
			err = nil
		}
		if err != nil {
			blocked[key] = true
			if nerr := c.noteParked(ctx, lineage, index, err.Error()); nerr != nil {
				log.Warn("cluster consumer: noting the failed retry of %s at %d: %v", repo, index, nerr)
			}
			continue
		}
		effects.Run()
		applied += len(entry)
		unparkedTotal.Add(float64(len(entry)))
		log.Info("cluster consumer: applied the parked event of %s at %d", repo, index)
	}
	return applied, nil
}

// parkedRepo is the repository a parked row holds back: its id when the
// row has one, else its key.
type parkedRepo struct {
	id  int64
	key string
}

func parkedRepoOf(row *cluster_model.ParkedEvent) parkedRepo {
	if row.RepoID != 0 {
		return parkedRepo{id: row.RepoID}
	}
	return parkedRepo{key: row.Repo}
}

// RetryParked retries the parked events now, as `forgejo cluster reconcile
// --apply` asks, and reports how many were applied and how many remain.
func (r *Replicator) RetryParked(ctx context.Context) (applied int, remaining int64, err error) {
	if r.consumer == nil || db.DefaultContext == nil {
		return 0, 0, errors.New("database not ready")
	}
	applied, err = r.consumer.retryParked(ctx)
	if err != nil {
		return applied, 0, err
	}
	remaining, err = cluster_model.CountParked(ctx)
	return applied, remaining, err
}

// maybeAck trims the table's events behind what SQL holds durably: the
// cursor READ BACK from SQL, never at or past a parked event, lagging by
// the configured margin, never past the cursor of the last PostgreSQL
// backup whatever its lag (R8-23, R9-14), and only
// once the durability check, run AFTER those reads, confirms that a
// failover of the database cannot bring them back.
func (c *consumer) maybeAck(ctx context.Context, lineage string) {
	readBack, err := c.cursor(ctx, cursorName(lineage))
	if err != nil || readBack.Lineage != lineage {
		return
	}
	bound := uint64(readBack.Position)
	if p, ok, err := c.minParked(ctx, lineage); err != nil {
		return
	} else if ok {
		if p <= 0 {
			return
		}
		bound = min(bound, uint64(p)-1)
	}
	if bound <= setting.Cluster.AckMargin {
		return
	}
	target := bound - setting.Cluster.AckMargin
	var backupLag uint64
	if b, err := c.cursor(ctx, cluster_model.CursorBackup); err != nil {
		return
	} else if b.Lineage == lineage {
		// Never past the backup, whatever its lag: a PostgreSQL restored to
		// it replays what follows from the log. A backup that lags too far
		// holds the trim, loudly, until the next checkpoint closes.
		pos := uint64(readBack.Position)
		if bp := uint64(b.Position); bp < target {
			target = bp
			if pos-bp >= backupMaxLag {
				backupLag = pos - bp
			}
		}
	}
	c.mu.Lock()
	prevLag := c.backupLag
	c.backupLag = backupLag
	c.mu.Unlock()
	if backupLag > 0 {
		backupTrimHeldTotal.Inc()
		if prevLag == 0 {
			log.Error("cluster consumer: the PostgreSQL backup lags %d entries behind the cursor; the event queue is not trimmed past it until a checkpoint closes", backupLag)
		}
	}
	c.mu.Lock()
	last := c.lastAck[lineage]
	c.mu.Unlock()
	if target < last+ackEvery {
		return
	}
	if !c.checkDurability(ctx) {
		trimHeldTotal.Inc()
		return
	}
	if err := c.ack(ctx, lineage, target); err != nil {
		log.Warn("cluster consumer: ack at %d: %v", target, err)
		return
	}
	c.mu.Lock()
	c.lastAck[lineage] = target
	c.mu.Unlock()
}

// waitCursor waits until the dispatcher's cursor of a token's lineage has
// passed it (R8-24).
func (c *consumer) waitCursor(ctx context.Context, tok consensus.Token) error {
	if !c.dbReady() {
		return errors.New("database not ready")
	}
	name := cursorName(tok.Identity().String())
	for {
		cur, err := c.cursor(ctx, name)
		if err == nil && uint64(cur.Position) >= tok.Index {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(100 * time.Millisecond):
		}
	}
}

// pushWork is the post-receive work of a group of events of one repository:
// branch rows, then push actions, webhooks, CI triggers and pull request
// updates, as the node-local post-receive hook did before the cluster. It
// knows no rename nor deletion: the consumer uses pushWorkWith and the
// table.
func pushWork(ctx context.Context, evs []reftable.Event) error {
	return pushWorkWith(ctx, evs, nil)
}

// renamedFromFunc is the table's RenamedFromBefore: the newest rename into
// repo before index.
type renamedFromFunc func(repo string, before uint64) (from string, index uint64, ok bool)

// lifecycleTable is what the post-receive work reads of the table: the
// renames and deletions that happened to a repository after an event.
type lifecycleTable interface {
	RenamedFromBefore(repo string, before uint64) (string, uint64, bool)
	RenamedAfter(from string, after uint64) (string, uint64, bool)
	Deleted(repo string) (uint64, bool)
}

func pushWorkWith(ctx context.Context, evs []reftable.Event, tbl lifecycleTable) error {
	var renamedFrom renamedFromFunc
	if tbl != nil {
		renamedFrom = tbl.RenamedFromBefore
	}
	first := evs[0]
	if first.Kind != reftable.EventRef || first.IsWiki {
		// Repository deletion and rename change SQL in the request that
		// asked for them; wikis have no post-receive work.
		return nil
	}
	if first.PusherID == 0 {
		// No pusher: Forgejo wrote the ref itself, outside receive-pack (a
		// tag or a branch deletion through the API, a branch rename, a pull
		// mirror's fetch). The request that did it writes its rows and sends
		// its notifications, as it does without the cluster; post-receive
		// work here would repeat them, and fails on the missing user.
		// dispatch groups events by pusher, so the whole group is such.
		return nil
	}
	repo, err := lookupRepo(ctx, first)
	if err != nil {
		if repo_model.IsErrRepoNotExist(err) && (deletedAfter(tbl, first) || idSuperseded(ctx, tbl, first)) {
			// The repository was deleted after the push: its rows are gone
			// with it, and the work has nothing left to land on. Kept, it
			// would fail at every retry, parked for good, and hold back
			// the trim of the whole event queue.
			log.Info("cluster consumer: %s was deleted after the event at %d; its post-receive work is dropped", first.Repo, first.Token.Index)
			return nil
		}
		return err
	}
	if first.RepoID > 0 && !eventLeadsTo(first, clusterhook.RepoKey(repo.OwnerName, repo.Name, first.IsWiki), renamedFrom) {
		// The event's ID now names another repository: the row it was
		// written for is gone (a PostgreSQL failover that lost its tail, a
		// recovery to an earlier position) and the sequence handed its ID
		// out again. Its branch rows, webhooks and CI must not land there.
		return fmt.Errorf("repository ID %d is %s/%s, which the event's %s does not lead to: the ID was reused; run `forgejo cluster reconcile`", first.RepoID, repo.OwnerName, repo.Name, first.Repo)
	}
	u, err := getPusher(ctx, first.PusherID)
	if err != nil && !user_model.IsErrUserNotExist(err) {
		// Retried, never passed over: the lookup PushUpdatesNow repeats
		// checks only that the account exists, not that it pushed.
		return fmt.Errorf("looking up the pusher %d of %s at %d: %w", first.PusherID, first.Repo, first.Token.Index, err)
	}
	if err == nil && pusherReissued(ctx, u, first) {
		// The same loss reissues user IDs: another account now holds the
		// pusher's ID, and must not be credited with the push.
		return fmt.Errorf("pusher ID %d is %s, not the account that pushed at %d: the ID was reused; run `forgejo cluster reconcile`", first.PusherID, u.Name, first.Token.Index)
	}
	// A pusher whose account was deleted since: the branch rows are still
	// written, but the notifications that need the account are not, as
	// upstream Forgejo drops that work. Retried, it would fail for good,
	// parked, and hold back the trim of the whole queue (#95).
	pusherGone := first.PusherID > 0 && user_model.IsErrUserNotExist(err)
	var gitRepo *git.Repository
	openGit := func() (*git.Repository, error) {
		if gitRepo == nil {
			var err error
			if gitRepo, err = gitrepo.OpenRepository(ctx, repo); err != nil {
				return nil, err
			}
		}
		return gitRepo, nil
	}
	defer func() {
		if gitRepo != nil {
			gitRepo.Close()
		}
	}()
	var updates []*repo_module.PushUpdateOptions
	var branches []string
	for _, ev := range evs {
		ref := git.RefName(ev.Ref)
		if !ref.IsBranch() && !ref.IsTag() {
			continue
		}
		pusherName := ""
		if u, err := user_model.GetUserByID(ctx, ev.PusherID); err == nil {
			pusherName = u.Name
		}
		opt := &repo_module.PushUpdateOptions{
			RefFullName: ref, OldCommitID: ev.Old, NewCommitID: ev.New,
			PusherID: ev.PusherID, PusherName: pusherName,
			RepoUserName: repo.OwnerName, RepoName: repo.Name, TimeNano: ev.Time,
		}
		if opt.OldCommitID == "" {
			opt.OldCommitID = git.ObjectFormatFromName(repo.ObjectFormatName).EmptyObjectID().String()
		}
		if opt.NewCommitID == "" {
			opt.NewCommitID = git.ObjectFormatFromName(repo.ObjectFormatName).EmptyObjectID().String()
		}
		if opt.IsNewRef() && opt.IsDelRef() {
			continue
		}
		if ref.IsTag() {
			gone, err := tagGone(ctx, repo, opt, openGit)
			if err != nil {
				return err
			}
			if gone {
				// Created then deleted before this work ran: there is no
				// tag to describe, or no release row to remove, and the
				// work would fail at every retry, parked for good (#44).
				log.Info("cluster consumer: the tag %s of %s at %d is gone; its post-receive work is dropped", ref.TagName(), first.Repo, ev.Token.Index)
				continue
			}
		}
		updates = append(updates, opt)
		if ref.IsBranch() {
			if opt.IsDelRef() {
				// A branch whose creation was never written in SQL (created
				// and deleted before its work ran) has nothing to mark
				// deleted (#44).
				if err := git_model.AddDeletedBranch(ctx, repo.ID, ref.BranchName(), ev.PusherID); err != nil && !git_model.IsErrBranchNotExist(err) {
					return err
				}
			} else if !slices.Contains(branches, ref.BranchName()) {
				branches = append(branches, ref.BranchName())
			}
		}
	}
	if len(branches) > 0 {
		gitRepo, err := openGit()
		if err != nil {
			return err
		}
		if err := repo_service.SyncBranchesToDB(ctx, repo.ID, first.PusherID, branches, gitRepo.GetBranchCommit); err != nil {
			return err
		}
	}
	if len(updates) == 0 {
		return nil
	}
	if pusherGone {
		log.Warn("cluster consumer: the pusher %d of %s at %d is gone; the notifications of its push are dropped", first.PusherID, first.Repo, first.Token.Index)
		return nil
	}
	return repo_service.PushUpdatesNow(clusterhook.WithCommand(ctx, first.Token.String()), updates)
}

// tagGone reports whether a tag update has nothing left to land on: a new
// tag git no longer holds, or a deleted tag without a release row.
func tagGone(ctx context.Context, repo *repo_model.Repository, opt *repo_module.PushUpdateOptions, openGit func() (*git.Repository, error)) (bool, error) {
	name := opt.RefFullName.TagName()
	if opt.IsDelRef() {
		_, err := repo_model.GetRelease(ctx, repo.ID, name)
		if repo_model.IsErrReleaseNotExist(err) {
			return true, nil
		}
		return false, err
	}
	gitRepo, err := openGit()
	if err != nil {
		return false, err
	}
	return !gitRepo.IsTagExist(name), nil
}

// getPusher looks up the account holding a pusher's ID; tests replace it.
var getPusher = user_model.GetUserByID

// pusherClockSkew bounds the clock difference between the node that
// proposed a push and the one that created its pusher's account.
const pusherClockSkew = 5 * time.Minute

// pusherReissued reports whether u, the account holding the event's pusher
// ID now, is not the one that pushed: its ID was handed out again.
//
// From payload version 4 the event names the pusher, and the name decides,
// with no clock: the same name, or a name that redirects to u (a rename
// since), is the pusher; any other name is not. An event written before
// carries no name and falls back to the account's creation time against
// the push's, two nodes' clocks: a skew beyond pusherClockSkew between
// them can still park a legitimate push.
func pusherReissued(ctx context.Context, u *user_model.User, ev reftable.Event) bool {
	if ev.PusherName != "" {
		if strings.EqualFold(u.Name, ev.PusherName) {
			return false
		}
		r, err := user_model.GetUserRedirect(ctx, ev.PusherName)
		return err != nil || r.RedirectUserID != u.ID
	}
	if ev.Time <= 0 || u.CreatedUnix <= 0 {
		return false
	}
	return time.Unix(int64(u.CreatedUnix), 0).After(time.Unix(0, ev.Time).Add(pusherClockSkew))
}

// eventLeadsTo reports whether the repository an event was written for is
// the one now keyed key: the same key, or a key it was renamed to after the
// event, following the table's renames backwards. Each step goes back in
// the log, to the newest rename into the key before the previous step, so
// a rename and its undo cannot loop, and a key another rename took again
// later still leads to the earlier one (#138).
func eventLeadsTo(ev reftable.Event, key string, renamedFrom renamedFromFunc) bool {
	at := uint64(1<<64 - 1)
	for key != ev.Repo {
		if renamedFrom == nil {
			return false
		}
		from, idx, ok := renamedFrom(key, at)
		if !ok || idx <= ev.Token.Index || idx >= at {
			return false
		}
		key, at = from, idx
	}
	return true
}

// deletedAfter reports whether the table deleted the repository an event
// was written for after the event, following its renames forward. A
// repository still being created (its row not committed yet) has no such
// deletion, and neither has a key written again since: only those events
// are dropped whose repository the cluster saw go.
func deletedAfter(tbl lifecycleTable, ev reftable.Event) bool {
	if tbl == nil {
		return false
	}
	key, at := ev.Repo, ev.Token.Index
	for range 64 {
		if idx, deleted := tbl.Deleted(key); deleted && idx > at {
			return true
		}
		to, idx, ok := tbl.RenamedAfter(key, at)
		if !ok {
			return false
		}
		key, at = to, idx
	}
	return false
}

// idSuperseded reports whether the repository an event's ID names is gone
// for good: the key it leads to, following the table's renames forward,
// is now held by a row with another ID. A repository deleted and created
// again under its name clears the table's deletion mark at its first
// write, so deletedAfter no longer sees it (#78). IDs are not handed out
// again short of a database loss, which eventLeadsTo reports.
func idSuperseded(ctx context.Context, tbl lifecycleTable, ev reftable.Event) bool {
	if ev.RepoID <= 0 {
		return false
	}
	key, at := ev.Repo, ev.Token.Index
	for range 64 {
		if tbl == nil {
			break
		}
		to, idx, ok := tbl.RenamedAfter(key, at)
		if !ok {
			break
		}
		key, at = to, idx
	}
	owner, name, _ := strings.Cut(strings.TrimSuffix(key, ".git"), "/")
	r, err := repo_model.GetRepositoryByOwnerAndName(ctx, owner, name)
	return err == nil && r.ID != ev.RepoID
}

func lookupRepo(ctx context.Context, ev reftable.Event) (*repo_model.Repository, error) {
	if ev.RepoID > 0 {
		return repo_model.GetRepositoryByID(ctx, ev.RepoID)
	}
	owner, name, _ := strings.Cut(strings.TrimSuffix(ev.Repo, ".git"), "/")
	return repo_model.GetRepositoryByOwnerAndName(ctx, owner, name)
}

// repoKnownInDB answers the object channel's question: does Forgejo know
// this repository?
func repoKnownInDB(ctx context.Context, key string) (bool, error) {
	if db.DefaultContext == nil {
		return false, errors.New("database not ready")
	}
	owner, name, _ := strings.Cut(strings.TrimSuffix(key, ".git"), "/")
	name = strings.TrimSuffix(name, ".wiki")
	_, err := repo_model.GetRepositoryByOwnerAndName(ctx, owner, name)
	if repo_model.IsErrRepoNotExist(err) {
		return false, nil
	}
	return err == nil, err
}
