// Copyright 2026 The Forgejo Authors. All rights reserved.
// SPDX-License-Identifier: GPL-3.0-or-later

package cluster

import (
	"context"
	"errors"
	"fmt"
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
	"forgejo.org/modules/log"
	repo_module "forgejo.org/modules/repository"
	"forgejo.org/modules/setting"
	"forgejo.org/services/cluster/consensus"
	"forgejo.org/services/cluster/reftable"
	repo_service "forgejo.org/services/repository"
)

// consumer derives SQL from the log (ADR 0152 D19, R7-29). ONE dispatcher,
// on the current leader, runs the post-receive work of each event, keyed by
// its log position, and commits a per-lineage cursor as a compare-and-set.
// It rebuilds its queue from the table's events past the cursor whenever it
// gains leadership, so a failover loses nothing.
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
	// tx runs f in one SQL transaction; tests replace it.
	tx func(ctx context.Context, f func(ctx context.Context) error) error

	// gate holds the dispatch at an index while a checkpoint reads
	// PostgreSQL's position (checkpoint.go).
	gate sync.RWMutex
	hold uint64
	held bool

	mu       sync.Mutex
	lastAck  map[string]uint64
	failures map[uint64]int
	lag      uint64
	// stall is the head event waiting for its repository's projection.
	stall struct {
		index uint64
		repo  string
		since time.Time
	}
}

// projectionStallBound is how long the dispatcher waits for a repository
// to be projected before it gives up on that event: a repository that never
// projects (a fetch that keeps failing, a native git process that hangs)
// would otherwise hold back the SQL of every repository and grow the event
// queue until pushes are refused.
var projectionStallBound = 10 * time.Minute

const (
	batchMax     = 64
	maxAttempts  = 5
	ackEvery     = 256
	backupMaxLag = 1 << 20
)

func newConsumer(r *Replicator) *consumer {
	return &consumer{
		r: r, work: func(ctx context.Context, evs []reftable.Event) error {
			return pushWorkWith(ctx, evs, r.table.RenamedFrom)
		},
		dbReady: func() bool { return db.DefaultContext != nil },
		cursor:  cluster_model.GetCursor, advance: cluster_model.AdvanceCursor, tx: db.WithTx,
		lastAck: map[string]uint64{}, failures: map[uint64]int{},
	}
}

func cursorName(lineage string) string { return "dispatch@" + lineage }

// projected reports whether the events of one entry are projected on this
// node. When the head entry has waited longer than projectionStallBound,
// its events are let through marked as failed, so dispatch skips them with
// the reconcile hint and the queue moves on.
func (c *consumer) projected(evs []reftable.Event) bool {
	var waiting string
	for _, ev := range evs {
		if c.r.projector.Projected(ev.Repo) < ev.Token.Index {
			waiting = ev.Repo
			break
		}
	}
	if waiting == "" {
		return true
	}
	idx := evs[0].Token.Index
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.stall.index != idx || c.stall.repo != waiting {
		c.stall.index, c.stall.repo, c.stall.since = idx, waiting, time.Now()
		return false
	}
	if time.Since(c.stall.since) < projectionStallBound {
		return false
	}
	log.Error("cluster consumer: %s is still not projected at %d after %s; skipping its event, run `forgejo cluster reconcile`", waiting, idx, projectionStallBound)
	c.failures[idx] = maxAttempts
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

func (c *consumer) run(ctx context.Context) {
	tick := time.NewTicker(time.Second)
	defer tick.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-c.r.wake:
		case <-tick.C:
		}
		if err := c.step(ctx); err != nil && ctx.Err() == nil {
			log.Warn("cluster consumer: %v", err)
		}
	}
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
	if eng == nil || eng.Status().Role != consensus.RoleLeader || !c.dbReady() || c.r.writerGate(eng) != nil {
		return nil
	}
	events := c.r.table.Events()
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
			if cur, err = c.cursor(ctx, cursorName(lin)); err != nil {
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
			return nil // the projector has not caught up; the next pass retries
		}
		c.clearStall()
		batch := pending[:n]
		if err := c.dispatch(ctx, lin, cursors[lin], batch); err != nil {
			return err
		}
		pending = pending[n:]
	}
	for lin := range cursors {
		c.maybeAck(ctx, lin)
	}
	return nil
}

// dispatch runs a batch's work and moves the lineage's cursor past it in
// ONE SQL transaction (R7-29, R8-25): the work commits with its cursor, or
// neither does, and a replay after a failover finds the cursor where the
// committed work left it. A group that kept failing is skipped after
// maxAttempts so it cannot stall every repository behind it.
func (c *consumer) dispatch(ctx context.Context, lineage string, cur *cluster_model.Cursor, batch []reftable.Event) error {
	next := cluster_model.Cursor{Lineage: lineage, Position: int64(batch[len(batch)-1].Token.Index)}
	// Side effects outside SQL (pull request tests, webhook deliveries) run
	// only once this transaction commits: a rolled-back attempt is replayed,
	// and its effects would otherwise happen twice.
	ctx, effects := clusterhook.WithAfterCommit(ctx)
	err := c.tx(ctx, func(ctx context.Context) error {
		start := 0
		for i := 1; i <= len(batch); i++ {
			// One group per pusher too: the work attributes its branch rows to
			// the group's first pusher, and a group skipped after maxAttempts
			// must not take another user's push down with it.
			if i < len(batch) && batch[i].Repo == batch[start].Repo && batch[i].Kind == batch[start].Kind &&
				batch[i].Trigger == batch[start].Trigger && batch[i].PusherID == batch[start].PusherID {
				continue
			}
			group := batch[start:i]
			start = i
			if group[0].Trigger == TriggerImport {
				// An import describes refs that already existed: no push.
				continue
			}
			idx := group[0].Token.Index
			c.mu.Lock()
			attempts := c.failures[idx]
			c.mu.Unlock()
			if attempts >= maxAttempts {
				log.Error("cluster consumer: skipping the event of %s at %d after %d failed attempts; run `forgejo cluster reconcile`", group[0].Repo, idx, attempts)
				continue
			}
			if err := c.work(ctx, group); err != nil {
				c.mu.Lock()
				c.failures[idx]++
				attempts = c.failures[idx]
				c.mu.Unlock()
				return fmt.Errorf("event %s at %d: %w (attempt %d)", group[0].Repo, idx, err, attempts)
			}
		}
		return c.advance(ctx, cursorName(lineage), *cur, next)
	})
	if err != nil {
		if errors.Is(err, cluster_model.ErrCursorMoved) {
			return nil // another dispatcher moved it; its work stands, ours rolled back
		}
		dispatchFailuresTotal.Inc()
		return err
	}
	effects.Run()
	dispatchedTotal.Add(float64(len(batch)))
	cur.Lineage, cur.Position = next.Lineage, next.Position
	c.mu.Lock()
	for _, ev := range batch {
		delete(c.failures, ev.Token.Index)
	}
	c.mu.Unlock()
	return nil
}

// maybeAck trims the table's events behind the cursor READ BACK from SQL,
// lagging it by the configured margin, and never past the cursor of the
// last PostgreSQL backup unless that backup lags beyond its bound (R8-23,
// R9-14).
func (c *consumer) maybeAck(ctx context.Context, lineage string) {
	readBack, err := c.cursor(ctx, cursorName(lineage))
	if err != nil || readBack.Lineage != lineage {
		return
	}
	pos := uint64(readBack.Position)
	if pos <= setting.Cluster.AckMargin {
		return
	}
	target := pos - setting.Cluster.AckMargin
	if b, err := c.cursor(ctx, cluster_model.CursorBackup); err == nil && b.Lineage == lineage {
		if bp := uint64(b.Position); bp < target && pos-bp < backupMaxLag {
			target = bp
		} else if bp < target {
			log.Warn("cluster consumer: the PostgreSQL backup lags %d entries behind; trimming past it, that backup is not replayable", pos-bp)
		}
	}
	c.mu.Lock()
	last := c.lastAck[lineage]
	c.mu.Unlock()
	if target < last+ackEvery {
		return
	}
	data, err := reftable.Encode(reftable.KindAck, &reftable.Ack{Identity: lineage, Index: target})
	if err != nil {
		return
	}
	if _, _, err := c.r.proposeCommand(ctx, data, nil); err != nil {
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
// knows no rename: the consumer uses pushWorkWith and the table's.
func pushWork(ctx context.Context, evs []reftable.Event) error {
	return pushWorkWith(ctx, evs, nil)
}

// renamedFromFunc is the table's RenamedFrom.
type renamedFromFunc func(repo string) (from string, index uint64, ok bool)

func pushWorkWith(ctx context.Context, evs []reftable.Event, renamedFrom renamedFromFunc) error {
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
		return err
	}
	if first.RepoID > 0 && !eventLeadsTo(first, clusterhook.RepoKey(repo.OwnerName, repo.Name, first.IsWiki), renamedFrom) {
		// The event's ID now names another repository: the row it was
		// written for is gone (a PostgreSQL failover that lost its tail, a
		// recovery to an earlier position) and the sequence handed its ID
		// out again. Its branch rows, webhooks and CI must not land there.
		return fmt.Errorf("repository ID %d is %s/%s, which the event's %s does not lead to: the ID was reused; run `forgejo cluster reconcile`", first.RepoID, repo.OwnerName, repo.Name, first.Repo)
	}
	if u, err := user_model.GetUserByID(ctx, first.PusherID); err == nil && pusherReissued(u, first) {
		// The same loss reissues user IDs: an account created after the
		// push cannot have made it, and must not be credited with it.
		return fmt.Errorf("pusher ID %d is %s, created after the push at %d: the ID was reused; run `forgejo cluster reconcile`", first.PusherID, u.Name, first.Token.Index)
	}
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
		updates = append(updates, opt)
		if ref.IsBranch() {
			if opt.IsDelRef() {
				if err := git_model.AddDeletedBranch(ctx, repo.ID, ref.BranchName(), ev.PusherID); err != nil {
					return err
				}
			} else {
				branches = append(branches, ref.BranchName())
			}
		}
	}
	if len(branches) > 0 {
		gitRepo, err := gitrepo.OpenRepository(ctx, repo)
		if err != nil {
			return err
		}
		err = repo_service.SyncBranchesToDB(ctx, repo.ID, first.PusherID, branches, gitRepo.GetBranchCommit)
		gitRepo.Close()
		if err != nil {
			return err
		}
	}
	if len(updates) == 0 {
		return nil
	}
	return repo_service.PushUpdatesNow(clusterhook.WithCommand(ctx, first.Token.String()), updates)
}

// pusherClockSkew bounds the clock difference between the node that
// proposed a push and the one that created its pusher's account.
const pusherClockSkew = 5 * time.Minute

// pusherReissued reports whether u was created after the event, beyond the
// clock skew between nodes: its ID was handed out again.
func pusherReissued(u *user_model.User, ev reftable.Event) bool {
	if ev.Time <= 0 || u.CreatedUnix <= 0 {
		return false
	}
	return time.Unix(int64(u.CreatedUnix), 0).After(time.Unix(0, ev.Time).Add(pusherClockSkew))
}

// eventLeadsTo reports whether the repository an event was written for is
// the one now keyed key: the same key, or a key it was renamed to after the
// event, following the table's renames backwards. Each step must go back
// in the log, so a rename and its undo cannot loop.
func eventLeadsTo(ev reftable.Event, key string, renamedFrom renamedFromFunc) bool {
	at := uint64(1<<64 - 1)
	for key != ev.Repo {
		if renamedFrom == nil {
			return false
		}
		from, idx, ok := renamedFrom(key)
		if !ok || idx <= ev.Token.Index || idx >= at {
			return false
		}
		key, at = from, idx
	}
	return true
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
