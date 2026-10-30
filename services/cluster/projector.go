// Copyright 2026 The Forgejo Authors. All rights reserved.
// SPDX-License-Identifier: GPL-3.0-or-later

package cluster

import (
	"context"
	"errors"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"forgejo.org/modules/json"
	"forgejo.org/modules/log"
	"forgejo.org/services/cluster/reftable"
)

// projector is the ONLY authority over git on this node (ADR 0152 D19,
// R2-35). It makes the local repositories match the table, from state and
// never from replay, and certifies a per-repository watermark once a
// repository matches.
type projector struct {
	root     string
	table    *reftable.Table
	registry *registry
	// holds keeps the projection, its fetches and the object channel out
	// of the directories Forgejo moves on this node: an owner's (#257), a
	// repository's and its wiki's (#274).
	holds *dirHolds
	// fetch brings missing objects from the other members.
	fetch func(ctx context.Context, repo, dir string, shas []string) error

	// passMu is held by a pass, so refence knows when none runs.
	passMu sync.Mutex

	mu sync.Mutex
	// dirty maps a repository to the highest index that touched it and is
	// not yet certified.
	dirty map[string]uint64
	// watermark is the certified index of each repository.
	watermark map[string]uint64
	// fenced holds projection and certification until a quorum-confirmed
	// commit boundary has been applied after the start (R4-19, R5-03).
	fenced bool
	// pendingFirst counts repositories not yet reconciled once since the
	// start: no watermark is certified as "everything" before they are.
	// Each one holds the writes of its own key only (#39).
	pendingFirst map[string]bool
	// unfencedAt is when the fence was lifted: past firstPassGrace, the
	// keys still pending no longer hold the node's readiness.
	unfencedAt time.Time
	// graceLogged is set once the keys holding the first pass past its
	// grace were reported.
	graceLogged bool
	failures    map[string]string
	// current is the repository the pass projects now, "" between them.
	current string
	// deletedAt holds the index of a repository's deletion until this node
	// removed its directory: a key revived meanwhile by a rename finds the
	// deleted repository's directory still there.
	deletedAt map[string]uint64
	// fetching holds the cancellation of each repository whose missing
	// objects are fetched outside the pass (#137): the pass skips it, and
	// moves no directory of it, until the fetch ended.
	fetching map[string]context.CancelFunc
	// retrying holds the fetches in flight that retry a repository whose
	// last projection or fetch failed: they hold a slot without the
	// assurance of progress a first attempt has (#160).
	retrying map[string]bool
	// queued holds the repositories whose missing objects wait for a fetch
	// slot, all maxFetches taken by other repositories' (#151).
	queued map[string]bool
	// fetches counts the fetches in flight, for refence.
	fetches sync.WaitGroup
	wake    chan struct{}
	waiters []chan struct{}
}

func newProjector(root string, table *reftable.Table, reg *registry) *projector {
	p := &projector{
		root: root, table: table, registry: reg, holds: newDirHolds(),
		dirty: map[string]uint64{}, watermark: map[string]uint64{},
		fenced: true, pendingFirst: map[string]bool{}, failures: map[string]string{}, deletedAt: map[string]uint64{},
		fetching: map[string]context.CancelFunc{}, retrying: map[string]bool{}, queued: map[string]bool{},
		wake: make(chan struct{}, 1),
	}
	table.OnChange(p.onChange)
	return p
}

// onChange is called by the table under its lock: it only records.
func (p *projector) onChange(ch reftable.Change) {
	p.mu.Lock()
	for _, r := range ch.Repos {
		if ch.Index > p.dirty[r] {
			p.dirty[r] = ch.Index
		}
	}
	for _, r := range ch.Deleted {
		p.deletedAt[r] = ch.Index
	}
	p.mu.Unlock()
	p.signal()
}

func (p *projector) signal() {
	select {
	case p.wake <- struct{}{}:
	default:
	}
}

// markDirty asks for a reconciliation, after a native git transaction
// finished: the pre-write it made is checked against the table.
func (p *projector) markDirty(repo string) {
	_, idx := p.table.RepoHash(repo)
	pos, _ := p.table.Applied()
	if pos.Index > idx {
		idx = pos.Index
	}
	p.mu.Lock()
	if idx > p.dirty[repo] {
		p.dirty[repo] = idx
	} else if _, ok := p.dirty[repo]; !ok {
		p.dirty[repo] = idx
	}
	p.mu.Unlock()
	p.signal()
}

// unfence lifts the start-up fence once a barrier's index is applied, and
// schedules every repository the table knows for a first reconciliation.
func (p *projector) unfence() {
	repos := p.table.Repos()
	pos, _ := p.table.Applied()
	p.mu.Lock()
	for _, r := range repos {
		p.pendingFirst[r] = true
		if p.dirty[r] < pos.Index {
			p.dirty[r] = pos.Index
		}
	}
	p.fenced = false
	p.unfencedAt = time.Now()
	p.graceLogged = false
	p.mu.Unlock()
	p.signal()
}

// refence puts the start-up fence back and waits for a pass in flight,
// before the table is emptied for a rejoin under a new id (#41): no pass
// may project a table that replays from its start. The fetches the passes
// started are cancelled and waited for too, so none records a failure of
// the old table.
//
// The state is cleared once that pass ended, which may still have
// certified a watermark or recorded a failure of the old table. deletedAt
// stays: a snapshot the new table installs replays no deletion, and a
// directory a deletion left here is set aside, never reused.
func (p *projector) refence() {
	p.mu.Lock()
	p.fenced = true
	p.mu.Unlock()
	p.passMu.Lock()
	defer p.passMu.Unlock()
	// No pass runs and none starts while fenced: no fetch starts either.
	p.mu.Lock()
	for _, cancel := range p.fetching {
		cancel()
	}
	p.mu.Unlock()
	p.fetches.Wait()
	p.mu.Lock()
	p.pendingFirst = map[string]bool{}
	p.dirty = map[string]uint64{}
	p.watermark = map[string]uint64{}
	p.failures = map[string]string{}
	p.queued = map[string]bool{}
	p.retrying = map[string]bool{}
	p.mu.Unlock()
}

// Fenced reports whether the start-up fence still holds.
func (p *projector) Fenced() bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.fenced
}

// firstPassGrace bounds how long the first reconciliation of every
// repository holds the node's readiness after a start. A repository whose
// projection keeps failing (a damaged packed-refs, a lock left behind)
// would otherwise keep the node out of service for good (#39): past the
// grace it stays behind alone, refused to writes and reported in Lag and
// in projection_failing_repositories.
var firstPassGrace = 10 * time.Minute

// Ready reports whether projection caught up once since the start, or the
// first pass's grace ran out with only some keys behind.
func (p *projector) Ready() bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.fenced {
		return false
	}
	if len(p.pendingFirst) == 0 {
		return true
	}
	if time.Since(p.unfencedAt) < firstPassGrace {
		return false
	}
	if !p.graceLogged {
		p.graceLogged = true
		keys := make([]string, 0, len(p.pendingFirst))
		for r := range p.pendingFirst {
			keys = append(keys, r)
		}
		sort.Strings(keys)
		if len(keys) > 20 {
			keys = append(keys[:20], "...")
		}
		log.Error("cluster: %d repositories are still not reconciled %s after the start, they stay refused to writes until their projection succeeds: %s", len(p.pendingFirst), firstPassGrace, strings.Join(keys, ", "))
	}
	return true
}

// Writable reports whether a write to repo may be proposed from this node:
// the fence is lifted and repo itself was reconciled once since the start.
// Another key still behind does not hold it (#39); Apply checks the old
// values against the table, and gitWillApply the local git.
func (p *projector) Writable(repo string) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	return !p.fenced && !p.pendingFirst[repo]
}

// Projected returns the index up to which every entry touching repo is
// projected on this node. The applied index is read BEFORE the dirty set,
// and the table marks a repository dirty before its new applied index is
// visible, so the answer never runs ahead of git.
func (p *projector) Projected(repo string) uint64 {
	pos, _ := p.table.Applied()
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.fenced || p.pendingFirst[repo] {
		return 0
	}
	if _, ok := p.dirty[repo]; ok {
		return p.watermark[repo]
	}
	return pos.Index
}

// Wait blocks until repo is projected at index or ctx ends.
func (p *projector) Wait(ctx context.Context, repo string, index uint64) error {
	for {
		if p.Projected(repo) >= index {
			return nil
		}
		ch := make(chan struct{})
		p.mu.Lock()
		p.waiters = append(p.waiters, ch)
		p.mu.Unlock()
		if p.Projected(repo) >= index {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ch:
		case <-time.After(250 * time.Millisecond):
		}
	}
}

func (p *projector) notifyWaiters() {
	p.mu.Lock()
	ws := p.waiters
	p.waiters = nil
	p.mu.Unlock()
	for _, w := range ws {
		close(w)
	}
}

// Lag lists the repositories not yet certified at their last change, with
// how far behind they are, for the administration.
type RepoLag struct {
	Repo      string
	Watermark uint64
	Target    uint64
	Error     string
	// Pending is set while the repository was not reconciled once since
	// the start: its writes are refused on this node.
	Pending bool
}

func (p *projector) Lag() []RepoLag {
	p.mu.Lock()
	defer p.mu.Unlock()
	out := make([]RepoLag, 0, len(p.dirty))
	for r, idx := range p.dirty {
		out = append(out, RepoLag{Repo: r, Watermark: p.watermark[r], Target: idx, Error: p.failures[r], Pending: p.pendingFirst[r]})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Target-out[i].Watermark > out[j].Target-out[j].Watermark })
	return out
}

// run projects until ctx ends.
func (p *projector) run(ctx context.Context) {
	tick := time.NewTicker(2 * time.Second)
	defer tick.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-p.wake:
		case <-tick.C:
		}
		p.pass(ctx)
	}
}

// projectBudget bounds the projection of one repository in a pass, and
// the fetch of its missing objects: a repository whose git or whose
// members stall must not hold it for good. The object channel cuts a
// stalled transfer much sooner (objectIdle).
var projectBudget = time.Hour

// maxFetches bounds the fetches of missing objects in flight at once. The
// pass itself stays serial, the order of the directory moves of chained
// renames depends on it; only the fetch, the long part (several gigabytes
// behind a slow member), runs beside it, so one repository's transfer does
// not freeze the derivation of every other one (#137).
var maxFetches = 4

// pass projects every dirty repository once.
func (p *projector) pass(ctx context.Context) {
	p.passMu.Lock()
	defer p.passMu.Unlock()
	p.mu.Lock()
	if p.fenced {
		p.mu.Unlock()
		return
	}
	work := make([]string, 0, len(p.dirty))
	for r := range p.dirty {
		work = append(work, r)
	}
	p.mu.Unlock()
	// A rename's target goes first, so its directory moves before the old
	// key, whose refs are all tombstones, is projected. Targets go in the
	// order of their renames: in a chain (a to z, then b to a), a's
	// directory reaches z before b's takes its place.
	renIdx := make(map[string]uint64, len(work))
	renamed := make(map[string]bool, len(work))
	for _, r := range work {
		_, renIdx[r], renamed[r] = p.table.RenamedFrom(r)
	}
	sort.Slice(work, func(i, j int) bool {
		ri, rj := renamed[work[i]], renamed[work[j]]
		if ri != rj {
			return ri
		}
		if ri && renIdx[work[i]] != renIdx[work[j]] {
			return renIdx[work[i]] < renIdx[work[j]]
		}
		return work[i] < work[j]
	})
	for _, repo := range work {
		if ctx.Err() != nil {
			return
		}
		// A key whose directory Forgejo moves on this node, alone or with
		// its owner's, stays dirty until the hold's release, which wakes a
		// pass (#257, #274).
		leave, ok := p.holds.enter(repo)
		if !ok {
			continue
		}
		p.mu.Lock()
		p.current = repo
		p.mu.Unlock()
		pctx, cancel := context.WithTimeout(ctx, projectBudget)
		err := p.project(ctx, pctx, repo)
		cancel()
		leave()
		p.mu.Lock()
		p.current = ""
		if err != nil {
			p.failures[repo] = err.Error()
		}
		p.mu.Unlock()
		if err != nil {
			log.Warn("cluster: projecting %s: %v", repo, err)
		}
	}
	p.notifyWaiters()
}

// awaitsFetch reports whether a fetch of repo's objects is in flight. A
// repository deleted meanwhile needs none of them: its fetch is cancelled,
// and the next pass removes its directory once the fetch ended.
func (p *projector) awaitsFetch(repo string) bool {
	p.mu.Lock()
	cancel, ok := p.fetching[repo]
	p.mu.Unlock()
	if ok {
		if _, deleted := p.table.Deleted(repo); deleted {
			cancel()
		}
	}
	return ok
}

// isFetching reports whether a fetch of repo's objects is in flight.
func (p *projector) isFetching(repo string) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	_, ok := p.fetching[repo]
	return ok
}

// startFetch fetches repo's missing objects beside the pass, at most
// maxFetches at once and one per repository; past the bound, repo stays
// dirty, queued, and the next pass tries again. When the fetch succeeds, a pass is
// woken to project repo with its objects here; a failure is recorded as
// the projection's, and retried at the next tick.
//
// A retry of a repository whose last attempt failed leaves a slot to each
// queued repository that did not fail: the retries come back at every tick
// and, ahead in the pass's order, took every freed slot again, while a
// healthy repository behind them waited for good (#160).
func (p *projector) startFetch(base context.Context, repo, dir string, missing []string) {
	p.mu.Lock()
	if _, ok := p.fetching[repo]; ok || p.fenced {
		p.mu.Unlock()
		return
	}
	retry := p.failures[repo] != ""
	reserved := 0
	if retry {
		for q := range p.queued {
			if q != repo && p.failures[q] == "" {
				reserved++
			}
		}
	}
	if len(p.fetching)+reserved >= maxFetches {
		p.queued[repo] = true
		p.mu.Unlock()
		return
	}
	// The fetch writes under repo's directory after the pass moved on: it
	// is a writer of that directory until it ends, entered under p.mu, so
	// a hold of repo or of its owner finds it in p.fetching and cancels it
	// (holdDirs, #257, #274).
	leave, ok := p.holds.enter(repo)
	if !ok {
		p.mu.Unlock()
		return
	}
	delete(p.queued, repo)
	ctx, cancel := context.WithTimeout(base, projectBudget)
	p.fetching[repo] = cancel
	if retry {
		p.retrying[repo] = true
	}
	p.fetches.Add(1)
	p.mu.Unlock()
	go func() {
		defer p.fetches.Done()
		defer leave()
		err := p.fetch(ctx, repo, dir, missing)
		cancel()
		p.mu.Lock()
		delete(p.fetching, repo)
		delete(p.retrying, repo)
		if err != nil {
			p.failures[repo] = fmt.Sprintf("fetching %d missing objects: %v", len(missing), err)
		}
		p.mu.Unlock()
		if err != nil {
			// Retried at the next tick, as a failed projection is: woken
			// now, a fetch that fails at once would spin the pass.
			log.Warn("cluster: projecting %s: fetching %d missing objects: %v", repo, len(missing), err)
			return
		}
		p.signal()
	}()
}

// holdDirs keeps the projection, its fetches and the object channel out
// of the directories names covers, owners' (#257) or repositories' by
// their keys (#274), until release, and returns once nothing of the
// cluster writes there any more: a projection under way there ends, and a
// fetch in flight there is cancelled, retried as a failed one once
// released. On ctx's error nothing stays held.
func (p *projector) holdDirs(ctx context.Context, names []string) (func(), error) {
	release, err := p.holds.hold(ctx, names, func(covers func(string) bool) {
		p.mu.Lock()
		defer p.mu.Unlock()
		for repo, cancel := range p.fetching {
			if covers(repo) {
				cancel()
			}
		}
	})
	if err != nil {
		p.signal()
		return nil, err
	}
	return func() {
		release()
		p.signal()
	}, nil
}

// WaitsBehind reports whether a repository's projection waits behind
// other repositories': the serial pass projects another one now, or every
// fetch slot is taken by other repositories' fetches while its own
// objects wait for one (#151), and its own last projection did not fail.
// The consumer does not count that wait against the repository (#137).
// A wait for a slot counts only while a first attempt holds one: slots
// held by retries of failed fetches alone, which may fail or stall again
// for as long as projectBudget each time, free none for certain, and a
// wait behind them is bounded as the repository's own (#160).
func (p *projector) WaitsBehind(repo string) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.failures[repo] != "" {
		return false
	}
	if p.current != "" && p.current != repo {
		return true
	}
	if !p.queued[repo] {
		return false
	}
	for r := range p.fetching {
		if r != repo && !p.retrying[r] {
			return true
		}
	}
	return false
}

// projectedKeys refuses to turn into a path a key, or a rename's source,
// that Apply would refuse: the table checks them on every entry and every
// snapshot, and this check keeps a key that slipped past from removing or
// moving a directory outside the repository root (#81).
func projectedKeys(table *reftable.Table, repo string) error {
	if err := reftable.ValidRepoKey(repo); err != nil {
		return fmt.Errorf("refusing to project %q: %w", repo, err)
	}
	if from, _, ok := table.RenamedFrom(repo); ok {
		if err := reftable.ValidRepoKey(from); err != nil {
			return fmt.Errorf("refusing to move %q into %s: %w", from, repo, err)
		}
	}
	return nil
}

// project makes one repository match the table, then certifies it. Its
// missing objects are fetched outside the pass, under base: the
// repository is projected again once they are here.
func (p *projector) project(base, ctx context.Context, repo string) error {
	if p.registry.inFlight(repo) {
		return nil // retried at the next pass, once git is done
	}
	if p.awaitsFetch(repo) {
		return nil // retried at the next pass, once the fetch ended
	}
	if err := projectedKeys(p.table, repo); err != nil {
		return err
	}
	dir := filepath.Join(p.root, filepath.FromSlash(repo))
	refs, id, at := p.table.RefsWithID(repo)
	withRenamedAway(p.table, repo, refs)

	if _, deleted := p.table.Deleted(repo); deleted {
		if err := os.RemoveAll(dir); err != nil {
			return err
		}
		p.mu.Lock()
		delete(p.deletedAt, repo)
		p.mu.Unlock()
		p.certify(repo, at, nil)
		return nil
	}
	if p.movesOn(repo, dir) {
		return nil // the projection of the rename's target moves it first
	}
	if err := p.setAsideStale(repo, dir); err != nil {
		return err
	}
	if err := p.setAsideInvalid(repo, dir, at); err != nil {
		return err
	}
	want := tableProjection(refs)
	if err := p.setAsideLeftBehind(repo, dir, want); err != nil {
		return err
	}
	if _, err := os.Stat(dir); errors.Is(err, os.ErrNotExist) && !p.passesOn(repo, want) {
		if from, to, renIdx, ok := p.renamedIn(repo); ok {
			src := filepath.Join(p.root, filepath.FromSlash(from))
			if p.registry.inFlight(from) || p.isFetching(from) {
				// A git process still writes under the old key, or a fetch
				// of its objects: moving its directory now would land its
				// write under the new name, outside the table, or fail the
				// fetch. The move waits for the next pass.
				return nil
			}
			// Nor while Forgejo moves the old key's directory on this node,
			// alone or with its owner's: it may go, or come back (#257,
			// #274).
			leaveFrom, ok := p.holds.enter(from)
			if !ok {
				return nil
			}
			defer leaveFrom()
			if p.holdsRenamed(src, from, to, renIdx) {
				if err := os.MkdirAll(filepath.Dir(dir), 0o755); err != nil {
					return err
				}
				if err := os.Rename(src, dir); err != nil {
					return err
				}
			}
		}
	}
	if _, err := os.Stat(dir); errors.Is(err, os.ErrNotExist) {
		if len(want) == 0 {
			p.certify(repo, at, nil)
			return nil
		}
		format := ""
		for _, v := range refs {
			if v.Format != "" {
				format = v.Format
				break
			}
		}
		if err := initBare(ctx, dir, format); err != nil {
			return err
		}
	}

	// A symbolic ref the table holds is read even when dangling, which
	// for-each-ref lists nowhere: written again at every pass, it was
	// never certified (#348). So is one the table deleted: kept, it was
	// never deleted, and certified (#359).
	have, err := readTrackedRefs(ctx, dir, refs)
	if err != nil {
		return err
	}
	set := map[string]reftable.RefValue{}
	var del []string
	var need []string
	for name, v := range refs {
		g, ok := have[name]
		switch {
		case !v.Live():
			if ok && name != "HEAD" {
				del = append(del, name)
			}
		case v.Symbolic != "":
			if !ok || g.Symbolic != v.Symbolic {
				set[name] = v
			}
		default:
			if !ok || g.SHA != v.SHA {
				set[name] = v
				need = append(need, v.SHA)
			}
		}
	}
	if len(need) > 0 {
		missing, err := missingObjects(ctx, dir, need)
		if err != nil {
			return err
		}
		if len(missing) > 0 {
			if p.fetch == nil {
				return fmt.Errorf("%d objects missing and no object channel", len(missing))
			}
			p.startFetch(base, repo, dir, missing)
			return nil
		}
	}
	if len(set) > 0 || len(del) > 0 {
		if err := writeRefs(ctx, dir, set, del); err != nil {
			// A git killed while it held its locks (SIGKILL, OOM, a power
			// cut) leaves them behind, and every later transaction on
			// those refs fails for good (#48).
			if p.clearStaleLocks(repo, dir, set, del) == 0 {
				return err
			}
			if err := writeRefs(ctx, dir, set, del); err != nil {
				return err
			}
		}
	}
	// A directory never certified may come from an init whose sync of its
	// name failed: certified, it is vouched for, so its name is synced.
	if _, known := readStamp(dir); !known {
		if err := syncRepoParents(dir); err != nil {
			return err
		}
	}
	// Certification re-reads git under the registry's lock, so a native
	// transaction cannot publish between the check and the watermark (R3-12).
	// When a transaction registered meanwhile, certify does nothing and the
	// repository stays dirty for the next pass.
	var verr error
	p.registry.certify(repo, func() {
		now, err := readTrackedRefs(ctx, dir, refs)
		if err != nil {
			verr = err
			return
		}
		got := filterTracked(now, refs)
		if reftable.ProjectionDigest(got) != reftable.ProjectionDigest(want) {
			// The refs are named: an operator finds which keeps the
			// repository uncertified (#348).
			verr = fmt.Errorf("git does not match the table after projection: %s", projectionDiff(got, want))
			return
		}
		p.certify(repo, at, writeStamp(dir, repo, id, at, reftable.ProjectionDigest(want)))
	})
	return verr
}

// clearStaleLocks removes the locks a killed git left on the refs the
// projector writes: each ref's own, HEAD's, packed-refs' and the
// packed-refs.new a pack-refs writes before its rename. The projector is
// the only authority over git: a lock is removed only while no native
// transaction is registered for the repository, a reservation included,
// and only past both the registry's TTL and staleLockAge, past which git
// holds none of its own (git gc waits for the gate, which a repository
// behind keeps closed). Only regular files are removed. It returns how
// many were.
func (p *projector) clearStaleLocks(repo, dir string, set map[string]reftable.RefValue, del []string) int {
	if p.registry.busy(repo) {
		return 0
	}
	age := max(p.registry.ttl, staleLockAge)
	paths := []string{"HEAD.lock", "packed-refs.lock", "packed-refs.new"}
	for name := range set {
		paths = append(paths, name+".lock")
	}
	for _, name := range del {
		paths = append(paths, name+".lock")
	}
	removed := 0
	for _, rel := range paths {
		// The names come from the table, whose Apply validated them: no
		// ".." component, no absolute path.
		path := filepath.Join(dir, filepath.FromSlash(rel))
		st, err := os.Lstat(path)
		if err != nil || !st.Mode().IsRegular() || time.Since(st.ModTime()) <= age {
			continue
		}
		if err := os.Remove(path); err != nil {
			log.Warn("cluster: removing the stale lock %s of %s: %v", rel, repo, err)
			continue
		}
		log.Warn("cluster: removed %s of %s, left behind by a git that died %s ago", rel, repo, time.Since(st.ModTime()).Round(time.Second))
		staleLocksRemovedTotal.Inc()
		removed++
	}
	return removed
}

// renameLineageDepth bounds the walk back through a chain of renames.
const renameLineageDepth = 32

// withRenamedAway adds to refs, as tombstones, the refs the keys renamed
// into repo held and repo does not: a rename copies only the live refs, and
// the directory a node behind moves in from the old key still holds the
// refs deleted there before the rename (#96). Projected from repo's refs
// alone, such a ref was neither deleted nor compared, and the node served
// a deleted branch under the new name, certified. Each key of the chain is
// followed back through the renames before the one into it.
func withRenamedAway(t *reftable.Table, repo string, refs map[string]reftable.RefValue) {
	key, before := repo, uint64(math.MaxUint64)
	seen := map[string]bool{repo: true}
	for range renameLineageDepth {
		from, idx, ok := t.RenamedFrom(key)
		if !ok || idx >= before || seen[from] {
			return
		}
		seen[from] = true
		old, _ := t.Refs(from)
		for name := range old {
			if _, known := refs[name]; !known {
				refs[name] = reftable.RefValue{Tombstone: true}
			}
		}
		key, before = from, idx
	}
}

// filterTracked keeps the refs of git the table knows about: a ref the
// table never held (one from before the cluster) is reported elsewhere and
// left alone, never deleted by the projector.
//
// HEAD whose table entry is a tombstone is left out too: the projector never
// deletes HEAD, and a bare repository always has one (#43). Compared, it
// kept a key whose refs are all tombstones (renamed away, deleted) from
// ever certifying.
func filterTracked(git map[string]reftable.ProjectedRef, refs map[string]reftable.RefValue) map[string]reftable.ProjectedRef {
	out := map[string]reftable.ProjectedRef{}
	for name, r := range git {
		if v, known := refs[name]; known && (name != "HEAD" || v.Live()) {
			out[name] = r
		}
	}
	return out
}

// certify sets the watermark of repo to at, and clears it from the dirty set
// unless a newer change arrived meanwhile.
func (p *projector) certify(repo string, at uint64, stampErr error) {
	if stampErr != nil {
		log.Warn("cluster: projection stamp of %s: %v", repo, stampErr)
	}
	p.mu.Lock()
	if at > p.watermark[repo] {
		p.watermark[repo] = at
	}
	if p.dirty[repo] <= at {
		delete(p.dirty, repo)
	}
	delete(p.pendingFirst, repo)
	delete(p.failures, repo)
	delete(p.queued, repo)
	p.mu.Unlock()
}

// stamp is the per-repository projection record (D19): the key, the
// watermark and the projected digest. A repository restored from a backup
// carries a stale stamp, which the start-up reconciliation reprojects anyway.
// A directory moves with its stamp, so the key it names is what the
// directory holds: the repository certified under that key at that
// watermark. Stamps written before the key was recorded name none. ID is
// the database ID of that repository, as the table held it at the
// watermark: 0 when no transaction carrying one wrote it, or in a stamp
// written before it was recorded.
type stamp struct {
	Repo      string `json:"repo,omitempty"`
	ID        int64  `json:"id,omitempty"`
	Watermark uint64 `json:"watermark"`
	Digest    string `json:"digest"`
	Written   int64  `json:"written"`
}

const stampName = "forgejo-cluster-stamp"

// writeStamp records the projection AFTER the refs, through a rename.
func writeStamp(dir, repo string, id int64, at uint64, digest reftable.Sum) error {
	b, err := json.Marshal(stamp{Repo: repo, ID: id, Watermark: at, Digest: digest.String(), Written: time.Now().Unix()})
	if err != nil {
		return err
	}
	tmp := filepath.Join(dir, stampName+".tmp")
	if err := os.WriteFile(tmp, b, 0o644); err != nil {
		return err
	}
	return os.Rename(tmp, filepath.Join(dir, stampName))
}

// heldBefore reports, from its stamp, whether dir holds key's repository as
// this node certified it before index. known is false when the stamp names
// no key: absent, unreadable, or written by an older binary.
func heldBefore(dir, key string, index uint64) (held, known bool) {
	s, known := readStamp(dir)
	return known && s.Repo == key && s.Watermark < index, known
}

// readStamp reads dir's stamp; false when it names no key.
func readStamp(dir string) (stamp, bool) {
	b, err := os.ReadFile(filepath.Join(dir, stampName))
	if err != nil {
		return stamp{}, false
	}
	var s stamp
	if json.Unmarshal(b, &s) != nil || s.Repo == "" {
		return stamp{}, false
	}
	return s, true
}

// liveRename is the table's RenamedFrom, unless repo was deleted since that
// rename: the repository the rename brought is gone, and a repository
// created anew under the key owes nothing to the rename's source. A node
// behind the log would otherwise move the source's directory, the deleted
// repository's, under the live key (#98).
func (p *projector) liveRename(repo string) (string, uint64, bool) {
	from, renIdx, ok := p.table.RenamedFrom(repo)
	if !ok {
		return "", 0, false
	}
	if at, deleted := p.table.DeletedBefore(repo, math.MaxUint64); deleted && at > renIdx {
		return "", 0, false
	}
	return from, renIdx, true
}

// renamedIn is where the repository the newest rename into repo brought
// there comes from on this node: the old key of the chain renameSource
// walks back to, the key the rename from there moved it to, and that
// rename's index. ok is false when no rename into repo is live
// (liveRename), or when the repository that rename brought is not the one
// under repo any more: followed from its arrival along the renames that
// moved it on, it does not end at repo by that rename. A rename's mark
// stays on its target once the repository it brought left it, renamed on
// (o/a to o/b, then o/b to o/c), and a repository created anew under the
// freed name finds that mark: a node behind the log moved the old o/a
// directory, o/c's repository, under the new o/b, certified it there and
// served o/c's objects by their ids, and through the next rename of the new
// o/b, under that one's name. The directory stays under its own key, whose
// projection sets it aside once the chain's end is projected (movesOn,
// departed), and repo is projected afresh, its objects fetched from the
// members (#204).
func (p *projector) renamedIn(repo string) (from, to string, renIdx uint64, ok bool) {
	first, firstIdx, ok := p.liveRename(repo)
	if !ok {
		return "", "", 0, false
	}
	from, to, renIdx = p.renameSource(repo, first, firstIdx)
	holder, at, held := p.heldAfter(to, renIdx, math.MaxUint64)
	if !held || holder != repo || at != firstIdx {
		return "", "", 0, false
	}
	return from, to, renIdx, true
}

// heldAfter follows the repository certified under key at index at along
// the renames that moved it on, before index before: the key it was under
// last, and the index of the rename that brought it there, at when it
// never moved. A chain of renames that comes back ends where it started
// (#43). ok is false when it was deleted on the way, idx then the index
// of that deletion, or when the chain is longer than the walk follows.
//
// A rename's mark alone does not tell which repository it moved: the key
// it names may have been taken since by another repository, renamed in
// turn. The walk starts from what the directory's stamp says it holds and
// takes, from each key, the first rename after the repository arrived
// there, which is the one that moved it (#181).
func (p *projector) heldAfter(key string, at, before uint64) (string, uint64, bool) {
	idx := at
	for range renameLineageDepth {
		next, nextIdx, moved := p.table.RenamedAfter(key, idx)
		if !moved || nextIdx >= before {
			if del, deleted := p.table.DeletedBefore(key, before); deleted && del > idx {
				return key, del, false
			}
			return key, idx, true
		}
		if del, deleted := p.table.DeletedBefore(key, nextIdx); deleted && del > idx {
			return key, del, false
		}
		key, idx = next, nextIdx
	}
	return key, idx, false
}

// departed reports whether dir's stamp s names a repository that is no
// longer the one under repo: the key it went to and since which index, or,
// with deleted, the key it was deleted under and the deletion's index.
// Certified under repo, it was deleted since (#52, #98), or renamed away
// by renames that did not come back to repo (#181). Certified under
// another key, it went on to a key that is not repo: the directory came
// here with an owner's directory, which Forgejo moved whole on a node that
// had not projected one of the owner's renames (#181). A repository that
// did not leave the key its stamp names is not judged: Forgejo moves a
// directory on its own disk before the entry of the rename is applied. A
// chain of renames longer than heldAfter follows counts as a departure:
// the directory is set aside and the repository fetched again, never
// served from a directory this node cannot place.
func (p *projector) departed(repo string, s stamp) (key string, at uint64, deleted, left bool) {
	holder, idx, ok := p.heldAfter(s.Repo, s.Watermark, math.MaxUint64)
	switch {
	case !ok:
		return holder, idx, true, true
	case holder == repo || holder == s.Repo:
		return "", 0, false, false
	default:
		return holder, idx, false, true
	}
}

// Verify compares a repository's git state with the table under the
// projection rules, and schedules a reprojection on a difference.
//
// A directory this node lost while the table holds the repository is one:
// git cannot read it, and Verify failed without scheduling anything, so
// `forgejo cluster reproject` never gave a node back the copy that "delete
// unadopted", or an operator, removed from its disk alone. The projector
// creates it again from the table and the other members' objects, as it
// does at the node's start (#245).
func (p *projector) Verify(ctx context.Context, repo string) (bool, error) {
	dir := filepath.Join(p.root, filepath.FromSlash(repo))
	refs, _ := p.table.Refs(repo)
	withRenamedAway(p.table, repo, refs)
	want := tableProjection(refs)
	if _, err := os.Stat(dir); errors.Is(err, os.ErrNotExist) {
		if len(want) == 0 {
			return true, nil
		}
		p.markDirty(repo)
		return false, nil
	}
	have, err := readTrackedRefs(ctx, dir, refs)
	if err != nil {
		return false, err
	}
	got := filterTracked(have, refs)
	same := reftable.ProjectionDigest(got) == reftable.ProjectionDigest(want)
	if !same {
		log.Info("cluster: %s differs from the table: %s", repo, projectionDiff(got, want))
		p.markDirty(repo)
	}
	return same, nil
}

// MayCollectGarbage is the projector's gate for git gc: projected, no native
// transaction in flight, and no unconsumed event pinning objects of it.
func (p *projector) MayCollectGarbage(repo string) bool {
	if p.registry.busy(repo) || len(p.table.PinnedObjects(repo)) > 0 {
		return false
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	_, dirty := p.dirty[repo]
	return !p.fenced && !dirty && !p.pendingFirst[repo]
}

// staleDirName is where, under the repository root, the projector sets aside
// a directory it must not reuse; its leading dot keeps it outside every
// repository key.
const staleDirName = ".cluster-stale"

// holdsRenamed reports whether src, the directory of a rename's old key, is
// the repository the rename at renIdx moved to to: the one certified under
// from before the rename. The old key may have been taken again since (a
// repository created anew under the freed name), and moving that one would
// take a live repository away from its name. Without a keyed stamp, the
// directory moves only while from was not taken again and its newest
// rename is this one.
func (p *projector) holdsRenamed(src, from, to string, renIdx uint64) bool {
	if _, err := os.Stat(src); err != nil {
		return false
	}
	if s, known := readStamp(src); known {
		// The rename moved the repository from held just before it: a
		// directory certified under from holds that one only while the
		// repository certified then stayed, never deleted (#98) nor
		// renamed away by renames that did not come back (#181).
		if s.Repo != from || s.Watermark >= renIdx {
			return false
		}
		holder, _, ok := p.heldAfter(from, s.Watermark, renIdx)
		return ok && holder == from
	}
	moved, ok := p.table.MovedAway(from)
	return ok && moved == to
}

// movesOn reports whether dir still holds the repository a rename moved
// from repo to another key, which this node has not moved yet: in a chain
// (repo to Z, then another key to repo), or while Z's projection waits (a
// transaction in flight, a failure). Projecting repo now would rewrite that
// directory as repo, certified past the rename, and Z would no longer take
// it; repo waits, and Z is scheduled to move it.
func (p *projector) movesOn(repo, dir string) bool {
	if _, err := os.Stat(dir); err != nil {
		return false
	}
	first, firstIdx, z, zIdx, ok := p.movedOn(repo, dir)
	if !ok || z == repo {
		return false
	}
	if _, deleted := p.table.Deleted(z); deleted {
		return false
	}
	if at, deleted := p.table.DeletedBefore(z, math.MaxUint64); deleted && at > zIdx {
		// Z was deleted since, and maybe taken again: the repository this
		// directory holds is gone (#98).
		return false
	}
	if _, err := os.Stat(filepath.Join(p.root, filepath.FromSlash(z))); !errors.Is(err, os.ErrNotExist) {
		// Z's directory may be the previous repository under that name,
		// which Z's projection sets aside before this one moves in: only a
		// Z certified since its rename no longer wants this directory.
		p.mu.Lock()
		done := p.watermark[z] >= zIdx
		p.mu.Unlock()
		if done {
			return false
		}
	}
	if !p.holdsRenamed(dir, repo, first, firstIdx) {
		return false
	}
	p.markDirty(z)
	return true
}

// movedOn finds where the repository dir holds went from repo: the rename
// that moved it, and the end of the chain of renames it took, through its
// waypoints (#43); the chain's end is repo when it came back. ok is false
// when no rename moved it, or when it was deleted on the way.
//
// With a stamp naming repo, the chain starts at the first rename after the
// stamp's watermark: the newest rename from repo moves the repository that
// took repo since, when one did (#181). Without one, it starts at the
// newest rename, through keys left without a live ref.
func (p *projector) movedOn(repo, dir string) (first string, firstIdx uint64, z string, zIdx uint64, ok bool) {
	if s, known := readStamp(dir); known && s.Repo == repo {
		first, firstIdx, ok = p.table.RenamedAfter(repo, s.Watermark)
		if !ok {
			return "", 0, "", 0, false
		}
		z, zIdx, ok = p.heldAfter(repo, s.Watermark, math.MaxUint64)
		return first, firstIdx, z, zIdx, ok
	}
	first, firstIdx, ok = p.table.RenamedInto(repo, math.MaxUint64)
	if !ok {
		return "", 0, "", 0, false
	}
	z, zIdx = first, firstIdx
	for range renameLineageDepth {
		if _, moved := p.table.MovedAway(z); !moved {
			break
		}
		next, nextIdx, ok := p.table.RenamedInto(z, math.MaxUint64)
		if !ok || nextIdx <= zIdx {
			break
		}
		z, zIdx = next, nextIdx
	}
	return first, firstIdx, z, zIdx, true
}

// setAsideStale handles a key taken by a rename while this node still holds
// the key's previous repository under it, one deleted and not yet removed
// here: the renamed repository must move in instead of being projected into
// that directory (other objects, maybe another object format, refs the
// table never held there). The directory is moved under staleDirName, never
// removed.
//
// The directory's stamp says which repository it holds, and survives a
// restart and a snapshot. Without a keyed stamp, only a deletion this
// process saw applied (deletedAt) marks the directory, and only when the
// rename's source directory is still here: on the node that served the
// rename, Forgejo already moved the source in, and the directory is the
// right one.
//
// A directory whose keyed stamp names a repository that left the key, which
// a node behind the log did not remove or move, is set aside too (departed):
// one deleted since its certification, or renamed away and not come back,
// before a repository created anew under the key or renamed into it is
// projected into it, its objects kept and served by their ids, its object
// format maybe another (#52, #98, #181).
func (p *projector) setAsideStale(repo, dir string) error {
	p.mu.Lock()
	delIdx, pending := p.deletedAt[repo]
	delete(p.deletedAt, repo)
	p.mu.Unlock()
	if s, known := readStamp(dir); known {
		if key, at, deleted, left := p.departed(repo, s); left {
			if err := p.setAside(dir, repo, at); err != nil {
				if pending {
					p.mu.Lock()
					p.deletedAt[repo] = delIdx
					p.mu.Unlock()
				}
				return err
			}
			if deleted {
				log.Warn("cluster: %s holds the repository certified under %s at %d, which was deleted as %s at %d: that directory is set aside", repo, s.Repo, s.Watermark, key, at)
			} else {
				log.Warn("cluster: %s holds the repository certified under %s at %d, which is %s since %d: that directory is set aside", repo, s.Repo, s.Watermark, key, at)
			}
			return nil
		}
	}
	from, renIdx, renamed := p.liveRename(repo)
	if !renamed {
		return nil
	}
	if _, err := os.Stat(dir); err != nil {
		return nil
	}
	// A chain of renames that came back (repo to b, then b to repo): the
	// directory is repo's own, certified before the chain started (#43).
	if src, _, srcIdx := p.renameSource(repo, from, renIdx); src == repo {
		if held, known := heldBefore(dir, repo, srcIdx); known && held {
			return nil
		}
	}
	stale, known := heldBefore(dir, repo, renIdx)
	if !known {
		_, err := os.Stat(filepath.Join(p.root, filepath.FromSlash(from)))
		stale = pending && renIdx > delIdx && err == nil
	}
	if !stale {
		return nil
	}
	mark := delIdx
	if known || !pending {
		mark = renIdx
	}
	aside := filepath.Join(p.root, staleDirName, fmt.Sprintf("%s-%d", strings.ReplaceAll(repo, "/", "_"), mark))
	if err := p.setAside(dir, repo, mark); err != nil {
		if pending {
			p.mu.Lock()
			p.deletedAt[repo] = delIdx
			p.mu.Unlock()
		}
		return err
	}
	log.Warn("cluster: %s was taken by a rename of %s while this node still held its previous repository: that directory is set aside in %s", repo, from, aside)
	return nil
}

// setAsideInvalid moves aside a directory under repo's key that is no git
// repository, a git init killed halfway or never synced (#42): the
// projection then creates it anew and fetches its objects from the
// members, where it would otherwise fail at every pass. A directory a
// native git of this key may still be writing is left alone: one with a
// transaction registered, or one changed lately, as Forgejo's own git init
// writes HEAD through the hook before it creates objects/. Like
// setAsideStale, it is moved under staleDirName, never removed.
func (p *projector) setAsideInvalid(repo, dir string, at uint64) error {
	st, err := os.Stat(dir)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("inspecting the directory of %s: %w", repo, err)
	}
	invalid, err := invalidRepoDir(dir)
	if err != nil {
		// A repository that cannot be validated now may be sound: it is
		// never moved on that, and the projection reports it.
		return fmt.Errorf("validating the directory of %s: %w", repo, err)
	}
	if !invalid || p.registry.busy(repo) || time.Since(st.ModTime()) <= max(p.registry.ttl, staleLockAge) {
		return nil
	}
	aside := filepath.Join(p.root, staleDirName, fmt.Sprintf("%s-invalid-%d-%d", strings.ReplaceAll(repo, "/", "_"), at, time.Now().UnixNano()))
	if err := os.MkdirAll(filepath.Dir(aside), 0o700); err != nil {
		return err
	}
	if err := os.Rename(dir, aside); err != nil {
		return err
	}
	log.Warn("cluster: %s held no git repository on this node (an interrupted init): that directory is set aside in %s and the repository is projected anew", repo, aside)
	return nil
}

// passesOn reports whether repo is a waypoint of a chain of renames: no
// live ref, and renamed on to a key not deleted. The directory of the
// chain's start belongs to the end of the chain, which takes it from where
// it is (renameSource): moved here, it would be stamped under no key the
// end recognises (#43).
func (p *projector) passesOn(repo string, want map[string]reftable.ProjectedRef) bool {
	if len(want) != 0 {
		return false
	}
	z, _, ok := p.table.RenamedInto(repo, math.MaxUint64)
	if !ok {
		return false
	}
	_, deleted := p.table.Deleted(z)
	return !deleted
}

// renameSource walks back from the rename into repo (from, at renIdx)
// through keys whose directory is absent and whose refs went on, unchanged,
// to the next key of the chain: the first key whose directory is here is
// where a node behind still holds the repository (a to b, then b to c: c
// takes a's directory), with the key its rename moved it to and that
// rename's index. It returns from, repo and renIdx when the walk finds
// nothing.
func (p *projector) renameSource(repo, from string, renIdx uint64) (string, string, uint64) {
	key, to, idx := from, repo, renIdx
	for range renameLineageDepth {
		if _, err := os.Stat(filepath.Join(p.root, filepath.FromSlash(key))); err == nil {
			return key, to, idx
		}
		prev, prevIdx, ok := p.table.RenamedFrom(key)
		if !ok || prevIdx >= idx {
			break
		}
		if moved, ok := p.table.MovedAway(key); !ok || moved != to {
			break
		}
		key, to, idx = prev, key, prevIdx
	}
	return from, repo, renIdx
}

// setAsideLeftBehind moves aside a directory left under a key a rename moved
// away from, when no rename's target takes it any more (the target was
// deleted, or projected anew meanwhile): kept under the freed name, it
// would fail Forgejo's creation of a repository there (#43). Only a
// directory whose stamp names this key is concerned, never one a native git
// of the key may be writing; it is moved under staleDirName, never removed.
func (p *projector) setAsideLeftBehind(repo, dir string, want map[string]reftable.ProjectedRef) error {
	if len(want) != 0 || p.registry.busy(repo) {
		return nil
	}
	if _, moved := p.table.MovedAway(repo); !moved {
		return nil
	}
	if _, err := os.Stat(dir); err != nil {
		return nil
	}
	if held, known := heldBefore(dir, repo, math.MaxUint64); !known || !held {
		return nil
	}
	aside := filepath.Join(p.root, staleDirName, fmt.Sprintf("%s-left-%d", strings.ReplaceAll(repo, "/", "_"), time.Now().UnixNano()))
	if err := os.MkdirAll(filepath.Dir(aside), 0o700); err != nil {
		return err
	}
	if err := os.Rename(dir, aside); err != nil {
		return err
	}
	log.Warn("cluster: %s was renamed away and no rename takes its directory any more: it is set aside in %s", repo, aside)
	return nil
}

// leavesKey reports whether the directory under repo is one the projector
// sets aside or removes before repo is projected: the key is deleted in the
// table, the key holds a repository and the directory, by its stamp,
// another one, which left the key or came with another (departed), or a
// rename took the key while the directory still holds the repository
// certified under it before that rename (setAsideStale,
// setAsideLeftBehind). The object channel refuses to index a pack there
// (#99). Without a keyed stamp, nothing is refused.
func (p *projector) leavesKey(repo string) bool {
	dir := filepath.Join(p.root, filepath.FromSlash(repo))
	if _, err := os.Stat(dir); err != nil {
		return false
	}
	if _, deleted := p.table.Deleted(repo); deleted {
		return true
	}
	// A repository under repo that this directory, by its stamp, is not:
	// a key renamed away, holding none, takes no push, and its directory
	// may still move on to the rename's target.
	if s, known := readStamp(dir); known {
		if refs, _ := p.table.Refs(repo); anyLive(refs) {
			if _, _, _, left := p.departed(repo, s); left {
				return true
			}
		}
	}
	from, renIdx, ok := p.table.RenamedFrom(repo)
	if !ok {
		return false
	}
	if src, _, srcIdx := p.renameSource(repo, from, renIdx); src == repo {
		if held, known := heldBefore(dir, repo, srcIdx); known && held {
			return false
		}
	}
	held, known := heldBefore(dir, repo, renIdx)
	return known && held
}

// awaitsRename reports whether repo's directory is absent here while the
// repository a rename moved into it still sits under an older key of the
// chain, which the projection moves in: a directory created under repo
// meanwhile would keep it out (#100). It follows the projection's rule
// (renamedIn): a key whose rename's repository left it again waits for no
// directory, and a pack for the repository created anew there is taken
// (#204).
func (p *projector) awaitsRename(repo string) bool {
	if _, err := os.Stat(filepath.Join(p.root, filepath.FromSlash(repo))); err == nil {
		return false
	}
	from, to, renIdx, ok := p.renamedIn(repo)
	if !ok {
		return false
	}
	return p.holdsRenamed(filepath.Join(p.root, filepath.FromSlash(from)), from, to, renIdx)
}

// holdsAnotherID reports whether repo's directory holds, by its stamp,
// another repository than the one the database gives repoID: a node that
// never applied a rename or a deletion followed by a recreation of the
// name (cut off from the others, the database still shared) knows nothing
// of it in its table, but the new repository has a new ID (#82). A stamp
// or a caller without an ID is not judged.
func (p *projector) holdsAnotherID(repo string, repoID int64) bool {
	if repoID <= 0 {
		return false
	}
	s, known := readStamp(filepath.Join(p.root, filepath.FromSlash(repo)))
	return known && s.ID > 0 && s.ID != repoID
}

// servesAnother reports whether repo's directory holds, by its stamp,
// another repository than the one the table holds under repo: the stamp
// names a key that is not repo nor one renamed into it before its
// certification there, or it names repo certified before a rename that took
// repo or moved it away, which is not a chain of renames come back. A
// directory without a keyed stamp is not judged.
func (p *projector) servesAnother(repo string) bool {
	b, err := os.ReadFile(filepath.Join(p.root, filepath.FromSlash(repo), stampName))
	if err != nil {
		return false
	}
	var s stamp
	if json.Unmarshal(b, &s) != nil || s.Repo == "" {
		return false
	}
	if s.Repo != repo {
		// The directory of a key renamed into repo, moved in and not
		// certified yet: the same repository.
		key, before := repo, uint64(math.MaxUint64)
		for range renameLineageDepth {
			from, idx, ok := p.table.RenamedFromBefore(key, before)
			if !ok {
				break
			}
			if from == s.Repo && s.Watermark < idx {
				return false
			}
			key, before = from, idx
		}
		return true
	}
	if from, idx, ok := p.table.RenamedFrom(repo); ok && idx > s.Watermark {
		if src, _, srcIdx := p.renameSource(repo, from, idx); src == repo && s.Watermark < srcIdx {
			return false
		}
		return true
	}
	if _, idx, ok := p.table.RenamedInto(repo, math.MaxUint64); ok && idx > s.Watermark {
		return true
	}
	return false
}

// setAside moves dir, which holds a repository repo must not reuse, under
// staleDirName, named after repo and mark; it never removes it.
func (p *projector) setAside(dir, repo string, mark uint64) error {
	aside := filepath.Join(p.root, staleDirName, fmt.Sprintf("%s-%d", strings.ReplaceAll(repo, "/", "_"), mark))
	if err := os.MkdirAll(filepath.Dir(aside), 0o700); err != nil {
		return err
	}
	if _, err := os.Stat(aside); err == nil {
		aside = fmt.Sprintf("%s-%d", aside, time.Now().UnixNano())
	}
	return os.Rename(dir, aside)
}
