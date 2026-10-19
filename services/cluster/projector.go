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
	// fetch brings missing objects from the other members.
	fetch func(ctx context.Context, repo, dir string, shas []string) error

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
	pendingFirst map[string]bool
	failures     map[string]string
	// deletedAt holds the index of a repository's deletion until this node
	// removed its directory: a key revived meanwhile by a rename finds the
	// deleted repository's directory still there.
	deletedAt map[string]uint64
	wake      chan struct{}
	waiters   []chan struct{}
}

func newProjector(root string, table *reftable.Table, reg *registry) *projector {
	p := &projector{
		root: root, table: table, registry: reg,
		dirty: map[string]uint64{}, watermark: map[string]uint64{},
		fenced: true, pendingFirst: map[string]bool{}, failures: map[string]string{}, deletedAt: map[string]uint64{},
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
	p.mu.Unlock()
	p.signal()
}

// Fenced reports whether the start-up fence still holds.
func (p *projector) Fenced() bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.fenced
}

// Ready reports whether projection caught up once since the start.
func (p *projector) Ready() bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	return !p.fenced && len(p.pendingFirst) == 0
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
}

func (p *projector) Lag() []RepoLag {
	p.mu.Lock()
	defer p.mu.Unlock()
	out := make([]RepoLag, 0, len(p.dirty))
	for r, idx := range p.dirty {
		out = append(out, RepoLag{Repo: r, Watermark: p.watermark[r], Target: idx, Error: p.failures[r]})
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

// pass projects every dirty repository once.
func (p *projector) pass(ctx context.Context) {
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
		if err := p.project(ctx, repo); err != nil {
			log.Warn("cluster: projecting %s: %v", repo, err)
			p.mu.Lock()
			p.failures[repo] = err.Error()
			p.mu.Unlock()
		}
	}
	p.notifyWaiters()
}

// project makes one repository match the table, then certifies it.
func (p *projector) project(ctx context.Context, repo string) error {
	if p.registry.inFlight(repo) {
		return nil // retried at the next pass, once git is done
	}
	dir := filepath.Join(p.root, filepath.FromSlash(repo))
	refs, at := p.table.Refs(repo)

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
	want := tableProjection(refs)
	if _, err := os.Stat(dir); errors.Is(err, os.ErrNotExist) {
		if from, renIdx, ok := p.table.RenamedFrom(repo); ok {
			src := filepath.Join(p.root, filepath.FromSlash(from))
			if p.registry.inFlight(from) {
				// A git process still writes under the old key: moving its
				// directory now would land its write under the new name,
				// outside the table. The move waits for the next pass.
				return nil
			}
			if p.holdsRenamed(src, from, repo, renIdx) {
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

	have, err := readGitRefs(ctx, dir)
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
			if err := p.fetch(ctx, repo, dir, missing); err != nil {
				return fmt.Errorf("fetching %d missing objects: %w", len(missing), err)
			}
		}
	}
	if len(set) > 0 || len(del) > 0 {
		if err := writeRefs(ctx, dir, set, del); err != nil {
			return err
		}
	}
	// Certification re-reads git under the registry's lock, so a native
	// transaction cannot publish between the check and the watermark (R3-12).
	// When a transaction registered meanwhile, certify does nothing and the
	// repository stays dirty for the next pass.
	var verr error
	p.registry.certify(repo, func() {
		now, err := readGitRefs(ctx, dir)
		if err != nil {
			verr = err
			return
		}
		got := reftable.ProjectionDigest(filterTracked(now, refs))
		if got != reftable.ProjectionDigest(want) {
			verr = errors.New("git does not match the table after projection")
			return
		}
		p.certify(repo, at, writeStamp(dir, repo, at, reftable.ProjectionDigest(want)))
	})
	return verr
}

// filterTracked keeps the refs of git the table knows about: a ref the
// table never held (one from before the cluster) is reported elsewhere and
// left alone, never deleted by the projector.
func filterTracked(git map[string]reftable.ProjectedRef, refs map[string]reftable.RefValue) map[string]reftable.ProjectedRef {
	out := map[string]reftable.ProjectedRef{}
	for name, r := range git {
		if _, known := refs[name]; known {
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
	p.mu.Unlock()
}

// stamp is the per-repository projection record (D19): the key, the
// watermark and the projected digest. A repository restored from a backup
// carries a stale stamp, which the start-up reconciliation reprojects anyway.
// A directory moves with its stamp, so the key it names is what the
// directory holds: the repository certified under that key at that
// watermark. Stamps written before the key was recorded name none.
type stamp struct {
	Repo      string `json:"repo,omitempty"`
	Watermark uint64 `json:"watermark"`
	Digest    string `json:"digest"`
	Written   int64  `json:"written"`
}

const stampName = "forgejo-cluster-stamp"

// writeStamp records the projection AFTER the refs, through a rename.
func writeStamp(dir, repo string, at uint64, digest reftable.Sum) error {
	b, err := json.Marshal(stamp{Repo: repo, Watermark: at, Digest: digest.String(), Written: time.Now().Unix()})
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
	b, err := os.ReadFile(filepath.Join(dir, stampName))
	if err != nil {
		return false, false
	}
	var s stamp
	if json.Unmarshal(b, &s) != nil || s.Repo == "" {
		return false, false
	}
	return s.Repo == key && s.Watermark < index, true
}

// Verify compares a repository's git state with the table under the
// projection rules, and schedules a reprojection on a difference.
func (p *projector) Verify(ctx context.Context, repo string) (bool, error) {
	dir := filepath.Join(p.root, filepath.FromSlash(repo))
	refs, _ := p.table.Refs(repo)
	want := tableProjection(refs)
	have, err := readGitRefs(ctx, dir)
	if err != nil {
		return false, err
	}
	same := reftable.ProjectionDigest(filterTracked(have, refs)) == reftable.ProjectionDigest(want)
	if !same {
		p.markDirty(repo)
	}
	return same, nil
}

// MayCollectGarbage is the projector's gate for git gc: projected, no native
// transaction in flight, and no unconsumed event pinning objects of it.
func (p *projector) MayCollectGarbage(repo string) bool {
	if p.registry.inFlight(repo) || len(p.table.PinnedObjects(repo)) > 0 {
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
	if held, known := heldBefore(src, from, renIdx); known {
		return held
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
	z, zIdx, ok := p.table.RenamedInto(repo, math.MaxUint64)
	if !ok {
		return false
	}
	if _, deleted := p.table.Deleted(z); deleted {
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
	if !p.holdsRenamed(dir, repo, z, zIdx) {
		return false
	}
	p.markDirty(z)
	return true
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
func (p *projector) setAsideStale(repo, dir string) error {
	p.mu.Lock()
	delIdx, pending := p.deletedAt[repo]
	delete(p.deletedAt, repo)
	p.mu.Unlock()
	from, renIdx, renamed := p.table.RenamedFrom(repo)
	if !renamed {
		return nil
	}
	if _, err := os.Stat(dir); err != nil {
		return nil
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
	if err := os.MkdirAll(filepath.Dir(aside), 0o700); err != nil {
		return err
	}
	if err := os.Rename(dir, aside); err != nil {
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
