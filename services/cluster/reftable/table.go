// Copyright 2026 The Forgejo Authors. All rights reserved.
// SPDX-License-Identifier: GPL-3.0-or-later

// Package reftable is the replicated state machine of Forgejo's cluster: a
// table of every repository's refs, with tombstones and per-ref revisions,
// the event queue the log consumer drains, and the audit of administrative
// operations (kitsunium ADR 0152 D3, D19). Apply is pure: it validates and
// changes the table and nothing else. Git is a projection of the table, made
// by the projector, outside this package.
package reftable

import (
	"bytes"
	"encoding/json" //nolint:depguard // the state machine decodes with the standard library alone: Apply's verdict must not depend on a third-party codec's version
	"errors"
	"fmt"
	"io"
	"sort"
	"strconv"
	"strings"
	"sync"

	"forgejo.org/services/cluster/consensus"
)

// RefValue is one ref of the table.
type RefValue struct {
	SHA       string `json:"sha,omitempty"`
	Format    string `json:"fmt,omitempty"`
	Symbolic  string `json:"sym,omitempty"`
	Rev       uint64 `json:"rev"`
	Tombstone bool   `json:"tomb,omitempty"`
}

// Live reports whether git should hold the ref.
func (v RefValue) Live() bool { return !v.Tombstone }

// Event is the small deterministic record Apply writes per ref update, for
// the log consumer (R7-29). Its time is the one carried in the entry.
type Event struct {
	Token       consensus.Token   `json:"tok"`
	Kind        string            `json:"k"`
	Repo        string            `json:"repo"`
	RepoID      int64             `json:"repo_id"`
	Ref         string            `json:"ref,omitempty"`
	Old         string            `json:"old,omitempty"`
	New         string            `json:"new,omitempty"`
	Rev         uint64            `json:"rev,omitempty"`
	PusherID    int64             `json:"pusher,omitempty"`
	Time        int64             `json:"t"`
	Trigger     string            `json:"trigger,omitempty"`
	PRID        int64             `json:"pr,omitempty"`
	IsWiki      bool              `json:"wiki,omitempty"`
	PushOptions map[string]string `json:"opts,omitempty"`
	RenamedFrom string            `json:"from,omitempty"`
}

// Event kinds.
const (
	EventRef        = "ref"
	EventRepoDelete = "repo-delete"
	EventRepoRename = "repo-rename"
)

func (e *Event) size() int64 {
	n := 160 + len(e.Repo) + len(e.Ref) + len(e.Old) + len(e.New) + len(e.Trigger) + len(e.RenamedFrom)
	for k, v := range e.PushOptions {
		n += len(k) + len(v)
	}
	return int64(n)
}

// AuditRecord is an Audit command once committed, with its position.
type AuditRecord struct {
	Audit
	Token consensus.Token `json:"tok"`
}

type repoState struct {
	refs map[string]*RefValue
	hash Sum
	// last is the index of the last entry that touched the repository.
	last uint64
}

// Change tells the projector which repositories an applied entry touched.
type Change struct {
	Index   uint64
	Repos   []string
	Deleted []string
	Renamed [][2]string
}

// Table is the state machine. Apply runs on one goroutine; every other
// method may be called concurrently.
type Table struct {
	mu         sync.RWMutex
	repos      map[string]*repoState
	root       Sum
	events     []Event
	eventBytes int64
	audit      []AuditRecord
	// deleted maps a deleted repository to the index of its deletion, until
	// refs are written under its key again.
	deleted map[string]uint64
	// renamed maps a new key to the old one and the index of the rename.
	renamed  map[string]renameMark
	grants   map[string]JoinGrant
	applied  consensus.Position
	identity consensus.LogIdentity
	// pending is the open checkpoint barrier; checkpoints the closed ones.
	pending     *pendingCheckpoint
	checkpoints []CheckpointRecord
	// payload is the payload version the cluster agreed to write, 0 for
	// BaseVersion (WriteVersion).
	payload uint8
	// nonces are the nonces of the newest committed ref transactions, with
	// their index, oldest first: a proposer whose outcome was unknown finds
	// its own entry there even after another write moved the ref.
	nonces  []NonceMark
	byNonce map[string]uint64

	// onChange is called after each Apply, outside the lock. It is not state.
	onChange func(Change)
	// onCheckpoint is the coordinator's capture of a barrier. It is not state.
	onCheckpoint func(nonce string, index uint64, encode func() ([]byte, error))
}

type renameMark struct {
	From  string `json:"from"`
	Index uint64 `json:"i"`
}

// New returns an empty table.
func New() *Table {
	return &Table{
		repos:   map[string]*repoState{},
		deleted: map[string]uint64{},
		renamed: map[string]renameMark{},
		grants:  map[string]JoinGrant{},
		byNonce: map[string]uint64{},
	}
}

// NonceMark is a committed ref transaction's nonce and index.
type NonceMark struct {
	Nonce string `json:"n"`
	Index uint64 `json:"i"`
}

// NoncesKept is how many committed nonces the table retains: far more than
// the transactions in flight at once, which the registry caps.
const NoncesKept = 4096

func (t *Table) keepNonce(nonce string, index uint64) {
	if nonce == "" {
		return
	}
	t.nonces = append(t.nonces, NonceMark{Nonce: nonce, Index: index})
	t.byNonce[nonce] = index
	if n := len(t.nonces); n > NoncesKept {
		for _, m := range t.nonces[:n-NoncesKept] {
			if t.byNonce[m.Nonce] == m.Index {
				delete(t.byNonce, m.Nonce)
			}
		}
		t.nonces = append([]NonceMark(nil), t.nonces[n-NoncesKept:]...)
	}
}

// CommittedNonce reports whether a ref transaction carrying nonce
// committed and was accepted, and at which index, while it is retained.
func (t *Table) CommittedNonce(nonce string) (uint64, bool) {
	t.mu.RLock()
	defer t.mu.RUnlock()
	i, ok := t.byNonce[nonce]
	return i, ok && nonce != ""
}

// WriteVersion is the payload version a proposer writes: BaseVersion until
// an Audit entry raised it, once every member could read the new one.
func (t *Table) WriteVersion() uint8 {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return max(t.payload, BaseVersion)
}

// OnChange registers the projector's notification. It is called under the
// table's lock, and must neither block nor call back into the table.
func (t *Table) OnChange(f func(Change)) {
	t.mu.Lock()
	t.onChange = f
	t.mu.Unlock()
}

// Apply is the pure transition of ADR 0152 D3. A refusal is returned as a
// Result; a payload this code does not know stops the node by panicking,
// because skipping an entry forks the replica (D18).
func (t *Table) Apply(e consensus.Entry) any {
	kind, body, err := decode(e.Data)
	var version uint8
	if len(e.Data) > 0 {
		version = e.Data[0]
	}
	if err != nil && !errors.Is(err, errMalformed) {
		panic(err)
	}
	t.mu.Lock()
	var res Result
	var ch Change
	if body == nil {
		res = refuse("malformed command body")
	} else {
		switch kind {
		case KindRefTxn:
			res, ch = t.applyRefTxn(e, version, body.(*RefTxn))
		case KindAck:
			res = t.applyAck(body.(*Ack))
		case KindAudit:
			res = t.applyAudit(e, body.(*Audit))
		case KindRepoDelete:
			res, ch = t.applyRepoDelete(e, body.(*RepoDelete))
		case KindRepoRename:
			res, ch = t.applyRepoRename(e, body.(*RepoRename))
		case KindJoinGrant:
			res = t.applyJoinGrant(body.(*JoinGrant))
		case KindCheckpointBarrier:
			res = t.applyCheckpointBarrier(e, body.(*CheckpointBarrier))
		case KindCheckpointPosition:
			res = t.applyCheckpointPosition(e, body.(*CheckpointPosition))
		}
	}
	t.applied = e.Position
	if !e.Token.IsZero() {
		t.identity = e.Token.Identity()
	}
	if kind == KindCheckpointBarrier && res.OK && t.onCheckpoint != nil {
		t.onCheckpoint(body.(*CheckpointBarrier).Nonce, e.Index, func() ([]byte, error) { return json.Marshal(t.persistedLocked()) })
	}
	// The projector learns of the change before the new applied position
	// is visible to any reader, so a repository is never reported projected
	// at an index whose change it has not yet seen.
	ch.Index = e.Index
	if t.onChange != nil {
		t.onChange(ch)
	}
	t.mu.Unlock()
	return res
}

func (t *Table) repo(key string) *repoState {
	r := t.repos[key]
	if r == nil {
		r = &repoState{refs: map[string]*RefValue{}}
		t.repos[key] = r
	}
	return r
}

// setRef replaces one entry and keeps both checksums in step.
func (t *Table) setRef(repo string, r *repoState, ref string, v *RefValue) {
	if len(r.refs) > 0 {
		t.root = t.root.sub(rootTerm(repo, r.hash))
	}
	if old := r.refs[ref]; old != nil {
		r.hash = r.hash.sub(leaf(repo, ref, old))
	}
	r.refs[ref] = v
	r.hash = r.hash.add(leaf(repo, ref, v))
	t.root = t.root.add(rootTerm(repo, r.hash))
}

func (t *Table) applyRefTxn(e consensus.Entry, version uint8, tx *RefTxn) (Result, Change) {
	if err := ValidRepoKey(tx.Repo); err != nil {
		return refuse("repository: %v", err), Change{}
	}
	if version >= 2 && tx.Push {
		// A push still running when its repository was deleted or renamed
		// on another member: it must not bring the key back to life under
		// a name the database no longer has (payload version 2).
		if _, deleted := t.deleted[tx.Repo]; deleted {
			return refuse("the repository %s was deleted", tx.Repo), Change{}
		}
		if to, moved := t.movedAwayLocked(tx.Repo); moved {
			return refuse("the repository %s was renamed to %s", tx.Repo, to), Change{}
		}
	}
	if tx.Format != "sha1" && tx.Format != "sha256" {
		return refuse("unknown object format %q", tx.Format), Change{}
	}
	if len(tx.Updates) == 0 || len(tx.Updates) > MaxUpdatesPerTxn {
		return refuse("a transaction holds 1 to %d refs", MaxUpdatesPerTxn), Change{}
	}
	r := t.repos[tx.Repo]
	cur := func(ref string) *RefValue {
		if r == nil {
			return nil
		}
		return r.refs[ref]
	}
	seen := make(map[string]bool, len(tx.Updates))
	var added int64
	// Every expectation is checked before anything changes (R1-03).
	for i := range tx.Updates {
		u := &tx.Updates[i]
		if seen[u.Ref] {
			return refuse("ref %s twice in one transaction", u.Ref), Change{}
		}
		seen[u.Ref] = true
		if err := ValidRefName(u.Ref); err != nil {
			return refuse("ref %q: %v", u.Ref, err), Change{}
		}
		modes := 0
		if u.Delete {
			modes++
		}
		if u.Symbolic != "" {
			modes++
			if err := ValidRefName(u.Symbolic); err != nil || u.Symbolic == "HEAD" {
				return refuse("symbolic target of %s is invalid", u.Ref), Change{}
			}
		}
		if u.New != "" {
			modes++
			if !validSHA(u.New, tx.Format) || IsZeroSHA(u.New) {
				return refuse("ref %s: invalid object id", u.Ref), Change{}
			}
		}
		if modes != 1 {
			return refuse("ref %s: exactly one of new, symbolic or delete", u.Ref), Change{}
		}
		if u.Ref == "HEAD" && u.Symbolic == "" {
			return refuse("HEAD is held as a symbolic ref only"), Change{}
		}
		c := cur(u.Ref)
		var rev uint64
		if c != nil {
			rev = c.Rev
		}
		if u.ExpectRev != rev {
			return refuse("ref %s changed since it was read (revision %d, expected %d)", u.Ref, rev, u.ExpectRev), Change{}
		}
		if u.Old != "" && !IsZeroSHA(u.Old) {
			if c == nil || c.Tombstone || c.Symbolic != "" || c.SHA != u.Old {
				return refuse("ref %s: git expected %s, the table holds another value", u.Ref, u.Old), Change{}
			}
		}
		if u.Delete && (c == nil || c.Tombstone) {
			return refuse("ref %s does not exist", u.Ref), Change{}
		}
		ev := Event{Repo: tx.Repo, Ref: u.Ref, New: u.New, Trigger: tx.Trigger}
		added += ev.size()
		for k, v := range tx.PushOptions {
			added += int64(len(k) + len(v))
		}
	}
	if t.eventBytes+added > MaxEventBytes {
		return refuse("the cluster's event queue is full; retry once the consumer has caught up"), Change{}
	}
	if err := t.dirFileConflict(tx); err != nil {
		return refuse("%v", err), Change{}
	}

	r = t.repo(tx.Repo)
	delete(t.deleted, tx.Repo)
	revs := make([]uint64, len(tx.Updates))
	for i := range tx.Updates {
		u := &tx.Updates[i]
		var old string
		var rev uint64
		if c := r.refs[u.Ref]; c != nil {
			rev = c.Rev
			if !c.Tombstone && c.Symbolic == "" {
				old = c.SHA
			}
		}
		v := &RefValue{Rev: rev + 1}
		switch {
		case u.Delete:
			v.Tombstone = true
		case u.Symbolic != "":
			// The format rides along so a member creating the repository
			// from its first entry, often HEAD alone, uses the right one.
			v.Symbolic, v.Format = u.Symbolic, tx.Format
		default:
			v.SHA, v.Format = u.New, tx.Format
		}
		t.setRef(tx.Repo, r, u.Ref, v)
		revs[i] = v.Rev
		ev := Event{
			Token: e.Token, Kind: EventRef, Repo: tx.Repo, RepoID: tx.RepoID, Ref: u.Ref,
			Old: old, New: v.SHA, Rev: v.Rev, PusherID: tx.PusherID, Time: tx.Time,
			Trigger: tx.Trigger, PRID: tx.PRID, IsWiki: tx.IsWiki, PushOptions: tx.PushOptions,
		}
		if u.Symbolic != "" {
			ev.New = "ref:" + u.Symbolic
		}
		t.pushEvent(ev)
	}
	r.last = e.Index
	t.keepNonce(tx.Nonce, e.Index)
	return Result{OK: true, Revs: revs}, Change{Repos: []string{tx.Repo}}
}

// dirFileConflict refuses a ref whose name is a directory prefix of another
// live ref, or the reverse, on the state the transaction would leave (R7-35).
func (t *Table) dirFileConflict(tx *RefTxn) error {
	live := map[string]bool{}
	if r := t.repos[tx.Repo]; r != nil {
		for name, v := range r.refs {
			if v.Live() {
				live[name] = true
			}
		}
	}
	for _, u := range tx.Updates {
		if u.Delete {
			delete(live, u.Ref)
		} else {
			live[u.Ref] = true
		}
	}
	// The smallest live descendant of each update target, found by walking
	// each live ref's ancestors once: O(live refs x depth), not O(updates x
	// live refs), on the apply goroutine of every voter.
	targets := map[string]string{}
	for _, u := range tx.Updates {
		if !u.Delete && u.Ref != "HEAD" {
			targets[u.Ref] = ""
		}
	}
	for name := range live {
		for i := strings.LastIndexByte(name, '/'); i > 0; i = strings.LastIndexByte(name[:i], '/') {
			anc := name[:i]
			if smallest, ok := targets[anc]; ok && (smallest == "" || name < smallest) {
				targets[anc] = name
			}
		}
	}
	for _, u := range tx.Updates {
		if u.Delete || u.Ref == "HEAD" {
			continue
		}
		parts := strings.Split(u.Ref, "/")
		for i := 2; i < len(parts); i++ {
			if p := strings.Join(parts[:i], "/"); live[p] {
				return fmt.Errorf("ref %s conflicts with existing ref %s", u.Ref, p)
			}
		}
		// The smallest conflicting name is reported, so the refusal reads the
		// same on every node whatever the map's order.
		if conflict := targets[u.Ref]; conflict != "" {
			return fmt.Errorf("ref %s conflicts with existing ref %s", u.Ref, conflict)
		}
	}
	return nil
}

func (t *Table) pushEvent(ev Event) {
	t.events = append(t.events, ev)
	t.eventBytes += ev.size()
}

func (t *Table) applyAck(a *Ack) Result {
	keep := t.events[:0]
	var trimmed int
	for _, ev := range t.events {
		if ev.Token.Identity().String() == a.Identity && ev.Token.Index <= a.Index {
			t.eventBytes -= ev.size()
			trimmed++
			continue
		}
		keep = append(keep, ev)
	}
	// Clear the tail so trimmed events are not retained by the backing array.
	for i := len(keep); i < len(t.events); i++ {
		t.events[i] = Event{}
	}
	t.events = keep
	return Result{OK: true, Revs: []uint64{uint64(trimmed)}}
}

func (t *Table) applyAudit(e consensus.Entry, a *Audit) Result {
	if consensus.Operator(a.Operator).Validate() != nil || a.Action == "" || len(a.Action) > 64 ||
		len(a.Target) > 256 || len(a.Detail) > 1024 || len(a.Outcome) > 256 || len(a.ID) > 64 {
		return refuse("malformed audit record")
	}
	if a.ID != "" {
		for _, x := range t.audit {
			if x.ID == a.ID {
				return Result{OK: true} // a retry of a record already kept
			}
		}
	}
	t.audit = append(t.audit, AuditRecord{Audit: *a, Token: e.Token})
	t.payload = max(t.payload, raisedVersion(a))
	if len(t.audit) > AuditKeep {
		t.audit = append([]AuditRecord(nil), t.audit[len(t.audit)-AuditKeep:]...)
	}
	return Result{OK: true}
}

func (t *Table) applyRepoDelete(e consensus.Entry, d *RepoDelete) (Result, Change) {
	if err := ValidRepoKey(d.Repo); err != nil {
		return refuse("repository: %v", err), Change{}
	}
	r := t.repo(d.Repo)
	names := make([]string, 0, len(r.refs))
	for name, v := range r.refs {
		if v.Live() {
			names = append(names, name)
		}
	}
	sort.Strings(names)
	for _, name := range names {
		t.setRef(d.Repo, r, name, &RefValue{Rev: r.refs[name].Rev + 1, Tombstone: true})
	}
	r.last = e.Index
	t.deleted[d.Repo] = e.Index
	t.pushEvent(Event{Token: e.Token, Kind: EventRepoDelete, Repo: d.Repo, RepoID: d.RepoID, Time: d.Time})
	return Result{OK: true}, Change{Repos: []string{d.Repo}, Deleted: []string{d.Repo}}
}

func (t *Table) applyRepoRename(e consensus.Entry, rn *RepoRename) (Result, Change) {
	moves := rn.Moves()
	// All moves are checked before any is applied: one refused move leaves
	// every key as it was.
	sources, targets := map[string]bool{}, map[string]bool{}
	for _, m := range moves {
		if ValidRepoKey(m[0]) != nil || ValidRepoKey(m[1]) != nil || m[0] == m[1] {
			return refuse("invalid rename"), Change{}
		}
		if sources[m[0]] || targets[m[1]] {
			return refuse("a rename moves %s or into %s twice", m[0], m[1]), Change{}
		}
		sources[m[0]], targets[m[1]] = true, true
		if to := t.repos[m[1]]; to != nil {
			for _, v := range to.refs {
				if v.Live() {
					return refuse("repository %s already holds refs", m[1]), Change{}
				}
			}
		}
	}
	for _, m := range moves {
		if targets[m[0]] {
			return refuse("a rename chains through %s", m[0]), Change{}
		}
	}
	var ch Change
	for _, m := range moves {
		t.moveRepo(e, m[0], m[1], rn.Time)
		ch.Repos = append(ch.Repos, m[0], m[1])
		ch.Renamed = append(ch.Renamed, [2]string{m[0], m[1]})
	}
	return Result{OK: true}, ch
}

func (t *Table) moveRepo(e consensus.Entry, fromKey, toKey string, at int64) {
	from := t.repo(fromKey)
	to := t.repo(toKey)
	names := make([]string, 0, len(from.refs))
	for name := range from.refs {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		v := *from.refs[name]
		if !v.Live() {
			continue
		}
		var rev uint64
		if c := to.refs[name]; c != nil {
			rev = c.Rev
		}
		nv := v
		nv.Rev = max(rev, v.Rev) + 1
		t.setRef(toKey, to, name, &nv)
		t.setRef(fromKey, from, name, &RefValue{Rev: v.Rev + 1, Tombstone: true})
	}
	from.last, to.last = e.Index, e.Index
	delete(t.deleted, toKey)
	t.renamed[toKey] = renameMark{From: fromKey, Index: e.Index}
	t.pushEvent(Event{Token: e.Token, Kind: EventRepoRename, Repo: toKey, RenamedFrom: fromKey, Time: at})
}

func (t *Table) applyJoinGrant(g *JoinGrant) Result {
	if !validSlot(g.Slot) || consensus.Operator(g.Operator).Validate() != nil || g.Expires <= 0 {
		return refuse("malformed join grant")
	}
	if g.SPKI != "" && !(len(g.SPKI) == 64 && validSHA(g.SPKI, "sha256")) {
		return refuse("a join grant's key is 64 hex digits")
	}
	t.grants[g.Slot] = *g
	return Result{OK: true}
}

func validSlot(s string) bool {
	if len(s) == 0 || len(s) > 32 {
		return false
	}
	for i := 0; i < len(s); i++ {
		c := s[i]
		if !(c >= 'a' && c <= 'z' || c >= '0' && c <= '9' || c == '-') {
			return false
		}
	}
	return true
}

// JoinGrant returns the latest grant for a slot.
func (t *Table) JoinGrant(slot string) (JoinGrant, bool) {
	t.mu.RLock()
	defer t.mu.RUnlock()
	g, ok := t.grants[slot]
	return g, ok
}

// ---- reads ----

// Get returns one entry of the table.
func (t *Table) Get(repo, ref string) (RefValue, bool) {
	t.mu.RLock()
	defer t.mu.RUnlock()
	if r := t.repos[repo]; r != nil {
		if v := r.refs[ref]; v != nil {
			return *v, true
		}
	}
	return RefValue{}, false
}

// Refs copies a repository's entries, tombstones included, with the index
// the copy is consistent with.
func (t *Table) Refs(repo string) (map[string]RefValue, uint64) {
	t.mu.RLock()
	defer t.mu.RUnlock()
	out := map[string]RefValue{}
	if r := t.repos[repo]; r != nil {
		for k, v := range r.refs {
			out[k] = *v
		}
	}
	return out, t.applied.Index
}

// RepoHash returns a repository's checksum and the index of its last change.
func (t *Table) RepoHash(repo string) (Sum, uint64) {
	t.mu.RLock()
	defer t.mu.RUnlock()
	if r := t.repos[repo]; r != nil {
		return r.hash, r.last
	}
	return Sum{}, 0
}

// Root returns the table's checksum.
func (t *Table) Root() Sum {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.root
}

// Applied returns the last applied position and its lineage.
func (t *Table) Applied() (consensus.Position, consensus.LogIdentity) {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.applied, t.identity
}

// Repos lists every repository the table knows, sorted.
func (t *Table) Repos() []string {
	t.mu.RLock()
	defer t.mu.RUnlock()
	out := make([]string, 0, len(t.repos))
	for k := range t.repos {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

// Deleted reports whether a repository is deleted, and since which index.
func (t *Table) Deleted(repo string) (uint64, bool) {
	t.mu.RLock()
	defer t.mu.RUnlock()
	i, ok := t.deleted[repo]
	return i, ok
}

// RenamedFrom reports the key a repository was renamed from.
func (t *Table) RenamedFrom(repo string) (string, uint64, bool) {
	t.mu.RLock()
	defer t.mu.RUnlock()
	m, ok := t.renamed[repo]
	return m.From, m.Index, ok
}

// MovedAway reports where a rename moved repo's refs, when it did and no
// live ref was written under repo since: the key is free, and only a new
// repository may take it again. The newest rename wins.
func (t *Table) MovedAway(repo string) (string, bool) {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.movedAwayLocked(repo)
}

// raisedVersion is the payload version an audit record raises the cluster
// to, 0 when it raises none. A version this code cannot apply is not
// adopted: its entries would stop this node anyway.
func raisedVersion(a *Audit) uint8 {
	if a.Action != AuditPayloadVersion || a.Outcome != AuditRaised {
		return 0
	}
	v, err := strconv.ParseUint(a.Target, 10, 8)
	if err != nil || v <= BaseVersion || v > PayloadVersion {
		return 0
	}
	return uint8(v)
}

func (t *Table) movedAwayLocked(repo string) (string, bool) {
	if r := t.repos[repo]; r != nil {
		for _, v := range r.refs {
			if v.Live() {
				return "", false
			}
		}
	}
	to, at, found := "", uint64(0), false
	for k, m := range t.renamed {
		if m.From == repo && (!found || m.Index > at || m.Index == at && k < to) {
			to, at, found = k, m.Index, true
		}
	}
	return to, found
}

// Events returns the pending events, in log order.
func (t *Table) Events() []Event {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return append([]Event(nil), t.events...)
}

// EventCount is the number of events not yet trimmed, without copying them.
func (t *Table) EventCount() int {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return len(t.events)
}

// EventBytes is the queue's accounted size, for the hook's pre-check.
func (t *Table) EventBytes() int64 {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.eventBytes
}

// HasAudit reports whether a record with this id is retained.
func (t *Table) HasAudit(id string) bool {
	t.mu.RLock()
	defer t.mu.RUnlock()
	for _, a := range t.audit {
		if a.ID == id {
			return true
		}
	}
	return false
}

// Audit returns the retained audit records, newest first.
func (t *Table) Audit() []AuditRecord {
	t.mu.RLock()
	defer t.mu.RUnlock()
	out := make([]AuditRecord, len(t.audit))
	for i, a := range t.audit {
		out[len(t.audit)-1-i] = a
	}
	return out
}

// PinnedObjects lists the old and new ids of every event not yet trimmed,
// which gc must keep (R8-27).
func (t *Table) PinnedObjects(repo string) []string {
	t.mu.RLock()
	defer t.mu.RUnlock()
	var out []string
	for _, ev := range t.events {
		if ev.Repo != repo {
			continue
		}
		if ev.Old != "" {
			out = append(out, ev.Old)
		}
		if ev.New != "" && !strings.HasPrefix(ev.New, "ref:") {
			out = append(out, ev.New)
		}
	}
	return out
}

// ---- snapshots ----

// SnapshotFormat is the version of the snapshot encoding.
const SnapshotFormat = 1

type persisted struct {
	Format   int                             `json:"format"`
	Applied  consensus.Position              `json:"applied"`
	Identity consensus.LogIdentity           `json:"identity"`
	Repos    map[string]map[string]*RefValue `json:"repos"`
	Last     map[string]uint64               `json:"last"`
	Events   []Event                         `json:"events"`
	Audit    []AuditRecord                   `json:"audit"`
	Deleted  map[string]uint64               `json:"deleted"`
	Renamed  map[string]renameMark           `json:"renamed"`
	Grants   map[string]JoinGrant            `json:"grants,omitempty"`
	// Pending and Checkpoints are absent from snapshots older than the
	// checkpoints, which then restore with none.
	Pending     *pendingCheckpoint `json:"pending,omitempty"`
	Checkpoints []CheckpointRecord `json:"checkpoints,omitempty"`
	// Payload and Nonces are absent from older snapshots: the payload
	// version is then read back from the audit records kept.
	Payload uint8       `json:"payload,omitempty"`
	Nonces  []NonceMark `json:"nonces,omitempty"`
}

type bytesSnapshot struct{ b []byte }

func (s *bytesSnapshot) WriteTo(w io.Writer) (int64, error) {
	n, err := w.Write(s.b)
	return int64(n), err
}
func (s *bytesSnapshot) Release() { s.b = nil }

// Snapshot encodes the table while Apply is paused: the table is not a
// copy-on-write store, so it declares SnapshotPausesApply (ADR 0152 D3,
// R2-34) and the cut costs O(state) on the apply goroutine.
func (t *Table) Snapshot() (consensus.Snapshot, error) {
	t.mu.RLock()
	b, err := json.Marshal(t.persistedLocked())
	t.mu.RUnlock()
	if err != nil {
		return nil, err
	}
	return &bytesSnapshot{b: b}, nil
}

// persistedLocked is the table's snapshot form; the caller holds the lock.
func (t *Table) persistedLocked() *persisted {
	p := &persisted{
		Format: SnapshotFormat, Applied: t.applied, Identity: t.identity,
		Repos: make(map[string]map[string]*RefValue, len(t.repos)), Last: map[string]uint64{},
		Events: t.events, Audit: t.audit, Deleted: t.deleted, Renamed: t.renamed, Grants: t.grants,
		Pending: t.pending, Checkpoints: t.checkpoints, Payload: t.payload, Nonces: t.nonces,
	}
	for k, r := range t.repos {
		p.Repos[k] = r.refs
		p.Last[k] = r.last
	}
	return p
}

// Restore replaces the table with a snapshot and recomputes both checksums.
func (t *Table) Restore(r io.Reader, _ consensus.SnapshotMeta) error {
	var buf bytes.Buffer
	if _, err := buf.ReadFrom(r); err != nil {
		return err
	}
	var p persisted
	if err := json.Unmarshal(buf.Bytes(), &p); err != nil {
		return fmt.Errorf("decode ref table snapshot: %w", err)
	}
	if p.Format != SnapshotFormat {
		return fmt.Errorf("%w: snapshot format %d", ErrVersion, p.Format)
	}
	nt := New()
	for repo, refs := range p.Repos {
		rs := nt.repo(repo)
		rs.last = p.Last[repo]
		for name, v := range refs {
			rs.refs[name] = v
			rs.hash = rs.hash.add(leaf(repo, name, v))
		}
		if len(rs.refs) > 0 {
			nt.root = nt.root.add(rootTerm(repo, rs.hash))
		}
	}
	for _, ev := range p.Events {
		nt.pushEvent(ev)
	}
	nt.audit = p.Audit
	nt.payload = p.Payload
	for i := range nt.audit {
		nt.payload = max(nt.payload, raisedVersion(&nt.audit[i].Audit))
	}
	for _, m := range p.Nonces {
		nt.keepNonce(m.Nonce, m.Index)
	}
	if p.Deleted != nil {
		nt.deleted = p.Deleted
	}
	if p.Renamed != nil {
		nt.renamed = p.Renamed
	}
	if p.Grants != nil {
		nt.grants = p.Grants
	}
	t.mu.Lock()
	t.repos, t.root, t.events, t.eventBytes = nt.repos, nt.root, nt.events, nt.eventBytes
	t.audit, t.deleted, t.renamed, t.grants = nt.audit, nt.deleted, nt.renamed, nt.grants
	t.applied, t.identity = p.Applied, p.Identity
	t.pending, t.checkpoints = p.Pending, p.Checkpoints
	t.payload, t.nonces, t.byNonce = nt.payload, nt.nonces, nt.byNonce
	if t.onChange != nil {
		repos := make([]string, 0, len(t.repos))
		for k := range t.repos {
			repos = append(repos, k)
		}
		sort.Strings(repos)
		t.onChange(Change{Index: p.Applied.Index, Repos: repos})
	}
	t.mu.Unlock()
	return nil
}
