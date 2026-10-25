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
	"slices"
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
	// PusherName is the pusher's name when it pushed, from payload
	// version 4 (PusherVersion); empty before. It is not counted in the
	// queue's size, which stays what an older binary computes.
	PusherName string `json:"pn,omitempty"`
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
	renamed map[string]renameMark
	// renames keeps the newest renames into each key, oldest first, also
	// once another rename took the key again: a walk along a chain of
	// renames finds the rename into a key before a given index (#138). No
	// verdict reads it, so it is kept at every version.
	renames map[string][]renameMark
	// ids maps a key to the database ID of the repository it holds, as the
	// newest ref transaction carrying one gave it; a rename moves it, a
	// deletion forgets it. The projector stamps a directory with it, and a
	// read compares it to the database's (#82). No verdict reads it, so it
	// is kept at every version.
	ids    map[string]int64
	grants map[string]JoinGrant
	// revoked maps a revoked key, 64 hex digits, to its revocation;
	// pendingRevoke the revocations an operator asked for before the
	// cluster could write them (AuditRevokeKey), by key.
	revoked       map[string]RevokeKey
	pendingRevoke map[string]RevokeKey
	// revokeRequests lists, for each key of pendingRevoke, the requests
	// open for it by the index of their commit, oldest first, 0 standing
	// for every request committed before WithdrawVersion, which no
	// withdrawal names (#171). The key stays asked for while one is open.
	revokeRequests map[string][]uint64
	applied        consensus.Position
	identity       consensus.LogIdentity
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
	// deletions keeps the indexes of each key's newest deletions, oldest
	// first, also once the key was written again: the projector tells by
	// them which repository a directory under the key holds (#52, #98). No
	// verdict reads it, so it is kept at every version.
	deletions map[string][]uint64
	// departedAt is the index at which the repository under a key last
	// left it, deleted or renamed away, written by GenerationVersion
	// entries only and read by their verdicts: Apply refuses a ref
	// transaction read before it, even once another repository took the
	// key (#91).
	departedAt map[string]uint64
	// limit is the highest payload version this table applies:
	// PayloadVersion, but for the tests that play an older binary.
	limit uint8

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
		repos:          map[string]*repoState{},
		deleted:        map[string]uint64{},
		renamed:        map[string]renameMark{},
		renames:        map[string][]renameMark{},
		ids:            map[string]int64{},
		grants:         map[string]JoinGrant{},
		revoked:        map[string]RevokeKey{},
		pendingRevoke:  map[string]RevokeKey{},
		revokeRequests: map[string][]uint64{},
		byNonce:        map[string]uint64{},
		deletions:      map[string][]uint64{},
		departedAt:     map[string]uint64{},
		limit:          PayloadVersion,
	}
}

// keptPerKey bounds the deletions kept per key.
const keptPerKey = 16

func keepLast[T any](s []T, v T) []T {
	s = append(s, v)
	if len(s) > keptPerKey {
		s = append([]T(nil), s[len(s)-keptPerKey:]...)
	}
	return s
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
	if t.replayed(e) {
		// An entry of this lineage at or below the applied index is a
		// replay: applying it again on the table that already holds it
		// diverges, a deletion or a rename being unconditional (#41).
		return refuse("entry %d is already applied", e.Index)
	}
	kind, body, err := decode(e.Data, t.limit)
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
			res, ch = t.applyRepoDelete(e, version, body.(*RepoDelete))
		case KindRepoRename:
			res, ch = t.applyRepoRename(e, version, body.(*RepoRename))
		case KindJoinGrant:
			res = t.applyJoinGrant(version, body.(*JoinGrant))
		case KindCheckpointBarrier:
			res = t.applyCheckpointBarrier(e, body.(*CheckpointBarrier))
		case KindCheckpointPosition:
			res = t.applyCheckpointPosition(e, body.(*CheckpointPosition))
		case KindRevokeKey:
			if version < RevokeVersion {
				res = refuse("a key revocation is written at payload version %d", RevokeVersion)
				break
			}
			res = t.applyRevokeKey(body.(*RevokeKey))
		case KindWithdrawRevocation:
			if version < WithdrawVersion {
				res = refuse("a revocation is withdrawn at payload version %d", WithdrawVersion)
				break
			}
			res = t.applyWithdrawRevocation(body.(*WithdrawRevocation))
		case KindOwnerRename:
			if version < OwnerRenameVersion {
				res = refuse("an owner is renamed by its names at payload version %d", OwnerRenameVersion)
				break
			}
			res, ch = t.applyOwnerRename(e, version, body.(*OwnerRename))
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

// replayed reports whether e belongs to the lineage the table applied and
// lies at or below its applied index.
func (t *Table) replayed(e consensus.Entry) bool {
	if e.Token.IsZero() {
		return false
	}
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.applied.Index > 0 && e.Index <= t.applied.Index && e.Token.Identity() == t.identity
}

// Reset empties the table, before a node that rejoins under a new id opens
// an engine that replays the log from its start (#41). The projector is
// not told: it is fenced until the new engine applied a barrier.
func (t *Table) Reset() {
	nt := New()
	t.mu.Lock()
	t.repos, t.root, t.events, t.eventBytes = nt.repos, nt.root, nil, 0
	t.audit, t.deleted, t.renamed, t.grants, t.revoked = nil, nt.deleted, nt.renamed, nt.grants, nt.revoked
	t.pendingRevoke, t.revokeRequests = nt.pendingRevoke, nt.revokeRequests
	t.applied, t.identity = consensus.Position{}, consensus.LogIdentity{}
	t.pending, t.checkpoints = nil, nil
	t.payload, t.nonces, t.byNonce = 0, nil, nt.byNonce
	t.deletions, t.departedAt, t.renames, t.ids = nt.deletions, nt.departedAt, nt.renames, nt.ids
	t.mu.Unlock()
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
	if err := ValidRepoKeyAt(tx.Repo, version); err != nil {
		return refuse("repository: %v", err), Change{}
	}
	if version >= GenerationVersion {
		if why := t.generationRefuses(tx, version); why != "" {
			return refuse("%s", why), Change{}
		}
	} else if version >= 2 && tx.Push {
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
	pusherName := ""
	if version >= PusherVersion {
		if len(tx.PusherName) > MaxPusherName {
			return refuse("a pusher's name holds at most %d bytes", MaxPusherName), Change{}
		}
		pusherName = tx.PusherName
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
			PusherName: pusherName,
		}
		if u.Symbolic != "" {
			ev.New = "ref:" + u.Symbolic
		}
		t.pushEvent(ev)
	}
	r.last = e.Index
	if tx.RepoID > 0 {
		t.ids[tx.Repo] = tx.RepoID
	}
	t.keepNonce(tx.Nonce, e.Index)
	return Result{OK: true, Revs: revs}, Change{Repos: []string{tx.Repo}}
}

// generationRefuses is the GenerationVersion rule of a ref transaction, ""
// when it passes. Version 2 refused a push into a deleted or renamed-away
// key, and only a push: a mirror sync, a ref Forgejo writes or an API tag
// in flight during a deletion brought the deleted repository back (#77).
// A key that was deleted or renamed away now takes only a creation, the
// HEAD that git init writes; whatever comes later finds the key live.
// Version 2 also lost its guard once the name was taken again: a push
// prepared against the old repository landed in the new one (#91). A
// transaction read before the key's repository left it is refused now,
// a creation aside, as is one of another object format than the key's
// live refs. From InitVersion a creation is git init's HEAD only: a change
// of the default branch run in the directory of a deleted repository the
// projector had not removed yet brought the key back (#136).
func (t *Table) generationRefuses(tx *RefTxn, version uint8) string {
	_, deleted := t.deleted[tx.Repo]
	to, moved := t.movedAwayLocked(tx.Repo)
	creation := creates(tx) && (version < InitVersion || tx.Init)
	if (deleted || moved) && !creation {
		if deleted {
			return fmt.Sprintf("the repository %s was deleted", tx.Repo)
		}
		return fmt.Sprintf("the repository %s was renamed to %s", tx.Repo, to)
	}
	if at := max(t.deleted[tx.Repo], t.departedAt[tx.Repo]); tx.ReadAt != 0 && tx.ReadAt < at && !creation {
		return fmt.Sprintf("the transaction was read at %d, before the repository %s was deleted or renamed away at %d: retry", tx.ReadAt, tx.Repo, at)
	}
	if r := t.repos[tx.Repo]; r != nil {
		names := make([]string, 0, len(r.refs))
		for name, v := range r.refs {
			if v.Live() && v.Format != "" && v.Format != tx.Format {
				names = append(names, name)
			}
		}
		if len(names) > 0 {
			// The smallest name, so the refusal reads the same on every node.
			sort.Strings(names)
			return fmt.Sprintf("the repository %s holds %s objects (%s), the transaction %s ones", tx.Repo, r.refs[names[0]].Format, names[0], tx.Format)
		}
	}
	return ""
}

// creates reports whether a transaction is a repository's creation: git
// init writes HEAD, symbolic, alone. A fork and a migration clone into a
// directory git init made first, so their refs find the key live.
func creates(tx *RefTxn) bool {
	for _, u := range tx.Updates {
		if u.Ref != "HEAD" || u.Symbolic == "" {
			return false
		}
	}
	return len(tx.Updates) > 0
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
	t.payload = max(t.payload, adoptedVersion(a, t.limit))
	if k, ok := pendingRevocation(a); ok {
		if _, done := t.revoked[k.SPKI]; !done {
			t.pendingRevoke[k.SPKI] = k
			// From WithdrawVersion a request is numbered by its commit,
			// which a withdrawal names; one committed before is not, and
			// stays until the revocation is written.
			var n uint64
			if t.payload >= WithdrawVersion {
				n = e.Index
			}
			t.revokeRequests[k.SPKI] = openRequest(t.revokeRequests[k.SPKI], n)
		}
	}
	if len(t.audit) > AuditKeep {
		t.audit = append([]AuditRecord(nil), t.audit[len(t.audit)-AuditKeep:]...)
	}
	return Result{OK: true}
}

func (t *Table) applyRepoDelete(e consensus.Entry, version uint8, d *RepoDelete) (Result, Change) {
	if err := ValidRepoKeyAt(d.Repo, version); err != nil {
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
	t.deletions[d.Repo] = keepLast(t.deletions[d.Repo], e.Index)
	delete(t.ids, d.Repo)
	if version >= GenerationVersion {
		t.departedAt[d.Repo] = e.Index
		t.keepNonce(d.Nonce, e.Index)
	}
	t.pushEvent(Event{Token: e.Token, Kind: EventRepoDelete, Repo: d.Repo, RepoID: d.RepoID, Time: d.Time})
	return Result{OK: true}, Change{Repos: []string{d.Repo}, Deleted: []string{d.Repo}}
}

func (t *Table) applyRepoRename(e consensus.Entry, version uint8, rn *RepoRename) (Result, Change) {
	moves := rn.Moves()
	// All moves are checked before any is applied: one refused move leaves
	// every key as it was.
	sources, targets := map[string]bool{}, map[string]bool{}
	for _, m := range moves {
		if ValidRepoKeyAt(m[0], version) != nil || ValidRepoKeyAt(m[1], version) != nil || m[0] == m[1] {
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
	if version >= RenameSourceVersion && rn.Owner == [2]string{} {
		// An owner's rename moves the keys its owner holds at this index,
		// checked by ownerRenameRefuses; a repository's rename moves the
		// one it names, which must still be there (#220).
		if why := t.renameSourceRefusesLocked(moves[0][0]); why != "" {
			return refuse("%s", why), Change{}
		}
	}
	if version >= GenerationVersion && rn.Owner != [2]string{} {
		if why := t.ownerRenameRefuses(rn.Owner, moves, sources); why != "" {
			return refuse("%s", why), Change{}
		}
	}
	var ch Change
	if version >= GenerationVersion {
		t.keepNonce(rn.Nonce, e.Index)
	}
	for _, m := range moves {
		if version >= GenerationVersion {
			t.departedAt[m[0]] = e.Index
		}
		t.moveRepo(e, m[0], m[1], rn.Time)
		ch.Repos = append(ch.Repos, m[0], m[1])
		ch.Renamed = append(ch.Renamed, [2]string{m[0], m[1]})
	}
	return Result{OK: true}, ch
}

// RenameSourceRefusal says why a repository's rename from key would move
// nothing the database names, "" when it may be proposed: the repository
// under key was deleted, or renamed away, and no live ref was written
// under key since. Forgejo moved the directory of a repository it loaded
// before that, on a node whose disk was behind, and the rename recorded
// empty left the repository under a key the database no longer knows
// (#220). A key the table never held passes: there is nothing to lose.
// It is Apply's check from RenameSourceVersion, which the proposer makes
// on its confirmed table at every version.
func (t *Table) RenameSourceRefusal(key string) string {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.renameSourceRefusesLocked(key)
}

// renameSourceRefusesLocked is RenameSourceRefusal; the caller holds the
// lock. Its reason reads the same on every node: movedAwayLocked picks the
// newest rename, the smallest key on a tie.
func (t *Table) renameSourceRefusesLocked(key string) string {
	if _, deleted := t.deleted[key]; deleted {
		return fmt.Sprintf("the repository %s was deleted", key)
	}
	if to, moved := t.movedAwayLocked(key); moved {
		return fmt.Sprintf("the repository %s was renamed to %s", key, to)
	}
	return ""
}

// ownerRenameRefuses checks an owner's rename against the table at its
// index, "" when it passes: every move goes from the old owner to the new
// one, and every key under the old owner with a live ref moves. The
// proposer lists the keys in its own table, which may be behind the log: a
// repository created just before would stay under the old owner while the
// database renamed it (#56). The refusal is retried on a table that caught
// up.
func (t *Table) ownerRenameRefuses(owner [2]string, moves [][2]string, sources map[string]bool) string {
	from, to := owner[0]+"/", owner[1]+"/"
	for _, m := range moves {
		if !strings.HasPrefix(m[0], from) || m[1] != to+strings.TrimPrefix(m[0], from) {
			return fmt.Sprintf("the rename of owner %s moves %s to %s", owner[0], m[0], m[1])
		}
	}
	var keys []string
	for key, r := range t.repos {
		if !strings.HasPrefix(key, from) || sources[key] {
			continue
		}
		for _, v := range r.refs {
			if v.Live() {
				keys = append(keys, key)
				break
			}
		}
	}
	if len(keys) > 0 {
		sort.Strings(keys)
		return fmt.Sprintf("the owner %s holds %s, which the rename does not move", owner[0], keys[0])
	}
	return ""
}

// applyOwnerRename moves every key under the owner o.From that holds a
// repository to the owner o.To: it is the RepoRename an owner's rename lists
// for that owner on this table at this index, the same moves in the same
// order, applied as that RepoRename, so the marks, the events, the
// checksums and the snapshot are the ones it leaves (#182). The keys come
// from this table alone, never from the proposer's: every member expands
// the entry at the same index of the same log into the same moves. The
// expansion reads memory only, in one pass over the table's keys, beside
// the one ownerRenameRefuses makes for that RepoRename, and holds the
// owner's moves, which that RepoRename's body held decoded.
func (t *Table) applyOwnerRename(e consensus.Entry, version uint8, o *OwnerRename) (Result, Change) {
	if validOwnerAt(o.From, version) != nil || validOwnerAt(o.To, version) != nil || o.From == o.To {
		return refuse("invalid owner rename"), Change{}
	}
	keys := t.ownerKeysLocked(o.From)
	if len(keys) == 0 {
		// Nothing left to move: the owner's repositories went since its
		// proposer listed them. The rename stands, as in the database.
		t.keepNonce(o.Nonce, e.Index)
		return Result{OK: true}, Change{}
	}
	moves := make([][2]string, len(keys))
	for i, key := range keys {
		moves[i] = [2]string{key, o.To + strings.TrimPrefix(key, o.From)}
	}
	return t.applyRepoRename(e, version, &RepoRename{
		From: moves[0][0], To: moves[0][1], Time: o.Time, Also: moves[1:], Nonce: o.Nonce, Owner: [2]string{o.From, o.To},
	})
}

// validOwnerAt checks an owner's name as the first component of a key that
// Apply admits at payload version v.
func validOwnerAt(owner string, v uint8) error {
	return ValidRepoKeyAt(owner+"/o.git", v)
}

// OwnerKeys lists, sorted, the keys under owner that hold a repository,
// those an owner's rename moves: a live ref at least, and no deletion
// since. A key a rename moved away holds none, nor the key of a wiki never
// created, which a repository's rename moves with it (#181).
func (t *Table) OwnerKeys(owner string) []string {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.ownerKeysLocked(owner)
}

func (t *Table) ownerKeysLocked(owner string) []string {
	prefix := owner + "/"
	var keys []string
	for key, r := range t.repos {
		if !strings.HasPrefix(key, prefix) {
			continue
		}
		if _, deleted := t.deleted[key]; deleted {
			continue
		}
		for _, v := range r.refs {
			if v.Live() {
				keys = append(keys, key)
				break
			}
		}
	}
	sort.Strings(keys)
	return keys
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
	if id, ok := t.ids[fromKey]; ok {
		t.ids[toKey] = id
	} else {
		delete(t.ids, toKey)
	}
	delete(t.ids, fromKey)
	// The history starts from the marks already known: a table restored
	// from a snapshot cut before it was kept holds the newest one in
	// renamed only.
	marks := t.marksLocked(toKey)
	t.renamed[toKey] = renameMark{From: fromKey, Index: e.Index}
	t.renames[toKey] = keepLast(marks, renameMark{From: fromKey, Index: e.Index})
	t.pushEvent(Event{Token: e.Token, Kind: EventRepoRename, Repo: toKey, RenamedFrom: fromKey, Time: at})
}

func (t *Table) applyJoinGrant(version uint8, g *JoinGrant) Result {
	if !validSlot(g.Slot) || consensus.Operator(g.Operator).Validate() != nil || g.Expires <= 0 {
		return refuse("malformed join grant")
	}
	if g.SPKI != "" && !(len(g.SPKI) == 64 && validSHA(g.SPKI, "sha256")) {
		return refuse("a join grant's key is 64 hex digits")
	}
	if cur, ok := t.grants[g.Slot]; version >= GenerationVersion && ok && cur.SPKI != "" && g.SPKI != cur.SPKI && cur.Expires == g.Expires {
		// Two members binding the same grant to two joiners at once: the
		// first binding stands, the second joiner is refused (#70). An
		// operator's new grant carries a new expiry.
		return refuse("the join grant for slot %s is already bound to another key", g.Slot)
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

func (t *Table) applyRevokeKey(k *RevokeKey) Result {
	if !validSlot(k.Slot) || consensus.Operator(k.Operator).Validate() != nil || !(len(k.SPKI) == 64 && validSHA(k.SPKI, "sha256")) {
		return refuse("malformed key revocation")
	}
	if _, ok := t.revoked[k.SPKI]; !ok {
		t.revoked[k.SPKI] = *k
	}
	delete(t.pendingRevoke, k.SPKI)
	delete(t.revokeRequests, k.SPKI)
	// A grant bound to the revoked key admits nothing any more.
	if g, ok := t.grants[k.Slot]; ok && g.SPKI == k.SPKI {
		delete(t.grants, k.Slot)
	}
	return Result{OK: true}
}

// applyWithdrawRevocation withdraws the revocation request of w.SPKI
// numbered w.Index: its Remove or DenyPin was refused for certain (#171).
// The key stays asked for while another request of it is open, and a
// request numbered otherwise, or a key already revoked, is not touched.
func (t *Table) applyWithdrawRevocation(w *WithdrawRevocation) Result {
	if consensus.Operator(w.Operator).Validate() != nil || !(len(w.SPKI) == 64 && validSHA(w.SPKI, "sha256")) || w.Index == 0 {
		return refuse("malformed revocation withdrawal")
	}
	reqs := t.revokeRequests[w.SPKI]
	i := slices.Index(reqs, w.Index)
	if i < 0 {
		return refuse("no revocation of this key was asked at %d, or it is written", w.Index)
	}
	reqs = slices.Delete(slices.Clone(reqs), i, i+1)
	if len(reqs) == 0 {
		delete(t.revokeRequests, w.SPKI)
		delete(t.pendingRevoke, w.SPKI)
		return Result{OK: true}
	}
	t.revokeRequests[w.SPKI] = reqs
	return Result{OK: true}
}

// openRequest adds request n to the open requests of a key once: the
// requests committed before WithdrawVersion are all 0.
func openRequest(reqs []uint64, n uint64) []uint64 {
	if slices.Contains(reqs, n) {
		return reqs
	}
	return append(slices.Clone(reqs), n)
}

// pendingRevocation reads an AuditRevokeKey record; ok is false for any
// other record.
func pendingRevocation(a *Audit) (RevokeKey, bool) {
	if a.Action != AuditRevokeKey || a.Outcome != AuditPending {
		return RevokeKey{}, false
	}
	slot, spki, _ := strings.Cut(a.Target, ":")
	if !validSlot(slot) || !(len(spki) == 64 && validSHA(spki, "sha256")) {
		return RevokeKey{}, false
	}
	return RevokeKey{SPKI: spki, Slot: slot, Operator: a.Operator, Time: a.Time}, true
}

// PendingRevocations lists the revocations asked for and not yet written,
// by key.
func (t *Table) PendingRevocations() []RevokeKey {
	t.mu.RLock()
	defer t.mu.RUnlock()
	out := make([]RevokeKey, 0, len(t.pendingRevoke))
	for _, k := range t.pendingRevoke {
		out = append(out, k)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].SPKI < out[j].SPKI })
	return out
}

// RevocationRequests lists the open revocation requests of spki by the
// index of their commit, 0 for those committed before WithdrawVersion.
func (t *Table) RevocationRequests(spki string) []uint64 {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return slices.Clone(t.revokeRequests[spki])
}

// AuditIndex reports the index at which the audit record id committed,
// while the table retains it.
func (t *Table) AuditIndex(id string) (uint64, bool) {
	t.mu.RLock()
	defer t.mu.RUnlock()
	for _, a := range t.audit {
		if a.ID == id && id != "" {
			return a.Token.Index, true
		}
	}
	return 0, false
}

// KeyRevoked reports whether an operator revoked spki, 64 hex digits.
func (t *Table) KeyRevoked(spki string) bool {
	t.mu.RLock()
	defer t.mu.RUnlock()
	_, ok := t.revoked[spki]
	return ok
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

// RefsWithID is Refs with the database ID of the repository the key
// holds at the same index, 0 when no transaction carrying one wrote it.
func (t *Table) RefsWithID(repo string) (map[string]RefValue, int64, uint64) {
	t.mu.RLock()
	defer t.mu.RUnlock()
	out := map[string]RefValue{}
	if r := t.repos[repo]; r != nil {
		for k, v := range r.refs {
			out[k] = *v
		}
	}
	return out, t.ids[repo], t.applied.Index
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

// DeletedBefore reports the newest deletion of repo at an index below
// before, kept also once the key was written again; deletions older than
// the last few are forgotten.
func (t *Table) DeletedBefore(repo string, before uint64) (uint64, bool) {
	t.mu.RLock()
	defer t.mu.RUnlock()
	for _, at := range slices.Backward(t.deletions[repo]) {
		if at < before {
			return at, true
		}
	}
	return 0, false
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

// RenamedInto reports the key the newest rename before index moved from's
// refs to, while that key still names from as its origin: in a chain of
// renames (from to Z, then another key to from), the directory a node still
// holds under from belongs to Z until it is moved there.
func (t *Table) RenamedInto(from string, before uint64) (string, uint64, bool) {
	t.mu.RLock()
	defer t.mu.RUnlock()
	to, at, found := "", uint64(0), false
	for k, m := range t.renamed {
		if m.From == from && m.Index < before && (!found || m.Index > at || m.Index == at && k < to) {
			to, at, found = k, m.Index, true
		}
	}
	return to, at, found
}

// RenamedAfter reports the oldest rename after index that moved from's refs
// to another key: the way forward from an event written under from.
//
// It reads every rename kept, not only the newest into each key: a key
// freed by a rename and taken again by another one still leads forward
// from an event written before (#138).
func (t *Table) RenamedAfter(from string, after uint64) (string, uint64, bool) {
	t.mu.RLock()
	defer t.mu.RUnlock()
	to, at, found := "", uint64(0), false
	for k := range t.renameTargetsLocked() {
		for _, m := range t.marksLocked(k) {
			if m.From == from && m.Index > after && (!found || m.Index < at || m.Index == at && k < to) {
				to, at, found = k, m.Index, true
			}
		}
	}
	return to, at, found
}

// RenamedFromBefore reports the newest rename into repo before index: the
// way back from repo along a chain of renames, also once another rename
// took repo again after it (#138).
func (t *Table) RenamedFromBefore(repo string, before uint64) (string, uint64, bool) {
	t.mu.RLock()
	defer t.mu.RUnlock()
	for _, m := range slices.Backward(t.marksLocked(repo)) {
		if m.Index < before {
			return m.From, m.Index, true
		}
	}
	return "", 0, false
}

// marksLocked is the renames into to kept, oldest first. A table restored
// from a snapshot cut before the history was kept knows the newest one
// only.
func (t *Table) marksLocked(to string) []renameMark {
	h := t.renames[to]
	if m, ok := t.renamed[to]; ok && (len(h) == 0 || h[len(h)-1] != m) {
		h = append(slices.Clip(h), m)
	}
	return h
}

// renameTargetsLocked is every key a rename moved a repository into.
func (t *Table) renameTargetsLocked() map[string]struct{} {
	keys := make(map[string]struct{}, len(t.renamed))
	for k := range t.renamed {
		keys[k] = struct{}{}
	}
	for k := range t.renames {
		keys[k] = struct{}{}
	}
	return keys
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

// adoptedVersion is raisedVersion on a binary applying versions up to limit.
func adoptedVersion(a *Audit, limit uint8) uint8 {
	if v := raisedVersion(a); v <= limit {
		return v
	}
	return 0
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

// EventBytesBetween is the accounted size of the events of a lineage after
// index from, up to and including index to: what a trim at to frees that a
// trim at from does not. It reads the queue, it changes nothing.
func (t *Table) EventBytesBetween(lineage string, from, to uint64) int64 {
	t.mu.RLock()
	defer t.mu.RUnlock()
	var n int64
	// The lineage is compared as a string once per identity met, not per
	// event: the queue may hold a few hundred thousand of them.
	var last consensus.LogIdentity
	same, met := false, false
	for i := range t.events {
		ev := &t.events[i]
		if ev.Token.Index <= from || ev.Token.Index > to {
			continue
		}
		if id := ev.Token.Identity(); !met || id != last {
			last, same, met = id, id.String() == lineage, true
		}
		if same {
			n += ev.size()
		}
	}
	return n
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
// which gc must keep (R8-27): of every lineage, dispatched or not. These
// are the "unconsumed" events of the projector's gc gate: an event the
// consumer dispatched stays while its work may run again (the margin
// behind the cursor, a parked event before it, the backup cursor), and a
// replay reads these objects. A lineage a Recover or a restore ended is
// trimmed to its end once nothing can replay it (#205): its last events
// do not pin their objects for good.
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

// SnapshotFormat is the newest version of the snapshot encoding. Format 1
// is written while the cluster writes a payload version below
// GenerationVersion, format 2 from then on: a binary that cannot keep the
// state of these versions (the departures of the keys, the revocations of
// an older one still) refuses format 2 and stops, rather than restore without error
// and lose it, then hand its own cut to a current member (#50). Format 2
// is format 1's encoding.
const SnapshotFormat = 2

// snapshotFormat is the format a table at payload version payload writes.
func snapshotFormat(payload uint8) int {
	if payload >= GenerationVersion {
		return 2
	}
	return 1
}

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
	Revoked  map[string]RevokeKey            `json:"revoked,omitempty"`
	// PendingRevoke is written even empty: an empty map left out read as a
	// snapshot of a binary older than it, and a request withdrawn on the
	// log came back from the audit records kept (#223). It is absent from
	// a snapshot an older member cut, and from one that left it out empty:
	// below WithdrawVersion it is then read back from the audit records
	// kept, from it it is empty (Restore).
	PendingRevoke map[string]RevokeKey `json:"pending_revoke"`
	// RevokeRequests is absent from a snapshot an older binary cut, which
	// numbered no request: each key it asked for has one request, 0,
	// which no withdrawal names, as on a table that replayed the log. It
	// is written even empty, beside PendingRevoke.
	RevokeRequests map[string][]uint64 `json:"revoke_requests"`
	// Pending and Checkpoints are absent from snapshots older than the
	// checkpoints, which then restore with none.
	Pending     *pendingCheckpoint `json:"pending,omitempty"`
	Checkpoints []CheckpointRecord `json:"checkpoints,omitempty"`
	// Payload and Nonces are absent from older snapshots: the payload
	// version is then read back from the audit records kept.
	Payload uint8       `json:"payload,omitempty"`
	Nonces  []NonceMark `json:"nonces,omitempty"`
	// Deletions is absent from older snapshots: the projector then knows
	// the deletions it applies from then on only. DepartedAt is written
	// from GenerationVersion, in format 2.
	Deletions  map[string][]uint64 `json:"deletions,omitempty"`
	DepartedAt map[string]uint64   `json:"departed_at,omitempty"`
	// Renames is absent from older snapshots: the table then knows the
	// newest rename into each key only, as before (#138).
	Renames map[string][]renameMark `json:"renames,omitempty"`
	// IDs is absent from older snapshots: the keys' repository IDs are then
	// known again from their next ref transaction (#82).
	IDs map[string]int64 `json:"ids,omitempty"`
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
		Format: snapshotFormat(t.payload), Applied: t.applied, Identity: t.identity,
		Repos: make(map[string]map[string]*RefValue, len(t.repos)), Last: map[string]uint64{},
		Events: t.events, Audit: t.audit, Deleted: t.deleted, Renamed: t.renamed, Grants: t.grants, Revoked: t.revoked, PendingRevoke: t.pendingRevoke,
		RevokeRequests: t.revokeRequests,
		Pending:        t.pending, Checkpoints: t.checkpoints, Payload: t.payload, Nonces: t.nonces,
		Deletions: t.deletions, DepartedAt: t.departedAt, Renames: t.renames, IDs: t.ids,
	}
	for k, r := range t.repos {
		p.Repos[k] = r.refs
		p.Last[k] = r.last
	}
	return p
}

// validNames refuses a snapshot holding a repository key or a ref name
// Apply refuses: the projector, the observer and a recover turn these keys
// into paths, and a key such as "../raft-data" would reach outside the
// repository root (#81). The snapshot's digest is its sender's, so nothing
// else stands between a compromised leader and those paths.
func (p *persisted) validNames() error {
	refuse := func(what string, err error) error {
		return fmt.Errorf("%w: the ref table snapshot holds %s: %w", consensus.ErrStateMachineFailed, what, err)
	}
	for repo, refs := range p.Repos {
		if err := ValidRepoKey(repo); err != nil {
			return refuse(fmt.Sprintf("the repository key %q", repo), err)
		}
		for name, v := range refs {
			if err := ValidRefName(name); err != nil {
				return refuse(fmt.Sprintf("the ref %q of %s", name, repo), err)
			}
			if v != nil && v.Symbolic != "" {
				if err := ValidRefName(v.Symbolic); err != nil {
					return refuse(fmt.Sprintf("the target %q of %s %s", v.Symbolic, repo, name), err)
				}
			}
		}
	}
	for repo := range p.Last {
		if err := ValidRepoKey(repo); err != nil {
			return refuse(fmt.Sprintf("the repository key %q", repo), err)
		}
	}
	for repo := range p.Deleted {
		if err := ValidRepoKey(repo); err != nil {
			return refuse(fmt.Sprintf("the deleted repository key %q", repo), err)
		}
	}
	for repo := range p.Deletions {
		if err := ValidRepoKey(repo); err != nil {
			return refuse(fmt.Sprintf("the deleted repository key %q", repo), err)
		}
	}
	for repo := range p.DepartedAt {
		if err := ValidRepoKey(repo); err != nil {
			return refuse(fmt.Sprintf("the departed repository key %q", repo), err)
		}
	}
	for to, m := range p.Renamed {
		if err := ValidRepoKey(to); err != nil {
			return refuse(fmt.Sprintf("the renamed repository key %q", to), err)
		}
		if err := ValidRepoKey(m.From); err != nil {
			return refuse(fmt.Sprintf("the rename source %q", m.From), err)
		}
	}
	for repo := range p.IDs {
		if err := ValidRepoKey(repo); err != nil {
			return refuse(fmt.Sprintf("the repository key %q", repo), err)
		}
	}
	for to, marks := range p.Renames {
		if err := ValidRepoKey(to); err != nil {
			return refuse(fmt.Sprintf("the renamed repository key %q", to), err)
		}
		for _, m := range marks {
			if err := ValidRepoKey(m.From); err != nil {
				return refuse(fmt.Sprintf("the rename source %q", m.From), err)
			}
		}
	}
	return nil
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
	if p.Format < 1 || p.Format > snapshotFormat(t.limit) {
		return fmt.Errorf("%w: snapshot format %d", ErrVersion, p.Format)
	}
	// A snapshot cut at a payload version this binary does not apply holds
	// state it cannot keep: restoring it would drop that state and leave
	// the node writing a version it cannot encode (#50). It stops here.
	if p.Payload > t.limit {
		return fmt.Errorf("%w: snapshot written at payload version %d, this binary applies %d", ErrVersion, p.Payload, t.limit)
	}
	if err := p.validNames(); err != nil {
		return err
	}
	nt := New()
	nt.limit = t.limit
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
		nt.payload = max(nt.payload, adoptedVersion(&nt.audit[i].Audit, nt.limit))
	}
	if p.Format < snapshotFormat(nt.payload) {
		// A format 1 snapshot of a cluster that raised its version to
		// GenerationVersion was cut by a binary that could not read the
		// raise: what it dropped (#50) would be lost here without a word.
		return fmt.Errorf("%w: snapshot format %d cut after the cluster raised its payload version to %d: an older binary cut it", ErrVersion, p.Format, nt.payload)
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
	if p.Deletions != nil {
		nt.deletions = p.Deletions
	}
	if p.Renames != nil {
		nt.renames = p.Renames
	}
	if p.IDs != nil {
		nt.ids = p.IDs
	}
	if p.DepartedAt != nil {
		nt.departedAt = p.DepartedAt
	}
	if p.Grants != nil {
		nt.grants = p.Grants
	}
	if p.Revoked != nil {
		nt.revoked = p.Revoked
	}
	// The map is absent from a snapshot of a binary older than it, and
	// from one an earlier binary cut empty, which it left out. Below
	// WithdrawVersion a request left pendingRevoke only once its
	// revocation was written, in revoked: the audit records kept then say
	// which requests are pending (#50). From WithdrawVersion a withdrawal
	// closes a request as well, and leaves no record of it: every binary
	// that applies it writes the map whenever a request is open, so its
	// absence means none, and the withdrawn request, whose record the
	// audit keeps, is not read back (#223). The snapshot's own payload
	// version tells the two apart: a withdrawal is written once the
	// cluster raised its version to WithdrawVersion, which every table
	// that applies it adopted first, at an earlier index.
	switch {
	case p.PendingRevoke != nil:
		nt.pendingRevoke = p.PendingRevoke
	case p.Payload < WithdrawVersion:
		for i := range nt.audit {
			if k, ok := pendingRevocation(&nt.audit[i].Audit); ok {
				if _, done := nt.revoked[k.SPKI]; !done {
					nt.pendingRevoke[k.SPKI] = k
				}
			}
		}
	}
	for spki, reqs := range p.RevokeRequests {
		if _, asked := nt.pendingRevoke[spki]; asked && len(reqs) > 0 {
			nt.revokeRequests[spki] = reqs
		}
	}
	for spki := range nt.pendingRevoke {
		if len(nt.revokeRequests[spki]) == 0 {
			nt.revokeRequests[spki] = []uint64{0}
		}
	}
	t.mu.Lock()
	t.repos, t.root, t.events, t.eventBytes = nt.repos, nt.root, nt.events, nt.eventBytes
	t.audit, t.deleted, t.renamed, t.grants, t.revoked = nt.audit, nt.deleted, nt.renamed, nt.grants, nt.revoked
	t.pendingRevoke, t.revokeRequests = nt.pendingRevoke, nt.revokeRequests
	t.applied, t.identity = p.Applied, p.Identity
	t.pending, t.checkpoints = p.Pending, p.Checkpoints
	t.payload, t.nonces, t.byNonce = nt.payload, nt.nonces, nt.byNonce
	t.deletions, t.departedAt, t.renames, t.ids = nt.deletions, nt.departedAt, nt.renames, nt.ids
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
