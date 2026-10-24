// Copyright 2026 The Forgejo Authors. All rights reserved.
// SPDX-License-Identifier: GPL-3.0-or-later

// Package observer is a forgejo-raft observer node (observer plan v3, v4):
// a node that is never a member of the cluster, opens no port, and pulls
// from the leader, over SSH connections it opens itself, checkpoints that
// pair the refs, the git objects, the stored files and Forgejo's
// PostgreSQL at one causal cut. Each checkpoint is verified, published
// atomically, then protected by a snapshot of the whole observer directory
// the operator's hook takes; from a protected checkpoint, `forgejo cluster
// recover --from-observer` rebuilds a cluster.
//
// The package never imports the consensus node: it reads the cluster only
// through the export's read-only calls (Source).
package observer

import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"regexp"
	"slices"
	"sort"
	"strings"
	"time"

	"forgejo.org/modules/clusterhook"
	"forgejo.org/modules/json"
	"forgejo.org/modules/log"
	"forgejo.org/services/cluster/consensus"
	"forgejo.org/services/cluster/reftable"

	"golang.org/x/crypto/nacl/box"
)

// Source is one member's export, as the observer reaches it: one read-only
// call, its answer's head and, for a stream, its body. It is all the
// observer can ask of a member.
type Source interface {
	Export(ctx context.Context, req *clusterhook.ExportRequest) (*clusterhook.ExportAnswer, io.ReadCloser, error)
}

// Config is an observer's configuration.
type Config struct {
	Name string
	Dir  string
	// Endpoints are the members' exports by slot: the only places a
	// leader hint is followed to.
	Endpoints map[string]Source
	// SecretsKey is the X25519 key the secrets bundle is sealed to.
	SecretsKey *[32]byte
	// Hook snapshots the whole observer directory; nil runs HookCommand.
	Hook        func(ctx context.Context, checkpoint string) (string, error)
	HookCommand string
	// StagingQuota stops ingestion when staging holds that much.
	StagingQuota int64
	// MaxArtifact bounds a refs snapshot, a manifest or a WAL file; a base
	// backup is bounded by the staging quota.
	MaxArtifact int64
	// LiveKeep is how long a protected checkpoint stays in the live
	// directory; older ones live on in the hook's snapshots. Zero keeps
	// everything.
	LiveKeep time.Duration
	// BaseEvery is how old the newest base backup may grow before another,
	// taken after a pass's checkpoint even while the older still covers:
	// it bounds the WAL held and the WAL a restore replays.
	BaseEvery time.Duration
	// StepTimeout bounds one pass, a base backup included: a member that
	// stalls ends the pass, never the observer.
	StepTimeout time.Duration
	Git         string
	Now         func() time.Time
}

// Observer is one observer node.
type Observer struct {
	cfg   Config
	store *store
	state *State
	// slots is the round-robin order of the endpoints.
	slots []string
	next  int
	// uncovered counts the passes whose checkpoint the newest base did not
	// reach: past gapPasses, the WAL has a gap, not a lag.
	uncovered int
	// renewAfter is when a base renewal that failed is tried again: each
	// try may stream a whole base before it fails.
	renewAfter time.Time
	// stepStarted is when the running pass began, zero between passes.
	stepStarted time.Time
	// lock holds the directory for this process.
	lock *os.File
}

// The files of the observer directory that coordinate its processes.
const (
	lockFile   = "observer.lock"
	acceptFile = "ACCEPT"
)

// viewTimeout bounds one member's answer to the view request.
const viewTimeout = time.Minute

// gapPasses is how many passes a base backup may fail to cover before the
// observer takes a new one: any gap in the archive forces a new base.
const gapPasses = 20

// renewRetry is how long after a failed base renewal the next is tried.
const renewRetry = time.Hour

// The refusals that stop ingestion until a human acts.
var (
	ErrQuarantined = errors.New("QUARANTINED")
	ErrHookFailing = errors.New("SNAPSHOT_HOOK_FAILING")
	ErrQuota       = errors.New("STAGING_QUOTA_REACHED")
)

// ErrBusy is an observer directory another process holds.
var ErrBusy = errors.New("another process runs the observer on this directory")

// New opens an observer's directory.
func New(cfg Config) (*Observer, error) {
	if cfg.Git == "" {
		cfg.Git = "git"
	}
	if cfg.Now == nil {
		cfg.Now = time.Now
	}
	if cfg.MaxArtifact == 0 {
		cfg.MaxArtifact = 1 << 30
	}
	if cfg.BaseEvery == 0 {
		cfg.BaseEvery = 7 * 24 * time.Hour
	}
	if cfg.StepTimeout == 0 {
		cfg.StepTimeout = 6 * time.Hour
	}
	if len(cfg.Endpoints) == 0 {
		return nil, errors.New("no endpoint")
	}
	st := newStore(cfg.Dir, cfg.Git)
	// The lock comes before staging is cleaned: a second process never
	// removes what the running one is ingesting (#86).
	if err := os.MkdirAll(cfg.Dir, 0o700); err != nil {
		return nil, err
	}
	lock, err := lockDir(st.path(lockFile))
	if err != nil {
		if errors.Is(err, ErrBusy) {
			return nil, fmt.Errorf("%w: %s", ErrBusy, cfg.Dir)
		}
		return nil, err
	}
	if err := st.init(); err != nil {
		lock.Close()
		return nil, err
	}
	state, err := st.loadState()
	if err != nil {
		lock.Close()
		return nil, err
	}
	o := &Observer{cfg: cfg, store: st, state: state, lock: lock}
	for slot := range cfg.Endpoints {
		o.slots = append(o.slots, slot)
	}
	sort.Strings(o.slots)
	return o, nil
}

// Close releases the observer's directory.
func (o *Observer) Close() {
	if o.lock != nil {
		o.lock.Close()
		o.lock = nil
	}
}

// RequestAccept asks the observer that runs on dir to accept lineage at its
// next pass: `observe accept` while the service runs, which holds the
// directory and keeps its state in memory (#86).
func RequestAccept(dir, lineage string) error {
	return writeAtomic(filepath.Join(dir, acceptFile), []byte(lineage+"\n"), true)
}

// acceptRequested applies an accept another process asked for, once: the
// request goes whether it was granted or not, and a refusal ends the pass.
func (o *Observer) acceptRequested() error {
	path := o.store.path(acceptFile)
	b, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	if err := os.Remove(path); err != nil {
		return err
	}
	if err := syncDir(o.cfg.Dir); err != nil {
		return err
	}
	if err := o.Accept(strings.TrimSpace(string(b))); err != nil {
		return fmt.Errorf("the accept asked by `forgejo cluster observe accept`: %w", err)
	}
	return nil
}

// State returns a copy of the observer's state.
func (o *Observer) State() State {
	s := *o.state
	s.Checkpoints = append([]Accepted(nil), o.state.Checkpoints...)
	return s
}

// Run ingests until ctx ends, one step per interval; it logs and backs off,
// and never stops on an error but a quarantine, which waits for a human.
func (o *Observer) Run(ctx context.Context, interval time.Duration) {
	backoff := interval
	var err error
	for {
		// The heartbeat: STATUS.json says a pass started before it runs,
		// so a pass that hangs shows as an old step_started.
		o.stepStarted = o.cfg.Now()
		o.writeStatus(err)
		sctx, cancel := context.WithTimeout(ctx, o.cfg.StepTimeout)
		err = o.Step(sctx)
		cancel()
		o.stepStarted = time.Time{}
		o.writeStatus(err)
		switch {
		case err == nil:
			backoff = interval
		case errors.Is(err, ErrQuarantined), errors.Is(err, ErrHookFailing), errors.Is(err, ErrQuota):
			log.Error("cluster observer: %v", err)
		default:
			log.Warn("cluster observer: %v", err)
			backoff = min(backoff*2, 10*interval)
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(backoff):
		}
	}
}

// Step runs one pass: the pending hooks, the leader's view, the log, the
// newest new checkpoint, the prune, then a new base backup when it is due.
func (o *Observer) Step(ctx context.Context) error {
	if err := o.acceptRequested(); err != nil {
		return err
	}
	if q := o.state.Quarantine; q != nil {
		return fmt.Errorf("%w since %s: %s; see `forgejo cluster observe status`", ErrQuarantined, q.At.Format(time.RFC3339), q.Reason)
	}
	if err := o.protectPending(ctx); err != nil {
		return err
	}
	if n := o.store.stagingSize(); o.cfg.StagingQuota > 0 && n >= o.cfg.StagingQuota {
		return fmt.Errorf("%w: staging holds %d bytes", ErrQuota, n)
	}
	view, slot, err := o.leaderView(ctx)
	if err != nil {
		return err
	}
	// The accepted checkpoints are compared first: a compacted log jumps
	// to the newest of them, which must still be the leader's.
	if err := o.checkKnown(view); err != nil {
		return err
	}
	if view.LogSupported {
		if err := o.follow(ctx, slot, view); err != nil {
			return err
		}
	}
	cp, ok := o.nextCheckpoint(view)
	if !ok {
		return o.prune(ctx)
	}
	if err := o.ingest(ctx, slot, cp, view.Voters); err != nil {
		// A pass stopped or timed out is never taken for a fork.
		if errors.Is(err, ErrImmutable) && ctx.Err() == nil {
			return o.quarantineFor(fmt.Sprintf("checkpoint %s: %v", cp.Name, err), "")
		}
		return fmt.Errorf("checkpoint %s: %w", cp.Name, err)
	}
	if err := o.protectPending(ctx); err != nil {
		return err
	}
	if err := o.prune(ctx); err != nil {
		return err
	}
	o.renewBase(ctx, slot, cp.SystemID)
	return nil
}

// Accept is the human act that ends a quarantine. For a new lineage, the
// operator names it, having checked it against a member's status: the
// observer follows it from its next checkpoint, its log from the start.
// For a fork or a rollback, the operator names the current lineage: the
// accepted checkpoints stay as they are, and the log restarts from the
// newest one.
func (o *Observer) Accept(lineage string) error {
	q := o.state.Quarantine
	if q == nil {
		return errors.New("the observer is not quarantined")
	}
	switch {
	case q.Lineage != "" && lineage == q.Lineage:
		o.state.Lineage, o.state.Term, o.state.Commit = lineage, 0, 0
		o.state.LogIndex, o.state.LogTerm = 0, 0
	case q.Lineage == "" && lineage == o.state.Lineage:
		o.state.Term, o.state.Commit = 0, 0
		o.state.LogIndex, o.state.LogTerm = 0, 0
		if a, ok := o.state.newestOf(lineage); ok {
			o.state.LogIndex, o.state.LogTerm = a.Index, a.Term
		}
	default:
		want := q.Lineage
		if want == "" {
			want = o.state.Lineage
		}
		return fmt.Errorf("the quarantine is about lineage %q; name it to accept it", want)
	}
	log.Warn("cluster observer: quarantine lifted by the operator (%s); following lineage %s", q.Reason, lineage)
	o.state.Quarantine = nil
	return o.store.saveState(o.state)
}

func (o *Observer) quarantineFor(reason, lineage string) error {
	o.state.Quarantine = &Quarantine{Reason: reason, Lineage: lineage, At: o.cfg.Now()}
	if err := o.store.saveState(o.state); err != nil {
		return err
	}
	return fmt.Errorf("%w: %s", ErrQuarantined, reason)
}

// call asks one endpoint, with the answer decoded into out when it is not
// a stream.
func call(ctx context.Context, src Source, req *clusterhook.ExportRequest, out any) (io.ReadCloser, error) {
	ans, body, err := src.Export(ctx, req)
	if err != nil {
		return nil, err
	}
	if !ans.OK {
		if body != nil {
			body.Close()
		}
		return nil, &clusterhook.ExportError{Code: ans.Code, Message: ans.Message, Leader: ans.Leader}
	}
	if out != nil && len(ans.Data) > 0 {
		if err := json.Unmarshal(ans.Data, out); err != nil {
			if body != nil {
				body.Close()
			}
			return nil, err
		}
	}
	if body == nil {
		body = io.NopCloser(bytes.NewReader(nil))
	}
	return body, nil
}

// leaderView asks every pinned endpoint, the last leader first, and keeps
// the view of the highest term. A member that does not lead names its
// term: a view older than it comes from a deposed leader that still
// answers, and is passed over. Nothing but a pinned endpoint is asked,
// whatever a hint names.
func (o *Observer) leaderView(ctx context.Context) (*clusterhook.ExportView, string, error) {
	var errs []error
	var best *clusterhook.ExportView
	bestSlot := ""
	var seen uint64
	for _, slot := range o.order() {
		var v clusterhook.ExportView
		vctx, cancel := context.WithTimeout(ctx, viewTimeout)
		body, err := call(vctx, o.cfg.Endpoints[slot], &clusterhook.ExportRequest{Op: clusterhook.OpExportCheckpoints, Observer: o.cfg.Name, FromIndex: o.viewFrom()}, &v)
		if err == nil {
			body.Close()
		}
		cancel()
		if err != nil {
			if e, ok := errors.AsType[*clusterhook.ExportError](err); ok && e.Code == clusterhook.CodeNotLeader {
				seen = max(seen, e.Term)
			}
			errs = append(errs, fmt.Errorf("%s: %w", slot, err))
			continue
		}
		if v.Leader != slot {
			errs = append(errs, fmt.Errorf("%s answered as %s", slot, v.Leader))
			continue
		}
		if err := validView(&v); err != nil {
			errs = append(errs, fmt.Errorf("%s: %w", slot, err))
			continue
		}
		if best == nil || v.Term > best.Term {
			best, bestSlot = &v, slot
		}
	}
	if best == nil {
		return nil, "", fmt.Errorf("no leader answered: %w", errors.Join(errs...))
	}
	if best.Term < seen {
		return nil, "", fmt.Errorf("%w: %s answers for term %d, and a member is in term %d", errStale, bestSlot, best.Term, seen)
	}
	if err := o.acceptView(bestSlot, best); err != nil {
		return nil, "", err
	}
	return best, bestSlot, nil
}

func (o *Observer) order() []string {
	var out []string
	if _, ok := o.cfg.Endpoints[o.state.Leader]; ok {
		out = append(out, o.state.Leader)
	}
	for i := range o.slots {
		if s := o.slots[(o.next+i)%len(o.slots)]; s != o.state.Leader {
			out = append(out, s)
		}
	}
	o.next = (o.next + 1) % len(o.slots)
	return out
}

// lastIndex is the index of the newest checkpoint accepted in the lineage
// followed, 0 in a lineage newly accepted: the cursor of nextCheckpoint.
func (o *Observer) lastIndex() uint64 {
	if a, ok := o.state.newestOf(o.state.Lineage); ok {
		return a.Index
	}
	return 0
}

// viewFrom is the index the view is asked from: just before the oldest
// checkpoint accepted in the lineage followed, so that the view describes
// again every accepted checkpoint the leader's table still lists, which
// checkKnown compares.
func (o *Observer) viewFrom() uint64 {
	if a, ok := o.state.oldestOf(o.state.Lineage); ok {
		return a.Index - 1
	}
	return 0
}

// validCheckpoint checks a checkpoint's name, which names directories of
// the store, against the only form a member derives from its lineage and
// index.
func validCheckpoint(cp clusterhook.ExportCheckpoint) error {
	if !checkpointName.MatchString(cp.Name) || cp.Name != reftable.CheckpointName(cp.Lineage, cp.Index) {
		return fmt.Errorf("invalid checkpoint name %q", cp.Name)
	}
	return nil
}

// validView refuses a view that names a checkpoint otherwise than a member
// would: it comes from no honest member, and nothing of it is used.
func validView(v *clusterhook.ExportView) error {
	for _, cp := range v.Checkpoints {
		if err := validCheckpoint(cp); err != nil {
			return err
		}
	}
	return nil
}

var errStale = errors.New("a deposed leader's view, behind the term already seen")

// acceptView checks a leader's view against what was accepted: the same
// lineage, a term and a commit that never go back.
func (o *Observer) acceptView(slot string, v *clusterhook.ExportView) error {
	st := o.state
	for _, m := range v.Members {
		if m == o.cfg.Name {
			return o.quarantineFor(fmt.Sprintf("the membership names this observer (%s): an observer is never a member", m), "")
		}
	}
	switch {
	case st.Lineage == "":
		// The first contact sets the trust root: the operator reads it in
		// `forgejo cluster observe status` against a member's status.
		log.Warn("cluster observer: following lineage %s from %s for the first time", v.Lineage, slot)
		st.Lineage = v.Lineage
	case v.Lineage != st.Lineage:
		b, _ := json.Marshal(v)
		o.store.quarantine("lineage", b)
		return o.quarantineFor(fmt.Sprintf("the leader serves lineage %s, not %s: a Recover or a restore started a new history, which a human accepts", v.Lineage, st.Lineage), v.Lineage)
	}
	if v.Term < st.Term {
		return fmt.Errorf("%w: %s answers for term %d, behind %d", errStale, slot, v.Term, st.Term)
	}
	if v.Commit < st.Commit || v.Commit < st.LogIndex {
		b, _ := json.Marshal(v)
		o.store.quarantine("rollback", b)
		return o.quarantineFor(fmt.Sprintf("rollback: the leader of term %d confirms commit %d, behind %d already accepted", v.Term, v.Commit, max(st.Commit, st.LogIndex)), "")
	}
	st.Term, st.Commit, st.Leader = v.Term, v.Commit, slot
	return o.store.saveState(st)
}

// follow reads the committed log from the cursor to the leader's commit,
// checking that it continues what was accepted: consecutive indexes,
// terms that never decrease nor pass the leader's. A divergence at the
// cursor is a fork; a compacted log restarts from the newest checkpoint.
func (o *Observer) follow(ctx context.Context, slot string, v *clusterhook.ExportView) error {
	st := o.state
	for st.LogIndex < v.Commit {
		req := &clusterhook.ExportRequest{Op: clusterhook.OpExportLog, Observer: o.cfg.Name, Lineage: st.Lineage, FromIndex: st.LogIndex, FromTerm: st.LogTerm}
		body, err := call(ctx, o.cfg.Endpoints[slot], req, nil)
		switch clusterhook.ExportCode(err) {
		case "":
		case clusterhook.CodeDiverged:
			return o.quarantineFor(fmt.Sprintf("fork: the leader's log does not hold term %d at %d, which this observer accepted", st.LogTerm, st.LogIndex), "")
		case clusterhook.CodeCompacted:
			a, ok := o.state.newestOf(st.Lineage)
			if !ok || a.Index <= st.LogIndex {
				st.Rebootstrap = "the log was compacted past this observer; waiting for a checkpoint to restart from"
				return o.store.saveState(st)
			}
			st.LogIndex, st.LogTerm, st.Rebootstrap = a.Index, a.Term, ""
			if err := o.store.saveState(st); err != nil {
				return err
			}
			continue
		default:
			return err
		}
		entries, err := readEntries(body, o.cfg.MaxArtifact)
		body.Close()
		if err != nil {
			return err
		}
		if len(entries) == 0 {
			return nil
		}
		prev, prevTerm := st.LogIndex, st.LogTerm
		for _, e := range entries {
			if e.Index != prev+1 || e.Term < prevTerm || e.Term > v.Term || e.Index > v.Commit {
				b, _ := json.Marshal(entries)
				o.store.quarantine("log", b)
				return o.quarantineFor(fmt.Sprintf("the log is not a continuation: entry %d of term %d after %d of term %d", e.Index, e.Term, prev, prevTerm), "")
			}
			prev, prevTerm = e.Index, e.Term
		}
		if err := o.appendLog(st.Lineage, entries); err != nil {
			return err
		}
		st.LogIndex, st.LogTerm = prev, prevTerm
		if err := o.store.saveState(st); err != nil {
			return err
		}
	}
	return nil
}

func readEntries(r io.Reader, limit int64) ([]clusterhook.ExportEntry, error) {
	var out []clusterhook.ExportEntry
	sc := bufio.NewScanner(io.LimitReader(r, limit))
	sc.Buffer(make([]byte, 64<<10), 64<<20)
	for sc.Scan() {
		var e clusterhook.ExportEntry
		if err := json.Unmarshal(sc.Bytes(), &e); err != nil {
			return nil, err
		}
		out = append(out, e)
	}
	return out, sc.Err()
}

// appendLog keeps the committed entries, synced before the cursor moves.
func (o *Observer) appendLog(lineage string, entries []clusterhook.ExportEntry) error {
	f, err := os.OpenFile(o.store.path("log", strings.NewReplacer("/", "-").Replace(lineage)+".jsonl"), os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o600)
	if err != nil {
		return err
	}
	enc := json.NewEncoder(f)
	for _, e := range entries {
		if err = enc.Encode(e); err != nil {
			break
		}
	}
	if err == nil {
		err = f.Sync()
	}
	if cerr := f.Close(); err == nil {
		err = cerr
	}
	return err
}

func checkpointDigest(cp clusterhook.ExportCheckpoint) string {
	b, _ := json.Marshal(cp)
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:])
}

// checkKnown compares the leader's description of checkpoints already
// accepted with what was accepted: an accepted checkpoint is immutable, and
// another description of it is a fork.
func (o *Observer) checkKnown(v *clusterhook.ExportView) error {
	listed := map[string]bool{}
	var oldest uint64
	for _, cp := range v.Checkpoints {
		listed[cp.Name] = true
		if cp.Lineage == o.state.Lineage && (oldest == 0 || cp.Index < oldest) {
			oldest = cp.Index
		}
		if i, ok := o.state.accepted(cp.Name); ok && o.state.Checkpoints[i].Digest != checkpointDigest(cp) {
			b, _ := json.Marshal(cp)
			o.store.quarantine("checkpoint", b)
			return o.quarantineFor(fmt.Sprintf("fork: checkpoint %s is described otherwise than when it was accepted", cp.Name), "")
		}
	}
	// The table keeps its newest checkpoints: one it lists older than an
	// accepted checkpoint means that accepted one should be listed too.
	for _, a := range o.state.Checkpoints {
		if inLineage(a, o.state.Lineage) && oldest != 0 && oldest < a.Index && !listed[a.Name] {
			b, _ := json.Marshal(v.Checkpoints)
			o.store.quarantine("checkpoint", b)
			return o.quarantineFor(fmt.Sprintf("fork: checkpoint %s, accepted, is gone from the leader's table", a.Name), "")
		}
	}
	return nil
}

// nextCheckpoint picks the newest checkpoint past the newest accepted:
// older ones are not back-filled, each one stands alone.
func (o *Observer) nextCheckpoint(v *clusterhook.ExportView) (clusterhook.ExportCheckpoint, bool) {
	last := o.lastIndex()
	var best clusterhook.ExportCheckpoint
	found := false
	for _, cp := range v.Checkpoints {
		if cp.Lineage == o.state.Lineage && cp.Index > last && (!found || cp.Index > best.Index) {
			best, found = cp, true
		}
	}
	return best, found
}

// ---- ingestion ----

// ingest verifies one checkpoint into staging, then publishes it. What a
// checkpoint that fails leaves in staging goes with it.
func (o *Observer) ingest(ctx context.Context, leader string, cp clusterhook.ExportCheckpoint, voters []string) (err error) {
	// The name is checked before anything is cleaned up under it: a name
	// such as ".." would otherwise remove the store, or what lies past it.
	if err := validCheckpoint(cp); err != nil {
		return err
	}
	// A checkpoint published by a pass stopped before it saved the state
	// is this one again, whole: it is taken into the state, never refused
	// as a fork (#61). Another description of it still is.
	if pm, err := o.store.ReadManifest(cp.Name); err == nil {
		if checkpointDigest(pm.Checkpoint) != checkpointDigest(cp) {
			return fmt.Errorf("%w: %s is already published with another description", ErrImmutable, cp.Name)
		}
		return o.adopt(cp, pm)
	} else if !errors.Is(err, os.ErrNotExist) {
		return err
	}
	stage := o.store.path("staging", cp.Name)
	defer func() {
		if err != nil {
			_ = os.RemoveAll(stage)
		}
	}()
	target, err := ParseLSN(cp.LSN)
	if err != nil {
		return err
	}
	if cp.WALSegmentSize <= 0 || cp.WALSegmentSize&(cp.WALSegmentSize-1) != 0 {
		return errors.New("invalid WAL segment size")
	}
	if err := os.RemoveAll(stage); err != nil {
		return err
	}
	if err := os.MkdirAll(stage, 0o700); err != nil {
		return err
	}
	src := o.cfg.Endpoints[leader]
	m := &Manifest{Format: 1, Checkpoint: cp, Repos: map[string]RepoManifest{}, Source: leader, Voters: voters}
	if prev, ok := o.state.newest(); ok {
		m.Previous = prev.Name
	}

	// The refs at the barrier: the leader's artifact, by the digest the
	// replicated position carries, and the root the barrier recorded.
	table, err := o.fetchRefs(ctx, src, cp, stage)
	if err != nil {
		return err
	}
	// The git objects of every live ref, closed and checked.
	if err := o.fetchObjects(ctx, leader, cp, table, m); err != nil {
		return err
	}
	// The stored files present at the fence.
	if err := o.fetchFiles(ctx, leader, cp, stage, m); err != nil {
		return err
	}
	// The secrets, sealed to this observer; checked, kept sealed.
	if err := o.fetchSecrets(ctx, src, stage); err != nil {
		return err
	}
	// PostgreSQL: a base before the position and the WAL up to it.
	cov, err := o.coverPG(ctx, leader, cp, target)
	if err != nil {
		return err
	}
	m.PG = cov
	m.Verified = o.cfg.Now().UTC()
	m.Loss = fmt.Sprintf("Recovering %s restores the refs at index %d and PostgreSQL before %s (%s). Every push, SQL write and stored file after them is lost, and so is the replicated history before them: the cluster starts a new lineage.",
		cp.Name, cp.Index, cp.LSN, time.Unix(0, cp.Time).UTC().Format(time.RFC3339))
	b, err := json.MarshalIndent(m, "", "  ")
	if err != nil {
		return err
	}
	if err := writeAtomic(o.store.path("staging", cp.Name, "manifest.json"), b, false); err != nil {
		return err
	}

	// The refs of every repository, create-only, once everything is here.
	for repo, rm := range m.Repos {
		if len(rm.Refs) == 0 {
			continue
		}
		if err := o.store.git.createRefs(ctx, o.store.git.dirFor(repo, rm.Format), cp.Name, rm.Refs); err != nil {
			return err
		}
	}
	if err := o.store.publish(cp.Name); err != nil {
		return err
	}
	return o.record(cp, m)
}

// adopt takes into the state a checkpoint found published: its directory
// is synced again, since the pass that renamed it may have stopped before.
func (o *Observer) adopt(cp clusterhook.ExportCheckpoint, m *Manifest) error {
	if err := syncDir(o.store.path("checkpoints")); err != nil {
		return err
	}
	log.Warn("cluster observer: checkpoint %s was published by a pass that did not record it; taken as is", cp.Name)
	return o.record(cp, m)
}

// record records a published checkpoint as verified.
func (o *Observer) record(cp clusterhook.ExportCheckpoint, m *Manifest) error {
	o.state.Checkpoints = append(o.state.Checkpoints, Accepted{
		Name: cp.Name, Index: cp.Index, Term: cp.Term, Root: cp.Root, Time: cp.Time, State: StateVerified,
		Verified: m.Verified, Digest: checkpointDigest(cp),
	})
	if o.state.LogIndex < cp.Index && o.state.Rebootstrap != "" {
		o.state.Rebootstrap = ""
	}
	log.Info("cluster observer: checkpoint %s verified (%d repositories, %d files, PostgreSQL to %s)", cp.Name, m.Sizes.Repos, m.Sizes.Files, cp.LSN)
	return o.store.saveState(o.state)
}

func (o *Observer) fetchRefs(ctx context.Context, src Source, cp clusterhook.ExportCheckpoint, stage string) (*reftable.Table, error) {
	body, err := call(ctx, src, &clusterhook.ExportRequest{Op: clusterhook.OpExportRefs, Observer: o.cfg.Name, Checkpoint: cp.Name}, nil)
	if err != nil {
		return nil, err
	}
	defer body.Close()
	path := stage + "/refs.snapshot"
	if _, sum, err := writeStream(path, body, o.cfg.MaxArtifact, false); err != nil {
		return nil, err
	} else if sum != cp.RefsSHA256 {
		return nil, fmt.Errorf("the refs snapshot's digest %s is not the position's %s", sum, cp.RefsSHA256)
	}
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	t := reftable.New()
	if err := t.Restore(f, consensus.SnapshotMeta{}); err != nil {
		return nil, err
	}
	if pos, _ := t.Applied(); pos.Index != cp.Index || t.Root().String() != cp.Root {
		return nil, fmt.Errorf("the refs snapshot is at %d with root %s, not the barrier's %d with %s", pos.Index, t.Root(), cp.Index, cp.Root)
	}
	return t, nil
}

func (o *Observer) fetchObjects(ctx context.Context, leader string, cp clusterhook.ExportCheckpoint, t *reftable.Table, m *Manifest) error {
	var prev *Manifest
	if m.Previous != "" {
		prev, _ = o.store.ReadManifest(m.Previous)
	}
	for _, repo := range t.Repos() {
		if _, deleted := t.Deleted(repo); deleted {
			m.Deleted = append(m.Deleted, repo)
			continue
		}
		refs, _ := t.Refs(repo)
		rm := RepoManifest{Refs: map[string]string{}, Symbolic: map[string]string{}}
		wantSet := map[string]bool{}
		for name, v := range refs {
			if v.Format != "" {
				rm.Format = v.Format
			}
			switch {
			case v.Tombstone:
				rm.Tombstones = append(rm.Tombstones, name)
			case v.Symbolic != "":
				rm.Symbolic[name] = v.Symbolic
			default:
				rm.Refs[name] = v.SHA
				wantSet[v.SHA] = true
			}
		}
		sort.Strings(rm.Tombstones)
		m.Repos[repo] = rm
		m.Sizes.Repos++
		m.Sizes.Refs += len(rm.Refs)
		if len(wantSet) == 0 {
			continue
		}
		dir, err := o.store.git.ensure(ctx, repo, rm.Format)
		if err != nil {
			return err
		}
		wants := make([]string, 0, len(wantSet))
		for s := range wantSet {
			wants = append(wants, s)
		}
		sort.Strings(wants)
		// The previous checkpoint's values are haves only in the same
		// object format: a repository recreated in the other one shares
		// nothing with them (#84).
		var prevRefs []string
		if prm, ok := prev.repo(repo); ok && storeFormat(prm.Format) == storeFormat(rm.Format) {
			for _, s := range prm.Refs {
				prevRefs = append(prevRefs, s)
			}
		}
		haves := o.store.git.present(ctx, dir, prevRefs)
		missing := wants
		if have := o.store.git.present(ctx, dir, wants); len(have) > 0 {
			held := map[string]bool{}
			for _, s := range have {
				held[s] = true
			}
			missing = nil
			for _, s := range wants {
				if !held[s] {
					missing = append(missing, s)
				}
			}
		}
		if len(missing) > 0 {
			if err := o.fetchPack(ctx, leader, cp.Name, repo, dir, missing, haves); err != nil {
				return fmt.Errorf("%s: %w", repo, err)
			}
		}
		if err := o.store.git.closed(ctx, dir, wants, haves); err != nil {
			return fmt.Errorf("%s: %w", repo, err)
		}
	}
	return nil
}

// fetchPack takes a pack from the leader first, then from any member.
func (o *Observer) fetchPack(ctx context.Context, leader, checkpoint, repo, dir string, wants, haves []string) error {
	var errs []error
	for _, slot := range append([]string{leader}, o.slots...) {
		body, err := call(ctx, o.cfg.Endpoints[slot], &clusterhook.ExportRequest{Op: clusterhook.OpExportPacks, Observer: o.cfg.Name, Checkpoint: checkpoint, Repo: repo, Wants: wants, Haves: haves}, nil)
		if err != nil {
			errs = append(errs, err)
			continue
		}
		err = o.store.git.ingestPack(ctx, dir, body)
		body.Close()
		if err == nil {
			return nil
		}
		errs = append(errs, err)
	}
	return errors.Join(errs...)
}

func (o *Observer) fetchFiles(ctx context.Context, leader string, cp clusterhook.ExportCheckpoint, stage string, m *Manifest) error {
	body, err := call(ctx, o.cfg.Endpoints[leader], &clusterhook.ExportRequest{Op: clusterhook.OpExportFiles, Observer: o.cfg.Name, Checkpoint: cp.Name}, nil)
	if err != nil {
		return err
	}
	path := stage + "/files.json"
	_, sum, err := writeStream(path, body, o.cfg.MaxArtifact, false)
	body.Close()
	if err != nil {
		return err
	}
	if sum != cp.FilesSHA256 {
		return fmt.Errorf("the files manifest's digest %s is not the position's %s", sum, cp.FilesSHA256)
	}
	b, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	var fm clusterhook.FilesManifest
	if err := json.Unmarshal(b, &fm); err != nil {
		return err
	}
	if fm.Checkpoint != cp.Name {
		return fmt.Errorf("the files manifest is %s's", fm.Checkpoint)
	}
	// A file whose digest is known, listed or named by its path, is reused
	// by that digest alone. Only a leader that predates listed digests
	// leaves an attachment reused by path and size (#106).
	known := map[string]string{}
	if m.Previous != "" {
		if prev, err := o.store.ReadManifest(m.Previous); err == nil {
			for _, f := range prev.Files {
				known[fmt.Sprintf("%s/%s/%d", f.Storage, f.Path, f.Size)] = f.SHA256
			}
		}
	}
	for _, f := range fm.Files {
		want, err := expectedDigest(f)
		if err != nil {
			return fmt.Errorf("%s/%s: %w", f.Storage, f.Path, err)
		}
		sum, ok := want, want != ""
		if !ok {
			sum, ok = known[fmt.Sprintf("%s/%s/%d", f.Storage, f.Path, f.Size)]
		}
		if ok && o.store.haveBlob(sum) {
			m.Files = append(m.Files, FileBlob{FileEntry: f, SHA256: sum})
			continue
		}
		sum, err = o.fetchBlob(ctx, leader, f, want)
		if err != nil {
			return fmt.Errorf("%s/%s: %w", f.Storage, f.Path, err)
		}
		m.Files = append(m.Files, FileBlob{FileEntry: f, SHA256: sum})
		m.Sizes.Bytes += f.Size
	}
	m.Sizes.Files = len(m.Files)
	return nil
}

var digestPattern = regexp.MustCompile(`^[0-9a-f]{64}$`)

// expectedDigest is the digest a stored file must have, "" when nothing
// names it: an LFS object's path is its digest split in three, a package
// blob's last component is its digest, an attachment's is listed by the
// leader (#106). A file of the packages storage not named so is taken as
// it comes.
func expectedDigest(f clusterhook.FileEntry) (string, error) {
	var want string
	switch {
	case f.SHA256 != "":
		want = f.SHA256
	case f.Storage == "lfs":
		want = strings.ReplaceAll(f.Path, "/", "")
	case f.Storage == "packages" && digestPattern.MatchString(path.Base(f.Path)):
		want = path.Base(f.Path)
	default:
		return "", nil
	}
	if !digestPattern.MatchString(want) {
		return "", fmt.Errorf("%q is not a SHA-256 digest", want)
	}
	return want, nil
}

// fetchBlob stores one file by its digest, which must be want when known.
func (o *Observer) fetchBlob(ctx context.Context, leader string, f clusterhook.FileEntry, want string) (string, error) {
	var errs []error
	for _, slot := range append([]string{leader}, o.slots...) {
		body, err := call(ctx, o.cfg.Endpoints[slot], &clusterhook.ExportRequest{Op: clusterhook.OpExportFile, Observer: o.cfg.Name, Storage: f.Storage, Name: f.Path}, nil)
		if err != nil {
			errs = append(errs, err)
			continue
		}
		pathSum := sha256.Sum256([]byte(f.Storage + "/" + f.Path))
		tmp := o.store.path("staging", "blob-"+hex.EncodeToString(pathSum[:8]))
		_ = os.Remove(tmp)
		n, sum, err := writeStream(tmp, body, f.Size, false)
		body.Close()
		if err == nil && n != f.Size {
			err = fmt.Errorf("%d bytes, not %d", n, f.Size)
		}
		if err == nil && want != "" && want != sum {
			err = fmt.Errorf("the file's digest is %s, not %s", sum, want)
		}
		if err == nil {
			err = o.keepBlob(tmp, sum)
		}
		os.Remove(tmp)
		if err == nil {
			return sum, nil
		}
		errs = append(errs, err)
	}
	return "", errors.Join(errs...)
}

func (o *Observer) keepBlob(tmp, sum string) error {
	if o.store.haveBlob(sum) {
		return nil
	}
	dst := o.store.blobPath(sum)
	if err := os.MkdirAll(dst[:len(dst)-len(sum)-1], 0o700); err != nil {
		return err
	}
	if err := os.Link(tmp, dst); err != nil && !os.IsExist(err) {
		return err
	}
	return syncDir(dst[:len(dst)-len(sum)-1])
}

func (o *Observer) fetchSecrets(ctx context.Context, src Source, stage string) error {
	body, err := call(ctx, src, &clusterhook.ExportRequest{Op: clusterhook.OpExportSecrets, Observer: o.cfg.Name}, nil)
	if err != nil {
		return err
	}
	defer body.Close()
	sealed, err := io.ReadAll(io.LimitReader(body, 1<<20))
	if err != nil {
		return err
	}
	if o.cfg.SecretsKey != nil {
		pub := new([32]byte)
		curve25519Base(pub, o.cfg.SecretsKey)
		if _, ok := box.OpenAnonymous(nil, sealed, pub, o.cfg.SecretsKey); !ok {
			return errors.New("the secrets bundle does not open with this observer's key")
		}
	}
	return writeAtomic(stage+"/secrets.box", sealed, false)
}

// ---- PostgreSQL ----

// coverPG brings the WAL archive up to date and finds the base and WAL
// that restore the checkpoint. Without one, it fetches a base backup when
// none could ever reach it, and the checkpoint waits for a later one.
func (o *Observer) coverPG(ctx context.Context, leader string, cp clusterhook.ExportCheckpoint, target LSN) (Coverage, error) {
	if err := o.fetchWAL(ctx, leader, cp); err != nil {
		return Coverage{}, err
	}
	sysid := cp.SystemID
	have := func(name string) bool { return o.store.haveWAL(sysid, name) }
	history := func(tli uint32) ([]Switch, error) { return o.store.history(sysid, tli) }
	cov, err := Cover(o.state.Bases, sysid, target, cp.Timeline, cp.WALSegmentSize, have, history, o.store.abandoned(sysid))
	if err == nil {
		o.uncovered = 0
		return cov, nil
	}
	if !errors.Is(err, ErrNoCoverage) {
		return Coverage{}, err
	}
	o.uncovered++
	if o.needsBase(cp.SystemID) || o.uncovered >= gapPasses {
		o.uncovered = 0
		o.state.Rebootstrap = "taking a base backup: " + err.Error()
		if berr := o.fetchBase(ctx, leader); berr != nil {
			return Coverage{}, fmt.Errorf("%w; the base backup failed: %w", err, berr)
		}
		o.state.Rebootstrap = ""
		if serr := o.store.saveState(o.state); serr != nil {
			return Coverage{}, serr
		}
	}
	return Coverage{}, err
}

// renewBase takes a new base backup once the newest of the system is older
// than BaseEvery, though the bases held still cover the checkpoints: coverPG
// takes one only when they cover nothing, and with an archive that has no
// gap the first base would restore every checkpoint, with all the WAL since
// it, kept forever and replayed by each restore (#179). The next checkpoints
// restore from the new base, and the prune lets the older one and its WAL
// go with the last live checkpoint that restores from them.
//
// It runs once the pass's checkpoint is published, protected and the prune
// done: that checkpoint never waits for the base nor fails with it, and no
// checkpoint is in staging, so every base a checkpoint restores from is a
// published one's, which the dropIdleBases of the new base keeps. A
// failure is logged, the bases held still cover, and a pass renewRetry
// later tries again.
func (o *Observer) renewBase(ctx context.Context, leader, sysid string) {
	if !o.needsBase(sysid) || o.cfg.Now().Before(o.renewAfter) {
		return
	}
	if err := o.fetchBase(ctx, leader); err != nil {
		o.renewAfter = o.cfg.Now().Add(renewRetry)
		log.Warn("cluster observer: renewing the base backup of system %s: %v; the bases held still cover, tried again in %s", sysid, err, renewRetry)
		return
	}
	o.renewAfter = time.Time{}
}

// needsBase reports whether no base held could start a coverage: none of
// this system, or the newest older than BaseEvery.
func (o *Observer) needsBase(sysid string) bool {
	var newest time.Time
	for _, b := range o.state.Bases {
		if b.SystemID != sysid {
			continue
		}
		if fi, err := os.Stat(o.store.path("pg", "base", b.Label)); err == nil && fi.ModTime().After(newest) {
			newest = fi.ModTime()
		}
	}
	return newest.IsZero() || o.cfg.Now().Sub(newest) > o.cfg.BaseEvery
}

// walSource is a member that lists a WAL file, and what it lists.
type walSource struct {
	slot string
	file clusterhook.WALFile
}

// fetchWAL brings the archive up to date from every member: only the
// PostgreSQL primary's archive advances, and its host need not be the Raft
// leader. The lists are merged, and each file is taken from a member that
// lists it, the leader first (#57).
func (o *Observer) fetchWAL(ctx context.Context, leader string, cp clusterhook.ExportCheckpoint) error {
	if !sysidPattern.MatchString(cp.SystemID) {
		return fmt.Errorf("invalid system identifier %q", cp.SystemID)
	}
	listed := map[string][]walSource{}
	var errs []error
	answered := false
	for _, slot := range o.leaderFirst(leader) {
		var files []clusterhook.WALFile
		body, err := call(ctx, o.cfg.Endpoints[slot], &clusterhook.ExportRequest{Op: clusterhook.OpExportWALList, Observer: o.cfg.Name, Name: o.store.lastSegment(cp.SystemID)}, &files)
		if err != nil {
			errs = append(errs, fmt.Errorf("%s: %w", slot, err))
			continue
		}
		body.Close()
		// The name comes from a member: it becomes a path under the
		// archive only once it is a WAL file's name. A member that lists
		// another name is passed over whole, the others still serve.
		if i := slices.IndexFunc(files, func(f clusterhook.WALFile) bool { return !ValidWALName(f.Name) }); i >= 0 {
			errs = append(errs, fmt.Errorf("%s listed an invalid WAL name %q", slot, files[i].Name))
			continue
		}
		answered = true
		for _, f := range files {
			listed[f.Name] = append(listed[f.Name], walSource{slot: slot, file: f})
		}
	}
	if !answered {
		return errors.Join(errs...)
	}
	names := make([]string, 0, len(listed))
	for name := range listed {
		names = append(names, name)
	}
	sort.Strings(names)
	if err := os.MkdirAll(o.store.walDir(cp.SystemID), 0o700); err != nil {
		return err
	}
	// Nothing past the checkpoint's position is taken: a name a member
	// plants ahead of it never stands for a segment not yet written, and
	// the cursor never passes the confirmed position (#105).
	target, err := ParseLSN(cp.LSN)
	if err != nil {
		return err
	}
	final := finalSegment(target, cp.WALSegmentSize)
	for _, name := range names {
		if tli, segno, ok := parseSegment(name, cp.WALSegmentSize); ok && (tli > cp.Timeline || segno > final) {
			continue
		}
		if err := o.fetchWALFile(ctx, cp, name, listed[name]); err != nil {
			return err
		}
	}
	return nil
}

// leaderFirst is every endpoint, the leader first.
func (o *Observer) leaderFirst(leader string) []string {
	out := []string{leader}
	for _, slot := range o.slots {
		if slot != leader {
			out = append(out, slot)
		}
	}
	return out
}

// fetchWALFile takes one listed WAL file from the first member that serves
// bytes matching what it listed.
func (o *Observer) fetchWALFile(ctx context.Context, cp clusterhook.ExportCheckpoint, name string, srcs []walSource) error {
	if o.store.haveWAL(cp.SystemID, name) {
		// A timeline history held with other bytes is a timeline number
		// another history reused: segments under that number are not the
		// ones this archive holds, and a restore would replay the wrong
		// history.
		if !strings.HasSuffix(name, ".history") {
			return nil
		}
		for _, src := range srcs {
			if src.file.SHA256 == "" {
				continue
			}
			held, err := fileSHA256(o.store.walPath(cp.SystemID, name))
			if err != nil {
				return fmt.Errorf("WAL %s: %w", name, err)
			}
			if held != src.file.SHA256 {
				return o.quarantineFor(fmt.Sprintf("timeline history %s is held with other bytes than the cluster's: a timeline was reused", name), o.state.Lineage)
			}
		}
		return nil
	}
	var errs []error
	for _, src := range srcs {
		f := src.file
		if f.Size > o.cfg.MaxArtifact {
			continue
		}
		body, err := call(ctx, o.cfg.Endpoints[src.slot], &clusterhook.ExportRequest{Op: clusterhook.OpExportWAL, Observer: o.cfg.Name, Name: name}, nil)
		if err != nil {
			errs = append(errs, fmt.Errorf("%s: %w", src.slot, err))
			continue
		}
		path := o.store.newWALPath(cp.SystemID, name)
		// A segment is checked against its name and the checkpoint's
		// system before it takes the name.
		br := bufio.NewReaderSize(body, walHeaderSize)
		head, _ := br.Peek(walHeaderSize)
		err = checkSegmentHeader(head, name, cp.SystemID, cp.WALSegmentSize)
		var n int64
		var sum string
		if err == nil {
			n, sum, err = writeStream(path, br, f.Size, !strings.HasSuffix(name, ".history"))
			if err != nil {
				path = ""
			}
		}
		body.Close()
		if err == nil && n != f.Size {
			err = fmt.Errorf("%d bytes, not %d", n, f.Size)
		}
		if err == nil && f.SHA256 != "" && sum != f.SHA256 {
			err = errors.New("the bytes do not match the listed digest")
		}
		if err == nil {
			return nil
		}
		if path != "" {
			os.Remove(path)
		}
		errs = append(errs, fmt.Errorf("%s: %w", src.slot, err))
	}
	if len(errs) == 0 {
		return nil
	}
	return fmt.Errorf("WAL %s: %w", name, errors.Join(errs...))
}

func (o *Observer) fetchBase(ctx context.Context, leader string) (err error) {
	body, err := call(ctx, o.cfg.Endpoints[leader], &clusterhook.ExportRequest{Op: clusterhook.OpExportPGBase, Observer: o.cfg.Name}, nil)
	if err != nil {
		return err
	}
	defer body.Close()
	// A label is to the second: a second base within the same second (a
	// gap right after a scheduled base) takes the next free suffix, which
	// still sorts after the first, instead of failing on the rename.
	label := o.cfg.Now().UTC().Format("20060102T150405Z")
	for i, first := 1, label; ; i++ {
		free, err := o.freeLabel(label)
		if err != nil {
			// A storage failure is reported, never taken for a used label.
			return fmt.Errorf("base backup label %s: %w", label, err)
		}
		if free {
			break
		}
		label = fmt.Sprintf("%s-%02d", first, i)
	}
	stage := o.store.path("pg", "staging", label)
	// A base refused leaves nothing in staging: a tar left there would
	// count against the quota until a restart (#112).
	defer func() {
		if err != nil {
			_ = os.RemoveAll(stage)
		}
	}()
	if err := os.MkdirAll(stage, 0o700); err != nil {
		return err
	}
	limit := o.cfg.StagingQuota
	if limit <= 0 {
		limit = 1 << 40
	}
	n, sum, err := writeStream(stage+"/base.tar", body, limit, false)
	if err != nil {
		return err
	}
	f, err := os.Open(stage + "/base.tar")
	if err != nil {
		return err
	}
	info, err := ScanBaseTar(f)
	f.Close()
	if err != nil {
		return err
	}
	info.Label, info.Size, info.SHA256 = label, n, sum
	b, _ := json.Marshal(info)
	if err := writeAtomic(stage+"/base.json", b, false); err != nil {
		return err
	}
	if err := os.Rename(stage, o.store.path("pg", "base", label)); err != nil {
		return err
	}
	if err := syncDir(o.store.path("pg", "base")); err != nil {
		return err
	}
	o.state.Bases = append(o.state.Bases, info)
	log.Info("cluster observer: base backup %s at %s on timeline %d (%d bytes)", label, info.Start, info.Timeline, n)
	if err := o.store.saveState(o.state); err != nil {
		return err
	}
	return o.dropIdleBases(label)
}

// dropIdleBases removes the base backups no published checkpoint restores
// from, but newest: a base taken because the older ones cover nothing
// replaces them, instead of piling up beside them (#57).
func (o *Observer) dropIdleBases(newest string) error {
	used := map[string]bool{newest: true}
	for _, name := range o.store.Published() {
		m, err := o.store.ReadManifest(name)
		if err != nil {
			// A manifest that does not read keeps every base.
			return nil
		}
		used[m.PG.Base.Label] = true
	}
	var kept []BaseInfo
	var drop []string
	for _, b := range o.state.Bases {
		if used[b.Label] {
			kept = append(kept, b)
		} else {
			drop = append(drop, b.Label)
		}
	}
	if len(drop) == 0 {
		return nil
	}
	o.state.Bases = kept
	if err := o.store.saveState(o.state); err != nil {
		return err
	}
	for _, label := range drop {
		if err := os.RemoveAll(o.store.path("pg", "base", label)); err != nil {
			return err
		}
		log.Info("cluster observer: base backup %s let go: no checkpoint restores from it", label)
	}
	return nil
}

// freeLabel reports whether no base backup, published or in staging, has
// label.
func (o *Observer) freeLabel(label string) (bool, error) {
	for _, dir := range []string{"base", "staging"} {
		_, err := os.Stat(o.store.path("pg", dir, label))
		if err == nil {
			return false, nil
		}
		if !errors.Is(err, os.ErrNotExist) {
			return false, err
		}
	}
	return true, nil
}

// ---- protection ----

// protectPending runs the snapshot hook for every verified checkpoint not
// yet protected, oldest first. A failure stops ingestion and pruning until
// the hook succeeds; a retry runs it again for the same checkpoint.
func (o *Observer) protectPending(ctx context.Context) error {
	for i := range o.state.Checkpoints {
		a := &o.state.Checkpoints[i]
		// Only a verified checkpoint waits for its snapshot: a pruned one
		// was protected before its directory went, and is never protected
		// again.
		if a.State != StateVerified {
			continue
		}
		if p, ok := o.store.Protection(a.Name); ok {
			// The record was written and the state not: a crash between.
			a.State, a.Snapshot = StateProtected, p.Snapshot
			continue
		}
		snap, err := o.runHook(ctx, a.Name)
		if err != nil {
			o.state.HookFailing = fmt.Sprintf("%s: %v", a.Name, err)
			_ = o.store.saveState(o.state)
			return fmt.Errorf("%w: %s", ErrHookFailing, o.state.HookFailing)
		}
		p := Protection{Checkpoint: a.Name, Snapshot: snap, At: o.cfg.Now().UTC()}
		b, _ := json.Marshal(p)
		if err := writeAtomic(o.store.path("checkpoints", a.Name, "PROTECTED"), b, false); err != nil {
			return err
		}
		a.State, a.Snapshot = StateProtected, snap
		o.state.HookFailing = ""
		if err := o.store.saveState(o.state); err != nil {
			return err
		}
		log.Info("cluster observer: checkpoint %s protected by snapshot %s", a.Name, snap)
	}
	return o.store.saveState(o.state)
}

// runHook runs the snapshot hook: it must snapshot the whole observer
// directory atomically, print the snapshot's identity on its first line,
// and exit 0. It is given the checkpoint, and must be idempotent for it.
func (o *Observer) runHook(ctx context.Context, checkpoint string) (string, error) {
	if o.cfg.Hook != nil {
		return o.cfg.Hook(ctx, checkpoint)
	}
	var out, errb bytes.Buffer
	cmd := exec.CommandContext(ctx, o.cfg.HookCommand, checkpoint)
	cmd.Env = append(os.Environ(), "OBSERVER_DIR="+o.cfg.Dir, "OBSERVER_CHECKPOINT="+checkpoint)
	cmd.Stdout, cmd.Stderr = &out, &errb
	if err := cmd.Run(); err != nil {
		return "", fmt.Errorf("%w: %s", err, strings.TrimSpace(errb.String()))
	}
	id, _, _ := strings.Cut(strings.TrimSpace(out.String()), "\n")
	if id == "" {
		return "", errors.New("the hook printed no snapshot identity")
	}
	return id, nil
}
