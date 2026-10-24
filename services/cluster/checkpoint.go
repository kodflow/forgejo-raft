// Copyright 2026 The Forgejo Authors. All rights reserved.
// SPDX-License-Identifier: GPL-3.0-or-later

package cluster

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	cluster_model "forgejo.org/models/cluster"
	"forgejo.org/models/db"
	"forgejo.org/modules/clusterhook"
	"forgejo.org/modules/json"
	"forgejo.org/modules/log"
	"forgejo.org/modules/setting"
	"forgejo.org/services/cluster/consensus"
	"forgejo.org/services/cluster/reftable"
)

// The checkpoint coordinator runs on the leader. Each period it pairs the
// table at one index with PostgreSQL at one WAL position, so an observer
// holds refs and SQL of the same cut (observer plan v4, causal cut):
//
//  1. the leader's own writers queue behind its fence, and the log
//     consumer is held at the last applied index;
//  2. a barrier entry commits at B; its Apply captures the table at B;
//  3. the consumer is let through to B and no further, and drained: the
//     SQL of every entry up to B committed;
//  4. under that fence, PostgreSQL's flushed WAL position L, its timeline
//     and system identifier are read, and a restore point is created;
//  5. the fence is released, the WAL switched so L is archived soon, the
//     artifacts written, and a position entry closing B proposed.
//
// SQL committed after step 4 is past L, whatever entry it derives from.
// Followers' pushes are ordered after B by the log, and their SQL is held
// by the consumer. A deletion, a rename or a transfer writes its own SQL
// beside its entry, on any member, and a creation, a fork or a generation
// from a template writes a repository's refs inside the transaction of its
// row: each holds the cluster's cut across both, which the coordinator
// takes with the fence (cut.go), so both fall on one side of (B, L). The
// branch rows a mirror's synchronisation or a migration writes long after
// their refs derive from the refs, and a Recover derives them again (#159).
// What else a web request writes to SQL beside a push is reconciled after
// a Recover (doctor, pull request heads). Nothing here waits on an
// observer.

// readToCommitBound is how long the position may take to commit once read:
// a checkpoint that exceeds it is discarded and taken again.
const readToCommitBound = 5 * time.Second

// drainBound is how long the fence waits for the consumer to reach B.
const drainBound = 5 * time.Second

// checkpointArtifactsKept is how many checkpoints' artifacts the leader
// keeps for the export; older checkpoints stay in the table, without them.
const checkpointArtifactsKept = 8

// walMark is PostgreSQL's position as the coordinator read it.
type walMark struct {
	LSN         string
	Timeline    uint32
	SystemID    string
	SegmentSize int64
}

// walSource is the primary PostgreSQL, as the coordinator and the export
// see it; tests replace it.
type walSource interface {
	// Mark reads the flushed WAL position, its timeline and the system
	// identifier, then creates a restore point named name.
	Mark(ctx context.Context, name string) (walMark, error)
	// SwitchWAL closes the current segment, so the archive gets it now.
	SwitchWAL(ctx context.Context) error
	// Foreign lists the databases beside Forgejo's, template0, template1
	// and postgres, and the tablespaces beside pg_default and pg_global:
	// an export of the whole PostgreSQL cluster must hold Forgejo alone.
	Foreign(ctx context.Context) (databases, tablespaces []string, err error)
}

type coordinator struct {
	r        *Replicator
	interval time.Duration
	dir      string
	wal      walSource
	// files lists the stored files after the fence, under the cut; their
	// digests are read once it is released. Tests replace it.
	files func() (*storedListing, error)
	now   func() time.Time
	// backup records the backup cursor; tests replace it.
	backup func(ctx context.Context, name string, next cluster_model.Cursor) error

	mu      sync.Mutex
	waiting string
	got     chan capturedTable
}

// capturedTable is the table as a barrier's Apply encoded it, with that
// barrier's index.
type capturedTable struct {
	b     []byte
	index uint64
}

func newCoordinator(r *Replicator, interval time.Duration, dir string, wal walSource) *coordinator {
	c := &coordinator{r: r, interval: interval, dir: dir, wal: wal, files: storedFiles, now: time.Now, backup: cluster_model.SetCursor}
	r.table.OnCheckpoint(c.capture)
	return c
}

// fenceAndCut takes the leader's fence, then the cluster's cut, within
// drainBound; neither is waited for while the other is held. It returns
// with the fence held, and the function that releases the cut, once.
func (c *coordinator) fenceAndCut(r *Replicator) (func(), error) {
	deadline := c.now().Add(drainBound)
	for {
		if r.fence.TryLock() {
			if r.cut == nil {
				return func() {}, nil
			}
			ctx, cancel := context.WithTimeout(context.Background(), drainBound)
			unlock, ok, err := r.cut.TryExclusive(ctx)
			cancel()
			if err != nil {
				r.fence.Unlock()
				return nil, fmt.Errorf("taking the checkpoint's cut: %w", err)
			}
			if ok {
				var once sync.Once
				return func() { once.Do(unlock) }, nil
			}
			r.fence.Unlock()
		}
		if c.now().After(deadline) {
			return nil, errors.New("writers, or a deletion, a rename or a transfer, kept the fence for longer than the bound")
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// capture keeps the table at the barrier this node is waiting for. It runs
// under the table's lock: it encodes and hands the bytes over, nothing more.
func (c *coordinator) capture(nonce string, index uint64, encode func() ([]byte, error)) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if nonce != c.waiting || c.got == nil {
		return
	}
	b, err := encode()
	if err != nil {
		log.Error("cluster checkpoint: encoding the table at the barrier: %v", err)
		b = nil
	}
	select {
	case c.got <- capturedTable{b: b, index: index}:
	default:
	}
}

func (c *coordinator) expect(nonce string) chan capturedTable {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.waiting, c.got = nonce, make(chan capturedTable, 1)
	return c.got
}

func (c *coordinator) run(ctx context.Context) {
	t := time.NewTicker(c.interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
		eng := c.r.Engine()
		if eng == nil || eng.Status().Role != consensus.RoleLeader || c.r.writerGate(eng) != nil {
			continue
		}
		rec, err := c.take(ctx)
		if err != nil {
			checkpointsTotal.WithLabelValues("failed").Inc()
			log.Warn("cluster checkpoint: %v; the next period takes another", err)
			continue
		}
		checkpointsTotal.WithLabelValues("closed").Inc()
		log.Info("cluster checkpoint %s closed: index %d, PostgreSQL %s on timeline %d", rec.Name(), rec.Index, rec.LSN, rec.Timeline)
	}
}

// take runs one checkpoint, steps 1 to 5.
func (c *coordinator) take(ctx context.Context) (reftable.CheckpointRecord, error) {
	r := c.r
	eng := r.Engine()
	nonce := clusterhook.NewNonce()
	got := c.expect(nonce)
	defer c.expect("")

	// 1. The fence: the leader's writers queue, the consumer holds; and
	// the cut: a deletion, a rename, a transfer or the deletion of a
	// stored file on any member waits (cut.go). A writer in flight
	// (objects on their way to a majority) or an operation holding the cut
	// is not waited for past the bound: this period is skipped instead.
	unlockCut, err := c.fenceAndCut(r)
	if err != nil {
		return reftable.CheckpointRecord{}, err
	}
	defer unlockCut()
	fenced := true
	release := func() {
		if fenced {
			r.consumer.release()
			r.fence.Unlock()
			fenced = false
		}
	}
	defer release()
	applied, _ := r.table.Applied()
	r.consumer.holdAt(applied.Index)

	// 2. The barrier, and the table at it.
	data, err := reftable.Encode(reftable.KindCheckpointBarrier, &reftable.CheckpointBarrier{Nonce: nonce, Time: c.now().UnixNano()})
	if err != nil {
		return reftable.CheckpointRecord{}, err
	}
	bctx, cancel := context.WithTimeout(ctx, drainBound)
	defer cancel()
	tok, res, err := r.proposeCommand(bctx, data, nil)
	if err != nil {
		return reftable.CheckpointRecord{}, fmt.Errorf("barrier: %w", err)
	}
	if !res.OK {
		return reftable.CheckpointRecord{}, fmt.Errorf("barrier refused: %s", res.Reason)
	}
	if err := eng.WaitApplied(bctx, tok); err != nil {
		return reftable.CheckpointRecord{}, fmt.Errorf("barrier not applied here: %w", err)
	}
	snap, err := tableAtBarrier(bctx, got, tok.Index)
	if err != nil {
		return reftable.CheckpointRecord{}, err
	}

	// 3. The consumer up to B, and no further.
	r.consumer.holdAt(tok.Index)
	r.signalConsumer()
	for {
		ok, err := r.consumer.drained(bctx, tok)
		if err == nil && ok {
			break
		}
		select {
		case <-bctx.Done():
			return reftable.CheckpointRecord{}, fmt.Errorf("the SQL of the entries up to %d did not drain within %s", tok.Index, drainBound)
		case <-time.After(20 * time.Millisecond):
		}
	}
	// Whether the lineages before B's were drained too, read before
	// PostgreSQL's position: their cursors never go back, so the SQL at
	// that position holds them at least there. It is recorded with the
	// backup cursor, not waited for: a checkpoint right after a Recover
	// does not wait for the old lineage's tail.
	earlier := r.consumer.earlierDrained(bctx, tok.Identity())

	// 4. PostgreSQL's position under the fence.
	name := reftable.CheckpointName(tok.Identity().String(), tok.Index)
	mark, err := c.wal.Mark(bctx, name)
	if err != nil {
		return reftable.CheckpointRecord{}, fmt.Errorf("reading PostgreSQL's position: %w", err)
	}
	readAt := c.now()

	// 5. Released: the rest is bounded, and waits on nobody. The stored
	// files are listed here, outside the fence: a walk of a large storage
	// would otherwise freeze the leader's writers for its whole length.
	// The cut is still held: no stored file is deleted until the walk
	// ends, so the list holds every file the SQL at L names (#90), plus
	// files added since, which a restore ignores. The digests of the
	// attachments are read once the cut is released: the cut guarantees
	// the files are listed, not how long they take to read, and a first
	// checkpoint on a new leader reads every one of them (#161).
	release()
	listing, err := c.files()
	unlockCut()
	if err != nil {
		return reftable.CheckpointRecord{}, fmt.Errorf("listing the stored files: %w", err)
	}
	files, err := listing.digest()
	if err != nil {
		return reftable.CheckpointRecord{}, fmt.Errorf("reading the stored files' digests: %w", err)
	}
	// The bound counts from the end of the walk and of the digests: their
	// length depends on the storage, not on the cluster, and must not
	// discard every checkpoint.
	listedAt := c.now()
	if err := c.wal.SwitchWAL(ctx); err != nil {
		log.Warn("cluster checkpoint: switching the WAL segment: %v", err)
	}
	manifest, err := json.Marshal(clusterhook.FilesManifest{Checkpoint: name, Files: files})
	if err != nil {
		return reftable.CheckpointRecord{}, err
	}
	refsSum, err := c.writeArtifact(name, "refs.snapshot", snap)
	if err != nil {
		return reftable.CheckpointRecord{}, err
	}
	filesSum, err := c.writeArtifact(name, "files.json", manifest)
	if err != nil {
		return reftable.CheckpointRecord{}, err
	}
	pos := &reftable.CheckpointPosition{
		Nonce: nonce, Index: tok.Index, LSN: mark.LSN, Timeline: mark.Timeline, SystemID: mark.SystemID,
		RestorePoint: name, WALSegmentSize: mark.SegmentSize, RefsSHA256: refsSum, FilesSHA256: filesSum,
		ReadAt: readAt.UnixNano(),
	}
	if data, err = reftable.Encode(reftable.KindCheckpointPosition, pos); err != nil {
		return reftable.CheckpointRecord{}, err
	}
	left := readToCommitBound - c.now().Sub(listedAt)
	if left <= 0 {
		return reftable.CheckpointRecord{}, fmt.Errorf("checkpoint %s discarded: %s between the read and the proposal", name, readToCommitBound)
	}
	pctx, pcancel := context.WithTimeout(ctx, left)
	defer pcancel()
	_, res, err = r.proposeCommand(pctx, data, nil)
	if err != nil {
		return reftable.CheckpointRecord{}, fmt.Errorf("checkpoint %s discarded: the position did not commit within %s: %w", name, readToCommitBound, err)
	}
	if !res.OK {
		return reftable.CheckpointRecord{}, fmt.Errorf("checkpoint %s refused: %s", name, res.Reason)
	}
	// The backup cursor: a PostgreSQL restored to this checkpoint's position
	// holds the SQL of every entry up to B, and the log must keep what
	// follows for it to be replayed; the consumer never trims past it.
	// Closed says that it holds the earlier lineages drained as well: the
	// consumer may then trim their tails to their end (#205).
	if r.consumer != nil && r.consumer.dbReady() {
		if err := c.backup(ctx, cluster_model.CursorBackup, cluster_model.Cursor{Lineage: tok.Identity().String(), Position: int64(tok.Index), Closed: earlier}); err != nil {
			log.Warn("cluster checkpoint %s: recording the backup cursor: %v", name, err)
		}
	}
	c.prune(tok.Identity().String())
	for _, cp := range r.table.Checkpoints() {
		if cp.Nonce == nonce {
			return cp, nil
		}
	}
	return reftable.CheckpointRecord{}, fmt.Errorf("checkpoint %s committed and not yet applied here", name)
}

// tableAtBarrier is the table captured at the barrier that committed at
// index. A barrier proposed again after an unknown outcome applies twice
// under one nonce: the first capture is then another index than the
// position names, and the checkpoint is not taken.
func tableAtBarrier(ctx context.Context, got <-chan capturedTable, index uint64) ([]byte, error) {
	var c capturedTable
	select {
	case c = <-got:
	case <-ctx.Done():
	}
	switch {
	case c.b == nil:
		return nil, errors.New("the table was not captured at the barrier")
	case c.index != index:
		return nil, fmt.Errorf("the table was captured at %d, the barrier committed at %d: the barrier was proposed twice", c.index, index)
	}
	return c.b, nil
}

// writeArtifact writes one artifact of a checkpoint durably and returns
// its SHA-256, which the position entry carries.
func (c *coordinator) writeArtifact(name, file string, b []byte) (string, error) {
	dir := filepath.Join(c.dir, name)
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return "", err
	}
	tmp := filepath.Join(dir, file+".partial")
	f, err := os.OpenFile(tmp, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o600)
	if err != nil {
		return "", err
	}
	_, err = f.Write(b)
	if err == nil {
		err = f.Sync()
	}
	if cerr := f.Close(); err == nil {
		err = cerr
	}
	if err == nil {
		err = os.Rename(tmp, filepath.Join(dir, file))
	}
	if err == nil {
		err = syncDir(dir)
	}
	if err != nil {
		os.Remove(tmp)
		return "", err
	}
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:]), nil
}

// prune keeps the artifacts of the newest checkpoints only.
// prune keeps the artifacts of the newest checkpointArtifactsKept
// checkpoints of lineage, by index, and removes every other lineage's: a
// Recover or a restore starts a lineage whose name may sort before the old
// one's, and the export serves the current lineage only.
func (c *coordinator) prune(lineage string) {
	entries, err := os.ReadDir(c.dir)
	if err != nil {
		return
	}
	prefix := strings.TrimSuffix(reftable.CheckpointName(lineage, 0), fmt.Sprintf("%020d", 0))
	type kept struct {
		name  string
		index uint64
	}
	var current []kept
	for _, e := range entries {
		if !e.IsDir() || !checkpointNamePattern.MatchString(e.Name()) {
			continue
		}
		rest, ok := strings.CutPrefix(e.Name(), prefix)
		index, err := strconv.ParseUint(rest, 10, 64)
		if !ok || err != nil || len(rest) != 20 {
			_ = os.RemoveAll(filepath.Join(c.dir, e.Name()))
			continue
		}
		current = append(current, kept{e.Name(), index})
	}
	sort.Slice(current, func(i, j int) bool { return current[i].index < current[j].index })
	for len(current) > checkpointArtifactsKept {
		_ = os.RemoveAll(filepath.Join(c.dir, current[0].name))
		current = current[1:]
	}
}

var checkpointNamePattern = regexp.MustCompile(`^[0-9a-f]{32}-[0-9]+-[0-9]{20}$`)

// ---- the consumer's side of the fence ----

// holdAt lets the consumer dispatch the events up to index, no further.
func (c *consumer) holdAt(index uint64) {
	c.gate.Lock()
	c.hold = index
	c.held = true
	c.gate.Unlock()
}

// release lifts the hold.
func (c *consumer) release() {
	c.gate.Lock()
	c.held = false
	c.gate.Unlock()
}

// limit returns the index the consumer may dispatch up to, and whether
// one is set.
func (c *consumer) limit() (uint64, bool) {
	c.gate.RLock()
	defer c.gate.RUnlock()
	return c.hold, c.held
}

// drained reports whether the SQL of every event up to tok committed.
func (c *consumer) drained(ctx context.Context, tok consensus.Token) (bool, error) {
	var last uint64
	for _, ev := range c.r.table.Events() {
		if ev.Token.Identity() == tok.Identity() && ev.Token.Index <= tok.Index && ev.Token.Index > last {
			last = ev.Token.Index
		}
	}
	if last == 0 {
		return true, nil
	}
	lin := tok.Identity().String()
	cur, err := c.cursor(ctx, cursorName(lin))
	if err != nil {
		return false, err
	}
	return cur.Lineage == lin && uint64(cur.Position) >= last, nil
}

// earlierDrained reports whether every event the table holds of a lineage
// other than id's, the current one, is behind that lineage's cursor in
// SQL: dispatched, or parked with its row. drained waits for id's events
// only, and a checkpoint right after a Recover or a restore may be taken
// while the old lineage's tail still waits for its work; its backup holds
// that lineage's cursor behind its end. Anything it cannot read says no.
func (c *consumer) earlierDrained(ctx context.Context, id consensus.LogIdentity) bool {
	current := id.String()
	for lin, tail := range lastIndexes(c.events()) {
		if lin == current {
			continue
		}
		cur, err := c.cursor(ctx, cursorName(lin))
		if err != nil || cur.Lineage != lin || uint64(cur.Position) < tail {
			return false
		}
	}
	return true
}

// ---- PostgreSQL ----

// pgWAL is the primary PostgreSQL through Forgejo's own connection. The
// restore point and the WAL switch run under [cluster] CHECKPOINT_ROLE, a
// role granted EXECUTE on pg_create_restore_point, pg_switch_wal and
// pg_control_system only, which Forgejo's user may SET ROLE to.
type pgWAL struct{ role string }

var pgIdentifier = regexp.MustCompile(`^[a-z_][a-z0-9_]{0,62}$`)

func (p pgWAL) asRole(ctx context.Context, f func(e db.Engine) error) error {
	if !pgIdentifier.MatchString(p.role) {
		return fmt.Errorf("CHECKPOINT_ROLE %q is not a plain PostgreSQL identifier", p.role)
	}
	return db.WithTx(ctx, func(ctx context.Context) error {
		e := db.GetEngine(ctx)
		if _, err := e.Exec("SET LOCAL ROLE " + p.role); err != nil {
			return err
		}
		return f(e)
	})
}

func queryOne(e db.Engine, query string, args ...any) (map[string]string, error) {
	rows, err := e.Query(append([]any{query}, args...)...)
	if err != nil {
		return nil, err
	}
	if len(rows) != 1 {
		return nil, fmt.Errorf("%d rows for %q", len(rows), query)
	}
	out := map[string]string{}
	for k, v := range rows[0] {
		out[k] = string(v)
	}
	return out, nil
}

func (p pgWAL) Mark(ctx context.Context, name string) (walMark, error) {
	var m walMark
	err := p.asRole(ctx, func(e db.Engine) error {
		// pg_walfile_name refuses to run on a standby: the position is the
		// primary's, and the file name carries its timeline.
		row, err := queryOne(e, `SELECT pg_current_wal_flush_lsn()::text AS lsn,
			pg_walfile_name(pg_current_wal_flush_lsn()) AS walfile,
			(SELECT system_identifier::text FROM pg_control_system()) AS sysid,
			current_setting('wal_segment_size') AS segsize`)
		if err != nil {
			return err
		}
		tli, err := strconv.ParseUint(row["walfile"][:8], 16, 32)
		if err != nil || len(row["walfile"]) != 24 {
			return fmt.Errorf("WAL file name %q", row["walfile"])
		}
		size, err := parsePGSize(row["segsize"])
		if err != nil {
			return err
		}
		m = walMark{LSN: strings.ToUpper(row["lsn"]), Timeline: uint32(tli), SystemID: row["sysid"], SegmentSize: size}
		_, err = queryOne(e, "SELECT pg_create_restore_point(?)::text AS rp", name)
		return err
	})
	return m, err
}

func (p pgWAL) SwitchWAL(ctx context.Context) error {
	return p.asRole(ctx, func(e db.Engine) error {
		_, err := queryOne(e, "SELECT pg_switch_wal()::text AS lsn")
		return err
	})
}

func (p pgWAL) Foreign(ctx context.Context) ([]string, []string, error) {
	var dbs, spcs []string
	err := db.WithTx(ctx, func(ctx context.Context) error {
		e := db.GetEngine(ctx)
		rows, err := e.Query("SELECT datname FROM pg_database WHERE datname NOT IN (current_database(), 'template0', 'template1', 'postgres') ORDER BY datname")
		if err != nil {
			return err
		}
		for _, r := range rows {
			dbs = append(dbs, string(r["datname"]))
		}
		rows, err = e.Query("SELECT spcname FROM pg_tablespace WHERE spcname NOT IN ('pg_default', 'pg_global') ORDER BY spcname")
		if err != nil {
			return err
		}
		for _, r := range rows {
			spcs = append(spcs, string(r["spcname"]))
		}
		return nil
	})
	return dbs, spcs, err
}

// parsePGSize reads a size as current_setting prints it: 16MB, 1GB.
func parsePGSize(s string) (int64, error) {
	units := map[string]int64{"kB": 1 << 10, "MB": 1 << 20, "GB": 1 << 30}
	for u, mul := range units {
		if n, ok := strings.CutSuffix(s, u); ok {
			v, err := strconv.ParseInt(n, 10, 64)
			return v * mul, err
		}
	}
	return strconv.ParseInt(s, 10, 64)
}

// ---- stored files ----

// The storages a checkpoint covers, by the name the export uses.
const (
	StorageLFS         = "lfs"
	StorageAttachments = "attachments"
	StoragePackages    = "packages"
)

// storageRoots returns the local roots of the stored files; a storage that
// is not local is refused, since the export cannot copy it.
func storageRoots() (map[string]string, error) {
	sts, err := storageConfigs()
	if err != nil {
		return nil, err
	}
	out := map[string]string{}
	for name, st := range sts {
		out[name] = st.Path
	}
	return out, nil
}

// storageConfigs returns the local storages a checkpoint covers.
func storageConfigs() (map[string]*setting.Storage, error) {
	out := map[string]*setting.Storage{}
	for name, st := range map[string]*setting.Storage{
		StorageLFS: setting.LFS.Storage, StorageAttachments: setting.Attachment.Storage, StoragePackages: setting.Packages.Storage,
	} {
		if st == nil {
			continue
		}
		if st.Type != setting.LocalStorageType {
			return nil, fmt.Errorf("the %s storage is %q: observers support local storage only", name, st.Type)
		}
		out[name] = st
	}
	return out, nil
}

// storageTemporaryPath is where a local storage writes an upload before
// renaming it into place: <root>/tmp unless TEMPORARY_PATH says otherwise.
func storageTemporaryPath(st *setting.Storage) string {
	if st.TemporaryPath != "" {
		return filepath.Clean(st.TemporaryPath)
	}
	return filepath.Join(st.Path, "tmp")
}

// storedFiles lists every stored file, by storage and relative path.
func storedFiles() (*storedListing, error) {
	sts, err := storageConfigs()
	if err != nil {
		return nil, err
	}
	return listStoredFiles(sts, storedDigests)
}

// digestCache keeps the digests of stored files by path, size and
// modification time: a checkpoint reads again only what changed since the
// one before.
type digestCache struct {
	mu   sync.Mutex
	sums map[string]digestEntry
}

type digestEntry struct {
	size  int64
	mtime int64
	sum   string
}

// storedDigests is the process's: it outlives a coordinator, and a member
// that leads again reads only what changed since its last checkpoint.
var storedDigests = newDigestCache()

func newDigestCache() *digestCache { return &digestCache{sums: map[string]digestEntry{}} }

// cached is the digest kept for a file still of this size and time.
func (c *digestCache) cached(path string, info fs.FileInfo) (string, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	e, ok := c.sums[path]
	if !ok || e.size != info.Size() || e.mtime != info.ModTime().UnixNano() {
		return "", false
	}
	return e.sum, true
}

func (c *digestCache) put(path string, info fs.FileInfo, sum string) {
	c.mu.Lock()
	c.sums[path] = digestEntry{size: info.Size(), mtime: info.ModTime().UnixNano(), sum: sum}
	c.mu.Unlock()
}

// keep forgets the files a listing did not see: they are gone.
func (c *digestCache) keep(seen map[string]bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for p := range c.sums {
		if !seen[p] {
			delete(c.sums, p)
		}
	}
}

// storedListing is the stored files as a walk found them. The digests of
// the attachments the cache did not hold are read by digest, after the
// walk and outside the checkpoint's cut (#161).
type storedListing struct {
	files   []clusterhook.FileEntry
	pending []pendingDigest
	// seen is every attachment listed, which the cache keeps.
	seen map[string]bool
	// cache holds the digests read, by the walk's paths.
	cache *digestCache
	// read reads one file's digest; tests replace it.
	read func(path string) (string, error)
}

// pendingDigest is an attachment whose digest is still to be read: the
// file at path, as the walk found it, listed at files[at].
type pendingDigest struct {
	at   int
	path string
	info fs.FileInfo
}

// digest reads the digests the walk left out, and returns the files. A
// file is read only if it is still the one the walk listed: one gone or
// changed since fails the checkpoint, which the next period takes again,
// the digests read so far cached. The cut no longer holds the deletions
// back, and a file the SQL at L names may have gone meanwhile: listing it
// without its digest would leave the observer to take it as it comes, and
// leaving it out would lose it.
func (l *storedListing) digest() ([]clusterhook.FileEntry, error) {
	if l == nil {
		return nil, nil
	}
	read := l.read
	if read == nil {
		read = fileDigest
	}
	var errs []error
	for _, p := range l.pending {
		sum, err := l.readListed(p, read)
		if err != nil {
			f := l.files[p.at]
			errs = append(errs, fmt.Errorf("%s/%s: %w", f.Storage, f.Path, err))
			continue
		}
		l.cache.put(p.path, p.info, sum)
		l.files[p.at].SHA256 = sum
	}
	if len(errs) > 0 {
		return nil, errors.Join(errs...)
	}
	l.cache.keep(l.seen)
	return l.files, nil
}

func (l *storedListing) readListed(p pendingDigest, read func(string) (string, error)) (string, error) {
	unchanged := func() error {
		info, err := os.Stat(p.path)
		switch {
		case errors.Is(err, fs.ErrNotExist):
			return errors.New("listed, and gone before its digest was read")
		case err != nil:
			return err
		case info.Size() != p.info.Size() || !info.ModTime().Equal(p.info.ModTime()):
			return errors.New("listed, and changed before its digest was read")
		}
		return nil
	}
	if err := unchanged(); err != nil {
		return "", err
	}
	sum, err := read(p.path)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return "", errors.New("listed, and gone before its digest was read")
		}
		return "", err
	}
	// Read whole, and still the file listed: the digest is its own.
	if err := unchanged(); err != nil {
		return "", err
	}
	return sum, nil
}

// listStoredFiles walks each storage root. An upload in flight lives in
// the storage's temporary directory until it is renamed: listing it would
// name a file that is gone, or partial, when an observer asks for it, and
// fail the whole checkpoint. It reads no file: an attachment's digest is
// taken from the cache, or left to the listing's digest. A cache forgets what
// its last listing did not see: one cache serves one set of roots.
func listStoredFiles(roots map[string]*setting.Storage, cache *digestCache) (*storedListing, error) {
	type listed struct {
		e    clusterhook.FileEntry
		path string
		info fs.FileInfo
	}
	var found []listed
	for name, st := range roots {
		root, tmp := st.Path, storageTemporaryPath(st)
		err := filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
			if err != nil {
				if errors.Is(err, fs.ErrNotExist) {
					return nil
				}
				return err
			}
			if d.IsDir() && path != root && filepath.Clean(path) == tmp {
				return filepath.SkipDir
			}
			if !d.Type().IsRegular() {
				return nil
			}
			info, err := d.Info()
			if err != nil {
				return err
			}
			rel, err := filepath.Rel(root, path)
			if err != nil {
				return err
			}
			found = append(found, listed{clusterhook.FileEntry{Storage: name, Path: filepath.ToSlash(rel), Size: info.Size()}, path, info})
			return nil
		})
		if err != nil {
			return nil, err
		}
	}
	sort.Slice(found, func(i, j int) bool {
		if found[i].e.Storage != found[j].e.Storage {
			return found[i].e.Storage < found[j].e.Storage
		}
		return found[i].e.Path < found[j].e.Path
	})
	l := &storedListing{files: make([]clusterhook.FileEntry, len(found)), seen: map[string]bool{}, cache: cache}
	for i, f := range found {
		l.files[i] = f.e
		// An LFS object's or a package blob's path is its digest; an
		// attachment's is not, and its digest is listed (#106).
		if f.e.Storage != StorageAttachments {
			continue
		}
		l.seen[f.path] = true
		if sum, ok := cache.cached(f.path, f.info); ok {
			l.files[i].SHA256 = sum
			continue
		}
		l.pending = append(l.pending, pendingDigest{at: i, path: f.path, info: f.info})
	}
	return l, nil
}
