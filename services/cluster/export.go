// Copyright 2026 The Forgejo Authors. All rights reserved.
// SPDX-License-Identifier: GPL-3.0-or-later

package cluster

import (
	"bufio"
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"time"

	"forgejo.org/modules/clusterhook"
	"forgejo.org/modules/json"
	"forgejo.org/modules/log"
	"forgejo.org/modules/setting"
	"forgejo.org/services/cluster/consensus"
	"forgejo.org/services/cluster/reftable"

	"golang.org/x/crypto/nacl/box"
)

// The observers' export (observer plan v4, E1). `forgejo cluster export`,
// an observer's SSH forced command, forwards each call here over the
// private socket: the command never opens the data directory, and every
// op is a read. The log, the checkpoints and their artifacts come from the
// leader alone, which confirms its commit with a quorum before answering;
// a member that does not lead answers NOT_LEADER with the leader's slot,
// which an observer follows only to an endpoint it pins. Packs, WAL and
// files come from any member: the observer checks them by digest against
// what the leader told it.

// exportSide is the PostgreSQL and archive side of the export; tests
// replace its parts.
type exportSide struct {
	wal walSource
	// baseBackup streams a base backup of the primary as one tar.
	baseBackup func(ctx context.Context, w io.Writer) error
	archiveDir string
	ckptDir    string
	storages   func() (map[string]string, error)
	recipients map[string]string
	secretKey  func() string
}

func newExportSide(wal walSource) exportSide {
	return exportSide{
		wal: wal, baseBackup: pgBaseBackup, archiveDir: setting.Cluster.WALArchiveDir, ckptDir: setting.Cluster.CheckpointDir,
		storages: storageRoots, recipients: setting.Cluster.ObserverRecipients, secretKey: func() string { return setting.SecretKey },
	}
}

// logBatchBytes bounds the entries of one export-log answer.
const logBatchBytes = 4 << 20

// walListMax bounds the names of one export-wal-list answer.
const walListMax = 4096

// serveExport answers one export call on conn: the answer's head, then
// for a stream its bytes.
func (r *Replicator) serveExport(ctx context.Context, conn net.Conn, req *clusterhook.ExportRequest) {
	if req == nil {
		req = &clusterhook.ExportRequest{}
	}
	w := bufio.NewWriter(conn)
	defer w.Flush()
	head := func(a clusterhook.ExportAnswer) bool {
		b, err := json.Marshal(a)
		if err != nil {
			return false
		}
		_, err = w.Write(append(b, '\n'))
		return err == nil
	}
	data, stream, err := r.export1(ctx, req)
	if err != nil {
		exportsTotal.WithLabelValues(req.Op, "refused").Inc()
		a := clusterhook.ExportAnswer{Message: err.Error()}
		if ee, ok := errors.AsType[*clusterhook.ExportError](err); ok {
			a.Code, a.Message, a.Leader, a.Term = ee.Code, ee.Message, ee.Leader, ee.Term
		}
		head(a)
		return
	}
	exportsTotal.WithLabelValues(req.Op, "ok").Inc()
	if stream == nil {
		head(clusterhook.ExportAnswer{OK: true, Data: data})
		return
	}
	if !head(clusterhook.ExportAnswer{OK: true, Stream: true, Data: data}) {
		return
	}
	if err := stream(ctx, w); err != nil {
		// The head is gone: the observer sees a stream that does not
		// verify, and discards it.
		log.Warn("cluster export: %s: %v", req.Op, err)
	}
}

type streamFunc func(ctx context.Context, w io.Writer) error

func exportErr(code, format string, args ...any) error {
	return &clusterhook.ExportError{Code: code, Message: fmt.Sprintf(format, args...)}
}

// export1 dispatches an export op: an answer, or a stream.
func (r *Replicator) export1(ctx context.Context, req *clusterhook.ExportRequest) (clusterhook.RawJSON, streamFunc, error) {
	eng := r.Engine()
	if eng == nil {
		return nil, nil, exportErr(clusterhook.CodeUnavailable, "the consensus node is not open")
	}
	switch req.Op {
	case clusterhook.OpExportCheckpoints:
		v, err := r.exportView(ctx, eng, req.FromIndex)
		if err != nil {
			return nil, nil, err
		}
		b, err := json.Marshal(v)
		return b, nil, err
	case clusterhook.OpExportLog:
		s, err := r.exportLog(ctx, eng, req)
		return nil, s, err
	case clusterhook.OpExportRefs, clusterhook.OpExportFiles:
		if err := r.leaderOnly(eng); err != nil {
			return nil, nil, err
		}
		file := map[string]string{clusterhook.OpExportRefs: "refs.snapshot", clusterhook.OpExportFiles: "files.json"}[req.Op]
		s, err := r.export.artifact(req.Checkpoint, file)
		return nil, s, err
	case clusterhook.OpExportPacks:
		s, err := r.exportPacks(ctx, req)
		return nil, s, err
	case clusterhook.OpExportPGBase:
		s, err := r.export.pgBase(ctx)
		return nil, s, err
	case clusterhook.OpExportWALList:
		files, err := r.export.walList(req.Name)
		if err != nil {
			return nil, nil, err
		}
		b, err := json.Marshal(files)
		return b, nil, err
	case clusterhook.OpExportWAL:
		s, err := r.export.walFile(req.Name)
		return nil, s, err
	case clusterhook.OpExportFile:
		s, err := r.export.storedFile(req.Storage, req.Name)
		return nil, s, err
	case clusterhook.OpExportSecrets:
		s, err := r.export.secrets(req.Observer)
		return nil, s, err
	}
	return nil, nil, fmt.Errorf("unknown export op %q", req.Op)
}

// leaderOnly refuses a member that does not lead, naming the leader's slot.
func (r *Replicator) leaderOnly(eng consensus.Engine) error {
	st := eng.Status()
	if st.Role == consensus.RoleLeader {
		return nil
	}
	e := &clusterhook.ExportError{Code: clusterhook.CodeNotLeader, Message: "this member does not lead; ask the leader", Term: st.Term}
	for _, m := range eng.Membership().Members {
		if m.ID == st.Leader && st.Leader != 0 {
			e.Leader = m.Slot
		}
	}
	return e
}

// confirmedCommit is the leader's commit index confirmed by a quorum, and
// applied here: a deposed leader still answering cannot pass it.
func confirmedCommit(ctx context.Context, eng consensus.Engine) (consensus.Token, error) {
	bctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	tok, err := eng.Barrier(bctx)
	if err == nil {
		err = eng.WaitApplied(bctx, tok)
	}
	if err != nil {
		return tok, exportErr(clusterhook.CodeNotLeader, "the leadership is not confirmed: %v", err)
	}
	return tok, nil
}

func (r *Replicator) exportView(ctx context.Context, eng consensus.Engine, from uint64) (*clusterhook.ExportView, error) {
	if err := r.leaderOnly(eng); err != nil {
		return nil, err
	}
	tok, err := confirmedCommit(ctx, eng)
	if err != nil {
		return nil, err
	}
	st := eng.Status()
	_, readable := eng.(consensus.CommittedReader)
	v := &clusterhook.ExportView{
		Lineage: st.Identity.String(), Term: st.Term, Commit: tok.Index, Leader: st.Slot, LogSupported: readable,
		Checkpoints: []clusterhook.ExportCheckpoint{},
	}
	for _, m := range eng.Membership().Members {
		v.Members = append(v.Members, m.Slot)
	}
	for _, cp := range r.table.Checkpoints() {
		if cp.Index <= from || cp.Identity != v.Lineage {
			continue
		}
		v.Checkpoints = append(v.Checkpoints, exportCheckpoint(cp))
	}
	return v, nil
}

func exportCheckpoint(cp reftable.CheckpointRecord) clusterhook.ExportCheckpoint {
	return clusterhook.ExportCheckpoint{
		Name: cp.Name(), Lineage: cp.Identity, Index: cp.Index, Term: cp.Term, ClosedAt: cp.ClosedAt, Root: cp.Root,
		Time: cp.Time, LSN: cp.LSN, Timeline: cp.Timeline, SystemID: cp.SystemID, RestorePoint: cp.RestorePoint,
		WALSegmentSize: cp.WALSegmentSize, RefsSHA256: cp.RefsSHA256, FilesSHA256: cp.FilesSHA256,
	}
}

// exportLog streams the committed entries after (FromIndex, FromTerm), up
// to the commit the leader confirmed, one JSON object per line. The entry
// at FromIndex must carry FromTerm: otherwise the observer's history and
// the cluster's diverge, which it must hear rather than be handed a tail.
func (r *Replicator) exportLog(ctx context.Context, eng consensus.Engine, req *clusterhook.ExportRequest) (streamFunc, error) {
	if err := r.leaderOnly(eng); err != nil {
		return nil, err
	}
	reader, ok := eng.(consensus.CommittedReader)
	if !ok {
		return nil, exportErr(clusterhook.CodeUnsupported, "this engine does not read its log back; follow the checkpoints")
	}
	if lin := eng.Status().Identity.String(); req.Lineage != lin {
		return nil, exportErr(clusterhook.CodeLineage, "the cluster's lineage is %s, not %s", lin, req.Lineage)
	}
	tok, err := confirmedCommit(ctx, eng)
	if err != nil {
		return nil, err
	}
	if req.FromIndex > tok.Index {
		return nil, exportErr(clusterhook.CodeDiverged, "the observer is at %d, past the commit %d", req.FromIndex, tok.Index)
	}
	start := req.FromIndex + 1
	if req.FromIndex > 0 {
		start = req.FromIndex
	}
	entries, err := reader.ReadCommitted(ctx, start, tok.Index, logBatchBytes)
	if errors.Is(err, consensus.ErrCompacted) {
		return nil, exportErr(clusterhook.CodeCompacted, "the log before %d is compacted; start again from a checkpoint", start)
	}
	if err != nil {
		return nil, err
	}
	if req.FromIndex > 0 {
		if len(entries) == 0 || entries[0].Index != req.FromIndex || entries[0].Term != req.FromTerm {
			return nil, exportErr(clusterhook.CodeDiverged, "the entry at %d does not carry term %d", req.FromIndex, req.FromTerm)
		}
		entries = entries[1:]
	}
	return func(_ context.Context, w io.Writer) error {
		enc := json.NewEncoder(w)
		for _, e := range entries {
			if err := enc.Encode(clusterhook.ExportEntry{Index: e.Index, Term: e.Term, Data: e.Data}); err != nil {
				return err
			}
		}
		return nil
	}, nil
}

// exportPacks streams a pack of wants minus haves. The wants must be ref
// values of the repository: at the checkpoint named when this member holds
// its artifacts, in the table otherwise.
func (r *Replicator) exportPacks(ctx context.Context, req *clusterhook.ExportRequest) (streamFunc, error) {
	if err := reftable.ValidRepoKey(req.Repo); err != nil {
		return nil, err
	}
	for _, s := range append(append([]string(nil), req.Wants...), req.Haves...) {
		if !isHex(s) {
			return nil, errors.New("invalid object id")
		}
	}
	if len(req.Wants) == 0 {
		return nil, errors.New("no wants")
	}
	named := map[string]bool{}
	if t, err := r.export.refsAt(req.Checkpoint); err == nil {
		refs, _ := t.Refs(req.Repo)
		for _, v := range refs {
			named[v.SHA] = true
		}
	} else {
		refs, _ := r.table.Refs(req.Repo)
		for _, v := range refs {
			named[v.SHA] = true
		}
		for _, s := range r.table.PinnedObjects(req.Repo) {
			named[s] = true
		}
	}
	for _, s := range req.Wants {
		if !named[s] {
			return nil, fmt.Errorf("%s is not a ref value of %s at the checkpoint", s, req.Repo)
		}
	}
	dir := filepath.Join(r.root, filepath.FromSlash(req.Repo))
	if missing, err := missingObjects(ctx, dir, req.Wants); err != nil || len(missing) > 0 {
		return nil, exportErr(clusterhook.CodeUnavailable, "objects not held here")
	}
	haves, _ := presentObjects(ctx, dir, req.Haves)
	return func(ctx context.Context, w io.Writer) error {
		return packObjects(ctx, dir, req.Wants, haves, w)
	}, nil
}

// ---- the artifacts of a checkpoint ----

func (x exportSide) artifact(name, file string) (streamFunc, error) {
	if !checkpointNamePattern.MatchString(name) {
		return nil, errors.New("invalid checkpoint name")
	}
	f, err := os.Open(filepath.Join(x.ckptDir, name, file))
	if err != nil {
		return nil, exportErr(clusterhook.CodeUnavailable, "the artifacts of %s are not kept here", name)
	}
	return func(_ context.Context, w io.Writer) error {
		defer f.Close()
		_, err := io.Copy(w, f)
		return err
	}, nil
}

// refsAt reads the table of a checkpoint whose artifacts this member keeps.
func (x exportSide) refsAt(name string) (*reftable.Table, error) {
	if !checkpointNamePattern.MatchString(name) {
		return nil, errors.New("invalid checkpoint name")
	}
	f, err := os.Open(filepath.Join(x.ckptDir, name, "refs.snapshot"))
	if err != nil {
		return nil, err
	}
	defer f.Close()
	t := reftable.New()
	return t, t.Restore(f, consensus.SnapshotMeta{})
}

// ---- PostgreSQL ----

// checkDedicated refuses a PostgreSQL cluster that holds another database
// than Forgejo's, or a tablespace a single tar cannot carry.
func (x exportSide) checkDedicated(ctx context.Context) error {
	dbs, spcs, err := x.wal.Foreign(ctx)
	if err != nil {
		return err
	}
	if len(dbs) > 0 {
		return exportErr(clusterhook.CodeUnsupported, "PostgreSQL holds other databases than Forgejo's (%s): an observer copies a dedicated cluster only", strings.Join(dbs, ", "))
	}
	if len(spcs) > 0 {
		return exportErr(clusterhook.CodeUnsupported, "PostgreSQL has tablespaces besides pg_default (%s): a base backup to stdout carries one", strings.Join(spcs, ", "))
	}
	return nil
}

func (x exportSide) pgBase(ctx context.Context) (streamFunc, error) {
	if err := x.checkDedicated(ctx); err != nil {
		return nil, err
	}
	return x.baseBackup, nil
}

// pgBaseBackup runs pg_basebackup to stdout: one tar, the WAL left to the
// archive (-X none), so a checkpoint needs the archive from the backup's
// start to its position, which the observer checks.
func pgBaseBackup(ctx context.Context, w io.Writer) error {
	if setting.Cluster.PGBaseBackupDSN == "" {
		return errors.New("[cluster] PG_BASEBACKUP_DSN is empty")
	}
	var stderr bytes.Buffer
	cmd := exec.CommandContext(ctx, setting.Cluster.PGBaseBackup, "-d", setting.Cluster.PGBaseBackupDSN, "-D", "-", "-Ft", "-X", "none", "--checkpoint=fast")
	cmd.Stdout, cmd.Stderr = w, &stderr
	if err := cmd.Run(); err != nil {
		return fmt.Errorf("pg_basebackup: %w: %s", err, strings.TrimSpace(stderr.String()))
	}
	return nil
}

// walNamePattern accepts what archive_command archives: segments, timeline
// histories and backup history files.
var walNamePattern = regexp.MustCompile(`^[0-9A-F]{24}(\.[0-9A-F]{8}\.backup)?$|^[0-9A-F]{8}\.history$`)

// ValidWALName reports whether name is a file of the WAL archive.
func ValidWALName(name string) bool { return walNamePattern.MatchString(name) }

func (x exportSide) walList(after string) ([]clusterhook.WALFile, error) {
	if x.archiveDir == "" {
		return nil, exportErr(clusterhook.CodeUnsupported, "[cluster] WAL_ARCHIVE_DIR is empty")
	}
	entries, err := os.ReadDir(x.archiveDir)
	if err != nil {
		return nil, err
	}
	var out []clusterhook.WALFile
	for _, e := range entries {
		// Timeline histories are few and always listed: an observer needs
		// each one on the path to its checkpoints.
		if !e.Type().IsRegular() || !ValidWALName(e.Name()) || e.Name() <= after && !strings.HasSuffix(e.Name(), ".history") {
			continue
		}
		info, err := e.Info()
		if err != nil {
			continue
		}
		out = append(out, clusterhook.WALFile{Name: e.Name(), Size: info.Size()})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	if len(out) > walListMax {
		out = out[:walListMax]
	}
	return out, nil
}

func (x exportSide) walFile(name string) (streamFunc, error) {
	if x.archiveDir == "" || !ValidWALName(name) {
		return nil, errors.New("invalid WAL file name")
	}
	f, err := os.Open(filepath.Join(x.archiveDir, name))
	if err != nil {
		return nil, exportErr(clusterhook.CodeUnavailable, "%s is not in this member's archive", name)
	}
	return func(_ context.Context, w io.Writer) error {
		defer f.Close()
		_, err := io.Copy(w, f)
		return err
	}, nil
}

// ---- stored files and secrets ----

func (x exportSide) storedFile(storage, name string) (streamFunc, error) {
	roots, err := x.storages()
	if err != nil {
		return nil, exportErr(clusterhook.CodeUnsupported, "%v", err)
	}
	root, ok := roots[storage]
	clean := path.Clean("/" + name)[1:]
	if !ok || clean == "" || clean != name {
		return nil, errors.New("invalid stored file")
	}
	full := filepath.Join(root, filepath.FromSlash(clean))
	info, err := os.Lstat(full)
	if err != nil || !info.Mode().IsRegular() {
		return nil, exportErr(clusterhook.CodeUnavailable, "%s/%s is not held here", storage, name)
	}
	f, err := os.Open(full)
	if err != nil {
		return nil, err
	}
	return func(_ context.Context, w io.Writer) error {
		defer f.Close()
		_, err := io.Copy(w, f)
		return err
	}, nil
}

// SecretsBundle is what the secrets op seals to an observer: the key the
// database's encrypted columns need. Every other secret is rotated by a
// Recover, never restored.
type SecretsBundle struct {
	SecretKey string `json:"secret_key"`
	Time      int64  `json:"time"`
}

// secrets seals the bundle to the observer's X25519 key, named by the
// forced command, never by the observer itself.
func (x exportSide) secrets(observer string) (streamFunc, error) {
	key, ok := x.recipients[observer]
	if !ok || observer == "" {
		return nil, exportErr(clusterhook.CodeUnsupported, "no OBSERVER_RECIPIENTS key for observer %q", observer)
	}
	raw, err := hex.DecodeString(key)
	if err != nil || len(raw) != 32 {
		return nil, errors.New("invalid recipient key")
	}
	b, err := json.Marshal(SecretsBundle{SecretKey: x.secretKey(), Time: time.Now().Unix()})
	if err != nil {
		return nil, err
	}
	sealed, err := box.SealAnonymous(nil, b, (*[32]byte)(raw), rand.Reader)
	if err != nil {
		return nil, err
	}
	return func(_ context.Context, w io.Writer) error {
		_, err := w.Write(sealed)
		return err
	}, nil
}
