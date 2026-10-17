// Copyright 2026 The Forgejo Authors. All rights reserved.
// SPDX-License-Identifier: GPL-3.0-or-later

package clusterhook

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"slices"
	"strings"

	"forgejo.org/modules/json"
)

// The export ops, read-only, which `forgejo cluster export` forwards for an
// observer: its SSH forced command. Every other op is refused there. The
// log, the checkpoints and their artifacts are served by the leader only;
// packs, WAL and files by any member, checked by digest on the observer.
const (
	OpExportCheckpoints = "export-checkpoints"
	OpExportLog         = "export-log"
	OpExportRefs        = "export-refs"
	OpExportPacks       = "export-packs"
	OpExportPGBase      = "export-pgbase"
	OpExportWALList     = "export-wal-list"
	OpExportWAL         = "export-wal"
	OpExportFiles       = "export-files"
	OpExportFile        = "export-file"
	OpExportSecrets     = "export-secrets"
)

// ExportOps lists the export ops.
var ExportOps = []string{
	OpExportCheckpoints, OpExportLog, OpExportRefs, OpExportPacks, OpExportPGBase,
	OpExportWALList, OpExportWAL, OpExportFiles, OpExportFile, OpExportSecrets,
}

// IsExportOp reports whether op is one of the export ops.
func IsExportOp(op string) bool {
	return slices.Contains(ExportOps, op)
}

// The codes an export answers with besides a plain refusal.
const (
	CodeNotLeader   = "NOT_LEADER"
	CodeCompacted   = "LOG_COMPACTED"
	CodeDiverged    = "LOG_DIVERGED"
	CodeLineage     = "LINEAGE_MISMATCH"
	CodeUnsupported = "UNSUPPORTED"
	CodeUnavailable = "UNAVAILABLE"
)

// ExportRequest is an export call. Observer is set by the forced command
// from its own argument, never from what the observer sent.
type ExportRequest struct {
	Op         string   `json:"op"`
	Observer   string   `json:"observer,omitempty"`
	Checkpoint string   `json:"checkpoint,omitempty"`
	Lineage    string   `json:"lineage,omitempty"`
	FromIndex  uint64   `json:"from_index,omitempty"`
	FromTerm   uint64   `json:"from_term,omitempty"`
	Repo       string   `json:"repo,omitempty"`
	Wants      []string `json:"wants,omitempty"`
	Haves      []string `json:"haves,omitempty"`
	Name       string   `json:"name,omitempty"`
	Storage    string   `json:"storage,omitempty"`
}

// ExportAnswer heads every export answer; when Stream is set, the raw bytes
// of the answer follow it on the connection until its end.
type ExportAnswer struct {
	OK      bool    `json:"ok"`
	Code    string  `json:"code,omitempty"`
	Message string  `json:"message,omitempty"`
	Leader  string  `json:"leader,omitempty"`
	Stream  bool    `json:"stream,omitempty"`
	Data    RawJSON `json:"data,omitempty"`
}

// ExportError is a refused export.
type ExportError struct {
	Code    string
	Message string
	// Leader is the slot of the leader, as the member knows it: a hint
	// the observer follows only to an endpoint it pins.
	Leader string
}

func (e *ExportError) Error() string {
	if e.Code == "" {
		return e.Message
	}
	return e.Code + ": " + e.Message
}

// ExportCode returns the code of an export refusal, empty for any other
// error.
func ExportCode(err error) string {
	if e, ok := errors.AsType[*ExportError](err); ok {
		return e.Code
	}
	return ""
}

// ExportCheckpoint is a closed checkpoint as the export describes it.
type ExportCheckpoint struct {
	Name           string `json:"name"`
	Lineage        string `json:"lineage"`
	Index          uint64 `json:"index"`
	Term           uint64 `json:"term"`
	ClosedAt       uint64 `json:"closed"`
	Root           string `json:"root"`
	Time           int64  `json:"time"`
	LSN            string `json:"lsn"`
	Timeline       uint32 `json:"timeline"`
	SystemID       string `json:"sysid"`
	RestorePoint   string `json:"restore_point"`
	WALSegmentSize int64  `json:"wal_segment_size"`
	RefsSHA256     string `json:"refs_sha256"`
	FilesSHA256    string `json:"files_sha256"`
}

// ExportView answers export-checkpoints: the leader's commit, confirmed by
// a quorum, and the checkpoints closed after FromIndex.
type ExportView struct {
	Lineage      string             `json:"lineage"`
	Term         uint64             `json:"term"`
	Commit       uint64             `json:"commit"`
	Leader       string             `json:"leader"`
	Members      []string           `json:"members"`
	LogSupported bool               `json:"log_supported"`
	Checkpoints  []ExportCheckpoint `json:"checkpoints"`
}

// ExportEntry is a committed entry of export-log, one JSON object per line.
type ExportEntry struct {
	Index uint64 `json:"i"`
	Term  uint64 `json:"t"`
	Data  []byte `json:"d"`
}

// WALFile is one file of the WAL archive.
type WALFile struct {
	Name string `json:"name"`
	Size int64  `json:"size"`
}

// FileEntry is one stored file a checkpoint needs: an LFS object, an
// attachment or a package blob, by its path under its storage.
type FileEntry struct {
	Storage string `json:"storage"`
	Path    string `json:"path"`
	Size    int64  `json:"size"`
}

// FilesManifest lists the files present at a checkpoint's fence.
type FilesManifest struct {
	Checkpoint string      `json:"checkpoint"`
	Files      []FileEntry `json:"files"`
}

// MaxExportRequest bounds the request the forced command reads from the
// observer: the wants of one repository fit it.
const MaxExportRequest = 16 << 20

// CallExport sends an export request on the replicator's socket and
// returns its answer and the body to read the stream from, empty unless
// the answer is a stream; the caller closes it.
func CallExport(ctx context.Context, socket string, req *ExportRequest) (*ExportAnswer, io.ReadCloser, error) {
	d := net.Dialer{}
	conn, err := d.DialContext(ctx, "unix", socket)
	if err != nil {
		return nil, nil, fmt.Errorf("cluster replicator unreachable on %s: %w", socket, err)
	}
	if dl, ok := ctx.Deadline(); ok {
		_ = conn.SetDeadline(dl)
	}
	if err := json.NewEncoder(conn).Encode(&Request{Op: req.Op, Export: req}); err != nil {
		conn.Close()
		return nil, nil, err
	}
	br := bufio.NewReader(conn)
	line, err := br.ReadBytes('\n')
	if err != nil {
		conn.Close()
		return nil, nil, fmt.Errorf("cluster replicator answer: %w", err)
	}
	var ans ExportAnswer
	if err := json.Unmarshal(line, &ans); err != nil {
		conn.Close()
		return nil, nil, fmt.Errorf("cluster replicator answer: %w", err)
	}
	if !ans.Stream {
		conn.Close()
		return &ans, io.NopCloser(strings.NewReader("")), nil
	}
	return &ans, readCloser{Reader: br, Closer: conn}, nil
}

type readCloser struct {
	io.Reader
	io.Closer
}
