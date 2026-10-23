// Copyright 2026 The Forgejo Authors. All rights reserved.
// SPDX-License-Identifier: GPL-3.0-or-later

package clusterhook

import (
	"bufio"
	"context"
	"encoding/binary"
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
	// Framed asks the member's node to frame a stream, so that a stream
	// cut short by an error is told from its end (#111). Only the
	// member's own export command sets it, on its socket.
	Framed bool `json:"framed,omitempty"`
}

// ExportAnswer heads every export answer; when Stream is set, the raw bytes
// of the answer follow it on the connection until its end.
type ExportAnswer struct {
	OK      bool    `json:"ok"`
	Code    string  `json:"code,omitempty"`
	Message string  `json:"message,omitempty"`
	Leader  string  `json:"leader,omitempty"`
	Term    uint64  `json:"term,omitempty"`
	Stream  bool    `json:"stream,omitempty"`
	Data    RawJSON `json:"data,omitempty"`
	// Framed says the stream that follows is framed: chunks, then an end
	// or an error. A node that predates framing never sets it.
	Framed bool `json:"framed,omitempty"`
}

// ExportError is a refused export.
type ExportError struct {
	Code    string
	Message string
	// Leader is the slot of the leader, as the member knows it: a hint
	// the observer follows only to an endpoint it pins. Term is the
	// member's: a view of an older term comes from a deposed leader.
	Leader string
	Term   uint64
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
	Lineage string   `json:"lineage"`
	Term    uint64   `json:"term"`
	Commit  uint64   `json:"commit"`
	Leader  string   `json:"leader"`
	Members []string `json:"members"`
	// Voters are the node ids of the voting members, as ParseNodeID reads
	// them: a recovery from an observer's checkpoint needs the witnesses'
	// receipts fencing each of them.
	Voters       []string           `json:"voters,omitempty"`
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
	// SHA256 is a timeline history's digest: a history name held with
	// other bytes is a timeline reused by another history, which the
	// observer refuses. Segments do not carry it, hashing them on every
	// list would read the whole archive.
	SHA256 string `json:"sha256,omitempty"`
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

// MaxExportHead bounds the head line of an export's answer, which a member
// writes and an observer reads before anything else: a view, a WAL list.
const MaxExportHead = 64 << 20

// ErrExportHead is a head line past MaxExportHead.
var ErrExportHead = errors.New("the export's answer head is larger than its bound")

// ReadExportHead reads an answer's head line, never more than max bytes
// of it: a member that writes without end costs the reader that much at
// most (#107).
func ReadExportHead(br *bufio.Reader, max int) ([]byte, error) {
	var line []byte
	for {
		chunk, err := br.ReadSlice('\n')
		if len(line)+len(chunk) > max {
			return nil, ErrExportHead
		}
		line = append(line, chunk...)
		if errors.Is(err, bufio.ErrBufferFull) {
			continue
		}
		return line, err
	}
}

// A framed stream is a sequence of chunks, each a big-endian length then
// its bytes, ended by a zero length (the end) or by frameError then the
// error's message up to a newline: a stream cut otherwise is truncated.
const frameError = 0xffffffff

// maxFrameMessage bounds an error's message in a framed stream.
const maxFrameMessage = 64 << 10

// FrameWriter frames a stream's bytes; End or Fail closes it.
type FrameWriter struct{ W io.Writer }

func (f FrameWriter) Write(p []byte) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}
	var head [4]byte
	binary.BigEndian.PutUint32(head[:], uint32(len(p)))
	if _, err := f.W.Write(head[:]); err != nil {
		return 0, err
	}
	return f.W.Write(p)
}

// End marks the stream complete.
func (f FrameWriter) End() error {
	_, err := f.W.Write([]byte{0, 0, 0, 0})
	return err
}

// Fail marks the stream failed with err.
func (f FrameWriter) Fail(err error) error {
	msg := strings.ReplaceAll(err.Error(), "\n", " ")
	if len(msg) > maxFrameMessage-1 {
		msg = msg[:maxFrameMessage-1]
	}
	var head [4]byte
	binary.BigEndian.PutUint32(head[:], frameError)
	_, werr := f.W.Write(append(head[:], msg+"\n"...))
	return werr
}

// ErrStreamTruncated is a framed stream that ended without its end mark.
var ErrStreamTruncated = errors.New("the export's stream was cut short")

// frameReader reads a framed stream's bytes: io.EOF at its end mark, the
// node's error at an error mark, ErrStreamTruncated otherwise.
type frameReader struct {
	r    *bufio.Reader
	left uint32
	err  error
}

func (f *frameReader) Read(p []byte) (int, error) {
	for f.left == 0 {
		if f.err != nil {
			return 0, f.err
		}
		var head [4]byte
		if _, err := io.ReadFull(f.r, head[:]); err != nil {
			f.err = fmt.Errorf("%w: %w", ErrStreamTruncated, err)
			continue
		}
		switch n := binary.BigEndian.Uint32(head[:]); n {
		case 0:
			f.err = io.EOF
		case frameError:
			msg, err := ReadExportHead(f.r, maxFrameMessage)
			if err != nil && !errors.Is(err, io.EOF) {
				f.err = fmt.Errorf("%w: %w", ErrStreamTruncated, err)
				continue
			}
			f.err = fmt.Errorf("the export's stream failed on the member: %s", strings.TrimSpace(string(msg)))
		default:
			f.left = n
		}
	}
	if uint32(len(p)) > f.left {
		p = p[:f.left]
	}
	n, err := f.r.Read(p)
	f.left -= uint32(n)
	if errors.Is(err, io.EOF) {
		if f.left > 0 {
			f.err = fmt.Errorf("%w: %w", ErrStreamTruncated, io.ErrUnexpectedEOF)
			return n, f.err
		}
		err = nil
	}
	return n, err
}

// CallExport sends an export request on the replicator's socket and
// returns its answer and the body to read the stream from, empty unless
// the answer is a stream; the caller closes it. It asks for a framed
// stream: the body it returns holds the raw bytes, and fails when the node
// failed or the stream was cut short.
func CallExport(ctx context.Context, socket string, req *ExportRequest) (*ExportAnswer, io.ReadCloser, error) {
	framed := *req
	framed.Framed = true
	req = &framed
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
	line, err := ReadExportHead(br, MaxExportHead)
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
	if ans.Framed {
		ans.Framed = false
		return &ans, readCloser{Reader: &frameReader{r: br}, Closer: conn}, nil
	}
	return &ans, readCloser{Reader: br, Closer: conn}, nil
}

type readCloser struct {
	io.Reader
	io.Closer
}
