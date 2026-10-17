// Copyright 2026 The Forgejo Authors. All rights reserved.
// SPDX-License-Identifier: GPL-3.0-or-later

package observer

import (
	"compress/gzip"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"sort"
	"strings"
	"time"

	"forgejo.org/modules/json"

	"forgejo.org/modules/clusterhook"
)

// The observer's directory, which the snapshot hook covers whole:
//
//	state.json                 what was accepted, and the follower's cursor
//	STATUS.json, ALERTS        freshness and alarms, for the host's monitoring
//	staging/<checkpoint>/      a checkpoint being verified, discarded on a crash
//	checkpoints/<checkpoint>/  published atomically: manifest.json, refs.snapshot,
//	                           files.json, secrets.box, PROTECTED once the hook ran
//	git/<owner>/<repo>.git     objects, and refs/observer/<checkpoint>/ per checkpoint
//	blobs/<ab>/<sha256>        stored files, by digest
//	pg/base/<label>/           base backups, as pg_basebackup wrote them
//	pg/wal/                    the WAL archive, segments gzipped, histories as is
//	log/<lineage>.jsonl        the committed log, when the engine reads it back
//	quarantine/                what a fork, a rollback or an unknown lineage left
//
// Nothing accepted is ever rewritten: files are created once, under a
// temporary name then renamed, and a name that exists is compared, never
// replaced.

// The states of a checkpoint.
const (
	StateVerified  = "VERIFIED"
	StateProtected = "PROTECTED"
)

// Manifest is a published checkpoint: everything that restores it.
type Manifest struct {
	Format     int                          `json:"format"`
	Checkpoint clusterhook.ExportCheckpoint `json:"checkpoint"`
	Repos      map[string]RepoManifest      `json:"repos"`
	// Deleted are the repositories the table holds deleted at the cut.
	Deleted  []string    `json:"deleted,omitempty"`
	Files    []FileBlob  `json:"files"`
	PG       Coverage    `json:"pg"`
	Verified time.Time   `json:"verified"`
	Source   string      `json:"source"`
	Loss     string      `json:"loss"`
	Previous string      `json:"previous,omitempty"`
	Sizes    ManifestSum `json:"sizes"`
}

// ManifestSum totals what a checkpoint references.
type ManifestSum struct {
	Repos int   `json:"repos"`
	Refs  int   `json:"refs"`
	Files int   `json:"files"`
	Bytes int64 `json:"bytes"`
}

// RepoManifest is one repository at a checkpoint: its refs (also under the
// checkpoint's namespace in the git store), its symbolic refs, and the refs
// deleted, kept as tombstones.
type RepoManifest struct {
	Format     string            `json:"format"`
	Refs       map[string]string `json:"refs"`
	Symbolic   map[string]string `json:"symbolic,omitempty"`
	Tombstones []string          `json:"tombstones,omitempty"`
}

// FileBlob is a stored file of a checkpoint and the digest it is kept by.
type FileBlob struct {
	clusterhook.FileEntry
	SHA256 string `json:"sha256"`
}

// Protection records the snapshot hook's success for a checkpoint.
type Protection struct {
	Checkpoint string    `json:"checkpoint"`
	Snapshot   string    `json:"snapshot"`
	At         time.Time `json:"at"`
}

// Accepted is a checkpoint in the state.
type Accepted struct {
	Name     string    `json:"name"`
	Index    uint64    `json:"index"`
	Term     uint64    `json:"term"`
	Root     string    `json:"root"`
	Time     int64     `json:"time"`
	State    string    `json:"state"`
	Snapshot string    `json:"snapshot,omitempty"`
	Verified time.Time `json:"verified"`
	// Digest is the SHA-256 of the checkpoint as the leader described it:
	// the same name later described otherwise is a fork.
	Digest string `json:"digest"`
}

// Quarantine stops ingestion until a human acts.
type Quarantine struct {
	Reason  string    `json:"reason"`
	Lineage string    `json:"lineage,omitempty"`
	At      time.Time `json:"at"`
}

// State is state.json.
type State struct {
	Lineage     string      `json:"lineage"`
	Term        uint64      `json:"term"`
	Commit      uint64      `json:"commit"`
	Leader      string      `json:"leader"`
	LogIndex    uint64      `json:"log_index"`
	LogTerm     uint64      `json:"log_term"`
	Checkpoints []Accepted  `json:"checkpoints"`
	Bases       []BaseInfo  `json:"bases"`
	Quarantine  *Quarantine `json:"quarantine,omitempty"`
	HookFailing string      `json:"hook_failing,omitempty"`
	// Rebootstrap is set while the observer waits for a new base backup or
	// a checkpoint to restart the log from after a compaction.
	Rebootstrap string `json:"rebootstrap,omitempty"`
}

// newest returns the newest accepted checkpoint.
func (s *State) newest() (Accepted, bool) {
	if len(s.Checkpoints) == 0 {
		return Accepted{}, false
	}
	return s.Checkpoints[len(s.Checkpoints)-1], true
}

func (s *State) accepted(name string) (int, bool) {
	for i, a := range s.Checkpoints {
		if a.Name == name {
			return i, true
		}
	}
	return -1, false
}

// LastProtected returns the newest protected checkpoint.
func (s *State) LastProtected() (Accepted, bool) {
	for _, a := range slices.Backward(s.Checkpoints) {
		if a.State == StateProtected {
			return a, true
		}
	}
	return Accepted{}, false
}

// store is the observer's directory.
type store struct {
	dir string
	git gitStore
}

func newStore(dir, git string) *store {
	return &store{dir: dir, git: gitStore{root: filepath.Join(dir, "git"), git: git}}
}

func (s *store) path(elem ...string) string {
	return filepath.Join(append([]string{s.dir}, elem...)...)
}

func (s *store) init() error {
	for _, d := range []string{"staging", "checkpoints", "git", "blobs", "pg/base", "pg/wal", "pg/staging", "log", "quarantine"} {
		if err := os.MkdirAll(s.path(d), 0o700); err != nil {
			return err
		}
	}
	// What a crash left in staging was never published: it goes.
	for _, d := range []string{"staging", "pg/staging"} {
		entries, err := os.ReadDir(s.path(d))
		if err != nil {
			return err
		}
		for _, e := range entries {
			if err := os.RemoveAll(s.path(d, e.Name())); err != nil {
				return err
			}
		}
	}
	return nil
}

func (s *store) loadState() (*State, error) {
	b, err := os.ReadFile(s.path("state.json"))
	if errors.Is(err, os.ErrNotExist) {
		return &State{}, nil
	}
	if err != nil {
		return nil, err
	}
	var st State
	if err := json.Unmarshal(b, &st); err != nil {
		return nil, fmt.Errorf("state.json: %w", err)
	}
	return &st, nil
}

func (s *store) saveState(st *State) error {
	b, err := json.MarshalIndent(st, "", "  ")
	if err != nil {
		return err
	}
	return writeAtomic(s.path("state.json"), b, true)
}

// writeAtomic writes a file durably under a temporary name, then renames
// it; with replace false, an existing file is never replaced.
func writeAtomic(path string, b []byte, replace bool) error {
	if !replace {
		if _, err := os.Stat(path); err == nil {
			return fmt.Errorf("%s exists", path)
		}
	}
	tmp, err := os.CreateTemp(filepath.Dir(path), "."+filepath.Base(path)+".*")
	if err != nil {
		return err
	}
	defer os.Remove(tmp.Name())
	_, err = tmp.Write(b)
	if err == nil {
		err = tmp.Sync()
	}
	if cerr := tmp.Close(); err == nil {
		err = cerr
	}
	if err != nil {
		return err
	}
	if !replace {
		if err := os.Link(tmp.Name(), path); err != nil {
			return err
		}
	} else if err := os.Rename(tmp.Name(), path); err != nil {
		return err
	}
	return syncDir(filepath.Dir(path))
}

func syncDir(dir string) error {
	f, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer f.Close()
	return f.Sync()
}

// writeStream copies r durably into path, never replacing it, bounded by
// limit bytes, and returns its size and SHA-256.
func writeStream(path string, r io.Reader, limit int64, gz bool) (int64, string, error) {
	if _, err := os.Stat(path); err == nil {
		return 0, "", fmt.Errorf("%s exists", path)
	}
	tmp, err := os.CreateTemp(filepath.Dir(path), "."+filepath.Base(path)+".*")
	if err != nil {
		return 0, "", err
	}
	defer os.Remove(tmp.Name())
	h := sha256.New()
	var w io.Writer = tmp
	var zw *gzip.Writer
	if gz {
		zw = gzip.NewWriter(tmp)
		w = zw
	}
	n, err := io.Copy(io.MultiWriter(w, h), io.LimitReader(r, limit+1))
	if err == nil && n > limit {
		err = fmt.Errorf("larger than %d bytes", limit)
	}
	if err == nil && zw != nil {
		err = zw.Close()
	}
	if err == nil {
		err = tmp.Sync()
	}
	if cerr := tmp.Close(); err == nil {
		err = cerr
	}
	if err == nil {
		err = os.Link(tmp.Name(), path)
	}
	if err == nil {
		err = syncDir(filepath.Dir(path))
	}
	if err != nil {
		return 0, "", err
	}
	return n, hex.EncodeToString(h.Sum(nil)), nil
}

// ---- WAL ----

func (s *store) walPath(name string) string {
	if strings.HasSuffix(name, ".history") {
		return s.path("pg", "wal", name)
	}
	return s.path("pg", "wal", name+".gz")
}

func (s *store) haveWAL(name string) bool {
	_, err := os.Stat(s.walPath(name))
	return err == nil
}

// OpenWAL opens a held WAL file, decompressed.
func (s *store) OpenWAL(name string) (io.ReadCloser, error) {
	f, err := os.Open(s.walPath(name))
	if err != nil || strings.HasSuffix(name, ".history") {
		return f, err
	}
	zr, err := gzip.NewReader(f)
	if err != nil {
		f.Close()
		return nil, err
	}
	return readCloser{Reader: zr, Closer: f}, nil
}

type readCloser struct {
	io.Reader
	io.Closer
}

func (s *store) history(tli uint32) ([]Switch, error) {
	f, err := os.Open(s.walPath(HistoryName(tli)))
	if err != nil {
		return nil, err
	}
	defer f.Close()
	return ParseHistory(f)
}

// abandoned lists where each timeline was left, from every history held.
func (s *store) abandoned() []Switch {
	entries, _ := os.ReadDir(s.path("pg", "wal"))
	var out []Switch
	for _, e := range entries {
		if !strings.HasSuffix(e.Name(), ".history") {
			continue
		}
		f, err := os.Open(s.path("pg", "wal", e.Name()))
		if err != nil {
			continue
		}
		h, err := ParseHistory(f)
		f.Close()
		if err == nil {
			out = append(out, h...)
		}
	}
	return out
}

// lastSegment is the greatest segment name held, the export's cursor.
func (s *store) lastSegment() string {
	entries, _ := os.ReadDir(s.path("pg", "wal"))
	last := ""
	for _, e := range entries {
		if name, ok := strings.CutSuffix(e.Name(), ".gz"); ok && segmentPattern.MatchString(name) && name > last {
			last = name
		}
	}
	return last
}

// ---- blobs ----

func (s *store) blobPath(sum string) string { return s.path("blobs", sum[:2], sum) }

func (s *store) haveBlob(sum string) bool {
	_, err := os.Stat(s.blobPath(sum))
	return err == nil
}

// ---- checkpoints ----

var checkpointName = regexp.MustCompile(`^[0-9a-f]{32}-[0-9]+-[0-9]{20}$`)

// ReadManifest reads a published checkpoint's manifest.
func (s *store) ReadManifest(name string) (*Manifest, error) {
	if !checkpointName.MatchString(name) {
		return nil, fmt.Errorf("invalid checkpoint name %q", name)
	}
	b, err := os.ReadFile(s.path("checkpoints", name, "manifest.json"))
	if err != nil {
		return nil, err
	}
	var m Manifest
	if err := json.Unmarshal(b, &m); err != nil {
		return nil, err
	}
	return &m, nil
}

// Protection reads a checkpoint's protection record, if the hook ran.
func (s *store) Protection(name string) (*Protection, bool) {
	b, err := os.ReadFile(s.path("checkpoints", name, "PROTECTED"))
	if err != nil {
		return nil, false
	}
	var p Protection
	if json.Unmarshal(b, &p) != nil {
		return nil, false
	}
	return &p, true
}

// publish moves a verified checkpoint from staging into checkpoints/, in
// one rename: it is there whole, or not at all.
func (s *store) publish(name string) error {
	dst := s.path("checkpoints", name)
	if _, err := os.Stat(dst); err == nil {
		return fmt.Errorf("%w: %s is already published", ErrImmutable, name)
	}
	if err := os.Rename(s.path("staging", name), dst); err != nil {
		return err
	}
	return syncDir(s.path("checkpoints"))
}

// stagingSize is what staging holds, against the quota.
func (s *store) stagingSize() int64 {
	var n int64
	for _, d := range []string{"staging", "pg/staging"} {
		_ = filepath.Walk(s.path(d), func(_ string, info os.FileInfo, err error) error {
			if err == nil && info.Mode().IsRegular() {
				n += info.Size()
			}
			return nil
		})
	}
	return n
}

// quarantine keeps what a refused answer carried, for the human who acts.
func (s *store) quarantine(what string, b []byte) {
	name := time.Now().UTC().Format("20060102T150405.000000000") + "-" + what
	_ = writeAtomic(s.path("quarantine", name), b, false)
}

// Published lists the published checkpoints, oldest first.
func (s *store) Published() []string {
	entries, _ := os.ReadDir(s.path("checkpoints"))
	var out []string
	for _, e := range entries {
		if e.IsDir() && checkpointName.MatchString(e.Name()) {
			out = append(out, e.Name())
		}
	}
	sort.Strings(out)
	return out
}
