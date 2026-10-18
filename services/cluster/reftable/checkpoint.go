// Copyright 2026 The Forgejo Authors. All rights reserved.
// SPDX-License-Identifier: GPL-3.0-or-later

package reftable

import (
	"encoding/hex"
	"fmt"
	"regexp"

	"forgejo.org/services/cluster/consensus"
)

// A checkpoint pairs the table at one index with PostgreSQL at one WAL
// position, so an observer can rebuild both at the same cut. It takes two
// entries: a barrier, whose Apply records the table's position and root,
// then, once the leader drained the SQL of every entry up to it and read
// PostgreSQL's position under that fence, the position itself. Neither
// changes a ref.

// CheckpointBarrier opens a checkpoint. Nonce matches the position entry
// that closes it; Time is the proposer's clock, carried for display.
type CheckpointBarrier struct {
	Nonce string `json:"nonce"`
	Time  int64  `json:"t"`
}

// CheckpointPosition closes the barrier at Index with what the leader read
// under its fence: PostgreSQL's flushed WAL position, timeline, system
// identifier and restore point, and the digests of the artifacts it wrote
// for that cut (the table's snapshot at the barrier, the files' manifest).
type CheckpointPosition struct {
	Nonce          string `json:"nonce"`
	Index          uint64 `json:"i"`
	LSN            string `json:"lsn"`
	Timeline       uint32 `json:"tli"`
	SystemID       string `json:"sysid"`
	RestorePoint   string `json:"rp"`
	WALSegmentSize int64  `json:"segsize"`
	RefsSHA256     string `json:"refs"`
	FilesSHA256    string `json:"files"`
	ReadAt         int64  `json:"read"`
}

// CheckpointRecord is a closed checkpoint, as the table keeps it.
type CheckpointRecord struct {
	CheckpointPosition
	Term     uint64 `json:"term"`
	Identity string `json:"id"`
	Root     string `json:"root"`
	Time     int64  `json:"t"`
	// ClosedAt is the index of the entry that closed it.
	ClosedAt uint64 `json:"closed"`
}

// Name is the checkpoint's name, unique across lineages and safe as a path
// element and a ref component.
func (c CheckpointRecord) Name() string {
	return CheckpointName(c.Identity, c.Index)
}

// CheckpointName names the checkpoint at index of a lineage.
func CheckpointName(identity string, index uint64) string {
	return fmt.Sprintf("%s-%020d", lineagePath.ReplaceAllString(identity, "-"), index)
}

var lineagePath = regexp.MustCompile(`[^0-9a-f]+`)

// pendingCheckpoint is an open barrier.
type pendingCheckpoint struct {
	Nonce    string `json:"nonce"`
	Index    uint64 `json:"i"`
	Term     uint64 `json:"term"`
	Identity string `json:"id"`
	Root     string `json:"root"`
	Time     int64  `json:"t"`
}

// CheckpointKeep is how many closed checkpoints the table retains: the
// observers read them back, and the artifacts behind older ones are gone.
const CheckpointKeep = 256

var (
	lsnPattern   = regexp.MustCompile(`^[0-9A-F]{1,8}/[0-9A-F]{1,8}$`)
	sysIDPattern = regexp.MustCompile(`^[0-9]{1,20}$`)
)

func (t *Table) applyCheckpointBarrier(e consensus.Entry, b *CheckpointBarrier) Result {
	if len(b.Nonce) != 32 {
		return refuse("checkpoint nonce must be 32 hex digits")
	}
	// A newer barrier supersedes an open one: its position, read under a
	// fence another term or attempt held, is refused when it comes.
	t.pending = &pendingCheckpoint{
		Nonce: b.Nonce, Index: e.Index, Term: e.Term, Identity: e.Token.Identity().String(),
		Root: t.root.String(), Time: b.Time,
	}
	return Result{OK: true}
}

func (t *Table) applyCheckpointPosition(e consensus.Entry, p *CheckpointPosition) Result {
	pc := t.pending
	switch {
	case pc == nil || pc.Nonce != p.Nonce || pc.Index != p.Index:
		return refuse("no open checkpoint barrier %s at %d", p.Nonce, p.Index)
	case e.Term != pc.Term:
		// The leader that held the fence lost its term: another leader's
		// SQL may have passed the barrier, so the cut is not provable.
		t.pending = nil
		return refuse("the checkpoint at %d was opened in term %d and closed in term %d", pc.Index, pc.Term, e.Term)
	case !lsnPattern.MatchString(p.LSN) || p.Timeline == 0 || !sysIDPattern.MatchString(p.SystemID):
		return refuse("the checkpoint's PostgreSQL position is malformed")
	case p.WALSegmentSize <= 0 || p.WALSegmentSize&(p.WALSegmentSize-1) != 0:
		return refuse("the WAL segment size must be a power of two")
	case !validDigest(p.RefsSHA256) || !validDigest(p.FilesSHA256):
		return refuse("the checkpoint's artifact digests are malformed")
	case len(p.RestorePoint) > 63:
		return refuse("the restore point name is longer than PostgreSQL allows")
	}
	t.pending = nil
	t.checkpoints = append(t.checkpoints, CheckpointRecord{
		CheckpointPosition: *p, Term: pc.Term, Identity: pc.Identity, Root: pc.Root, Time: pc.Time, ClosedAt: e.Index,
	})
	if n := len(t.checkpoints); n > CheckpointKeep {
		t.checkpoints = append([]CheckpointRecord(nil), t.checkpoints[n-CheckpointKeep:]...)
	}
	return Result{OK: true}
}

func validDigest(s string) bool {
	b, err := hex.DecodeString(s)
	return err == nil && len(b) == 32
}

// Checkpoints returns the closed checkpoints, oldest first.
func (t *Table) Checkpoints() []CheckpointRecord {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return append([]CheckpointRecord(nil), t.checkpoints...)
}

// OnCheckpoint registers the coordinator's capture of a barrier: it is
// called under the table's lock, right after the barrier applied, with a
// function that encodes the table as it is at that index. It must neither
// block nor call back into the table, and calls encode only for its own
// barrier: the table is the cut, and nothing else can give it later.
func (t *Table) OnCheckpoint(f func(nonce string, index uint64, encode func() ([]byte, error))) {
	t.mu.Lock()
	t.onCheckpoint = f
	t.mu.Unlock()
}
