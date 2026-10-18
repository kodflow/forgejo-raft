// Copyright 2026 The Forgejo Authors. All rights reserved.
// SPDX-License-Identifier: GPL-3.0-or-later

package cluster

import (
	"context"
	"errors"

	"forgejo.org/models/db"
	"forgejo.org/modules/timeutil"
)

// ParkedEvent is an event of the log whose post-receive work the consumer
// could not run (it kept failing, or its repository was never projected).
// It is never dropped: the row commits with the cursor that moves past the
// event, the log keeps the event while the row exists, and the row is
// removed only by the transaction that finally runs the work.
type ParkedEvent struct {
	ID       int64  `xorm:"pk autoincr"`
	Lineage  string `xorm:"VARCHAR(64) NOT NULL UNIQUE(lineage_index_seq) INDEX(lineage_index)"`
	LogIndex int64  `xorm:"NOT NULL UNIQUE(lineage_index_seq) INDEX(lineage_index)"`
	// Seq is the event's place among the events of its entry.
	Seq  int    `xorm:"NOT NULL DEFAULT 0 UNIQUE(lineage_index_seq)"`
	Repo string `xorm:"VARCHAR(255) NOT NULL INDEX"`
	// Event is the event, as the table encodes it.
	Event       string             `xorm:"LONGTEXT NOT NULL"`
	Reason      string             `xorm:"TEXT"`
	Attempts    int                `xorm:"NOT NULL DEFAULT 0"`
	CreatedUnix timeutil.TimeStamp `xorm:"created"`
	UpdatedUnix timeutil.TimeStamp `xorm:"updated"`
}

// TableName keeps the table's name when the type is read from another package.
func (ParkedEvent) TableName() string { return "cluster_parked_event" }

func init() {
	db.RegisterModel(new(ParkedEvent))
}

// ErrParkedGone is a retry that lost: another retrier ran the work first.
var ErrParkedGone = errors.New("parked event already applied")

// ParkEvents inserts the rows of parked events, in the transaction ctx
// carries, so they commit with the cursor that moves past them.
func ParkEvents(ctx context.Context, rows []*ParkedEvent) error {
	if len(rows) == 0 {
		return nil
	}
	_, err := db.GetEngine(ctx).Insert(rows)
	return err
}

// RepoHasParked reports whether a repository has a parked event: its later
// events are parked behind it, so the post-receive work keeps its order.
func RepoHasParked(ctx context.Context, repo string) (bool, error) {
	return db.GetEngine(ctx).Where("repo = ?", repo).Exist(new(ParkedEvent))
}

// ListParked returns every parked event, by lineage, index and place.
func ListParked(ctx context.Context) ([]*ParkedEvent, error) {
	var out []*ParkedEvent
	err := db.GetEngine(ctx).OrderBy("lineage, log_index, seq").Find(&out)
	return out, err
}

// CountParked counts the parked events.
func CountParked(ctx context.Context) (int64, error) {
	return db.GetEngine(ctx).Count(new(ParkedEvent))
}

// MinParkedIndex returns the lowest parked index of a lineage; ok is false
// when the lineage has none.
func MinParkedIndex(ctx context.Context, lineage string) (int64, bool, error) {
	p := new(ParkedEvent)
	has, err := db.GetEngine(ctx).Where("lineage = ?", lineage).OrderBy("log_index").Limit(1).Get(p)
	if err != nil || !has {
		return 0, false, err
	}
	return p.LogIndex, true, nil
}

// UnparkEntry removes the rows of one entry, in the transaction ctx
// carries, which must also hold the entry's work: want is how many rows
// the caller read. Fewer means another retrier applied it: ErrParkedGone,
// and the caller's transaction rolls back.
func UnparkEntry(ctx context.Context, lineage string, index int64, want int) error {
	n, err := db.GetEngine(ctx).Where("lineage = ? AND log_index = ?", lineage, index).Delete(new(ParkedEvent))
	if err != nil {
		return err
	}
	if int(n) != want {
		return ErrParkedGone
	}
	return nil
}

// NoteParkedFailure records a failed retry of one entry.
func NoteParkedFailure(ctx context.Context, lineage string, index int64, reason string) error {
	_, err := db.GetEngine(ctx).Exec("UPDATE cluster_parked_event SET attempts = attempts + 1, reason = ?, updated_unix = ? WHERE lineage = ? AND log_index = ?",
		reason, timeutil.TimeStampNow(), lineage, index)
	return err
}
