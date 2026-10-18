// Copyright 2026 The Forgejo Authors. All rights reserved.
// SPDX-License-Identifier: GPL-3.0-or-later

package cluster

import (
	"context"
	"errors"
	"strings"

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

// ParkedResolution is the durable record of a parked event an operator
// resolved without its work: who, when, why, and the event itself, so
// nothing leaves the forge silently.
type ParkedResolution struct {
	ID        int64  `xorm:"pk autoincr"`
	Lineage   string `xorm:"VARCHAR(64) NOT NULL INDEX(lineage_index)"`
	LogIndex  int64  `xorm:"NOT NULL INDEX(lineage_index)"`
	Seq       int    `xorm:"NOT NULL DEFAULT 0"`
	Repo      string `xorm:"VARCHAR(255) NOT NULL"`
	Event     string `xorm:"LONGTEXT NOT NULL"`
	LastError string `xorm:"TEXT"`
	Attempts  int    `xorm:"NOT NULL DEFAULT 0"`
	// Operator is the opaque identity of who resolved it, Reason why.
	Operator     string             `xorm:"VARCHAR(255) NOT NULL"`
	Reason       string             `xorm:"TEXT NOT NULL"`
	ParkedUnix   timeutil.TimeStamp `xorm:"NOT NULL DEFAULT 0"`
	ResolvedUnix timeutil.TimeStamp `xorm:"created"`
}

// TableName keeps the table's name when the type is read from another package.
func (ParkedResolution) TableName() string { return "cluster_parked_resolution" }

func init() {
	db.RegisterModel(new(ParkedResolution))
}

// ErrResolveNeedsReason refuses a resolution without its why.
var ErrResolveNeedsReason = errors.New("a parked event is resolved with a reason")

// ErrParkedNotFound is a resolution of an index that holds no parked event.
var ErrParkedNotFound = errors.New("no parked event at that index")

// ErrParkedAmbiguous is an index parked in several lineages: the lineage
// must be named.
var ErrParkedAmbiguous = errors.New("that index is parked in several lineages; name the lineage")

// ResolveParked removes the parked rows of one entry without running its
// work, and records each one, payload included, with who resolved it and
// why, in the same transaction. lineage may be empty when the index is
// parked in one lineage only.
func ResolveParked(ctx context.Context, lineage string, index int64, operator, reason string) ([]*ParkedResolution, error) {
	if strings.TrimSpace(reason) == "" {
		return nil, ErrResolveNeedsReason
	}
	if strings.TrimSpace(operator) == "" {
		return nil, errors.New("a parked event is resolved by a named operator")
	}
	var out []*ParkedResolution
	err := db.WithTx(ctx, func(ctx context.Context) error {
		e := db.GetEngine(ctx)
		var rows []*ParkedEvent
		q := e.Where("log_index = ?", index)
		if lineage != "" {
			q = q.And("lineage = ?", lineage)
		}
		if err := q.OrderBy("lineage, seq").Find(&rows); err != nil {
			return err
		}
		if len(rows) == 0 {
			return ErrParkedNotFound
		}
		for _, r := range rows {
			if r.Lineage != rows[0].Lineage {
				return ErrParkedAmbiguous
			}
		}
		for _, r := range rows {
			out = append(out, &ParkedResolution{
				Lineage: r.Lineage, LogIndex: r.LogIndex, Seq: r.Seq, Repo: r.Repo, Event: r.Event,
				LastError: r.Reason, Attempts: r.Attempts, Operator: operator, Reason: reason, ParkedUnix: r.CreatedUnix,
			})
		}
		if _, err := e.Insert(out); err != nil {
			return err
		}
		return UnparkEntry(ctx, rows[0].Lineage, index, len(rows))
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}

// ListParkedResolutions returns the resolutions, newest first.
func ListParkedResolutions(ctx context.Context, limit int) ([]*ParkedResolution, error) {
	var out []*ParkedResolution
	err := db.GetEngine(ctx).OrderBy("id DESC").Limit(limit).Find(&out)
	return out, err
}
