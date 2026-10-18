// Copyright 2026 The Forgejo Authors. All rights reserved.
// SPDX-License-Identifier: GPL-3.0-or-later

// Package cluster holds the SQL side of the ref replicator: the cursors of
// the log consumer, which say how far SQL derived from the replicated log
// has come (kitsunium ADR 0152 D19, R7-29, R8-25, R8-29, R9-14).
package cluster

import (
	"context"
	"errors"

	"forgejo.org/models/db"
	"forgejo.org/modules/timeutil"
)

// Cursor names.
const (
	// CursorDispatch is the consumer's position: every event at or below it
	// has had its post-receive work committed.
	CursorDispatch = "dispatch"
	// CursorBackup is written by each checkpoint with the index of its
	// barrier; trimming never passes it, whatever its lag (R9-14).
	CursorBackup = "backup"
)

// Cursor is one cursor of the consumer: a lineage and a position in it.
type Cursor struct {
	Name        string             `xorm:"pk VARCHAR(64)"`
	Lineage     string             `xorm:"VARCHAR(64) NOT NULL"`
	Position    int64              `xorm:"NOT NULL DEFAULT 0"`
	Closed      bool               `xorm:"NOT NULL DEFAULT false"`
	UpdatedUnix timeutil.TimeStamp `xorm:"updated"`
}

// TableName keeps the table's name when the type is read from another package.
func (Cursor) TableName() string { return "cluster_cursor" }

func init() {
	db.RegisterModel(new(Cursor))
}

// ErrCursorMoved is a compare-and-set that lost: another dispatcher, or a
// replay after a failover, moved the cursor first.
var ErrCursorMoved = errors.New("cluster cursor moved")

// GetCursor reads a cursor; a missing one is the zero cursor.
func GetCursor(ctx context.Context, name string) (*Cursor, error) {
	c := &Cursor{Name: name}
	has, err := db.GetEngine(ctx).Get(c)
	if err != nil {
		return nil, err
	}
	if !has {
		return &Cursor{Name: name}, nil
	}
	return c, nil
}

// AdvanceCursor moves a cursor from prev to next as a compare-and-set, in the
// transaction ctx carries, so the cursor commits with the work it covers.
func AdvanceCursor(ctx context.Context, name string, prev, next Cursor) error {
	e := db.GetEngine(ctx)
	if prev.Lineage == "" && prev.Position == 0 {
		has, err := e.Exist(&Cursor{Name: name})
		if err != nil {
			return err
		}
		if !has {
			_, err := e.Insert(&Cursor{Name: name, Lineage: next.Lineage, Position: next.Position, Closed: next.Closed})
			return err
		}
	}
	n, err := e.Table("cluster_cursor").
		Where("name = ? AND lineage = ? AND position = ? AND closed = ?", name, prev.Lineage, prev.Position, prev.Closed).
		Cols("lineage", "position", "closed", "updated_unix").
		Update(&Cursor{Lineage: next.Lineage, Position: next.Position, Closed: next.Closed})
	if err != nil {
		return err
	}
	if n != 1 {
		return ErrCursorMoved
	}
	return nil
}

// SetCursor moves a cursor to next whatever it held: the backup cursor,
// which the checkpoint coordinator alone writes.
func SetCursor(ctx context.Context, name string, next Cursor) error {
	return db.WithTx(ctx, func(ctx context.Context) error {
		cur, err := GetCursor(ctx, name)
		if err != nil {
			return err
		}
		if cur.Lineage == next.Lineage && cur.Position == next.Position && cur.Closed == next.Closed {
			return nil
		}
		return AdvanceCursor(ctx, name, *cur, next)
	})
}
