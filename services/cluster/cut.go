// Copyright 2026 The Forgejo Authors. All rights reserved.
// SPDX-License-Identifier: GPL-3.0-or-later

package cluster

import (
	"context"
	"database/sql"
	"fmt"
	"sync"
	"time"

	"forgejo.org/models/db"
	"forgejo.org/modules/log"
)

// The cut of a checkpoint is one lock for the whole cluster, in the
// PostgreSQL every member shares. A deletion, a rename, a transfer or an
// owner's rename holds it shared from before its SQL transaction until
// both its SQL and its replicated entry committed, on whichever member it
// runs: they then fall on the same side of the checkpoint's (B, L) (#88). The
// deletion of a stored file holds it shared too, so a file the SQL at L
// names is still there when the checkpoint lists the files (#90). The
// coordinator takes it exclusively, without waiting: from the fence until
// the files are listed. No member waits on another for it but through
// PostgreSQL, and a member of an older version that does not take it
// behaves as before.
type cutLock interface {
	// Shared holds the cut until release, waiting while a checkpoint
	// holds it.
	Shared(ctx context.Context) (release func(), err error)
	// TryExclusive takes the cut for a checkpoint, false at once when an
	// operation holds it; release is then a no-op.
	TryExclusive(ctx context.Context) (release func(), ok bool, err error)
}

// cutLockKey is the advisory lock's key, the same on every member.
const cutLockKey int64 = 0x666a63757400 // "fjcut\0"

// pgCut is the cut as a PostgreSQL advisory lock, held by a transaction of
// its own: a lock its transaction ends with is never left behind on a
// pooled connection, and a member that dies releases it with its session.
// db is the pool those transactions use; tests give another.
type pgCut struct {
	db func() (*sql.DB, error)
}

var cutPool struct {
	once sync.Once
	db   *sql.DB
	err  error
}

// cutDB is a pool of its own, on Forgejo's database: a hold keeps its
// connection while the operation it covers opens others, and in Forgejo's
// bounded pool (MAX_OPEN_CONNS) enough holds at once would wait on one
// another for a connection.
func cutDB() (*sql.DB, error) {
	cutPool.once.Do(func() {
		x, err := db.GetMasterEngine(db.DefaultContext.(db.Engined).Engine())
		if err != nil {
			cutPool.err = err
			return
		}
		cutPool.db, cutPool.err = sql.Open(x.DriverName(), x.DataSourceName())
		if cutPool.err == nil {
			cutPool.db.SetMaxIdleConns(2)
		}
	})
	return cutPool.db, cutPool.err
}

func (p pgCut) begin() (*sql.Tx, error) {
	sqlDB, err := p.db()
	if err != nil {
		return nil, err
	}
	// The transaction lives as long as the hold, not as the caller's
	// context: database/sql rolls back a transaction whose context ends.
	tx, err := sqlDB.BeginTx(context.Background(), nil)
	if err != nil {
		return nil, err
	}
	// It stays idle while the operation it covers runs: a server-wide
	// idle_in_transaction_session_timeout would end it, and drop the
	// cut, under the operation.
	if _, err := tx.Exec("SET LOCAL idle_in_transaction_session_timeout = 0"); err != nil {
		_ = tx.Rollback()
		return nil, err
	}
	return tx, nil
}

func (p pgCut) Shared(ctx context.Context) (func(), error) {
	tx, err := p.begin()
	if err != nil {
		return nil, err
	}
	if _, err := tx.ExecContext(ctx, "SELECT pg_advisory_xact_lock_shared($1)", cutLockKey); err != nil {
		_ = tx.Rollback()
		return nil, err
	}
	return func() { rollbackCut(tx) }, nil
}

func (p pgCut) TryExclusive(ctx context.Context) (func(), bool, error) {
	tx, err := p.begin()
	if err != nil {
		return nil, false, err
	}
	var ok bool
	if err := tx.QueryRowContext(ctx, "SELECT pg_try_advisory_xact_lock($1)", cutLockKey).Scan(&ok); err != nil {
		_ = tx.Rollback()
		return nil, false, err
	}
	if !ok {
		_ = tx.Rollback()
		return func() {}, false, nil
	}
	return func() { rollbackCut(tx) }, true, nil
}

func rollbackCut(tx *sql.Tx) {
	if err := tx.Rollback(); err != nil {
		log.Warn("cluster: releasing the checkpoint's cut: %v", err)
	}
}

// holdCut is clusterhook.HoldCut's side in the replicator. Without the
// cut (no PostgreSQL), no checkpoint runs, and it holds nothing.
func (r *Replicator) holdCut(ctx context.Context) (func(), error) {
	if r.cut == nil {
		return func() {}, nil
	}
	return r.cut.Shared(ctx)
}

// cutDeleteBound is how long the deletion of a stored file waits for a
// checkpoint's listing before it fails: the file then stays, an orphan
// the caller logs, rather than a file the SQL at L names and the
// checkpoint misses.
var cutDeleteBound = 10 * time.Minute

// gateDelete is the stored files' deletion gate (storage.SetDeleteGate).
func (r *Replicator) gateDelete(del func() error) error {
	if r.cut == nil {
		return del()
	}
	ctx, cancel := context.WithTimeout(context.Background(), cutDeleteBound)
	defer cancel()
	release, err := r.cut.Shared(ctx)
	if err != nil {
		return fmt.Errorf("waiting for the cluster's checkpoint to list the stored files: %w", err)
	}
	defer release()
	return del()
}
