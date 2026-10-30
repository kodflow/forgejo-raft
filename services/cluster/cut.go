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
	"forgejo.org/modules/clusterhook"
	"forgejo.org/modules/log"
	"forgejo.org/modules/setting"
	"forgejo.org/modules/storage"
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

// begin opens the hold's transaction within ctx, and returns the function
// that ends it, once.
//
// The transaction lives as long as the hold, not as the caller's context:
// database/sql rolls back a transaction whose context ends. Its context is
// one of its own, which the caller's ends only until the transaction is
// open: the coordinator takes the cut under the leader's fence, and a
// PostgreSQL that no longer answers the leader held every write of the
// leader until the kernel gave up connecting, at every period (#152).
func (p pgCut) begin(ctx context.Context) (*sql.Tx, func(), error) {
	sqlDB, err := p.db()
	if err != nil {
		return nil, nil, err
	}
	txCtx, cancel := context.WithCancel(context.Background())
	stop := context.AfterFunc(ctx, cancel)
	tx, err := sqlDB.BeginTx(txCtx, nil)
	if err == nil {
		// It stays idle while the operation it covers runs: a server-wide
		// idle_in_transaction_session_timeout would end it, and drop the
		// cut, under the operation.
		if _, err = tx.ExecContext(ctx, "SET LOCAL idle_in_transaction_session_timeout = 0"); err != nil {
			_ = tx.Rollback()
		}
	}
	if !stop() {
		// ctx ended: the transaction, if open, is rolled back or about to
		// be, and a failure is ctx's own.
		if err == nil {
			_ = tx.Rollback()
		}
		err = context.Cause(ctx)
	}
	if err != nil {
		cancel()
		return nil, nil, err
	}
	var once sync.Once
	return tx, func() {
		once.Do(func() {
			rollbackCut(tx)
			cancel()
		})
	}, nil
}

func (p pgCut) Shared(ctx context.Context) (func(), error) {
	tx, end, err := p.begin(ctx)
	if err != nil {
		return nil, err
	}
	if _, err := tx.ExecContext(ctx, "SELECT pg_advisory_xact_lock_shared($1)", cutLockKey); err != nil {
		end()
		return nil, err
	}
	return end, nil
}

func (p pgCut) TryExclusive(ctx context.Context) (func(), bool, error) {
	tx, end, err := p.begin(ctx)
	if err != nil {
		return nil, false, err
	}
	var ok bool
	if err := tx.QueryRowContext(ctx, "SELECT pg_try_advisory_xact_lock($1)", cutLockKey).Scan(&ok); err != nil {
		end()
		return nil, false, err
	}
	if !ok {
		end()
		return func() {}, false, nil
	}
	return end, true, nil
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
	return deleteUnderCut(context.Background(), r.cut, del)
}

// deleteUnderCut runs del while it holds cut shared, so that it waits for
// a checkpoint listing the stored files. It waits cutDeleteBound at most,
// and no longer than ctx: the deletion is then refused, and del never runs
// without the cut.
func deleteUnderCut(ctx context.Context, cut cutLock, del func() error) error {
	ctx, cancel := context.WithTimeout(ctx, cutDeleteBound)
	defer cancel()
	release, err := cut.Shared(ctx)
	if err != nil {
		return fmt.Errorf("waiting for the cluster's checkpoint to list the stored files: %w", err)
	}
	defer release()
	return del()
}

// cutWithoutReplicator is the cut as a process of a cluster node that runs
// no replicator reaches it: the replicator's advisory lock, in the
// PostgreSQL every member shares. Without PostgreSQL no checkpoint runs,
// and it is nil. It is read at each deletion: `forgejo doctor` loads the
// database's settings after it registered its gate. Tests give another.
var cutWithoutReplicator = func() cutLock {
	if !setting.Database.Type.IsPostgreSQL() {
		return nil
	}
	return pgCut{db: cutDB}
}

// GateDeletionsWithoutReplicator registers, in a process of a cluster node
// that runs no replicator (`forgejo doctor`), the deletion gate a running
// node's replicator registers: a stored file of the storages a checkpoint
// covers is deleted under the cluster's cut, which the process takes in
// PostgreSQL as a member does. Without it, `forgejo doctor check --run
// storages --fix` deleted a file it found without a row at once, while a
// checkpoint listed the files, and the checkpoint missed a file the SQL at
// its L names (#358). A deletion waits cutDeleteBound at most, and no
// longer than ctx: it is then refused, and the file stays, an orphan a
// later run deletes. Outside a cluster, and in the process of a
// replicator, whose own gate stays, it registers nothing. release removes
// the gate.
func GateDeletionsWithoutReplicator(ctx context.Context) (release func()) {
	if !setting.Cluster.Enabled || clusterhook.Enabled() {
		return func() {}
	}
	storage.SetDeleteGate(func(del func() error) error {
		cut := cutWithoutReplicator()
		if cut == nil {
			return del()
		}
		return deleteUnderCut(ctx, cut, del)
	})
	return func() { storage.SetDeleteGate(nil) }
}
