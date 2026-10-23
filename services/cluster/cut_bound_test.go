// Copyright 2026 The Forgejo Authors. All rights reserved.
// SPDX-License-Identifier: GPL-3.0-or-later

package cluster

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"io"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// stuckPG is a database/sql driver for a PostgreSQL the leader no longer
// reaches: a connection, or a BEGIN on one, waits as long as its context.
// Without stuck it answers, and the advisory lock is granted.
type stuckPG struct {
	stuckConnect, stuckBegin bool

	mu         sync.Mutex
	rolledBack int
}

func (d *stuckPG) Connect(ctx context.Context) (driver.Conn, error) {
	if d.stuckConnect {
		<-ctx.Done()
		return nil, ctx.Err()
	}
	return &stuckConn{d: d}, nil
}

func (d *stuckPG) Driver() driver.Driver { return nil }

func (d *stuckPG) rollbacks() int {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.rolledBack
}

type stuckConn struct{ d *stuckPG }

func (c *stuckConn) Prepare(string) (driver.Stmt, error) { return nil, errors.New("not supported") }
func (c *stuckConn) Close() error                        { return nil }
func (c *stuckConn) Begin() (driver.Tx, error)           { return nil, errors.New("not supported") }

func (c *stuckConn) BeginTx(ctx context.Context, _ driver.TxOptions) (driver.Tx, error) {
	if c.d.stuckBegin {
		<-ctx.Done()
		return nil, ctx.Err()
	}
	return c, nil
}

func (c *stuckConn) ExecContext(context.Context, string, []driver.NamedValue) (driver.Result, error) {
	return driver.RowsAffected(0), nil
}

func (c *stuckConn) QueryContext(context.Context, string, []driver.NamedValue) (driver.Rows, error) {
	return &grantedRows{}, nil
}

func (c *stuckConn) Commit() error { return nil }

func (c *stuckConn) Rollback() error {
	c.d.mu.Lock()
	c.d.rolledBack++
	c.d.mu.Unlock()
	return nil
}

// grantedRows is the one row of pg_try_advisory_xact_lock: true.
type grantedRows struct{ done bool }

func (r *grantedRows) Columns() []string { return []string{"granted"} }
func (r *grantedRows) Close() error      { return nil }

func (r *grantedRows) Next(dest []driver.Value) error {
	if r.done {
		return io.EOF
	}
	r.done = true
	dest[0] = true
	return nil
}

func stuckCut(t *testing.T, d *stuckPG) pgCut {
	sqlDB := sql.OpenDB(d)
	t.Cleanup(func() { sqlDB.Close() })
	return pgCut{db: func() (*sql.DB, error) { return sqlDB, nil }}
}

// The coordinator takes the cut under the leader's fence, within
// drainBound: a PostgreSQL that no longer answers the leader held the
// fence, and every write of the leader, until the kernel gave up
// connecting, at every period (#152). The cut's transaction is opened
// within the caller's context, and outlives it once open.
func TestTheCutIsTakenWithinItsBound(t *testing.T) {
	t.Parallel()
	for _, d := range []*stuckPG{{stuckConnect: true}, {stuckBegin: true}} {
		cut := stuckCut(t, d)
		for name, take := range map[string]func(ctx context.Context) error{
			"exclusive": func(ctx context.Context) error { _, _, err := cut.TryExclusive(ctx); return err },
			"shared":    func(ctx context.Context) error { _, err := cut.Shared(ctx); return err },
		} {
			ctx, cancel := context.WithTimeout(t.Context(), 100*time.Millisecond)
			done := make(chan error, 1)
			go func() { done <- take(ctx) }()
			select {
			case err := <-done:
				require.ErrorIs(t, err, context.DeadlineExceeded, "%s, connect stuck %v", name, d.stuckConnect)
			case <-time.After(10 * time.Second):
				t.Fatalf("%s, connect stuck %v: the cut waits past its context", name, d.stuckConnect)
			}
			cancel()
		}
	}

	d := &stuckPG{}
	cut := stuckCut(t, d)
	ctx, cancel := context.WithCancel(t.Context())
	release, ok, err := cut.TryExclusive(ctx)
	require.NoError(t, err)
	require.True(t, ok)
	cancel()
	time.Sleep(50 * time.Millisecond)
	assert.Zero(t, d.rollbacks(), "the cut is held past the context it was taken in")
	release()
	release()
	assert.Equal(t, 1, d.rollbacks(), "released once")
}
