// Copyright 2026 The Forgejo Authors. All rights reserved.
// SPDX-License-Identifier: GPL-3.0-or-later

package cluster

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	_ "github.com/jackc/pgx/v5/stdlib" // the driver Forgejo uses for PostgreSQL
)

// The cut on a real PostgreSQL, two members as two connection pools:
// holds nest, a checkpoint is refused while one is held, a hold waits
// while a checkpoint holds the cut, and a hold whose context ends gives
// up. It runs when FORGEJO_CLUSTER_TEST_PGSQL names a database, for
// example postgres://postgres:postgres@127.0.0.1:5432/postgres; it
// changes that database's idle_in_transaction_session_timeout for a while.
func TestCutOnPostgreSQL(t *testing.T) {
	dsn := os.Getenv("FORGEJO_CLUSTER_TEST_PGSQL")
	if dsn == "" {
		t.Skip("FORGEJO_CLUSTER_TEST_PGSQL is not set")
	}
	member := func() pgCut {
		sqlDB, err := sql.Open("pgx", dsn)
		require.NoError(t, err)
		t.Cleanup(func() { sqlDB.Close() })
		return pgCut{db: func() (*sql.DB, error) { return sqlDB, nil }}
	}
	a, b := member(), member()
	ctx := context.Background()
	// PostgreSQL's view of the cut, from a pool of neither member.
	probe, err := sql.Open("pgx", dsn)
	require.NoError(t, err)
	t.Cleanup(func() { probe.Close() })

	// A hold outlives an idle_in_transaction_session_timeout set on the
	// server: the operation it covers may take longer.
	release, err := a.Shared(ctx)
	require.NoError(t, err)
	sqlDB, err := b.db()
	require.NoError(t, err)
	_, err = sqlDB.Exec("ALTER DATABASE postgres SET idle_in_transaction_session_timeout = '100ms'")
	require.NoError(t, err)
	t.Cleanup(func() { _, _ = sqlDB.Exec("ALTER DATABASE postgres RESET idle_in_transaction_session_timeout") })
	release()
	sqlDB.SetMaxIdleConns(0) // new sessions read the new default
	a = member()
	idle, err := a.Shared(ctx)
	require.NoError(t, err)
	time.Sleep(300 * time.Millisecond)
	_, ok, err := b.TryExclusive(ctx)
	require.NoError(t, err)
	assert.False(t, ok, "the idle hold still holds the cut")
	idle()

	r1, err := a.Shared(ctx)
	require.NoError(t, err)
	r2, err := b.Shared(ctx)
	require.NoError(t, err, "holds on two members nest")
	_, ok, err = b.TryExclusive(ctx)
	require.NoError(t, err)
	assert.False(t, ok, "a checkpoint is refused while an operation holds the cut")
	r1()
	_, ok, err = a.TryExclusive(ctx)
	require.NoError(t, err)
	assert.False(t, ok)
	r2()

	unlock, ok, err := a.TryExclusive(ctx)
	require.NoError(t, err)
	require.True(t, ok, "released, the cut is the checkpoint's")
	got := make(chan error, 1)
	go func() {
		release, err := b.Shared(ctx)
		if err == nil {
			release()
		}
		got <- err
	}()
	select {
	case err := <-got:
		t.Fatalf("a hold went through a checkpoint's cut: %v", err)
	case <-time.After(300 * time.Millisecond):
	}
	short, cancel := context.WithTimeout(ctx, 100*time.Millisecond)
	defer cancel()
	_, err = a.Shared(short)
	require.Error(t, err, "a hold whose context ends gives up")
	unlock()
	require.NoError(t, <-got, "the hold goes through once the checkpoint released the cut")

	// The hold that gave up returned before PostgreSQL let its session go:
	// pgx cancels the query of a context that ended from a goroutine of its
	// own, and the checkpoint's release granted that session the cut
	// meanwhile, which it holds until its cancel lands. The checkpoint
	// that follows then found the cut held, at random (#270); the
	// coordinator tries again within drainBound. Every hold returned: the
	// cut is soon nobody's, or one was left behind.
	require.EventuallyWithT(t, func(c *assert.CollectT) {
		sessions, err := cutSessions(probe)
		if assert.NoError(c, err) {
			assert.Empty(c, sessions, "the sessions that hold or wait for the cut")
		}
	}, 20*time.Second, 10*time.Millisecond, "the session of the hold that gave up lets the cut go")
	unlock, ok, err = b.TryExclusive(ctx)
	require.NoError(t, err)
	assert.True(t, ok, "nothing was left behind on a pooled connection")
	unlock()
}

// cutSessions lists the sessions of the current database that hold or wait
// for the cut, as PostgreSQL sees them: the advisory lock of a bigint key
// shows its high half as classid, its low half as objid, and 1 as objsubid.
func cutSessions(probe *sql.DB) ([]string, error) {
	rows, err := probe.Query(`SELECT l.pid, l.mode, l.granted, coalesce(a.state, '')
		FROM pg_locks l LEFT JOIN pg_stat_activity a ON a.pid = l.pid
		WHERE l.locktype = 'advisory'
		AND l.database = (SELECT oid FROM pg_database WHERE datname = current_database())
		AND l.classid::bigint = $1 AND l.objid::bigint = $2 AND l.objsubid = 1
		ORDER BY l.pid`, cutLockKey>>32, cutLockKey&0xffffffff)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var pid int
		var mode, state string
		var granted bool
		if err := rows.Scan(&pid, &mode, &granted, &state); err != nil {
			return nil, err
		}
		out = append(out, fmt.Sprintf("pid %d %s granted=%v %q", pid, mode, granted, state))
	}
	return out, rows.Err()
}
