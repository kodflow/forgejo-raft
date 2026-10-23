// Copyright 2026 The Forgejo Authors. All rights reserved.
// SPDX-License-Identifier: GPL-3.0-or-later

package cluster

import (
	"context"
	"database/sql"
	"os"
	"testing"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib" // the driver Forgejo uses for PostgreSQL

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
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

	unlock, ok, err = b.TryExclusive(ctx)
	require.NoError(t, err)
	assert.True(t, ok, "nothing was left behind on a pooled connection")
	unlock()
}
