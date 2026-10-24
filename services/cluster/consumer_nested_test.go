// Copyright 2026 The Forgejo Authors. All rights reserved.
// SPDX-License-Identifier: GPL-3.0-or-later

package cluster

import (
	"context"
	"errors"
	"fmt"
	"testing"

	cluster_model "forgejo.org/models/cluster"
	"forgejo.org/models/db"
	"forgejo.org/models/unittest"
	"forgejo.org/services/cluster/consensus"
	"forgejo.org/services/cluster/reftable"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// nestedFailureWork is the post-receive work of a batch of two groups where
// the second one, like handleWorkflows around InsertRun, runs a nested
// transaction that fails, logs the error and goes on.
func nestedFailureWork(probe string) func(ctx context.Context, evs []reftable.Event) error {
	return func(ctx context.Context, evs []reftable.Event) error {
		name := probe + "-" + evs[0].Repo
		if evs[0].Repo == "a/two.git" {
			_ = db.WithTx(ctx, func(ctx context.Context) error {
				// A row that exists already: the insert fails in SQL.
				_, err := db.GetEngine(ctx).Insert(&cluster_model.Cursor{Name: probe + "-a/one.git"})
				return err
			})
		}
		return cluster_model.AdvanceCursor(ctx, name, cluster_model.Cursor{}, cluster_model.Cursor{Lineage: "work", Position: int64(evs[0].Token.Index)})
	}
}

func probeExists(t *testing.T, name string) bool {
	t.Helper()
	c, err := cluster_model.GetCursor(db.DefaultContext, name)
	require.NoError(t, err)
	return c.Lineage != ""
}

// A nested transaction that fails inside the batch's work, its error
// swallowed, rolls back alone: the batch's other groups and its cursor
// commit together, never split between a rollback and autocommit (#104,
// TestHuntR7ProveNestedBatchPG, TestHuntChaossqlNestedTxFailureSplitsTheBatch).
func TestASwallowedNestedFailureKeepsTheBatch(t *testing.T) {
	require.NoError(t, unittest.PrepareTestDatabase())
	ctx := db.DefaultContext
	c := newConsumer(newReplicator(t.TempDir(), newRegistry(0, 1, 1)))
	c.hasParked = nil
	c.work = nestedFailureWork("nested")
	tok := func(i uint64) consensus.Token {
		return consensus.Token{Cluster: consensus.ClusterID{0x10, 0x4}, Epoch: 1, Term: 1, Index: i}
	}
	lineage := tok(1).Identity().String()
	batch := []reftable.Event{
		{Token: tok(11), Kind: reftable.EventRef, Repo: "a/one.git", Ref: "refs/heads/main", PusherID: 1},
		{Token: tok(12), Kind: reftable.EventRef, Repo: "a/two.git", Ref: "refs/heads/main", PusherID: 2},
	}
	require.NoError(t, c.dispatch(ctx, lineage, &cluster_model.Cursor{}, batch))
	got, err := cluster_model.GetCursor(ctx, cursorName(lineage))
	require.NoError(t, err)
	assert.EqualValues(t, 12, got.Position)
	assert.True(t, probeExists(t, "nested-a/one.git"), "the first group's SQL committed with the cursor")
	assert.True(t, probeExists(t, "nested-a/two.git"), "the second group's SQL committed with the cursor")

	// A parked entry retried the same way: its work and the removal of its
	// row commit together.
	c.work = nestedFailureWork("parked")
	require.NoError(t, cluster_model.ParkEvents(ctx, []*cluster_model.ParkedEvent{
		{Lineage: lineage, LogIndex: 13, Repo: "a/one.git", Event: `{"tok":{},"k":"ref","repo":"a/one.git"}`, Reason: "boom", Attempts: maxAttempts},
		{Lineage: lineage, LogIndex: 14, Repo: "a/two.git", Event: `{"tok":{},"k":"ref","repo":"a/two.git"}`, Reason: "boom", Attempts: maxAttempts},
	}))
	c.lineageNow = nil
	applied, err := c.retryParked(ctx)
	require.NoError(t, err)
	assert.Equal(t, 2, applied)
	n, err := cluster_model.CountParked(ctx)
	require.NoError(t, err)
	assert.Zero(t, n)
	assert.True(t, probeExists(t, "parked-a/one.git"))
	assert.True(t, probeExists(t, "parked-a/two.git"))
}

// A work that ended the batch's transaction all the same, a nested
// transaction outside a savepoint: nothing advances past it.
func TestABatchWhoseTransactionEndedDoesNotAdvance(t *testing.T) {
	require.NoError(t, unittest.PrepareTestDatabase())
	ctx := db.DefaultContext
	c := newConsumer(newReplicator(t.TempDir(), newRegistry(0, 1, 1)))
	c.hasParked = nil
	c.tx = db.WithTx
	c.work = nestedFailureWork("ended")
	tok := func(i uint64) consensus.Token {
		return consensus.Token{Cluster: consensus.ClusterID{0x10, 0x5}, Epoch: 1, Term: 1, Index: i}
	}
	lineage := tok(1).Identity().String()
	batch := []reftable.Event{
		{Token: tok(21), Kind: reftable.EventRef, Repo: "a/one.git", Ref: "refs/heads/main", PusherID: 1},
		{Token: tok(22), Kind: reftable.EventRef, Repo: "a/two.git", Ref: "refs/heads/main", PusherID: 2},
	}
	err := c.dispatch(ctx, lineage, &cluster_model.Cursor{}, batch)
	require.ErrorIs(t, err, errBatchTxEnded)
	got, err := cluster_model.GetCursor(ctx, cursorName(lineage))
	require.NoError(t, err)
	assert.Empty(t, got.Lineage, "the cursor did not move past the lost work")
}

// A statement that gave up waiting for a lock in a nested transaction
// whose caller logs and swallows the failure, as pushUpdates swallows
// updateIssuesCommit's: its write was rolled back to its savepoint, and
// the batch committed without it, where a wait without bound would have
// made it (#177). The group fails instead, is tried again, then parked
// while the batch's other groups go on; its parked entry stays parked
// while its retry gives up too, and is applied once the lock is free.
func TestASwallowedLockTimeoutFailsItsGroup(t *testing.T) {
	require.NoError(t, unittest.PrepareTestDatabase())
	ctx := db.DefaultContext
	c := newConsumer(newReplicator(t.TempDir(), newRegistry(0, 1, 1)))
	c.lineageNow = nil
	held := true
	c.work = func(ctx context.Context, evs []reftable.Event) error {
		repo := evs[0].Repo
		_ = db.WithTx(ctx, func(ctx context.Context) error {
			if err := cluster_model.AdvanceCursor(ctx, "lockwait-nested-"+repo, cluster_model.Cursor{}, cluster_model.Cursor{Lineage: "work", Position: 1}); err != nil {
				return err
			}
			if repo == "a/held.git" && held {
				return fmt.Errorf("UpdateRepoSize: %v", &pgconn.PgError{Severity: "ERROR", Code: pgLockNotAvailable, Message: "canceling statement due to lock timeout"})
			}
			return nil
		})
		return cluster_model.AdvanceCursor(ctx, "lockwait-"+repo, cluster_model.Cursor{}, cluster_model.Cursor{Lineage: "work", Position: int64(evs[0].Token.Index)})
	}
	tok := func(i uint64) consensus.Token {
		return consensus.Token{Cluster: consensus.ClusterID{0x17, 0x7}, Epoch: 1, Term: 1, Index: i}
	}
	lineage := tok(1).Identity().String()
	batch := []reftable.Event{
		{Token: tok(31), Kind: reftable.EventRef, Repo: "a/held.git", Ref: "refs/heads/main", PusherID: 1},
		{Token: tok(32), Kind: reftable.EventRef, Repo: "a/other.git", Ref: "refs/heads/main", PusherID: 2},
	}
	cur := &cluster_model.Cursor{}
	for range maxAttempts {
		err := c.dispatch(ctx, lineage, cur, batch)
		require.ErrorContains(t, err, "gave up waiting for a lock")
		got, err := cluster_model.GetCursor(ctx, cursorName(lineage))
		require.NoError(t, err)
		require.Empty(t, got.Lineage, "the batch without the lost write did not commit")
		require.False(t, probeExists(t, "lockwait-a/held.git"))
		require.False(t, probeExists(t, "lockwait-a/other.git"))
	}
	require.NoError(t, c.dispatch(ctx, lineage, cur, batch), "the held group is parked, the rest goes on")
	got, err := cluster_model.GetCursor(ctx, cursorName(lineage))
	require.NoError(t, err)
	assert.EqualValues(t, 32, got.Position)
	assert.True(t, probeExists(t, "lockwait-a/other.git"), "the other group committed with the cursor")
	assert.False(t, probeExists(t, "lockwait-a/held.git"))
	assert.False(t, probeExists(t, "lockwait-nested-a/held.git"), "the lost write is not half-applied")
	parked, err := cluster_model.ListParked(ctx)
	require.NoError(t, err)
	require.Len(t, parked, 1)
	assert.Equal(t, "a/held.git", parked[0].Repo)
	assert.Contains(t, parked[0].Reason, "gave up waiting for a lock")

	// Retried while the lock is still held, the entry stays parked.
	applied, err := c.retryParked(ctx)
	require.NoError(t, err)
	assert.Zero(t, applied)
	assert.False(t, probeExists(t, "lockwait-a/held.git"))
	n, err := cluster_model.CountParked(ctx)
	require.NoError(t, err)
	assert.EqualValues(t, 1, n)

	// Once the lock is free, the retry applies it, the nested write with it.
	held = false
	applied, err = c.retryParked(ctx)
	require.NoError(t, err)
	assert.Equal(t, 1, applied)
	assert.True(t, probeExists(t, "lockwait-a/held.git"))
	assert.True(t, probeExists(t, "lockwait-nested-a/held.git"))
	n, err = cluster_model.CountParked(ctx)
	require.NoError(t, err)
	assert.Zero(t, n)
}

// The lock waits a nested transaction gave up are told from its other
// failures, wrapped with %w or formatted with %v on the way.
func TestIsLockTimeout(t *testing.T) {
	pgErr := &pgconn.PgError{Severity: "ERROR", Code: pgLockNotAvailable, Message: "canceling statement due to lock timeout"}
	assert.True(t, isLockTimeout(pgErr))
	assert.True(t, isLockTimeout(fmt.Errorf("wrapped: %w", pgErr)))
	assert.True(t, isLockTimeout(fmt.Errorf("formatted: %v", pgErr)))
	assert.False(t, isLockTimeout(&pgconn.PgError{Severity: "ERROR", Code: "23505", Message: "duplicate key value violates unique constraint"}))
	assert.False(t, isLockTimeout(errors.New("database is locked")))
}
