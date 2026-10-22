// Copyright 2026 The Forgejo Authors. All rights reserved.
// SPDX-License-Identifier: GPL-3.0-or-later

package cluster

import (
	"context"
	"testing"

	cluster_model "forgejo.org/models/cluster"
	"forgejo.org/models/db"
	"forgejo.org/models/unittest"
	"forgejo.org/services/cluster/consensus"
	"forgejo.org/services/cluster/reftable"

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
