// Copyright 2026 The Forgejo Authors. All rights reserved.
// SPDX-License-Identifier: GPL-3.0-or-later

package cluster

import (
	"context"
	"errors"
	"testing"

	cluster_model "forgejo.org/models/cluster"
	"forgejo.org/models/db"
	"forgejo.org/models/unittest"
	"forgejo.org/services/cluster/consensus"
	"forgejo.org/services/cluster/reftable"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// The post-receive work and the cursor commit in one transaction: when the
// cursor's compare-and-set loses, the work rolls back with it (R7-29).
func TestConsumerWorkCommitsWithItsCursor(t *testing.T) {
	require.NoError(t, unittest.PrepareTestDatabase())
	ctx := db.DefaultContext
	c := newConsumer(newReplicator(t.TempDir(), newRegistry(0, 1, 1)))
	tok := consensus.Token{Cluster: consensus.ClusterID{7}, Epoch: 1, Term: 1, Index: 5}
	lineage := tok.Identity().String()
	batch := []reftable.Event{{Token: tok, Kind: reftable.EventRef, Repo: "a/b.git", Ref: "refs/heads/main"}}
	// The work's side effect is a row the test can see.
	c.work = func(ctx context.Context, evs []reftable.Event) error {
		return cluster_model.AdvanceCursor(ctx, "probe", cluster_model.Cursor{}, cluster_model.Cursor{Lineage: "work", Position: int64(evs[0].Token.Index)})
	}

	// Another dispatcher moved the cursor first: ours loses, the work rolls back.
	require.NoError(t, cluster_model.AdvanceCursor(ctx, cursorName(lineage), cluster_model.Cursor{}, cluster_model.Cursor{Lineage: lineage, Position: 9}))
	stale := &cluster_model.Cursor{}
	require.NoError(t, c.dispatch(ctx, lineage, stale, batch))
	probe, err := cluster_model.GetCursor(ctx, "probe")
	require.NoError(t, err)
	assert.Empty(t, probe.Lineage, "the work rolled back with the lost cursor")

	// With the current cursor, work and cursor commit together.
	fresh := &cluster_model.Cursor{Lineage: lineage, Position: 4}
	require.NoError(t, cluster_model.AdvanceCursor(ctx, cursorName(lineage), cluster_model.Cursor{Lineage: lineage, Position: 9}, *fresh))
	require.NoError(t, c.dispatch(ctx, lineage, fresh, batch))
	probe, _ = cluster_model.GetCursor(ctx, "probe")
	assert.Equal(t, "work", probe.Lineage)
	got, _ := cluster_model.GetCursor(ctx, cursorName(lineage))
	assert.EqualValues(t, 5, got.Position)

	// A failing work leaves the cursor where it was.
	c.work = func(context.Context, []reftable.Event) error { return errors.New("boom") }
	batch[0].Token.Index = 6
	batch[0].Token = consensus.Token{Cluster: tok.Cluster, Epoch: 1, Term: 1, Index: 6}
	require.Error(t, c.dispatch(ctx, lineage, fresh, batch))
	got, _ = cluster_model.GetCursor(ctx, cursorName(lineage))
	assert.EqualValues(t, 5, got.Position)
}

// A repository being created is known to the object channel through the
// table before its row commits in SQL (the auto-init push).
func TestRepoKnownThroughTheTableFirst(t *testing.T) {
	require.NoError(t, unittest.PrepareTestDatabase())
	ctx := db.DefaultContext
	r := newReplicator(t.TempDir(), newRegistry(0, 1, 1))
	ok, err := r.repoKnown(ctx, "nobody/new.git")
	require.NoError(t, err)
	assert.False(t, ok)

	data, err := reftable.Encode(reftable.KindRefTxn, &reftable.RefTxn{
		Repo: "nobody/new.git", Format: "sha1",
		Updates: []reftable.RefUpdate{{Ref: "HEAD", Symbolic: "refs/heads/main"}},
	})
	require.NoError(t, err)
	require.True(t, r.table.Apply(consensus.Entry{Term: 1, Index: 1, Data: data}).(reftable.Result).OK)
	ok, err = r.repoKnown(ctx, "nobody/new.git")
	require.NoError(t, err)
	assert.True(t, ok)

	// A fixture repository is known through SQL.
	ok, err = r.repoKnown(ctx, "user2/repo1.git")
	require.NoError(t, err)
	assert.True(t, ok)
}
