// Copyright 2026 The Forgejo Authors. All rights reserved.
// SPDX-License-Identifier: GPL-3.0-or-later

package cluster

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"forgejo.org/models/db"
	"forgejo.org/models/unittest"
	"forgejo.org/modules/setting"
	"forgejo.org/services/cluster/consensus"
	"forgejo.org/services/cluster/consensus/memraft"
	"forgejo.org/services/cluster/reftable"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Reconcile classifies what SQL holds beyond the table: branch rows are
// regenerable, a release whose tag the table lacks is not and names the
// operator's action (ADR 0152 D17, R7-30).
func TestReconcileClassifiesRows(t *testing.T) {
	require.NoError(t, unittest.PrepareTestDatabase())
	ctx := db.DefaultContext
	r := newReplicator(setting.RepoRootPath, newRegistry(0, 1, 1))
	dir := filepath.Join(setting.RepoRootPath, "user2", "repo1.git")
	refs, err := readGitRefs(context.Background(), dir)
	require.NoError(t, err)
	master := refs["refs/heads/master"].SHA
	require.NotEmpty(t, master)

	// The table holds master only.
	data, err := reftable.Encode(reftable.KindRefTxn, &reftable.RefTxn{
		Repo: "user2/repo1.git", RepoID: 1, Format: "sha1",
		Updates: []reftable.RefUpdate{{Ref: "refs/heads/master", New: master}},
	})
	require.NoError(t, err)
	res := r.table.Apply(consensus.Entry{Term: 1, Index: 1, Data: data}).(reftable.Result)
	require.True(t, res.OK, res.Reason)

	ms, err := r.Reconcile(ctx, false)
	require.NoError(t, err)
	byKind := map[string][]Mismatch{}
	for _, m := range ms {
		assert.Equal(t, "user2/repo1.git", m.Repo)
		assert.False(t, m.Applied, "a dry run changes nothing")
		byKind[m.Kind] = append(byKind[m.Kind], m)
	}
	for _, m := range byKind["branch"] {
		assert.NotEqual(t, "master", m.Ref)
		assert.True(t, m.Regenerable)
	}
	require.NotEmpty(t, byKind["release"], "fixture releases name tags the table lacks")
	for _, m := range byKind["release"] {
		assert.False(t, m.Regenerable)
		assert.Equal(t, ActionReleaseNoTag, m.Action)
	}
}

// Reconcile with apply rebuilds SQL rows from this node's table: a table
// that lags the cluster would bring back a branch the cluster deleted. The
// apply runs only once a commit boundary a quorum confirmed is applied
// here; a node that cannot confirm one changes nothing.
func TestReconcileApplyNeedsAConfirmedTable(t *testing.T) {
	require.NoError(t, unittest.PrepareTestDatabase())
	mem, rs := NewForTesting(setting.RepoRootPath, "node-a", "node-b", "node-c")
	r := rs[0]
	for _, other := range rs[1:] {
		mem.SetDown(other.Engine().(*memraft.Node), true)
	}
	ctx, cancel := context.WithTimeout(db.DefaultContext, time.Second)
	defer cancel()
	_, err := r.Reconcile(ctx, true)
	require.Error(t, err)

	_, err = r.Reconcile(db.DefaultContext, false)
	require.NoError(t, err, "a dry run reads the local table as it is")
}
