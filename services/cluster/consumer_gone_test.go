// Copyright 2026 The Forgejo Authors. All rights reserved.
// SPDX-License-Identifier: GPL-3.0-or-later

package cluster

import (
	"testing"

	"forgejo.org/models/db"
	"forgejo.org/models/unittest"
	"forgejo.org/services/cluster/consensus"
	"forgejo.org/services/cluster/reftable"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// applyAt applies one command to a table at index i.
func applyAt(t *testing.T, tbl *reftable.Table, i uint64, kind reftable.Kind, body any) {
	t.Helper()
	data, err := reftable.Encode(kind, body)
	require.NoError(t, err)
	res := tbl.Apply(consensus.Entry{
		Term: 1, Index: i, Kind: consensus.EntryCommand, Version: reftable.PayloadVersion, Data: data,
		Token: consensus.Token{Cluster: consensus.ClusterID{1}, Epoch: 1, Term: 1, Index: i},
	}).(reftable.Result)
	require.True(t, res.OK, res.Reason)
}

// The post-receive work of a push whose repository was deleted afterwards
// has nothing to land on: it is done, not failed, so the event is neither
// parked for good nor holding back the trim of the queue (R-D6). A
// repository the table never deleted, one still being created, keeps
// failing until its row commits.
func TestPushWorkOfADeletedRepositoryIsDone(t *testing.T) {
	require.NoError(t, unittest.PrepareTestDatabase())
	ctx := db.DefaultContext
	const sha = "65f1bf27bc3bf70f64657658635e66094edbcb4d"
	ev := func(repo string, i uint64) reftable.Event {
		return reftable.Event{
			Token: consensus.Token{Cluster: consensus.ClusterID{1}, Epoch: 1, Term: 1, Index: i},
			Kind:  reftable.EventRef, Repo: repo, RepoID: 99999, PusherID: 2, Ref: "refs/heads/main", New: sha,
		}
	}
	tbl := reftable.New()
	push := func(repo string) *reftable.RefTxn {
		return &reftable.RefTxn{Repo: repo, RepoID: 99999, Format: "sha1", PusherID: 2, Updates: []reftable.RefUpdate{{Ref: "refs/heads/main", New: sha}}}
	}

	// Pushed, then deleted.
	applyAt(t, tbl, 1, reftable.KindRefTxn, push("gone/a.git"))
	applyAt(t, tbl, 2, reftable.KindRepoDelete, &reftable.RepoDelete{Repo: "gone/a.git", RepoID: 99999})
	require.NoError(t, pushWorkWith(ctx, []reftable.Event{ev("gone/a.git", 1)}, tbl))

	// Pushed, renamed, then deleted under its new name.
	applyAt(t, tbl, 3, reftable.KindRefTxn, push("gone/b.git"))
	applyAt(t, tbl, 4, reftable.KindRepoRename, &reftable.RepoRename{From: "gone/b.git", To: "gone/c.git"})
	applyAt(t, tbl, 5, reftable.KindRepoDelete, &reftable.RepoDelete{Repo: "gone/c.git", RepoID: 99999})
	require.NoError(t, pushWorkWith(ctx, []reftable.Event{ev("gone/b.git", 3)}, tbl))

	// No deletion after the event: the row may not be committed yet.
	applyAt(t, tbl, 6, reftable.KindRefTxn, push("new/d.git"))
	err := pushWorkWith(ctx, []reftable.Event{ev("new/d.git", 6)}, tbl)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "does not exist")

	// A deletion before the event, the key written again since: same.
	applyAt(t, tbl, 7, reftable.KindRepoDelete, &reftable.RepoDelete{Repo: "new/d.git"})
	applyAt(t, tbl, 8, reftable.KindRefTxn, &reftable.RefTxn{Repo: "new/d.git", RepoID: 99999, Format: "sha1", PusherID: 2, Updates: []reftable.RefUpdate{{Ref: "refs/heads/main", New: sha, ExpectRev: 2}}})
	require.Error(t, pushWorkWith(ctx, []reftable.Event{ev("new/d.git", 8)}, tbl))

	// Without the table nothing is known: the work keeps failing.
	require.Error(t, pushWork(ctx, []reftable.Event{ev("gone/a.git", 1)}))
}
