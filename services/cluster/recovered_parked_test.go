// Copyright 2026 The Forgejo Authors. All rights reserved.
// SPDX-License-Identifier: GPL-3.0-or-later

package cluster

import (
	"context"
	"math"
	"testing"
	"time"

	cluster_model "forgejo.org/models/cluster"
	"forgejo.org/models/db"
	repo_model "forgejo.org/models/repo"
	"forgejo.org/models/unittest"
	user_model "forgejo.org/models/user"
	"forgejo.org/modules/json"
	"forgejo.org/services/cluster/consensus"
	"forgejo.org/services/cluster/reftable"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// The clusters of these tests: the old one, whose lineage a recovery from
// an observer ended, and the new one it started with a new table.
var (
	recoveredOld = consensus.ClusterID{0xc0, 1}
	recoveredNew = consensus.ClusterID{0x1e, 2}
)

// recoveredApply applies one command to a table at index i of a cluster's
// lineage.
func recoveredApply(t *testing.T, tbl *reftable.Table, cluster consensus.ClusterID, i uint64, kind reftable.Kind, body any) {
	t.Helper()
	data, err := reftable.Encode(kind, body)
	require.NoError(t, err)
	res := tbl.Apply(consensus.Entry{
		Term: 1, Index: i, Kind: consensus.EntryCommand, Version: reftable.PayloadVersion, Data: data,
		Token: consensus.Token{Cluster: cluster, Epoch: 1, Term: 1, Index: i},
	}).(reftable.Result)
	require.True(t, res.OK, res.Reason)
}

// recoveredPush applies a push of a tag to repo, ID 1, at index i of a
// cluster's lineage, and returns its event.
func recoveredPush(t *testing.T, tbl *reftable.Table, cluster consensus.ClusterID, repo string, i uint64) reftable.Event {
	t.Helper()
	recoveredApply(t, tbl, cluster, i, reftable.KindRefTxn, &reftable.RefTxn{
		Repo: repo, RepoID: 1, Format: "sha1", PusherID: 2, PusherName: "user2", Time: time.Now().UnixNano(),
		Updates: []reftable.RefUpdate{{Ref: "refs/tags/recovered-parked", New: "65f1bf27bc3bf70f64657658635e66094edbcb4d"}},
	})
	for _, ev := range tbl.Events() {
		if ev.Kind == reftable.EventRef && ev.Token.Index == i && ev.Token.Cluster == cluster {
			return ev
		}
	}
	t.Fatalf("no event at %d", i)
	return reftable.Event{}
}

// recoveredPark parks an event as parkGroup writes it, after its attempts.
func recoveredPark(t *testing.T, ev reftable.Event, reason string) {
	t.Helper()
	b, err := json.Marshal(ev)
	require.NoError(t, err)
	require.NoError(t, cluster_model.ParkEvents(db.DefaultContext, []*cluster_model.ParkedEvent{{
		Lineage: ev.Token.Identity().String(), LogIndex: int64(ev.Token.Index), Repo: ev.Repo, RepoID: ev.RepoID,
		Event: string(b), Reason: reason, Attempts: maxAttempts,
	}}))
}

// recoveredConsumer is the consumer of the new cluster, whose table holds
// none of the old lineage's renames, recording the key each work is given.
func recoveredConsumer(t *testing.T) (*consumer, *Replicator, *[]string) {
	t.Helper()
	r := newReplicator(t.TempDir(), newRegistry(0, 1, 1))
	c := newConsumer(r)
	// The projection of the new lineage's events is not what is tested.
	c.projectedAt = func(string) uint64 { return math.MaxUint64 }
	worked := &[]string{}
	c.work = func(ctx context.Context, evs []reftable.Event) error {
		*worked = append(*worked, evs[0].Repo)
		return pushWorkWith(ctx, evs, r.table)
	}
	return c, r, worked
}

// recover --from-observer restores PostgreSQL at the checkpoint's L, its
// cluster_parked_event rows included, and starts a NEW table (init,
// bootstrap, import) that holds none of the old lineage's renames. A push
// of repository 1, parked in the old lineage under its name then
// (user2/oldname), the repository renamed afterwards to user2/repo1 before
// the checkpoint, failed at every retry after the recovery: eventLeadsTo
// follows the renames of the new table only, and took the rename for a
// reused ID. The repository's pushes of the new lineage were parked behind
// it, and the trim of the new lineage held below them, for good (#238).
// The rename's SQL, under the cut, left the redirect from the old name:
// it leads the push to user2/repo1, and the pushes behind it follow.
func TestRecoveredParkedPushOfARenamedRepoIsApplied(t *testing.T) {
	require.NoError(t, unittest.PrepareTestDatabase())
	ctx := db.DefaultContext

	// The old cluster: the push at 500, parked; the rename at 600, its SQL
	// (the row and the redirect from the old name) under the cut.
	oldTbl := reftable.New()
	ev := recoveredPush(t, oldTbl, recoveredOld, "user2/oldname.git", 500)
	recoveredApply(t, oldTbl, recoveredOld, 600, reftable.KindRepoRename, &reftable.RepoRename{From: "user2/oldname.git", To: "user2/repo1.git"})
	require.NoError(t, repo_model.NewRedirect(ctx, 2, 1, "oldname", "repo1"))
	recoveredPark(t, ev, "failed 5 attempts")
	// The old table follows the rename: the old cluster's retry passes.
	require.NoError(t, pushWorkWith(ctx, []reftable.Event{ev}, oldTbl))

	// After the recovery: the same SQL, a new table. The first push of the
	// repository in the new lineage comes before the retry, parked behind.
	c, r, worked := recoveredConsumer(t)
	nev := recoveredPush(t, r.table, recoveredNew, "user2/repo1.git", 7)
	newLineage := nev.Token.Identity().String()
	require.NoError(t, c.dispatch(ctx, newLineage, &cluster_model.Cursor{}, []reftable.Event{nev}))
	_, held, err := cluster_model.MinParkedIndex(ctx, newLineage)
	require.NoError(t, err)
	require.True(t, held, "parked behind the old lineage's push")

	applied, err := c.retryParked(ctx)
	require.NoError(t, err)
	rows, err := cluster_model.ListParked(ctx)
	require.NoError(t, err)
	for _, row := range rows {
		t.Logf("still parked: lineage=%.10s index=%d repo=%s reason=%q", row.Lineage, row.LogIndex, row.Repo, row.Reason)
	}
	assert.Equal(t, 2, applied, "the old lineage's push, then the one behind it")
	assert.Empty(t, rows)
	assert.Equal(t, []string{"user2/repo1.git", "user2/repo1.git"}, *worked, "the old push's work runs on the repository its ID holds now")
	_, held, err = cluster_model.MinParkedIndex(ctx, newLineage)
	require.NoError(t, err)
	assert.False(t, held, "the new lineage's trim is free")
}

// A push of the old lineage whose repository kept its name is retried and
// applied after the recovery as before, its key as it was.
func TestRecoveredParkedPushWithoutRenameIsApplied(t *testing.T) {
	require.NoError(t, unittest.PrepareTestDatabase())
	ctx := db.DefaultContext
	recoveredPark(t, recoveredPush(t, reftable.New(), recoveredOld, "user2/repo1.git", 500), "failed 5 attempts")

	c, _, worked := recoveredConsumer(t)
	applied, err := c.retryParked(ctx)
	require.NoError(t, err)
	assert.Equal(t, 1, applied)
	assert.Equal(t, []string{"user2/repo1.git"}, *worked)
	n, err := cluster_model.CountParked(ctx)
	require.NoError(t, err)
	assert.Zero(t, n)
}

// A push of the old lineage whose ID now names a repository SQL does not
// lead its key to (no redirect: the old cluster reissued the ID, or another
// repository took the old name since) is never applied there. It stays
// parked, and its error names the way out the operator has.
func TestRecoveredParkedPushOfAReusedIDStaysParked(t *testing.T) {
	require.NoError(t, unittest.PrepareTestDatabase())
	ctx := db.DefaultContext
	ev := recoveredPush(t, reftable.New(), recoveredOld, "user2/lost.git", 500)
	recoveredPark(t, ev, "failed 5 attempts")

	c, _, worked := recoveredConsumer(t)
	for range 2 {
		applied, err := c.retryParked(ctx)
		require.NoError(t, err)
		assert.Zero(t, applied)
	}
	assert.Equal(t, []string{"user2/lost.git", "user2/lost.git"}, *worked, "never moved to user2/repo1")
	rows, err := cluster_model.ListParked(ctx)
	require.NoError(t, err)
	require.Len(t, rows, 1)
	t.Logf("reason: %s", rows[0].Reason)
	assert.Contains(t, rows[0].Reason, "the ID was reused")
	assert.Contains(t, rows[0].Reason, "forgejo cluster parked resolve 500 --lineage "+ev.Token.Identity().String())
}

// In the table's own lineage, the table alone judges a reused ID, as
// before: a redirect in SQL does not move an event its renames do not
// lead to the repository.
func TestReusedIDOfTheTablesLineageIgnoresTheRedirects(t *testing.T) {
	require.NoError(t, unittest.PrepareTestDatabase())
	ctx := db.DefaultContext
	require.NoError(t, repo_model.NewRedirect(ctx, 2, 1, "oldname", "repo1"))

	c, r, worked := recoveredConsumer(t)
	ev := recoveredPush(t, r.table, recoveredNew, "user2/oldname.git", 5)
	recoveredPark(t, ev, "failed 5 attempts")
	applied, err := c.retryParked(ctx)
	require.NoError(t, err)
	assert.Zero(t, applied)
	assert.Equal(t, []string{"user2/oldname.git"}, *worked)
	rows, err := cluster_model.ListParked(ctx)
	require.NoError(t, err)
	require.Len(t, rows, 1)
	assert.Contains(t, rows[0].Reason, "the ID was reused")
	assert.NotContains(t, rows[0].Reason, "parked resolve", "the table's own lineage keeps its error")
}

// The redirects a rename, a transfer and an owner's rename leave lead an
// event's key to the repository; a key whose redirect another repository
// or account took, or never had one, leads nowhere.
func TestRedirectsLeadThroughRenamesTransfersAndOwnerRenames(t *testing.T) {
	require.NoError(t, unittest.PrepareTestDatabase())
	ctx := db.DefaultContext
	repo := unittest.AssertExistsAndLoadBean(t, &repo_model.Repository{ID: 1})
	require.Equal(t, "user2/repo1", repo.OwnerName+"/"+repo.LowerName)
	// A transfer from user4, and a rename of user2 from formeruser2.
	require.NoError(t, repo_model.NewRedirect(ctx, 4, 1, "repo1", "repo1"))
	require.NoError(t, db.Insert(ctx, &user_model.Redirect{LowerName: "formeruser2", RedirectUserID: 2}))
	// A rename from user2/taken, whose name a repository created since took:
	// its creation removed the redirect.
	require.NoError(t, repo_model.NewRedirect(ctx, 2, 1, "taken", "repo1"))
	require.NoError(t, repo_model.DeleteRedirect(ctx, 2, "taken"))

	for key, want := range map[string]bool{
		"user2/repo1.git":          true,  // its key now
		"user2/oldrepo1.git":       true,  // renamed (fixture redirect)
		"user4/repo1.git":          true,  // transferred
		"formeruser2/repo1.git":    true,  // its owner renamed
		"formeruser2/oldrepo1.git": true,  // its owner renamed, and itself
		"user2/lost.git":           false, // no redirect: a reused ID
		"user2/taken.git":          false, // the name taken again: SQL no longer says
		"user5/repo1.git":          false, // another owner, no redirect from it
		"nobody/repo1.git":         false, // an owner neither SQL nor its redirects know
		"user2":                    false,
	} {
		got, err := redirectsLead(ctx, key, repo)
		require.NoError(t, err, key)
		assert.Equal(t, want, got, key)
	}
}
