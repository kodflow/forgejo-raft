// Copyright 2026 The Forgejo Authors. All rights reserved.
// SPDX-License-Identifier: GPL-3.0-or-later

package cluster

import (
	"context"
	"errors"
	"testing"
	"time"

	cluster_model "forgejo.org/models/cluster"
	"forgejo.org/models/db"
	"forgejo.org/models/unittest"
	user_model "forgejo.org/models/user"
	"forgejo.org/modules/test"
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

// A ref Forgejo wrote itself outside receive-pack (a tag made through the
// API) carries no pusher: its request already wrote the rows and sent the
// notifications, and the post-receive work does nothing. It used to fail on
// the missing user, five times, then skip its whole batch.
func TestPushWorkLeavesPusherlessEventsToTheirRequest(t *testing.T) {
	require.NoError(t, unittest.PrepareTestDatabase())
	ev := reftable.Event{
		Token: consensus.Token{Index: 9}, Kind: reftable.EventRef, Repo: "user2/repo1.git", RepoID: 1,
		Ref: "refs/tags/api-tag", New: "65f1bf27bc3bf70f64657658635e66094edbcb4d",
	}
	require.NoError(t, pushWork(db.DefaultContext, []reftable.Event{ev}))
}

// An event whose repository ID now names another repository is refused:
// the row it was written for was lost (a failover that dropped PostgreSQL's
// tail, a recovery to an earlier position) and the sequence reissued its ID.
// Its branch rows, webhooks and CI used to land on the other repository.
func TestPushWorkRefusesAReusedRepoID(t *testing.T) {
	require.NoError(t, unittest.PrepareTestDatabase())
	ev := reftable.Event{
		Token: consensus.Token{Index: 9}, Kind: reftable.EventRef, Repo: "alice/lost.git", RepoID: 1, PusherID: 2,
		Ref: "refs/tags/v1", New: "65f1bf27bc3bf70f64657658635e66094edbcb4d",
	}
	err := pushWork(db.DefaultContext, []reftable.Event{ev})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "reused")
}

// A rename after the event leads it to the repository's new key; a rename
// before it, or none, does not.
func TestEventLeadsToFollowsLaterRenames(t *testing.T) {
	marks := map[string]struct {
		from string
		idx  uint64
	}{
		"bob/c.git":   {"bob/b.git", 20},
		"bob/b.git":   {"alice/a.git", 10},
		"carol/x.git": {"carol/y.git", 3},
		// A rename undone: y -> z at 30, z -> y at 40.
		"dave/y.git": {"dave/z.git", 40},
		"dave/z.git": {"dave/y.git", 30},
	}
	rf := func(k string) (string, uint64, bool) { m, ok := marks[k]; return m.from, m.idx, ok }
	at := func(repo string, i uint64) reftable.Event {
		return reftable.Event{Repo: repo, Token: consensus.Token{Index: i}}
	}
	assert.True(t, eventLeadsTo(at("bob/c.git", 25), "bob/c.git", rf))
	assert.True(t, eventLeadsTo(at("alice/a.git", 5), "bob/c.git", rf))
	assert.True(t, eventLeadsTo(at("bob/b.git", 15), "bob/c.git", rf))
	assert.False(t, eventLeadsTo(at("alice/a.git", 15), "bob/c.git", rf), "renamed before the event")
	assert.False(t, eventLeadsTo(at("carol/y.git", 5), "carol/x.git", rf), "renamed before the event")
	assert.False(t, eventLeadsTo(at("alice/lost.git", 5), "bob/c.git", rf))
	assert.False(t, eventLeadsTo(at("alice/a.git", 5), "bob/c.git", nil))
	assert.False(t, eventLeadsTo(at("dave/q.git", 1), "dave/y.git", rf), "a rename and its undo do not loop")
}

// A pusher ID that names an account created after the push was reissued:
// the push is not credited to that account.
func TestPushWorkRefusesAReusedPusherID(t *testing.T) {
	require.NoError(t, unittest.PrepareTestDatabase())
	ctx := db.DefaultContext
	u := unittest.AssertExistsAndLoadBean(t, &user_model.User{ID: 2})
	ev := reftable.Event{
		Token: consensus.Token{Index: 9}, Kind: reftable.EventRef, Repo: "user2/repo1.git", RepoID: 1, PusherID: 2,
		Ref: "refs/tags/v1", New: "65f1bf27bc3bf70f64657658635e66094edbcb4d",
		Time: time.Unix(int64(u.CreatedUnix), 0).Add(-time.Hour).UnixNano(),
	}
	err := pushWork(ctx, []reftable.Event{ev})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "pusher ID 2")

	assert.False(t, pusherReissued(ctx, u, reftable.Event{Time: time.Unix(int64(u.CreatedUnix), 0).Add(-time.Minute).UnixNano()}), "within the clock skew")
	assert.False(t, pusherReissued(ctx, u, reftable.Event{Time: time.Unix(int64(u.CreatedUnix), 0).Add(time.Hour).UnixNano()}))
}

// From payload version 4 the event names its pusher, and the name decides,
// whatever the clocks: a node whose clock ran an hour behind the one that
// created the account no longer parks that account's push, and an ID now
// held by another account is caught even with the clocks agreeing.
func TestPushWorkTellsAReusedPusherIDByName(t *testing.T) {
	require.NoError(t, unittest.PrepareTestDatabase())
	ctx := db.DefaultContext
	u := unittest.AssertExistsAndLoadBean(t, &user_model.User{ID: 2})
	behind := time.Unix(int64(u.CreatedUnix), 0).Add(-time.Hour).UnixNano()
	ev := reftable.Event{
		Token: consensus.Token{Index: 9}, Kind: reftable.EventRef, Repo: "user2/repo1.git", RepoID: 1, PusherID: 2,
		Ref: "refs/tags/v-name", New: "65f1bf27bc3bf70f64657658635e66094edbcb4d", Time: behind, PusherName: "user2",
	}
	assert.False(t, pusherReissued(ctx, u, ev), "the same name, a clock an hour behind")
	ev.PusherName = "USER2"
	assert.False(t, pusherReissued(ctx, u, ev), "names compare without case")
	if err := pushWork(ctx, []reftable.Event{ev}); err != nil {
		// The fixture repository lacks the tag: the work fails further on,
		// past the pusher check.
		assert.NotContains(t, err.Error(), "pusher ID")
	}

	ev.PusherName = "someone-else"
	ev.Time = time.Unix(int64(u.CreatedUnix), 0).Add(time.Hour).UnixNano()
	assert.True(t, pusherReissued(ctx, u, ev), "another account's name, the clocks agreeing")
	err := pushWork(ctx, []reftable.Event{ev})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "pusher ID 2")

	// A rename since the push: the old name redirects to the account.
	u1 := unittest.AssertExistsAndLoadBean(t, &user_model.User{ID: 1})
	assert.False(t, pusherReissued(ctx, u1, reftable.Event{PusherName: "olduser1", Time: behind}))
	assert.True(t, pusherReissued(ctx, u, reftable.Event{PusherName: "olduser1", Time: behind}), "a redirect to another account")
}

// A parked event's row commits with the cursor that moves past it, or not
// at all; `forgejo cluster reconcile --apply` retries it, and its work and
// the removal of its row commit together, exactly once (finding 6).
func TestParkedEventCommitsWithItsCursorAndReconcileAppliesIt(t *testing.T) {
	require.NoError(t, unittest.PrepareTestDatabase())
	ctx := db.DefaultContext
	// Reconcile --apply runs on a node whose table a quorum confirmed.
	_, reps := NewForTesting(t.TempDir(), "node-a")
	r := reps[0]
	require.Eventually(t, func() bool { return r.Engine().Status().Role == consensus.RoleLeader }, 5*time.Second, 10*time.Millisecond)
	c := newConsumer(r)
	r.consumer = c
	tok := consensus.Token{Cluster: consensus.ClusterID{8}, Epoch: 1, Term: 1, Index: 5}
	lineage := tok.Identity().String()
	batch := []reftable.Event{{Token: tok, Kind: reftable.EventRef, Repo: "a/b.git", Ref: "refs/heads/main", PusherID: 1}}
	broken := true
	c.work = func(ctx context.Context, evs []reftable.Event) error {
		if broken {
			return errors.New("boom")
		}
		return cluster_model.AdvanceCursor(ctx, "probe-parked", cluster_model.Cursor{}, cluster_model.Cursor{Lineage: "work", Position: int64(evs[0].Token.Index)})
	}
	c.failures[5], c.reasons[5] = maxAttempts, "boom"

	// The cursor's compare-and-set loses: the parked row rolls back with it.
	require.NoError(t, cluster_model.AdvanceCursor(ctx, cursorName(lineage), cluster_model.Cursor{}, cluster_model.Cursor{Lineage: lineage, Position: 1}))
	require.NoError(t, c.dispatch(ctx, lineage, &cluster_model.Cursor{}, batch))
	n, err := cluster_model.CountParked(ctx)
	require.NoError(t, err)
	assert.Zero(t, n, "no row without its cursor")

	// With the current cursor, both commit.
	require.NoError(t, c.dispatch(ctx, lineage, &cluster_model.Cursor{Lineage: lineage, Position: 1}, batch))
	got, _ := cluster_model.GetCursor(ctx, cursorName(lineage))
	assert.EqualValues(t, 5, got.Position)
	rows, err := cluster_model.ListParked(ctx)
	require.NoError(t, err)
	require.Len(t, rows, 1)
	assert.Equal(t, "boom", rows[0].Reason)
	minIdx, ok, err := cluster_model.MinParkedIndex(ctx, lineage)
	require.NoError(t, err)
	assert.True(t, ok)
	assert.EqualValues(t, 5, minIdx, "the trim stops below it")

	// Reconcile reports it; with apply and the work still failing, it stays.
	ms, err := r.Reconcile(ctx, true)
	require.NoError(t, err)
	require.Len(t, ms, 1)
	assert.Equal(t, ParkedEvent, ms[0].Category)
	assert.False(t, ms[0].Applied)
	rows, _ = cluster_model.ListParked(ctx)
	require.Len(t, rows, 1)
	assert.Equal(t, maxAttempts+1, rows[0].Attempts, "the failed retry is counted")

	broken = false
	ms, err = r.Reconcile(ctx, true)
	require.NoError(t, err)
	require.Len(t, ms, 1)
	assert.True(t, ms[0].Applied)
	n, _ = cluster_model.CountParked(ctx)
	assert.Zero(t, n)
	probe, _ := cluster_model.GetCursor(ctx, "probe-parked")
	assert.EqualValues(t, 5, probe.Position, "the work committed")

	// Applied once: nothing is left to run.
	ms, err = r.Reconcile(ctx, true)
	require.NoError(t, err)
	assert.Empty(t, ms)
}

// The operator's path for an event that can never succeed: listed with its
// error, resolved only with a reason, recorded in the replicated audit and
// in cluster_parked_resolution with its payload; the trim is then free.
func TestResolveParkedIsAuditedAndFreesTheTrim(t *testing.T) {
	require.NoError(t, unittest.PrepareTestDatabase())
	ctx := db.DefaultContext
	_, reps := NewForTesting(t.TempDir(), "node-a")
	r := reps[0]
	require.Eventually(t, func() bool { return r.Engine().Status().Role == consensus.RoleLeader }, 5*time.Second, 10*time.Millisecond)
	require.NoError(t, cluster_model.ParkEvents(ctx, []*cluster_model.ParkedEvent{
		{Lineage: "lin", LogIndex: 12, Repo: "o/a.git", Event: `{"repo":"o/a.git"}`, Reason: "repository ID 3 is o/b: the ID was reused", Attempts: 5},
	}))
	ps, err := r.ListParked(ctx)
	require.NoError(t, err)
	require.Len(t, ps, 1)
	assert.Equal(t, "repository ID 3 is o/b: the ID was reused", ps[0].Error)
	v := r.View(ctx)
	assert.EqualValues(t, 1, v.ParkedEvents)
	require.Len(t, v.Parked, 1, "the administration lists it")

	op := consensus.Operator("local-uid:1000")
	_, err = r.ResolveParked(ctx, op, "", 12, "")
	require.ErrorIs(t, err, cluster_model.ErrResolveNeedsReason)
	_, err = r.ResolveParked(ctx, op, "", 12, "repository recreated, push replayed")
	require.NoError(t, err)

	_, ok, err := cluster_model.MinParkedIndex(ctx, "lin")
	require.NoError(t, err)
	assert.False(t, ok, "nothing holds the trim any more")
	found := false
	for _, a := range r.table.Audit() {
		if a.Action == string(ActionResolveParked) && a.Detail == "repository recreated, push replayed" && a.Operator == string(op) {
			found = true
		}
	}
	assert.True(t, found, "the resolution is in the replicated audit")
}

// A failed lookup of the pusher is retried, never passed over: the lookup
// that PushUpdatesNow repeats checks only that the account exists, not
// that it is the one that pushed (CodeRabbit on #17 and #120).
func TestPushWorkRetriesAFailedPusherLookup(t *testing.T) {
	require.NoError(t, unittest.PrepareTestDatabase())
	ctx := db.DefaultContext
	u := unittest.AssertExistsAndLoadBean(t, &user_model.User{ID: 2})
	ev := reftable.Event{
		Token: consensus.Token{Cluster: consensus.ClusterID{9}, Epoch: 1, Term: 1, Index: 9}, Kind: reftable.EventRef, Repo: "user2/repo1.git", RepoID: 1, PusherID: 2,
		Ref: "refs/tags/v1", New: "65f1bf27bc3bf70f64657658635e66094edbcb4d",
		Time: time.Unix(int64(u.CreatedUnix), 0).Add(-time.Hour).UnixNano(),
	}
	defer test.MockVariableValue(&getPusher, func(context.Context, int64) (*user_model.User, error) {
		return nil, errors.New("connection reset")
	})()
	err := pushWork(ctx, []reftable.Event{ev})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "connection reset")
}
