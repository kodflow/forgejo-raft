// Copyright 2026 The Forgejo Authors. All rights reserved.
// SPDX-License-Identifier: GPL-3.0-or-later

package cluster

import (
	"context"
	"errors"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	cluster_model "forgejo.org/models/cluster"
	"forgejo.org/modules/clusterhook"
	"forgejo.org/modules/test"
	"forgejo.org/services/cluster/consensus"
	"forgejo.org/services/cluster/reftable"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// A repository the leader cannot project does not hold back the SQL of
// every other repository: past projectionStallBound its event is parked,
// never dropped, and applied once its repository projects, and the stall is visible meanwhile.
func TestAStalledProjectionDoesNotHoldBackOtherRepositories(t *testing.T) {
	defer test.MockVariableValue(&projectionStallBound, 500*time.Millisecond)()
	tc := newTestCluster(t, "node-a", "node-b", "node-c")
	leader := tc.nodes[0]
	// A native git transaction that does not end keeps the leader from
	// projecting o/stall.git.
	require.NoError(t, leader.r.registry.register("held", "o/stall.git", nil, os.Getpid()))

	// The stall is reported while it lasts.
	var seen atomic.Value
	done := make(chan struct{})
	defer close(done)
	go func() {
		for {
			select {
			case <-done:
				return
			case <-time.After(5 * time.Millisecond):
			}
			if repo, _ := leader.r.consumer.Stalled(); repo != "" {
				seen.Store(repo)
			}
		}
	}()

	work := t.TempDir()
	run(t, work, "init", "-q")
	c1 := commit(t, work, "one")
	zero := strings.Repeat("0", 40)
	require.True(t, tc.pushVia(tc.nodes[1], work, "o/stall.git", "refs/heads/main", zero, c1).OK)
	require.True(t, tc.pushVia(tc.nodes[1], work, "o/other.git", "refs/heads/main", zero, c1).OK)

	require.Eventually(t, func() bool {
		leader.work.mu.Lock()
		defer leader.work.mu.Unlock()
		for _, ev := range leader.work.events {
			if ev.Repo == "o/other.git" {
				return true
			}
		}
		return false
	}, 5*time.Second, 10*time.Millisecond, "the next repository's event is dispatched")
	leader.work.mu.Lock()
	for _, ev := range leader.work.events {
		assert.NotEqual(t, "o/stall.git", ev.Repo, "the stalled event was skipped, not run unprojected")
	}
	leader.work.mu.Unlock()
	assert.Equal(t, "o/stall.git", seen.Load(), "the stall was reported")
	repo, _ := leader.r.consumer.Stalled()
	assert.Empty(t, repo, "the stall clears once the queue moves")

	// The stalled event was parked, not dropped; once its repository
	// projects, a retry runs its work, once.
	rows, err := tc.parked.list(context.Background())
	require.NoError(t, err)
	require.Len(t, rows, 1)
	assert.Equal(t, "o/stall.git", rows[0].Repo)
	leader.r.registry.release("held")
	leader.r.projector.signal()
	require.Eventually(t, func() bool {
		n, err := leader.r.consumer.retryParked(context.Background())
		return err == nil && n == 1
	}, 10*time.Second, 50*time.Millisecond, "the parked event is applied once projected")
	n, err := leader.r.consumer.retryParked(context.Background())
	require.NoError(t, err)
	assert.Zero(t, n)
	leader.work.mu.Lock()
	stalled := 0
	for _, ev := range leader.work.events {
		if ev.Repo == "o/stall.git" {
			stalled++
		}
	}
	leader.work.mu.Unlock()
	assert.Equal(t, 1, stalled, "the parked event's work ran exactly once")
}

// A batch whose second group fails is replayed; the side effects the first
// group deferred (a pull request test, a webhook delivery) run once, after
// the attempt that commits, never on the rolled-back ones.
func TestReplayedBatchRunsSideEffectsOnce(t *testing.T) {
	var effects []string
	failB := 2
	c := &consumer{
		work: func(ctx context.Context, evs []reftable.Event) error {
			repo := evs[0].Repo
			clusterhook.Defer(ctx, func() { effects = append(effects, repo) })
			if repo == "o/b.git" && failB > 0 {
				failB--
				return errors.New("transient")
			}
			return nil
		},
		tx:       func(ctx context.Context, f func(context.Context) error) error { return f(ctx) },
		advance:  func(context.Context, string, cluster_model.Cursor, cluster_model.Cursor) error { return nil },
		failures: map[uint64]int{}, reasons: map[uint64]string{}, seen: map[string]int64{}, lastAck: map[string]uint64{},
	}
	batch := []reftable.Event{
		{Token: consensus.Token{Index: 1}, Kind: "ref", Repo: "o/a.git"},
		{Token: consensus.Token{Index: 2}, Kind: "ref", Repo: "o/b.git"},
	}
	cur := &cluster_model.Cursor{}
	for range 2 {
		require.Error(t, c.dispatch(context.Background(), "lin", cur, batch))
		assert.Empty(t, effects, "nothing runs for a rolled-back attempt")
	}
	require.NoError(t, c.dispatch(context.Background(), "lin", cur, batch))
	assert.Equal(t, []string{"o/a.git", "o/b.git"}, effects, "each once, after the commit")
}

// Events of one repository pushed by two users are two groups: the one that
// keeps failing is parked after maxAttempts and takes nothing down with it;
// the other user's later push to the same repository is parked behind it,
// never dropped, so the repository's work keeps its order (a branch
// deletion applied after its re-creation would leave a live branch marked
// deleted), and both run, in order, once the first one's work succeeds.
func TestAFailingPusherDoesNotSkipAnotherPushersEvents(t *testing.T) {
	var ran [][]int64
	broken := true
	c := &consumer{
		work: func(ctx context.Context, evs []reftable.Event) error {
			var pushers []int64
			for _, ev := range evs {
				pushers = append(pushers, ev.PusherID)
			}
			if evs[0].PusherID == 7 && broken {
				return errors.New("always")
			}
			ran = append(ran, pushers)
			return nil
		},
		tx:       func(ctx context.Context, f func(context.Context) error) error { return f(ctx) },
		advance:  func(context.Context, string, cluster_model.Cursor, cluster_model.Cursor) error { return nil },
		failures: map[uint64]int{}, reasons: map[uint64]string{}, seen: map[string]int64{}, lastAck: map[string]uint64{},
	}
	parked := &memParked{}
	parked.wire(c)
	batch := []reftable.Event{
		{Token: consensus.Token{Index: 1}, Kind: "ref", Repo: "o/a.git", Ref: "refs/tags/v1", PusherID: 7},
		{Token: consensus.Token{Index: 2}, Kind: "ref", Repo: "o/a.git", Ref: "refs/heads/main", PusherID: 3},
		{Token: consensus.Token{Index: 3}, Kind: "ref", Repo: "o/b.git", Ref: "refs/heads/main", PusherID: 3},
	}
	cur := &cluster_model.Cursor{}
	for range maxAttempts {
		require.Error(t, c.dispatch(context.Background(), "lin", cur, batch))
	}
	require.NoError(t, c.dispatch(context.Background(), "lin", cur, batch))
	assert.Equal(t, [][]int64{{3}}, ran, "another repository's push ran")
	assert.Equal(t, []int64{1, 2}, parked.indexes(), "the failing push is parked, the later one of its repository behind it")

	broken = false
	n, err := c.retryParked(context.Background())
	require.NoError(t, err)
	assert.Equal(t, 2, n)
	assert.Equal(t, [][]int64{{3}, {7}, {3}}, ran, "both ran once, in their order")
}
