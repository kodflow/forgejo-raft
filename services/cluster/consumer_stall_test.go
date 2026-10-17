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
// every other repository: past projectionStallBound its event is skipped
// with the reconcile hint, and the stall is visible meanwhile.
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
		failures: map[uint64]int{}, lastAck: map[string]uint64{},
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
