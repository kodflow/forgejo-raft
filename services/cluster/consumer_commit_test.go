// Copyright 2026 The Forgejo Authors. All rights reserved.
// SPDX-License-Identifier: GPL-3.0-or-later

package cluster

import (
	"context"
	"errors"
	"testing"

	cluster_model "forgejo.org/models/cluster"
	"forgejo.org/modules/clusterhook"
	"forgejo.org/services/cluster/reftable"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// A COMMIT whose outcome is unknown: the transaction ran, then the
// connection reported an error. landed says whether the database kept it.
func unknownCommitTx(landed bool) func(ctx context.Context, f func(context.Context) error) error {
	once := true
	return func(ctx context.Context, f func(context.Context) error) error {
		if !once {
			return f(ctx)
		}
		once = false
		if landed {
			if err := f(ctx); err != nil {
				return err
			}
		}
		return errors.New("driver: bad connection")
	}
}

// The effects after commit of a batch whose COMMIT committed but returned
// an error run exactly once, as those of a batch that rolled back and is
// replayed (#103, TestHuntR7ProveUcfx).
func TestEffectsOfACommitWithAnUnknownOutcomeRunOnce(t *testing.T) {
	for _, landed := range []bool{true, false} {
		store, lg, parked := newSQLStore(), &memLog{}, &memParked{}
		lg.events = []reftable.Event{{Token: testToken(1), Kind: reftable.EventRef, Repo: "o/r.git", Ref: "refs/heads/main", PusherID: 1}}
		effects := 0
		c := unitConsumer(store, lg, parked, func(ctx context.Context, _ []reftable.Event) error {
			clusterhook.Defer(ctx, func() { effects++ })
			return nil
		})
		c.tx = unknownCommitTx(landed)
		for range 3 {
			_ = c.pass(context.Background())
		}
		cur, _ := store.get(context.Background(), cursorName(testToken(1).Identity().String()))
		assert.EqualValues(t, 1, cur.Position, "landed %t", landed)
		assert.Equal(t, 1, effects, "landed %t: the effects run once", landed)
	}
}

// Same for a parked entry: its rows went with the work, so its effects run.
func TestEffectsOfARetriedParkedEntryWithAnUnknownOutcomeRunOnce(t *testing.T) {
	for _, landed := range []bool{true, false} {
		store, lg, parked := newSQLStore(), &memLog{}, &memParked{}
		effects := 0
		c := unitConsumer(store, lg, parked, func(ctx context.Context, _ []reftable.Event) error {
			clusterhook.Defer(ctx, func() { effects++ })
			return nil
		})
		lin := testToken(1).Identity().String()
		require.NoError(t, parked.park(context.Background(), []*cluster_model.ParkedEvent{
			{Lineage: lin, LogIndex: 4, Repo: "o/r.git", Event: `{"tok":{},"k":"ref","repo":"o/r.git"}`, Reason: "boom", Attempts: maxAttempts},
		}))
		// The memory transaction cannot roll back the unpark: one that did
		// not land never ran it.
		c.tx = unknownCommitTx(landed)
		for range 2 {
			_, err := c.retryParked(context.Background())
			require.NoError(t, err)
		}
		assert.Empty(t, parked.indexes(), "landed %t", landed)
		assert.Equal(t, 1, effects, "landed %t: the effects run once", landed)
	}
}
