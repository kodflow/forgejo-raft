// Copyright 2026 The Forgejo Authors. All rights reserved.
// SPDX-License-Identifier: GPL-3.0-or-later

package cluster

import (
	"context"
	"encoding/hex"
	"fmt"
	"testing"

	cluster_model "forgejo.org/models/cluster"
	"forgejo.org/models/db"
	"forgejo.org/models/unittest"
	"forgejo.org/services/cluster/reftable"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// The parked events are retried in the order they were parked, which is
// the log's, across lineages: after a Recover, a newer push of a
// repository does not pass an older one still parked, whatever the order
// of the two lineages' random ids (#101, TestHuntR7ProveLineageOrder).
func TestParkedEventsAreRetriedOldestFirstAcrossLineages(t *testing.T) {
	for _, newer := range []string{"1e", "f0"} {
		t.Run(newer, func(t *testing.T) {
			require.NoError(t, unittest.PrepareTestDatabase())
			ctx := db.DefaultContext
			lineage := func(prefix string) string {
				b, _ := hex.DecodeString(prefix)
				id := make([]byte, 16)
				copy(id, b)
				return fmt.Sprintf("%x/1", id)
			}
			old, recovered := lineage("c0"), lineage(newer)
			event := func(index int) string {
				return fmt.Sprintf(`{"tok":{},"k":"ref","repo":"o/r.git","new":"%d"}`, index)
			}
			require.NoError(t, cluster_model.ParkEvents(ctx, []*cluster_model.ParkedEvent{{Lineage: old, LogIndex: 500, Repo: "o/r.git", Event: event(500), Reason: "before the disaster"}}))
			require.NoError(t, cluster_model.ParkEvents(ctx, []*cluster_model.ParkedEvent{{Lineage: recovered, LogIndex: 501, Repo: "o/r.git", Event: event(501), Reason: "behind an earlier parked event of the repository"}}))

			c := newConsumer(newReplicator(t.TempDir(), newRegistry(0, 1, 1)))
			var order []string
			c.work = func(_ context.Context, evs []reftable.Event) error {
				order = append(order, evs[0].New)
				return nil
			}
			applied, err := c.retryParked(ctx)
			require.NoError(t, err)
			assert.Equal(t, 2, applied)
			assert.Equal(t, []string{"500", "501"}, order)
		})
	}
}
