// Copyright 2026 The Forgejo Authors. All rights reserved.
// SPDX-License-Identifier: GPL-3.0-or-later

package memraft

import (
	"context"
	"testing"
	"time"

	"forgejo.org/services/cluster/consensus"
	"forgejo.org/services/cluster/reftable"

	"github.com/stretchr/testify/require"
)

func closedWithin(t *testing.T, ch <-chan consensus.Status) {
	t.Helper()
	deadline := time.After(2 * time.Second)
	for {
		select {
		case _, ok := <-ch:
			if !ok {
				return
			}
		case <-deadline:
			t.Fatal("the watch channel was not closed")
		}
	}
}

// Watch's channel closes when its context ends, and when the node closes.
func TestWatchClosesItsChannel(t *testing.T) {
	c := New()
	n := c.Add("node-a", [32]byte{1}, reftable.New())
	ctx, cancel := context.WithCancel(context.Background())
	ch := n.Watch(ctx)
	cancel()
	closedWithin(t, ch)

	ch = n.Watch(context.Background())
	require.NoError(t, n.Close(context.Background()))
	closedWithin(t, ch)
	require.NoError(t, n.Close(context.Background()), "a second close is harmless")
	closedWithin(t, n.Watch(context.Background()))
}
