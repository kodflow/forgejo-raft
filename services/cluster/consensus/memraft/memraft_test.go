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

// A raise is refused while a member does not answer, whatever it announced,
// as the engine holds every member to its vote window (kitsunium/sdk#665),
// and goes through once it answers again.
func TestARaiseWaitsForEveryMemberToAnswer(t *testing.T) {
	c := New()
	a := c.Add("node-a", [32]byte{1}, reftable.New())
	c.Add("node-b", [32]byte{2}, reftable.New())
	silent := c.Add("node-c", [32]byte{3}, reftable.New())
	c.SetDown(silent, true)
	st := a.Status()
	pre := consensus.Precondition{Identity: st.Identity, Generation: st.ClusterGeneration}
	require.ErrorIs(t, a.RaiseClusterVersion(t.Context(), "op:test", pre, ProtocolMax), consensus.ErrVersionUnsupported)
	require.Zero(t, a.Status().ClusterVersion)

	c.SetDown(silent, false)
	require.NoError(t, a.RaiseClusterVersion(t.Context(), "op:test", pre, ProtocolMax))
	require.Equal(t, ProtocolMax, a.Status().ClusterVersion)
}
