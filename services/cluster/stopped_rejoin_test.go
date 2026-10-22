// Copyright 2026 The Forgejo Authors. All rights reserved.
// SPDX-License-Identifier: GPL-3.0-or-later

package cluster

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"forgejo.org/services/cluster/consensus"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// stoppedEngine is an engine that lost its membership, stopped or not.
type stoppedEngine struct {
	consensus.Engine
	id      consensus.NodeID
	stopped bool
}

func (e *stoppedEngine) Status() consensus.Status { return consensus.Status{ID: e.id} }

func (e *stoppedEngine) Membership() consensus.Membership { return consensus.Membership{} }

func (e *stoppedEngine) Health() consensus.Health {
	if e.stopped {
		return consensus.Health{Stopped: true, StopReason: "STATE_MACHINE_FAILED"}
	}
	return consensus.Health{}
}

// A stopped engine lists no membership, which reads as a retired id: it
// never rejoins under a new id, its stop belongs to the operator (#67,
// TestHuntR3ProveStoppedNodeRejoinsPastStopMarker). A running engine whose
// id a quorum retired rejoins.
func TestAStoppedNodeNeverRejoinsUnderANewID(t *testing.T) {
	self := consensus.NodeID(0x51)
	voters := []string{"a1", "a2", "a3"}
	peers := map[string]string{"node-a": "self:1", "a1": "a1:1", "a2": "a2:1", "a3": "a3:1"}
	for _, stopped := range []bool{true, false} {
		rejoins := 0
		r := &Replicator{auto: &autoMode{
			self: "node-a", peers: peers, tick: time.Second,
			probe: func(_ context.Context, addr, slot string) (hello, error) {
				return hello{Slot: slot, State: helloMember, Node: slot, Index: 9, Voters: voters, Retired: []string{self.String()}}, nil
			},
			rejoin: func(context.Context) error { rejoins++; return nil },
		}}
		r.checkRetired(context.Background(), &stoppedEngine{id: self, stopped: stopped})
		if stopped {
			assert.Zero(t, rejoins, "a stopped node does not rejoin")
		} else {
			assert.Equal(t, 1, rejoins, "a running node whose id is retired rejoins")
		}
	}
}

// A data directory holding a stop marker is never moved aside for a
// rejoin: the marker would go with it and the node reopen past it (#67).
func TestARejoinRefusesADataDirectoryWithAStopMarker(t *testing.T) {
	driver, err := consensus.Lookup("kitsunium")
	require.NoError(t, err)
	dir := filepath.Join(t.TempDir(), "data")
	_, _, err = driver.Init(dir, consensus.ClusterID{0x67}, "node-a")
	require.NoError(t, err)
	require.NoError(t, refuseStoppedDir(driver, dir))

	require.NoError(t, os.WriteFile(filepath.Join(dir, "STOP-STATE_MACHINE_FAILED"), []byte("entry 12\n"), 0o600))
	err = refuseStoppedDir(driver, dir)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "STOP-STATE_MACHINE_FAILED")
}
