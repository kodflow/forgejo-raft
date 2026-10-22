// Copyright 2026 The Forgejo Authors. All rights reserved.
// SPDX-License-Identifier: GPL-3.0-or-later

package cluster

import (
	"context"
	"encoding/hex"
	"testing"
	"time"

	"forgejo.org/services/cluster/consensus"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// laggingEngine is a follower whose table has not applied the entries
// up to the commit boundary yet: WaitApplied applies them.
type laggingEngine struct {
	consensus.Engine
	m       consensus.Membership
	pending func()
	barrier error
}

func (e *laggingEngine) Status() consensus.Status         { return consensus.Status{ID: 0xc, Slot: "node-c"} }
func (e *laggingEngine) Membership() consensus.Membership { return e.m }
func (e *laggingEngine) Barrier(context.Context) (consensus.Token, error) {
	return consensus.Token{Index: 9}, e.barrier
}

func (e *laggingEngine) WaitApplied(context.Context, consensus.Token) error {
	if e.pending != nil {
		e.pending()
		e.pending = nil
	}
	return nil
}

// A follower whose table lags the membership it appended reads the
// revocation of a removed key before it authorizes a JOIN under that key:
// the JOIN is refused (#70). Without a commit boundary, nothing is admitted.
func TestRevokedKeyRefusedOnALaggingFollower(t *testing.T) {
	r := newReplicator(t.TempDir(), newRegistry(time.Second, 8, 64))
	r.auto = &autoMode{expected: 4, self: "node-c", peers: map[string]string{"node-a": "a:1", "node-b": "b:1", "node-c": "c:1", "node-d": "d:1"}}
	old := [32]byte{0xbb}
	e := &laggingEngine{m: consensus.Membership{Members: []consensus.Member{
		{ID: 0xa, Slot: "node-a", Voter: true}, {ID: 0xc, Slot: "node-c", Voter: true}, {ID: 0xd, Slot: "node-d", Voter: true},
	}}}
	e.pending = func() { revokeKey(t, r.table, old) }
	r.setEngine(e)
	require.False(t, r.table.KeyRevoked(hex.EncodeToString(old[:])), "the revocation is not applied yet")

	_, err := r.authorizeJoin(t.Context(), consensus.JoinRequest{Slot: "node-b", SPKI: old, Address: "b:1"})
	require.ErrorContains(t, err, "was revoked")

	e.barrier = consensus.ErrNotLeader
	_, err = r.authorizeJoin(t.Context(), consensus.JoinRequest{Slot: "node-b", SPKI: [32]byte{0xb2}, Address: "b:1"})
	require.ErrorContains(t, err, "JOIN_REFUSED")
	e.barrier = nil
	_, err = r.authorizeJoin(t.Context(), consensus.JoinRequest{Slot: "node-b", SPKI: [32]byte{0xb2}, Address: "b:1"})
	require.NoError(t, err, "a new key")
}

// A learner admitted under a revoked key is removed by the autopilot, and
// never promoted (#70).
func TestRevokedLearnerIsRemovedNotPromoted(t *testing.T) {
	r, e, _ := newLeaderBench(t, 3)
	e.join(consensus.Member{ID: 0xb, Slot: "node-b", SPKI: [32]byte{0xb}})
	e.join(consensus.Member{ID: 0xc, Slot: "node-c", SPKI: [32]byte{0xc}})
	revokeKey(t, r.table, [32]byte{0xb})
	r.autopilotStep(t.Context())
	assert.Equal(t, []consensus.NodeID{0xb}, e.removes)
	assert.Empty(t, e.promotes)
	assert.Equal(t, 1, e.voters())
}
