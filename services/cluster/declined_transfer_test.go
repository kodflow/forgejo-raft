// Copyright 2026 The Forgejo Authors. All rights reserved.
// SPDX-License-Identifier: GPL-3.0-or-later

package cluster

import (
	"testing"
	"time"

	"forgejo.org/services/cluster/consensus"

	"github.com/stretchr/testify/assert"
)

// A voter that handed its leadership over because its apply made no
// progress declines a transfer back until that apply moves: the engine
// fails the transfer TRANSFER_FAILED (kitsunium/sdk#674), and its log
// still works, so it stays the least behind. A leader that cannot derive
// the SQL asked that voter again at every yieldBackoff, and never the other
// one: the SQL of the whole cluster stayed frozen for as long as that apply
// did. The next try goes to another voter, and to the same one only when
// no other is in reach.
func TestALeaderThatCannotDeriveTheSQLTriesAnotherVoterAfterADeclinedTransfer(t *testing.T) {
	stuck := func(t *testing.T) (*Replicator, *leaderEngine, *fakeClock) {
		r, e, clock := newLeaderBench(t, 3)
		for _, id := range []consensus.NodeID{0xb, 0xc} {
			e.join(consensus.Member{ID: id, Slot: "node-" + id.String()[15:], SPKI: [32]byte{byte(id)}, Voter: true})
		}
		e.peers[0xb] = consensus.PeerHealth{ID: 0xb}
		e.peers[0xc] = consensus.PeerHealth{ID: 0xc, Lag: 2}
		e.declines[0xb] = true
		r.writerHook = &fakeWriterHook{failing: true}
		r.hookFails = yieldAfterPromotions
		return r, e, clock
	}
	ctx := t.Context()

	r, e, clock := stuck(t)
	assert.False(t, r.yieldIfStuck(ctx, e, e.Membership()), "node-b, the least behind, declines")
	clock.advance(yieldBackoff)
	assert.True(t, r.yieldIfStuck(ctx, e, e.Membership()), "node-c takes it")
	assert.Equal(t, []consensus.NodeID{0xb, 0xc}, e.transferred(), "the next try passes node-b over")
	assert.NotZero(t, r.sqlYield.Load())

	// The only voter in reach is asked again, once per yieldBackoff.
	r, e, clock = stuck(t)
	e.peers[0xc] = consensus.PeerHealth{ID: 0xc, Unreachable: true}
	assert.False(t, r.yieldIfStuck(ctx, e, e.Membership()))
	clock.advance(yieldBackoff - time.Second)
	assert.False(t, r.yieldIfStuck(ctx, e, e.Membership()))
	clock.advance(time.Second)
	assert.False(t, r.yieldIfStuck(ctx, e, e.Membership()))
	assert.Equal(t, []consensus.NodeID{0xb, 0xb}, e.transferred(), "node-b alone, asked again past yieldBackoff")
	assert.Zero(t, r.sqlYield.Load(), "the leadership never went")
}

// The same for a leader that takes no checkpoint (#176): a taker that
// declines the transfer is passed over at the next try when another taker
// is caught up, and asked again when it is the only one.
func TestALeaderWithoutCheckpointsTriesAnotherTakerAfterADeclinedTransfer(t *testing.T) {
	takers := func(t *testing.T) (*Replicator, *leaderEngine, *fakeClock) {
		r, e, clock, answers := checkpointBench(t)
		for _, slot := range []string{"node-b", "node-c"} {
			h := answers[slot]
			h.Checkpoints = checkpointsOn
			answers[slot] = h
		}
		e.peers[0xb] = consensus.PeerHealth{ID: 0xb, Lag: 1}
		e.peers[0xc] = consensus.PeerHealth{ID: 0xc, Lag: 3}
		e.declines[0xb] = true
		return r, e, clock
	}
	ctx := t.Context()

	r, e, clock := takers(t)
	r.autopilotStep(ctx)
	assert.Equal(t, []consensus.NodeID{0xb}, e.transferred(), "node-b, the least behind, declines")
	clock.advance(yieldBackoff)
	r.autopilotStep(ctx)
	assert.Equal(t, []consensus.NodeID{0xb, 0xc}, e.transferred(), "the next try passes node-b over")

	r, e, clock = takers(t)
	e.peers[0xc] = consensus.PeerHealth{ID: 0xc, Lag: checkpointYieldLag + 1}
	r.autopilotStep(ctx)
	clock.advance(yieldBackoff)
	r.autopilotStep(ctx)
	assert.Equal(t, []consensus.NodeID{0xb, 0xb}, e.transferred(), "node-b, the only taker caught up, asked again past yieldBackoff")
}
