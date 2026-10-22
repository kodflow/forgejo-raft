// Copyright 2026 The Forgejo Authors. All rights reserved.
// SPDX-License-Identifier: GPL-3.0-or-later

package cluster

import (
	"context"
	"slices"
	"sync"
	"testing"
	"time"

	"forgejo.org/services/cluster/consensus"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// leaderEngine is a leader whose membership changes as the autopilot asks,
// and whose peers' health the test sets.
type leaderEngine struct {
	consensus.Engine
	mu       sync.Mutex
	id       consensus.NodeID
	m        consensus.Membership
	peers    map[consensus.NodeID]consensus.PeerHealth
	promotes []consensus.NodeID
	removes  []consensus.NodeID
	// refuse is refused by Promote, as the engine refuses a learner it
	// did not hear.
	refuse map[consensus.NodeID]bool
}

func (e *leaderEngine) Status() consensus.Status {
	return consensus.Status{ID: e.id, Role: consensus.RoleLeader, Slot: "node-a"}
}

// Propose refuses with certainty: the audit records are not the point.
func (e *leaderEngine) Propose(context.Context, []byte) (consensus.Token, any, error) {
	return consensus.Token{}, nil, consensus.ErrEntryTooLarge
}

func (e *leaderEngine) Membership() consensus.Membership {
	e.mu.Lock()
	defer e.mu.Unlock()
	m := e.m
	m.Members = slices.Clone(m.Members)
	return m
}

func (e *leaderEngine) Health() consensus.Health {
	e.mu.Lock()
	defer e.mu.Unlock()
	h := consensus.Health{Role: consensus.RoleLeader, QuorumReachable: true}
	for _, p := range e.peers {
		h.Peers = append(h.Peers, p)
	}
	return h
}

func (e *leaderEngine) Promote(_ context.Context, _ consensus.Operator, id consensus.NodeID) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.promotes = append(e.promotes, id)
	if e.refuse[id] {
		return consensus.ErrMembershipInvalid
	}
	i := slices.IndexFunc(e.m.Members, func(x consensus.Member) bool { return x.ID == id })
	e.m.Members[i].Voter = true
	return nil
}

func (e *leaderEngine) Remove(_ context.Context, _ consensus.Operator, id consensus.NodeID) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.removes = append(e.removes, id)
	e.m.Members = slices.DeleteFunc(e.m.Members, func(x consensus.Member) bool { return x.ID == id })
	e.m.Retired = append(e.m.Retired, id)
	delete(e.peers, id)
	return nil
}

func (e *leaderEngine) join(x consensus.Member) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.m.Members = append(e.m.Members, x)
	e.peers[x.ID] = consensus.PeerHealth{ID: x.ID}
}

func (e *leaderEngine) voters() int {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.m.Voters()
}

func newLeaderBench(t *testing.T, expected int) (*Replicator, *leaderEngine, *fakeClock) {
	clock := &fakeClock{t: time.Date(2026, 10, 22, 12, 0, 0, 0, time.UTC)}
	r := newReplicator(t.TempDir(), newRegistry(time.Second, 8, 64))
	r.auto = &autoMode{
		expected: expected, self: "node-a", tick: time.Second, now: clock.now,
		deadTimeout: 5 * time.Minute, downAfter: 10 * time.Second,
		probe: func(context.Context, string, string) (hello, error) { return hello{}, consensus.ErrNotLeader },
	}
	e := &leaderEngine{id: 0xa, peers: map[consensus.NodeID]consensus.PeerHealth{}, refuse: map[consensus.NodeID]bool{}}
	e.m.Members = []consensus.Member{{ID: 0xa, Slot: "node-a", SPKI: [32]byte{0xa}, Voter: true}}
	r.setEngine(e)
	return r, e, clock
}

// In a forming cluster, the eldest learner is dead: the autopilot removes
// it, a learner and no voter, and never promotes the live one alone. The
// rebuilt node takes the freed slot, and both learners are promoted in the
// same pass: three voters, writes accepted (#94, #92).
func TestHuntR6ProveEldestDeadLearnerFake(t *testing.T) {
	r, e, clock := newLeaderBench(t, 3)
	e.join(consensus.Member{ID: 0xb, Slot: "node-b", SPKI: [32]byte{0xb}})
	e.join(consensus.Member{ID: 0xc, Slot: "node-c", SPKI: [32]byte{0xc}})
	e.peers[0xb] = consensus.PeerHealth{ID: 0xb, Unreachable: true, LastContact: time.Hour}
	e.refuse[0xb] = true
	ctx := t.Context()
	for range 1800 {
		r.autopilotStep(ctx)
		clock.advance(2 * time.Second)
	}
	assert.Equal(t, []consensus.NodeID{0xb}, e.removes, "the dead learner is removed at one voter")
	assert.Empty(t, e.promotes, "the live learner is never promoted alone")
	assert.Equal(t, 1, e.voters())

	e.join(consensus.Member{ID: 0xbb, Slot: "node-b", SPKI: [32]byte{0xbb}})
	r.autopilotStep(ctx)
	assert.Equal(t, []consensus.NodeID{0xc, 0xbb}, e.promotes, "both learners in one pass")
	assert.Equal(t, 3, e.voters())
	assert.NoError(t, formingRefusal(r.auto, e.Membership()))
}

// A live eldest learner that never catches up holds back neither the
// promotion of the learners after it nor the passes that follow (#94).
func TestHuntR6ProveEldestLaggingLearnerFake(t *testing.T) {
	r, e, clock := newLeaderBench(t, 5)
	for _, id := range []consensus.NodeID{0xb, 0xc} {
		e.join(consensus.Member{ID: id, Slot: "node-" + id.String()[15:], SPKI: [32]byte{byte(id)}, Voter: true})
	}
	e.join(consensus.Member{ID: 0xd, Slot: "node-d", SPKI: [32]byte{0xd}})
	e.join(consensus.Member{ID: 0xe, Slot: "node-e", SPKI: [32]byte{0xe}})
	e.peers[0xd] = consensus.PeerHealth{ID: 0xd, Lag: 1 << 20, Snapshot: true}
	e.refuse[0xd] = true
	for range 10 {
		r.autopilotStep(t.Context())
		clock.advance(2 * time.Second)
	}
	assert.Equal(t, []consensus.NodeID{0xe}, e.promotes)
	assert.Equal(t, 4, e.voters())
}

// From one voter, a single ready learner is never promoted: the second
// voter's loss would leave the cluster with no leader for good (#92).
func TestHuntPerfMbrSecondVoterLossLeavesFormingLeaderless(t *testing.T) {
	r, e, clock := newLeaderBench(t, 3)
	e.join(consensus.Member{ID: 0xb, Slot: "node-b", SPKI: [32]byte{0xb}})
	e.join(consensus.Member{ID: 0xc, Slot: "node-c", SPKI: [32]byte{0xc}})
	e.peers[0xc] = consensus.PeerHealth{ID: 0xc, Lag: 1 << 20, Snapshot: true}
	for range 30 {
		r.autopilotStep(t.Context())
		clock.advance(2 * time.Second)
	}
	require.Empty(t, e.promotes, "node-b alone would make two voters")
	e.peers[0xc] = consensus.PeerHealth{ID: 0xc}
	r.autopilotStep(t.Context())
	assert.Equal(t, []consensus.NodeID{0xb, 0xc}, e.promotes)
	assert.Equal(t, 3, e.voters())
}
