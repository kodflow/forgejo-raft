// Copyright 2026 The Forgejo Authors. All rights reserved.
// SPDX-License-Identifier: GPL-3.0-or-later

package cluster

import (
	"context"
	"slices"
	"strconv"
	"sync"
	"testing"
	"time"

	"forgejo.org/services/cluster/consensus"
	"forgejo.org/services/cluster/reftable"

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
	// version is the cluster version, raises the versions asked for, and
	// raiseErr the answer while it is set: the engine's while a member
	// does not announce the protocol.
	version  uint32
	raises   []uint32
	raiseErr error
	// transfers are the leadership transfers asked for, and declines the
	// voters that do not take one: the engine's TRANSFER_FAILED for a voter
	// that handed its own leadership over for an apply that has not moved
	// since (kitsunium/sdk#674).
	transfers []consensus.NodeID
	declines  map[consensus.NodeID]bool
}

func (e *leaderEngine) TransferLeadership(_ context.Context, _ consensus.Operator, to consensus.NodeID) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.transfers = append(e.transfers, to)
	if e.declines[to] {
		return consensus.ErrTransferFailed
	}
	return nil
}

func (e *leaderEngine) Status() consensus.Status {
	e.mu.Lock()
	defer e.mu.Unlock()
	return consensus.Status{ID: e.id, Role: consensus.RoleLeader, Slot: "node-a", ClusterVersion: e.version}
}

func (e *leaderEngine) RaiseClusterVersion(_ context.Context, _ consensus.Operator, _ consensus.Precondition, v uint32) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.raises = append(e.raises, v)
	if e.raiseErr != nil {
		return e.raiseErr
	}
	e.version = v
	return nil
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
	e := &leaderEngine{id: 0xa, peers: map[consensus.NodeID]consensus.PeerHealth{}, refuse: map[consensus.NodeID]bool{}, declines: map[consensus.NodeID]bool{}}
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

// raiseTo raises a table's written payload version, as the audit records
// of the raises do.
func raiseTo(t *testing.T, r *Replicator, v int) {
	t.Helper()
	for i := reftable.BaseVersion + 1; i <= v; i++ {
		data, err := reftable.Encode(reftable.KindAudit, &reftable.Audit{Operator: "op", Action: reftable.AuditPayloadVersion, Target: strconv.Itoa(i), Outcome: reftable.AuditRaised})
		require.NoError(t, err)
		require.True(t, r.table.Apply(consensus.Entry{Term: 1, Index: uint64(i), Data: data}).(reftable.Result).OK)
	}
	require.Equal(t, uint8(v), r.table.WriteVersion())
}

// Once the cluster writes a payload version above the base, a binary that
// does not apply it is refused at the JOIN, never promoted, and holds a
// raise from the administration back (#50).
func TestHuntR1ProveOlderBinaryJoinsAfterTheRaise(t *testing.T) {
	r, e, clock := newLeaderBench(t, 3)
	payload := map[string]int{"node-b": reftable.PayloadVersion, "node-c": reftable.PayloadVersion - 1}
	r.auto.peers = map[string]string{"node-a": "a:1", "node-b": "b:1", "node-c": "c:1"}
	r.auto.probe = func(_ context.Context, _, slot string) (hello, error) {
		id := map[string]consensus.NodeID{"node-b": 0xb, "node-c": 0xc}[slot]
		return hello{Slot: slot, State: helloJoining, Node: id.String(), Payload: payload[slot], SPKI: [32]byte{byte(id)}}, nil
	}

	// At the base version, nothing is asked.
	require.NoError(t, r.joinerApplies(t.Context(), consensus.JoinRequest{Slot: "node-c", Address: "c:1"}))
	raiseTo(t, r, reftable.PayloadVersion)
	require.NoError(t, r.joinerApplies(t.Context(), consensus.JoinRequest{Slot: "node-b", Address: "b:1"}))
	err := r.joinerApplies(t.Context(), consensus.JoinRequest{Slot: "node-c", Address: "c:1"})
	require.ErrorContains(t, err, "JOIN_REFUSED")
	payload["node-c"] = 0 // a binary older than the field applies version 1
	require.ErrorContains(t, r.joinerApplies(t.Context(), consensus.JoinRequest{Slot: "node-c", Address: "c:1"}), "JOIN_REFUSED")

	// Members already: the older one is never promoted, so neither is the
	// other from one voter.
	e.join(consensus.Member{ID: 0xb, Slot: "node-b", SPKI: [32]byte{0xb}})
	e.join(consensus.Member{ID: 0xc, Slot: "node-c", SPKI: [32]byte{0xc}})
	for range 5 {
		r.autopilotStep(t.Context())
		clock.advance(2 * time.Second)
	}
	assert.Empty(t, e.promotes)
	payload["node-c"] = reftable.PayloadVersion
	r.autopilotStep(t.Context())
	assert.Equal(t, []consensus.NodeID{0xb, 0xc}, e.promotes)
}

// A raise from the administration, in the autonomous mode, waits for every
// member to apply the version, as the autopilot's does (#50).
func TestRaiseFromTheAdministrationWaitsForEveryMember(t *testing.T) {
	r, e, _ := newLeaderBench(t, 3)
	e.join(consensus.Member{ID: 0xb, Slot: "node-b", SPKI: [32]byte{0xb}, Voter: true})
	r.auto.peers = map[string]string{"node-a": "a:1", "node-b": "b:1"}
	r.auto.probe = func(_ context.Context, _, slot string) (hello, error) {
		return hello{Slot: slot, State: helloMember, Node: consensus.NodeID(0xb).String(), Payload: reftable.PayloadVersion - 1, SPKI: [32]byte{0xb}}, nil
	}
	err := r.operate(t.Context(), e, "op", Operation{Action: ActionRaisePayload, Target: strconv.Itoa(reftable.PayloadVersion)})
	require.ErrorContains(t, err, "not raised")
	assert.Equal(t, uint8(reftable.BaseVersion), r.table.WriteVersion())
}
