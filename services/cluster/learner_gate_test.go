// Copyright 2026 The Forgejo Authors. All rights reserved.
// SPDX-License-Identifier: GPL-3.0-or-later

package cluster

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"forgejo.org/modules/test"
	"forgejo.org/services/cluster/consensus"

	sdk "github.com/kitsunium/sdk/pkg/v1/app/consensus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// gateRefusal is one write learnerGate refused, with what the node knew
// just before and just after the call.
type gateRefusal struct {
	slot                    string
	before, after           consensus.Status
	leaderAnswersRightAfter bool
}

// The autonomous cluster of the real SDK engine (autoBench) forms, and
// while the autopilot promotes the two learners, learnerGate is called on
// every node in a loop, as every write served there is (propose.go). The
// SDK fails every pending read with NOT_LEADER when the node's role
// changes (node.go leadershipChanged), the learner's own promotion
// included: learnerGate took that for a leader that does not answer, and
// refused a write on a member whose leader answered right after, the
// failure of suite C of the E2E (C.repo-created, "this node is a learner
// and no leader answers") (#199).
func TestALearnerPromotedDuringAWriteIsNotRefused(t *testing.T) {
	// A learner that no leader answers is still refused, within this.
	t.Cleanup(test.MockVariableValue(&writeLeaderWait, 2*time.Second))
	var mu sync.Mutex
	var refusals []gateRefusal
	var calls atomic.Int64
	b := &autoBench{
		t: t, network: sdk.NewMemoryNetwork(), cluster: sdk.ClusterID{0x0f, 0x07, 0x9e, 0x31}, expected: 3,
		timing: sdk.TimingContractSpec{
			HeartbeatInterval:  10 * time.Millisecond,
			ElectionTimeoutMin: 100 * time.Millisecond, ElectionTimeoutMax: 200 * time.Millisecond,
		},
		clock:  &fakeClock{t: time.Date(2026, 10, 17, 12, 0, 0, 0, time.UTC)},
		byAddr: map[string]*autoNode{},
	}
	newTestClusterWith(t, consensus.ClusterID(b.cluster), func(tc *testCluster, _ map[string][32]byte) {
		b.ctx = tc.ctx
		peers := map[string]string{}
		for _, n := range tc.nodes {
			peers[n.slot] = n.slot + ".forgejo.test:7480"
		}
		var wg sync.WaitGroup
		for _, n := range tc.nodes {
			an := &autoNode{n: n, addr: n.slot + ".forgejo.test:7480", dir: filepath.Join(t.TempDir(), "data"), cluster: b.cluster}
			an.r().auto = &autoMode{
				expected: b.expected, self: n.slot, listen: an.addr, peers: peers,
				probe: b.probe, tick: 50 * time.Millisecond,
				now: b.clock.now, deadTimeout: benchDeadTimeout, downAfter: benchDownAfter,
				rejoin: func(ctx context.Context) error { return b.rejoin(an) },
			}
			an.r().objects.hello = an.r().helloOf
			b.nodes = append(b.nodes, an)
			wg.Go(func() { b.form(an) })
		}
		wg.Wait()

		// Every engine is open, the joiners are learners or not yet
		// members: the writes start before the autopilot promotes anyone.
		hctx, stop := context.WithCancel(tc.ctx)
		var hammers sync.WaitGroup
		for _, an := range b.nodes {
			for range 4 {
				hammers.Go(func() {
					for hctx.Err() == nil {
						eng := an.r().Engine()
						before := eng.Status()
						err := an.r().learnerGate(hctx, eng)
						calls.Add(1)
						if !errors.Is(err, errReadOnly) {
							continue
						}
						after := eng.Status()
						answers := probeLeader(hctx, eng)
						mu.Lock()
						refusals = append(refusals, gateRefusal{slot: an.n.slot, before: before, after: after, leaderAnswersRightAfter: answers})
						mu.Unlock()
					}
				})
			}
		}
		for _, an := range b.nodes {
			b.startAutopilot(an)
		}
		b.waitMembers(len(tc.nodes), len(tc.nodes))
		// A little more, once every member votes: no refusal may follow.
		time.Sleep(200 * time.Millisecond)
		stop()
		hammers.Wait()
	}, "node-a", "node-b", "node-c")

	mu.Lock()
	defer mu.Unlock()
	var spurious []string
	for _, x := range refusals {
		// A member that knew its leader, and heard it again right after:
		// "no leader answers" is false for it.
		if x.before.Leader != 0 && x.leaderAnswersRightAfter {
			spurious = append(spurious, fmt.Sprintf("%s: before role=%s term=%d leader=%x, after role=%s term=%d leader=%x, leader answers right after=%v",
				x.slot, x.before.Role, x.before.Term, uint64(x.before.Leader), x.after.Role, x.after.Term, uint64(x.after.Leader), x.leaderAnswersRightAfter))
		}
	}
	t.Logf("%d gate calls, %d refusals, %d with a known leader that answers right after", calls.Load(), len(refusals), len(spurious))
	for _, s := range spurious {
		t.Log(s)
	}
	require.NotZero(t, calls.Load())
	assert.Empty(t, spurious, "a write was refused as \"no leader answers\" on a member whose leader answered")
}

// promotedLearner is a learner whose leader answers, promoted while its
// read barrier is in flight: the SDK then answers that barrier NOT_LEADER,
// as it answers every pending read on a change of role (node.go,
// leadershipChanged), and the next barrier passes.
type promotedLearner struct {
	consensus.Engine
	mu       sync.Mutex
	voter    bool
	barriers int
}

func (e *promotedLearner) Status() consensus.Status {
	e.mu.Lock()
	defer e.mu.Unlock()
	role := consensus.RoleLearner
	if e.voter {
		role = consensus.RoleFollower
	}
	return consensus.Status{ID: 0xc, Role: role, Leader: 0xa}
}

func (e *promotedLearner) Membership() consensus.Membership {
	e.mu.Lock()
	defer e.mu.Unlock()
	return consensus.Membership{Members: []consensus.Member{
		{ID: 0xa, Slot: "node-a", Voter: true}, {ID: 0xb, Slot: "node-b", Voter: true}, {ID: 0xc, Slot: "node-c", Voter: e.voter},
	}}
}

func (e *promotedLearner) Barrier(context.Context) (consensus.Token, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.barriers++
	if !e.voter {
		e.voter = true // the promotion lands while the read is pending
		return consensus.Token{}, consensus.ErrNotLeader
	}
	return consensus.Token{}, nil
}

// The same interleaving, made deterministic: learnerGate refused the write
// as "no leader answers" although the node is a voter when it answers and
// its leader answers the very next barrier (#199).
func TestLearnerGateTakesItsOwnPromotionForAVoter(t *testing.T) {
	r := newReplicator(t.TempDir(), newRegistry(time.Second, 8, 64))
	r.auto = &autoMode{expected: 3, self: "node-c"}
	e := &promotedLearner{}
	err := r.learnerGate(t.Context(), e)
	t.Logf("learnerGate: %v; voter now: %v; next probe answered: %v", err, e.Status().Role == consensus.RoleFollower, probeLeader(t.Context(), e))
	assert.NoError(t, err, "a write refused on a node promoted to voter, whose leader answers")
}

// lostLearner is a learner whose leader never answers.
type lostLearner struct {
	consensus.Engine
	barriers atomic.Int32
}

func (e *lostLearner) Status() consensus.Status {
	return consensus.Status{ID: 0xc, Role: consensus.RoleLearner, Leader: 0xa}
}

func (e *lostLearner) Membership() consensus.Membership {
	return consensus.Membership{Members: []consensus.Member{
		{ID: 0xa, Slot: "node-a", Voter: true}, {ID: 0xb, Slot: "node-b", Voter: true}, {ID: 0xc, Slot: "node-c"},
	}}
}

func (e *lostLearner) Barrier(context.Context) (consensus.Token, error) {
	e.barriers.Add(1)
	return consensus.Token{}, consensus.ErrLeadershipUnconfirmed
}

// A learner that no leader answers is still refused, once the probes
// asked again within writeLeaderWait found none.
func TestLearnerGateStillRefusesALearnerNoLeaderAnswers(t *testing.T) {
	defer test.MockVariableValue(&writeLeaderWait, 400*time.Millisecond)()
	r := newReplicator(t.TempDir(), newRegistry(time.Second, 8, 64))
	r.auto = &autoMode{expected: 3, self: "node-c"}
	e := &lostLearner{}
	start := time.Now()
	require.ErrorIs(t, r.learnerGate(t.Context(), e), errReadOnly)
	assert.Less(t, time.Since(start), 2*time.Second, "refused within writeLeaderWait")
	assert.Greater(t, e.barriers.Load(), int32(1), "the probe was asked again")
}
