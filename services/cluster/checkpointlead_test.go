// Copyright 2026 The Forgejo Authors. All rights reserved.
// SPDX-License-Identifier: GPL-3.0-or-later

package cluster

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	cluster_model "forgejo.org/models/cluster"
	"forgejo.org/modules/setting"
	"forgejo.org/modules/test"
	"forgejo.org/services/cluster/consensus"
	"forgejo.org/services/cluster/consensus/memraft"
	"forgejo.org/services/cluster/reftable"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// checkpointBench is the autopilot's leader node-a, which takes no
// checkpoint, with the voters node-b and node-c answering as answers says.
func checkpointBench(t *testing.T) (*Replicator, *leaderEngine, *fakeClock, map[string]hello) {
	r, e, clock := newLeaderBench(t, 3)
	answers := map[string]hello{}
	var mu sync.Mutex
	r.auto.peers = map[string]string{"node-a": "a:1", "node-b": "b:1", "node-c": "c:1"}
	r.auto.probe = func(_ context.Context, _, slot string) (hello, error) {
		mu.Lock()
		defer mu.Unlock()
		h, ok := answers[slot]
		if !ok {
			return hello{}, errors.New("no answer")
		}
		return h, nil
	}
	for _, id := range []consensus.NodeID{0xb, 0xc} {
		slot := map[consensus.NodeID]string{0xb: "node-b", 0xc: "node-c"}[id]
		e.join(consensus.Member{ID: id, Slot: slot, SPKI: [32]byte{byte(id)}, Voter: true})
		answers[slot] = hello{Slot: slot, State: helloMember, Node: id.String(), SPKI: [32]byte{byte(id)}, Checkpoints: checkpointsOff}
	}
	return r, e, clock, answers
}

func (e *leaderEngine) transferred() []consensus.NodeID {
	e.mu.Lock()
	defer e.mu.Unlock()
	return append([]consensus.NodeID(nil), e.transfers...)
}

// A leader that takes no checkpoint hands the leadership to the voter that
// takes them once it is caught up, and not before; once handed, it is not
// handed again within yieldBackoff, and a leader that takes them never
// hands it over for them: two members never pass it back and forth (#176).
func TestALeaderWithoutCheckpointsHandsTheLeadershipToOneThatTakesThem(t *testing.T) {
	r, e, clock, answers := checkpointBench(t)
	ctx := t.Context()
	h := answers["node-c"]
	h.Checkpoints = checkpointsOn
	answers["node-c"] = h
	e.peers[0xc] = consensus.PeerHealth{ID: 0xc, Lag: checkpointYieldLag + 1}
	r.autopilotStep(ctx)
	assert.Empty(t, e.transferred(), "node-c is not caught up")
	view, ok := r.ckState.fresh(clock.now())
	require.True(t, ok)
	assert.Equal(t, []string{"node-c"}, view.On)
	assert.Equal(t, []string{"node-c: 65 entries behind, at most 64"}, view.Held, "the health says why node-c is not handed it")

	e.mu.Lock()
	e.peers[0xc] = consensus.PeerHealth{ID: 0xc, Lag: 3}
	e.mu.Unlock()
	clock.advance(payloadProbeEvery)
	r.autopilotStep(ctx)
	assert.Equal(t, []consensus.NodeID{0xc}, e.transferred(), "handed to node-c once it is caught up")
	view, ok = r.ckState.fresh(clock.now())
	require.True(t, ok)
	assert.Empty(t, view.Held)

	for range 8 {
		clock.advance(payloadProbeEvery)
		r.autopilotStep(ctx)
	}
	assert.Len(t, e.transferred(), 1, "not again within yieldBackoff")

	// A leader that takes them never hands the leadership over for them.
	r2, e2, clock2, answers2 := checkpointBench(t)
	r2.checkpoints.Store(&coordinator{r: r2})
	h2 := answers2["node-c"]
	h2.Checkpoints = checkpointsOn
	answers2["node-c"] = h2
	for range 20 {
		r2.autopilotStep(ctx)
		clock2.advance(yieldBackoff)
	}
	assert.Empty(t, e2.transferred())
}

// The leader hands the leadership, for the checkpoints, only to a taker that
// is caught up, in contact, receives no snapshot and did not give it away
// for the SQL within checkpointHandBack; otherwise it says why, which the
// health repeats (#176).
func TestTakerHoldSaysWhyTheLeadershipIsNotHandedOver(t *testing.T) {
	now := time.Unix(1_800_000_000, 0)
	ok := consensus.PeerHealth{Lag: 3, LastContact: time.Second}
	for _, tc := range []struct {
		name  string
		h     hello
		p     consensus.PeerHealth
		known bool
		why   string
	}{
		{"caught up", hello{}, ok, true, ""},
		{"gave it away for the SQL", hello{SQLYield: now.Add(-time.Minute).Unix()}, ok, true, "could not derive the SQL as of 2027-01-15T07:59:00Z"},
		{"gave it away long ago", hello{SQLYield: now.Add(-2 * checkpointHandBack).Unix()}, ok, true, ""},
		{"no health", hello{}, ok, false, "no replication health for it"},
		{"unreachable", hello{}, consensus.PeerHealth{Unreachable: true}, true, "unreachable"},
		{"a snapshot", hello{}, consensus.PeerHealth{Snapshot: true}, true, "receiving a snapshot"},
		{"behind", hello{}, consensus.PeerHealth{Lag: checkpointYieldLag + 1}, true, "65 entries behind, at most 64"},
		{"silent", hello{}, consensus.PeerHealth{LastContact: checkpointContact + time.Second}, true, "last heard of 11s ago, at most 10s"},
	} {
		got := takerHold(tc.h, tc.p, tc.known, now)
		if tc.why == "" {
			assert.Empty(t, got, tc.name)
		} else {
			assert.Contains(t, got, tc.why, tc.name)
		}
	}
}

// A member that gave the leadership away because it could not derive the
// SQL is not handed it back for its checkpoints within checkpointHandBack:
// it would give it away again, and the leadership would go back and forth.
func TestTheLeadershipIsNotHandedBackToAMemberThatGaveItAwayForTheSQL(t *testing.T) {
	r, e, clock, answers := checkpointBench(t)
	ctx := t.Context()
	h := answers["node-c"]
	h.Checkpoints, h.SQLYield = checkpointsOn, clock.now().Add(-time.Minute).Unix()
	answers["node-c"] = h
	for range int(checkpointHandBack/payloadProbeEvery) - 4 {
		r.autopilotStep(ctx)
		clock.advance(payloadProbeEvery)
	}
	assert.Empty(t, e.transferred(), "within checkpointHandBack of its yield")
	for range 8 {
		r.autopilotStep(ctx)
		clock.advance(payloadProbeEvery)
	}
	assert.Equal(t, []consensus.NodeID{0xc}, e.transferred(), "tried again past it, once")
}

// A member that takes no checkpoint says so in its hello, a member that
// takes them too, and so does the time it last gave the leadership away
// for the SQL.
func TestTheHelloSaysWhetherTheMemberTakesTheCheckpoints(t *testing.T) {
	_, reps := NewForTesting(t.TempDir(), "node-a")
	r := reps[0]
	assert.Equal(t, checkpointsOff, r.memberHello().Checkpoints)
	r.checkpoints.Store(&coordinator{r: r})
	r.sqlYield.Store(1234)
	h := r.memberHello()
	assert.Equal(t, checkpointsOn, h.Checkpoints)
	assert.EqualValues(t, 1234, h.SQLYield)
}

// refusalBench is a voter of three on the in-process log, whose consumer
// reads the backup cursor from backup, and whose peers answer with the
// checkpoints said by answers (absent: no answer; "": an older binary).
func refusalBench(t *testing.T, backup *cluster_model.Cursor, answers map[string]string) *Replicator {
	_, reps := NewForTesting(t.TempDir(), "node-a", "node-b", "node-c")
	r := reps[0]
	r.consumer = &consumer{r: r, cursor: func(context.Context, string) (*cluster_model.Cursor, error) {
		if backup == nil {
			return nil, errors.New("the database does not answer")
		}
		c := *backup
		return &c, nil
	}}
	r.memberProbe = func(_ context.Context, _, slot string) (hello, error) {
		word, ok := answers[slot]
		if !ok {
			return hello{}, errors.New("no answer")
		}
		for _, m := range r.Engine().Membership().Members {
			if m.Slot == slot {
				return hello{Slot: slot, State: helloMember, Node: m.ID.String(), SPKI: m.SPKI, Checkpoints: word}, nil
			}
		}
		return hello{}, errors.New("no member under slot " + slot)
	}
	return r
}

// A voter whose CHECKPOINT_INTERVAL is 0 does not start when the backup
// cursor of its lineage exists and every other voter answers that it takes
// no checkpoint either: no checkpoint would ever close again. It starts on
// anything less than certain: a voter that does not answer, an older binary
// that does not say, a database that does not answer, another lineage, a
// voter that takes them, its own CHECKPOINT_INTERVAL set (#176).
func TestAVoterRefusesToStartWhenNoVoterTakesCheckpoints(t *testing.T) {
	t.Cleanup(test.MockVariableValue(&setting.Cluster.CheckpointInterval, 0))
	ctx := t.Context()
	bothOff := map[string]string{"node-b": checkpointsOff, "node-c": checkpointsOff}
	r := refusalBench(t, nil, bothOff)
	lineage := r.Engine().Status().Identity.String()
	backup := &cluster_model.Cursor{Name: cluster_model.CursorBackup, Lineage: lineage, Position: 10}

	r = refusalBench(t, backup, bothOff)
	err := r.checkpointsRefusal(ctx, r.Engine())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "CHECKPOINT_INTERVAL is 0 on this voter and on every other one (node-b, node-c)")
	assert.Contains(t, err.Error(), "stands at 10")

	for name, tc := range map[string]struct {
		backup  *cluster_model.Cursor
		answers map[string]string
	}{
		"a voter takes them":       {backup, map[string]string{"node-b": checkpointsOff, "node-c": checkpointsOn}},
		"a voter does not answer":  {backup, map[string]string{"node-b": checkpointsOff}},
		"an older binary":          {backup, map[string]string{"node-b": checkpointsOff, "node-c": ""}},
		"the database":             {nil, bothOff},
		"another lineage's cursor": {&cluster_model.Cursor{Name: cluster_model.CursorBackup, Lineage: "00/1", Position: 10}, bothOff},
		"no backup cursor":         {&cluster_model.Cursor{Name: cluster_model.CursorBackup}, bothOff},
	} {
		r := refusalBench(t, tc.backup, tc.answers)
		require.NoError(t, r.checkpointsRefusal(ctx, r.Engine()), name)
	}

	r = refusalBench(t, backup, bothOff)
	r.checkpoints.Store(&coordinator{r: r})
	require.NoError(t, r.checkpointsRefusal(ctx, r.Engine()), "this voter takes them")

	// A learner leads nothing: it is never refused.
	r = refusalBench(t, backup, bothOff)
	require.NoError(t, r.checkpointsRefusal(ctx, learnerView{Engine: r.Engine(), learner: r.Engine().Status().ID}), "a learner")
}

// closeRecorder is an engine whose Close is recorded, not done: the bench
// closes the engine itself.
type closeRecorder struct {
	consensus.Engine
	closed atomic.Bool
}

func (c *closeRecorder) Close(context.Context) error {
	c.closed.Store(true)
	return nil
}

// The refusal reads the backup cursor from SQL, so the start runs it once
// the database is up, after the replicator started (routers/init.go): read
// in Init, before the database, it panicked every node whose
// CHECKPOINT_INTERVAL is 0. A refused voter closes its consensus node and
// the start fails with why; without a replicator, or when the voter may
// start, nothing is closed (#176).
func TestTheStartRefusesOnceTheDatabaseIsUp(t *testing.T) {
	t.Cleanup(test.MockVariableValue(&setting.Cluster.CheckpointInterval, 0))
	ctx := t.Context()
	require.NoError(t, RefuseWithoutCheckpoints(ctx), "no replicator")

	bothOff := map[string]string{"node-b": checkpointsOff, "node-c": checkpointsOff}
	lineage := refusalBench(t, nil, bothOff).Engine().Status().Identity.String()
	backup := &cluster_model.Cursor{Name: cluster_model.CursorBackup, Lineage: lineage, Position: 10}

	bench := func(answers map[string]string) *closeRecorder {
		r := refusalBench(t, backup, answers)
		e := &closeRecorder{Engine: r.Engine()}
		r.setEngine(e)
		t.Cleanup(SetCurrentForTesting(r))
		return e
	}
	e := bench(bothOff)
	err := RefuseWithoutCheckpoints(ctx)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "CHECKPOINT_INTERVAL is 0 on this voter and on every other one (node-b, node-c)")
	assert.True(t, e.closed.Load(), "the refused voter closes its consensus node")

	e = bench(map[string]string{"node-b": checkpointsOff, "node-c": checkpointsOn})
	require.NoError(t, RefuseWithoutCheckpoints(ctx), "a voter takes them")
	assert.False(t, e.closed.Load(), "a voter that may start keeps its consensus node")
}

// On the SDK's engine, a leader whose CHECKPOINT_INTERVAL is 0 hands the
// leadership to the member whose CHECKPOINT_INTERVAL is set, by the pilot of
// the manual mode, and it stays there: no member passes it back (#176).
func TestThePilotGivesTheLeadershipToTheMemberThatTakesCheckpoints(t *testing.T) {
	// Short pauses, restored once the nodes stopped: even with a pause of
	// a few ticks between two hand-overs, nobody passes it back.
	t.Cleanup(test.MockVariableValue(&checkpointProbeEvery, 20*time.Millisecond))
	t.Cleanup(test.MockVariableValue(&yieldBackoff, 100*time.Millisecond))
	tc := newTestClusterOn(t, true, "node-a", "node-b", "node-c")
	bySlot := map[string]*testNode{}
	for _, n := range tc.nodes {
		bySlot[n.slot] = n
	}
	var leader *testNode
	require.Eventually(t, func() bool {
		for _, n := range tc.nodes {
			if n.r.Engine().Status().Role == consensus.RoleLeader {
				leader = n
				return true
			}
		}
		return false
	}, 10*time.Second, 20*time.Millisecond)
	var taker *testNode
	for _, n := range tc.nodes {
		if n != leader {
			taker = n
			break
		}
	}
	taker.r.checkpoints.Store(&coordinator{r: taker.r})
	for _, n := range tc.nodes {
		n.r.memberProbe = func(_ context.Context, _, slot string) (hello, error) {
			other := bySlot[slot]
			h := other.r.memberHello()
			h.Slot = slot
			for _, m := range n.r.Engine().Membership().Members {
				if m.Slot == slot {
					h.SPKI = m.SPKI
				}
			}
			return h, nil
		}
	}
	ctx, cancel := context.WithCancel(tc.ctx)
	var wg sync.WaitGroup
	for _, n := range tc.nodes {
		wg.Go(func() { n.r.leaderPilot(ctx, 20*time.Millisecond) })
	}
	t.Cleanup(func() { cancel(); wg.Wait() })

	takerLeads := func() bool { return taker.r.Engine().Status().Role == consensus.RoleLeader }
	require.Eventually(t, takerLeads, 20*time.Second, 20*time.Millisecond, "the leadership goes to %s, which takes the checkpoints", taker.slot)
	for deadline := time.Now().Add(2 * time.Second); time.Now().Before(deadline); time.Sleep(20 * time.Millisecond) {
		require.True(t, takerLeads(), "the leadership stays with the member that takes the checkpoints")
	}
}

// manualBench is the manual mode on the in-process log: node-a leads,
// node-b alone takes the checkpoints ([cluster] CHECKPOINT_INTERVAL set on
// it alone, as replicator.go admits), every member answers the others with
// its own hello, an event waits for its SQL, and every member's pilot runs,
// every 5 ms unless pilotEvery says otherwise. No consumer runs: each member plays, every 5 ms, what the consumer's step
// does with a pass, a follower's count starting over, and a leader's pass
// failing while down says its database does not answer.
type manualBench struct {
	bySlot map[string]*Replicator
	down   map[string]*atomic.Bool
}

func newManualBench(t *testing.T, pilotEvery map[string]time.Duration) *manualBench {
	mem, rs := NewForTesting(t.TempDir(), "node-a", "node-b", "node-c")
	mem.SetLeader(rs[0].Engine().(*memraft.Node))
	require.Nil(t, rs[0].auto, "the manual mode: no autopilot, no yieldIfStuck")
	b := &manualBench{bySlot: map[string]*Replicator{}, down: map[string]*atomic.Bool{}}
	for i, slot := range []string{"node-a", "node-b", "node-c"} {
		b.bySlot[slot], b.down[slot] = rs[i], &atomic.Bool{}
	}
	b.bySlot["node-b"].checkpoints.Store(&coordinator{r: b.bySlot["node-b"]})
	for _, r := range rs {
		r.memberProbe = func(_ context.Context, _, slot string) (hello, error) {
			h := b.bySlot[slot].memberHello()
			h.Slot = slot
			for _, m := range r.Engine().Membership().Members {
				if m.Slot == slot {
					h.SPKI = m.SPKI
				}
			}
			return h, nil
		}
	}
	require.NoError(t, rs[0].proposeRepoDelete(t.Context(), "alice/waiting.git", 1))
	require.Positive(t, b.bySlot["node-b"].table.EventCount(), "an event waits for its SQL")

	ctx, cancel := context.WithCancel(t.Context())
	var wg sync.WaitGroup
	for slot, r := range b.bySlot {
		every := 5 * time.Millisecond
		if d, ok := pilotEvery[slot]; ok {
			every = d
		}
		wg.Go(func() { r.leaderPilot(ctx, every) })
		wg.Go(func() {
			c := &consumer{r: r}
			tick := time.NewTicker(5 * time.Millisecond)
			defer tick.Stop()
			for {
				select {
				case <-ctx.Done():
					return
				case <-tick.C:
				}
				if r.Engine().Status().Role != consensus.RoleLeader {
					c.notePass(nil)
					continue
				}
				waited := r.table.EventCount() > 0
				var err error
				if b.down[slot].Load() {
					err = errors.New("the database does not answer")
				}
				c.notePass(err)
				r.noteLeaderPass(err, waited)
			}
		})
	}
	t.Cleanup(func() { cancel(); wg.Wait() })
	return b
}

func (b *manualBench) leader() string {
	for slot, r := range b.bySlot {
		if r.Engine().Status().Role == consensus.RoleLeader {
			return slot
		}
	}
	return ""
}

// In the manual mode nothing makes a leader that cannot derive the SQL give
// the leadership away: an operator's transfer is the remedy (#47). The
// pilot of the leader it was transferred to handed it back, for its
// checkpoints, to the taker whose consumer kept failing, once yieldBackoff
// passed: only the autonomous mode's yieldIfStuck said in the hello that a
// member could not derive the SQL. The SQL stayed frozen for the whole
// cluster. The taker now says it, as a follower too, and the operator's
// transfer stands, past yieldBackoff and past checkpointHandBack (#217).
func TestAnOperatorsTransferAwayFromATakerThatCannotDeriveTheSQLStands(t *testing.T) {
	t.Cleanup(test.MockVariableValue(&checkpointProbeEvery, 10*time.Millisecond))
	t.Cleanup(test.MockVariableValue(&yieldBackoff, 300*time.Millisecond))
	t.Cleanup(test.MockVariableValue(&yieldAfterPasses, 5))
	// sql_yield is in seconds: the hold is measured past four of them.
	t.Cleanup(test.MockVariableValue(&checkpointHandBack, 4*time.Second))
	bench := newManualBench(t, nil)
	a, b := bench.bySlot["node-a"], bench.bySlot["node-b"]
	ctx := t.Context()
	// node-b cannot derive the SQL: its database does not answer.
	bench.down["node-b"].Store(true)

	require.Eventually(t, func() bool { return bench.leader() == "node-b" }, 5*time.Second, 5*time.Millisecond,
		"node-a's pilot hands the leadership to node-b, which takes the checkpoints")
	assert.Eventually(t, func() bool { return b.memberHello().SQLYield != 0 }, 5*time.Second, 5*time.Millisecond,
		"node-b, leading, says it cannot derive the SQL")
	time.Sleep(200 * time.Millisecond)
	require.Equal(t, "node-b", bench.leader(), "in the manual mode nothing makes node-b give the leadership away")

	// The operator moves the leadership away from node-b, to node-a.
	require.NoError(t, b.Operate(ctx, OperatorFor(1), Operation{Action: ActionTransfer, Target: slotID(t, a, "node-a").String()}))
	require.Equal(t, "node-a", bench.leader())
	for deadline := time.Now().Add(5 * time.Second); time.Now().Before(deadline); time.Sleep(5 * time.Millisecond) {
		require.Equal(t, "node-a", bench.leader(), "the operator's transfer away from a taker that cannot derive the SQL stands")
	}
	assert.Zero(t, b.consumerFails.Load(), "node-b's consumer count started over as a follower")
	view, ok := a.ckState.fresh(time.Now())
	require.True(t, ok)
	require.Len(t, view.Held, 1)
	assert.Contains(t, view.Held[0], "node-b: could not derive the SQL as of", "the health says why node-b is not handed it")
}

// The consumer says it at the pass that makes yieldAfterPasses failed in a
// row: an operator's transfer right after it, before any tick of node-b's
// pilot, stands too, although node-b's consumer count starts over as a
// follower (#217).
func TestATransferRightAfterTheFailingPassStands(t *testing.T) {
	t.Cleanup(test.MockVariableValue(&checkpointProbeEvery, 10*time.Millisecond))
	t.Cleanup(test.MockVariableValue(&yieldBackoff, 300*time.Millisecond))
	t.Cleanup(test.MockVariableValue(&yieldAfterPasses, 5))
	bench := newManualBench(t, map[string]time.Duration{"node-b": time.Hour})
	a, b := bench.bySlot["node-a"], bench.bySlot["node-b"]
	ctx := t.Context()
	require.Eventually(t, func() bool { return bench.leader() == "node-b" }, 5*time.Second, 5*time.Millisecond)

	bench.down["node-b"].Store(true)
	require.Eventually(t, func() bool { return int(b.consumerFails.Load()) >= yieldAfterPasses }, 5*time.Second, time.Millisecond)
	require.NoError(t, b.Operate(ctx, OperatorFor(1), Operation{Action: ActionTransfer, Target: slotID(t, a, "node-a").String()}))
	for deadline := time.Now().Add(3 * yieldBackoff); time.Now().Before(deadline); time.Sleep(5 * time.Millisecond) {
		require.Equal(t, "node-a", bench.leader(), "the transfer right after the failing pass stands")
	}
	assert.Zero(t, b.consumerFails.Load(), "node-b's consumer count started over as a follower")
}

// A taker that can derive the SQL is handed the leadership back for its
// checkpoints, as before (#176): after an operator's transfer away from it,
// and after it could not derive the SQL for a while, once it leads again
// (the operator's transfer to it, once its database is repaired) and a pass
// of its consumer goes through (#217).
func TestTheLeadershipGoesBackToATakerThatDerivesTheSQL(t *testing.T) {
	t.Cleanup(test.MockVariableValue(&checkpointProbeEvery, 10*time.Millisecond))
	t.Cleanup(test.MockVariableValue(&yieldBackoff, 300*time.Millisecond))
	t.Cleanup(test.MockVariableValue(&yieldAfterPasses, 5))
	bench := newManualBench(t, nil)
	a, b := bench.bySlot["node-a"], bench.bySlot["node-b"]
	idA, idB := slotID(t, a, "node-a").String(), slotID(t, b, "node-b").String()
	ctx := t.Context()
	bLeads := func() bool { return bench.leader() == "node-b" }

	require.Eventually(t, bLeads, 5*time.Second, 5*time.Millisecond, "the leadership goes to node-b, which takes the checkpoints")
	require.NoError(t, b.Operate(ctx, OperatorFor(1), Operation{Action: ActionTransfer, Target: idA}))
	require.Equal(t, "node-a", bench.leader())
	require.Eventually(t, bLeads, 5*time.Second, 5*time.Millisecond, "node-b, which derives the SQL, is handed it back")
	assert.Zero(t, b.memberHello().SQLYield)

	// node-b's database fails, then the operator moves the leadership away.
	bench.down["node-b"].Store(true)
	assert.Eventually(t, func() bool { return b.memberHello().SQLYield != 0 }, 5*time.Second, 5*time.Millisecond,
		"node-b, leading, says it cannot derive the SQL")
	require.NoError(t, b.Operate(ctx, OperatorFor(1), Operation{Action: ActionTransfer, Target: idA}))
	for deadline := time.Now().Add(3 * yieldBackoff); time.Now().Before(deadline); time.Sleep(5 * time.Millisecond) {
		require.Equal(t, "node-a", bench.leader(), "not handed back while node-b cannot derive the SQL")
	}

	// Its database repaired, the operator transfers the leadership to it,
	// and a pass of its consumer goes through: it says so, and is handed the
	// leadership back after the next transfer away from it.
	bench.down["node-b"].Store(false)
	require.NoError(t, a.Operate(ctx, OperatorFor(1), Operation{Action: ActionTransfer, Target: idB}))
	require.Eventually(t, func() bool { return b.memberHello().SQLYield == 0 }, 5*time.Second, 5*time.Millisecond,
		"node-b, leading and deriving the SQL, no longer says it cannot")
	require.NoError(t, b.Operate(ctx, OperatorFor(1), Operation{Action: ActionTransfer, Target: idA}))
	require.Equal(t, "node-a", bench.leader())
	require.Eventually(t, bLeads, 5*time.Second, 5*time.Millisecond, "node-b is handed the leadership back")
}

// In the manual mode, the hello's sql_yield says this node could not
// derive the SQL from the consumer's pass, as the leader, that makes
// yieldAfterPasses failed in a row while events wait; the pilot keeps it
// fresh, as a follower too, long past checkpointHandBack; a pass that goes
// through while events are there ends it. The autonomous mode's sql_yield
// is yieldIfStuck's alone (#217).
func TestTheManualModeSaysTheNodeCannotDeriveTheSQLUntilItDoes(t *testing.T) {
	_, reps := NewForTesting(t.TempDir(), "node-a")
	r := reps[0]
	ctx := t.Context()
	caughtUp := consensus.PeerHealth{Lag: 0}
	held := func(at time.Time) string { return takerHold(r.memberHello(), caughtUp, true, at) }
	down := errors.New("the database does not answer")

	// The consumer's own step, on the leader: its pass fails reading the
	// cursor of the waiting event.
	c := &consumer{
		r: r, dbReady: func() bool { return true }, events: r.table.Events,
		cursor: func(context.Context, string) (*cluster_model.Cursor, error) { return nil, down },
	}
	require.NoError(t, r.proposeRepoDelete(ctx, "alice/waiting.git", 1))
	for range yieldAfterPasses - 1 {
		require.ErrorIs(t, c.step(ctx), down)
	}
	assert.Zero(t, r.memberHello().SQLYield, "fewer failed passes in a row say nothing")
	require.ErrorIs(t, c.step(ctx), down)
	assert.NotZero(t, r.memberHello().SQLYield, "the pass that makes yieldAfterPasses failed in a row says it")
	assert.Contains(t, held(time.Now()), "could not derive the SQL")

	// A follower's consumer count starts over; its pilot keeps it fresh.
	r.consumerFails.Store(0)
	later := time.Now().Add(3 * checkpointHandBack)
	r.keepSQLYield(later)
	assert.Equal(t, later.Unix(), r.memberHello().SQLYield, "past checkpointHandBack")
	assert.Contains(t, held(later), "could not derive the SQL")

	// With no event, failed passes freeze nothing, and a pass that goes
	// through read nothing of the SQL; one that began with events ends it,
	// and the pilot keeps it ended.
	_, empty := NewForTesting(t.TempDir(), "node-z")
	z := empty[0]
	z.consumerFails.Store(int32(yieldAfterPasses))
	z.noteLeaderPass(down, false)
	assert.False(t, z.ckState.sql.stuck, "no event waits: the SQL is not frozen")
	z.ckState.sql.stuck = true
	z.noteLeaderPass(nil, false)
	assert.True(t, z.ckState.sql.stuck, "a pass with no event read nothing of the SQL")
	r.noteLeaderPass(nil, true)
	assert.Zero(t, r.memberHello().SQLYield)
	r.keepSQLYield(later)
	assert.Zero(t, r.memberHello().SQLYield)
	assert.Empty(t, held(later))

	// The autonomous mode: yieldIfStuck's time stands.
	r.auto = &autoMode{}
	r.sqlYield.Store(42)
	r.consumerFails.Store(int32(yieldAfterPasses))
	r.noteLeaderPass(down, true)
	r.keepSQLYield(later)
	assert.EqualValues(t, 42, r.memberHello().SQLYield)
	assert.False(t, r.ckState.sql.stuck)
}

// The pass that goes through may trim the last event itself: with no
// margin (AckMargin = 0, or an ended lineage), maybeAck acks the cursor it
// just moved, and the table holds no event once the pass returns. That
// pass read the SQL all the same, and ends the claim that the node cannot
// derive it: what counts is that events waited when it began (#217).
func TestAPassThatTrimsTheLastEventEndsTheClaim(t *testing.T) {
	t.Cleanup(test.MockVariableValue(&setting.Cluster.AckMargin, 0))
	t.Cleanup(test.MockVariableValue(&ackEvery, 1))
	_, reps := NewForTesting(t.TempDir(), "node-a")
	r := reps[0]
	ctx := t.Context()
	require.NoError(t, r.proposeRepoDelete(ctx, "alice/waiting.git", 1))
	require.Equal(t, 1, r.table.EventCount())

	store, parked := newSQLStore(), &memParked{}
	var dbDown atomic.Bool
	down := errors.New("the database does not answer")
	c := &consumer{
		r: r, dbReady: func() bool { return true }, events: r.table.Events,
		work: func(context.Context, []reftable.Event) error { return nil },
		cursor: func(ctx context.Context, name string) (*cluster_model.Cursor, error) {
			if dbDown.Load() {
				return nil, down
			}
			return store.get(ctx, name)
		},
		advance:     store.advance,
		tx:          func(ctx context.Context, f func(context.Context) error) error { return f(ctx) },
		durable:     func(context.Context) (bool, string) { return true, "" },
		projectedAt: func(string) uint64 { return 1 << 62 },
		lastAck:     map[string]uint64{}, failures: map[uint64]int{}, reasons: map[uint64]string{}, seen: map[string]int64{},
	}
	c.ack = func(ctx context.Context, lineage string, index uint64) error {
		data, err := reftable.Encode(reftable.KindAck, &reftable.Ack{Identity: lineage, Index: index})
		if err != nil {
			return err
		}
		_, _, err = r.proposeCommand(ctx, data, nil)
		return err
	}
	parked.wire(c)

	dbDown.Store(true)
	for range yieldAfterPasses {
		require.ErrorIs(t, c.step(ctx), down)
	}
	require.NotZero(t, r.memberHello().SQLYield, "the node says it cannot derive the SQL")

	dbDown.Store(false)
	require.NoError(t, c.step(ctx))
	require.Zero(t, r.table.EventCount(), "the pass that went through trimmed the last event")
	assert.Zero(t, r.memberHello().SQLYield, "and it ends the claim")
	assert.False(t, r.ckState.sql.stuck)
}
