// Copyright 2026 The Forgejo Authors. All rights reserved.
// SPDX-License-Identifier: GPL-3.0-or-later

package cluster

import (
	"context"
	"encoding/hex"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"forgejo.org/modules/json"
	"forgejo.org/services/cluster/consensus"
	"forgejo.org/services/cluster/consensus/kitsunium"
	"forgejo.org/services/cluster/reftable"

	sdk "github.com/kitsunium/sdk/pkg/v1/app/consensus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// lose gives a killed node an empty data directory, as a lost disk does: it
// starts again, stamps a new id, finds a member, opens joining and runs its
// autopilot, as Init does, from an empty table. The bench derives a key
// from each id, so the new id comes with a new key, as the renewer gives it
// (newkey.go).
func (b *autoBench) lose(an *autoNode) consensus.NodeID {
	t := b.t
	require.NoError(t, os.RemoveAll(an.dir), "the disk is lost")
	dd, err := sdk.NewDataDirWith(an.dir, sdk.DataDirOptions{RuntimeDir: kitsunium.RuntimeDir})
	require.NoError(t, err)
	id, inc, err := sdk.Init(dd, an.cluster, an.n.slot)
	require.NoError(t, err)
	an.r().auto.noteJoining(consensus.NodeID(id), consensus.Incarnation(inc))
	an.r().setEngine(nil)
	an.r().resetForRejoin()
	b.mu.Lock()
	an.dd, an.id, an.tr, an.down = dd, id, sdk.NewMemoryTransport(b.network, id), false
	b.byAddr[an.addr] = an
	b.mu.Unlock()
	founded := false
	require.NoError(t, an.r().auto.formLoop(b.ctx, func(context.Context) error { founded = true; return nil }))
	require.False(t, founded, "a node that finds a member never founds")
	require.NoError(t, b.open(an))
	b.tc.loops.Go(func() { an.r().startupBarrier(b.ctx) })
	b.startAutopilot(an)
	return consensus.NodeID(id)
}

// raisePayload waits until the leader writes this binary's payload version,
// which the autopilot raises once every member answers.
func (b *autoBench) raisePayload() {
	require.Eventually(b.t, func() bool {
		b.clock.advance(payloadProbeEvery + time.Second)
		l := b.leader()
		return l != nil && l.r().table.WriteVersion() == reftable.PayloadVersion
	}, 20*time.Second, 100*time.Millisecond)
}

// floorWatch records the fewest voters any of nodes lists, from now until
// the returned function is called.
func floorWatch(nodes []*autoNode) func() int {
	var least atomic.Int32
	least.Store(1 << 20)
	stop := make(chan struct{})
	var wg sync.WaitGroup
	wg.Go(func() {
		for {
			for _, an := range nodes {
				if eng := an.r().Engine(); eng != nil {
					if v := int32(eng.Membership().Voters()); v > 0 && v < least.Load() {
						least.Store(v)
					}
				}
			}
			select {
			case <-stop:
				return
			case <-time.After(2 * time.Millisecond):
			}
		}
	})
	return func() int {
		close(stop)
		wg.Wait()
		return int(least.Load())
	}
}

// auditAt is the commit index of the newest audit record of action on
// target with outcome ok, 0 when there is none.
func auditAt(r *Replicator, action Action, target consensus.NodeID) uint64 {
	for _, a := range r.table.Audit() {
		if a.Action == string(action) && a.Target == target.String() && a.Outcome == "ok" {
			return a.Token.Index
		}
	}
	return 0
}

// madeChange is a membership change made through a changeEngine, with the
// configuration it was made on: the engine's membership just before it.
type madeChange struct {
	action Action
	target consensus.NodeID
	on     consensus.Membership
}

// memberChanges records the membership changes made through the engines
// watchChanges wraps, on any node.
type memberChanges struct {
	mu   sync.Mutex
	made []madeChange
}

// watchChanges wraps the engine of each node, from now on, in one that
// records each membership change made through it. The autopilot makes its
// changes through the engine its node holds (autopilotStep).
func watchChanges(nodes ...*autoNode) *memberChanges {
	c := &memberChanges{}
	for _, an := range nodes {
		an.r().setEngine(&changeEngine{Engine: an.r().Engine(), changes: c})
	}
	return c
}

func (c *memberChanges) note(action Action, target consensus.NodeID, on consensus.Membership) {
	c.mu.Lock()
	c.made = append(c.made, madeChange{action: action, target: target, on: on})
	c.mu.Unlock()
}

// only is the one change of action on target that was made.
func (c *memberChanges) only(t *testing.T, action Action, target consensus.NodeID) madeChange {
	t.Helper()
	c.mu.Lock()
	defer c.mu.Unlock()
	var out []madeChange
	for _, x := range c.made {
		if x.action == action && x.target == target {
			out = append(out, x)
		}
	}
	require.Len(t, out, 1, "one %s of %s made", action, target)
	return out[0]
}

// changeEngine is an engine that records each membership change made
// through it, once the engine accepted it.
type changeEngine struct {
	consensus.Engine
	changes *memberChanges
}

func (e *changeEngine) AddLearner(ctx context.Context, op consensus.Operator, m consensus.Member) error {
	on := e.Membership()
	err := e.Engine.AddLearner(ctx, op, m)
	if err == nil {
		e.changes.note(ActionAddLearner, m.ID, on)
	}
	return err
}

func (e *changeEngine) Promote(ctx context.Context, op consensus.Operator, id consensus.NodeID) error {
	on := e.Membership()
	err := e.Engine.Promote(ctx, op, id)
	if err == nil {
		e.changes.note(ActionPromote, id, on)
	}
	return err
}

func (e *changeEngine) Remove(ctx context.Context, op consensus.Operator, id consensus.NodeID) error {
	on := e.Membership()
	err := e.Engine.Remove(ctx, op, id)
	if err == nil {
		e.changes.note(ActionRemove, id, on)
	}
	return err
}

// voterIn and learnerIn say how m lists id.
func voterIn(m consensus.Membership, id consensus.NodeID) bool {
	i := Rank(m, id)
	return i > 0 && m.Members[i-1].Voter
}

func learnerIn(m consensus.Membership, id consensus.NodeID) bool {
	i := Rank(m, id)
	return i > 0 && !m.Members[i-1].Voter
}

// proposeHookEngine is an engine whose proposals pass through hook first,
// on the goroutine that makes them: the autopilot audits a change through
// the engine its node holds (audit, proposeSettled).
type proposeHookEngine struct {
	consensus.Engine
	hook func(ctx context.Context, data []byte)
}

func (e *proposeHookEngine) Propose(ctx context.Context, data []byte) (consensus.Token, any, error) {
	e.hook(ctx, data)
	return e.Engine.Propose(ctx, data)
}

// auditOf decodes an audit record's envelope (reftable.EncodeVersion), nil
// for any other command.
func auditOf(data []byte) *reftable.Audit {
	if len(data) < 2 || reftable.Kind(data[1]) != reftable.KindAudit {
		return nil
	}
	var a reftable.Audit
	if json.Unmarshal(data[2:], &a) != nil {
		return nil
	}
	return &a
}

// An autonomous cluster of three loses a voter's disk (#186). The node
// starts again on an empty directory under a new id, with a new key: once
// its old id has been out of reach for DEAD_MEMBER_TIMEOUT, the leader adds
// the new id as a learner under the same slot, promotes it, then removes
// the old id, in that order. The membership never lists fewer than three
// voters, the new member follows the leader, and the old id's key is never
// admitted again (#209).
func TestALostVoterOfThreeIsReplacedUnderItsSlot(t *testing.T) {
	lostVoterReplaced(t, nil)
}

// A change of leader between the death of a voter of three and its
// replacement starts the dead clock of the new leader again, at the fake
// time it first leads: the replacement waits for DEAD_MEMBER_TIMEOUT more
// of that clock, which the bench lets pass (untilDeadTimeout), and is made
// under every guarantee of one made without a change (#317).
func TestALostVoterIsReplacedAcrossALeaderChange(t *testing.T) {
	lostVoterReplaced(t, func(b *autoBench, survivors []*autoNode) {
		l := b.leader()
		require.NotNil(t, l)
		to := survivors[0]
		if to == l {
			to = survivors[1]
		}
		require.Eventually(t, func() bool {
			cur := b.leader()
			if cur == to {
				return true
			}
			if cur != nil {
				_ = cur.r().Engine().TransferLeadership(t.Context(), autopilotOperator, b.idOf(to))
			}
			return false
		}, 10*time.Second, 50*time.Millisecond, "the leadership moves to %s", to.n.slot)
		t.Logf("the leadership moved from %s to %s before DEAD_MEMBER_TIMEOUT passed", l.n.slot, to.n.slot)
	})
}

// The audit of a membership change is a proposal of its own, made by the
// autopilot that made the change, after it, and retried until a leader
// commits it (autoChange, audit, proposeSettled). A leader that adds the
// learner and loses the leadership right after may see its audit of the
// addition commit after the next leader promoted the learner and audited
// the promotion, as CI's load had it (#346): the replacement is the same,
// made in the same order, and holds every guarantee of one led throughout
// by one node. This forces that interleaving: the proposal of the audit of
// the addition first moves the leadership to the other survivor, then
// waits until that one audited the promotion. A run where the bench did
// not reach that point in time is skipped once checked, never failed: it
// proved nothing of the interleaving.
func TestALostVoterIsReplacedWhenItsAdditionIsAuditedLast(t *testing.T) {
	var forced atomic.Bool
	var how atomic.Value
	how.Store("the audit of the addition was never proposed")
	lostVoterReplaced(t, func(b *autoBench, survivors []*autoNode) {
		var once sync.Once
		for i, s := range survivors {
			inner, other := s.r().Engine(), survivors[1-i]
			s.r().setEngine(&proposeHookEngine{Engine: inner, hook: func(ctx context.Context, data []byte) {
				a := auditOf(data)
				if a == nil || a.Action != string(ActionAddLearner) || a.Outcome != "ok" {
					return
				}
				id, err := ParseNodeID(a.Target)
				if err != nil {
					return
				}
				once.Do(func() {
					// Both waits fit in the 10 s the audit is given
					// (audit), so that it still commits after them.
					deadline := time.Now().Add(3 * time.Second)
					for time.Now().Before(deadline) && b.leader() != other {
						_ = inner.TransferLeadership(ctx, autopilotOperator, b.idOf(other))
						time.Sleep(20 * time.Millisecond)
					}
					deadline = time.Now().Add(4 * time.Second)
					for time.Now().Before(deadline) && auditAt(s.r(), ActionPromote, id) == 0 {
						time.Sleep(10 * time.Millisecond)
					}
					if auditAt(s.r(), ActionPromote, id) == 0 {
						how.Store("no promotion audited in time after " + s.n.slot + " added the learner")
						return
					}
					forced.Store(true)
					how.Store(s.n.slot + " added the learner, " + other.n.slot + " led, promoted it and audited the promotion, then the audit of the addition was proposed")
				})
			}})
		}
	})
	if !forced.Load() {
		t.Skipf("the interleaving was not forced: %s", how.Load())
	}
	t.Log(how.Load())
}

// lostVoterReplaced is TestALostVoterOfThreeIsReplacedUnderItsSlot; before
// DEAD_MEMBER_TIMEOUT passes, after the voter was out of reach for part of
// it, it runs between, when given.
func lostVoterReplaced(t *testing.T, between func(b *autoBench, survivors []*autoNode)) {
	tc, b := newAutoCluster(t, 3, "node-a", "node-b", "node-c")
	b.waitMembers(3, 3)
	b.raisePayload()
	victim := b.follower()
	oldID := b.idOf(victim)
	oldKey := sdk.MemorySPKI(sdk.NodeID(oldID))
	var survivors []*autoNode
	for _, an := range b.alive() {
		if an != victim {
			survivors = append(survivors, an)
		}
	}
	least := floorWatch(survivors)

	b.kill(victim)
	from := b.lead()
	newID := b.lose(victim)
	// Before DEAD_MEMBER_TIMEOUT, so before any change: the replaced node
	// too, which may lead once it votes and remove the old id itself.
	changes := watchChanges(survivors[0], survivors[1], victim)
	time.Sleep(benchDownAfter + 500*time.Millisecond)

	// Not before DEAD_MEMBER_TIMEOUT: the old id may come back.
	b.clock.advance(benchDeadTimeout - time.Minute)
	time.Sleep(time.Second)
	m := b.leader().r().Engine().Membership()
	assert.Zero(t, Rank(m, newID), "not replaced before DEAD_MEMBER_TIMEOUT")
	assert.Positive(t, Rank(m, oldID))
	if between != nil {
		between(b, survivors)
	}

	b.clock.advance(2 * time.Minute)
	b.untilDeadTimeout(from, func() bool {
		l := b.leader()
		if l == nil {
			return false
		}
		m = l.r().Engine().Membership()
		i := Rank(m, newID)
		return i > 0 && m.Members[i-1].Voter && Rank(m, oldID) == 0 && m.Voters() == 3
	}, "the new id replaces the old one: %+v", m)
	assert.Contains(t, m.Retired, oldID)
	assert.Equal(t, victim.n.slot, m.Members[Rank(m, newID)-1].Slot, "under the same slot")
	assert.GreaterOrEqual(t, least(), 3, "never fewer than three voters listed")

	// Each change is audited once it is made, by the autopilot that made
	// it: the record of the last one may commit after the membership. The
	// audit is a proposal of its own, retried until a leader commits it
	// (autoChange, audit, proposeSettled): the audit of a change made by a
	// leader that lost the leadership right after may commit after the
	// next leader's changes and their audits (#346). The audits say that
	// each change was made, and ok; not in which order.
	l := b.leader().r()
	var added, promoted, removed uint64
	require.Eventually(t, func() bool {
		added, promoted, removed = auditAt(l, ActionAddLearner, newID), auditAt(l, ActionPromote, newID), auditAt(l, ActionRemove, oldID)
		return added != 0 && promoted != 0 && removed != 0
	}, 10*time.Second, 20*time.Millisecond, "the autopilot audits the addition, the promotion and the removal")
	// The order is the log's: each change is recorded with the membership
	// it was made on, whichever leader made it, and a configuration that
	// lists the learner, or the new id as a voter, comes after the change
	// that wrote it.
	add, promote, remove := changes.only(t, ActionAddLearner, newID), changes.only(t, ActionPromote, newID), changes.only(t, ActionRemove, oldID)
	t.Logf("changes made on the configurations at %d, %d and %d; audited at %d, %d and %d",
		add.on.At.Index, promote.on.At.Index, remove.on.At.Index, added, promoted, removed)
	assert.True(t, voterIn(add.on, oldID) && Rank(add.on, newID) == 0, "added while the old id votes: %+v", add.on)
	assert.True(t, learnerIn(promote.on, newID) && voterIn(promote.on, oldID), "promoted once a learner, the old id still voting: %+v", promote.on)
	assert.True(t, voterIn(remove.on, newID), "the old id removed once the new one votes: %+v", remove.on)
	assert.Less(t, add.on.At.Index, promote.on.At.Index, "promoted after it was added")
	assert.Less(t, promote.on.At.Index, remove.on.At.Index, "the old id removed after the new one votes")

	// The new member follows the leader: a write reaches its repositories.
	work := t.TempDir()
	run(t, work, "init", "-q")
	sha, ok, msg := pushOne(tc, b.leader().n, work, "auto/replaced.git", "0000000000000000000000000000000000000000", "after the replacement")
	require.True(t, ok, msg)
	require.Eventually(t, func() bool { return gitRef(t, victim.n, "auto/replaced.git", "refs/heads/main") == sha },
		20*time.Second, 20*time.Millisecond, "the replaced node serves the write")

	// The old id's key is marked: it never joins again, under any id.
	spki := hex.EncodeToString(oldKey[:])
	require.Eventually(t, func() bool { return l.table.KeyRevoked(spki) }, 10*time.Second, 20*time.Millisecond)
	_, err := l.authorizeJoin(t.Context(), consensus.JoinRequest{Slot: victim.n.slot, SPKI: oldKey, Address: victim.addr})
	require.ErrorContains(t, err, "revoked")
}

// replaceEngine is a leader of three that adds a learner as the autopilot
// asks, on top of leaderEngine.
type replaceEngine struct {
	*leaderEngine
	adds []consensus.Member
}

func (e *replaceEngine) AddLearner(_ context.Context, _ consensus.Operator, x consensus.Member) error {
	e.adds = append(e.adds, x)
	e.join(x)
	return nil
}

func (e *replaceEngine) Barrier(context.Context) (consensus.Token, error) {
	return consensus.Token{}, nil
}
func (e *replaceEngine) WaitApplied(context.Context, consensus.Token) error { return nil }

// askRevocation applies an operator's request for the revocation of spki,
// as revokeMember records it, at index.
func askRevocation(t *testing.T, table *reftable.Table, spki [32]byte, index uint64) {
	t.Helper()
	k := hex.EncodeToString(spki[:])
	data, err := reftable.Encode(reftable.KindAudit, &reftable.Audit{
		ID: "asked-" + k[:8], Operator: "op", Action: reftable.AuditRevokeKey, Target: "node-b:" + k, Outcome: reftable.AuditPending, Time: 1,
	})
	require.NoError(t, err)
	require.True(t, table.Apply(consensus.Entry{Term: 1, Index: index, Kind: consensus.EntryCommand, Data: data}).(reftable.Result).OK)
}

// The leader replaces a lost voter of three only when every condition of
// decision A of #186 holds, and never with a key a member holds, nor one
// revoked or asked for revocation. Then it adds the new id, promotes it,
// and removes the old one, in that order, never below three voters.
func TestALostVoterIsReplacedOnlyUnderEveryCondition(t *testing.T) {
	r, le, clock := newLeaderBench(t, 3)
	e := &replaceEngine{leaderEngine: le}
	r.setEngine(e)
	e.join(consensus.Member{ID: 0xb, Slot: "node-b", SPKI: [32]byte{0xb}, Voter: true})
	e.join(consensus.Member{ID: 0xc, Slot: "node-c", SPKI: [32]byte{0xc}, Voter: true})
	r.auto.peers = map[string]string{"node-a": "a:1", "node-b": "b:1", "node-c": "c:1"}
	inc := consensus.Incarnation{0xbb, 1}
	var mu sync.Mutex
	joiner := hello{Slot: "node-b", State: helloJoining, Address: "b:1", Node: consensus.NodeID(0xbb).String(), Incarnation: inc.String(), Payload: reftable.PayloadVersion, SPKI: [32]byte{0xbb}}
	r.auto.probe = func(_ context.Context, _, slot string) (hello, error) {
		mu.Lock()
		defer mu.Unlock()
		if slot == "node-b" {
			return joiner, nil
		}
		return hello{Slot: slot, State: helloMember, Node: consensus.NodeID(0xc).String(), Payload: reftable.PayloadVersion, SPKI: [32]byte{0xc}}, nil
	}
	setKey := func(k [32]byte) {
		mu.Lock()
		joiner.SPKI = k
		mu.Unlock()
	}
	least := 3
	pass := func(n int, d time.Duration) {
		for range n {
			r.autopilotStep(t.Context())
			least = min(least, e.voters())
			clock.advance(d)
		}
	}

	// The old id still answers: nothing, whatever the clock says.
	e.peers[0xb] = consensus.PeerHealth{ID: 0xb}
	pass(5, 2*time.Minute)
	assert.Empty(t, e.adds, "the old id answers")

	// Out of reach, not for DEAD_MEMBER_TIMEOUT yet: nothing.
	e.peers[0xb] = consensus.PeerHealth{ID: 0xb, Unreachable: true, LastContact: time.Hour}
	pass(1, 0)
	pass(4, time.Minute)
	assert.Empty(t, e.adds, "before DEAD_MEMBER_TIMEOUT")
	clock.advance(2 * time.Minute)

	// Past it, but the joiner presents the old id's key, a key asked for
	// revocation, or a revoked one: nothing.
	setKey([32]byte{0xb})
	pass(2, time.Second)
	assert.Empty(t, e.adds, "the old id's key")
	askRevocation(t, r.table, [32]byte{0xb1}, 1)
	setKey([32]byte{0xb1})
	pass(2, time.Second)
	assert.Empty(t, e.adds, "a key asked for revocation")
	revokeKey(t, r.table, [32]byte{0xb2})
	setKey([32]byte{0xb2})
	pass(2, time.Second)
	assert.Empty(t, e.adds, "a revoked key")
	mu.Lock()
	joiner.Incarnation = ""
	joiner.SPKI = [32]byte{0xbb}
	mu.Unlock()
	pass(2, time.Second)
	assert.Empty(t, e.adds, "a hello without its incarnation: an older binary")
	assert.Equal(t, 3, e.voters())

	// Every condition holds: the new id is added under the slot, as the
	// hello names it.
	mu.Lock()
	joiner.Incarnation = inc.String()
	mu.Unlock()
	pass(1, time.Second)
	require.Len(t, e.adds, 1)
	assert.Equal(t, consensus.Member{ID: 0xbb, Incarnation: inc, Slot: "node-b", SPKI: [32]byte{0xbb}, Address: "b:1"}, e.adds[0])
	assert.Equal(t, 3, e.voters(), "a learner first")

	// Promoted, then the old id removed: four voters, then three.
	pass(1, time.Second)
	assert.Equal(t, []consensus.NodeID{0xbb}, e.promotes)
	assert.Empty(t, e.removes, "the old id stays until the new one votes")
	assert.Equal(t, 4, e.voters())
	pass(1, time.Second)
	assert.Equal(t, []consensus.NodeID{0xb}, e.removes)
	assert.Equal(t, 3, e.voters())
	pass(5, time.Minute)
	assert.Len(t, e.adds, 1, "one replacement")
	assert.Equal(t, 3, least, "never fewer than three voters")
}

// joiningEngine is an engine open joining: its id, and no membership yet.
type joiningEngine struct {
	consensus.Engine
	id consensus.NodeID
}

func (e *joiningEngine) Status() consensus.Status         { return consensus.Status{ID: e.id} }
func (e *joiningEngine) Membership() consensus.Membership { return consensus.Membership{} }

// A joining node's hello carries the id and incarnation of its stamp,
// before its engine opens and after; a member's does not.
func TestAJoiningHelloCarriesItsStamp(t *testing.T) {
	r := newReplicator(t.TempDir(), newRegistry(time.Second, 8, 64))
	r.auto = &autoMode{self: "node-b", listen: "b:1", expected: 3}
	inc := consensus.Incarnation{7, 7}
	r.auto.noteJoining(0x77, inc)
	h := r.helloOf()
	assert.Equal(t, helloJoining, h.State)
	assert.Equal(t, consensus.NodeID(0x77).String(), h.Node)
	assert.Equal(t, inc.String(), h.Incarnation)

	r.setEngine(&joiningEngine{id: 0x77})
	h = r.helloOf()
	assert.Equal(t, inc.String(), h.Incarnation, "the open engine names the stamp's id")

	r.auto.noteDisk(true)
	h = r.helloOf()
	assert.Equal(t, helloMember, h.State)
	assert.Empty(t, h.Incarnation, "a member's hello")
}

// A joining node whose certificate on disk changed under its open engine,
// as when an operator installs a new key while it runs, does not offer its
// stamp: the leader would add it under the key on disk, which the engine's
// transport does not present, and never reach that learner. Once the
// engine opens again, with the key on disk, the stamp is offered (#186,
// #209).
func TestAJoiningHelloWaitsForARestartAfterANewKey(t *testing.T) {
	pki := newPKI(t)
	cluster := consensus.ClusterID{0x18, 0x6a}
	n := newKeyNode(t, pki, cluster, "node-b")
	n.r.auto = &autoMode{self: "node-b", listen: "b:1", expected: 3}
	inc := consensus.Incarnation{0xb2, 2}
	n.r.auto.noteJoining(0xb2, inc)
	n.r.setOpenedKey(n.key)
	n.r.setEngine(&joiningEngine{id: 0xb2})
	require.Equal(t, inc.String(), n.r.helloOf().Incarnation, "the key on disk is the engine's")

	key := installKey(t, pki, cluster, "node-b")
	require.Equal(t, key, n.r.presentedKey(), "the object channel reads the new key")
	h := n.r.helloOf()
	assert.Equal(t, helloJoining, h.State)
	assert.Empty(t, h.Incarnation, "the engine presents its old key: no stamp until it opens again")

	n.r.setOpenedKey(n.r.presentedKey())
	assert.Equal(t, inc.String(), n.r.helloOf().Incarnation, "reopened with the key on disk")
}

// A replacement the leader added, then removed before it was ever heard,
// holds a joining stamp under an id the cluster retired: its JOIN is
// refused for good, and no membership of its own lists it. Once the members
// that answer say by a majority that its id is retired, it stamps another
// id; one answer of two, an answer under a revoked key, or a node whose
// directory holds a member's log, never does (#186).
func TestARetiredJoinerStampsAnotherID(t *testing.T) {
	self := consensus.NodeID(0xc2)
	retired := func(slot string, k byte) hello {
		return hello{Slot: slot, State: helloMember, Retired: []string{self.String()}, SPKI: [32]byte{k}}
	}
	member := func(slot string, k byte) hello {
		return hello{Slot: slot, State: helloMember, SPKI: [32]byte{k}}
	}
	for _, tc := range []struct {
		name    string
		member  bool
		revoke  [][32]byte
		answers map[string]hello
		rejoins int
	}{
		{name: "both members", rejoins: 1, answers: map[string]hello{"node-a": retired("node-a", 0xa), "node-b": retired("node-b", 0xb)}},
		{name: "one member, the other down", rejoins: 1, answers: map[string]hello{"node-a": retired("node-a", 0xa)}},
		{name: "one of two", answers: map[string]hello{"node-a": retired("node-a", 0xa), "node-b": member("node-b", 0xb)}},
		{name: "a revoked key", revoke: [][32]byte{{0xa}}, answers: map[string]hello{"node-a": retired("node-a", 0xa), "node-b": member("node-b", 0xb)}},
		{name: "a member's directory", member: true, answers: map[string]hello{"node-a": retired("node-a", 0xa), "node-b": retired("node-b", 0xb)}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r := newReplicator(t.TempDir(), newRegistry(time.Second, 8, 64))
			for _, k := range tc.revoke {
				revokeKey(t, r.table, k)
			}
			rejoins := 0
			r.auto = &autoMode{
				self: "node-c", tick: time.Second,
				peers: map[string]string{"node-a": "a:1", "node-b": "b:1", "node-c": "c:1"},
				probe: func(_ context.Context, _, slot string) (hello, error) {
					if h, ok := tc.answers[slot]; ok {
						return h, nil
					}
					return hello{}, consensus.ErrNotLeader
				},
				rejoin: func(context.Context) error { rejoins++; return nil },
			}
			if tc.member {
				r.auto.noteDisk(true)
			} else {
				r.auto.noteJoining(self, consensus.Incarnation{0xc2})
			}
			r.checkRetired(t.Context(), &fakeRetireEngine{id: self})
			assert.Equal(t, tc.rejoins, rejoins)
		})
	}
}
