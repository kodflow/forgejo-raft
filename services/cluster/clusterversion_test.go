// Copyright 2026 The Forgejo Authors. All rights reserved.
// SPDX-License-Identifier: GPL-3.0-or-later

package cluster

import (
	"context"
	"fmt"
	"path/filepath"
	"slices"
	"sync"
	"testing"
	"time"

	"forgejo.org/services/cluster/consensus"
	"forgejo.org/services/cluster/consensus/kitsunium"
	"forgejo.org/services/cluster/consensus/memraft"
	"forgejo.org/services/cluster/reftable"

	sdk "github.com/kitsunium/sdk/pkg/v1/app/consensus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// raisedAudit reports whether a table holds the audit record of a raise of
// the cluster version.
func raisedAudit(tb *reftable.Table) bool {
	return slices.ContainsFunc(tb.Audit(), func(a reftable.AuditRecord) bool {
		return a.Action == "raise-cluster-version" && a.Outcome == "ok"
	})
}

// A cluster the autonomous mode formed on the SDK's engine speaks the
// engine's protocol 2 with nobody's help: the engine never raises its
// cluster version by itself, and at 0 the leader record of
// kitsunium/sdk#542 is never sent (#170).
func TestAutonomousClusterRaisesItsClusterVersion(t *testing.T) {
	_, b := newAutoCluster(t, 3, "node-a", "node-b", "node-c")
	b.waitMembers(3, 3)
	require.Eventually(t, func() bool {
		// The autopilot asks at most once per payloadProbeEvery of its
		// clock, which only the test moves.
		b.clock.advance(payloadProbeEvery)
		for _, an := range b.alive() {
			if an.r().Engine().Status().ClusterVersion < 2 {
				return false
			}
		}
		return true
	}, 20*time.Second, 50*time.Millisecond, "the cluster version reaches protocol 2 without an operator")
	l := b.leader()
	require.NotNil(t, l)
	require.Eventually(t, func() bool { return raisedAudit(l.r().table) }, 10*time.Second, 20*time.Millisecond, "the raise is audited")
}

// tcpNode is a node on the SDK's engine over its mutual-TLS TCP transport,
// the transport that refuses a key its own configuration does not list: a
// voter that comes back with an older configuration does not hear a leader
// admitted while it was down, unless a member it knows hands it that
// leader's record (kitsunium/sdk#542).
type tcpNode struct {
	slot, cert, dir, addr string
	spki                  [32]byte
	id                    sdk.NodeID
	inc                   sdk.Incarnation
	dd                    sdk.DataDir
	tr                    *sdk.TCPTransport
	node                  *sdk.Node
	r                     *Replicator
	// stop ends the node's pilot of the cluster version.
	stop func()
	up   bool
}

type tcpBench struct {
	t       *testing.T
	pki     *testPKI
	cluster sdk.ClusterID
	timing  sdk.TimingContractSpec
	mu      sync.Mutex
	nodes   []*tcpNode
}

func newTCPBench(t *testing.T, cluster sdk.ClusterID) *tcpBench {
	return &tcpBench{
		t: t, pki: newPKI(t), cluster: cluster,
		// Wide enough that no election comes from a loaded runner: the
		// scenario needs node-4 to keep the leadership it is handed.
		timing: sdk.TimingContractSpec{HeartbeatInterval: 50 * time.Millisecond, ElectionTimeoutMin: 500 * time.Millisecond, ElectionTimeoutMax: time.Second},
	}
}

// stamp makes a node's data directory and its certificate, and opens its
// listener.
func (b *tcpBench) stamp(slot string) *tcpNode {
	t := b.t
	n := &tcpNode{slot: slot, dir: filepath.Join(t.TempDir(), "data"), addr: "127.0.0.1:0"}
	n.cert, n.spki = b.pki.issue(t, b.cluster.String(), slot)
	b.openDir(n)
	var err error
	n.id, n.inc, err = sdk.Init(n.dd, b.cluster, slot)
	require.NoError(t, err)
	b.listen(n)
	b.mu.Lock()
	b.nodes = append(b.nodes, n)
	b.mu.Unlock()
	t.Cleanup(func() { b.down(n) })
	return n
}

func (b *tcpBench) openDir(n *tcpNode) {
	dd, err := sdk.NewDataDirWith(n.dir, sdk.DataDirOptions{RuntimeDir: kitsunium.RuntimeDir})
	require.NoError(b.t, err)
	n.dd = dd
}

func (b *tcpBench) listen(n *tcpNode) {
	tr, err := sdk.NewTCPTransportFromPEM(n.cert, n.cert, b.pki.caFile, &sdk.TransportConfig{ListenAddress: n.addr})
	require.NoError(b.t, err)
	n.tr, n.addr = tr, tr.Addr()
}

func (b *tcpBench) config(n *tcpNode) sdk.Config {
	return sdk.Config{
		Cluster: b.cluster, Machine: kitsunium.Machine(n.r.table), Durability: sdk.DurabilityVolatile,
		Log: n.dd.Log(), Snapshots: n.dd.Snapshots(), Transport: n.tr, InitialTiming: b.timing,
		MaxSnapshotBytes: 1 << 30, MaxLogBytes: 1 << 30, SnapshotPausesApply: true,
		JoinAuthorizer: joinAuthorizerFor(n.r),
	}
}

// start opens the node's engine under a replicator of its own, as a
// process starting does, with the pilot of the manual mode.
func (b *tcpBench) start(n *tcpNode) {
	n.r = newReplicator(b.t.TempDir(), newRegistry(time.Second, 8, 64))
	node, err := sdk.NewNode(b.config(n))
	require.NoError(b.t, err)
	n.node = node
	n.r.setEngine(kitsunium.Wrap(node, n.slot))
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		n.r.leaderPilot(ctx, 50*time.Millisecond)
	}()
	n.stop = func() { cancel(); <-done }
	n.up = true
}

// down stops the node as a crash does: its pilot, its engine, its
// listener and its data directory.
func (b *tcpBench) down(n *tcpNode) {
	if !n.up {
		return
	}
	n.up = false
	n.stop()
	_ = n.node.Close(context.Background())
	_ = n.tr.Close()
	_ = n.dd.Close()
}

// restart starts a stopped node again on its data directory and address.
func (b *tcpBench) restart(n *tcpNode) {
	b.openDir(n)
	b.listen(n)
	b.start(n)
}

func (b *tcpBench) leader() *tcpNode {
	b.mu.Lock()
	defer b.mu.Unlock()
	for _, n := range b.nodes {
		if n.up && n.node.Status().Role == sdk.RoleLeader {
			return n
		}
	}
	return nil
}

// The scenario of #170, in the manual mode: n1 stops; n4 is admitted,
// promoted and made leader; n1 comes back with the configuration of its
// log, whose transport refuses n4's key. At protocol 2, n2 and n3 answer
// its PreVote with n4's record and n1 applies n4's writes; at 0, it stayed
// a pre-candidate as long as n4 led, a voter that acknowledges nothing, and
// the next failure cost the majority.
func TestAReturnedVoterHearsALeaderAdmittedWhileItWasDown(t *testing.T) {
	b := newTCPBench(t, sdk.ClusterID{0x0f, 0x07, 0x9e, 0x17})
	var founders []*tcpNode
	var members []sdk.MemberValue
	var pins [][32]byte
	for _, slot := range []string{"node-1", "node-2", "node-3"} {
		n := b.stamp(slot)
		founders = append(founders, n)
		members = append(members, sdk.MemberValue{ID: n.id, Incarnation: n.inc, Slot: slot, SPKI: n.spki, Address: n.addr, Voter: true})
		pins = append(pins, n.spki)
	}
	for _, n := range founders {
		n.r = newReplicator(t.TempDir(), newRegistry(time.Second, 8, 64))
		require.NoError(t, sdk.Bootstrap(context.Background(), new(b.config(n)), members))
		b.start(n)
	}
	var l *tcpNode
	require.Eventually(t, func() bool { l = b.leader(); return l != nil }, 10*time.Second, 20*time.Millisecond)

	// Nobody raises it: the pilot does, on every member.
	require.Eventually(t, func() bool {
		for _, n := range founders {
			if n.r.Engine().Status().ClusterVersion < 2 {
				return false
			}
		}
		return true
	}, 20*time.Second, 20*time.Millisecond, "the cluster version reaches protocol 2 without an operator")
	l = b.leader()
	require.NotNil(t, l)
	require.Eventually(t, func() bool { return raisedAudit(l.r.table) }, 10*time.Second, 20*time.Millisecond, "the raise is audited")
	// The pilot raises to clusterProtocol, never to the highest protocol
	// the engine speaks, 3 since kitsunium/sdk#659: only a release raises
	// clusterProtocol.
	for _, n := range founders {
		require.Equal(t, clusterProtocol, n.r.Engine().Status().ClusterVersion, n.slot)
	}

	// The engine's refusals reach Forgejo under its own sentinels.
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	eng := l.r.Engine()
	st := eng.Status()
	pre := consensus.Precondition{Identity: st.Identity, Generation: st.ClusterGeneration}
	beyond := l.node.Status().Versions.ProtocolMax + 1
	require.ErrorIs(t, eng.RaiseClusterVersion(ctx, autopilotOperator, pre, beyond), consensus.ErrVersionUnsupported, "a protocol this engine does not speak")
	require.ErrorIs(t, eng.RaiseClusterVersion(ctx, autopilotOperator, consensus.Precondition{Identity: st.Identity}, 2), consensus.ErrAdminPrecondition, "a stale generation")

	// n1, a follower, stops.
	var n1 *tcpNode
	for _, n := range founders {
		if n != l {
			n1 = n
			break
		}
	}
	b.down(n1)

	// n4 is granted, joins as a learner, is promoted and made leader.
	require.NoError(t, l.r.Operate(ctx, OperatorFor(1), Operation{Action: ActionAuthorizeJoin, Target: "node-4"}))
	n4 := b.stamp("node-4")
	rep, err := sdk.Inspect(n4.dd)
	require.NoError(t, err)
	b.start(n4)
	var addrs []string
	for _, n := range founders {
		if n.up {
			addrs = append(addrs, n.addr)
		}
	}
	require.Eventually(t, func() bool {
		jctx, jcancel := context.WithTimeout(ctx, 3*time.Second)
		defer jcancel()
		_, err := n4.tr.Join(jctx, sdk.JoinConfig{Addresses: addrs, Stamp: rep.Stamp, Address: n4.addr, Pins: pins})
		return err == nil
	}, 20*time.Second, 100*time.Millisecond, "node-4 joins")
	const op consensus.Operator = "op:test"
	require.Eventually(t, func() bool {
		l := b.leader()
		return l != nil && l.r.Engine().Promote(ctx, op, consensus.NodeID(n4.id)) == nil
	}, 20*time.Second, 50*time.Millisecond, "node-4 is promoted")
	require.Eventually(t, func() bool {
		l := b.leader()
		if l == n4 {
			return true
		}
		if l != nil {
			_ = l.r.Engine().TransferLeadership(ctx, op, consensus.NodeID(n4.id))
		}
		return b.leader() == n4
	}, 20*time.Second, 50*time.Millisecond, "node-4 leads")
	assert.Equal(t, uint32(2), n4.r.Engine().Status().ClusterVersion)
	term := n4.node.Status().Term

	// n1 comes back and must apply what n4 writes.
	b.restart(n1)
	data, err := reftable.Encode(reftable.KindAudit, &reftable.Audit{
		ID: "returned-voter", Operator: string(op), Action: "probe", Outcome: "ok", Time: time.Now().Unix(),
	})
	require.NoError(t, err)
	tok, res, err := n4.r.proposeCommand(ctx, data, func() bool { return n4.r.table.HasAudit("returned-voter") })
	require.NoError(t, err)
	require.True(t, res.OK, res.Reason)
	wait := 4*b.timing.ElectionTimeoutMax + 5*time.Second
	for deadline := time.Now().Add(wait); n1.node.Status().Applied < tok.Index || !n1.r.table.HasAudit("returned-voter"); time.Sleep(10 * time.Millisecond) {
		if time.Now().After(deadline) {
			t.Fatalf("cluster version %d: the returned voter never applied the new leader's write within %v: %+v",
				n4.r.Engine().Status().ClusterVersion, wait, n1.node.Status())
		}
	}
	assert.Equal(t, term, tok.Term, "the write is node-4's, in the term it was handed the leadership")
	assert.NotZero(t, n1.node.Status().Leader, "the returned voter knows its leader")
}

// While a member does not announce the protocol, the engine refuses the
// raise: the autopilot carries on with the membership, asks again once per
// payloadProbeEvery, and raises the version once the member does. The SDK
// runs a node of the previous protocol in its own tests only, so the
// refusal is the double's (#170).
func TestTheAutopilotRetriesTheClusterVersionUntilEveryMemberSpeaksIt(t *testing.T) {
	r, e, clock := newLeaderBench(t, 3)
	e.raiseErr = fmt.Errorf("%w: a member does not announce protocol 2", consensus.ErrVersionUnsupported)
	ctx := t.Context()
	r.autopilotStep(ctx)
	require.Equal(t, []uint32{2}, e.raises, "the leader asks at its first pass")
	for range 10 {
		clock.advance(time.Second)
		r.autopilotStep(ctx)
	}
	assert.Len(t, e.raises, 1, "not again before payloadProbeEvery")
	assert.Zero(t, e.Status().ClusterVersion)

	// A learner joins meanwhile: the refusal does not stop the autopilot.
	e.join(consensus.Member{ID: 0xb, Slot: "node-b", SPKI: [32]byte{0xb}})
	e.join(consensus.Member{ID: 0xc, Slot: "node-c", SPKI: [32]byte{0xc}})
	r.autopilotStep(ctx)
	assert.Equal(t, 3, e.voters(), "the learners are promoted")

	clock.advance(payloadProbeEvery)
	r.autopilotStep(ctx)
	require.Len(t, e.raises, 2, "asked again after payloadProbeEvery")
	assert.Zero(t, e.Status().ClusterVersion)

	e.mu.Lock()
	e.raiseErr = nil
	e.mu.Unlock()
	clock.advance(payloadProbeEvery)
	r.autopilotStep(ctx)
	assert.Equal(t, uint32(2), e.Status().ClusterVersion, "raised once every member speaks it")
	for range 3 {
		clock.advance(payloadProbeEvery)
		r.autopilotStep(ctx)
	}
	assert.Len(t, e.raises, 3, "never asked again once raised")
}

// The pilot of the manual mode waits as the autopilot does: a member on an
// older binary keeps the version at 0 and the cluster writing, and the
// version is raised once that member speaks the protocol.
func TestTheManualPilotWaitsForEveryMemberToSpeakTheProtocol(t *testing.T) {
	tc := newTestCluster(t, "node-a", "node-b", "node-c")
	tc.mem.SetProtocol(tc.nodes[2].node, 1)
	ctx, cancel := context.WithCancel(tc.ctx)
	var wg sync.WaitGroup
	for _, n := range tc.nodes {
		wg.Go(func() { n.r.leaderPilot(ctx, 10*time.Millisecond) })
	}
	t.Cleanup(func() { cancel(); wg.Wait() })

	time.Sleep(200 * time.Millisecond)
	for _, n := range tc.nodes {
		assert.Zero(t, n.r.Engine().Status().ClusterVersion, n.slot)
	}
	data, err := reftable.Encode(reftable.KindAudit, &reftable.Audit{ID: "still-writing", Operator: "op:test", Action: "probe", Outcome: "ok", Time: time.Now().Unix()})
	require.NoError(t, err)
	_, _, err = tc.nodes[1].r.Engine().Propose(ctx, data)
	require.NoError(t, err, "the cluster still writes")

	tc.mem.SetProtocol(tc.nodes[2].node, memraft.ProtocolMax)
	require.Eventually(t, func() bool {
		for _, n := range tc.nodes {
			if n.r.Engine().Status().ClusterVersion != 2 || !raisedAudit(n.r.table) {
				return false
			}
		}
		return true
	}, 10*time.Second, 10*time.Millisecond, "raised and audited once the last member speaks protocol 2")
	count := 0
	for _, a := range tc.nodes[0].r.table.Audit() {
		if a.Action == string(ActionRaiseCluster) {
			count++
		}
	}
	assert.Equal(t, 1, count, "raised once, by the leader alone")
}
