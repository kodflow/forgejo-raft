// Copyright 2026 The Forgejo Authors. All rights reserved.
// SPDX-License-Identifier: GPL-3.0-or-later

package cluster

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"forgejo.org/services/cluster/consensus"
	"forgejo.org/services/cluster/consensus/kitsunium"
	"forgejo.org/services/cluster/nomadgate"

	sdk "github.com/kitsunium/sdk/pkg/v1/app/consensus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// startSDKEngines founds a cluster of the SDK's real engine over its
// in-memory network, each node applying to its replicator's table.
func startSDKEngines(t *testing.T, cluster consensus.ClusterID, nodes []*testNode) {
	network := sdk.NewMemoryNetwork()
	type parts struct {
		log   *sdk.MemoryLog
		snaps *sdk.MemorySnapshots
		id    sdk.NodeID
	}
	var ps []parts
	var members []sdk.MemberValue
	for _, n := range nodes {
		p := parts{log: sdk.NewMemoryLog(), snaps: sdk.NewMemorySnapshots()}
		id, inc, err := sdk.InitLog(p.log, sdk.ClusterID(cluster), n.slot)
		require.NoError(t, err)
		p.id = id
		ps = append(ps, p)
		members = append(members, sdk.MemberValue{ID: id, Incarnation: inc, Slot: n.slot, SPKI: sdk.MemorySPKI(id), Address: n.addr, Voter: true})
	}
	timing := sdk.TimingContractSpec{
		HeartbeatInterval:  10 * time.Millisecond,
		ElectionTimeoutMin: 100 * time.Millisecond, ElectionTimeoutMax: 200 * time.Millisecond,
	}
	for i, n := range nodes {
		cfg := sdk.Config{
			Cluster: sdk.ClusterID(cluster), Machine: kitsunium.Machine(n.r.table), Durability: sdk.DurabilityVolatile,
			Log: ps[i].log, Snapshots: ps[i].snaps, Transport: sdk.NewMemoryTransport(network, ps[i].id), InitialTiming: timing,
			MaxSnapshotBytes: 1 << 30, MaxLogBytes: 1 << 30, MinVoters: 3, SnapshotPausesApply: true,
		}
		require.NoError(t, sdk.Bootstrap(context.Background(), &cfg, members))
		node, err := sdk.NewNode(cfg)
		require.NoError(t, err)
		t.Cleanup(func() { _ = node.Close(context.Background()) })
		n.r.setEngine(kitsunium.Wrap(node, n.slot))
	}
}

func TestThreeSDKNodesReplicateAPush(t *testing.T) {
	tc := newTestClusterOn(t, true, "node-a", "node-b", "node-c")
	work := t.TempDir()
	run(t, work, "init", "-q")
	c1 := commit(t, work, "one")
	c2 := commit(t, work, "two")
	zero := strings.Repeat("0", 40)

	// Any node takes the push: a follower forwards the proposal.
	var resp bool
	require.Eventually(t, func() bool {
		resp = tc.pushVia(tc.nodes[2], work, "alice/demo.git", "refs/heads/main", zero, c1).OK
		return resp
	}, 10*time.Second, 50*time.Millisecond)
	for _, n := range tc.nodes {
		require.Eventually(t, func() bool { return gitRef(t, n, "alice/demo.git", "refs/heads/main") == c1 },
			10*time.Second, 20*time.Millisecond, "projection on %s", n.slot)
	}
	require.True(t, tc.pushVia(tc.nodes[0], work, "alice/demo.git", "refs/heads/main", c1, c2).OK)
	for _, n := range tc.nodes {
		require.Eventually(t, func() bool { return gitRef(t, n, "alice/demo.git", "refs/heads/main") == c2 },
			10*time.Second, 20*time.Millisecond, "projection on %s", n.slot)
	}
	// A stale push is refused, whichever node takes it.
	assert.False(t, tc.pushVia(tc.nodes[1], work, "alice/demo.git", "refs/heads/main", c1, c1).OK)

	// The table is the same everywhere once applied.
	require.Eventually(t, func() bool {
		return tc.nodes[0].r.table.Root() == tc.nodes[1].r.table.Root() && tc.nodes[1].r.table.Root() == tc.nodes[2].r.table.Root()
	}, 5*time.Second, 10*time.Millisecond)

	// Exactly one consumer, the leader's, derived the two pushes.
	require.Eventually(t, func() bool {
		total := 0
		for _, n := range tc.nodes {
			total += n.work.count()
		}
		return total == 2
	}, 10*time.Second, 20*time.Millisecond)

	// An operation submitted on a follower moves the leadership, audited.
	var leader, follower *testNode
	for _, n := range tc.nodes {
		if n.r.Engine().Status().Role == consensus.RoleLeader {
			leader = n
		} else if follower == nil {
			follower = n
		}
	}
	require.NotNil(t, leader)
	// The leader knows the tolerance and each peer's progress; the view
	// shows them.
	require.Eventually(t, func() bool { return leader.r.Engine().Health().FailureTolerance == 1 }, 5*time.Second, 10*time.Millisecond)
	view := leader.r.View(context.Background())
	assert.Equal(t, 1, view.FailureTolerance)
	known := 0
	for _, m := range view.Members {
		if m.Known {
			known++
			assert.False(t, m.Unreachable, m.Slot)
		}
	}
	assert.Equal(t, 2, known)
	assert.Equal(t, -1, follower.r.Engine().Health().FailureTolerance)
	target := follower.r.Engine().Status().ID.String()
	err := follower.r.Operate(context.Background(), OperatorFor(1), Operation{Action: ActionTransfer, Target: target})
	require.NoError(t, err, "a transfer submitted on a follower")
	require.Eventually(t, func() bool { return follower.r.Engine().Status().Role == consensus.RoleLeader }, 5*time.Second, 10*time.Millisecond)
	require.Eventually(t, func() bool {
		a := leader.r.table.Audit()
		return len(a) >= 2 && a[0].Action == "transfer-leadership" && a[0].Outcome == "ok" && a[1].Outcome == "requested"
	}, 5*time.Second, 10*time.Millisecond)
}

func joinAuthorizerFor(r *Replicator) sdk.JoinAuthorizer {
	return kitsunium.JoinAuthorizer(r.authorizeJoin)
}

// joinBench is three founders on the SDK's real engine over its in-memory
// network, and a node stamped joining under joinerSlot.
type joinBench struct {
	founders   []*Replicator
	pins       [][32]byte
	join       func() error
	joinPinned func(pins [][32]byte) error
}

func newJoinBench(t *testing.T, joinerSlot string) *joinBench {
	network := sdk.NewMemoryNetwork()
	cluster := sdk.ClusterID{0x0f, 0x07, 0x9e, 0x11}
	timing := sdk.TimingContractSpec{HeartbeatInterval: 10 * time.Millisecond, ElectionTimeoutMin: 100 * time.Millisecond, ElectionTimeoutMax: 200 * time.Millisecond}
	type part struct {
		log   *sdk.MemoryLog
		snaps *sdk.MemorySnapshots
		id    sdk.NodeID
		r     *Replicator
		tr    *sdk.MemoryTransport
	}
	newPart := func(slot string) *part {
		p := &part{log: sdk.NewMemoryLog(), snaps: sdk.NewMemorySnapshots(), r: newReplicator(t.TempDir(), newRegistry(time.Second, 8, 64))}
		id, _, err := sdk.InitLog(p.log, cluster, slot)
		require.NoError(t, err)
		p.id = id
		p.tr = sdk.NewMemoryTransport(network, id)
		return p
	}
	cfg := func(p *part) sdk.Config {
		return sdk.Config{
			Cluster: cluster, Machine: kitsunium.Machine(p.r.table), Durability: sdk.DurabilityVolatile,
			Log: p.log, Snapshots: p.snaps, Transport: p.tr, InitialTiming: timing,
			MaxSnapshotBytes: 1 << 30, MaxLogBytes: 1 << 30, MinVoters: 3, SnapshotPausesApply: true,
			JoinAuthorizer: joinAuthorizerFor(p.r),
		}
	}
	b := &joinBench{}
	var founders []*part
	var members []sdk.MemberValue
	for _, slot := range []string{"node-a", "node-b", "node-c"} {
		p := newPart(slot)
		founders = append(founders, p)
		b.founders = append(b.founders, p.r)
		st, _ := p.log.Stamp()
		members = append(members, sdk.MemberValue{ID: p.id, Incarnation: st.Incarnation, Slot: slot, SPKI: sdk.MemorySPKI(p.id), Voter: true})
		b.pins = append(b.pins, sdk.MemorySPKI(p.id))
	}
	for _, p := range founders {
		require.NoError(t, sdk.Bootstrap(context.Background(), new(cfg(p)), members))
		n, err := sdk.NewNode(cfg(p))
		require.NoError(t, err)
		t.Cleanup(func() { _ = n.Close(context.Background()) })
		p.r.setEngine(kitsunium.Wrap(n, "x"))
	}
	require.Eventually(t, func() bool { return founders[0].r.Engine().Status().Leader != 0 }, 5*time.Second, 10*time.Millisecond)

	joiner := newPart(joinerSlot)
	n, err := sdk.NewNode(cfg(joiner))
	require.NoError(t, err)
	t.Cleanup(func() { _ = n.Close(context.Background()) })
	stamp, _ := joiner.log.Stamp()
	b.joinPinned = func(pins [][32]byte) error {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_, err := joiner.tr.Join(ctx, sdk.JoinConfig{Addresses: []string{"any"}, Stamp: stamp, Pins: pins})
		return err
	}
	b.join = func() error { return b.joinPinned(b.pins) }
	return b
}

func (b *joinBench) learner(slot string) bool {
	for _, m := range b.founders[0].Engine().Membership().Members {
		if m.Slot == slot {
			return !m.Voter
		}
	}
	return false
}

func (b *joinBench) grant(t *testing.T, target string) {
	slot, _, _ := strings.Cut(target, ":")
	require.NoError(t, b.founders[1].Operate(context.Background(), OperatorFor(1), Operation{Action: ActionAuthorizeJoin, Target: target}))
	require.Eventually(t, func() bool { _, ok := b.founders[0].table.JoinGrant(slot); return ok }, 5*time.Second, 10*time.Millisecond)
}

// A JOIN over the TCP transport trusts only the members it pins: none
// pinned, or only a key no member holds, and it is refused, granted or not;
// the members' own keys admit it.
func TestSelfJoinOverTCPTrustsOnlyPinnedMembers(t *testing.T) {
	pki := newPKI(t)
	cluster := sdk.ClusterID{0x0f, 0x07, 0x9e, 0x12}
	timing := sdk.TimingContractSpec{HeartbeatInterval: 20 * time.Millisecond, ElectionTimeoutMin: 200 * time.Millisecond, ElectionTimeoutMax: 400 * time.Millisecond}
	type part struct {
		dd sdk.DataDir
		tr *sdk.TCPTransport
		r  *Replicator
	}
	open := func(slot string) (*part, sdk.NodeID, sdk.Incarnation, [32]byte) {
		file, spki := pki.issue(t, cluster.String(), slot)
		p := &part{r: newReplicator(t.TempDir(), newRegistry(time.Second, 8, 64))}
		dd, err := sdk.NewDataDir(filepath.Join(t.TempDir(), "data"))
		require.NoError(t, err)
		p.dd = dd
		tr, err := sdk.NewTCPTransportFromPEM(file, file, pki.caFile, &sdk.TransportConfig{ListenAddress: "127.0.0.1:0"})
		require.NoError(t, err)
		p.tr = tr
		id, inc, err := sdk.Init(dd, cluster, slot)
		require.NoError(t, err)
		return p, id, inc, spki
	}
	cfg := func(p *part) sdk.Config {
		return sdk.Config{
			Cluster: cluster, Machine: kitsunium.Machine(p.r.table), Durability: sdk.DurabilityVolatile,
			Log: p.dd.Log(), Snapshots: p.dd.Snapshots(), Transport: p.tr, InitialTiming: timing,
			MaxSnapshotBytes: 1 << 30, MaxLogBytes: 1 << 30, SnapshotPausesApply: true,
			JoinAuthorizer: joinAuthorizerFor(p.r),
		}
	}
	var founders []*part
	var members []sdk.MemberValue
	var pins [][32]byte
	for _, slot := range []string{"node-a", "node-b", "node-c"} {
		p, id, inc, spki := open(slot)
		founders = append(founders, p)
		members = append(members, sdk.MemberValue{ID: id, Incarnation: inc, Slot: slot, SPKI: spki, Address: p.tr.Addr(), Voter: true})
		pins = append(pins, spki)
	}
	for _, p := range founders {
		c := cfg(p)
		require.NoError(t, sdk.Bootstrap(context.Background(), &c, members))
		n, err := sdk.NewNode(c)
		require.NoError(t, err)
		t.Cleanup(func() {
			_ = n.Close(context.Background())
			_ = p.tr.Close()
			_ = p.dd.Close()
		})
		p.r.setEngine(kitsunium.Wrap(n, "x"))
	}
	require.Eventually(t, func() bool { return founders[0].r.Engine().Status().Leader != 0 }, 10*time.Second, 20*time.Millisecond)
	require.NoError(t, founders[1].r.Operate(context.Background(), OperatorFor(1), Operation{Action: ActionAuthorizeJoin, Target: "node-d"}))
	require.Eventually(t, func() bool { _, ok := founders[0].r.table.JoinGrant("node-d"); return ok }, 5*time.Second, 10*time.Millisecond)

	joiner, _, _, _ := open("node-d")
	t.Cleanup(func() { _ = joiner.tr.Close(); _ = joiner.dd.Close() })
	rep, err := sdk.Inspect(joiner.dd)
	require.NoError(t, err)
	addrs := []string{founders[0].tr.Addr(), founders[1].tr.Addr(), founders[2].tr.Addr()}
	join := func(pins [][32]byte) error {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		_, err := joiner.tr.Join(ctx, sdk.JoinConfig{Addresses: addrs, Stamp: rep.Stamp, Address: joiner.tr.Addr(), Pins: pins})
		return err
	}
	learner := func() bool {
		for _, m := range founders[0].r.Engine().Membership().Members {
			if m.Slot == "node-d" {
				return true
			}
		}
		return false
	}
	err = join(nil)
	require.ErrorContains(t, err, "TRANSPORT_MISCONFIGURED", "no pin: refused before any answer counts")
	err = join([][32]byte{{0xde, 0xad}})
	require.ErrorContains(t, err, "PEER_REJECTED", "a key no member holds: no answer counts")
	assert.False(t, learner())
	require.Eventually(t, func() bool { return join(pins) == nil }, 10*time.Second, 100*time.Millisecond)
	require.Eventually(t, learner, 5*time.Second, 10*time.Millisecond, "the members' own keys: admitted")
}

// A new node joins by itself only once an operator granted its slot; the
// fence decision is Forgejo's (ADR 0152 D19, R8-09).
func TestSelfJoinNeedsAnOperatorGrant(t *testing.T) {
	b := newJoinBench(t, "node-d")
	require.Error(t, b.join(), "no grant: refused")

	// A grant for another key is refused too.
	b.grant(t, "node-d:"+strings.Repeat("ab", 32))
	require.Error(t, b.join(), "grant for another key: refused")

	b.grant(t, "node-d")
	require.Eventually(t, func() bool { return b.join() == nil }, 5*time.Second, 50*time.Millisecond)
	require.Eventually(t, func() bool { return b.learner("node-d") }, 5*time.Second, 10*time.Millisecond, "admitted as a learner")
}

// fakeNomadNodes is a read-only Nomad API with one node carrying slot.
type fakeNomadNodes struct {
	mu    sync.Mutex
	slot  string
	drain bool
}

func (f *fakeNomadNodes) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	f.mu.Lock()
	defer f.mu.Unlock()
	switch r.URL.Path {
	case "/v1/nodes":
		_, _ = io.WriteString(w, `[{"ID":"n1"}]`)
	case "/v1/node/n1":
		elig := "eligible"
		if f.drain {
			elig = "ineligible"
		}
		_, _ = fmt.Fprintf(w, `{"ID":"n1","Name":"host-d","Status":"ready","SchedulingEligibility":%q,"Drain":%t,"Meta":{"forgejo_slot":%q}}`, elig, f.drain, f.slot)
	default:
		http.NotFound(w, r)
	}
}

// A granted JOIN is refused while the joiner's Nomad node drains, and
// admitted once it is eligible again. With NOMAD_ADDR set, the gate reads
// that real Nomad (contrib/cluster-lab `make nomad-e2e`), whose node the
// script drained or not: NOMAD_E2E_EXPECT says which, NOMAD_E2E_SLOT is the
// slot its meta names, NOMAD_TOKEN_FILE its ACL token.
func TestSelfJoinRefusedWhileTheNomadNodeDrains(t *testing.T) {
	if addr := os.Getenv("NOMAD_ADDR"); addr != "" {
		slot := os.Getenv("NOMAD_E2E_SLOT")
		b := newJoinBench(t, slot)
		gate := &nomadgate.Gate{Addr: addr, TokenFile: os.Getenv("NOMAD_TOKEN_FILE"), SlotMeta: "forgejo_slot"}
		for _, r := range b.founders {
			r.gate = gate
		}
		b.grant(t, slot)
		switch os.Getenv("NOMAD_E2E_EXPECT") {
		case "refused":
			err := b.join()
			require.Error(t, err)
			// The joiner reads the authorizer's own reason.
			why, ok := sdk.JoinRefusalReason(err)
			require.True(t, ok, "%v", err)
			assert.Contains(t, why, os.Getenv("NOMAD_E2E_REASON"))
			t.Logf("JOIN of %s refused, the joiner reads: %s", slot, why)
			time.Sleep(500 * time.Millisecond)
			assert.False(t, b.learner(slot), "not admitted")
		case "admitted":
			require.Eventually(t, func() bool { return b.join() == nil }, 5*time.Second, 50*time.Millisecond)
			require.Eventually(t, func() bool { return b.learner(slot) }, 5*time.Second, 10*time.Millisecond)
			t.Logf("JOIN of %s admitted as a learner", slot)
		default:
			t.Fatal("NOMAD_E2E_EXPECT must be refused or admitted")
		}
		return
	}

	nomad := &fakeNomadNodes{slot: "node-d", drain: true}
	srv := httptest.NewServer(nomad)
	defer srv.Close()
	b := newJoinBench(t, "node-d")
	for _, r := range b.founders {
		r.gate = &nomadgate.Gate{Addr: srv.URL, SlotMeta: "forgejo_slot"}
	}
	b.grant(t, "node-d")
	err := b.join()
	require.Error(t, err, "granted, but the Nomad node drains")
	why, ok := sdk.JoinRefusalReason(err)
	require.True(t, ok, "%v", err)
	assert.Contains(t, why, "the Nomad node of slot node-d")
	assert.NotContains(t, why, "host-d", "the node name stays in the member's log")
	assert.Contains(t, why, "is draining")
	assert.False(t, b.learner("node-d"))

	nomad.mu.Lock()
	nomad.drain = false
	nomad.mu.Unlock()
	require.Eventually(t, func() bool { return b.join() == nil }, 5*time.Second, 50*time.Millisecond)
	require.Eventually(t, func() bool { return b.learner("node-d") }, 5*time.Second, 10*time.Millisecond, "admitted once eligible")
}

// With one of three nodes stopped, the leader included, a push on a
// survivor succeeds within seconds: objects return at majority, and the
// proposal waits for the new leader instead of failing.
func TestPushSurvivesAStoppedNode(t *testing.T) {
	for _, stopLeader := range []bool{false, true} {
		name := "a follower stopped"
		if stopLeader {
			name = "the leader stopped"
		}
		t.Run(name, func(t *testing.T) {
			tc := newTestClusterOn(t, true, "node-a", "node-b", "node-c")
			work := t.TempDir()
			run(t, work, "init", "-q")
			c1 := commit(t, work, "one")
			zero := strings.Repeat("0", 40)

			var victim *testNode
			var survivors []*testNode
			for _, n := range tc.nodes {
				isLeader := n.r.Engine().Status().Role == consensus.RoleLeader
				if victim == nil && isLeader == stopLeader {
					victim = n
				} else {
					survivors = append(survivors, n)
				}
			}
			require.NotNil(t, victim)
			require.NoError(t, victim.r.Engine().Close(context.Background()))
			_ = victim.srv.Close()

			start := time.Now()
			resp := tc.pushVia(survivors[0], work, "s/r.git", "refs/heads/main", zero, c1)
			require.True(t, resp.OK, resp.Message)
			assert.Less(t, time.Since(start), 10*time.Second)
			for _, n := range survivors {
				require.Eventually(t, func() bool { return gitRef(t, n, "s/r.git", "refs/heads/main") == c1 },
					10*time.Second, 20*time.Millisecond, n.slot)
			}
		})
	}
}

// Repository rename and delete, the audit, join grants and event trimming
// go through the same retry: with the leader stopped, they succeed on a
// survivor once the two others elect a new leader.
func TestCommandsSurviveAStoppedLeader(t *testing.T) {
	tc := newTestClusterOn(t, true, "node-a", "node-b", "node-c")
	work := t.TempDir()
	run(t, work, "init", "-q")
	c1 := commit(t, work, "one")
	var leader *testNode
	var survivors []*testNode
	for _, n := range tc.nodes {
		if leader == nil && n.r.Engine().Status().Role == consensus.RoleLeader {
			leader = n
		} else {
			survivors = append(survivors, n)
		}
	}
	require.NotNil(t, leader)
	require.True(t, tc.pushVia(survivors[0], work, "l/old.git", "refs/heads/main", strings.Repeat("0", 40), c1).OK)

	require.NoError(t, leader.r.Engine().Close(context.Background()))
	_ = leader.srv.Close()
	s := survivors[0]
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	start := time.Now()
	require.NoError(t, s.r.proposeRepoRename(ctx, "l/old.git", "l/new.git"))
	assert.Less(t, time.Since(start), 10*time.Second)
	require.NoError(t, s.r.proposeRepoDelete(ctx, "l/new.git", 0))
	require.Eventually(t, func() bool { _, deleted := survivors[1].r.table.Deleted("l/new.git"); return deleted }, 5*time.Second, 10*time.Millisecond)

	require.NoError(t, s.r.Operate(ctx, OperatorFor(1), Operation{Action: ActionAuthorizeJoin, Target: "node-d"}))
	_, ok := s.r.table.JoinGrant("node-d")
	assert.True(t, ok)
	audit := s.r.table.Audit()
	require.GreaterOrEqual(t, len(audit), 2)
	assert.Equal(t, "ok", audit[0].Outcome)
	// Each record once, whatever the retries.
	seen := map[string]bool{}
	for _, a := range audit {
		assert.False(t, seen[a.ID], "audit record %s twice", a.ID)
		seen[a.ID] = true
	}
}
