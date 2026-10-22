// Copyright 2026 The Forgejo Authors. All rights reserved.
// SPDX-License-Identifier: GPL-3.0-or-later

package cluster

import (
	"context"
	"errors"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"forgejo.org/services/cluster/consensus"
	"forgejo.org/services/cluster/consensus/kitsunium"

	sdk "github.com/kitsunium/sdk/pkg/v1/app/consensus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ---- clusters that form by themselves, on the SDK's engine ----

// fakeClock is the autopilot's clock in tests: DEAD_MEMBER_TIMEOUT passes
// when the test says so.
type fakeClock struct {
	mu sync.Mutex
	t  time.Time
}

func (c *fakeClock) now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.t
}

func (c *fakeClock) advance(d time.Duration) {
	c.mu.Lock()
	c.t = c.t.Add(d)
	c.mu.Unlock()
}

// autoBench forms a cluster the way the autonomous mode does, over the
// SDK's in-memory network: each node stamps a data directory, runs the
// founding rule against what the others answer, founds or joins, and runs
// the autopilot. Only the wire is simulated: the answers come from each
// node's helloOf, the keys from the memory transport. A node can be
// killed (its engine closed, its answers gone) and restarted on its data
// directory; a retired node rejoins as Init's rejoin does.
type autoBench struct {
	t        *testing.T
	ctx      context.Context
	network  *sdk.MemoryNetwork
	cluster  sdk.ClusterID
	expected int
	timing   sdk.TimingContractSpec
	clock    *fakeClock
	// hooks are the writer hooks by slot, set before the autopilot starts.
	hooks map[string]WriterHook

	mu     sync.Mutex
	byAddr map[string]*autoNode
	nodes  []*autoNode
}

// autoNode is one node; the fields after mu change on a rejoin, from the
// autopilot's goroutine, and are read under the bench's lock.
type autoNode struct {
	n       *testNode
	addr    string
	dir     string
	cluster sdk.ClusterID

	dd      sdk.DataDir
	tr      *sdk.MemoryTransport
	node    *sdk.Node
	id      sdk.NodeID
	down    bool
	retired []string
	// stop ends the node's autopilot and JOIN loop, as a crash does.
	stop func()
}

func (an *autoNode) r() *Replicator { return an.n.r }

// Dead-member timing of the bench: out of reach after 300 ms without a
// word, removed after five minutes of the fake clock.
const (
	benchDownAfter   = 300 * time.Millisecond
	benchDeadTimeout = 5 * time.Minute
)

func newAutoCluster(t *testing.T, expected int, slots ...string) (*testCluster, *autoBench) {
	return newAutoClusterWith(t, expected, nil, slots...)
}

func newAutoClusterWith(t *testing.T, expected int, hooks map[string]WriterHook, slots ...string) (*testCluster, *autoBench) {
	b := &autoBench{
		t: t, network: sdk.NewMemoryNetwork(), cluster: sdk.ClusterID{0x0f, 0x07, 0x9e, 0x30}, expected: expected,
		timing: sdk.TimingContractSpec{
			HeartbeatInterval:  10 * time.Millisecond,
			ElectionTimeoutMin: 100 * time.Millisecond, ElectionTimeoutMax: 200 * time.Millisecond,
		},
		clock: &fakeClock{t: time.Date(2026, 10, 17, 12, 0, 0, 0, time.UTC)},
		hooks: hooks, byAddr: map[string]*autoNode{},
	}
	tc := newTestClusterWith(t, consensus.ClusterID(b.cluster), func(tc *testCluster, _ map[string][32]byte) {
		b.ctx = tc.ctx
		b.formAll(tc.nodes)
	}, slots...)
	return tc, b
}

// probe answers as the node at addr would on its object channel.
func (b *autoBench) probe(_ context.Context, addr, _ string) (hello, error) {
	b.mu.Lock()
	an := b.byAddr[addr]
	var id sdk.NodeID
	if an != nil {
		id = an.id
	}
	b.mu.Unlock()
	if an == nil {
		return hello{}, errors.New("connection refused")
	}
	h := an.r().helloOf()
	h.SPKI = sdk.MemorySPKI(id)
	return h, nil
}

func (b *autoBench) config(an *autoNode, minVoters int) sdk.Config {
	return sdk.Config{
		Cluster: an.cluster, Machine: kitsunium.Machine(an.r().table), Durability: sdk.DurabilityVolatile,
		Log: an.dd.Log(), Snapshots: an.dd.Snapshots(), Transport: an.tr, InitialTiming: b.timing,
		MaxSnapshotBytes: 1 << 30, MaxLogBytes: 1 << 30, MinVoters: minVoters, SnapshotPausesApply: true,
		JoinAuthorizer: kitsunium.JoinAuthorizer(an.r().authorizeJoin),
	}
}

func (b *autoBench) formAll(nodes []*testNode) {
	t := b.t
	peers := map[string]string{}
	for _, n := range nodes {
		peers[n.slot] = n.slot + ".forgejo.test:7480"
	}
	var wg sync.WaitGroup
	for _, n := range nodes {
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
	for _, an := range b.nodes {
		require.NotNil(t, an.r().Engine(), an.n.slot)
		if h, ok := b.hooks[an.n.slot]; ok {
			an.r().writerHook = h
		}
		b.startAutopilot(an)
	}
	b.waitMembers(len(nodes), len(nodes))
}

// form is Init's part of the autonomous mode for one node: stamp, decide,
// found or join, open.
func (b *autoBench) form(an *autoNode) {
	t := b.t
	dd, err := sdk.NewDataDir(an.dir)
	require.NoError(t, err)
	id, inc, err := sdk.Init(dd, an.cluster, an.n.slot)
	require.NoError(t, err)
	an.r().auto.noteDisk(false)
	b.mu.Lock()
	an.dd, an.id, an.tr = dd, id, sdk.NewMemoryTransport(b.network, id)
	b.byAddr[an.addr] = an
	b.mu.Unlock()

	self := sdk.MemberValue{ID: id, Incarnation: inc, Slot: an.n.slot, SPKI: sdk.MemorySPKI(id), Address: an.addr, Voter: true}
	require.NoError(t, an.r().auto.formLoop(b.ctx, func(ctx context.Context) error {
		return sdk.Bootstrap(ctx, new(b.config(an, 1)), []sdk.MemberValue{self})
	}))
	require.NoError(t, b.open(an))
}

// startAutopilot runs the node's autopilot until kill stops it.
func (b *autoBench) startAutopilot(an *autoNode) {
	ctx, cancel := context.WithCancel(b.ctx)
	done := make(chan struct{})
	go func() {
		defer close(done)
		an.r().autopilot(ctx)
	}()
	b.mu.Lock()
	an.stop = func() { cancel(); <-done }
	b.mu.Unlock()
}

// open starts the node's engine and, while it is not a member, its JOIN
// loop, as the driver's Open does.
func (b *autoBench) open(an *autoNode) error {
	b.mu.Lock()
	cfg := b.config(an, autoMinVoters)
	dd, tr, slot := an.dd, an.tr, an.n.slot
	b.mu.Unlock()
	node, err := sdk.NewNode(cfg)
	if err != nil {
		return err
	}
	b.mu.Lock()
	an.node = node
	b.mu.Unlock()
	b.t.Cleanup(func() { _ = node.Close(context.Background()); _ = dd.Close() })
	an.r().setEngine(kitsunium.Wrap(node, slot))
	stamp, err := dd.Log().Stamp()
	if err != nil {
		return err
	}
	if stamp.State != sdk.StampJoining {
		return nil
	}
	go func() {
		for b.ctx.Err() == nil {
			b.mu.Lock()
			gone := an.down || an.tr != tr
			b.mu.Unlock()
			if gone {
				return
			}
			addrs, pins := an.r().auto.discoverForJoin(b.ctx)
			if len(addrs) > 0 && len(pins) > 0 {
				jctx, cancel := context.WithTimeout(b.ctx, 2*time.Second)
				_, err := tr.Join(jctx, sdk.JoinConfig{Addresses: addrs, Stamp: stamp, Address: an.addr, Pins: pins})
				cancel()
				if err == nil {
					return
				}
			}
			time.Sleep(50 * time.Millisecond)
		}
	}()
	return nil
}

// rejoin is rejoinUnderNewID on the bench: engine closed, data directory
// renamed aside, a new id stamped, the node opened joining.
func (b *autoBench) rejoin(an *autoNode) error {
	b.mu.Lock()
	node, dd := an.node, an.dd
	b.mu.Unlock()
	_ = node.Close(context.Background())
	_ = dd.Close()
	moved, err := retireDataDir(an.dir, time.Now())
	if err != nil {
		return err
	}
	ndd, err := sdk.NewDataDir(an.dir)
	if err != nil {
		return err
	}
	id, _, err := sdk.Init(ndd, an.cluster, an.n.slot)
	if err != nil {
		return err
	}
	an.r().auto.noteDisk(false)
	an.r().resetForRejoin()
	b.mu.Lock()
	an.retired = append(an.retired, moved)
	an.dd, an.id, an.tr = ndd, id, sdk.NewMemoryTransport(b.network, id)
	b.mu.Unlock()
	if err := b.open(an); err != nil {
		return err
	}
	go an.r().startupBarrier(b.ctx)
	return nil
}

// kill stops a node as a crash would: engine and data directory closed,
// its answers gone.
func (b *autoBench) kill(an *autoNode) {
	b.mu.Lock()
	stop := an.stop
	b.mu.Unlock()
	if stop != nil {
		stop()
	}
	b.mu.Lock()
	an.down = true
	delete(b.byAddr, an.addr)
	node, dd := an.node, an.dd
	b.mu.Unlock()
	require.NoError(b.t, node.Close(context.Background()))
	require.NoError(b.t, dd.Close())
}

// restart starts a killed node again on its data directory.
func (b *autoBench) restart(an *autoNode) {
	b.restartAfter(an, 0)
}

// restartAfter is restart with a slow start: the node answers from what
// its data directory holds for delay before its engine opens, as a node
// replaying a long log does.
func (b *autoBench) restartAfter(an *autoNode, delay time.Duration) {
	dd, err := sdk.NewDataDir(an.dir)
	require.NoError(b.t, err)
	stamp, err := dd.Log().Stamp()
	require.NoError(b.t, err)
	an.r().auto.noteDisk(stamp.State != sdk.StampJoining)
	b.mu.Lock()
	an.dd, an.tr, an.down = dd, sdk.NewMemoryTransport(b.network, an.id), false
	b.byAddr[an.addr] = an
	b.mu.Unlock()
	time.Sleep(delay)
	require.NoError(b.t, b.open(an))
	b.startAutopilot(an)
}

func (b *autoBench) idOf(an *autoNode) consensus.NodeID {
	b.mu.Lock()
	defer b.mu.Unlock()
	return consensus.NodeID(an.id)
}

func (b *autoBench) alive() []*autoNode {
	b.mu.Lock()
	defer b.mu.Unlock()
	var out []*autoNode
	for _, an := range b.nodes {
		if !an.down {
			out = append(out, an)
		}
	}
	return out
}

func (b *autoBench) leader() *autoNode {
	for _, an := range b.alive() {
		if e := an.r().Engine(); e != nil && e.Status().Role == consensus.RoleLeader {
			return an
		}
	}
	return nil
}

// follower is an alive member that does not lead.
func (b *autoBench) follower() *autoNode {
	l := b.leader()
	require.NotNil(b.t, l)
	m := l.r().Engine().Membership()
	for _, an := range b.alive() {
		if an != l && Rank(m, b.idOf(an)) > 0 {
			return an
		}
	}
	b.t.Fatal("no follower")
	return nil
}

// waitMembers waits until the leader's membership holds members members,
// voters of them voting.
func (b *autoBench) waitMembers(members, voters int) consensus.Membership {
	var m consensus.Membership
	require.Eventually(b.t, func() bool {
		l := b.leader()
		if l == nil {
			return false
		}
		m = l.r().Engine().Membership()
		return len(m.Members) == members && m.Voters() == voters
	}, 30*time.Second, 20*time.Millisecond, "%d members, %d voters: %+v", members, voters, m)
	return m
}

// expire kills a node and lets DEAD_MEMBER_TIMEOUT pass on the fake clock,
// once the leader saw it out of reach.
func (b *autoBench) expire(an *autoNode) {
	b.kill(an)
	time.Sleep(benchDownAfter + 500*time.Millisecond)
	b.clock.advance(benchDeadTimeout + time.Minute)
}

func voterSlots(m consensus.Membership) (voters, learners []string) {
	for _, x := range m.Members {
		if x.Voter {
			voters = append(voters, x.Slot)
		} else {
			learners = append(learners, x.Slot)
		}
	}
	return voters, learners
}

// pushOne pushes a new commit to repo through n and reports the answer.
func pushOne(tc *testCluster, n *testNode, work, repo, old, msg string) (string, bool, string) {
	return pushWithin(tc, 30*time.Second, n, work, repo, old, msg)
}

func pushWithin(tc *testCluster, budget time.Duration, n *testNode, work, repo, old, msg string) (string, bool, string) {
	sha := commit(tc.t, work, msg)
	resp := tc.pushViaWithin(budget, n, work, repo, "refs/heads/main", old, sha)
	return sha, resp.OK, resp.Message
}

// Three nodes told EXPECTED_NODES = 3 and the PEERS list form a cluster of
// three voters with nobody's help, and a push reaches all of them.
func TestAutonomousThreeFromPeers(t *testing.T) {
	tc, b := newAutoCluster(t, 3, "node-a", "node-b", "node-c")
	m := b.waitMembers(3, 3)
	assert.Equal(t, "node-a", m.Members[0].Slot, "the lowest slot founded the cluster")

	work := t.TempDir()
	run(t, work, "init", "-q")
	sha, ok, msg := pushOne(tc, tc.nodes[2], work, "auto/three.git", strings.Repeat("0", 40), "one")
	require.True(t, ok, msg)
	for _, n := range tc.nodes {
		require.Eventually(t, func() bool { return gitRef(t, n, "auto/three.git", "refs/heads/main") == sha },
			10*time.Second, 20*time.Millisecond, n.slot)
	}
	v := b.leader().r().View(context.Background())
	assert.Equal(t, 3, v.ExpectedNodes)
	assert.Equal(t, 3, v.VoterTarget)
	assert.Equal(t, 1, v.Members[0].Rank)
	assert.False(t, v.Members[0].Since.IsZero())
}

// Every live member votes: three, four and five nodes give as many voters.
func TestVotersAllLive(t *testing.T) {
	for _, n := range []int{3, 4, 5} {
		t.Run(fmt.Sprintf("N=%d", n), func(t *testing.T) {
			var slots []string
			for i := range n {
				slots = append(slots, fmt.Sprintf("node-%c", 'a'+i))
			}
			_, b := newAutoCluster(t, n, slots...)
			m := b.waitMembers(n, n)
			voters, learners := voterSlots(m)
			assert.Len(t, voters, n)
			assert.Empty(t, learners)
			// It stays so.
			time.Sleep(300 * time.Millisecond)
			assert.Equal(t, n, b.leader().r().Engine().Membership().Voters())
		})
	}
}

// A member out of reach is removed once DEAD_MEMBER_TIMEOUT passed on the
// autopilot's clock, not before; the status shows it removed, and since
// when.
func TestDeadMemberRemovedAfterTimeout(t *testing.T) {
	_, b := newAutoCluster(t, 4, "node-a", "node-b", "node-c", "node-d")
	b.waitMembers(4, 4)
	victim := b.follower()
	old := b.idOf(victim)

	b.kill(victim)
	time.Sleep(benchDownAfter + 500*time.Millisecond)
	b.clock.advance(benchDeadTimeout - time.Minute)
	time.Sleep(500 * time.Millisecond)
	assert.Len(t, b.leader().r().Engine().Membership().Members, 4, "not before the timeout")

	b.clock.advance(2 * time.Minute)
	m := b.waitMembers(3, 3)
	assert.Contains(t, m.Retired, old)
	v := b.leader().r().View(context.Background())
	i := slices.IndexFunc(v.Removed, func(x RemovedView) bool { return x.ID == old.String() })
	require.GreaterOrEqual(t, i, 0, "the status lists it removed")
	assert.Equal(t, victim.n.slot, v.Removed[i].Slot)
	assert.False(t, v.Removed[i].Since.IsZero())
}

// Five nodes fail one at a time: each failure is removed after the timeout
// down to three voters, and writes keep working after each removal. The
// third failure is not removed: two voters of three are a majority, and
// they keep writing.
func TestSequentialFailuresDownToTwo(t *testing.T) {
	tc, b := newAutoCluster(t, 5, "node-a", "node-b", "node-c", "node-d", "node-e")
	b.waitMembers(5, 5)
	work := t.TempDir()
	run(t, work, "init", "-q")
	head := strings.Repeat("0", 40)
	push := func(n *testNode, msg string) {
		sha, ok, why := pushOne(tc, n, work, "auto/down.git", head, msg)
		require.True(t, ok, "%s: %s", msg, why)
		head = sha
	}
	push(b.leader().n, "five")

	for members := 4; members >= 3; members-- {
		b.expire(b.follower())
		b.waitMembers(members, members)
		push(b.leader().n, fmt.Sprintf("down to %d", members))
	}

	// Down to two alive: not removed, three voters, and the two write.
	b.expire(b.follower())
	time.Sleep(time.Second)
	m := b.waitMembers(3, 3)
	assert.Len(t, m.Retired, 2, "the third failure stays a voter")
	assert.Len(t, b.alive(), 2)
	push(b.leader().n, "two alive, on the leader")
	for _, an := range b.alive() {
		push(an.n, "two alive, on "+an.n.slot)
	}
}

// Three voters, two die: the survivor is read-only, promotes nothing and
// removes nothing, well past many election timeouts and DEAD_MEMBER_TIMEOUT.
func TestTwoSurvivorsNoPromotion(t *testing.T) {
	tc, b := newAutoCluster(t, 3, "node-a", "node-b", "node-c")
	b.waitMembers(3, 3)
	survivor := b.follower()
	for _, an := range b.alive() {
		if an != survivor {
			b.kill(an)
		}
	}
	time.Sleep(3 * time.Second) // 15 times the longest election timeout
	b.clock.advance(time.Hour)
	time.Sleep(time.Second)

	assert.NotEqual(t, consensus.RoleLeader, survivor.r().Engine().Status().Role)
	m := survivor.r().Engine().Membership()
	assert.Len(t, m.Members, 3, "nothing removed")
	assert.Equal(t, 3, m.Voters())
	assert.Empty(t, m.Retired)

	work := t.TempDir()
	run(t, work, "init", "-q")
	_, ok, _ := pushWithin(tc, 3*time.Second, survivor.n, work, "auto/two.git", strings.Repeat("0", 40), "refused")
	assert.False(t, ok, "read-only")
}

// Four nodes, two die at once: no majority, so nothing is removed and
// nothing is written, whatever the clock says, until one returns.
func TestSimultaneousMajorityLossReadOnly(t *testing.T) {
	tc, b := newAutoCluster(t, 4, "node-a", "node-b", "node-c", "node-d")
	b.waitMembers(4, 4)
	leader := b.leader()
	var victims, survivors []*autoNode
	for _, an := range b.alive() {
		if an != leader && len(victims) < 2 {
			victims = append(victims, an)
		} else {
			survivors = append(survivors, an)
		}
	}
	b.kill(victims[0])
	b.kill(victims[1])
	time.Sleep(benchDownAfter + 500*time.Millisecond)
	b.clock.advance(time.Hour)
	time.Sleep(time.Second)

	work := t.TempDir()
	run(t, work, "init", "-q")
	zero := strings.Repeat("0", 40)
	for _, s := range survivors {
		assert.Len(t, s.r().Engine().Membership().Members, 4, "nothing removed, seen from %s", s.n.slot)
		_, ok, _ := pushWithin(tc, 3*time.Second, s.n, work, "auto/majority.git", zero, "lost on "+s.n.slot)
		assert.False(t, ok, "no write on %s", s.n.slot)
	}

	// One returns: a majority again, and writes resume.
	b.restart(victims[0])
	require.Eventually(t, func() bool { return b.leader() != nil }, 10*time.Second, 20*time.Millisecond)
	_, ok, msg := pushOne(tc, b.leader().n, work, "auto/majority.git", zero, "back")
	require.True(t, ok, msg)
	assert.Len(t, b.leader().r().Engine().Membership().Members, 4)
}

// A removed node that comes back finds, from a quorum, that its id is
// retired: its data directory is renamed and kept, it rejoins under a new
// id, and the autopilot makes it a voter again.
func TestRemovedNodeRejoinsUnderNewID(t *testing.T) {
	_, b := newAutoCluster(t, 4, "node-a", "node-b", "node-c", "node-d")
	b.waitMembers(4, 4)
	victim := b.follower()
	old := b.idOf(victim)
	b.expire(victim)
	m := b.waitMembers(3, 3)
	require.Contains(t, m.Retired, old)

	b.restart(victim)
	m = b.waitMembers(4, 4)
	now := b.idOf(victim)
	assert.NotEqual(t, old, now, "a new id")
	assert.True(t, m.Members[Rank(m, now)-1].Voter, "a voter again")
	b.mu.Lock()
	retired := slices.Clone(victim.retired)
	b.mu.Unlock()
	require.Len(t, retired, 1)
	assert.True(t, strings.HasPrefix(retired[0], victim.dir+".retired-"))
	entries, err := os.ReadDir(retired[0])
	require.NoError(t, err)
	assert.NotEmpty(t, entries, "the old data directory is kept whole")
}

// Four nodes split two and two, at several moments: at most one side ever
// commits, nothing is removed, and once healed every node holds the same
// table.
func TestPartitionNeverTwoWriters(t *testing.T) {
	tc, b := newAutoCluster(t, 4, "node-a", "node-b", "node-c", "node-d")
	b.waitMembers(4, 4)
	var works [2]string
	for i := range works {
		works[i] = t.TempDir()
		run(t, works[i], "init", "-q")
	}
	zero := strings.Repeat("0", 40)
	sides := [2][]*autoNode{{b.nodes[0], b.nodes[1]}, {b.nodes[2], b.nodes[3]}}
	cut := func(on bool) {
		for _, x := range sides[0] {
			for _, y := range sides[1] {
				b.network.Cut(x.id, y.id, on)
				b.network.Cut(y.id, x.id, on)
			}
		}
	}
	// writeBoth pushes on one node of each side at once and counts the
	// sides that committed.
	writeBoth := func(repo string, during func()) int {
		var wg sync.WaitGroup
		var mu sync.Mutex
		committed := 0
		var shas [2]string
		for i := range sides {
			shas[i] = commit(t, works[i], fmt.Sprintf("%s side %d", repo, i))
		}
		for i, side := range sides {
			wg.Go(func() {
				ok := tc.pushViaWithin(3*time.Second, side[0].n, works[i], repo, "refs/heads/main", zero, shas[i]).OK
				if ok {
					mu.Lock()
					committed++
					mu.Unlock()
				}
			})
		}
		if during != nil {
			during()
		}
		wg.Wait()
		return committed
	}
	heal := func() {
		cut(false)
		require.Eventually(t, func() bool { return b.leader() != nil }, 10*time.Second, 20*time.Millisecond)
	}

	// Cut first, then write.
	cut(true)
	assert.LessOrEqual(t, writeBoth("auto/split-1.git", nil), 1)
	heal()

	// Cut while the writes are in flight.
	assert.LessOrEqual(t, writeBoth("auto/split-2.git", func() {
		time.Sleep(20 * time.Millisecond)
		cut(true)
	}), 1)
	heal()

	// Cut, let DEAD_MEMBER_TIMEOUT pass on both sides, write, heal.
	cut(true)
	time.Sleep(benchDownAfter + 500*time.Millisecond)
	b.clock.advance(time.Hour)
	time.Sleep(500 * time.Millisecond)
	assert.LessOrEqual(t, writeBoth("auto/split-3.git", nil), 1)
	heal()

	// Flapping.
	for i := range 3 {
		cut(true)
		time.Sleep(time.Duration(50*(i+1)) * time.Millisecond)
		cut(false)
	}
	assert.LessOrEqual(t, writeBoth("auto/split-4.git", func() { cut(true) }), 1)
	heal()

	assert.Len(t, b.leader().r().Engine().Membership().Members, 4, "no side removed the other")
	require.Eventually(t, func() bool {
		root := b.nodes[0].r().table.Root()
		for _, an := range b.nodes[1:] {
			if an.r().table.Root() != root {
				return false
			}
		}
		return true
	}, 10*time.Second, 20*time.Millisecond, "one history once healed")
}

// The whole cluster reboots and the lowest slot's data directory is gone.
// The members answer from their data directories before their engines are
// open, so the wiped node-a never founds a second cluster under the same
// id: it waits, then joins, and once its old member is removed it is a
// voter again under a new id.
func TestFullRebootWithAWipedFounderNeverFoundsTwice(t *testing.T) {
	_, b := newAutoCluster(t, 4, "node-a", "node-b", "node-c", "node-d")
	m := b.waitMembers(4, 4)
	require.Equal(t, "node-a", m.Members[0].Slot)
	a := b.nodes[0]
	oldA := b.idOf(a)
	for _, an := range b.nodes {
		b.kill(an)
	}
	require.NoError(t, os.RemoveAll(a.dir), "node-a's disk is lost")

	// The members restart slowly, answering from their disks meanwhile.
	var wg sync.WaitGroup
	for _, an := range b.nodes[1:] {
		wg.Go(func() { b.restartAfter(an, 2*time.Second) })
	}

	// node-a starts on an empty directory and runs the founding rule.
	time.Sleep(100 * time.Millisecond)
	dd, err := sdk.NewDataDir(a.dir)
	require.NoError(t, err)
	id, inc, err := sdk.Init(dd, a.cluster, a.n.slot)
	require.NoError(t, err)
	a.r().auto.noteDisk(false)
	a.r().setEngine(nil)
	b.mu.Lock()
	a.dd, a.id, a.tr, a.down = dd, id, sdk.NewMemoryTransport(b.network, id), false
	b.byAddr[a.addr] = a
	b.mu.Unlock()
	founded := false
	self := sdk.MemberValue{ID: id, Incarnation: inc, Slot: a.n.slot, SPKI: sdk.MemorySPKI(id), Address: a.addr, Voter: true}
	require.NoError(t, a.r().auto.formLoop(context.Background(), func(ctx context.Context) error {
		founded = true
		return sdk.Bootstrap(ctx, new(b.config(a, 1)), []sdk.MemberValue{self})
	}))
	assert.False(t, founded, "no second founding under the same cluster id")
	require.NoError(t, b.open(a))
	b.startAutopilot(a)
	wg.Wait()

	// The old node-a is removed after the timeout; the new one joins.
	require.Eventually(t, func() bool { return b.leader() != nil }, 10*time.Second, 20*time.Millisecond)
	time.Sleep(benchDownAfter + 500*time.Millisecond)
	b.clock.advance(benchDeadTimeout + time.Minute)
	require.Eventually(t, func() bool {
		l := b.leader()
		if l == nil {
			return false
		}
		m = l.r().Engine().Membership()
		i := Rank(m, consensus.NodeID(id))
		return slices.Contains(m.Retired, oldA) && i > 0 && m.Members[i-1].Voter && len(m.Members) == 4
	}, 30*time.Second, 20*time.Millisecond, "node-a rejoins under its new id, a voter: %+v", m)
	assert.Equal(t, consensus.ClusterID(b.cluster), b.leader().r().Engine().Status().Identity.Cluster, "one lineage")
}

// fakeWriterHook records what the writer hook is asked, and fails Promote
// while failing is set.
type fakeWriterHook struct {
	mu       sync.Mutex
	failing  bool
	promoted int
	demoted  int
}

func (h *fakeWriterHook) Promote(context.Context, WriterTransition) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.failing {
		return errors.New("the database is not promoted yet")
	}
	h.promoted++
	return nil
}

func (h *fakeWriterHook) Demote(context.Context, WriterTransition) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.demoted++
	return nil
}

func (h *fakeWriterHook) counts() (promoted, demoted int) {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.promoted, h.demoted
}

// The writer hook runs before the writer accepts a write: while it fails,
// the leader refuses writes and its consumer derives nothing; once it
// succeeds, writes are accepted. The other nodes are demoted, never
// promoted.
func TestWriterHookGatesTheWriter(t *testing.T) {
	// The gate alone: no leader yields to another for a failing hook.
	prev := yieldAfterPromotions
	yieldAfterPromotions = math.MaxInt
	t.Cleanup(func() { yieldAfterPromotions = prev })
	hooks := map[string]*fakeWriterHook{"node-a": {failing: true}, "node-b": {failing: true}, "node-c": {failing: true}}
	writerHooks := map[string]WriterHook{}
	for slot, h := range hooks {
		writerHooks[slot] = h
	}
	tc, b := newAutoClusterWith(t, 3, writerHooks, "node-a", "node-b", "node-c")
	b.waitMembers(3, 3)
	leader := b.leader()
	require.NotNil(t, leader)

	work := t.TempDir()
	run(t, work, "init", "-q")
	zero := strings.Repeat("0", 40)
	_, ok, msg := pushOne(tc, leader.n, work, "auto/hook.git", zero, "one")
	require.False(t, ok)
	assert.Contains(t, msg, "not ready to write yet")
	for slot, h := range hooks {
		if slot != leader.n.slot {
			require.Eventually(t, func() bool { _, d := h.counts(); return d == 1 }, 5*time.Second, 20*time.Millisecond, "%s is demoted", slot)
		}
	}

	for _, h := range hooks {
		h.mu.Lock()
		h.failing = false
		h.mu.Unlock()
	}
	lh := hooks[leader.n.slot]
	require.Eventually(t, func() bool { p, _ := lh.counts(); return p == 1 }, 5*time.Second, 20*time.Millisecond)
	_, ok, msg = pushOne(tc, leader.n, work, "auto/hook.git", zero, "two")
	require.True(t, ok, msg)
	require.Eventually(t, func() bool { return leader.n.work.count() == 1 }, 5*time.Second, 20*time.Millisecond, "the consumer derives once promoted")

	time.Sleep(300 * time.Millisecond)
	for slot, h := range hooks {
		p, d := h.counts()
		if slot == leader.n.slot {
			assert.Equal(t, 1, p, "the leader is promoted once")
			continue
		}
		assert.Zero(t, p, "%s is never promoted", slot)
		assert.Equal(t, 1, d, "%s is demoted once", slot)
	}
}

// A leader whose writer hook keeps failing gives the leadership to a voter
// whose hook works, and the SQL is derived again (#47).
func TestHuntR1ProveWriterStall(t *testing.T) {
	// node-a founds the cluster and leads it first; only its hook fails.
	hooks := map[string]*fakeWriterHook{"node-a": {failing: true}, "node-b": {}, "node-c": {}}
	writerHooks := map[string]WriterHook{}
	for slot, h := range hooks {
		writerHooks[slot] = h
	}
	tc, b := newAutoClusterWith(t, 3, writerHooks, "node-a", "node-b", "node-c")
	b.waitMembers(3, 3)
	var first, via *autoNode
	for _, an := range b.alive() {
		if an.n.slot == "node-a" {
			first = an
		} else if via == nil {
			via = an
		}
	}
	require.NotNil(t, first)
	work := t.TempDir()
	run(t, work, "init", "-q")
	_, ok, msg := pushOne(tc, via.n, work, "auto/stall.git", strings.Repeat("0", 40), "one")
	require.True(t, ok, msg)
	require.Eventually(t, func() bool {
		l := b.leader()
		return l != nil && l != first && l.n.work.count() > 0
	}, 20*time.Second, 50*time.Millisecond, "the leadership left the leader whose hook fails, and the SQL is derived")
	p, _ := hooks["node-a"].counts()
	assert.Zero(t, p, "node-a never promoted")
}

// statusEngine answers Status alone, for the writer hook's steps.
type statusEngine struct {
	consensus.Engine
	st consensus.Status
}

func (e *statusEngine) Status() consensus.Status { return e.st }

// A leadership lost and won back between two passes of the autopilot is
// another term: the node refuses writes and is promoted again, since
// another node may have been promoted in between.
func TestWriterHookPromotesAgainInANewTerm(t *testing.T) {
	h := &fakeWriterHook{}
	r := &Replicator{writerHook: h}
	eng := &statusEngine{st: consensus.Status{Slot: "node-a", Role: consensus.RoleLeader, Term: 1}}
	require.ErrorIs(t, r.writerGate(eng), errBecomingWriter)
	r.stepWriter(context.Background(), eng)
	require.NoError(t, r.writerGate(eng))
	r.stepWriter(context.Background(), eng)
	p, _ := h.counts()
	assert.Equal(t, 1, p, "the same term is promoted once")

	eng.st.Term = 3
	require.ErrorIs(t, r.writerGate(eng), errBecomingWriter, "a promotion of term 1 does not cover term 3")
	r.stepWriter(context.Background(), eng)
	require.NoError(t, r.writerGate(eng))
	p, _ = h.counts()
	assert.Equal(t, 2, p)
}
