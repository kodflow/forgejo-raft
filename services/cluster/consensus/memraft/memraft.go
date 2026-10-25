// Copyright 2026 The Forgejo Authors. All rights reserved.
// SPDX-License-Identifier: GPL-3.0-or-later

// Package memraft is an in-process stand-in for the consensus engine, for
// tests only. Every node shares one ordered log, so it proves what the
// replicator does with committed entries — the state machine, the
// projector, the consumer, the administration — and nothing about
// consensus itself, which is the SDK's to prove.
package memraft

import (
	"bytes"
	"context"
	"crypto/sha256"
	"fmt"
	"io"
	"slices"
	"sync"

	"forgejo.org/services/cluster/consensus"
)

// Cluster is a group of in-process nodes sharing one log.
type Cluster struct {
	mu       sync.Mutex
	id       consensus.ClusterID
	term     uint64
	log      []consensus.Entry
	nodes    map[consensus.NodeID]*Node
	order    []consensus.NodeID
	leader   consensus.NodeID
	members  consensus.Membership
	alarm    consensus.AlarmID
	watchers []watcher
	// compacted is the last index ReadCommitted no longer returns.
	compacted uint64
	// version and generation are the committed cluster version and its
	// raises; speaks is the protocol a member announces, ProtocolMax unless
	// SetProtocol said otherwise.
	version    uint32
	generation uint64
	speaks     map[consensus.NodeID]uint32
}

// ProtocolMax is the highest cluster version the stand-in speaks: the one
// Forgejo raises to. The SDK speaks 3 since kitsunium/sdk#659, whose
// retired keys the stand-in does not model.
const ProtocolMax uint32 = 2

type watcher struct {
	n  *Node
	ch chan consensus.Status
}

// New returns an empty cluster.
func New() *Cluster {
	return &Cluster{id: consensus.ClusterID{0xf0, 0x6e, 0x10}, term: 1, nodes: map[consensus.NodeID]*Node{}}
}

// Node is one member.
type Node struct {
	c       *Cluster
	id      consensus.NodeID
	slot    string
	sm      consensus.StateMachine
	applied uint64
	down    bool
	results map[uint64]any
	closed  bool
	// stop is closed by Close, so the goroutines of Watch end with it.
	stop chan struct{}
}

// ID returns the cluster's id.
func (c *Cluster) ID() consensus.ClusterID { return c.id }

// Add creates a voting member applying to sm, pinned to spki. The first one
// leads.
func (c *Cluster) Add(slot string, spki [32]byte, sm consensus.StateMachine) *Node {
	c.mu.Lock()
	defer c.mu.Unlock()
	id := consensus.NodeID(len(c.order) + 1)
	n := &Node{c: c, id: id, slot: slot, sm: sm, results: map[uint64]any{}, stop: make(chan struct{})}
	c.nodes[id] = n
	c.order = append(c.order, id)
	if c.leader == 0 {
		c.leader = id
	}
	c.members.Members = append(c.members.Members, consensus.Member{ID: id, Slot: slot, Voter: true, Address: slot + ":7480", SPKI: spki})
	c.catchUp(n)
	return n
}

// SetDown cuts a node off: it neither applies nor answers proposals.
func (c *Cluster) SetDown(n *Node, down bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	n.down = down
	if !down {
		c.catchUp(n)
	}
	if down && c.leader == n.id {
		for _, id := range c.order {
			if !c.nodes[id].down {
				c.leader = id
				c.term++
				break
			}
		}
	}
}

// SetLeader moves leadership.
func (c *Cluster) SetLeader(n *Node) {
	c.mu.Lock()
	c.leader = n.id
	c.term++
	c.mu.Unlock()
}

// SetProtocol makes n announce protocol v, as a member on an older binary
// does: a raise above it is refused, as the engine refuses it.
func (c *Cluster) SetProtocol(n *Node, v uint32) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.speaks == nil {
		c.speaks = map[consensus.NodeID]uint32{}
	}
	c.speaks[n.id] = v
}

// RaiseNoSpace raises the replicated no-space alarm.
func (c *Cluster) RaiseNoSpace() {
	c.mu.Lock()
	c.alarm = consensus.AlarmID(len(c.log) + 1)
	c.mu.Unlock()
}

func (c *Cluster) quorum() bool {
	up := 0
	for _, n := range c.nodes {
		if !n.down {
			up++
		}
	}
	return up*2 > len(c.nodes)
}

func (c *Cluster) catchUp(n *Node) {
	for n.applied < uint64(len(c.log)) {
		e := c.log[n.applied]
		n.results[e.Index] = n.sm.Apply(e)
		n.applied = e.Index
	}
}

func (c *Cluster) append(data []byte) consensus.Entry {
	idx := uint64(len(c.log) + 1)
	e := consensus.Entry{
		Term: c.term, Index: idx, Kind: consensus.EntryCommand, Version: 1,
		Data:  append([]byte(nil), data...),
		Token: consensus.Token{Cluster: c.id, Epoch: 1, Term: c.term, Index: idx},
	}
	c.log = append(c.log, e)
	for _, id := range c.order {
		if n := c.nodes[id]; !n.down && !n.closed {
			c.catchUp(n)
		}
	}
	for _, w := range c.watchers {
		select {
		case w.ch <- c.status(w.n):
		default:
		}
	}
	return e
}

// ---- consensus.Engine ----

func (n *Node) Propose(ctx context.Context, data []byte) (consensus.Token, any, error) {
	c := n.c
	c.mu.Lock()
	defer c.mu.Unlock()
	if n.down || n.closed {
		return consensus.Token{}, nil, consensus.ErrNotLeader
	}
	if !c.quorum() {
		return consensus.Token{}, nil, consensus.ErrNotLeader
	}
	if c.alarm != 0 {
		return consensus.Token{}, nil, consensus.ErrNoSpace
	}
	e := c.append(data)
	return e.Token, n.results[e.Index], nil
}

func (n *Node) Barrier(ctx context.Context) (consensus.Token, error) {
	c := n.c
	c.mu.Lock()
	defer c.mu.Unlock()
	if n.down || !c.quorum() {
		return consensus.Token{}, consensus.ErrLeadershipUnconfirmed
	}
	idx := uint64(len(c.log))
	return consensus.Token{Cluster: c.id, Epoch: 1, Term: c.term, Index: idx}, nil
}

func (n *Node) WaitApplied(ctx context.Context, t consensus.Token) error {
	c := n.c
	c.mu.Lock()
	defer c.mu.Unlock()
	if t.Cluster != c.id {
		return consensus.ErrLineageMismatch
	}
	if n.applied < t.Index {
		return ctx.Err()
	}
	return nil
}

func (c *Cluster) status(n *Node) consensus.Status {
	if n == nil {
		return consensus.Status{}
	}
	role := consensus.RoleFollower
	if c.leader == n.id {
		role = consensus.RoleLeader
	}
	return consensus.Status{
		ID: n.id, Slot: n.slot, Role: role, Term: c.term, Leader: c.leader,
		Commit: uint64(len(c.log)), Applied: n.applied,
		Identity: consensus.LogIdentity{Cluster: c.id, Epoch: 1}, ClusterVersion: c.version, ClusterGeneration: c.generation,
	}
}

func (n *Node) Status() consensus.Status {
	n.c.mu.Lock()
	defer n.c.mu.Unlock()
	return n.c.status(n)
}

func (n *Node) Health() consensus.Health {
	c := n.c
	c.mu.Lock()
	defer c.mu.Unlock()
	st := c.status(n)
	h := consensus.Health{
		Ready: !n.down && c.quorum(), Role: st.Role, LeaderKnown: c.leader != 0, QuorumReachable: c.quorum(),
		Identity: st.Identity, ClusterVersion: c.version,
	}
	up := 0
	for _, id := range c.order {
		p := c.nodes[id]
		if !p.down {
			up++
		}
		if c.leader == n.id && id != n.id {
			h.Peers = append(h.Peers, consensus.PeerHealth{ID: id, Match: p.applied, Lag: uint64(len(c.log)) - p.applied, Unreachable: p.down})
		}
	}
	h.FailureTolerance = up - (len(c.nodes)/2 + 1)
	if c.alarm != 0 {
		h.Alarms = []consensus.Alarm{{ID: c.alarm, Kind: "NO_SPACE", Nodes: []consensus.NodeID{n.id}}}
	}
	return h
}

func (n *Node) Membership() consensus.Membership {
	n.c.mu.Lock()
	defer n.c.mu.Unlock()
	m := n.c.members
	m.Members = append([]consensus.Member(nil), m.Members...)
	return m
}

func (n *Node) member(id consensus.NodeID) int {
	for i, m := range n.c.members.Members {
		if m.ID == id {
			return i
		}
	}
	return -1
}

func (n *Node) AddLearner(ctx context.Context, op consensus.Operator, m consensus.Member) error {
	n.c.mu.Lock()
	defer n.c.mu.Unlock()
	if slices.Contains(n.c.members.Retired, m.ID) {
		return consensus.ErrNodeIDRetired
	}
	if n.member(m.ID) >= 0 {
		return consensus.ErrMembershipInvalid
	}
	m.Voter = false
	n.c.members.Members = append(n.c.members.Members, m)
	return nil
}

func (n *Node) Promote(ctx context.Context, op consensus.Operator, id consensus.NodeID) error {
	n.c.mu.Lock()
	defer n.c.mu.Unlock()
	i := n.member(id)
	if i < 0 {
		return consensus.ErrMembershipInvalid
	}
	n.c.members.Members[i].Voter = true
	return nil
}

func (n *Node) Remove(ctx context.Context, op consensus.Operator, id consensus.NodeID) error {
	n.c.mu.Lock()
	defer n.c.mu.Unlock()
	i := n.member(id)
	if i < 0 || n.c.members.Voters() <= 3 && n.c.members.Members[i].Voter {
		return consensus.ErrMembershipInvalid
	}
	n.c.members.Members = append(n.c.members.Members[:i], n.c.members.Members[i+1:]...)
	n.c.members.Retired = append(n.c.members.Retired, id)
	return nil
}

func (n *Node) UpdateAddress(ctx context.Context, op consensus.Operator, id consensus.NodeID, addr string) error {
	n.c.mu.Lock()
	defer n.c.mu.Unlock()
	i := n.member(id)
	if i < 0 {
		return consensus.ErrMembershipInvalid
	}
	n.c.members.Members[i].Address = addr
	return nil
}

func (n *Node) DenyPin(ctx context.Context, op consensus.Operator, id consensus.NodeID) error {
	n.c.mu.Lock()
	defer n.c.mu.Unlock()
	i := n.member(id)
	if i < 0 {
		return consensus.ErrMembershipInvalid
	}
	n.c.members.Members[i].SPKI = [32]byte{}
	return nil
}

func (n *Node) TransferLeadership(ctx context.Context, op consensus.Operator, to consensus.NodeID) error {
	n.c.mu.Lock()
	defer n.c.mu.Unlock()
	t := n.c.nodes[to]
	if t == nil || t.down {
		return consensus.ErrTransferFailed
	}
	n.c.leader = to
	n.c.term++
	return nil
}

func (n *Node) ClearNoSpace(ctx context.Context, op consensus.Operator, pre consensus.Precondition) error {
	n.c.mu.Lock()
	defer n.c.mu.Unlock()
	if pre.AlarmID != n.c.alarm || n.c.alarm == 0 {
		return consensus.ErrAdminPrecondition
	}
	n.c.alarm = 0
	return nil
}

// RaiseClusterVersion raises the cluster version as the engine does: on a
// node that reaches a quorum, against the current lineage and generation,
// once every member speaks v and answers. The engine holds every member to
// its vote window, whatever it announced (kitsunium/sdk#665): here, a
// member down or closed.
func (n *Node) RaiseClusterVersion(ctx context.Context, op consensus.Operator, pre consensus.Precondition, v uint32) error {
	c := n.c
	c.mu.Lock()
	defer c.mu.Unlock()
	if n.down || n.closed || !c.quorum() {
		return consensus.ErrNotLeader
	}
	if v > ProtocolMax {
		return consensus.ErrVersionUnsupported
	}
	if pre.Identity != (consensus.LogIdentity{Cluster: c.id, Epoch: 1}) || pre.Generation != c.generation || v <= c.version {
		return consensus.ErrAdminPrecondition
	}
	for _, m := range c.members.Members {
		if s, ok := c.speaks[m.ID]; ok && s < v {
			return consensus.ErrVersionUnsupported
		}
		if p := c.nodes[m.ID]; p != nil && (p.down || p.closed) {
			return consensus.ErrVersionUnsupported
		}
	}
	c.version, c.generation = v, c.generation+1
	return nil
}

func (n *Node) SaveSnapshot(ctx context.Context, op consensus.Operator, w io.Writer) (consensus.SnapshotMeta, error) {
	n.c.mu.Lock()
	defer n.c.mu.Unlock()
	s, err := n.sm.Snapshot()
	if err != nil {
		return consensus.SnapshotMeta{}, err
	}
	defer s.Release()
	var buf bytes.Buffer
	if _, err := s.WriteTo(&buf); err != nil {
		return consensus.SnapshotMeta{}, err
	}
	if _, err := w.Write(buf.Bytes()); err != nil {
		return consensus.SnapshotMeta{}, err
	}
	st := n.c.status(n)
	return consensus.SnapshotMeta{
		ID: fmt.Sprintf("mem-%d", st.Applied), Position: consensus.Position{Term: st.Term, Index: st.Applied},
		Identity: st.Identity, Size: int64(buf.Len()), SHA256: sha256.Sum256(buf.Bytes()),
	}, nil
}

// Watch honours the Engine contract: the channel is closed when ctx ends
// or the node closes, whichever comes first.
func (n *Node) Watch(ctx context.Context) <-chan consensus.Status {
	ch := make(chan consensus.Status, 1)
	n.c.mu.Lock()
	if n.closed {
		n.c.mu.Unlock()
		close(ch)
		return ch
	}
	n.c.watchers = append(n.c.watchers, watcher{n: n, ch: ch})
	n.c.mu.Unlock()
	go func() {
		select {
		case <-ctx.Done():
		case <-n.stop:
			return // Close dropped it already
		}
		n.c.mu.Lock()
		n.c.dropWatchers(func(w watcher) bool { return w.ch == ch })
		n.c.mu.Unlock()
	}()
	return ch
}

// dropWatchers removes and closes the watchers match selects, under c.mu:
// a channel leaves the list when it is closed, so it is closed once.
func (c *Cluster) dropWatchers(match func(watcher) bool) {
	keep := c.watchers[:0]
	for _, w := range c.watchers {
		if match(w) {
			close(w.ch)
			continue
		}
		keep = append(keep, w)
	}
	c.watchers = keep
}

func (n *Node) Close(ctx context.Context) error {
	n.c.mu.Lock()
	if !n.closed {
		n.closed = true
		close(n.stop)
	}
	n.c.dropWatchers(func(w watcher) bool { return w.n == n })
	n.c.mu.Unlock()
	return nil
}

// ReadCommitted reads [from, to] of the shared log, as an engine that keeps
// its log would; Compact stands for a log truncated behind a snapshot.
func (n *Node) ReadCommitted(ctx context.Context, from, to uint64, maxBytes int) ([]consensus.Entry, error) {
	c := n.c
	c.mu.Lock()
	defer c.mu.Unlock()
	if n.down || n.closed {
		return nil, consensus.ErrNodeStopped
	}
	if from == 0 || from <= c.compacted {
		return nil, consensus.ErrCompacted
	}
	if last := uint64(len(c.log)); to > last {
		to = last
	}
	var out []consensus.Entry
	size := 0
	for i := from; i <= to; i++ {
		e := c.log[i-1]
		if len(out) > 0 && size+len(e.Data) > maxBytes {
			break
		}
		size += len(e.Data)
		e.Data = append([]byte(nil), e.Data...)
		out = append(out, e)
	}
	return out, nil
}

// Compact drops the entries up to index from what ReadCommitted returns.
func (c *Cluster) Compact(index uint64) {
	c.mu.Lock()
	c.compacted = index
	c.mu.Unlock()
}

// Leader returns the current leader node.
func (c *Cluster) Leader() *Node {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.nodes[c.leader]
}

var (
	_ consensus.Engine          = (*Node)(nil)
	_ consensus.CommittedReader = (*Node)(nil)
)
