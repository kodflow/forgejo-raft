// Copyright 2026 The Forgejo Authors. All rights reserved.
// SPDX-License-Identifier: GPL-3.0-or-later

package cluster

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"sync"
	"testing"

	"forgejo.org/services/cluster/consensus"
	"forgejo.org/services/cluster/reftable"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// followerEngine is a follower whose proposals the leader applies and
// answers before this node applied them: its own table catches up only
// in WaitApplied. fail makes the next proposals fail.
type followerEngine struct {
	consensus.Engine
	leader, local *reftable.Table
	mu            sync.Mutex
	index         uint64
	queue         []consensus.Entry
	fail          error
}

func (e *followerEngine) Propose(_ context.Context, data []byte) (consensus.Token, any, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.fail != nil {
		return consensus.Token{}, nil, e.fail
	}
	e.index++
	tok := consensus.Token{Cluster: consensus.ClusterID{0x47}, Epoch: 1, Term: 1, Index: e.index}
	ent := consensus.Entry{Term: 1, Index: e.index, Kind: consensus.EntryCommand, Data: data, Token: tok}
	e.queue = append(e.queue, ent)
	return tok, e.leader.Apply(ent), nil
}

func (e *followerEngine) WaitApplied(_ context.Context, tok consensus.Token) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	for len(e.queue) > 0 && e.queue[0].Index <= tok.Index {
		e.local.Apply(e.queue[0])
		e.queue = e.queue[1:]
	}
	return nil
}

// Barrier stands for the leader's: everything proposed so far is committed.
func (e *followerEngine) Barrier(context.Context) (consensus.Token, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	return consensus.Token{Cluster: consensus.ClusterID{0x47}, Epoch: 1, Term: 1, Index: e.index}, nil
}

func (e *followerEngine) Membership() consensus.Membership { return consensus.Membership{} }

// A raise proposed from a follower returns once the leader applied it: the
// follower waits for it before it flushes the pending revocations, which
// read its own table. In manual mode nothing else flushes them, and the
// removed key stayed admissible (CodeRabbit on #30). A revocation the
// flush fails to write is reported to the operator.
func TestARaiseFromAFollowerFlushesThePendingRevocations(t *testing.T) {
	ctx := context.Background()
	r := newReplicator(t.TempDir(), newRegistry(0, 1, 1))
	eng := &followerEngine{leader: reftable.New(), local: r.table}
	r.setEngine(eng)
	key := sha256.Sum256([]byte("node-b"))
	spki := hex.EncodeToString(key[:])
	// A Remove recorded its revocation while the cluster wrote version 1.
	require.NoError(t, r.recordPendingRevocation(ctx, reftable.RevokeKey{SPKI: spki, Slot: "node-b", Operator: "op", Time: 1}))
	require.NoError(t, eng.WaitApplied(ctx, consensus.Token{Index: eng.index}))
	require.Len(t, r.table.PendingRevocations(), 1)

	require.NoError(t, r.RaisePayloadVersion(ctx, "op"))
	require.NoError(t, r.flushRevocations(ctx))
	assert.True(t, eng.leader.KeyRevoked(spki), "the revocation is written")

	// The flush's error reaches its caller instead of a log line alone.
	r2 := newReplicator(t.TempDir(), newRegistry(0, 1, 1))
	eng2 := &followerEngine{leader: reftable.New(), local: r2.table}
	r2.setEngine(eng2)
	require.NoError(t, r2.recordPendingRevocation(ctx, reftable.RevokeKey{SPKI: spki, Slot: "node-b", Operator: "op", Time: 1}))
	require.NoError(t, r2.RaisePayloadVersion(ctx, "op"))
	eng2.fail = consensus.ErrNoSpace
	assert.Error(t, r2.flushRevocations(ctx))
}
