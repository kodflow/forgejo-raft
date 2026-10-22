// Copyright 2026 The Forgejo Authors. All rights reserved.
// SPDX-License-Identifier: GPL-3.0-or-later

package kitsunium

import (
	"context"
	"fmt"
	"io"
	"sync/atomic"
	"testing"
	"time"

	"forgejo.org/services/cluster/consensus"
	"forgejo.org/services/cluster/reftable"

	sdk "github.com/kitsunium/sdk/pkg/v1/app/consensus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// formatRefusingMachine refuses every snapshot once armed, as the ref table
// refuses one of a format it does not read (reftable.Table.Restore).
type formatRefusingMachine struct {
	nopMachine
	armed    func() bool
	restores *atomic.Int64
}

func (m formatRefusingMachine) Restore(io.Reader, consensus.SnapshotMeta) error {
	if !m.armed() {
		return nil
	}
	m.restores.Add(1)
	return fmt.Errorf("%w: snapshot format %d", reftable.ErrVersion, reftable.SnapshotFormat+1)
}

// The adapter hands the table's VERSION_UNSUPPORTED to the engine under the
// SDK's code, so the error is recognised for what it is.
func TestRestoreCarriesVersionUnsupportedToTheEngine(t *testing.T) {
	var n atomic.Int64
	err := Machine(formatRefusingMachine{armed: func() bool { return true }, restores: &n}).Restore(nil, sdk.SnapshotMetaValue{})
	require.ErrorIs(t, err, sdk.VersionUnsupported)
	require.ErrorIs(t, err, consensus.ErrVersionUnsupported)

	plain := fmt.Errorf("decode ref table snapshot: %w", io.ErrUnexpectedEOF)
	assert.NotErrorIs(t, machine{failingRestore{err: plain}}.Restore(nil, sdk.SnapshotMetaValue{}), sdk.VersionUnsupported)
}

type failingRestore struct {
	nopMachine
	err error
}

func (m failingRestore) Restore(io.Reader, consensus.SnapshotMeta) error { return m.err }

// A follower whose table cannot read the snapshot the leader installs stops,
// VERSION_UNSUPPORTED, after one refused install: before, the engine took it
// for an ordinary failure, the leader streamed the snapshot again at every
// probe and the node stayed out of the quorum while reporting ready
// (kitsunium/sdk#513).
func TestAFollowerThatCannotReadTheSnapshotStops(t *testing.T) {
	network := sdk.NewMemoryNetwork()
	cluster := sdk.ClusterID{0x0f, 0x05, 0x13}
	var members []sdk.MemberValue
	var logs []*sdk.MemoryLog
	var ids []sdk.NodeID
	for _, slot := range []string{"a", "b", "c"} {
		l := sdk.NewMemoryLog()
		id, inc, err := sdk.InitLog(l, cluster, slot)
		require.NoError(t, err)
		logs, ids = append(logs, l), append(ids, id)
		members = append(members, sdk.MemberValue{ID: id, Incarnation: inc, Slot: slot, SPKI: sdk.MemorySPKI(id), Voter: true})
	}
	var restores atomic.Int64
	refusing := atomic.Int64{}
	refusing.Store(-1)
	nodes := make([]*sdk.Node, len(logs))
	for i := range logs {
		m := formatRefusingMachine{armed: func() bool { return refusing.Load() == int64(i) }, restores: &restores}
		cfg := sdk.Config{
			Cluster: cluster, Machine: Machine(m), Durability: sdk.DurabilityVolatile,
			Log: logs[i], Snapshots: sdk.NewMemorySnapshots(), Transport: sdk.NewMemoryTransport(network, ids[i]),
			InitialTiming:    sdk.TimingContractSpec{HeartbeatInterval: 10 * time.Millisecond, ElectionTimeoutMin: 100 * time.Millisecond, ElectionTimeoutMax: 200 * time.Millisecond},
			MaxSnapshotBytes: 1 << 30, MaxLogBytes: 1 << 30, MinVoters: 3,
			SnapshotEvery: 8, SnapshotTrailing: 1,
		}
		require.NoError(t, sdk.Bootstrap(context.Background(), &cfg, members))
		n, err := sdk.NewNode(cfg)
		require.NoError(t, err)
		t.Cleanup(func() { _ = n.HardClose() })
		nodes[i] = n
	}
	leader := -1
	require.Eventually(t, func() bool {
		for i, n := range nodes {
			if n.Status().Role == sdk.RoleLeader {
				leader = i
				return true
			}
		}
		return false
	}, 10*time.Second, 10*time.Millisecond)
	f := (leader + 1) % len(nodes)
	follower := nodes[f]
	refusing.Store(int64(f))

	// Cut the follower off while the leader writes past two snapshots and
	// compacts the entries it lacks: only an install can catch it up.
	network.Isolate(ids[f], true)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	for range 64 {
		_, _, err := nodes[leader].Propose(ctx, []byte("x"))
		require.NoError(t, err)
	}
	network.Isolate(ids[f], false)

	require.Eventually(t, func() bool {
		_, stopped := follower.StopReason()
		return stopped
	}, 15*time.Second, 10*time.Millisecond, "the follower kept running after refusing an install")
	r, _ := follower.StopReason()
	require.ErrorIs(t, r.Err, sdk.VersionUnsupported)
	assert.Equal(t, int64(1), restores.Load(), "installs the follower refused before it stopped")
}
