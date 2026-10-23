// Copyright 2026 The Forgejo Authors. All rights reserved.
// SPDX-License-Identifier: GPL-3.0-or-later

package kitsunium

import (
	"context"
	"io"
	"testing"
	"time"

	"forgejo.org/services/cluster/consensus"

	sdk "github.com/kitsunium/sdk/pkg/v1/app/consensus"
	sdkmetrics "github.com/kitsunium/sdk/pkg/v1/observe/metrics"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type nopMachine struct{}

func (nopMachine) Apply(consensus.Entry) any { return nil }
func (nopMachine) Snapshot() (consensus.Snapshot, error) {
	return nopSnap{}, nil
}
func (nopMachine) Restore(io.Reader, consensus.SnapshotMeta) error { return nil }

type nopSnap struct{}

func (nopSnap) WriteTo(io.Writer) (int64, error) { return 0, nil }
func (nopSnap) Release()                         {}

// The engine's consensus_* series reach Forgejo's /metrics.
func TestEngineMetricsReachPrometheus(t *testing.T) {
	network := sdk.NewMemoryNetwork()
	cluster := sdk.ClusterID{0x0f, 0x07, 0x9e, 0x12}
	var members []sdk.MemberValue
	var meters []sdkmetrics.FullMeter
	var nodes []*sdk.Node
	var logs []*sdk.MemoryLog
	var ids []sdk.NodeID
	for _, slot := range []string{"a", "b", "c"} {
		l := sdk.NewMemoryLog()
		id, inc, err := sdk.InitLog(l, cluster, slot)
		require.NoError(t, err)
		logs, ids = append(logs, l), append(ids, id)
		members = append(members, sdk.MemberValue{ID: id, Incarnation: inc, Slot: slot, SPKI: sdk.MemorySPKI(id), Voter: true})
	}
	for i := range logs {
		cfg := sdk.Config{
			Cluster: cluster, Machine: Machine(nopMachine{}), Durability: sdk.DurabilityVolatile,
			Log: logs[i], Snapshots: sdk.NewMemorySnapshots(), Transport: sdk.NewMemoryTransport(network, ids[i]),
			InitialTiming:    sdk.TimingContractSpec{HeartbeatInterval: 10 * time.Millisecond, ElectionTimeoutMin: 100 * time.Millisecond, ElectionTimeoutMax: 200 * time.Millisecond},
			MaxSnapshotBytes: 1 << 30, MaxLogBytes: 1 << 30, MinVoters: 3,
			Meter: newEngineMeter(), Logger: engineLogger(),
		}
		meters = append(meters, cfg.Meter.(sdkmetrics.FullMeter))
		require.NoError(t, sdk.Bootstrap(context.Background(), &cfg, members))
		n, err := sdk.NewNode(cfg)
		require.NoError(t, err)
		t.Cleanup(func() { _ = n.Close(context.Background()) })
		nodes = append(nodes, n)
	}
	// Commit a few entries, so the leader tracks its peers and the latency
	// histograms have samples.
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	leader := -1
	require.Eventually(t, func() bool {
		for i, n := range nodes {
			if n.Status().Role == sdk.RoleLeader {
				leader = i
				return true
			}
		}
		return false
	}, 5*time.Second, 10*time.Millisecond)
	for range 5 {
		_, _, err := nodes[leader].Propose(ctx, []byte("x"))
		require.NoError(t, err)
	}
	// The size a cut is held to reaches Forgejo's health, far from it here.
	h := Wrap(nodes[leader], "a").Health()
	assert.Equal(t, int64(1<<30), h.SnapshotLimit)
	assert.False(t, h.SnapshotNearLimit)
	reg := prometheus.NewRegistry()
	lm := meters[leader]
	require.NoError(t, reg.Register(engineCollector{meter: func() sdkmetrics.FullMeter { return lm }}))
	require.Eventually(t, func() bool {
		mfs, err := reg.Gather()
		if err != nil {
			return false
		}
		for _, mf := range mfs {
			if mf.GetName() == "consensus_role" || mf.GetName() == "consensus_term" {
				return true
			}
		}
		return false
	}, 5*time.Second, 50*time.Millisecond)
	mfs, err := reg.Gather()
	require.NoError(t, err)
	var names []string
	for _, mf := range mfs {
		names = append(names, mf.GetName())
	}
	t.Log(names)
	assert.Contains(t, names, "consensus_role")
	assert.Contains(t, names, "consensus_failure_tolerance")
	var perPeer, histogram bool
	scan := func() {
		mfs, err := reg.Gather()
		require.NoError(t, err)
		for _, mf := range mfs {
			for _, m := range mf.GetMetric() {
				for _, l := range m.GetLabel() {
					if l.GetName() == "peer" {
						perPeer = true
					}
				}
				if m.GetHistogram() != nil && m.GetHistogram().GetSampleCount() > 0 {
					histogram = true
				}
			}
		}
	}
	// The leader publishes its per-peer gauges at most every 100 ms.
	require.Eventually(t, func() bool { scan(); return perPeer && histogram }, 5*time.Second, 50*time.Millisecond)
	assert.True(t, perPeer, "the leader's per-peer series")
	assert.True(t, histogram, "a latency histogram with samples")

	line := []byte("time=x level=INFO msg=role\n")
	n, err := logWriter{}.Write(line)
	require.NoError(t, err)
	assert.Equal(t, len(line), n)
}
