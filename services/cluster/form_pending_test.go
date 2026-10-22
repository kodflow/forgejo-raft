// Copyright 2026 The Forgejo Authors. All rights reserved.
// SPDX-License-Identifier: GPL-3.0-or-later

package cluster

import (
	"context"
	"testing"

	"forgejo.org/services/cluster/consensus"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// pendingDriver holds a data directory whose founding was interrupted.
type pendingDriver struct {
	consensus.Driver
	rep       consensus.Report
	bootstrap [][]consensus.Member
}

func (d *pendingDriver) Inspect(string) (consensus.Report, error) { return d.rep, nil }

func (d *pendingDriver) Bootstrap(_ context.Context, _ consensus.Config, ms []consensus.Member) error {
	d.bootstrap = append(d.bootstrap, ms)
	d.rep.BootstrapPending = false
	return nil
}

// A founder whose Bootstrap stopped half way runs it again at its start,
// with the same one-member list, and says "member" only then (#62).
func TestFormResumesAPendingBootstrap(t *testing.T) {
	cluster := consensus.ClusterID{0x62}
	d := &pendingDriver{rep: consensus.Report{Cluster: cluster, Node: consensus.NodeID(7), Incarnation: consensus.Incarnation{3}, Slot: "a", BootstrapPending: true}}
	r := &Replicator{auto: &autoMode{}}
	cfg := consensus.Config{DataDir: t.TempDir(), Slot: "a", ListenAddress: "10.0.0.1:7001"}
	require.NoError(t, r.form(t.Context(), d, cfg, cluster, [32]byte{9}))
	require.Len(t, d.bootstrap, 1, "Bootstrap ran again")
	assert.Equal(t, []consensus.Member{{ID: consensus.NodeID(7), Incarnation: consensus.Incarnation{3}, Slot: "a", SPKI: [32]byte{9}, Address: "10.0.0.1:7001", Voter: true}}, d.bootstrap[0])
	assert.Equal(t, diskMember, r.auto.disk.Load())

	other := &pendingDriver{rep: consensus.Report{Cluster: consensus.ClusterID{0x63}, BootstrapPending: true}}
	require.ErrorIs(t, r.form(t.Context(), other, cfg, cluster, [32]byte{9}), consensus.ErrMisconfigured)
	assert.Empty(t, other.bootstrap)
}
