// Copyright 2026 The Forgejo Authors. All rights reserved.
// SPDX-License-Identifier: GPL-3.0-or-later

package cluster

import (
	"context"
	"encoding/hex"
	"net"
	"net/http"
	"path/filepath"
	"testing"
	"time"

	"forgejo.org/modules/setting"
	"forgejo.org/modules/test"
	"forgejo.org/services/cluster/consensus"
	"forgejo.org/services/cluster/witness"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Witnesses that do not answer keep the node down without fencing it: once
// one answers "clear", the boot check passes, with no operator (#68).
func TestTheBootCheckWaitsForTheWitnessesToComeBack(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "witness")
	pub, err := witness.Init(dir)
	require.NoError(t, err)
	st, err := witness.Open(dir)
	require.NoError(t, err)
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	addr := ln.Addr().String()
	require.NoError(t, ln.Close()) // down for now
	defer test.MockVariableValue(&setting.Cluster.Witnesses, []string{"http://" + addr})()
	defer test.MockVariableValue(&setting.Cluster.WitnessKeys, []string{pub})()
	defer test.MockVariableValue(&witnessRetryFirst, 20*time.Millisecond)()
	defer test.MockVariableValue(&witnessRetryMax, 100*time.Millisecond)()

	done := make(chan error, 1)
	go func() { done <- FenceWait(t.Context(), consensus.ClusterID{0x68}) }()
	select {
	case err := <-done:
		t.Fatalf("the check ended while no witness answered: %v", err)
	case <-time.After(500 * time.Millisecond):
	}

	ln, err = net.Listen("tcp", addr)
	require.NoError(t, err)
	srv := &http.Server{Handler: st.Handler(), ReadHeaderTimeout: 5 * time.Second}
	go func() { _ = srv.Serve(ln) }()
	t.Cleanup(func() { _ = srv.Close() })
	select {
	case err := <-done:
		require.NoError(t, err, "clear once the witness is back")
	case <-time.After(20 * time.Second):
		t.Fatal("the check never passed after the witness came back")
	}

	// It still gives up with its context, refused.
	ctx, cancel := context.WithTimeout(t.Context(), 200*time.Millisecond)
	defer cancel()
	defer test.MockVariableValue(&setting.Cluster.Witnesses, []string{"http://127.0.0.1:1"})()
	require.ErrorIs(t, FenceWait(ctx, consensus.ClusterID{0x68}), witness.ErrNoWitness)

	// A configuration error is neither.
	defer test.MockVariableValue(&setting.Cluster.WitnessKeys, []string{hex.EncodeToString([]byte{1})})()
	err = FenceWait(t.Context(), consensus.ClusterID{0x68})
	require.ErrorIs(t, err, ErrWitnessConfig)
	assert.NotErrorIs(t, err, witness.ErrNoWitness)
}
