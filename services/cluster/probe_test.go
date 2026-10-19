// Copyright 2026 The Forgejo Authors. All rights reserved.
// SPDX-License-Identifier: GPL-3.0-or-later

package cluster

import (
	"context"
	"crypto/tls"
	"net"
	"testing"

	"forgejo.org/services/cluster/consensus"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// The probe tells an old member (a certificate of the old cluster) from a
// node of another cluster, and a silent address from both; anything it
// cannot read blocks.
func TestProbeOldMembers(t *testing.T) {
	pki := newPKI(t)
	old := consensus.ClusterID{0x0a}
	other := consensus.ClusterID{0x0b}
	serve := func(cluster consensus.ClusterID, slot string) string {
		file, _ := pki.issue(t, cluster.String(), slot)
		id, err := newIdentity(file, file, pki.caFile)
		require.NoError(t, err)
		ln, err := tls.Listen("tcp", "127.0.0.1:0", id.serverConfig())
		require.NoError(t, err)
		t.Cleanup(func() { ln.Close() })
		go func() {
			for {
				c, err := ln.Accept()
				if err != nil {
					return
				}
				go func() { _ = c.(*tls.Conn).Handshake(); c.Close() }()
			}
		}()
		return ln.Addr().String()
	}
	plain, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	t.Cleanup(func() { plain.Close() })
	go func() {
		for {
			c, err := plain.Accept()
			if err != nil {
				return
			}
			c.Close()
		}
	}()
	silent, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	silentAddr := silent.Addr().String()
	silent.Close()

	got := ProbeOldMembers(context.Background(), old.String(), map[string]string{
		"node-b": serve(old, "node-b"),
		"node-c": serve(other, "node-c"),
		"node-d": plain.Addr().String(),
		"node-e": silentAddr,
	}, pki.caFile)
	require.Len(t, got, 3, "the silent address is not listed")
	assert.Equal(t, "node-b", got[0].Slot)
	assert.True(t, got[0].Proven && got[0].Blocks(), got[0].Detail)
	assert.Equal(t, "node-c", got[1].Slot)
	assert.False(t, got[1].Blocks(), "a node of another cluster: %s", got[1].Detail)
	assert.Equal(t, "node-d", got[2].Slot)
	assert.True(t, got[2].Blocks(), "a connection the probe cannot read blocks")
}
