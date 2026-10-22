// Copyright 2026 The Forgejo Authors. All rights reserved.
// SPDX-License-Identifier: GPL-3.0-or-later

package cluster

import (
	"crypto/rand"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"forgejo.org/modules/test"
	"forgejo.org/services/cluster/consensus"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// slowRelay forwards TCP connections to target at about rate bytes a
// second each way, and returns its address.
func slowRelay(t *testing.T, target string, rate int) string {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	t.Cleanup(func() { _ = ln.Close() })
	const tick = 50 * time.Millisecond
	chunk := max(rate/int(time.Second/tick), 1)
	pipe := func(dst, src net.Conn) {
		defer dst.Close()
		defer src.Close()
		buf := make([]byte, chunk)
		for {
			n, err := src.Read(buf)
			if n > 0 {
				if _, werr := dst.Write(buf[:n]); werr != nil {
					return
				}
				time.Sleep(tick)
			}
			if err != nil {
				return
			}
		}
	}
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			up, err := net.Dial("tcp", target)
			if err != nil {
				_ = c.Close()
				continue
			}
			go pipe(up, c)
			go pipe(c, up)
		}
	}()
	return ln.Addr().String()
}

// The shipment of a push's objects is bounded by the request's deadline,
// HOOK_TIMEOUT less ten seconds, not by the stragglers' budget: a push
// whose objects take longer than that budget to reach the peers, with time
// left to the request, is committed (#75).
func TestHuntR4ProveShipcut(t *testing.T) {
	// Restored after the cluster stops (cleanups run last in, first out).
	t.Cleanup(test.MockVariableValue(&stragglerBudget, 500*time.Millisecond))
	clusterID := consensus.ClusterID{0x0f, 0x07, 0x9e, 0x13}
	tc := newTestClusterWith(t, clusterID, func(tc *testCluster, pins map[string][32]byte) {
		// The origin reaches its peers through a link of 256 KiB/s.
		origin := tc.nodes[0]
		relays := map[string]string{}
		for _, n := range tc.nodes[1:] {
			relays[n.slot] = slowRelay(t, n.addr, 256<<10)
		}
		direct := origin.r.objects.resolve
		origin.r.objects.resolve = func(m consensus.Member) string {
			if a, ok := relays[m.Slot]; ok {
				return a
			}
			return direct(m)
		}
		for _, n := range tc.nodes {
			n.node = tc.mem.Add(n.slot, pins[n.slot], n.r.table)
			n.r.setEngine(n.node)
		}
	}, "node-a", "node-b", "node-c")

	work := t.TempDir()
	run(t, work, "init", "-q")
	blob := make([]byte, 1<<20)
	_, err := rand.Read(blob)
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(filepath.Join(work, "big.bin"), blob, 0o644))
	c1 := commit(t, work, "big")

	start := time.Now()
	resp := tc.pushViaWithin(30*time.Second, tc.nodes[0], work, "alice/big.git", "refs/heads/main", strings.Repeat("0", 40), c1)
	took := time.Since(start)
	require.True(t, resp.OK, "a push with time left is cut by the stragglers' budget: %s", resp.Message)
	assert.Greater(t, took, stragglerBudget, "the shipment outlasted the stragglers' budget, the test proves something")
	for _, n := range tc.nodes[1:] {
		require.Eventually(t, func() bool {
			missing, err := missingObjects(t.Context(), filepath.Join(n.r.root, "alice", "big.git"), []string{c1})
			return err == nil && len(missing) == 0
		}, 20*time.Second, 50*time.Millisecond, "%s holds the push's objects", n.slot)
	}
}
