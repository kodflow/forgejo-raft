// Copyright 2026 The Forgejo Authors. All rights reserved.
// SPDX-License-Identifier: GPL-3.0-or-later

package cluster

import (
	"context"
	"crypto/rand"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"strings"
	"testing"
	"time"

	"forgejo.org/modules/test"
	"forgejo.org/services/cluster/consensus"
	"forgejo.org/services/cluster/consensus/memraft"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// benchWriterCalls names what writes under a node's root, or ships to one,
// by a call it makes: the object channel's handlers, the shipments of a
// push, and the fetches of the projectors and of the consumers. No
// goroutine is named by its own function: the counter the bench's close
// waits on is done a few instructions before such a goroutine exits.
var benchWriterCalls = []string{
	"cluster.(*objectChannel).handlePush(",
	"cluster.(*objectChannel).handleFetch(",
	"cluster.(*objectChannel).handleHave(",
	"cluster.(*objectChannel).peerHolds(",
	"cluster.(*objectChannel).pushFile(",
	"cluster.(*objectChannel).fetchMissing(",
	"cluster.packToFile(",
	"cluster.receivePack(",
}

// benchWriters lists the goroutines of this binary that run one of
// benchWriterCalls, each by its state and that call.
func benchWriters() []string {
	buf := make([]byte, 16<<20)
	buf = buf[:runtime.Stack(buf, true)]
	var out []string
	for g := range strings.SplitSeq(string(buf), "\n\n") {
		lines := strings.Split(g, "\n")
		head, _, _ := strings.Cut(lines[0], " {")
		for i, line := range lines {
			if i == 0 || !slices.ContainsFunc(benchWriterCalls, func(call string) bool { return strings.Contains(line, call) }) {
				continue
			}
			call, _, _ := strings.Cut(line, "(0x")
			at := ""
			if i+1 < len(lines) {
				at = strings.TrimSpace(lines[i+1])
			}
			out = append(out, fmt.Sprintf("%s %s at %s", head, call, at))
			break
		}
	}
	return out
}

// noBenchWriters waits for the writers an earlier test of the binary left,
// so that what runs after the bench's close is this test's own.
func noBenchWriters(t *testing.T) {
	t.Helper()
	require.Eventually(t, func() bool { return len(benchWriters()) == 0 }, time.Minute, 10*time.Millisecond,
		"the object writers of an earlier test end")
}

// receiving reports whether the repository dir receives a pack, from a
// shipment or a fetch: a quarantine of receivePack holds some of it.
func receiving(dir string) bool {
	packs, _ := filepath.Glob(filepath.Join(dir, "objects", "cluster-incoming-*", "pack", "*"))
	for _, p := range packs {
		if fi, err := os.Stat(p); err == nil && fi.Size() > 0 {
			return true
		}
	}
	return false
}

// assertClosed checks what the bench's close promises once it returned:
// nothing of the bench writes under the nodes' roots, or ships to a node,
// any more; and the repository dirs that received keep no quarantine,
// which receivePack removes when it ends.
func assertClosed(t *testing.T, dirs ...string) {
	t.Helper()
	alive := benchWriters()
	assert.Empty(t, alive, "the bench's close returned while these still run:\n%s", strings.Join(alive, "\n"))
	for _, dir := range dirs {
		quarantines, err := filepath.Glob(filepath.Join(dir, "objects", "cluster-incoming-*"))
		require.NoError(t, err)
		assert.Empty(t, quarantines, "the receiving end ended, and removed its quarantine")
	}
}

// stalledPeer is member n's object channel as one of its peers reaches it
// through a link that stalls, on an address of its own: outside the bench,
// whose close neither closes it nor waits for it. It holds nothing, and it
// takes a shipment's whole pack without ever answering it.
func stalledPeer(t *testing.T, n *testNode) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	srv := &http.Server{
		Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.URL.Path != "/v1/push" {
				http.NotFound(w, r)
				return
			}
			_, _ = io.Copy(io.Discard, r.Body)
			<-r.Context().Done()
		}),
		TLSConfig:         n.r.objects.id.serverConfig(),
		ReadHeaderTimeout: 5 * time.Second,
	}
	go func() { _ = srv.ServeTLS(ln, "", "") }()
	t.Cleanup(func() { _ = srv.Close() })
	return ln.Addr().String()
}

// The shipment of a push's objects returns once a majority holds them: the
// shipment to the member left behind, a straggler, runs on detached from
// the request, within stragglerBudget, and nothing of the bench cancels it.
// The bench's close returned while it still ran (#271). node-a reaches
// node-c through a link that stalls, outside the bench: the straggler
// still waits for node-c's answer when the test ends, until its budget.
func TestTheBenchClosesOnceAStragglersShipmentEnded(t *testing.T) {
	noBenchWriters(t)
	// Restored after the bench closed (cleanups run last in, first out).
	t.Cleanup(test.MockVariableValue(&stragglerBudget, 2*time.Second))
	tc := newTestClusterWith(t, memraft.New().ID(), func(tc *testCluster, pins map[string][32]byte) {
		origin, nodeC := tc.nodes[0], tc.nodes[2]
		stalled := stalledPeer(t, nodeC)
		direct := origin.r.objects.resolve
		origin.r.objects.resolve = func(m consensus.Member) string {
			if m.Slot == nodeC.slot {
				return stalled
			}
			return direct(m)
		}
		for _, n := range tc.nodes {
			n.node = tc.mem.Add(n.slot, pins[n.slot], n.r.table)
			n.r.setEngine(n.node)
		}
	}, "node-a", "node-b", "node-c")
	origin := tc.nodes[0]
	work := t.TempDir()
	run(t, work, "init", "-q")
	sha := commit(t, work, "one")
	// The objects are in the repository, as receive-pack leaves them before
	// the hook ships them.
	repo := "o/x.git"
	dir := filepath.Join(origin.r.root, filepath.FromSlash(repo))
	run(t, origin.r.root, "init", "-q", "--bare", dir)
	run(t, work, "push", "-q", dir, sha+":refs/tmp/incoming")
	run(t, dir, "update-ref", "-d", "refs/tmp/incoming")
	require.NoError(t, origin.r.objects.shipToMajority(context.Background(), repo, dir, "sha1", []string{sha}, nil),
		"node-b holds the objects: a majority")

	tc.close()
	assertClosed(t)
}

// bigCommit commits a blob of size random bytes in a new scratch
// repository, and returns the repository and the commit.
func bigCommit(t *testing.T, size int) (string, string) {
	t.Helper()
	work := t.TempDir()
	run(t, work, "init", "-q")
	blob := make([]byte, size)
	_, err := rand.Read(blob)
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(filepath.Join(work, "big.bin"), blob, 0o644))
	return work, commit(t, work, "big")
}

// The straggler's peer is a member of the bench: its handler writes the
// pack under that member's root when the test ends. net/http's Close
// waits for no handler, and the bench's close returned under it:
// t.TempDir's RemoveAll then failed, "directory not empty" (#271,
// TestADeposedCoordinatorLeavesTheEndedLineageToTheNewLeader in CI).
// node-a reaches node-c through a slow link, so that node-c's handler
// receives the straggler's pack when the test ends.
func TestTheBenchClosesOnceANodesHandlerEnded(t *testing.T) {
	noBenchWriters(t)
	tc := newTestClusterWith(t, memraft.New().ID(), func(tc *testCluster, pins map[string][32]byte) {
		origin, nodeC := tc.nodes[0], tc.nodes[2]
		slow := slowRelay(t, nodeC.addr, 256<<10)
		direct := origin.r.objects.resolve
		origin.r.objects.resolve = func(m consensus.Member) string {
			if m.Slot == nodeC.slot {
				return slow
			}
			return direct(m)
		}
		for _, n := range tc.nodes {
			n.node = tc.mem.Add(n.slot, pins[n.slot], n.r.table)
			n.r.setEngine(n.node)
		}
	}, "node-a", "node-b", "node-c")
	work, sha := bigCommit(t, 1<<20)
	resp := tc.pushVia(tc.nodes[0], work, "o/x.git", "refs/heads/main", strings.Repeat("0", 40), sha)
	require.True(t, resp.OK, resp.Message)
	dir := filepath.Join(tc.nodes[2].r.root, "o", "x.git")
	require.Eventually(t, func() bool { return receiving(dir) }, 30*time.Second, time.Millisecond,
		"node-c's handler receives the straggler's pack")

	tc.close()
	assertClosed(t, dir)
}
