// Copyright 2026 The Forgejo Authors. All rights reserved.
// SPDX-License-Identifier: GPL-3.0-or-later

package cluster

import (
	"context"
	"crypto/sha256"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"testing"
	"time"

	"forgejo.org/services/cluster/consensus"
	"forgejo.org/services/cluster/nomadgate"
	"forgejo.org/services/cluster/reftable"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ---- the founding rule ----

func helloFrom(slot, state string, expected int) hello {
	return hello{Slot: slot, State: state, Expected: expected, Address: slot + ".test:7480"}
}

func TestFoundingRule(t *testing.T) {
	abc := []string{"node-a", "node-b", "node-c"}
	j := func(slot string) hello { return helloFrom(slot, helloJoining, 3) }

	// From PEERS: the lowest slot founds once every expected node answers.
	what, _ := decide("node-a", 3, abc, nil)
	assert.Equal(t, formWait, what, "alone: one of three")
	what, _ = decide("node-a", 3, abc, []hello{j("node-b")})
	assert.Equal(t, formWait, what, "two of three: the silent one may hold the log")
	what, _ = decide("node-a", 3, abc, []hello{j("node-b"), j("node-c")})
	assert.Equal(t, formFound, what, "three of three")
	what, _ = decide("node-b", 3, abc, nil)
	assert.Equal(t, formJoin, what, "not the lowest: join, whoever answers")

	// A member anywhere: join, even the lowest slot (a wiped founder).
	what, _ = decide("node-a", 3, abc, []hello{j("node-c"), helloFrom("node-b", helloMember, 3)})
	assert.Equal(t, formJoin, what)

	// Disagreements found nothing.
	what, _ = decide("node-a", 3, abc, []hello{helloFrom("node-b", helloJoining, 5)})
	assert.Equal(t, formWait, what, "another size")
	what, _ = decide("node-a", 3, abc, []hello{j("node-b"), j("node-d")})
	assert.Equal(t, formWait, what, "more slots than expected")

	// A node that has not read its data directory may hold a member's log:
	// nothing is founded while one answers so.
	what, _ = decide("node-a", 3, abc, []hello{j("node-b"), helloFrom("node-c", helloStarting, 3)})
	assert.Equal(t, formWait, what)
}

// A wiped founder that hears a joining node and not the last member, the
// one node still holding the log, waits: founding would start a second
// cluster under the same id, which that log can never rejoin.
func TestAWipedFounderWaitsForTheSilentMember(t *testing.T) {
	abc := []string{"node-a", "node-b", "node-c"}
	what, why := decide("node-a", 3, abc, []hello{helloFrom("node-c", helloJoining, 3)})
	assert.Equal(t, formWait, what, why)
	abcde := []string{"node-a", "node-b", "node-c", "node-d", "node-e"}
	what, why = decide("node-a", 5, abcde, []hello{helloFrom("node-d", helloJoining, 5), helloFrom("node-e", helloJoining, 5)})
	assert.Equal(t, formWait, what, why)
}

// Whatever subset of the others a node hears, only the lowest slot ever
// founds: two clusters are never founded from one PEERS list.
func TestOnlyTheLowestSlotEverFounds(t *testing.T) {
	for n := 3; n <= 5; n++ {
		var slots []string
		for i := range n {
			slots = append(slots, fmt.Sprintf("node-%c", 'a'+i))
		}
		for self := range slots {
			others := slices.Delete(slices.Clone(slots), self, self+1)
			for mask := 0; mask < 1<<len(others); mask++ {
				var seen []hello
				for i, s := range others {
					if mask&(1<<i) != 0 {
						seen = append(seen, helloFrom(s, helloJoining, n))
					}
				}
				what, why := decide(slots[self], n, slots, seen)
				if self != 0 {
					assert.NotEqual(t, formFound, what, "N=%d %s %v: %s", n, slots[self], seen, why)
				}
			}
		}
	}
}

// ---- discovery ----

func TestDiscoveryTargets(t *testing.T) {
	a := &autoMode{self: "node-a", peers: map[string]string{"node-a": "192.0.2.1:7480", "node-c": "192.0.2.3:7480", "node-b": "192.0.2.2:7480"}}
	assert.Equal(t, []target{{addr: "192.0.2.2:7480", slot: "node-b"}, {addr: "192.0.2.3:7480", slot: "node-c"}}, a.targets(),
		"the other slots of PEERS, in slot order")
}

// The hello is served to any node of the cluster over mutual TLS, member or
// not, and nothing else is: the other paths still need a member.
func TestHelloOverMutualTLS(t *testing.T) {
	pki := newPKI(t)
	cid := consensus.ClusterID{0x0f, 0x07, 0x9e, 0x20}
	fileA, spkiA := pki.issue(t, cid.String(), "node-a")
	fileB, _ := pki.issue(t, cid.String(), "node-b")
	fileX, _ := pki.issue(t, consensus.ClusterID{0xee}.String(), "node-x")
	idA, err := newIdentity(fileA, fileA, pki.caFile)
	require.NoError(t, err)

	r := newReplicator(t.TempDir(), newRegistry(time.Second, 8, 64))
	r.auto = &autoMode{expected: 3, self: "node-a", listen: "127.0.0.1:7480"}
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	_, port, _ := net.SplitHostPort(ln.Addr().String())
	oc := &objectChannel{root: r.root, cluster: cid.String(), id: idA, engine: r.Engine, self: r.selfID, hello: r.helloOf}
	srv := &http.Server{Handler: oc.handler(), TLSConfig: idA.serverConfig(), ReadHeaderTimeout: 5 * time.Second}
	go func() { _ = srv.ServeTLS(ln, "", "") }()
	defer srv.Close()

	client := func(file string) *objectChannel {
		id, err := newIdentity(file, file, pki.caFile)
		require.NoError(t, err)
		return &objectChannel{cluster: cid.String(), id: id, port: port}
	}
	ctx := context.Background()
	h, err := client(fileB).probeHello(ctx, "127.0.0.1:7480", "node-a")
	require.NoError(t, err)
	assert.Equal(t, "node-a", h.Slot)
	assert.Equal(t, helloStarting, h.State, "its data directory not read yet")
	r.auto.noteDisk(false)
	h, err = client(fileB).probeHello(ctx, "127.0.0.1:7480", "node-a")
	require.NoError(t, err)
	assert.Equal(t, helloJoining, h.State)
	r.auto.noteDisk(true)
	h, err = client(fileB).probeHello(ctx, "127.0.0.1:7480", "node-a")
	require.NoError(t, err)
	assert.Equal(t, helloMember, h.State, "a member's log on disk, before its engine opens")
	assert.Equal(t, 3, h.Expected)
	assert.Equal(t, spkiA, h.SPKI, "the key is the handshake's")

	// Another cluster's certificate: refused by the server, and its
	// answer would not count anyway.
	_, err = client(fileX).probeHello(ctx, "127.0.0.1:7480", "node-a")
	require.Error(t, err)

	// A path other than the hello still needs a member.
	bcl := client(fileB)
	c := &http.Client{Transport: &http.Transport{TLSClientConfig: bcl.id.clientConfig(func(peer) error { return nil })}}
	resp, err := c.Post("https://127.0.0.1:"+port+"/v1/fetch?repo=a/b.git", "application/octet-stream", nil)
	require.NoError(t, err)
	resp.Body.Close()
	assert.Equal(t, http.StatusForbidden, resp.StatusCode)
}

// ---- admission without a grant ----

func TestAutonomousAdmission(t *testing.T) {
	mem, rs := NewForTesting(t.TempDir(), "node-a")
	r := rs[0]
	r.auto = &autoMode{expected: 3, self: "node-a", peers: map[string]string{"node-a": "a:7480", "node-b": "b:7480", "node-c": "c:7480"}}
	ctx := context.Background()
	key := sha256.Sum256([]byte("node-b"))

	op, err := r.authorizeJoin(ctx, consensus.JoinRequest{Slot: "node-b", SPKI: key})
	require.NoError(t, err, "a CA-signed slot PEERS names")
	assert.Equal(t, autopilotOperator, op)

	_, err = r.authorizeJoin(ctx, consensus.JoinRequest{Slot: "node-x", SPKI: key})
	require.ErrorContains(t, err, "not in PEERS")

	// A grant bound to another key still refuses.
	require.NoError(t, r.Operate(ctx, OperatorFor(1), Operation{Action: ActionAuthorizeJoin, Target: "node-b:" + strings.Repeat("ab", 32)}))
	_, err = r.authorizeJoin(ctx, consensus.JoinRequest{Slot: "node-b", SPKI: key})
	require.ErrorContains(t, err, "another key")

	// The Nomad check applies in the autonomous mode too.
	nomad := &fakeNomadNodes{slot: "node-c", drain: true}
	nsrv := httptest.NewServer(nomad)
	defer nsrv.Close()
	r.gate = &nomadgate.Gate{Addr: nsrv.URL, SlotMeta: "forgejo_slot"}
	_, err = r.authorizeJoin(ctx, consensus.JoinRequest{Slot: "node-c", SPKI: key})
	require.ErrorContains(t, err, "draining")
	r.gate = nil

	// Full: one spare above the size, no more.
	r.auto.peers["node-d"] = "d:7480"
	mem.Add("node-b", key, reftable.New())
	mem.Add("node-c", key, reftable.New())
	_, err = r.authorizeJoin(ctx, consensus.JoinRequest{Slot: "node-d", SPKI: key})
	require.NoError(t, err, "a replacement may stand by")
	mem.Add("node-d", key, reftable.New())
	_, err = r.authorizeJoin(ctx, consensus.JoinRequest{Slot: "node-d", SPKI: key})
	require.ErrorContains(t, err, "remove one first")

	// The manual mode keeps requiring a grant.
	r.auto = nil
	_, err = r.authorizeJoin(ctx, consensus.JoinRequest{Slot: "node-c", SPKI: key})
	require.ErrorContains(t, err, "no operator granted")
}

// A member an operator removes has its key revoked: under a new id, with
// the same CA-signed certificate, it is refused at the JOIN and on the
// object channel, while the autopilot's removal of a dead member leaves
// the key alone so the member returns.
func TestRemovedKeyIsRevoked(t *testing.T) {
	mem, rs := NewForTesting(t.TempDir(), "node-a")
	r := rs[0]
	r.auto = &autoMode{expected: 3, self: "node-a", peers: map[string]string{"node-a": "a:7480", "node-b": "b:7480", "node-c": "c:7480"}}
	ctx := context.Background()
	keyB := sha256.Sum256([]byte("node-b"))
	keyC := sha256.Sum256([]byte("node-c"))
	mem.Add("node-b", keyB, reftable.New())
	mem.Add("node-c", keyC, reftable.New())
	// Five members, so two removals keep the floor of three voters.
	mem.Add("node-d", sha256.Sum256([]byte("node-d")), reftable.New())
	mem.Add("node-e", sha256.Sum256([]byte("node-e")), reftable.New())
	idOf := func(slot string) consensus.NodeID {
		for _, m := range r.Engine().Membership().Members {
			if m.Slot == slot {
				return m.ID
			}
		}
		t.Fatalf("no member at %s", slot)
		return 0
	}
	idB, idC := idOf("node-b"), idOf("node-c")

	require.NoError(t, r.Operate(ctx, OperatorFor(1), Operation{Action: ActionRemove, Target: idB.String()}))
	_, err := r.authorizeJoin(ctx, consensus.JoinRequest{Slot: "node-b", SPKI: keyB})
	require.ErrorContains(t, err, "revoked")
	err = r.Operate(ctx, OperatorFor(1), Operation{Action: ActionAuthorizeJoin, Target: "node-b"})
	require.NoError(t, err)
	_, err = r.authorizeJoin(ctx, consensus.JoinRequest{Slot: "node-b", SPKI: keyB})
	require.ErrorContains(t, err, "revoked", "a grant does not lift a revocation")
	_, err = r.authorizeJoin(ctx, consensus.JoinRequest{Slot: "node-b", SPKI: sha256.Sum256([]byte("node-b, new key"))})
	require.NoError(t, err, "a new key for the slot is admitted")

	// The autopilot's removal revokes nothing.
	r.autoChange(ctx, ActionRemove, idC, "out of reach", r.Engine().Remove)
	_, err = r.authorizeJoin(ctx, consensus.JoinRequest{Slot: "node-c", SPKI: keyC})
	require.NoError(t, err)
}

// A grant that admits a node is bound to its key: another key does not
// join under it, the same key may retry.
func TestAGrantIsBoundToTheKeyItAdmits(t *testing.T) {
	_, rs := NewForTesting(t.TempDir(), "node-a")
	r := rs[0]
	ctx := context.Background()
	require.NoError(t, r.Operate(ctx, OperatorFor(1), Operation{Action: ActionAuthorizeJoin, Target: "node-d"}))
	first := sha256.Sum256([]byte("node-d"))
	_, err := r.authorizeJoin(ctx, consensus.JoinRequest{Slot: "node-d", SPKI: first})
	require.NoError(t, err)
	_, err = r.authorizeJoin(ctx, consensus.JoinRequest{Slot: "node-d", SPKI: first})
	require.NoError(t, err, "the same key retries")
	_, err = r.authorizeJoin(ctx, consensus.JoinRequest{Slot: "node-d", SPKI: sha256.Sum256([]byte("another"))})
	require.ErrorContains(t, err, "another key")
}
