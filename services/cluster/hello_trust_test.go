// Copyright 2026 The Forgejo Authors. All rights reserved.
// SPDX-License-Identifier: GPL-3.0-or-later

package cluster

import (
	"context"
	"encoding/hex"
	"math"
	"net"
	"net/http"
	"strconv"
	"testing"
	"time"

	"forgejo.org/services/cluster/consensus"
	"forgejo.org/services/cluster/reftable"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// threeVoters is node 0xb's own view: it and two other voters, each with
// its slot and key.
func threeVoters(self consensus.NodeID) consensus.Membership {
	return consensus.Membership{Members: []consensus.Member{
		{ID: 0xa, Slot: "node-a", SPKI: [32]byte{0xa}, Voter: true},
		{ID: self, Slot: "node-b", SPKI: [32]byte{0xb}, Voter: true},
		{ID: 0xc, Slot: "node-c", SPKI: [32]byte{0xc}, Voter: true},
	}}
}

// trustByKey is trustedAnswer without a table: slot and key, and the keys
// in revoked refused.
func trustByKey(revoked ...[32]byte) func(consensus.Member, hello) bool {
	return func(m consensus.Member, h hello) bool {
		for _, k := range revoked {
			if k == h.SPKI || k == m.SPKI {
				return false
			}
		}
		return m.SPKI != [32]byte{} && h.Slot == m.Slot && h.SPKI == m.SPKI
	}
}

// One answer whose node id, index and voters nothing authenticates never
// confirms that a healthy member is retired, and never outweighs a voter
// of this node's own membership (#49).
func TestHuntR1ProveForgedHelloUnit(t *testing.T) {
	self := consensus.NodeID(0xb)
	own := threeVoters(self)
	forged := hello{
		Slot: "node-c", State: helloMember, Node: "00000000000000cc",
		Index: math.MaxUint64, Voters: []string{"00000000000000cc"}, Retired: []string{self.String()},
	}
	assert.False(t, retiredConfirmed(self, own, []hello{forged}, trustByKey()), "an answer under no member's key")
	forged.SPKI = [32]byte{0xc}
	assert.False(t, retiredConfirmed(self, own, []hello{forged}, trustByKey()), "one voter of three is no majority")
	honest := hello{Slot: "node-a", State: helloMember, Node: consensus.NodeID(0xa).String(), Index: 7, SPKI: [32]byte{0xa}}
	assert.False(t, retiredConfirmed(self, own, []hello{forged, honest}, trustByKey()), "an honest voter lists it as a member")
	assert.False(t, retiredConfirmed(self, consensus.Membership{}, []hello{forged}, trustByKey()), "no membership of its own, nothing to count against")

	honest.Retired = []string{self.String()}
	assert.True(t, retiredConfirmed(self, own, []hello{forged, honest}, trustByKey()), "two voters of three, each under its key")
	assert.False(t, retiredConfirmed(self, own, []hello{forged, honest}, trustByKey([32]byte{0xc})), "a revoked key does not count")

	// The direct answer to a node that named itself counts as the list.
	asked := hello{Slot: "node-a", State: helloMember, SPKI: [32]byte{0xa}}
	asked.answerFor(self.String())
	assert.False(t, asked.retires(self.String()))
	retired := hello{Slot: "node-a", State: helloMember, SPKI: [32]byte{0xa}, Retired: []string{self.String()}}
	retired.answerFor(self.String())
	assert.Nil(t, retired.Retired)
	assert.True(t, retired.retires(self.String()))
	assert.True(t, retiredConfirmed(self, own, []hello{forged, retired}, trustByKey()))
}

// fakeRetireEngine is a follower whose leader does not answer.
type fakeRetireEngine struct {
	consensus.Engine
	id consensus.NodeID
	m  consensus.Membership
}

func (e *fakeRetireEngine) Status() consensus.Status         { return consensus.Status{ID: e.id} }
func (e *fakeRetireEngine) Membership() consensus.Membership { return e.m }
func (e *fakeRetireEngine) Health() consensus.Health         { return consensus.Health{} }
func (e *fakeRetireEngine) Barrier(context.Context) (consensus.Token, error) {
	return consensus.Token{}, consensus.ErrNotLeader
}

// A node whose leader is gone hears one forged answer, from the slot of a
// member removed and revoked, whose key still has its certificate: it keeps
// its id. The same answers from its voters retire it (#49).
func TestHuntR1ProveForgedHello(t *testing.T) {
	self := consensus.NodeID(0xb)
	own := threeVoters(self)
	own.Members = append(own.Members, consensus.Member{ID: 0xd, Slot: "node-d", SPKI: [32]byte{0xd}, Voter: true})
	for _, tc := range []struct {
		name    string
		answers map[string]hello
		revoke  [][32]byte
		rejoins int
	}{
		{name: "forged by a revoked key", revoke: [][32]byte{{0xe}}, answers: map[string]hello{
			"node-e": {Slot: "node-e", State: helloMember, Node: "00000000000000ee", Index: math.MaxUint64, Voters: []string{"00000000000000ee"}, Retired: []string{self.String()}, SPKI: [32]byte{0xe}},
		}},
		{name: "forged as a voter's slot under another key", answers: map[string]hello{
			"node-c": {Slot: "node-c", State: helloMember, Node: consensus.NodeID(0xc).String(), Index: math.MaxUint64, Retired: []string{self.String()}, SPKI: [32]byte{0xe}},
			"node-d": {Slot: "node-d", State: helloMember, Node: consensus.NodeID(0xd).String(), Index: math.MaxUint64, Retired: []string{self.String()}, SPKI: [32]byte{0xe}},
		}},
		{name: "a majority of its voters, one revoked since", revoke: [][32]byte{{0xc}}, answers: map[string]hello{
			"node-a": {Slot: "node-a", State: helloMember, Retired: []string{self.String()}, SPKI: [32]byte{0xa}},
			"node-c": {Slot: "node-c", State: helloMember, Retired: []string{self.String()}, SPKI: [32]byte{0xc}},
			"node-d": {Slot: "node-d", State: helloMember, Retired: []string{self.String()}, SPKI: [32]byte{0xd}},
		}},
		{name: "a majority of its voters", rejoins: 1, answers: map[string]hello{
			"node-a": {Slot: "node-a", State: helloMember, Retired: []string{self.String()}, SPKI: [32]byte{0xa}},
			"node-c": {Slot: "node-c", State: helloMember, Retired: []string{self.String()}, SPKI: [32]byte{0xc}},
			"node-d": {Slot: "node-d", State: helloMember, Retired: []string{self.String()}, SPKI: [32]byte{0xd}},
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r := newReplicator(t.TempDir(), newRegistry(time.Second, 8, 64))
			for _, k := range tc.revoke {
				revokeKey(t, r.table, k)
			}
			rejoins := 0
			r.auto = &autoMode{
				self: "node-b", tick: time.Second,
				peers: map[string]string{"node-a": "a:1", "node-b": "b:1", "node-c": "c:1", "node-d": "d:1", "node-e": "e:1"},
				probe: func(_ context.Context, _, slot string) (hello, error) {
					if h, ok := tc.answers[slot]; ok {
						return h, nil
					}
					return hello{}, consensus.ErrNotLeader
				},
				rejoin: func(context.Context) error { rejoins++; return nil },
			}
			r.checkRetired(t.Context(), &fakeRetireEngine{id: self, m: own})
			assert.Equal(t, tc.rejoins, rejoins)
		})
	}
}

// revokeKey applies a revocation of spki to a table.
func revokeKey(t *testing.T, table *reftable.Table, spki [32]byte) {
	t.Helper()
	for v := reftable.BaseVersion + 1; v <= reftable.RevokeVersion; v++ {
		data, err := reftable.Encode(reftable.KindAudit, &reftable.Audit{Operator: "op", Action: reftable.AuditPayloadVersion, Target: strconv.Itoa(v), Outcome: reftable.AuditRaised})
		require.NoError(t, err)
		require.True(t, table.Apply(consensus.Entry{Term: 1, Index: uint64(v), Data: data}).(reftable.Result).OK)
	}
	data, err := reftable.EncodeVersion(reftable.RevokeVersion, reftable.KindRevokeKey, &reftable.RevokeKey{SPKI: hex.EncodeToString(spki[:]), Slot: "x", Operator: "op", Time: 1})
	require.NoError(t, err)
	res := table.Apply(consensus.Entry{Term: 1, Index: 100 + uint64(spki[0]), Data: data}).(reftable.Result)
	require.True(t, res.OK, res.Reason)
	require.True(t, table.KeyRevoked(hex.EncodeToString(spki[:])))
}

// The payload raise counts a member only for the answer its own slot and
// key gave with its node id: one holder of a certificate of the CA cannot
// claim to be the member still on an older binary (#49).
func TestForgedHelloDoesNotRaiseThePayload(t *testing.T) {
	m := threeVoters(0xb)
	v := reftable.PayloadVersion
	honestA := hello{Slot: "node-a", Node: consensus.NodeID(0xa).String(), Payload: v, SPKI: [32]byte{0xa}}
	forgedC := hello{Slot: "node-x", Node: consensus.NodeID(0xc).String(), Payload: v, SPKI: [32]byte{0xe}}
	assert.False(t, everyMemberApplies(m, 0xb, []hello{honestA, forgedC}, trustByKey()))
	honestC := hello{Slot: "node-c", Node: consensus.NodeID(0xc).String(), Payload: v, SPKI: [32]byte{0xc}}
	assert.True(t, everyMemberApplies(m, 0xb, []hello{honestA, honestC}, trustByKey()))
	assert.False(t, everyMemberApplies(m, 0xb, []hello{honestA, honestC}, trustByKey([32]byte{0xc})), "a revoked key")
}

// The hello of a cluster that retired ten thousand ids still decodes: a
// node naming itself gets whether it is retired, and a node that does not
// (an older binary) the list, under the raised read limit (#93).
func TestHuntR6ProveHelloReadLimit(t *testing.T) {
	pki := newPKI(t)
	cid := consensus.ClusterID{0x0f, 0x07, 0x9e, 0x93}
	fileA, spkiA := pki.issue(t, cid.String(), "node-a")
	fileB, _ := pki.issue(t, cid.String(), "node-b")
	idA, err := newIdentity(fileA, fileA, pki.caFile)
	require.NoError(t, err)

	retired := make([]string, 10000)
	for i := range retired {
		retired[i] = consensus.NodeID(uint64(i)*0x9e3779b97f4a7c15 + 1).String()
	}
	self := consensus.NodeID(0x93)
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	_, port, _ := net.SplitHostPort(ln.Addr().String())
	oc := &objectChannel{cluster: cid.String(), id: idA, hello: func() hello {
		return hello{Slot: "node-a", State: helloMember, Node: "000000000000000a", Index: 9, Retired: retired}
	}}
	srv := &http.Server{Handler: oc.handler(), TLSConfig: idA.serverConfig(), ReadHeaderTimeout: 5 * time.Second}
	go func() { _ = srv.ServeTLS(ln, "", "") }()
	defer srv.Close()

	idB, err := newIdentity(fileB, fileB, pki.caFile)
	require.NoError(t, err)
	asking := &objectChannel{cluster: cid.String(), id: idB, port: port, self: func() consensus.NodeID { return self }}
	h, err := asking.probeHello(t.Context(), "127.0.0.1:7480", "node-a")
	require.NoError(t, err)
	assert.Empty(t, h.Retired, "a node naming itself does not get the list")
	assert.False(t, h.retires(self.String()))
	assert.Equal(t, spkiA, h.SPKI)

	retired[len(retired)-1] = self.String()
	h, err = asking.probeHello(t.Context(), "127.0.0.1:7480", "node-a")
	require.NoError(t, err)
	assert.True(t, h.retires(self.String()), "the last id of ten thousand")

	older := &objectChannel{cluster: cid.String(), id: idB, port: port}
	h, err = older.probeHello(t.Context(), "127.0.0.1:7480", "node-a")
	require.NoError(t, err, "the whole list, from a node that does not name itself")
	assert.Len(t, h.Retired, 10000)
	assert.True(t, h.retires(self.String()))
}
