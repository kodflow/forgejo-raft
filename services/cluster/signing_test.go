// Copyright 2026 The Forgejo Authors. All rights reserved.
// SPDX-License-Identifier: GPL-3.0-or-later

package cluster

import (
	"testing"

	"forgejo.org/modules/json"
	"forgejo.org/services/cluster/consensus"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Behind the load balancer a token one node signs is verified by another:
// the leader compares its members' token-signing keys with its own, names
// the member whose keys differ and which, asks again once per
// signingProbeEvery, and says when they agree again (#285).
func TestTheLeaderNamesAMemberThatSignsWithOtherKeys(t *testing.T) {
	r, e, clock, answers := checkpointBench(t)
	ctx := t.Context()
	mine := localSigning()
	require.NotEmpty(t, mine.JWT)
	other := *mine
	other.JWT = signingDigest("jwt", []byte("the JWT_SECRET node-c drew for itself"))
	set := func(slot string, d *signingDigests) {
		h := answers[slot]
		h.Signing = d
		answers[slot] = h
	}
	set("node-b", mine)
	set("node-c", &other)
	r.maybeCheckSigning(ctx, e)
	assert.Equal(t, "node-c ([oauth2] JWT_SECRET)", r.signingNoted)

	set("node-c", mine)
	r.maybeCheckSigning(ctx, e)
	assert.Equal(t, "node-c ([oauth2] JWT_SECRET)", r.signingNoted, "asked again only after signingProbeEvery")
	clock.advance(signingProbeEvery)
	r.maybeCheckSigning(ctx, e)
	assert.Empty(t, r.signingNoted, "node-c was given the same secret")

	// An older binary says nothing and is not compared, nor a key this
	// node does not say: its LFS server is off.
	set("node-b", nil)
	lfs := *mine
	lfs.LFS = signingDigest("lfs HS256", []byte("node-c's LFS_JWT_SECRET"))
	set("node-c", &lfs)
	clock.advance(signingProbeEvery)
	r.maybeCheckSigning(ctx, e)
	assert.Empty(t, r.signingNoted)
}

// The comparison names every member whose keys differ, the LFS key too, and
// only from an answer the member's own key proves.
func TestSigningDiffersNamesEachKey(t *testing.T) {
	r, _, _, _ := checkpointBench(t)
	mine := &signingDigests{JWT: signingDigest("jwt", []byte("a")), LFS: signingDigest("lfs HS256", []byte("b"))}
	m := consensus.Membership{Members: []consensus.Member{
		{ID: 0xa, Slot: "node-a", SPKI: [32]byte{0xa}},
		{ID: 0xb, Slot: "node-b", SPKI: [32]byte{0xb}},
		{ID: 0xc, Slot: "node-c", SPKI: [32]byte{0xc}},
		{ID: 0xd, Slot: "node-d", SPKI: [32]byte{0xd}},
	}}
	seen := []hello{
		{Slot: "node-b", Node: consensus.NodeID(0xb).String(), SPKI: [32]byte{0xb}, Signing: &signingDigests{JWT: mine.JWT, LFS: signingDigest("lfs HS256", []byte("c"))}},
		{Slot: "node-c", Node: consensus.NodeID(0xc).String(), SPKI: [32]byte{0xc}, Signing: &signingDigests{JWT: signingDigest("jwt", []byte("c")), LFS: signingDigest("lfs EdDSA", []byte("b"))}},
		// Another key than node-d's: not its answer.
		{Slot: "node-d", Node: consensus.NodeID(0xd).String(), SPKI: [32]byte{0xe}, Signing: &signingDigests{JWT: "x"}},
	}
	assert.Equal(t, []string{
		"node-b ([server] LFS_JWT_SECRET)",
		"node-c ([oauth2] JWT_SECRET, [server] LFS_JWT_SECRET)",
	}, signingDiffers(mine, m, 0xa, seen, r.answerOf))
	assert.Empty(t, signingDiffers(mine, m, 0xa, nil, r.answerOf), "nobody answers")
}

// Each digest tells two keys apart, the same key under two labels too, and
// travels in the hello, which an older binary answers without.
func TestTheHelloCarriesTheSigningDigests(t *testing.T) {
	once, again := signingDigest("jwt", []byte("k")), signingDigest("jwt", []byte("k"))
	assert.Equal(t, once, again, "the same key, the same digest on every node")
	assert.NotEqual(t, signingDigest("jwt", []byte("k")), signingDigest("jwt", []byte("l")))
	assert.NotEqual(t, signingDigest("jwt", []byte("k")), signingDigest("lfs HS256", []byte("k")))
	assert.Len(t, signingDigest("jwt", []byte("k")), 32)

	r, _, _ := newLeaderBench(t, 3)
	assert.Equal(t, localSigning(), r.helloOf().Signing)
	r.auto = nil
	h := r.memberHello()
	assert.Equal(t, localSigning(), h.Signing)
	b, err := json.Marshal(h)
	require.NoError(t, err)
	var back hello
	require.NoError(t, json.Unmarshal(b, &back))
	assert.Equal(t, h.Signing, back.Signing)

	var older hello
	require.NoError(t, json.Unmarshal([]byte(`{"slot":"node-b","state":"member"}`), &older))
	assert.Nil(t, older.Signing)
}
