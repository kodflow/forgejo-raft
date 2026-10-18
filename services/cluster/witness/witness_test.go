// Copyright 2026 The Forgejo Authors. All rights reserved.
// SPDX-License-Identifier: GPL-3.0-or-later

package witness

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/hex"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"forgejo.org/services/cluster/consensus"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var (
	clusterA = consensus.ClusterID{0xa}
	clusterB = consensus.ClusterID{0xb}
	voter1   = consensus.NodeID(0x1111)
	voter2   = consensus.NodeID(0x2222)
)

// a witness served over HTTP, as on a witness host.
func newWitness(t *testing.T) (*Store, Witness, func()) {
	dir := filepath.Join(t.TempDir(), "w")
	pub, err := Init(dir)
	require.NoError(t, err)
	st, err := Open(dir)
	require.NoError(t, err)
	srv := httptest.NewServer(st.Handler())
	key, err := ParsePublicKey(pub)
	require.NoError(t, err)
	return st, Witness{URL: srv.URL, Key: key}, srv.Close
}

func TestInitIsIdempotentAndPrivate(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "w")
	a, err := Init(dir)
	require.NoError(t, err)
	b, err := Init(dir)
	require.NoError(t, err)
	assert.Equal(t, a, b)
	fi, err := os.Stat(filepath.Join(dir, keyFile))
	require.NoError(t, err)
	assert.Equal(t, os.FileMode(0o600), fi.Mode().Perm())
	require.NoError(t, os.Chmod(dir, 0o755))
	_, err = Open(dir)
	assert.Error(t, err, "a state directory others can read is refused")
}

// A damaged key is never replaced: the witness's identity would change
// under every node and Recover.
func TestInitKeepsADamagedKey(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "w")
	_, err := Init(dir)
	require.NoError(t, err)
	file := filepath.Join(dir, keyFile)
	require.NoError(t, os.WriteFile(file, []byte("not hex\n"), 0o600))
	_, err = Init(dir)
	require.ErrorContains(t, err, "left untouched")
	b, err := os.ReadFile(file)
	require.NoError(t, err)
	assert.Equal(t, "not hex\n", string(b))
}

func TestSupersededLineageAndReceipts(t *testing.T) {
	st, w, stop := newWitness(t)
	defer stop()
	ctx := context.Background()

	a := Ask(ctx, w, clusterA)
	require.NoError(t, a.Err)
	assert.Equal(t, Clear, a.Verdict)
	_, err := FetchReceipt(ctx, w, clusterA, clusterB, voter1)
	require.Error(t, err, "no receipt before the operator wrote the fence")

	require.NoError(t, st.Supersede(clusterA, clusterB, []consensus.NodeID{voter1, voter2}, "stopped"))
	require.Error(t, st.Supersede(clusterA, consensus.ClusterID{0xc}, nil, "stopped"), "a lineage once superseded stays so")

	a = Ask(ctx, w, clusterA)
	require.NoError(t, a.Err)
	assert.Equal(t, Superseded, a.Verdict)
	assert.Equal(t, clusterB.String(), a.By)
	assert.Equal(t, Clear, Ask(ctx, w, clusterB).Verdict)

	sig, err := FetchReceipt(ctx, w, clusterA, clusterB, voter1)
	require.NoError(t, err)
	assert.True(t, ed25519.Verify(w.Key, ReceiptMessage(clusterA, clusterB, voter1), sig))
	_, err = FetchReceipt(ctx, w, clusterA, clusterB, consensus.NodeID(0x3333))
	assert.Error(t, err, "no receipt for a voter the operator did not fence")
}

func TestAnAnswerUnderAnotherKeyIsNoAnswer(t *testing.T) {
	_, w, stop := newWitness(t)
	defer stop()
	other, _, _ := ed25519.GenerateKey(nil)
	w.Key = other
	a := Ask(context.Background(), w, clusterA)
	assert.Equal(t, Unanswered, a.Verdict)
	assert.Error(t, a.Err)
}

func TestBootCheckMatrix(t *testing.T) {
	ctx := context.Background()
	st1, w1, stop1 := newWitness(t)
	defer stop1()
	_, w2, stop2 := newWitness(t)
	defer stop2()
	down := Witness{URL: "http://127.0.0.1:1", Key: w2.Key}

	_, err := Check(ctx, []Witness{down, {URL: "http://127.0.0.1:2", Key: w1.Key}}, clusterA)
	require.ErrorIs(t, err, ErrNoWitness, "no answer: refused")

	_, err = Check(ctx, []Witness{w1, down}, clusterA)
	require.NoError(t, err, "one clear and one silent: starts")

	require.NoError(t, st1.Supersede(clusterA, clusterB, []consensus.NodeID{voter1}, "cut"))
	_, err = Check(ctx, []Witness{w1, w2}, clusterA)
	require.ErrorIs(t, err, ErrSuperseded, "one superseded and one clear: refused")

	_, err = Check(ctx, []Witness{w1, down}, clusterA)
	require.ErrorIs(t, err, ErrSuperseded)
	_, err = Check(ctx, []Witness{w1, w2}, clusterB)
	require.NoError(t, err, "the new cluster is clear")
}

func TestVerifyReceiptsNeedsEveryWitness(t *testing.T) {
	ctx := context.Background()
	st1, w1, stop1 := newWitness(t)
	defer stop1()
	st2, w2, stop2 := newWitness(t)
	defer stop2()
	require.NoError(t, st1.Supersede(clusterA, clusterB, []consensus.NodeID{voter1}, "stopped"))
	r1, err := FetchReceipt(ctx, w1, clusterA, clusterB, voter1)
	require.NoError(t, err)
	keys := []ed25519.PublicKey{w1.Key, w2.Key}
	err = VerifyReceipts(keys, clusterA, clusterB, []consensus.FencedVoter{{Voter: voter1, Receipts: [][]byte{r1}}})
	require.Error(t, err, "the second witness has not recorded the fence")

	require.NoError(t, st2.Supersede(clusterA, clusterB, []consensus.NodeID{voter1}, "stopped"))
	r2, err := FetchReceipt(ctx, w2, clusterA, clusterB, voter1)
	require.NoError(t, err)
	require.NoError(t, VerifyReceipts(keys, clusterA, clusterB, []consensus.FencedVoter{{Voter: voter1, Receipts: [][]byte{r1, r2}}}))
	assert.Len(t, hex.EncodeToString(r2), 128)
}

// A second supersede for the same recovery adds voters and drops none.
func TestSupersedeAgainMergesVoters(t *testing.T) {
	st, w, stop := newWitness(t)
	defer stop()
	ctx := context.Background()
	require.NoError(t, st.Supersede(clusterA, clusterB, []consensus.NodeID{voter1}, "stopped"))
	require.NoError(t, st.Supersede(clusterA, clusterB, []consensus.NodeID{voter2}, "powered-off"))
	for _, v := range []consensus.NodeID{voter1, voter2} {
		_, err := FetchReceipt(ctx, w, clusterA, clusterB, v)
		require.NoError(t, err, v.String())
	}
	recs, err := st.Records()
	require.NoError(t, err)
	assert.Equal(t, "stopped", recs[clusterA.String()].Method, "the first record's method stays")
}

// The nonce is framed by a NUL byte in the signed message: it may hold none.
func TestStatusRefusesANonceWithControlBytes(t *testing.T) {
	_, w, stop := newWitness(t)
	defer stop()
	for _, nonce := range []string{"0123456789abcdef\x00x", "0123456789abcdef x", "0123456789abcdef%"} {
		resp, err := http.Get(w.URL + "/v1/status?cluster=" + clusterA.String() + "&nonce=" + url.QueryEscape(nonce))
		require.NoError(t, err)
		resp.Body.Close()
		assert.Equal(t, http.StatusBadRequest, resp.StatusCode, "%q", nonce)
	}
}

// Recover takes the witness keys whose receipts it requires: two at least,
// all different, and every key the nodes are configured to ask at boot.
// One key given twice would pass for "both witnesses" while the other
// witness, never told of the fence, answers "clear" to a fenced voter.
func TestRecoverKeysCoverEveryConfiguredWitness(t *testing.T) {
	k := func() string {
		pub, _, err := ed25519.GenerateKey(rand.Reader)
		require.NoError(t, err)
		return hex.EncodeToString(pub)
	}
	a, b, c := k(), k(), k()

	got, err := RecoverKeys([]string{a, b}, nil)
	require.NoError(t, err)
	assert.Len(t, got, 2)
	_, err = RecoverKeys([]string{a, b}, []string{b, a})
	require.NoError(t, err)

	_, err = RecoverKeys([]string{a}, nil)
	require.ErrorContains(t, err, "both witnesses")
	_, err = RecoverKeys([]string{a, strings.ToUpper(a)}, nil)
	require.ErrorContains(t, err, "twice")
	_, err = RecoverKeys([]string{a, b}, []string{a, b, c})
	require.ErrorContains(t, err, c)
	_, err = RecoverKeys([]string{a, "zz"}, nil)
	require.Error(t, err)
}
