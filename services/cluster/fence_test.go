// Copyright 2026 The Forgejo Authors. All rights reserved.
// SPDX-License-Identifier: GPL-3.0-or-later

package cluster

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"net/http/httptest"
	"path/filepath"
	"testing"

	"forgejo.org/modules/setting"
	"forgejo.org/modules/test"
	"forgejo.org/services/cluster/consensus"
	"forgejo.org/services/cluster/reftable"
	"forgejo.org/services/cluster/witness"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// The whole fence on the SDK's real data directories: three founders of
// cluster A; the node-a survivor is recovered into cluster B once both
// witnesses recorded node-b and node-c fenced; then node-b and node-c, restarted on
// their old directories, are refused at boot, node-a is not, and the
// operator's override lets one start, logged.
func TestOldPairRestartedAfterRecoverIsRefused(t *testing.T) {
	driver, err := consensus.Lookup("kitsunium")
	require.NoError(t, err)
	ctx := context.Background()
	clusterA := consensus.ClusterID{0xaa, 1}
	clusterB := consensus.ClusterID{0xbb, 2}

	dirs := map[string]string{}
	var members []consensus.Member
	for _, slot := range []string{"node-a", "node-b", "node-c"} {
		dirs[slot] = filepath.Join(t.TempDir(), slot)
		id, inc, err := driver.Init(dirs[slot], clusterA, slot)
		require.NoError(t, err)
		var spki [32]byte
		_, _ = rand.Read(spki[:])
		members = append(members, consensus.Member{ID: id, Incarnation: inc, Slot: slot, SPKI: spki, Address: "127.0.0.1:1", Voter: true})
	}
	cfg := func(dir string) consensus.Config {
		return consensus.Config{DataDir: dir, Machine: reftable.New(), MaxLogBytes: 1 << 30, MaxSnapshotBytes: 1 << 30}
	}
	for _, slot := range []string{"node-a", "node-b", "node-c"} {
		require.NoError(t, driver.Bootstrap(ctx, cfg(dirs[slot]), members))
	}

	// Two witnesses, one per witness host.
	var ws []witness.Witness
	var stores []*witness.Store
	var urls, keys []string
	for i := range 2 {
		dir := filepath.Join(t.TempDir(), "witness", string(rune('a'+i)))
		pub, err := witness.Init(dir)
		require.NoError(t, err)
		st, err := witness.Open(dir)
		require.NoError(t, err)
		srv := httptest.NewServer(st.Handler())
		t.Cleanup(srv.Close)
		key, _ := witness.ParsePublicKey(pub)
		ws = append(ws, witness.Witness{URL: srv.URL, Key: key})
		stores = append(stores, st)
		urls, keys = append(urls, srv.URL), append(keys, pub)
	}
	defer test.MockVariableValue(&setting.Cluster.Witnesses, urls)()
	defer test.MockVariableValue(&setting.Cluster.WitnessKeys, keys)()

	// Before any Recover, every node is clear.
	require.NoError(t, FenceCheck(ctx, clusterA))

	// The operator writes the fence on both witnesses, then fetches the receipts.
	old := []consensus.NodeID{members[1].ID, members[2].ID}
	for _, st := range stores {
		require.NoError(t, st.Supersede(clusterA, clusterB, old, "stopped"))
	}
	var fenced []consensus.FencedVoter
	for _, v := range old {
		fv := consensus.FencedVoter{Voter: v, Method: "stopped"}
		for _, w := range ws {
			sig, err := witness.FetchReceipt(ctx, w, clusterA, clusterB, v)
			require.NoError(t, err)
			fv.Receipts = append(fv.Receipts, sig)
		}
		fenced = append(fenced, fv)
	}
	require.NoError(t, witness.VerifyReceipts([]ed25519.PublicKey{ws[0].Key, ws[1].Key}, clusterA, clusterB, fenced))
	survivor := members[0]
	err = driver.Recover(dirs["node-a"], clusterB, []consensus.Member{survivor}, consensus.RecoverOptions{
		Confirm: clusterB.String(), Operator: "local-uid:0", Fenced: fenced,
		WitnessKeys: [][]byte{ws[0].Key, ws[1].Key},
	})
	require.NoError(t, err)

	// The restarted old pair is refused; the recovered node starts.
	for _, slot := range []string{"node-b", "node-c"} {
		rep, err := driver.Inspect(dirs[slot])
		require.NoError(t, err)
		assert.Equal(t, clusterA, rep.Cluster)
		require.ErrorIs(t, FenceCheck(ctx, rep.Cluster), witness.ErrSuperseded, slot)
	}
	rep, err := driver.Inspect(dirs["node-a"])
	require.NoError(t, err)
	assert.Equal(t, clusterB, rep.Cluster)
	require.NoError(t, FenceCheck(ctx, rep.Cluster))

	// The override lets a fenced node start, and says so in the log.
	secret := "break-glass-secret"
	sum := sha256.Sum256([]byte(secret))
	defer test.MockVariableValue(&setting.Cluster.WitnessOverrideSHA256, hex.EncodeToString(sum[:]))()
	t.Setenv(EnvWitnessOverride, "wrong")
	require.ErrorIs(t, FenceCheck(ctx, clusterA), witness.ErrSuperseded, "a wrong secret overrides nothing")
	var logged []error
	prev := logOverride
	logOverride = func(err error) { logged = append(logged, err); prev(err) }
	defer func() { logOverride = prev }()
	t.Setenv(EnvWitnessOverride, secret)
	require.NoError(t, FenceCheck(ctx, clusterA))
	require.Len(t, logged, 1, "the override is logged")
	assert.ErrorIs(t, logged[0], witness.ErrSuperseded)
}

func TestNoWitnessAnswerFailsClosed(t *testing.T) {
	defer test.MockVariableValue(&setting.Cluster.Witnesses, []string{"http://127.0.0.1:1", "http://127.0.0.1:2"})()
	defer test.MockVariableValue(&setting.Cluster.WitnessKeys, []string{hex.EncodeToString(make([]byte, 32)), hex.EncodeToString(make([]byte, 32))})()
	require.ErrorIs(t, FenceCheck(context.Background(), consensus.ClusterID{1}), witness.ErrNoWitness)
}
