// Copyright 2026 The Forgejo Authors. All rights reserved.
// SPDX-License-Identifier: GPL-3.0-or-later

package cluster

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"forgejo.org/services/cluster/consensus"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// The snapshot lands with its digest last, named by its base name; a
// .partial a crash left behind does not block the next save.
func TestSaveSnapshotLandsDigestLast(t *testing.T) {
	tc := newTestCluster(t, "node-a", "node-b", "node-c")
	dir := t.TempDir()
	path := filepath.Join(dir, "table.snap")
	require.NoError(t, os.WriteFile(path+".partial", []byte("left by a crash"), 0o600))

	_, err := tc.nodes[0].r.SaveSnapshot(context.Background(), OperatorFor(1), path)
	require.NoError(t, err)
	data, err := os.ReadFile(path)
	require.NoError(t, err)
	sum := sha256.Sum256(data)
	line, err := os.ReadFile(path + ".sha256")
	require.NoError(t, err)
	want := hex.EncodeToString(sum[:]) + "  table.snap\n"
	// An engine that writes metadata has it bound by a second line.
	if meta, err := os.ReadFile(path + ".meta"); err == nil {
		msum := sha256.Sum256(meta)
		want += hex.EncodeToString(msum[:]) + "  table.snap.meta\n"
	}
	assert.Equal(t, want, string(line), "sha256sum -c works beside the file and its metadata")
	entries, _ := os.ReadDir(dir)
	for _, e := range entries {
		assert.False(t, strings.HasSuffix(e.Name(), ".partial"), e.Name())
	}
}

// The operator is told the snapshot is saved only once the file, its
// digest and its metadata are durable: the directory holding their names is
// synced after the last rename, or a power loss right after the answer can
// take the digest or the metadata a restore needs.
func TestSaveSnapshotSyncsItsDirectory(t *testing.T) {
	tc := newTestCluster(t, "node-a", "node-b", "node-c")
	dir := t.TempDir()
	path := filepath.Join(dir, "table.snap")
	var synced []string
	defer func(prev func(string) error) { syncDirFn = prev }(syncDirFn)
	syncDirFn = func(d string) error {
		_, err := os.Stat(path + ".sha256")
		synced = append(synced, fmt.Sprintf("%s digest=%t", d, err == nil))
		return nil
	}
	_, err := tc.nodes[0].r.SaveSnapshot(context.Background(), OperatorFor(1), path)
	require.NoError(t, err)
	assert.Contains(t, synced, dir+" digest=true", "the directory is synced once the digest has its name")
}

// A peer's object format is checked before git sees it.
func TestObjectChannelRefusesAnUnknownFormat(t *testing.T) {
	tc := newTestCluster(t, "node-a", "node-b", "node-c")
	from, to := tc.nodes[0], tc.nodes[1]
	var target consensus.Member
	for _, m := range from.r.Engine().Membership().Members {
		if m.Slot == to.slot {
			target = m
		}
	}
	resp, err := from.r.objects.client(target).Post("https://"+to.addr+"/v1/push?repo=o/r.git&format=--template=x", "application/octet-stream", strings.NewReader(""))
	require.NoError(t, err)
	resp.Body.Close()
	assert.Equal(t, http.StatusBadRequest, resp.StatusCode)
	require.Error(t, initBare(context.Background(), filepath.Join(t.TempDir(), "x.git"), "sha3"))
}

type inspectDriver struct {
	consensus.Driver
	err error
}

func (d inspectDriver) Inspect(string) (consensus.Report, error) { return consensus.Report{}, d.err }

// Only a missing data directory skips the witness check; one that cannot be
// read fails the start instead of skipping the fence.
func TestFenceCheckFailsWhenTheDataDirectoryCannotBeRead(t *testing.T) {
	ctx := context.Background()
	require.NoError(t, fenceOrExit(ctx, "x", inspectDriver{err: os.ErrNotExist}))
	require.NoError(t, fenceOrExit(ctx, "x", inspectDriver{}), "not stamped yet")
	require.ErrorContains(t, fenceOrExit(ctx, "x", inspectDriver{err: os.ErrPermission}), "witness check")
}

// The digest `snapshot save` writes and logs is the one a restore takes:
// the file is authenticated as written, the metadata by its line beside
// it, and a changed file or metadata is refused (#63).
func TestHuntR3ProveRestoreDigest(t *testing.T) {
	tc := newTestClusterOn(t, true, "node-a", "node-b", "node-c")
	dir := t.TempDir()
	path := filepath.Join(dir, "table.snap")
	_, err := tc.nodes[0].r.SaveSnapshot(context.Background(), OperatorFor(1), path)
	require.NoError(t, err)
	line, err := os.ReadFile(path + ".sha256")
	require.NoError(t, err)
	given, err := hex.DecodeString(strings.Fields(string(line))[0])
	require.NoError(t, err)

	driver, err := consensus.Lookup("kitsunium")
	require.NoError(t, err)
	restore := func(t *testing.T) error {
		f, meta, err := OpenSavedSnapshot(path, [32]byte(given))
		if err != nil {
			return err
		}
		defer f.Close()
		data := t.TempDir()
		cid := consensus.ClusterID{0x0e, 0x57, 0x03}
		id, inc, err := driver.Init(data, cid, "node-r")
		require.NoError(t, err)
		members := []consensus.Member{{ID: id, Incarnation: inc, Slot: "node-r", SPKI: [32]byte{1}, Address: "127.0.0.1:1", Voter: true}}
		_, err = driver.Restore(data, f, cid, members, consensus.RestoreOptions{Meta: meta, Confirm: cid.String(), Operator: OperatorFor(1), MaxBytes: 1 << 30})
		return err
	}
	require.NoError(t, restore(t), "the digest of table.snap.sha256 restores")

	data, err := os.ReadFile(path)
	require.NoError(t, err)
	meta, err := os.ReadFile(path + ".meta")
	require.NoError(t, err)

	t.Run("a changed file", func(t *testing.T) {
		changed := append([]byte(nil), data...)
		changed[len(changed)/2] ^= 0x01
		require.NoError(t, os.WriteFile(path, changed, 0o600))
		t.Cleanup(func() { require.NoError(t, os.WriteFile(path, data, 0o600)) })
		require.ErrorContains(t, restore(t), "does not match the digest given")
	})
	t.Run("a changed metadata", func(t *testing.T) {
		changed := append([]byte(nil), meta...)
		changed[len(changed)-1] ^= 0x01
		require.NoError(t, os.WriteFile(path+".meta", changed, 0o600))
		t.Cleanup(func() { require.NoError(t, os.WriteFile(path+".meta", meta, 0o600)) })
		require.ErrorContains(t, restore(t), "does not match its digest")
	})
}
