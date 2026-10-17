// Copyright 2026 The Forgejo Authors. All rights reserved.
// SPDX-License-Identifier: GPL-3.0-or-later

package cluster

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
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
	assert.Equal(t, hex.EncodeToString(sum[:])+"  table.snap\n", string(line), "sha256sum -c works beside the file")
	entries, _ := os.ReadDir(dir)
	for _, e := range entries {
		assert.False(t, strings.HasSuffix(e.Name(), ".partial"), e.Name())
	}
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
