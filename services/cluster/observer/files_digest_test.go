// Copyright 2026 The Forgejo Authors. All rights reserved.
// SPDX-License-Identifier: GPL-3.0-or-later

package observer

import (
	"bytes"
	"context"
	"os"
	"testing"

	"forgejo.org/modules/clusterhook"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func blobOf(t *testing.T, o *Observer, storage, path string) string {
	t.Helper()
	st := o.State()
	m, err := o.store.ReadManifest(st.Checkpoints[len(st.Checkpoints)-1].Name)
	require.NoError(t, err)
	for _, f := range m.Files {
		if f.Storage == storage && f.Path == path {
			return f.SHA256
		}
	}
	t.Fatalf("%s/%s is not in the manifest", storage, path)
	return ""
}

// An attachment rewritten with other bytes of the same size is taken
// again, by its listed digest, never reused by path and size; bytes that
// do not match the listed digest are refused from a member and taken from
// another (#106).
func TestObserverTakesAttachmentsByDigest(t *testing.T) {
	ctx := context.Background()
	c := newFakeCluster(t)
	o := newTestObserver(t, c, &hookLog{})
	c.storeAttachment("ab/cd/one", "first bytes")
	seed(t, c, o)
	require.NoError(t, o.Step(ctx))
	assert.Equal(t, digest([]byte("first bytes")), blobOf(t, o, "attachments", "ab/cd/one"))

	c.storeAttachment("ab/cd/one", "other bytes")
	c.tamper = func(slot string, req *clusterhook.ExportRequest, data []byte) []byte {
		if req.Op == clusterhook.OpExportFile && slot == "node-a" {
			return bytes.ToUpper(data)
		}
		return data
	}
	c.push("alice/demo.git", "refs/heads/main", c.commit("two"))
	c.checkpoint()
	require.NoError(t, o.Step(ctx))
	sum := blobOf(t, o, "attachments", "ab/cd/one")
	assert.Equal(t, digest([]byte("other bytes")), sum)
	held, err := os.ReadFile(o.store.blobPath(sum))
	require.NoError(t, err)
	assert.Equal(t, "other bytes", string(held))
}

// A package blob is checked against the digest its path names (#106).
func TestExpectedDigest(t *testing.T) {
	sum := digest([]byte("x"))
	for _, tc := range []struct {
		f    clusterhook.FileEntry
		want string
		err  bool
	}{
		{clusterhook.FileEntry{Storage: "lfs", Path: sum[:2] + "/" + sum[2:4] + "/" + sum[4:]}, sum, false},
		{clusterhook.FileEntry{Storage: "packages", Path: sum[:2] + "/" + sum[2:4] + "/" + sum}, sum, false},
		{clusterhook.FileEntry{Storage: "packages", Path: "tmp/upload"}, "", false},
		{clusterhook.FileEntry{Storage: "attachments", Path: "a/b/c", SHA256: sum}, sum, false},
		{clusterhook.FileEntry{Storage: "attachments", Path: "a/b/c"}, "", false},
		{clusterhook.FileEntry{Storage: "attachments", Path: "a/b/c", SHA256: "../../etc"}, "", true},
		{clusterhook.FileEntry{Storage: "lfs", Path: "../x"}, "", true},
	} {
		got, err := expectedDigest(tc.f)
		if tc.err {
			require.Error(t, err, tc.f)
			continue
		}
		require.NoError(t, err, tc.f)
		assert.Equal(t, tc.want, got, tc.f)
	}
}

// A leader that predates listed digests still has its attachments reused
// by path and size.
func TestObserverReusesLegacyAttachmentsByPath(t *testing.T) {
	ctx := context.Background()
	c := newFakeCluster(t)
	c.legacyFiles = true
	o := newTestObserver(t, c, &hookLog{})
	c.storeAttachment("ab/cd/one", "first bytes")
	seed(t, c, o)
	require.NoError(t, o.Step(ctx))
	c.push("alice/demo.git", "refs/heads/main", c.commit("two"))
	c.checkpoint()
	require.NoError(t, o.Step(ctx))
	assert.Equal(t, digest([]byte("first bytes")), blobOf(t, o, "attachments", "ab/cd/one"))
}
