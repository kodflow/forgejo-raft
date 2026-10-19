// Copyright 2026 The Forgejo Authors. All rights reserved.
// SPDX-License-Identifier: GPL-3.0-or-later

package cluster

import (
	"bufio"
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"forgejo.org/modules/clusterhook"
	"forgejo.org/modules/json"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/crypto/nacl/box"
)

func exportOf(t *testing.T, n *testNode, req *clusterhook.ExportRequest) (clusterhook.RawJSON, []byte, error) {
	t.Helper()
	data, stream, err := n.r.export1(context.Background(), req)
	if err != nil || stream == nil {
		return data, nil, err
	}
	var buf bytes.Buffer
	require.NoError(t, stream(context.Background(), &buf))
	return data, buf.Bytes(), nil
}

// The log and the checkpoints come from the leader alone; a follower names
// the leader's slot, and the export of a follower never serves either.
func TestExportIsLeaderOnlyWithAHint(t *testing.T) {
	tc := newTestCluster(t, "node-a", "node-b", "node-c")
	pg := newFakePG()
	withCheckpoints(t, tc, pg)
	_, err := tc.nodes[0].r.checkpoints.take(context.Background())
	require.NoError(t, err)

	for _, op := range []string{clusterhook.OpExportCheckpoints, clusterhook.OpExportLog, clusterhook.OpExportRefs, clusterhook.OpExportFiles} {
		_, _, err := exportOf(t, tc.nodes[1], &clusterhook.ExportRequest{Op: op})
		require.Error(t, err, op)
		assert.Equal(t, clusterhook.CodeNotLeader, clusterhook.ExportCode(err), op)
		var ee *clusterhook.ExportError
		require.ErrorAs(t, err, &ee)
		assert.Equal(t, "node-a", ee.Leader, op)
	}

	data, _, err := exportOf(t, tc.nodes[0], &clusterhook.ExportRequest{Op: clusterhook.OpExportCheckpoints})
	require.NoError(t, err)
	var v clusterhook.ExportView
	require.NoError(t, json.Unmarshal(data, &v))
	assert.Equal(t, "node-a", v.Leader)
	assert.True(t, v.LogSupported)
	require.Len(t, v.Checkpoints, 1)
	assert.Equal(t, []string{"node-a", "node-b", "node-c"}, v.Members, "the observer is never among them")
	var voters []string
	for _, m := range tc.nodes[0].r.Engine().Membership().Members {
		if m.Voter {
			voters = append(voters, m.ID.String())
		}
	}
	assert.Len(t, voters, 3)
	assert.Equal(t, voters, v.Voters, "the voters a recovery from this checkpoint must fence")

	// The artifact the leader serves is the one the position names.
	_, refs, err := exportOf(t, tc.nodes[0], &clusterhook.ExportRequest{Op: clusterhook.OpExportRefs, Checkpoint: v.Checkpoints[0].Name})
	require.NoError(t, err)
	assert.Equal(t, v.Checkpoints[0].RefsSHA256, sha256Hex(refs))
	_, _, err = exportOf(t, tc.nodes[0], &clusterhook.ExportRequest{Op: clusterhook.OpExportRefs, Checkpoint: "../../etc"})
	require.Error(t, err)
}

func sha256Hex(b []byte) string {
	h := sha256.Sum256(b)
	return hex.EncodeToString(h[:])
}

// The log is read after a position the observer names with its term: the
// entries follow it, a different term there is a divergence, and a log
// compacted past it says so.
func TestExportLogFollowsAndDetectsDivergence(t *testing.T) {
	tc := newTestCluster(t, "node-a", "node-b", "node-c")
	work := t.TempDir()
	run(t, work, "init", "-q")
	zero := strings.Repeat("0", 40)
	c1 := commit(t, work, "one")
	require.True(t, tc.pushVia(tc.nodes[0], work, "alice/log.git", "refs/heads/main", zero, c1).OK)
	c2 := commit(t, work, "two")
	require.True(t, tc.pushVia(tc.nodes[0], work, "alice/log.git", "refs/heads/main", c1, c2).OK)
	lineage := tc.nodes[0].node.Status().Identity.String()

	read := func(from, term uint64) ([]clusterhook.ExportEntry, error) {
		_, body, err := exportOf(t, tc.nodes[0], &clusterhook.ExportRequest{Op: clusterhook.OpExportLog, Lineage: lineage, FromIndex: from, FromTerm: term})
		if err != nil {
			return nil, err
		}
		var out []clusterhook.ExportEntry
		sc := bufio.NewScanner(bytes.NewReader(body))
		for sc.Scan() {
			var e clusterhook.ExportEntry
			require.NoError(t, json.Unmarshal(sc.Bytes(), &e))
			out = append(out, e)
		}
		return out, nil
	}
	all, err := read(0, 0)
	require.NoError(t, err)
	require.GreaterOrEqual(t, len(all), 2)
	assert.EqualValues(t, 1, all[0].Index)
	tail, err := read(all[0].Index, all[0].Term)
	require.NoError(t, err)
	assert.Equal(t, all[1:], tail)

	_, err = read(all[0].Index, all[0].Term+7)
	assert.Equal(t, clusterhook.CodeDiverged, clusterhook.ExportCode(err))
	_, err = read(1000, 1)
	assert.Equal(t, clusterhook.CodeDiverged, clusterhook.ExportCode(err), "past the commit")
	_, _, err = exportOf(t, tc.nodes[0], &clusterhook.ExportRequest{Op: clusterhook.OpExportLog, Lineage: "00/1"})
	assert.Equal(t, clusterhook.CodeLineage, clusterhook.ExportCode(err))

	tc.mem.Compact(1)
	_, err = read(1, all[0].Term)
	assert.Equal(t, clusterhook.CodeCompacted, clusterhook.ExportCode(err))
}

// Packs are bounded to ref values: an object no ref names is not served.
func TestExportPacksAreBoundToRefs(t *testing.T) {
	tc := newTestCluster(t, "node-a", "node-b", "node-c")
	work := t.TempDir()
	run(t, work, "init", "-q")
	c1 := commit(t, work, "one")
	require.True(t, tc.pushVia(tc.nodes[0], work, "alice/packs.git", "refs/heads/main", strings.Repeat("0", 40), c1).OK)
	tree := run(t, work, "rev-parse", "HEAD^{tree}")

	_, pack, err := exportOf(t, tc.nodes[1], &clusterhook.ExportRequest{Op: clusterhook.OpExportPacks, Repo: "alice/packs.git", Wants: []string{c1}})
	require.NoError(t, err, "any member serves packs")
	assert.True(t, bytes.HasPrefix(pack, []byte("PACK")))
	_, _, err = exportOf(t, tc.nodes[1], &clusterhook.ExportRequest{Op: clusterhook.OpExportPacks, Repo: "alice/packs.git", Wants: []string{tree}})
	require.ErrorContains(t, err, "not a ref value")
	_, _, err = exportOf(t, tc.nodes[1], &clusterhook.ExportRequest{Op: clusterhook.OpExportPacks, Repo: "../x.git", Wants: []string{c1}})
	require.Error(t, err)
}

// pg_basebackup -D - carries one tablespace and the observer copies a
// dedicated PostgreSQL: anything else is refused before a byte is sent.
func TestExportPGBaseRefusesTablespacesAndSharedClusters(t *testing.T) {
	tc := newTestCluster(t, "node-a", "node-b", "node-c")
	pg := newFakePG()
	n := tc.nodes[2]
	called := false
	n.r.export = exportSide{wal: pg, baseBackup: func(_ context.Context, w io.Writer) error {
		called = true
		_, err := w.Write([]byte("tar"))
		return err
	}}
	_, body, err := exportOf(t, n, &clusterhook.ExportRequest{Op: clusterhook.OpExportPGBase})
	require.NoError(t, err)
	assert.Equal(t, "tar", string(body))

	called = false
	pg.spcs = []string{"fast_ssd"}
	_, _, err = exportOf(t, n, &clusterhook.ExportRequest{Op: clusterhook.OpExportPGBase})
	require.ErrorContains(t, err, "tablespaces")
	assert.False(t, called)

	pg.spcs, pg.dbs = nil, []string{"authentik"}
	_, _, err = exportOf(t, n, &clusterhook.ExportRequest{Op: clusterhook.OpExportPGBase})
	require.ErrorContains(t, err, "dedicated")
	assert.False(t, called)
}

// The WAL archive serves archive files only, the secrets go sealed to the
// observer the forced command names, and stored files stay in their root.
func TestExportArchiveFilesAndSecrets(t *testing.T) {
	tc := newTestCluster(t, "node-a", "node-b", "node-c")
	n := tc.nodes[1]
	archive, lfs := t.TempDir(), t.TempDir()
	for _, f := range []string{"000000010000000000000001", "000000010000000000000002", "00000002.history", "junk"} {
		require.NoError(t, os.WriteFile(filepath.Join(archive, f), []byte(f), 0o600))
	}
	require.NoError(t, os.MkdirAll(filepath.Join(lfs, "ab", "cd"), 0o700))
	require.NoError(t, os.WriteFile(filepath.Join(lfs, "ab", "cd", "abcdef"), []byte("lfs object"), 0o600))
	pub, priv, err := box.GenerateKey(rand.Reader)
	require.NoError(t, err)
	n.r.export = exportSide{
		archiveDir: archive, storages: func() (map[string]string, error) { return map[string]string{StorageLFS: lfs}, nil },
		recipients: map[string]string{"nas-paris": hex.EncodeToString(pub[:])}, secretKey: func() string { return "the-secret-key" },
	}

	data, _, err := exportOf(t, n, &clusterhook.ExportRequest{Op: clusterhook.OpExportWALList, Name: "000000010000000000000001"})
	require.NoError(t, err)
	var files []clusterhook.WALFile
	require.NoError(t, json.Unmarshal(data, &files))
	assert.Equal(t, []clusterhook.WALFile{
		{Name: "000000010000000000000002", Size: 24},
		{Name: "00000002.history", Size: 16, SHA256: sha256Hex([]byte("00000002.history"))},
	}, files, "after the name given, and every history with its digest")
	_, seg, err := exportOf(t, n, &clusterhook.ExportRequest{Op: clusterhook.OpExportWAL, Name: "00000002.history"})
	require.NoError(t, err)
	assert.Equal(t, "00000002.history", string(seg))
	_, _, err = exportOf(t, n, &clusterhook.ExportRequest{Op: clusterhook.OpExportWAL, Name: "junk"})
	require.Error(t, err)

	_, obj, err := exportOf(t, n, &clusterhook.ExportRequest{Op: clusterhook.OpExportFile, Storage: StorageLFS, Name: "ab/cd/abcdef"})
	require.NoError(t, err)
	assert.Equal(t, "lfs object", string(obj))
	for _, bad := range []string{"../escape", "/abs", "ab/../ab/cd/abcdef", ""} {
		_, _, err = exportOf(t, n, &clusterhook.ExportRequest{Op: clusterhook.OpExportFile, Storage: StorageLFS, Name: bad})
		require.Error(t, err, bad)
	}

	_, sealed, err := exportOf(t, n, &clusterhook.ExportRequest{Op: clusterhook.OpExportSecrets, Observer: "nas-paris"})
	require.NoError(t, err)
	opened, ok := box.OpenAnonymous(nil, sealed, pub, priv)
	require.True(t, ok)
	var bundle SecretsBundle
	require.NoError(t, json.Unmarshal(opened, &bundle))
	assert.Equal(t, "the-secret-key", bundle.SecretKey)
	_, _, err = exportOf(t, n, &clusterhook.ExportRequest{Op: clusterhook.OpExportSecrets, Observer: "nas-other"})
	require.Error(t, err)
}

// Through the private socket, an export answers with its head and streams
// past the 32 MiB a JSON answer may hold.
func TestExportStreamsOverTheHookSocket(t *testing.T) {
	tc := newTestCluster(t, "node-a", "node-b", "node-c")
	n := tc.nodes[0]
	archive := t.TempDir()
	big := bytes.Repeat([]byte{0x5a}, 40<<20)
	require.NoError(t, os.WriteFile(filepath.Join(archive, "000000010000000000000001"), big, 0o600))
	n.r.export = exportSide{archiveDir: archive}
	sock := filepath.Join(shortTempDir(t), "h.sock")
	require.NoError(t, n.r.serveHookSocket(sock))

	ans, body, err := clusterhook.CallExport(context.Background(), sock, &clusterhook.ExportRequest{Op: clusterhook.OpExportWAL, Name: "000000010000000000000001"})
	require.NoError(t, err)
	require.True(t, ans.OK && ans.Stream)
	got, err := io.ReadAll(body)
	body.Close()
	require.NoError(t, err)
	assert.Len(t, got, len(big))

	ans, _, err = clusterhook.CallExport(context.Background(), sock, &clusterhook.ExportRequest{Op: clusterhook.OpExportWAL, Name: "nope"})
	require.NoError(t, err)
	assert.False(t, ans.OK)
}

// shortTempDir is a temporary directory whose path fits a Unix socket's.
func shortTempDir(t *testing.T) string {
	dir, err := os.MkdirTemp("/tmp", "fjx") //nolint:usetesting // t.TempDir is too long for a socket path on macOS
	require.NoError(t, err)
	t.Cleanup(func() { os.RemoveAll(dir) })
	return dir
}
