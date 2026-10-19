// Copyright 2026 The Forgejo Authors. All rights reserved.
// SPDX-License-Identifier: GPL-3.0-or-later

package observer

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// A recovery reads the observer directory without writing to it, restores
// PostgreSQL with the observer's configuration and never the source's, and
// restores the repositories and stored files of the checkpoint chosen.
func TestRecoverRestoresWithATrustedConfiguration(t *testing.T) {
	ctx := context.Background()
	c := newFakeCluster(t)
	o := newTestObserver(t, c, &hookLog{})
	// The source's base backup carries a hostile configuration.
	c.pg.extra = map[string]string{
		"postgresql.auto.conf": "archive_command = 'curl evil'\nshared_preload_libraries = 'evil'\n",
		"postgresql.conf":      "include '/etc/evil.conf'\n",
		"standby.signal":       "",
		"conf.d/evil.conf":     "ssl = off\n",
	}
	cp := seed(t, c, o)
	require.NoError(t, o.Step(ctx))

	a, err := OpenArchive(o.cfg.Dir, "")
	require.NoError(t, err)
	list := a.List()
	require.Len(t, list, 1)
	assert.Equal(t, cp.Name(), list[0].Name)
	assert.Equal(t, StateProtected, list[0].State)
	assert.Empty(t, list[0].Problem)

	pgdata, restore := filepath.Join(t.TempDir(), "pgdata"), filepath.Join(t.TempDir(), "restore")
	require.NoError(t, a.RestorePG(cp.Name(), pgdata, PGOptions{Port: 5433, SocketDir: pgdata, RestoreDir: restore}))
	conf, err := os.ReadFile(filepath.Join(pgdata, "postgresql.conf"))
	require.NoError(t, err)
	for _, want := range []string{"recovery_target_lsn = '" + cp.LSN + "'", "recovery_target_timeline = '1'", "recovery_target_inclusive = off", "recovery_target_action = 'promote'", "hot_standby = off", "archive_mode = off", "restore_command = 'cp \"" + restore + "/%f\" \"%p\"'"} {
		assert.Contains(t, string(conf), want)
	}
	assert.NotContains(t, string(conf), "evil")
	auto, err := os.ReadFile(filepath.Join(pgdata, "postgresql.auto.conf"))
	require.NoError(t, err)
	assert.NotContains(t, string(auto), "evil")
	for _, gone := range []string{"standby.signal", "conf.d"} {
		_, err := os.Stat(filepath.Join(pgdata, gone))
		assert.True(t, os.IsNotExist(err), gone)
	}
	_, err = os.Stat(filepath.Join(pgdata, "recovery.signal"))
	require.NoError(t, err)
	m, err := a.Manifest(cp.Name())
	require.NoError(t, err)
	for _, seg := range m.PG.Segments {
		b, err := os.ReadFile(filepath.Join(restore, seg))
		require.NoError(t, err, "the restore directory holds %s, decompressed", seg)
		assert.Equal(t, c.pg.archive[seg], b)
	}
	require.Error(t, a.RestorePG(cp.Name(), pgdata, PGOptions{SocketDir: pgdata, RestoreDir: t.TempDir()}), "never over a data directory")
	_, err = RecoveryConfig(m, PGOptions{SocketDir: "/tmp/x' ; evil", RestoreDir: restore})
	require.Error(t, err)

	root := t.TempDir()
	n, err := a.RestoreRepos(ctx, cp.Name(), root)
	require.NoError(t, err)
	assert.Equal(t, 1, n)
	main, _ := c.table.Get("alice/demo.git", "refs/heads/main")
	assert.Equal(t, main.SHA, c.git(filepath.Join(root, "alice", "demo.git"), "rev-parse", "refs/heads/main"))

	lfs := t.TempDir()
	files, err := a.RestoreFiles(cp.Name(), map[string]string{"lfs": lfs})
	require.NoError(t, err)
	assert.Equal(t, 1, files)
	for k, v := range c.files {
		b, err := os.ReadFile(filepath.Join(lfs, filepath.FromSlash(strings.TrimPrefix(k, "lfs/"))))
		require.NoError(t, err)
		assert.Equal(t, v, b)
	}

	bundle, err := a.OpenSecrets(cp.Name(), o.cfg.SecretsKey)
	require.NoError(t, err)
	assert.Equal(t, "fake-secret", bundle.SecretKey)
}

// The primary's configuration archives afresh, and quotes are refused.
func TestPrimaryConfigArchivesAfresh(t *testing.T) {
	conf, err := PrimaryConfig(PGOptions{Port: 5432, SocketDir: "/run/postgresql", Listen: "10.0.0.1", ArchiveCommand: "/usr/local/bin/forgejo cluster wal-archive --dir /srv/wal %p %f"})
	require.NoError(t, err)
	assert.Contains(t, conf, "archive_mode = on")
	assert.Contains(t, conf, "archive_timeout = 30s")
	_, err = PrimaryConfig(PGOptions{ArchiveCommand: "x' ; y"})
	require.Error(t, err)
}

// A recovery resumes from its progress file, which skips the steps it
// records: a step is recorded only once what it restored is durable, or a
// power loss after the record leaves PostgreSQL's data directory, the
// repositories or the stored files truncated and never restored again.
// Every file and directory each restore writes is synced before it returns.
func TestRecoverSyncsWhatItRestores(t *testing.T) {
	ctx := context.Background()
	c := newFakeCluster(t)
	o := newTestObserver(t, c, &hookLog{})
	cp := seed(t, c, o)
	require.NoError(t, o.Step(ctx))
	a, err := OpenArchive(o.cfg.Dir, "")
	require.NoError(t, err)

	synced := map[string]bool{}
	prev := syncPath
	defer func() { syncPath = prev }()
	syncPath = func(p string) error {
		synced[p] = true
		return prev(p)
	}
	// Every regular file and directory under root, and root's parent.
	assertSynced := func(t *testing.T, root string) {
		t.Helper()
		assert.True(t, synced[filepath.Dir(root)], "the directory holding %s", root)
		require.NoError(t, filepath.WalkDir(root, func(p string, d os.DirEntry, err error) error {
			require.NoError(t, err)
			if d.IsDir() || d.Type().IsRegular() {
				assert.True(t, synced[p], "%s is synced", p)
			}
			return nil
		}))
	}

	pgdata, restore := filepath.Join(t.TempDir(), "pgdata"), filepath.Join(t.TempDir(), "restore")
	require.NoError(t, a.RestorePG(cp.Name(), pgdata, PGOptions{Port: 5433, SocketDir: pgdata, RestoreDir: restore}))
	assertSynced(t, pgdata)
	assertSynced(t, restore)

	root := t.TempDir()
	_, err = a.RestoreRepos(ctx, cp.Name(), root)
	require.NoError(t, err)
	assertSynced(t, filepath.Join(root, "alice"))

	// A stored file is synced under its temporary name before it is
	// linked: the directories leading to it are what remains.
	lfs := t.TempDir()
	_, err = a.RestoreFiles(cp.Name(), map[string]string{"lfs": lfs})
	require.NoError(t, err)
	require.NoError(t, filepath.WalkDir(lfs, func(p string, d os.DirEntry, err error) error {
		require.NoError(t, err)
		if d.IsDir() {
			assert.True(t, synced[p], "%s is synced", p)
		}
		return nil
	}))
}

// The recovery's progress file is replaced durably: the new record is
// synced, then renamed, and its directory synced.
func TestWriteFileDurableReplacesAndSyncs(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "progress.json")
	require.NoError(t, os.WriteFile(path, []byte("old"), 0o600))
	var synced []string
	prev := syncPath
	defer func() { syncPath = prev }()
	syncPath = func(p string) error { synced = append(synced, p); return prev(p) }

	require.NoError(t, WriteFileDurable(path, []byte("new")))
	b, err := os.ReadFile(path)
	require.NoError(t, err)
	assert.Equal(t, "new", string(b))
	assert.Equal(t, []string{dir}, synced, "the directory is synced after the rename")
	info, err := os.Stat(path)
	require.NoError(t, err)
	assert.Equal(t, os.FileMode(0o600), info.Mode().Perm())
}
