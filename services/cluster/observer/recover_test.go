// Copyright 2026 The Forgejo Authors. All rights reserved.
// SPDX-License-Identifier: GPL-3.0-or-later

package observer

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"forgejo.org/modules/json"
	"forgejo.org/services/cluster/reftable"

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

	// Directories the restore creates on the way are named durably in
	// their parents, up to one that existed (CodeRabbit on #27).
	base := t.TempDir()
	deep := filepath.Join(base, "new", "deep", "pgdata")
	require.NoError(t, a.RestorePG(cp.Name(), deep, PGOptions{Port: 5433, SocketDir: deep, RestoreDir: filepath.Join(base, "new", "wal")}))
	for _, d := range []string{base, filepath.Join(base, "new"), filepath.Join(base, "new", "deep")} {
		assert.True(t, synced[d], "%s, which names a directory the restore created, is synced", d)
	}
	made := t.TempDir()
	_, err = a.RestoreFiles(cp.Name(), map[string]string{"lfs": filepath.Join(made, "made", "lfs")})
	require.NoError(t, err)
	assert.True(t, synced[made], "the directory naming the storage root the restore created")
	assert.True(t, synced[filepath.Join(made, "made")])

	// A retry after a failed attempt finds the directories that attempt
	// created, whose names may not be durable: they are synced all the same.
	retried := t.TempDir()
	require.NoError(t, os.MkdirAll(filepath.Join(retried, "left", "lfs"), 0o750))
	clear(synced)
	_, err = a.RestoreFiles(cp.Name(), map[string]string{"lfs": filepath.Join(retried, "left", "lfs")})
	require.NoError(t, err)
	assert.True(t, synced[retried], "the directory naming one a failed attempt left")
	assert.True(t, synced[filepath.Join(retried, "left")])

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

// The restore directory holds every timeline history the archive holds,
// not only those on the checkpoint's path: the promotion takes the newest
// plus one, never a number the old system used after the checkpoint.
func TestRecoverCopiesEveryTimelineHistory(t *testing.T) {
	ctx := context.Background()
	c := newFakeCluster(t)
	o := newTestObserver(t, c, &hookLog{})
	cp := seed(t, c, o)
	require.NoError(t, o.Step(ctx))
	c.pg.failover()
	c.pg.failover()
	c.push("alice/demo.git", "refs/heads/main", c.commit("on timeline 3"))
	c.checkpoint()
	require.NoError(t, o.Step(ctx))

	a, err := OpenArchive(o.cfg.Dir, "")
	require.NoError(t, err)
	m, err := a.Manifest(cp.Name())
	require.NoError(t, err)
	require.EqualValues(t, 1, m.Checkpoint.Timeline)
	assert.Equal(t, []string{"000000000000000a", "000000000000000b", "000000000000000c"}, m.Voters, "the old voters a recovery must fence")
	pgdata, restore := filepath.Join(t.TempDir(), "pgdata"), filepath.Join(t.TempDir(), "restore")
	require.NoError(t, a.RestorePG(cp.Name(), pgdata, PGOptions{Port: 5433, SocketDir: pgdata, RestoreDir: restore}))
	for _, h := range []string{HistoryName(2), HistoryName(3)} {
		b, err := os.ReadFile(filepath.Join(restore, h))
		require.NoError(t, err, "the restore directory holds %s", h)
		assert.Equal(t, c.pg.archive[h], b)
	}
}

// A repository restore interrupted or failed midway is resumed by running
// it again: a repository written whole is kept, one left marked incomplete
// (killed between git init and the fetch) is written again, and a
// directory this restore did not write is still refused (#58).
func TestHuntR2ProveStep4ResumeAfterFilesError(t *testing.T) {
	ctx := context.Background()
	c := newFakeCluster(t)
	o := newTestObserver(t, c, &hookLog{})
	c.push("bob/other.git", "refs/heads/main", c.commit("other"))
	cp := seed(t, c, o)
	require.NoError(t, o.Step(ctx))
	a, err := OpenArchive(o.cfg.Dir, "")
	require.NoError(t, err)
	m, err := a.Manifest(cp.Name())
	require.NoError(t, err)

	root := t.TempDir()
	n, err := a.RestoreRepos(ctx, cp.Name(), root)
	require.NoError(t, err)
	require.Equal(t, 2, n)
	for repo := range m.Repos {
		_, err := os.Stat(filepath.Join(root, repo, IncompleteMark))
		assert.True(t, os.IsNotExist(err), "%s carries no mark once whole", repo)
	}

	t.Run("a run that stopped after the repositories", func(t *testing.T) {
		n, err := a.RestoreRepos(ctx, cp.Name(), root)
		require.NoError(t, err)
		assert.Equal(t, 2, n)
	})

	t.Run("a repository killed between git init and the fetch", func(t *testing.T) {
		target := filepath.Join(root, "bob", "other.git")
		require.NoError(t, os.RemoveAll(target))
		require.NoError(t, os.MkdirAll(target, 0o755))
		require.NoError(t, os.WriteFile(filepath.Join(target, IncompleteMark), nil, 0o600))
		_, err := o.store.git.run(ctx, filepath.Dir(target), nil, "init", "-q", "--bare", target)
		require.NoError(t, err)
		n, err := a.RestoreRepos(ctx, cp.Name(), root)
		require.NoError(t, err)
		assert.Equal(t, 2, n)
		assert.True(t, o.store.git.restoredRefs(ctx, target, m.Repos["bob/other.git"].Refs))
	})

	t.Run("a repository this restore did not write", func(t *testing.T) {
		other := t.TempDir()
		target := filepath.Join(other, "alice", "demo.git")
		require.NoError(t, os.MkdirAll(target, 0o755))
		require.NoError(t, os.WriteFile(filepath.Join(target, "precious"), []byte("x"), 0o600))
		_, err := a.RestoreRepos(ctx, cp.Name(), other)
		require.ErrorContains(t, err, "is not empty")
		_, err = os.Stat(filepath.Join(target, "precious"))
		require.NoError(t, err, "never removed")
	})
}

// A repository with its symbolic HEAD alone (created without auto-init, or
// whose only branch was deleted) is restored, empty, with its HEAD: the
// forge would otherwise find its row and no directory (#114).
func TestHuntR8ProveEmptyRepoRestored(t *testing.T) {
	ctx := context.Background()
	c := newFakeCluster(t)
	o := newTestObserver(t, c, &hookLog{})
	c.mu.Lock()
	require.True(t, c.append(reftable.KindRefTxn, &reftable.RefTxn{Repo: "alice/empty.git", Format: "sha1", Updates: []reftable.RefUpdate{{Ref: "HEAD", Symbolic: "refs/heads/trunk"}}}).OK)
	c.mu.Unlock()
	c.push("bob/gone.git", "refs/heads/main", c.commit("gone"))
	c.push("bob/gone.git", "refs/heads/main", "")
	cp := seed(t, c, o)
	require.NoError(t, o.Step(ctx))
	a, err := OpenArchive(o.cfg.Dir, "")
	require.NoError(t, err)
	m, err := a.Manifest(cp.Name())
	require.NoError(t, err)
	require.Contains(t, m.Repos, "alice/empty.git")
	require.Contains(t, m.Repos, "bob/gone.git")
	require.NoError(t, a.Check(ctx, cp.Name()))

	root := t.TempDir()
	n, err := a.RestoreRepos(ctx, cp.Name(), root)
	require.NoError(t, err)
	assert.Equal(t, 3, n)
	for repo, head := range map[string]string{"alice/empty.git": "refs/heads/trunk", "bob/gone.git": "refs/heads/main"} {
		target := filepath.Join(root, repo)
		out, err := o.store.git.run(ctx, target, nil, "symbolic-ref", "HEAD")
		require.NoError(t, err, repo)
		assert.Equal(t, head, strings.TrimSpace(out), repo)
		out, err = o.store.git.run(ctx, target, nil, "for-each-ref")
		require.NoError(t, err, repo)
		assert.Empty(t, strings.TrimSpace(out), "%s holds no ref", repo)
		_, err = os.Stat(filepath.Join(target, IncompleteMark))
		assert.True(t, os.IsNotExist(err))
	}
	// Resumed, the empty repositories are kept as they are.
	n, err = a.RestoreRepos(ctx, cp.Name(), root)
	require.NoError(t, err)
	assert.Equal(t, 3, n)
}

// A manifest naming a repository key Apply refuses is never written: the
// key would climb out of the root the operator gave (#81). The key climbs
// one level, to a directory of the test.
func TestRestoreReposRefusesAnEscapingKey(t *testing.T) {
	ctx := context.Background()
	c := newFakeCluster(t)
	o := newTestObserver(t, c, &hookLog{})
	cp := seed(t, c, o)
	require.NoError(t, o.Step(ctx))
	a, err := OpenArchive(o.cfg.Dir, "")
	require.NoError(t, err)

	dir := filepath.Join(o.cfg.Dir, "checkpoints", cp.Name())
	path := filepath.Join(dir, "manifest.json")
	m, err := a.store.ReadManifest(cp.Name())
	require.NoError(t, err)
	for _, rm := range m.Repos {
		m.Repos["../escape.git"] = rm
		break
	}
	require.Contains(t, m.Repos, "../escape.git")
	b, err := json.Marshal(m)
	require.NoError(t, err)
	require.NoError(t, os.Chmod(dir, 0o755))
	require.NoError(t, os.Chmod(path, 0o644))
	require.NoError(t, os.WriteFile(path, b, 0o644))

	base := t.TempDir()
	root := filepath.Join(base, "root")
	require.NoError(t, os.Mkdir(root, 0o755))
	_, err = a.RestoreRepos(ctx, cp.Name(), root)
	require.Error(t, err)
	assert.NoDirExists(t, filepath.Join(base, "escape.git"))
}

// A rerun of step 4 removes the temporary files a killed copy left beside
// the stored files it restores, ".<name>.<digits>" as writeStream names
// them, whether the copy was killed before its link (the file absent) or
// after it (the file whole): the restored cluster's leader would list them,
// and its observers refuse an LFS path that is not a digest (#290). It
// removes nothing else: not a file the checkpoint itself holds, not a name
// writeStream does not make, not a directory.
func TestRestoreFilesRerunRemovesOnlyTheTemporaryFilesItLeft(t *testing.T) {
	ctx := t.Context()
	c := newFakeCluster(t)
	o := newTestObserver(t, c, &hookLog{})
	seed(t, c, o)
	// An attachment whose own name has the shape of a temporary file of
	// another one beside it: the checkpoint holds both.
	c.storeAttachment("a1/b2/9f0c2d4e-attachment", "an attachment")
	c.storeAttachment("a1/b2/.9f0c2d4e-attachment.12", "another attachment")
	cp := c.checkpoint()
	require.NoError(t, o.Step(ctx))
	a, err := OpenArchive(o.cfg.Dir, "")
	require.NoError(t, err)
	m, err := a.Manifest(cp.Name())
	require.NoError(t, err)
	var obj string
	for _, f := range m.Files {
		if f.Storage == "lfs" {
			obj = f.Path
		}
	}
	require.NotEmpty(t, obj)

	roots := map[string]string{"lfs": filepath.Join(t.TempDir(), "lfs"), "attachments": filepath.Join(t.TempDir(), "attachments")}
	n, err := a.RestoreFiles(cp.Name(), roots)
	require.NoError(t, err)
	require.Equal(t, len(m.Files), n)

	dst := filepath.Join(roots["lfs"], filepath.FromSlash(obj))
	dir, base := filepath.Dir(dst), filepath.Base(dst)
	write := func(name, body string) {
		require.NoError(t, os.WriteFile(filepath.Join(dir, name), []byte(body), 0o600))
	}
	// Killed after the link: the temporary file is the object's second name.
	require.NoError(t, os.Link(dst, filepath.Join(dir, "."+base+".1260260618")))
	// Killed during an earlier copy: a partial one.
	write("."+base+".984472376", "an lf")
	// Names writeStream does not make, and a directory.
	keep := []string{"." + base + ".tmp", "." + base + ".", base + ".123", ".other.456"}
	for _, k := range keep {
		write(k, "not the restore's")
	}
	require.NoError(t, os.Mkdir(filepath.Join(dir, "."+base+".789"), 0o700))
	keep = append(keep, "."+base+".789")

	synced := map[string]bool{}
	prev := syncPath
	defer func() { syncPath = prev }()
	syncPath = func(p string) error {
		synced[p] = true
		return prev(p)
	}
	n, err = a.RestoreFiles(cp.Name(), roots)
	require.NoError(t, err)
	require.Equal(t, len(m.Files), n)
	entries, err := os.ReadDir(dir)
	require.NoError(t, err)
	var names []string
	for _, e := range entries {
		names = append(names, e.Name())
	}
	assert.ElementsMatch(t, append([]string{base}, keep...), names, "the temporary files are gone, and nothing else")
	assert.True(t, synced[dir], "the removal is durable before the step is recorded")
	for _, f := range m.Files {
		b, err := os.ReadFile(filepath.Join(roots[f.Storage], filepath.FromSlash(f.Path)))
		require.NoError(t, err, "%s/%s", f.Storage, f.Path)
		assert.Equal(t, c.files[f.Storage+"/"+f.Path], b)
	}

	// Killed before the link: the object is absent, its partial copy left.
	require.NoError(t, os.Remove(dst))
	write("."+base+".42", "an l")
	_, err = a.RestoreFiles(cp.Name(), roots)
	require.NoError(t, err)
	assert.NoFileExists(t, filepath.Join(dir, "."+base+".42"))
	b, err := os.ReadFile(dst)
	require.NoError(t, err)
	assert.Equal(t, c.files["lfs/"+obj], b)
}

// A crash while Materialize wrote the mark under a temporary name, before
// its rename, left the target holding that file alone: no mark, no
// repository, refused as "not empty" by every rerun of step 4 (#291). The
// rerun takes such a target as its own and writes it again; a target that
// holds anything else is still refused, and nothing in it is removed.
func TestRestoreReposTakesUpATargetLeftBeforeItsMark(t *testing.T) {
	ctx := t.Context()
	c := newFakeCluster(t)
	o := newTestObserver(t, c, &hookLog{})
	cp := seed(t, c, o)
	require.NoError(t, o.Step(ctx))
	a, err := OpenArchive(o.cfg.Dir, "")
	require.NoError(t, err)
	m, err := a.Manifest(cp.Name())
	require.NoError(t, err)

	// The state that crash leaves, made by hand: the window is too short
	// to kill a process into.
	leave := func(t *testing.T, root string) (string, string) {
		target := filepath.Join(root, "alice", "demo.git")
		require.NoError(t, os.MkdirAll(target, 0o755))
		tmp, err := os.CreateTemp(target, "."+IncompleteMark+".*")
		require.NoError(t, err)
		require.NoError(t, tmp.Close())
		return target, tmp.Name()
	}

	root := t.TempDir()
	target, tmp := leave(t, root)
	n, err := a.RestoreRepos(ctx, cp.Name(), root)
	require.NoError(t, err, "the rerun of step 4 takes the target up")
	assert.Equal(t, 1, n)
	assert.True(t, o.store.git.restoredRefs(ctx, target, m.Repos["alice/demo.git"].Refs))
	assert.NoFileExists(t, tmp)
	assert.NoFileExists(t, filepath.Join(target, IncompleteMark))

	for what, add := range map[string]func(target string) error{
		"a file of its own": func(target string) error {
			return os.WriteFile(filepath.Join(target, "precious"), []byte("x"), 0o600)
		},
		"a file named otherwise": func(target string) error {
			return os.WriteFile(filepath.Join(target, "."+IncompleteMark+".tmp"), nil, 0o600)
		},
		"a directory named as the temporary file": func(target string) error {
			return os.Mkdir(filepath.Join(target, "."+IncompleteMark+".1"), 0o700)
		},
	} {
		t.Run("beside "+what, func(t *testing.T) {
			other := t.TempDir()
			target, tmp := leave(t, other)
			require.NoError(t, add(target))
			names := func() []string {
				entries, err := os.ReadDir(target)
				require.NoError(t, err)
				var out []string
				for _, e := range entries {
					out = append(out, e.Name())
				}
				return out
			}
			before := names()
			_, err := a.RestoreRepos(ctx, cp.Name(), other)
			require.ErrorContains(t, err, "is not empty")
			assert.Equal(t, before, names(), "nothing is removed")
			assert.FileExists(t, tmp)
		})
	}
}

// Materialize creates the mark under its own name, and syncs it and the
// target before git writes anything there: no crash leaves the target
// holding a temporary name instead of the mark (#291).
func TestMaterializeWritesTheMarkUnderItsOwnName(t *testing.T) {
	ctx := t.Context()
	c := newFakeCluster(t)
	o := newTestObserver(t, c, &hookLog{})
	cp := seed(t, c, o)
	require.NoError(t, o.Step(ctx))
	a, err := OpenArchive(o.cfg.Dir, "")
	require.NoError(t, err)

	root := t.TempDir()
	target := filepath.Join(root, "alice", "demo.git")
	mark := filepath.Join(target, IncompleteMark)
	// What the target holds each time the mark or the target is synced
	// while git has not written there yet.
	seen := map[string][]string{}
	prev := syncPath
	defer func() { syncPath = prev }()
	syncPath = func(p string) error {
		if p == mark || p == target {
			if _, err := os.Stat(filepath.Join(target, "HEAD")); os.IsNotExist(err) {
				entries, err := os.ReadDir(target)
				require.NoError(t, err)
				var names []string
				for _, e := range entries {
					names = append(names, e.Name())
				}
				seen[p] = names
			}
		}
		return prev(p)
	}
	_, err = a.RestoreRepos(ctx, cp.Name(), root)
	require.NoError(t, err)
	assert.Equal(t, []string{IncompleteMark}, seen[mark], "the mark is synced before git init")
	assert.Equal(t, []string{IncompleteMark}, seen[target], "and its name in the target")
}
