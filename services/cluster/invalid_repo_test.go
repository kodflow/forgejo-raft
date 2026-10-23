// Copyright 2026 The Forgejo Authors. All rights reserved.
// SPDX-License-Identifier: GPL-3.0-or-later

package cluster

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// An init cut at any point leaves either no directory under the key or a
// repository git reads (#42).
func TestAnInterruptedInitLeavesNoPartialRepository(t *testing.T) {
	root := t.TempDir()
	for i := range 300 {
		dir := filepath.Join(root, "o", "r.git")
		ctx, cancel := context.WithTimeout(context.Background(), time.Duration(200+i*100)*time.Microsecond)
		_ = initBare(ctx, dir, "sha1")
		cancel()
		if _, err := os.Stat(dir); err == nil {
			require.True(t, validRepoDir(dir), "try %d left a partial repository under the key", i)
			require.NoError(t, os.RemoveAll(dir))
		}
	}
	entries, err := os.ReadDir(filepath.Join(root, "o"))
	require.NoError(t, err)
	assert.Empty(t, entries, "no temporary directory left behind")
}

// A directory under a key that holds no git repository, the trace of an
// init killed before ours was atomic, is set aside and the repository
// projected anew from the members (#42); the node is ready after a start.
func TestAPartialRepositoryDirectoryIsRepaired(t *testing.T) {
	t.Parallel()
	tc := newTestCluster(t, "node-a", "node-b", "node-c")
	work := t.TempDir()
	run(t, work, "init", "-q")
	c1 := commit(t, work, "one")
	zero := strings.Repeat("0", 40)
	origin, lag := tc.nodes[0], tc.nodes[2]
	partial := map[string]func(dir string){
		"a/empty.git": func(string) {},
		"a/nohead.git": func(dir string) {
			for _, d := range []string{"hooks", "info", "refs"} {
				require.NoError(t, os.MkdirAll(filepath.Join(dir, d), 0o755))
			}
			require.NoError(t, os.WriteFile(filepath.Join(dir, "config"), []byte("[core]\n\tbare = true\n"), 0o644))
		},
		"a/zerohead.git": func(dir string) {
			for _, d := range []string{"objects", "refs/heads"} {
				require.NoError(t, os.MkdirAll(filepath.Join(dir, d), 0o755))
			}
			require.NoError(t, os.WriteFile(filepath.Join(dir, "HEAD"), nil, 0o644))
		},
		"a/noobjects.git": func(dir string) {
			require.NoError(t, os.MkdirAll(filepath.Join(dir, "refs", "heads"), 0o755))
			require.NoError(t, os.WriteFile(filepath.Join(dir, "HEAD"), []byte("ref: refs/heads/main\n"), 0o644))
		},
	}
	for repo, prepare := range partial {
		dir := filepath.Join(lag.r.root, filepath.FromSlash(repo))
		require.NoError(t, os.MkdirAll(dir, 0o755))
		prepare(dir)
		require.False(t, validRepoDir(dir), repo)
	}
	// Left by an init killed long ago: a directory changed lately may be
	// a git init still running, which the projector leaves alone.
	old := time.Now().Add(-time.Hour)
	for repo := range partial {
		require.NoError(t, os.Chtimes(filepath.Join(lag.r.root, filepath.FromSlash(repo)), old, old))
	}
	for repo := range partial {
		resp := tc.pushVia(origin, work, repo, "refs/heads/main", zero, c1)
		require.True(t, resp.OK, "%s: %s", repo, resp.Message)
	}
	for repo := range partial {
		require.Eventually(t, func() bool { return gitRef(t, lag, repo, "refs/heads/main") == c1 }, 15*time.Second, 20*time.Millisecond, "%s repaired: %+v", repo, lag.r.projector.Lag())
	}
	lag.r.projector.unfence()
	require.Eventually(t, lag.r.projector.Ready, 10*time.Second, 20*time.Millisecond, "ready after a start")
	entries, err := os.ReadDir(filepath.Join(lag.r.root, staleDirName))
	require.NoError(t, err)
	assert.Len(t, entries, 4, "the partial directories are set aside, never removed")
}

// Forgejo's own git init writes HEAD through the hook before it creates
// objects/: a directory that holds no repository yet, changed lately, is
// not set aside (#42).
func TestADirectoryUnderCreationIsLeftAlone(t *testing.T) {
	tc := newTestCluster(t, "node-a")
	n := tc.nodes[0]
	dir := filepath.Join(n.r.root, "o", "new.git")
	require.NoError(t, os.MkdirAll(filepath.Join(dir, "refs", "heads"), 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(dir, "HEAD"), []byte("ref: refs/heads/main\n"), 0o644))
	require.NoError(t, n.r.projector.setAsideInvalid("o/new.git", dir, 1))
	assert.DirExists(t, dir)
	old := time.Now().Add(-time.Hour)
	require.NoError(t, os.Chtimes(dir, old, old))
	require.NoError(t, n.r.projector.setAsideInvalid("o/new.git", dir, 1))
	assert.NoDirExists(t, dir)
}

// A directory whose validation fails for another reason than a missing or
// malformed entry (permission denied here, an I/O error elsewhere) may
// hold a sound repository: it is left in place, never set aside, and the
// projection reports the error (CodeRabbit on #122).
func TestADirectoryThatCannotBeValidatedIsLeftAlone(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("root reads a directory without the search permission")
	}
	tc := newTestCluster(t, "node-a")
	n := tc.nodes[0]
	dir := filepath.Join(n.r.root, "o", "locked.git")
	for _, d := range []string{"objects", "refs"} {
		require.NoError(t, os.MkdirAll(filepath.Join(dir, d), 0o755))
	}
	require.NoError(t, os.WriteFile(filepath.Join(dir, "HEAD"), []byte("ref: refs/heads/main\n"), 0o644))
	old := time.Now().Add(-time.Hour)
	require.NoError(t, os.Chtimes(dir, old, old))
	require.NoError(t, os.Chmod(dir, 0o200)) // writable, but nothing in it can be looked up
	t.Cleanup(func() { _ = os.Chmod(dir, 0o755) })
	require.Error(t, n.r.projector.setAsideInvalid("o/locked.git", dir, 1))
	require.NoError(t, os.Chmod(dir, 0o755))
	assert.DirExists(t, dir)
	assert.True(t, validRepoDir(dir))
}
