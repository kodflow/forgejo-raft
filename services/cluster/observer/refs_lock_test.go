// Copyright 2026 The Forgejo Authors. All rights reserved.
// SPDX-License-Identifier: GPL-3.0-or-later

//go:build !windows

package observer

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// gitWrapper writes a git that runs body for `update-ref`, and the real git
// for anything else.
func gitWrapper(t *testing.T, body string) string {
	t.Helper()
	real, err := exec.LookPath("git")
	require.NoError(t, err)
	path := filepath.Join(t.TempDir(), "git")
	script := fmt.Sprintf("#!/bin/sh\nfor a; do\n  if [ \"$a\" = update-ref ]; then\n%s\n  fi\ndone\nexec %q \"$@\"\n", body, real)
	require.NoError(t, os.WriteFile(path, []byte(script), 0o755))
	return path
}

// A pass stopped while git creates a checkpoint's refs ends with the stop,
// never with a quarantine for a fork, and the next pass ingests the same
// checkpoint (#85, a service stop or the step timeout).
func TestStopDuringCreateRefsIsNoFork(t *testing.T) {
	c := newFakeCluster(t)
	o := newTestObserver(t, c, &hookLog{})
	seed(t, c, o)
	mark := filepath.Join(t.TempDir(), "in-update-ref")
	real := o.store.git.git
	o.store.git.git = gitWrapper(t, fmt.Sprintf("    : > %q\n    exec sleep 30", mark))
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() {
		for {
			if _, err := os.Stat(mark); err == nil {
				cancel()
				return
			}
			time.Sleep(10 * time.Millisecond)
		}
	}()
	err := o.Step(ctx)
	require.Error(t, err)
	assert.NotErrorIs(t, err, ErrQuarantined)
	assert.NotErrorIs(t, err, ErrImmutable)
	require.Nil(t, o.State().Quarantine)
	o.store.git.git = real
	require.NoError(t, o.Step(context.Background()))
	assert.Len(t, o.State().Checkpoints, 1)
}

// An update-ref that fails for another reason than refs held with other
// values (a full disk, an I/O error) is an ordinary error, retried by the
// next pass (#85).
func TestCreateRefsFailureIsNotImmutable(t *testing.T) {
	c := newFakeCluster(t)
	o := newTestObserver(t, c, &hookLog{})
	seed(t, c, o)
	real := o.store.git.git
	o.store.git.git = gitWrapper(t, "    echo 'fatal: No space left on device' >&2\n    exit 128")
	err := o.Step(context.Background())
	require.Error(t, err)
	assert.NotErrorIs(t, err, ErrImmutable)
	assert.NotErrorIs(t, err, ErrQuarantined)
	o.store.git.git = real
	require.NoError(t, o.Step(context.Background()))
	assert.Len(t, o.State().Checkpoints, 1)
}

// A lock a killed git left under a checkpoint's namespace, and refs a
// transaction killed in its commit left behind, are taken up by the retry
// (#85).
func TestCreateRefsTakesUpWhatAKilledGitLeft(t *testing.T) {
	ctx := context.Background()
	c := newFakeCluster(t)
	g := gitStore{root: t.TempDir(), git: "git"}
	dir, err := g.ensure(ctx, "alice/demo.git", "sha1")
	require.NoError(t, err)
	one, two := c.commit("one"), c.commit("two")
	c.git(dir, "fetch", "-q", c.work, "HEAD")
	const cp = "c1050000000000000000000000000000-1-00000000000000000006"
	ns := filepath.Join(dir, "refs", "observer", cp, "refs", "heads")
	require.NoError(t, os.MkdirAll(ns, 0o700))
	require.NoError(t, os.WriteFile(filepath.Join(ns, "dev.lock"), []byte(two+"\n"), 0o600))
	// One of the transaction's refs made it before the kill.
	c.git(dir, "update-ref", RefNamespace(cp)+"refs/heads/main", one)
	refs := map[string]string{"refs/heads/main": one, "refs/heads/dev": two}
	require.NoError(t, g.createRefs(ctx, dir, cp, refs))
	have, err := g.refs(ctx, dir, cp)
	require.NoError(t, err)
	assert.Equal(t, refs, have)
	// Another value for a ref already there is still a fork.
	require.ErrorIs(t, g.createRefs(ctx, dir, cp, map[string]string{"refs/heads/main": two, "refs/heads/dev": two}), ErrImmutable)
}
