// Copyright 2026 The Forgejo Authors. All rights reserved.
// SPDX-License-Identifier: GPL-3.0-or-later

//go:build !windows

package observer

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"io/fs"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// killPreparedDeletion plays dropRefs's transaction on the refs of
// checkpoint in the store at dir, and kills git with SIGKILL once it is
// prepared, its locks taken: an OOM kill, a power loss, or gitStore.run's
// WaitDelay expired. It returns the locks git left, relative to dir.
func killPreparedDeletion(t *testing.T, g gitStore, dir, checkpoint string) []string {
	t.Helper()
	refs, err := g.refs(context.Background(), dir, checkpoint)
	require.NoError(t, err)
	require.NotEmpty(t, refs)
	var in strings.Builder
	in.WriteString("start\n")
	for name, sha := range refs {
		fmt.Fprintf(&in, "delete %s%s %s\n", RefNamespace(checkpoint), name, sha)
	}
	in.WriteString("prepare\n")
	cmd := exec.Command(g.git, "-c", "core.hooksPath=/dev/null", "update-ref", "--stdin")
	cmd.Dir = dir
	stdin, err := cmd.StdinPipe()
	require.NoError(t, err)
	stdout, err := cmd.StdoutPipe()
	require.NoError(t, err)
	require.NoError(t, cmd.Start())
	_, err = io.WriteString(stdin, in.String())
	require.NoError(t, err)
	br := bufio.NewReader(stdout)
	for {
		line, err := br.ReadString('\n')
		require.NoError(t, err)
		if strings.HasPrefix(line, "prepare:") {
			break
		}
	}
	require.NoError(t, cmd.Process.Kill())
	_ = cmd.Wait()
	var locks []string
	require.NoError(t, filepath.WalkDir(dir, func(p string, d fs.DirEntry, err error) error {
		if err == nil && !d.IsDir() && strings.HasSuffix(p, ".lock") {
			rel, _ := filepath.Rel(dir, p)
			locks = append(locks, rel)
		}
		return err
	}))
	return locks
}

// prune drops the refs of a checkpoint it lets go through dropRefs, one
// `git update-ref --stdin` transaction of deletions per store. git takes
// every lock of that transaction at prepare, each ref's <ref>.lock and,
// for a deletion, packed-refs.lock, and removes them at commit, or on a
// SIGTERM. A git killed in between (SIGKILL: an OOM kill, a power loss,
// the WaitDelay of gitStore.run expired) left them in the store, which
// createRefs takes up for its own (#85) and dropRefs did not: the prune
// of that checkpoint failed at every pass, the loop never reached a later
// one, pruneData never ran, and Step returned before renewBase, the
// observer's disk growing for good (#387).
func TestPruneTakesUpWhatAKilledDeletionLeft(t *testing.T) {
	ctx := context.Background()
	c := newFakeCluster(t)
	o := newTestObserver(t, c, &hookLog{})
	seed(t, c, o)
	require.NoError(t, o.Step(ctx))
	first := o.State().Checkpoints[0].Name
	dir := o.store.git.dirFor("alice/demo.git", "sha1")
	locks := killPreparedDeletion(t, o.store.git, dir, first)
	t.Logf("left by the killed git: %v", locks)
	require.Contains(t, locks, "packed-refs.lock")
	require.Greater(t, len(locks), 1, "a ref's lock beside packed-refs.lock")

	// Passes with new checkpoints, each one past LiveKeep at once, and a
	// base renewal due at each pass (BaseEvery past).
	o.cfg.LiveKeep = 1
	o.cfg.BaseEvery = 1
	labels := func() (out []string) {
		for _, b := range o.State().Bases {
			out = append(out, b.Label)
		}
		return out
	}
	before := labels()
	for i := range 3 {
		c.push("alice/demo.git", "refs/heads/main", c.commit(fmt.Sprintf("after the kill %d", i)))
		c.checkpoint()
		err := o.Step(ctx)
		t.Logf("pass %d: %v", i, err)
		require.NoError(t, err, "pass %d", i)
	}
	st := o.State()
	var states []string
	for _, a := range st.Checkpoints {
		states = append(states, a.Name[len(a.Name)-4:]+"="+a.State)
	}
	t.Logf("after 3 passes: %d accepted: %s", len(st.Checkpoints), strings.Join(states, " "))
	t.Logf("bases before: %v, after: %v", before, labels())
	assert.Equal(t, StatePruned, st.Checkpoints[0].State, "the checkpoint whose prune a killed git interrupted is let go")
	assert.NotEqual(t, before, labels(), "the base renewal due at each pass ran (#179)")
	refs, err := o.store.git.refs(ctx, dir, first)
	require.NoError(t, err)
	assert.Empty(t, refs, "its refs are gone")
	assert.NoFileExists(t, filepath.Join(dir, "packed-refs.lock"))
	requireRestorable(t, o)
}

// dropRefs takes up the locks under the namespace of the checkpoint it
// drops and packed-refs.lock, never a lock under another checkpoint's,
// nor a file outside the store.
func TestDropRefsTakesUpItsOwnLocksOnly(t *testing.T) {
	ctx := context.Background()
	c := newFakeCluster(t)
	g := gitStore{root: t.TempDir(), git: "git"}
	dir, err := g.ensure(ctx, "alice/demo.git", "sha1")
	require.NoError(t, err)
	one := c.commit("one")
	c.git(dir, "fetch", "-q", c.work, "HEAD")
	const cp, other = "c1050000000000000000000000000000-1-00000000000000000006", "c1050000000000000000000000000000-1-00000000000000000009"
	require.NoError(t, g.createRefs(ctx, dir, cp, map[string]string{"refs/heads/main": one, "refs/heads/dev": one}))
	require.NoError(t, g.createRefs(ctx, dir, other, map[string]string{"refs/heads/main": one}))
	locks := killPreparedDeletion(t, g, dir, cp)
	require.Contains(t, locks, "packed-refs.lock")
	// A lock under another checkpoint's namespace, and a file of the same
	// name beside the store, which nothing here may remove.
	otherLock := filepath.Join(dir, "refs", "observer", other, "refs", "heads", "dev.lock")
	require.NoError(t, os.WriteFile(otherLock, nil, 0o600))
	witness := filepath.Join(filepath.Dir(dir), "packed-refs.lock")
	require.NoError(t, os.WriteFile(witness, nil, 0o600))

	require.NoError(t, g.dropRefs(ctx, dir, cp))
	refs, err := g.refs(ctx, dir, cp)
	require.NoError(t, err)
	assert.Empty(t, refs)
	refs, err = g.refs(ctx, dir, other)
	require.NoError(t, err)
	assert.Equal(t, map[string]string{"refs/heads/main": one}, refs, "another checkpoint's refs stay")
	assert.FileExists(t, otherLock, "another checkpoint's lock stays")
	assert.FileExists(t, witness, "nothing outside the store is removed")
	assert.NoFileExists(t, filepath.Join(dir, "packed-refs.lock"))
	var left []string
	_ = filepath.WalkDir(filepath.Join(dir, "refs", "observer", cp), func(p string, d fs.DirEntry, err error) error {
		if err == nil && strings.HasSuffix(p, ".lock") {
			left = append(left, p)
		}
		return nil
	})
	assert.Empty(t, left, "the checkpoint's locks are gone")
}
