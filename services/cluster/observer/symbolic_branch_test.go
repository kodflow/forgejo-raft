// Copyright 2026 The Forgejo Authors. All rights reserved.
// SPDX-License-Identifier: GPL-3.0-or-later

package observer

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"forgejo.org/services/cluster/reftable"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// seedWithSymbolic is seed, plus, before the checkpoint the observer
// ingests, the symbolic refs of alice/demo.git in symbolic, name to target:
// what `forgejo cluster import` takes from a repository holding such an
// alias, or what `git symbolic-ref refs/heads/master refs/heads/main` run in
// a member's repository proposes through the reference-transaction hook (a
// "ref:" line). Apply accepts it, refusing only an invalid target or HEAD.
func seedWithSymbolic(t *testing.T, c *fakeCluster, o *Observer, symbolic [][2]string) reftable.CheckpointRecord {
	t.Helper()
	c.push("alice/demo.git", "refs/heads/main", c.commit("one"))
	c.storeFile("an lfs object")
	c.checkpoint()
	require.ErrorIs(t, o.Step(context.Background()), ErrNoCoverage)
	c.push("alice/demo.git", "refs/heads/dev", c.commit("two"))
	for _, s := range symbolic {
		c.mu.Lock()
		res := c.append(reftable.KindRefTxn, &reftable.RefTxn{Repo: "alice/demo.git", Format: "sha1", Updates: []reftable.RefUpdate{{Ref: s[0], Symbolic: s[1]}}})
		c.mu.Unlock()
		require.True(t, res.OK, res.Reason)
		// The members' repositories hold it too, as the projector writes it.
		c.git(filepath.Join(c.repos, "alice", "demo.git"), "symbolic-ref", s[0], s[1])
	}
	return c.checkpoint()
}

// Step 4 of recover --from-observer resumed after a stop (an error of
// RestoreFiles, a kill after the repositories, an SSH session lost) runs
// RestoreRepos again, which keeps a repository an earlier run wrote whole:
// the checkpoint's refs exactly, each naming an object it holds. The
// symbolic refs of the checkpoint are in it too, Materialize writes them:
// an alias left by the rename of a default branch, refs/heads/master ->
// refs/heads/main, made the restored repository one ref more than the
// checkpoint's refs, taken for one this restore did not write, and refused
// as not empty at every rerun (#338). Each case restores a checkpoint
// whole, then runs the step again, which keeps it.
func TestRestoreReposKeepsARestoredRepositoryWithSymbolicBranches(t *testing.T) {
	for _, tc := range []struct {
		name     string
		symbolic [][2]string
	}{
		{"without a symbolic branch", nil},
		{"with a symbolic branch", [][2]string{{"refs/heads/master", "refs/heads/main"}}},
		{"with a chain of symbolic branches", [][2]string{{"refs/heads/master", "refs/heads/main"}, {"refs/heads/trunk", "refs/heads/master"}}},
		// Its target deleted: git lists no such ref, and holds it all the
		// same.
		{"with a dangling symbolic branch", [][2]string{{"refs/heads/old", "refs/heads/gone"}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := t.Context()
			c := newFakeCluster(t)
			o := newTestObserver(t, c, &hookLog{})
			cp := seedWithSymbolic(t, c, o, tc.symbolic)
			require.NoError(t, o.Step(ctx))
			a, err := OpenArchive(o.cfg.Dir, "")
			require.NoError(t, err)
			m, err := a.Manifest(cp.Name())
			require.NoError(t, err)
			rm := m.Repos["alice/demo.git"]
			t.Logf("manifest of alice/demo.git: refs=%v symbolic=%v", rm.Refs, rm.Symbolic)
			require.Len(t, rm.Symbolic, len(tc.symbolic)+1, "the symbolic refs and HEAD")
			require.NoError(t, a.Check(ctx, cp.Name()), "the checkpoint is listed as restorable")

			root := t.TempDir()
			n, err := a.RestoreRepos(ctx, cp.Name(), root)
			require.NoError(t, err, "first run")
			require.Equal(t, 1, n)
			target := filepath.Join(root, "alice", "demo.git")
			out, err := o.store.git.run(ctx, target, nil, "for-each-ref", "--format=%(refname) %(symref)")
			require.NoError(t, err)
			t.Logf("restored refs:\n%s", strings.TrimSpace(out))
			for _, s := range tc.symbolic {
				to, err := o.store.git.run(ctx, target, nil, "symbolic-ref", "--no-recurse", s[0])
				require.NoError(t, err)
				assert.Equal(t, s[1], strings.TrimSpace(to), "%s is restored symbolic", s[0])
			}
			_, err = os.Stat(filepath.Join(target, IncompleteMark))
			require.True(t, os.IsNotExist(err), "whole and unmarked after the first run")
			assert.True(t, o.store.git.restoredRefs(ctx, target, rm), "the checkpoint's refs, its symbolic ones included")

			// The resumed run, as after RestoreFiles failed.
			n, err = a.RestoreRepos(ctx, cp.Name(), root)
			require.NoError(t, err, "a resumed step 4 keeps the repository it wrote whole")
			assert.Equal(t, 1, n)
		})
	}
}

// A repository holding the checkpoint's refs is still not one this
// restore wrote unless its symbolic refs are the checkpoint's too, each
// naming the ref the checkpoint names: one a branch of its own at the same
// commit, naming another ref, missing, or HEAD naming another branch, is
// refused as not empty, and nothing in it is changed. A dangling symbolic
// ref and a link of a chain, which for-each-ref does not tell apart, are
// read on their own.
func TestRestoreReposRefusesARepositoryWithOtherSymbolicRefs(t *testing.T) {
	alias := [][2]string{{"refs/heads/master", "refs/heads/main"}}
	for _, tc := range []struct {
		name     string
		symbolic [][2]string
		change   [][]string
	}{
		{"the alias a branch of its own", alias, [][]string{{"update-ref", "--no-deref", "refs/heads/master", "refs/heads/main"}}},
		{"the alias naming another branch", alias, [][]string{{"symbolic-ref", "refs/heads/master", "refs/heads/dev"}}},
		{"the alias missing", alias, [][]string{{"symbolic-ref", "--delete", "refs/heads/master"}}},
		{"an alias the checkpoint does not hold", nil, [][]string{{"symbolic-ref", "refs/heads/master", "refs/heads/main"}}},
		{"HEAD naming another branch", alias, [][]string{{"symbolic-ref", "HEAD", "refs/heads/dev"}}},
		{"a dangling alias missing", [][2]string{{"refs/heads/old", "refs/heads/gone"}}, [][]string{{"symbolic-ref", "--delete", "refs/heads/old"}}},
		{"a dangling alias naming another ref", [][2]string{{"refs/heads/old", "refs/heads/gone"}}, [][]string{{"symbolic-ref", "refs/heads/old", "refs/heads/elsewhere"}}},
		{
			"a link of a chain naming its end",
			[][2]string{{"refs/heads/master", "refs/heads/main"}, {"refs/heads/trunk", "refs/heads/master"}},
			[][]string{{"symbolic-ref", "refs/heads/trunk", "refs/heads/main"}},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := t.Context()
			c := newFakeCluster(t)
			o := newTestObserver(t, c, &hookLog{})
			cp := seedWithSymbolic(t, c, o, tc.symbolic)
			require.NoError(t, o.Step(ctx))
			a, err := OpenArchive(o.cfg.Dir, "")
			require.NoError(t, err)
			m, err := a.Manifest(cp.Name())
			require.NoError(t, err)
			rm := m.Repos["alice/demo.git"]

			root := t.TempDir()
			_, err = a.RestoreRepos(ctx, cp.Name(), root)
			require.NoError(t, err)
			target := filepath.Join(root, "alice", "demo.git")
			require.True(t, o.store.git.restoredRefs(ctx, target, rm), "whole after the first run")
			for _, args := range tc.change {
				_, err := o.store.git.run(ctx, target, nil, args...)
				require.NoError(t, err)
			}
			refs := func() string {
				out, err := o.store.git.run(ctx, target, nil, "for-each-ref", "--format=%(objectname) %(refname) %(symref)")
				require.NoError(t, err)
				head, err := o.store.git.run(ctx, target, nil, "symbolic-ref", "HEAD")
				require.NoError(t, err)
				return out + "HEAD " + head
			}
			before := refs()
			assert.False(t, o.store.git.restoredRefs(ctx, target, rm))

			_, err = a.RestoreRepos(ctx, cp.Name(), root)
			require.ErrorContains(t, err, "is not empty")
			assert.Equal(t, before, refs(), "nothing is changed")
		})
	}
}
