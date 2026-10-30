// Copyright 2026 The Forgejo Authors. All rights reserved.
// SPDX-License-Identifier: GPL-3.0-or-later

package cluster

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

// A dangling alias the table deletes leaves the git of every node (#359).
// The alias refs/heads/master -> refs/heads/main (#338, #348) dangles once
// main is renamed to trunk on another node; an operator then removes it
// there (`git symbolic-ref -d refs/heads/master`, through the hook: "0000
// 0000 refs/heads/master"), and the table tombstones it. for-each-ref lists
// a dangling symbolic ref nowhere, and the projector read one only under
// the names the table holds live: node N never deleted it, and certified
// the repository while its git still held it. receive-pack follows it
// there: a push creating a new branch master landed on main, which came
// back on every node. And a branch master/x never reached N's git, which
// stopped certifying the repository. Both of git's ref backends: the files
// one keeps a file per ref, the reftable one none.
func TestTombstonedDanglingAliasLeavesEveryNode(t *testing.T) {
	for _, format := range []string{"files", "reftable"} {
		t.Run(format, func(t *testing.T) { testTombstonedDanglingAlias(t, format) })
	}
}

func testTombstonedDanglingAlias(t *testing.T, format string) {
	ctx := context.Background()
	// start brings node N's git and the table to the alias master -> main,
	// dangling once main is renamed when dangleFirst, then deletes the
	// alias in the table, as another node does, and projects it on N.
	start := func(t *testing.T, dangleFirst bool) (*symbolicBench, string) {
		b := newSymbolicBenchOf(t, format)
		c1 := b.commit("one")
		run(t, b.dir, "update-ref", "refs/heads/main", c1)
		run(t, b.dir, "symbolic-ref", "HEAD", "refs/heads/main")
		run(t, b.dir, "symbolic-ref", "refs/heads/master", "refs/heads/main")
		b.apply(reftable.RefUpdate{Ref: "HEAD", Symbolic: "refs/heads/main"}, reftable.RefUpdate{Ref: "refs/heads/main", New: c1})
		b.apply(reftable.RefUpdate{Ref: "refs/heads/master", Symbolic: "refs/heads/main"})
		certified, failure := b.project()
		require.True(t, certified, "certified with the alias: %s", failure)
		if dangleFirst {
			// Another node renames the default branch main to trunk.
			b.apply(reftable.RefUpdate{Ref: "refs/heads/trunk", New: c1}, reftable.RefUpdate{Ref: "refs/heads/main", Delete: true, Old: c1})
			b.apply(reftable.RefUpdate{Ref: "HEAD", Symbolic: "refs/heads/trunk"})
			certified, failure = b.project()
			require.True(t, certified, "the dangling alias is certified (#348): %s", failure)
			require.Equal(t, "ref:refs/heads/main", b.git("refs/heads/master"), "N holds the dangling alias")
		}
		// Another node: git symbolic-ref -d refs/heads/master.
		b.apply(reftable.RefUpdate{Ref: "refs/heads/master", Delete: true})
		certified, failure = b.project()
		require.True(t, certified, "certified once the alias is deleted (projected %d, applied %d): %s", b.p.Projected(symbolicRepo), b.idx, failure)
		return b, c1
	}

	t.Run("the alias is deleted while its target lives", func(t *testing.T) {
		b, c1 := start(t, false)
		assert.Empty(t, b.git("refs/heads/master"), "N deleted the alias")
		assert.Equal(t, c1, b.git("refs/heads/main"), "never the branch it names")
	})

	t.Run("a dangling alias is deleted", func(t *testing.T) {
		b, _ := start(t, true)
		assert.Empty(t, b.git("refs/heads/master"), "N deleted the alias the table deleted")
		same, err := b.p.Verify(ctx, symbolicRepo)
		require.NoError(t, err)
		assert.True(t, same, "Verify finds git as the table holds it")

		// A node whose git keeps the alias, as N did before #359, differs
		// from the table: Verify says so, and the projection deletes it.
		run(t, b.dir, "symbolic-ref", "refs/heads/master", "refs/heads/main")
		same, err = b.p.Verify(ctx, symbolicRepo)
		require.NoError(t, err)
		assert.False(t, same, "Verify sees the alias the table deleted")
		certified, failure := b.project()
		assert.True(t, certified, failure)
		assert.Empty(t, b.git("refs/heads/master"), "the projection deletes it again")
	})

	t.Run("a push of a new branch master on N", func(t *testing.T) {
		b, _ := start(t, true)
		c2 := commit(t, b.work, "two")
		// receive-pack on N, with a reference-transaction hook that records
		// what the cluster's hook reads (cmd/hook_cluster.go).
		hooks := t.TempDir()
		log := filepath.Join(hooks, "lines")
		require.NoError(t, os.WriteFile(filepath.Join(hooks, "reference-transaction"),
			[]byte("#!/bin/sh\nwhile read o n r; do echo \"$1 $o $n $r\" >> '"+log+"'; done\n"), 0o755))
		out := run(t, b.work, "push", "--receive-pack=git -c core.hooksPath="+hooks+" receive-pack", b.dir, c2+":refs/heads/master")
		t.Logf("git push N %s:refs/heads/master: %s", c2[:12], out)
		lines, err := os.ReadFile(log)
		require.NoError(t, err)
		// buildTxn proposes the prepared lines; Apply takes them.
		var updates []reftable.RefUpdate
		for line := range strings.SplitSeq(strings.TrimSpace(string(lines)), "\n") {
			t.Logf("hook: %s", line)
			f := strings.Fields(line)
			if len(f) != 4 || f[0] != "prepared" {
				continue
			}
			u := reftable.RefUpdate{Ref: f[3]}
			if reftable.IsZeroSHA(f[2]) {
				u.Delete = true
			} else {
				u.New = f[2]
			}
			if !reftable.IsZeroSHA(f[1]) {
				u.Old = f[1]
			}
			updates = append(updates, u)
		}
		require.NotEmpty(t, updates)
		b.apply(updates...)
		certified, failure := b.project()
		assert.True(t, certified, failure)
		master, _ := b.tbl.Get(symbolicRepo, "refs/heads/master")
		main, _ := b.tbl.Get(symbolicRepo, "refs/heads/main")
		assert.True(t, master.Live() && master.SHA == c2, "the cluster holds the branch master the push created and acknowledged")
		assert.False(t, main.Live(), "main, renamed away, stays away")
		assert.Equal(t, c2, b.git("refs/heads/master"))
		assert.Empty(t, b.git("refs/heads/main"))
	})

	t.Run("a branch master/x created on another node", func(t *testing.T) {
		b, c1 := start(t, true)
		b.apply(reftable.RefUpdate{Ref: "refs/heads/master/x", New: c1})
		certified, failure := b.project()
		assert.True(t, certified, "N projects master/x (projected %d, applied %d): %s", b.p.Projected(symbolicRepo), b.idx, failure)
		assert.Equal(t, c1, b.git("refs/heads/master/x"))
	})
}
