// Copyright 2026 The Forgejo Authors. All rights reserved.
// SPDX-License-Identifier: GPL-3.0-or-later

package repository

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"forgejo.org/modules/git"
	"forgejo.org/services/cluster/consensus"
	"forgejo.org/services/cluster/reftable"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// recordTransactions installs, as the cluster's hook is, a reference-
// transaction hook that writes down the lines of each prepared transaction
// in a repository under root, and returns their reader. The hook is the
// only one of its own directory, which every git of the test runs through
// GIT_CONFIG_*: the git config of the git home, which another test of the
// package may point at its own home, is not read.
func recordTransactions(t *testing.T, root string) func() [][]string {
	t.Helper()
	root, err := filepath.EvalSymlinks(root)
	require.NoError(t, err)
	file := filepath.Join(t.TempDir(), "transactions")
	hooks := t.TempDir()
	script := fmt.Sprintf("#!/bin/sh\n[ \"$1\" = prepared ] || exit 0\ncase \"$(pwd -P)\" in '%s'/*) { cat; echo --; } >> '%s' ;; esac\n", root, file)
	require.NoError(t, os.WriteFile(filepath.Join(hooks, "reference-transaction"), []byte(script), 0o755))
	// As a node of a cluster runs git: no automatic maintenance, whose
	// pack-refs would rewrite every ref again, one transaction each.
	t.Setenv("GIT_CONFIG_COUNT", "3")
	t.Setenv("GIT_CONFIG_KEY_0", "gc.auto")
	t.Setenv("GIT_CONFIG_VALUE_0", "0")
	t.Setenv("GIT_CONFIG_KEY_1", "maintenance.auto")
	t.Setenv("GIT_CONFIG_VALUE_1", "false")
	t.Setenv("GIT_CONFIG_KEY_2", "core.hooksPath")
	t.Setenv("GIT_CONFIG_VALUE_2", hooks)
	return func() [][]string {
		b, err := os.ReadFile(file)
		if os.IsNotExist(err) {
			return nil
		}
		require.NoError(t, err)
		_ = os.Remove(file)
		var out [][]string
		var cur []string
		for line := range strings.SplitSeq(strings.TrimSuffix(string(b), "\n"), "\n") {
			if line == "--" {
				if len(cur) > 0 {
					out = append(out, cur)
				}
				cur = nil
				continue
			}
			cur = append(cur, line)
		}
		return out
	}
}

// entryBytes is the size of the entry the cluster's hook proposes for a
// transaction of lines, built as buildTxn builds it, every other field at
// a size a fork or a migration may give it.
func entryBytes(t *testing.T, lines []string) int {
	t.Helper()
	tx := &reftable.RefTxn{
		Repo: "org3/repo5.git", RepoID: 1 << 40, Format: "sha1", Time: 1 << 62,
		Nonce: strings.Repeat("a", 32) + "." + strings.Repeat("b", 64), ReadAt: 1 << 40,
	}
	for _, l := range lines {
		f := strings.SplitN(l, " ", 3)
		require.Len(t, f, 3, "a hook line: %q", l)
		u := reftable.RefUpdate{Ref: f[2], ExpectRev: 1 << 40}
		switch {
		case strings.HasPrefix(f[1], "ref:"):
			u.Symbolic = strings.TrimPrefix(f[1], "ref:")
		case reftable.IsZeroSHA(f[1]):
			u.Delete = true
		default:
			u.New = f[1]
		}
		if !strings.HasPrefix(f[0], "ref:") && !reftable.IsZeroSHA(f[0]) {
			u.Old = f[0]
		}
		tx.Updates = append(tx.Updates, u)
	}
	data, err := reftable.EncodeVersion(reftable.PayloadVersion, reftable.KindRefTxn, tx)
	require.NoError(t, err)
	return len(data)
}

// A fork or a migration of a repository whose ref names need JSON escapes:
// encoding/json writes '&', '<' and '>' on six bytes each, and git admits
// them in a name. A batch of 512 such names makes an entry the engine
// refuses past consensus.MaxEntryBytes, and the fork or the migration
// failed every time (#385). Each batch is cut on its weight as on its
// count.
func TestForkAndMirrorInBatchesWithinTheEntryBound(t *testing.T) {
	const branches = 600
	root := t.TempDir()
	transactions := recordTransactions(t, root)
	src := filepath.Join(t.TempDir(), "src.git")
	gitIn(t, filepath.Dir(src), "", "init", "-q", src)
	gitIn(t, src, "", "commit", "-q", "--allow-empty", "-m", "one")
	head := gitIn(t, src, "", "rev-parse", "HEAD")
	// A component of a loose ref is a file name: 255 bytes at most.
	amp := strings.Repeat("&", 200)
	var in strings.Builder
	for i := range branches {
		name := fmt.Sprintf("refs/heads/%s/%s/b%05d", amp, amp, i)
		require.NoError(t, reftable.ValidRefName(name))
		fmt.Fprintf(&in, "create %s %s\n", name, head)
	}
	gitIn(t, src, in.String(), "update-ref", "--stdin")

	check := func(what string) {
		t.Helper()
		txs := transactions()
		require.NotEmpty(t, txs)
		largest := 0
		for i, tx := range txs {
			size := entryBytes(t, tx)
			largest = max(largest, size)
			assert.LessOrEqual(t, size, consensus.MaxEntryBytes, "%s: transaction %d of %d refs", what, i, len(tx))
		}
		t.Logf("%s: %d transactions, the largest entry %d bytes, the bound is %d", what, len(txs), largest, consensus.MaxEntryBytes)
	}
	refsOf := func(dir string) int {
		return len(strings.Fields(gitIn(t, dir, "", "for-each-ref", "--format=%(refname)")))
	}
	ctx := t.Context()

	fork := filepath.Join(root, "fork.git")
	require.NoError(t, copyForkInCluster(ctx, ForkRepoOptions{}, src, fork, "main", ""))
	assert.Equal(t, branches+1, refsOf(fork))
	check("fork")

	mirror := filepath.Join(root, "mirror.git")
	require.NoError(t, cloneMirrorInCluster(ctx, src, mirror, git.CloneRepoOptions{Mirror: true, Quiet: true, Timeout: time.Minute}))
	assert.Equal(t, refsOf(src), refsOf(mirror))
	check("migration")
}
