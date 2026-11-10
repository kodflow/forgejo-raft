// Copyright 2026 The Forgejo Authors. All rights reserved.
// SPDX-License-Identifier: GPL-3.0-or-later

package mirror

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"forgejo.org/modules/clusterhook"
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
// a size a mirror's sync may give it.
func entryBytes(t *testing.T, lines []string) int {
	t.Helper()
	tx := &reftable.RefTxn{
		Repo: "org3/repo5.wiki.git", RepoID: 1 << 40, Format: "sha1", Time: 1 << 62, IsWiki: true,
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

// nestedTransaction reports whether lines are the transaction git's files
// backend runs on packed-refs when it deletes a packed ref, every line with
// two null ids, which the cluster does not propose (packedRefsTxn).
func nestedTransaction(lines []string) bool {
	for _, l := range lines {
		f := strings.SplitN(l, " ", 3)
		if len(f) != 3 || !reftable.IsZeroSHA(f[0]) || !reftable.IsZeroSHA(f[1]) {
			return false
		}
	}
	return len(lines) > 0
}

// A sync of a pull mirror, or of its wiki, of fewer refs than the cluster
// takes in one transaction, with long names: git writes the whole fetch in
// one transaction, whose entry the engine refuses past
// consensus.MaxEntryBytes, and every later sync fetched the same refs and
// was refused the same way, the mirror frozen for good (#385). encoding/
// json writes '&', '<' and '>' on six bytes each, which git admits in a
// name: past MaxUpdatesPerTxn refs, where the sync already went by
// batches, a batch of syncRefBatch such names passed the bound too. The
// sync goes by batches once its entry would pass the bound, each batch
// cut on its weight as on its count, its pruned refs still first.
func TestMirrorSyncInBatchesWithinTheEntryBound(t *testing.T) {
	long := strings.Repeat(strings.Repeat("x", 100)+"/", 9)
	for _, c := range []struct {
		name string
		wiki bool
		n    int
		ref  func(i int) string
	}{
		{name: "long names", n: 1200, ref: func(i int) string { return fmt.Sprintf("refs/heads/%sb%05d", long, i) }},
		// A component of a loose ref is a file name: 255 bytes at most.
		{name: "escaped names", n: 600, ref: func(i int) string { return fmt.Sprintf("refs/heads/%[1]s/%[1]s/b%05[2]d", strings.Repeat("&", 200), i) }},
		// Past MaxUpdatesPerTxn, which the batches always took: a batch of
		// syncRefBatch such names passed the bound.
		{name: "escaped names past the count", n: reftable.MaxUpdatesPerTxn + 100, ref: func(i int) string { return fmt.Sprintf("refs/heads/%[1]s/%[1]s/b%05[2]d", strings.Repeat("&", 200), i) }},
		{name: "wiki", wiki: true, n: 1200, ref: func(i int) string { return fmt.Sprintf("refs/heads/%sb%05d", long, i) }},
	} {
		t.Run(c.name, func(t *testing.T) {
			defer clusterhook.SetEnabled(clusterhook.Enabled())
			clusterhook.SetEnabled(true)

			root := t.TempDir()
			transactions := recordTransactions(t, root)
			src := filepath.Join(t.TempDir(), "src.git")
			gitIn(t, filepath.Dir(src), "", "init", "-q", src)
			gitIn(t, src, "", "commit", "-q", "--allow-empty", "-m", "one")
			gitIn(t, src, "", "branch", "gone")
			mirror := filepath.Join(root, "mirror.git")
			gitIn(t, root, "", "clone", "-q", "--mirror", src, mirror)

			gitIn(t, src, "", "commit", "-q", "--allow-empty", "-m", "two")
			head := gitIn(t, src, "", "rev-parse", "HEAD")
			var in strings.Builder
			var updates []string
			for i := range c.n {
				name := c.ref(i)
				require.NoError(t, reftable.ValidRefName(name))
				fmt.Fprintf(&in, "create %s %s\n", name, head)
				updates = append(updates, fmt.Sprintf("%s %s %s", strings.Repeat("0", 40), head, name))
			}
			gitIn(t, src, in.String(), "update-ref", "--stdin")
			gitIn(t, src, "", "branch", "-D", "gone")
			whole := entryBytes(t, updates)
			t.Logf("the sync writes %d refs; their entry in one transaction weighs %d bytes, the bound is %d", c.n+2, whole, consensus.MaxEntryBytes)
			require.Greater(t, whole, consensus.MaxEntryBytes)

			// The wiki's sync prunes and fetches no tags.
			dry := func() *git.Command {
				cmd := git.NewCommand(t.Context(), "fetch", "--dry-run", "--porcelain", "--prune")
				if !c.wiki {
					cmd.AddArguments("--tags")
				}
				return cmd.AddDynamicArguments("origin")
			}
			out, batched, err := fetchInBatches(t.Context(), dry(), mirror, time.Minute, nil, nil)
			require.NoError(t, err)
			require.True(t, batched, "a sync whose entry passes the bound goes by batches")

			written, largest := 0, 0
			prunedFirst := true
			for i, tx := range transactions() {
				if len(tx) == 0 || nestedTransaction(tx) {
					continue
				}
				size := entryBytes(t, tx)
				largest = max(largest, size)
				assert.LessOrEqual(t, size, consensus.MaxEntryBytes, "transaction %d of %d refs", i, len(tx))
				for _, l := range tx {
					if strings.HasSuffix(l, " refs/heads/gone") {
						prunedFirst = written == 0
					}
				}
				if len(tx) > 1 || !strings.HasSuffix(tx[0], " HEAD") {
					written += len(tx)
				}
			}
			t.Logf("written in transactions of at most %d bytes", largest)
			assert.Equal(t, c.n+2, written, "every update: the new refs, main and gone")
			assert.True(t, prunedFirst, "the pruned ref comes first")

			assert.Equal(t, head, gitIn(t, mirror, "", "rev-parse", "refs/heads/main"))
			assert.Equal(t, head, gitIn(t, mirror, "", "rev-parse", c.ref(c.n-1)))
			assert.Empty(t, gitIn(t, mirror, "", "for-each-ref", "refs/heads/gone"), "pruned")
			assert.Len(t, parseRemoteUpdateOutput(out, "origin"), c.n+2)

			// Nothing left: the next sync's own fetch has nothing to write.
			out, batched, err = fetchInBatches(t.Context(), dry(), mirror, time.Minute, nil, nil)
			require.NoError(t, err)
			assert.False(t, batched)
			assert.Empty(t, out)
		})
	}
}
