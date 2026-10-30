// Copyright 2026 The Forgejo Authors. All rights reserved.
// SPDX-License-Identifier: GPL-3.0-or-later

package mirror

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"
	"unicode/utf8"

	"forgejo.org/modules/clusterhook"
	"forgejo.org/modules/git"
	"forgejo.org/modules/setting"
	"forgejo.org/services/cluster/reftable"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func gitIn(t *testing.T, dir, stdin string, args ...string) string {
	t.Helper()
	cmd := exec.Command("git", append([]string{"-c", "core.hooksPath=/dev/null", "-c", "user.name=t", "-c", "user.email=t@t", "-c", "init.defaultBranch=main"}, args...)...)
	cmd.Dir = dir
	cmd.Stdin = strings.NewReader(stdin)
	cmd.Env = append(os.Environ(), "HOME="+setting.Git.HomePath)
	out, err := cmd.CombinedOutput()
	require.NoError(t, err, "git %v: %s", args, out)
	return strings.TrimSpace(string(out))
}

// countTransactions installs, as the cluster's hook is, a reference-
// transaction hook that writes down the number of refs of each prepared
// transaction in a repository under root, and returns their reader.
func countTransactions(t *testing.T, root string) func() []int {
	t.Helper()
	root, err := filepath.EvalSymlinks(root)
	require.NoError(t, err)
	counts := filepath.Join(t.TempDir(), "counts")
	hooks := filepath.Join(git.HomeDir(), "hooks")
	require.NoError(t, os.MkdirAll(hooks, 0o755))
	hook := filepath.Join(hooks, "reference-transaction")
	_, err = os.Stat(hook)
	require.True(t, os.IsNotExist(err), "a reference-transaction hook is already installed")
	script := fmt.Sprintf("#!/bin/sh\n[ \"$1\" = prepared ] || exit 0\ncase \"$(pwd -P)\" in '%s'/*) wc -l | tr -d ' ' >> '%s' ;; esac\n", root, counts)
	require.NoError(t, os.WriteFile(hook, []byte(script), 0o755))
	t.Cleanup(func() { _ = os.Remove(hook) })
	return func() []int {
		b, err := os.ReadFile(counts)
		if os.IsNotExist(err) {
			return nil
		}
		require.NoError(t, err)
		var out []int
		for f := range strings.FieldsSeq(string(b)) {
			n, err := strconv.Atoi(f)
			require.NoError(t, err)
			out = append(out, n)
		}
		_ = os.Remove(counts)
		return out
	}
}

// The clone of a mirror comes by batches (#80); a later sync that brings
// more refs than the cluster takes in one transaction comes by batches too,
// pruned refs included, and reports what the fetch would have (#134).
func TestMirrorSyncInBatches(t *testing.T) {
	t.Setenv("GIT_CONFIG_COUNT", "2")
	t.Setenv("GIT_CONFIG_KEY_0", "gc.auto")
	t.Setenv("GIT_CONFIG_VALUE_0", "0")
	t.Setenv("GIT_CONFIG_KEY_1", "maintenance.auto")
	t.Setenv("GIT_CONFIG_VALUE_1", "false")
	defer clusterhook.SetEnabled(clusterhook.Enabled())
	clusterhook.SetEnabled(true)

	src := filepath.Join(t.TempDir(), "src.git")
	gitIn(t, filepath.Dir(src), "", "init", "-q", src)
	gitIn(t, src, "", "commit", "-q", "--allow-empty", "-m", "one")
	gitIn(t, src, "", "branch", "gone")
	gitIn(t, src, "", "branch", "moved")
	root := t.TempDir()
	mirror := filepath.Join(root, "mirror.git")
	gitIn(t, root, "", "clone", "-q", "--mirror", src, mirror)
	transactions := countTransactions(t, root)

	dry := func(prune bool) *git.Command {
		cmd := git.NewCommand(t.Context(), "fetch", "--dry-run", "--porcelain")
		if prune {
			cmd.AddArguments("--prune")
		}
		return cmd.AddArguments("--tags").AddDynamicArguments("origin")
	}

	// A small sync: the sync's own fetch runs.
	gitIn(t, src, "", "commit", "-q", "--allow-empty", "-m", "two")
	out, batched, err := fetchInBatches(t.Context(), dry(true), mirror, time.Minute, nil, nil)
	require.NoError(t, err)
	assert.False(t, batched)
	assert.Empty(t, out)
	// git writes HEAD on any fetch of a mirror, a dry run included; no
	// fetched ref.
	for _, c := range transactions() {
		assert.LessOrEqual(t, c, 1, "the dry run writes no fetched ref")
	}

	head := gitIn(t, src, "", "rev-parse", "HEAD")
	var in strings.Builder
	n := reftable.MaxUpdatesPerTxn + 100
	for i := range n {
		fmt.Fprintf(&in, "create refs/tags/big-%05d %s\n", i, head)
	}
	gitIn(t, src, in.String(), "update-ref", "--stdin")
	gitIn(t, src, "", "branch", "-D", "gone")
	gitIn(t, src, "", "update-ref", "refs/heads/moved", head)

	out, batched, err = fetchInBatches(t.Context(), dry(true), mirror, time.Minute, nil, nil)
	require.NoError(t, err)
	require.True(t, batched)
	counts := transactions()
	require.NotEmpty(t, counts)
	assert.LessOrEqual(t, slices.Max(counts), syncRefBatch)

	assert.Equal(t, head, gitIn(t, mirror, "", "rev-parse", "refs/tags/big-04195"))
	assert.Equal(t, head, gitIn(t, mirror, "", "rev-parse", "refs/heads/main"))
	assert.Equal(t, head, gitIn(t, mirror, "", "rev-parse", "refs/heads/moved"))
	assert.Empty(t, gitIn(t, mirror, "", "for-each-ref", "refs/heads/gone"), "pruned")

	results := parseRemoteUpdateOutput(out, "origin")
	assert.Len(t, results, n+3, "every tag, main, moved and gone")
	byRef := map[string]*mirrorSyncResult{}
	for _, r := range results {
		byRef[r.refName.String()] = r
	}
	require.Contains(t, byRef, "refs/heads/gone")
	assert.Equal(t, gitShortEmptySha, byRef["refs/heads/gone"].newCommitID)
	require.Contains(t, byRef, "refs/tags/big-00000")
	assert.Equal(t, gitShortEmptySha, byRef["refs/tags/big-00000"].oldCommitID)
	require.Contains(t, byRef, "refs/heads/main")
	assert.Equal(t, head, byRef["refs/heads/main"].newCommitID)

	// Nothing left: the next sync's own fetch has nothing to write.
	out, batched, err = fetchInBatches(t.Context(), dry(true), mirror, time.Minute, nil, nil)
	require.NoError(t, err)
	assert.False(t, batched)
	assert.Empty(t, out)
}

func TestParsePorcelainFetch(t *testing.T) {
	updates, ok := parsePorcelainFetch("* 0000 aaaa refs/tags/v1\n= aaaa aaaa refs/heads/same\n- bbbb 0000 refs/heads/gone\n+ cccc dddd refs/heads/forced\n")
	require.True(t, ok)
	require.Len(t, updates, 3)
	assert.Equal(t, porcelainUpdate{flag: '-', oldID: "bbbb", newID: "0000", ref: "refs/heads/gone"}, updates[1])
	for _, bad := range []string{"! aaaa bbbb refs/heads/rejected\n", "garbage\n", "* 0000 aaaa FETCH_HEAD\n"} {
		_, ok := parsePorcelainFetch(bad)
		assert.False(t, ok, bad)
	}
}

// A sync by batches that prunes a branch and creates one under its name (a,
// then a/b) writes the pruning first, as git fetch does: in one transaction
// git refuses to create a/b while a exists, and every later sync failed the
// same way (#149).
func TestMirrorSyncInBatchesPrunesBeforeCreating(t *testing.T) {
	t.Setenv("GIT_CONFIG_COUNT", "2")
	t.Setenv("GIT_CONFIG_KEY_0", "gc.auto")
	t.Setenv("GIT_CONFIG_VALUE_0", "0")
	t.Setenv("GIT_CONFIG_KEY_1", "maintenance.auto")
	t.Setenv("GIT_CONFIG_VALUE_1", "false")
	defer clusterhook.SetEnabled(clusterhook.Enabled())
	clusterhook.SetEnabled(true)

	src := filepath.Join(t.TempDir(), "src.git")
	gitIn(t, filepath.Dir(src), "", "init", "-q", src)
	gitIn(t, src, "", "commit", "-q", "--allow-empty", "-m", "one")
	gitIn(t, src, "", "branch", "a")
	gitIn(t, src, "", "branch", "b/c")
	root := t.TempDir()
	mirror := filepath.Join(root, "mirror.git")
	gitIn(t, root, "", "clone", "-q", "--mirror", src, mirror)

	head := gitIn(t, src, "", "rev-parse", "HEAD")
	var in strings.Builder
	for i := range reftable.MaxUpdatesPerTxn + 100 {
		fmt.Fprintf(&in, "create refs/tags/big-%05d %s\n", i, head)
	}
	gitIn(t, src, in.String(), "update-ref", "--stdin")
	// A directory becomes a file, and a file a directory.
	gitIn(t, src, "", "branch", "-D", "a", "b/c")
	gitIn(t, src, "", "branch", "a/b")
	gitIn(t, src, "", "branch", "b")

	dry := git.NewCommand(t.Context(), "fetch", "--dry-run", "--porcelain", "--prune", "--tags").AddDynamicArguments("origin")
	out, batched, err := fetchInBatches(t.Context(), dry, mirror, time.Minute, nil, nil)
	require.NoError(t, err)
	require.True(t, batched)
	assert.Equal(t, head, gitIn(t, mirror, "", "rev-parse", "refs/heads/a/b"))
	assert.Equal(t, head, gitIn(t, mirror, "", "rev-parse", "refs/heads/b"))
	assert.Equal(t, head, gitIn(t, mirror, "", "rev-parse", "refs/tags/big-04195"))
	assert.Empty(t, gitIn(t, mirror, "", "for-each-ref", "refs/heads/b/c"), "pruned")
	results := parseRemoteUpdateOutput(out, "origin")
	assert.Len(t, results, reftable.MaxUpdatesPerTxn+100+4, "every tag, a, b/c, a/b and b")
}

// localRefs lists the refs of the repository at dir.
func localRefs(t *testing.T, dir string) []string {
	t.Helper()
	return strings.Fields(gitIn(t, dir, "", "for-each-ref", "--format=%(refname)"))
}

// A sync without prune whose upstream replaced a by a/b, and b/c by b, as
// its main moved on: git rejects a/b and b, which conflict as a directory
// and a file with the refs the mirror keeps, and writes main. From git
// 2.51 the fetch is one transaction that tolerates a failure per ref, and
// the cluster's hook reads the rejected refs too: the cluster refused the
// whole transaction, and every later sync the same way (#365). The sync
// leaves them, writes the rest, and fails naming them, as git does.
func TestMirrorSyncLeavesTheRefsGitRejects(t *testing.T) {
	t.Setenv("GIT_CONFIG_COUNT", "2")
	t.Setenv("GIT_CONFIG_KEY_0", "gc.auto")
	t.Setenv("GIT_CONFIG_VALUE_0", "0")
	t.Setenv("GIT_CONFIG_KEY_1", "maintenance.auto")
	t.Setenv("GIT_CONFIG_VALUE_1", "false")
	defer clusterhook.SetEnabled(clusterhook.Enabled())
	clusterhook.SetEnabled(true)

	src := filepath.Join(t.TempDir(), "src.git")
	gitIn(t, filepath.Dir(src), "", "init", "-q", src)
	gitIn(t, src, "", "commit", "-q", "--allow-empty", "-m", "one")
	gitIn(t, src, "", "branch", "a")
	gitIn(t, src, "", "branch", "b/c")
	root := t.TempDir()
	mirror := filepath.Join(root, "mirror.git")
	gitIn(t, root, "", "clone", "-q", "--mirror", src, mirror)
	before := gitIn(t, mirror, "", "rev-parse", "refs/heads/main")

	gitIn(t, src, "", "branch", "-D", "a", "b/c")
	gitIn(t, src, "", "commit", "-q", "--allow-empty", "-m", "two")
	gitIn(t, src, "", "branch", "a/b")
	gitIn(t, src, "", "branch", "b")
	head := gitIn(t, src, "", "rev-parse", "HEAD")

	dry := func(prune bool) *git.Command {
		cmd := git.NewCommand(t.Context(), "fetch", "--dry-run", "--porcelain")
		if prune {
			cmd.AddArguments("--prune")
		}
		return cmd.AddArguments("--tags").AddDynamicArguments("origin")
	}

	// With prune, the deletions free the names: the sync's own fetch runs.
	out, batched, err := fetchInBatches(t.Context(), dry(true), mirror, time.Minute, nil, nil)
	require.NoError(t, err)
	assert.False(t, batched)
	assert.Empty(t, out)

	out, batched, err = fetchInBatches(t.Context(), dry(false), mirror, time.Minute, nil, nil)
	require.True(t, batched)
	var left errRefsNotUpdated
	require.ErrorAs(t, err, &left)
	var names []string
	for _, r := range left {
		names = append(names, r.ref)
	}
	assert.ElementsMatch(t, []string{"refs/heads/a/b", "refs/heads/b"}, names)
	assert.Contains(t, err.Error(), "refs/heads/a/b (it conflicts with refs/heads/a, which the mirror keeps)")
	assert.Contains(t, err.Error(), "refs/heads/b (it conflicts with refs/heads/b/c, which the mirror keeps)")

	assert.Equal(t, head, gitIn(t, mirror, "", "rev-parse", "refs/heads/main"), "main moved on")
	assert.ElementsMatch(t, []string{"refs/heads/a", "refs/heads/b/c", "refs/heads/main"}, localRefs(t, mirror))
	assert.Equal(t, before, gitIn(t, mirror, "", "rev-parse", "refs/heads/a"), "a is kept")
	results := parseRemoteUpdateOutput(out, "origin")
	require.Len(t, results, 1, "main alone was written")
	assert.Equal(t, "refs/heads/main", results[0].refName.String())

	// The next sync has nothing else to write, and reports them again.
	out, batched, err = fetchInBatches(t.Context(), dry(false), mirror, time.Minute, nil, nil)
	require.True(t, batched)
	require.ErrorAs(t, err, &left)
	assert.Len(t, left, 2)
	assert.Empty(t, out)

	// Control: git alone writes main and rejects the same two.
	plain := filepath.Join(t.TempDir(), "plain.git")
	gitIn(t, filepath.Dir(plain), "", "clone", "-q", "--mirror", mirror, plain)
	gitIn(t, plain, "", "remote", "set-url", "origin", src)
	gitIn(t, plain, "", "update-ref", "refs/heads/main", before)
	cmd := exec.Command("git", "-c", "core.hooksPath=/dev/null", "fetch", "--tags", "origin")
	cmd.Dir = plain
	cmd.Env = append(os.Environ(), "HOME="+setting.Git.HomePath)
	ctl, cerr := cmd.CombinedOutput()
	require.Error(t, cerr, "control: %s", ctl)
	assert.Equal(t, head, gitIn(t, plain, "", "rev-parse", "refs/heads/main"), "control: git writes main")
	assert.ElementsMatch(t, []string{"refs/heads/a", "refs/heads/b/c", "refs/heads/main"}, localRefs(t, plain), "control: git rejects the same refs")
}

// An upstream ref whose name git takes and the cluster cannot hold, not
// valid UTF-8 or too long: the cluster's hook refuses the whole
// transaction that holds one, and every sync was refused whole, the
// upstream's other refs with it (#365). The sync leaves it out, writes
// the rest, and fails naming it, quoted.
func TestMirrorSyncLeavesTheNamesTheClusterCannotHold(t *testing.T) {
	t.Setenv("GIT_CONFIG_COUNT", "2")
	t.Setenv("GIT_CONFIG_KEY_0", "gc.auto")
	t.Setenv("GIT_CONFIG_VALUE_0", "0")
	t.Setenv("GIT_CONFIG_KEY_1", "maintenance.auto")
	t.Setenv("GIT_CONFIG_VALUE_1", "false")
	defer clusterhook.SetEnabled(clusterhook.Enabled())
	clusterhook.SetEnabled(true)

	src := filepath.Join(t.TempDir(), "src.git")
	gitIn(t, filepath.Dir(src), "", "init", "-q", src)
	gitIn(t, src, "", "commit", "-q", "--allow-empty", "-m", "one")
	root := t.TempDir()
	mirror := filepath.Join(root, "mirror.git")
	gitIn(t, root, "", "clone", "-q", "--mirror", src, mirror)

	gitIn(t, src, "", "commit", "-q", "--allow-empty", "-m", "two")
	head := gitIn(t, src, "", "rev-parse", "HEAD")
	notUTF8 := "refs/heads/caf\xe9"
	long := "refs/heads/" + strings.TrimSuffix(strings.Repeat(strings.Repeat("x", 200)+"/", 6), "/")
	require.Error(t, reftable.ValidRefName(long))
	gitIn(t, src, "", "update-ref", notUTF8, head)
	gitIn(t, src, "", "update-ref", long, head)

	dry := git.NewCommand(t.Context(), "fetch", "--dry-run", "--porcelain", "--prune", "--tags").AddDynamicArguments("origin")
	out, batched, err := fetchInBatches(t.Context(), dry, mirror, time.Minute, nil, nil)
	require.True(t, batched)
	var left errRefsNotUpdated
	require.ErrorAs(t, err, &left)
	require.Len(t, left, 2)
	assert.ElementsMatch(t, []string{notUTF8, long}, []string{left[0].ref, left[1].ref})
	assert.Contains(t, err.Error(), strconv.Quote(notUTF8)+" (ref name is not valid UTF-8, which the cluster cannot hold)")
	assert.True(t, utf8.ValidString(err.Error()), "a notice holds the error")

	assert.Equal(t, head, gitIn(t, mirror, "", "rev-parse", "refs/heads/main"), "main moved on")
	assert.Equal(t, []string{"refs/heads/main"}, localRefs(t, mirror))
	results := parseRemoteUpdateOutput(out, "origin")
	require.Len(t, results, 1, "main alone was written")
	assert.Equal(t, "refs/heads/main", results[0].refName.String())
}
