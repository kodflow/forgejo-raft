// Copyright 2026 The Forgejo Authors. All rights reserved.
// SPDX-License-Identifier: GPL-3.0-or-later

package repository

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

	"forgejo.org/modules/git"
	"forgejo.org/modules/setting"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// gitIn runs git in dir without any hook and returns its trimmed output.
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

// A fork or a migration of a repository with more refs than the cluster
// takes in one transaction (4096): git 2.54 and later fetch, and every git
// mirror clone, write them all in one, which the cluster refused every
// time. In a cluster the refs come by batches, none past 512, and the
// mirror keeps its remote (#80).
func TestHuntR4ProveBigTxnForkAndMirror(t *testing.T) {
	const tags = 4196
	// As a node of a cluster runs git: no automatic maintenance, whose
	// pack-refs would rewrite every ref again, one transaction each.
	t.Setenv("GIT_CONFIG_COUNT", "2")
	t.Setenv("GIT_CONFIG_KEY_0", "gc.auto")
	t.Setenv("GIT_CONFIG_VALUE_0", "0")
	t.Setenv("GIT_CONFIG_KEY_1", "maintenance.auto")
	t.Setenv("GIT_CONFIG_VALUE_1", "false")
	src := filepath.Join(t.TempDir(), "src.git")
	gitIn(t, filepath.Dir(src), "", "init", "-q", src)
	gitIn(t, src, "", "commit", "-q", "--allow-empty", "-m", "one")
	head := gitIn(t, src, "", "rev-parse", "HEAD")
	var in strings.Builder
	for i := range tags {
		fmt.Fprintf(&in, "create refs/tags/big-%05d %s\n", i, head)
	}
	gitIn(t, src, in.String(), "update-ref", "--stdin")
	gitIn(t, src, "", "update-ref", "refs/pull/1/head", head)

	root := t.TempDir()
	transactions := countTransactions(t, root)
	refsOf := func(dir string, patterns ...string) int {
		return len(strings.Fields(gitIn(t, dir, "", append([]string{"for-each-ref", "--format=%(refname)"}, patterns...)...)))
	}
	ctx := t.Context()

	fork := filepath.Join(root, "fork.git")
	require.NoError(t, copyForkInCluster(ctx, ForkRepoOptions{}, src, fork, "main", ""))
	assert.Equal(t, tags+1, refsOf(fork), "the fork holds the branch and the tags, not the other refs")
	counts := transactions()
	require.NotEmpty(t, counts)
	assert.LessOrEqual(t, slices.Max(counts), clusterRefBatch, "a fork's transaction: %v", counts)

	mirror := filepath.Join(root, "mirror.git")
	require.NoError(t, cloneMirrorInCluster(ctx, src, mirror, git.CloneRepoOptions{Mirror: true, Quiet: true, Timeout: time.Minute}))
	assert.Equal(t, refsOf(src), refsOf(mirror), "the mirror holds every ref")
	counts = transactions()
	require.NotEmpty(t, counts)
	assert.LessOrEqual(t, slices.Max(counts), clusterRefBatch, "a mirror's transaction: %v", counts)
	assert.Equal(t, "refs/heads/main", gitIn(t, mirror, "", "symbolic-ref", "HEAD"))
	assert.Equal(t, src, gitIn(t, mirror, "", "config", "--get", "remote.origin.url"))
	assert.Equal(t, "true", gitIn(t, mirror, "", "config", "--get", "remote.origin.mirror"))
	assert.Equal(t, "+refs/*:refs/*", gitIn(t, mirror, "", "config", "--get", "remote.origin.fetch"))
	_, err := os.Stat(filepath.Join(mirror, "FETCH_HEAD"))
	assert.True(t, os.IsNotExist(err), "no FETCH_HEAD left behind")
}
