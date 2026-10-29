// Copyright 2026 The Forgejo Authors. All rights reserved.
// SPDX-License-Identifier: GPL-3.0-or-later

package git_test

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"forgejo.org/models/unittest"
	"forgejo.org/modules/clusterhook"
	"forgejo.org/modules/git"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// renamedRefs are the names a rename test compares after the rename.
var renamedRefs = []string{
	"HEAD", "refs/heads/master", "refs/heads/branch1", "refs/heads/branch2",
	"refs/heads/alias", "refs/heads/dangling", "refs/heads/gone", "refs/heads/legacy", "refs/heads/trunk",
}

// refsOf returns what git holds in dir under each of renamedRefs, one level
// deep: "ref:" and the target of a symbolic ref, the object id of another,
// nothing for a name git does not hold.
func refsOf(t *testing.T, dir string) map[string]string {
	t.Helper()
	out := map[string]string{}
	for _, name := range renamedRefs {
		if to, _, err := git.NewCommand(t.Context(), "symbolic-ref", "-q", "--no-recurse", "--end-of-options").AddDynamicArguments(name).RunStdString(&git.RunOpts{Dir: dir}); err == nil {
			out[name] = "ref:" + strings.TrimSpace(to)
			continue
		}
		if sha, _, err := git.NewCommand(t.Context(), "rev-parse", "-q", "--verify", "--end-of-options").AddDynamicArguments(name).RunStdString(&git.RunOpts{Dir: dir}); err == nil {
			out[name] = strings.TrimSpace(sha)
		}
	}
	return out
}

// renameBench copies the fixture repository into the test's directory, runs
// setup there, then records what git hands its reference-transaction hook,
// set in the copy's own config: the refs each line names, in its prepared
// phase. It returns the copy, open, and the reader of the record.
func renameBench(t *testing.T, setup [][]string) (repo *git.Repository, dir string, prepared func() []string) {
	t.Helper()
	dir = filepath.Join(t.TempDir(), "repo")
	require.NoError(t, unittest.CopyDir(filepath.Join(testReposDir, "repo1_bare"), dir))
	for _, args := range setup {
		_, _, err := git.NewCommand(t.Context(), git.ToTrustedCmdArgs(args)...).RunStdString(&git.RunOpts{Dir: dir})
		require.NoError(t, err, "git %v", args)
	}
	hooks := t.TempDir()
	record := filepath.Join(t.TempDir(), "transactions")
	script := "#!/bin/sh\n[ \"$1\" = prepared ] || { cat >/dev/null; exit 0; }\ncat >> '" + record + "'\n"
	require.NoError(t, os.WriteFile(filepath.Join(hooks, "reference-transaction"), []byte(script), 0o755))
	_, _, runErr := git.NewCommand(t.Context(), "config", "core.hooksPath").AddDynamicArguments(hooks).RunStdString(&git.RunOpts{Dir: dir})
	require.NoError(t, runErr)
	repo, err := openRepositoryWithDefaultContext(dir)
	require.NoError(t, err)
	t.Cleanup(func() { _ = repo.Close() })
	return repo, dir, func() []string {
		b, err := os.ReadFile(record)
		if os.IsNotExist(err) {
			return nil
		}
		require.NoError(t, err)
		var refs []string
		for line := range strings.SplitSeq(strings.TrimSpace(string(b)), "\n") {
			if f := strings.Fields(line); len(f) == 3 {
				refs = append(refs, f[2])
			}
		}
		return refs
	}
}

// In a cluster, RenameBranch is one ref transaction (#38, #189) where
// `git branch -m` is outside it. Both end with the same refs, HEAD
// included, and the transaction names only the two branches and HEAD:
// update-ref --stdin followed a symbolic ref, so renaming the alias
// refs/heads/master -> refs/heads/main deleted main, the default branch,
// on every node, where git branch -m refuses to rename a symbolic ref; and
// a rename onto a dangling alias created the branch it names, not the
// alias (#356). A symbolic branch is now refused as git branch -m refuses
// it, with an error that says so.
func TestRenameBranchInAClusterEndsAsGitBranchM(t *testing.T) {
	alias := []string{"symbolic-ref", "refs/heads/alias", "refs/heads/master"}
	headOnAlias := []string{"symbolic-ref", "HEAD", "refs/heads/alias"}
	dangling := []string{"symbolic-ref", "refs/heads/dangling", "refs/heads/gone"}
	for _, tc := range []struct {
		name     string
		setup    [][]string
		from, to string
		refused  bool
	}{
		{name: "a branch", from: "branch1", to: "trunk"},
		{name: "the branch HEAD names", from: "master", to: "trunk"},
		{name: "the branch HEAD names through an alias", setup: [][]string{alias, headOnAlias}, from: "master", to: "trunk"},
		{name: "a branch onto a dangling alias", setup: [][]string{dangling}, from: "branch1", to: "dangling"},
		{name: "an alias", setup: [][]string{alias}, from: "alias", to: "legacy", refused: true},
		{name: "the alias HEAD names", setup: [][]string{alias, headOnAlias}, from: "alias", to: "legacy", refused: true},
		{name: "a dangling alias", setup: [][]string{dangling}, from: "dangling", to: "legacy", refused: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			outside, outsideDir, _ := renameBench(t, tc.setup)
			inside, insideDir, prepared := renameBench(t, tc.setup)
			before := refsOf(t, insideDir)

			errOutside := outside.RenameBranch(tc.from, tc.to)
			clusterhook.SetEnabled(true)
			errInside := inside.RenameBranch(tc.from, tc.to)
			clusterhook.SetEnabled(false)
			t.Logf("git branch -m %s %s: %v", tc.from, tc.to, errOutside)
			t.Logf("in a cluster: %v; the hook was handed %v", errInside, prepared())
			t.Logf("refs outside a cluster: %v", refsOf(t, outsideDir))
			t.Logf("refs in a cluster:      %v", refsOf(t, insideDir))

			if tc.refused {
				require.Error(t, errOutside, "git branch -m refuses to rename a symbolic ref")
				assert.True(t, git.IsErrSymbolicBranch(errInside), "the rename of a symbolic branch is refused in a cluster too, and says why: %v", errInside)
				assert.Empty(t, prepared(), "nothing reaches the cluster's hook")
				assert.Equal(t, before, refsOf(t, insideDir), "nothing was renamed in a cluster")
				assert.Equal(t, before, refsOf(t, outsideDir), "nothing was renamed outside a cluster")
				return
			}
			require.NoError(t, errOutside)
			require.NoError(t, errInside)
			assert.Equal(t, refsOf(t, outsideDir), refsOf(t, insideDir), "the cluster's rename ends where git branch -m does")
			for _, ref := range prepared() {
				assert.Contains(t, []string{"HEAD", git.BranchPrefix + tc.from, git.BranchPrefix + tc.to}, ref, "the transaction writes only the two branches and HEAD")
			}
		})
	}
}
