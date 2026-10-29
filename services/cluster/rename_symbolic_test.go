// Copyright 2026 The Forgejo Authors. All rights reserved.
// SPDX-License-Identifier: GPL-3.0-or-later

package cluster

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"forgejo.org/modules/clusterhook"
	"forgejo.org/modules/git"
	"forgejo.org/modules/setting"
	"forgejo.org/modules/test"
	"forgejo.org/services/cluster/reftable"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// recordTransactions sets, in the repository dir's own config, a
// reference-transaction hook that records what git hands the cluster's
// hook: each prepared transaction, and whether the git command said it
// only creates its refs (clusterhook.EnvCreateOnly). The repository's own
// config wins over the suite's global core.hooksPath; the projector and the
// tests' git keep core.hooksPath=/dev/null on their command line. It
// returns the reader of the record, as requests to the cluster's hook.
func recordTransactions(t *testing.T, dir string) func() []*clusterhook.Request {
	t.Helper()
	hooks := filepath.Join(t.TempDir(), "hooks")
	require.NoError(t, os.MkdirAll(hooks, 0o755))
	record := t.TempDir()
	script := fmt.Sprintf("#!/bin/sh\n[ \"$1\" = prepared ] || { cat >/dev/null; exit 0; }\nn=$(ls '%[1]s' | wc -l | tr -d ' ')\ncat > \"%[1]s/$(printf %%03d \"$n\")-$%[2]s\"\n", record, clusterhook.EnvCreateOnly)
	require.NoError(t, os.WriteFile(filepath.Join(hooks, "reference-transaction"), []byte(script), 0o755))
	run(t, dir, "config", "core.hooksPath", hooks)
	return func() []*clusterhook.Request {
		entries, err := os.ReadDir(record)
		require.NoError(t, err)
		var reqs []*clusterhook.Request
		for _, e := range entries {
			b, err := os.ReadFile(filepath.Join(record, e.Name()))
			require.NoError(t, err)
			lines, err := clusterhook.ParseLines(bytes.NewReader(b))
			require.NoError(t, err)
			reqs = append(reqs, &clusterhook.Request{GitDir: dir, Lines: lines, CreateOnly: strings.HasSuffix(e.Name(), "-1")})
		}
		return reqs
	}
}

// inACluster runs Forgejo's git as on a node of a cluster, until the
// returned func.
func inACluster() func() {
	restore := test.MockVariableValue(&setting.Cluster.Enabled, true)
	clusterhook.SetEnabled(true)
	return func() {
		clusterhook.SetEnabled(false)
		restore()
	}
}

// propose runs what git handed the hook through the proposer's buildTxn,
// as the cluster's hook does, and applies each transaction it takes to the
// bench's table. It returns the proposer's refusals.
func (b *symbolicBench) propose(reqs []*clusterhook.Request) []error {
	b.t.Helper()
	r := &Replicator{table: b.tbl}
	var refusals []error
	for _, req := range reqs {
		for _, l := range req.Lines {
			b.t.Logf("handed to the hook (create-only=%v): %s %s %s", req.CreateOnly, l.Old, l.New, l.Ref)
		}
		tx, _, err := r.buildTxn(context.Background(), symbolicRepo, req)
		if err != nil {
			refusals = append(refusals, err)
			continue
		}
		b.apply(tx.Updates...)
	}
	return refusals
}

// An alias branch the table holds (refs/heads/master -> refs/heads/main,
// the #338/#348 family; imported from a forge that kept it, and listed as a
// branch by Forgejo, which reads refs/heads/ with for-each-ref). A user
// renames the branch "master" to "legacy". Outside a cluster, git branch -m
// refuses: a symbolic ref cannot be renamed. In a cluster, RenameBranch
// took renameBranchInOneTransaction, whose update-ref --stdin followed the
// symbolic ref: it deleted main, the default branch, protected or not, and
// git handed the cluster's hook "delete refs/heads/main", which the
// proposer took. Every node projected and certified main gone, HEAD and the
// alias dangling (#356). The rename of an alias is now refused in a cluster
// as outside it: nothing reaches the hook, main stays in git and in the
// table.
func TestRenamingAnAliasBranchKeepsItsTarget(t *testing.T) {
	b := newSymbolicBench(t)
	c1 := b.commit("one")
	run(t, b.dir, "update-ref", "refs/heads/main", c1)
	run(t, b.dir, "symbolic-ref", "HEAD", "refs/heads/main")
	run(t, b.dir, "symbolic-ref", "refs/heads/master", "refs/heads/main")
	b.apply(
		reftable.RefUpdate{Ref: "HEAD", Symbolic: "refs/heads/main"},
		reftable.RefUpdate{Ref: "refs/heads/main", New: c1},
		reftable.RefUpdate{Ref: "refs/heads/master", Symbolic: "refs/heads/main"},
	)
	certified, failure := b.project()
	require.True(t, certified, "certified with the alias: %s", failure)
	recorded := recordTransactions(t, b.dir)

	ctx := context.Background()
	gitRepo, err := git.OpenRepository(ctx, b.dir)
	require.NoError(t, err)
	defer gitRepo.Close()
	t.Logf("before: IsBranchExist(master)=%v IsBranchExist(legacy)=%v", gitRepo.IsBranchExist("master"), gitRepo.IsBranchExist("legacy"))

	// Outside a cluster: git branch -m, unchanged.
	func() {
		defer test.MockVariableValue(&setting.Cluster.Enabled, false)()
		err := gitRepo.RenameBranch("master", "legacy")
		t.Logf("outside a cluster, RenameBranch(master, legacy): %v", err)
		require.Error(t, err, "git branch -m refuses to rename a symbolic ref")
	}()
	assert.Equal(t, c1, b.git("refs/heads/main"), "outside a cluster main is untouched")
	assert.Equal(t, "ref:refs/heads/main", b.git("refs/heads/master"))
	// git refused before any transaction.
	require.Empty(t, recorded())

	// In a cluster.
	var renameErr error
	func() {
		defer inACluster()()
		renameErr = gitRepo.RenameBranch("master", "legacy")
		t.Logf("in a cluster, RenameBranch(master, legacy): %v", renameErr)
	}()
	reqs := recorded()
	refusals := b.propose(reqs)
	t.Logf("git after: main=%q master=%q legacy=%q HEAD=%q", b.git("refs/heads/main"), b.git("refs/heads/master"), b.git("refs/heads/legacy"), b.git("HEAD"))
	_, headErr := gitIn(b.dir, "rev-parse", "-q", "--verify", "HEAD")
	certified, failure = b.project()
	main, _ := b.tbl.Get(symbolicRepo, "refs/heads/main")
	head, _ := b.tbl.Get(symbolicRepo, "HEAD")
	master, _ := b.tbl.Get(symbolicRepo, "refs/heads/master")
	t.Logf("table after: main live=%v, HEAD -> %q, master live=%v -> %q; certified=%v %s; refusals %v", main.Live(), head.Symbolic, master.Live(), master.Symbolic, certified, failure, refusals)

	assert.True(t, git.IsErrSymbolicBranch(renameErr), "the rename of a symbolic branch is refused, and says why: %v", renameErr)
	assert.Empty(t, reqs, "nothing of the rename reaches the cluster's hook")
	assert.Equal(t, c1, b.git("refs/heads/main"), "main is still in git after renaming the alias master")
	assert.Equal(t, "ref:refs/heads/main", b.git("refs/heads/master"), "the alias is kept")
	assert.Empty(t, b.git("refs/heads/legacy"))
	assert.True(t, certified, failure)
	assert.True(t, main.Live(), "main, the default branch, is still in the table after renaming the alias master")
	assert.Equal(t, c1, main.SHA)
	assert.Equal(t, "refs/heads/main", head.Symbolic)
	assert.Equal(t, "refs/heads/main", master.Symbolic)
	require.NoError(t, headErr, "HEAD still resolves")
}

// A dangling alias the table holds, refs/heads/master -> refs/heads/main
// once main was renamed away (#348). A user renames the branch feature to
// "master", which this node's git does not show as a branch. update-ref
// --stdin followed the symbolic ref there too: it created main, a name the
// rename never asked for, the default or a protected branch before, and
// the cluster took that creation, for every node; "master" stayed the
// alias. The update now names the alias itself, which the cluster holds:
// the proposer refuses to create it, so the rename answers that the target
// exists, nothing written (#356).
func TestRenamingOntoADanglingAliasNeverCreatesItsTarget(t *testing.T) {
	b := newSymbolicBench(t)
	c1 := b.commit("one")
	c2 := b.commit("two")
	run(t, b.dir, "update-ref", "refs/heads/trunk", c1)
	run(t, b.dir, "update-ref", "refs/heads/feature", c2)
	run(t, b.dir, "symbolic-ref", "HEAD", "refs/heads/trunk")
	run(t, b.dir, "symbolic-ref", "refs/heads/master", "refs/heads/main")
	b.apply(
		reftable.RefUpdate{Ref: "HEAD", Symbolic: "refs/heads/trunk"},
		reftable.RefUpdate{Ref: "refs/heads/trunk", New: c1},
		reftable.RefUpdate{Ref: "refs/heads/feature", New: c2},
		reftable.RefUpdate{Ref: "refs/heads/master", Symbolic: "refs/heads/main"},
	)
	certified, failure := b.project()
	require.True(t, certified, "certified with the dangling alias: %s", failure)
	recorded := recordTransactions(t, b.dir)

	gitRepo, err := git.OpenRepository(context.Background(), b.dir)
	require.NoError(t, err)
	defer gitRepo.Close()
	require.False(t, gitRepo.IsBranchExist("master"), "Forgejo does not see a dangling alias as a branch")
	func() {
		defer inACluster()()
		err = gitRepo.RenameBranch("feature", "master")
		t.Logf("in a cluster, RenameBranch(feature, master): %v", err)
	}()
	reqs := recorded()
	refusals := b.propose(reqs)
	t.Logf("proposer's refusals: %v", refusals)

	var named []string
	for _, req := range reqs {
		for _, l := range req.Lines {
			named = append(named, l.Ref)
		}
	}
	assert.NotContains(t, named, "refs/heads/main", "the rename's transaction never names the alias's target")
	assert.Contains(t, named, "refs/heads/master", "the rename's transaction names the alias itself")
	require.Len(t, refusals, 1, "the cluster refuses to create a name it holds")
	assert.True(t, clusterhook.IsStaleCreate(refusals[0]), "refused as a name the cluster holds, which the rename answers as a target that exists: %v", refusals[0])
	main, ok := b.tbl.Get(symbolicRepo, "refs/heads/main")
	assert.False(t, ok && main.Live(), "main was never created in the table")
	assert.Empty(t, b.git("refs/heads/main"), "nor in git")
	// The cluster's hook refuses the transaction the proposer refuses, so
	// git writes nothing of it; this bench's hook only records:
	// TestRenamingASymbolicBranchKeepsEveryMember runs the real one.
}

// The same two renames through Forgejo's git on a node of a three-node
// cluster, its hook the real one: the alias is refused before anything is
// proposed, the rename onto the dangling alias is refused by the cluster,
// and every member keeps main, the alias, and feature (#356).
func TestRenamingASymbolicBranchKeepsEveryMember(t *testing.T) {
	tc := newTestCluster(t, "node-a", "node-b", "node-c")
	n := tc.nodes[0]
	work := t.TempDir()
	run(t, work, "init", "-q")
	c1 := commit(t, work, "one")
	zero := strings.Repeat("0", 40)
	const repo = "alice/r1alias.git"
	require.True(t, tc.pushVia(n, work, repo, "refs/heads/main", zero, c1).OK)
	require.True(t, tc.pushVia(n, work, repo, "refs/heads/feature", zero, c1).OK)
	dir := filepath.Join(n.r.root, filepath.FromSlash(repo))

	withGitThroughReplicator(t, n)
	ctx := context.Background()
	for _, args := range [][]string{
		{"symbolic-ref", "refs/heads/master", "refs/heads/main"},
		{"symbolic-ref", "refs/heads/old", "refs/heads/gone"},
	} {
		_, _, err := git.NewCommand(ctx, git.ToTrustedCmdArgs(args)...).RunStdString(&git.RunOpts{Dir: dir})
		require.NoError(t, err, "git %v through the cluster", args)
	}
	held := func(m *testNode) bool {
		master, okMaster := m.r.table.Get(repo, "refs/heads/master")
		old, okOld := m.r.table.Get(repo, "refs/heads/old")
		return okMaster && master.Symbolic == "refs/heads/main" && okOld && old.Symbolic == "refs/heads/gone"
	}
	for _, m := range tc.nodes {
		require.Eventually(t, func() bool {
			return held(m) && gitRef(t, m, repo, "refs/heads/main") == c1 && gitRef(t, m, repo, "refs/heads/feature") == c1
		}, 15*time.Second, 20*time.Millisecond, "%s: precondition, the aliases are on every member", m.slot)
	}
	before, _ := n.r.table.Refs(repo)

	g, err := git.OpenRepository(ctx, dir)
	require.NoError(t, err)
	defer g.Close()
	errAlias := g.RenameBranch("master", "legacy")
	errOnto := g.RenameBranch("feature", "old")
	t.Logf("RenameBranch(master, legacy): %v", errAlias)
	t.Logf("RenameBranch(feature, old): %v", errOnto)
	assert.True(t, git.IsErrSymbolicBranch(errAlias), "the rename of the alias is refused: %v", errAlias)
	assert.True(t, clusterhook.IsStaleCreate(errOnto), "the rename onto the dangling alias is refused by the cluster: %v", errOnto)

	after, now := n.r.table.Refs(repo)
	assert.Equal(t, before, after, "nothing was written to the repository in the table")
	for _, m := range tc.nodes {
		require.Eventually(t, func() bool { return m.r.projector.Projected(repo) >= now }, 15*time.Second, 20*time.Millisecond, "%s certifies the repository", m.slot)
		mdir := filepath.Join(m.r.root, filepath.FromSlash(repo))
		read := func(name string) string {
			if to, err := gitIn(mdir, "symbolic-ref", "-q", "--no-recurse", name); err == nil {
				return "ref:" + to
			}
			sha, _ := gitIn(mdir, "rev-parse", "-q", "--verify", name)
			return sha
		}
		assert.True(t, held(m), "%s: the table keeps both aliases", m.slot)
		for name, want := range map[string]string{
			"refs/heads/main":    c1,
			"refs/heads/feature": c1,
			"refs/heads/master":  "ref:refs/heads/main",
			"refs/heads/old":     "ref:refs/heads/gone",
			"refs/heads/legacy":  "",
			"refs/heads/gone":    "",
		} {
			assert.Equal(t, want, read(name), "%s: %s", m.slot, name)
		}
		for _, name := range []string{"refs/heads/main", "refs/heads/feature"} {
			v, ok := m.r.table.Get(repo, name)
			assert.True(t, ok && v.Live() && v.SHA == c1, "%s: the table keeps %s", m.slot, name)
		}
	}
}
