// Copyright 2026 The Forgejo Authors. All rights reserved.
// SPDX-License-Identifier: GPL-3.0-or-later

package cluster

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"forgejo.org/services/cluster/reftable"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// symbolicBench is a node's bare repository under a table and a projector:
// the directory already holds the objects, as on the node that took the
// push, so no object channel is needed.
type symbolicBench struct {
	t    *testing.T
	root string
	dir  string
	work string
	tbl  *reftable.Table
	p    *projector
	idx  uint64
}

const symbolicRepo = "o/r.git"

func newSymbolicBench(t *testing.T) *symbolicBench {
	base := t.TempDir()
	b := &symbolicBench{t: t, root: filepath.Join(base, "repos"), work: filepath.Join(base, "work"), tbl: reftable.New()}
	require.NoError(t, os.MkdirAll(b.work, 0o755))
	run(t, b.work, "init", "-q")
	b.dir = filepath.Join(b.root, "o", "r.git")
	require.NoError(t, os.MkdirAll(filepath.Dir(b.dir), 0o755))
	run(t, base, "init", "-q", "--bare", b.dir)
	b.p = newProjector(b.root, b.tbl, newRegistry(time.Minute, 1, 1))
	b.p.unfence()
	return b
}

// commit makes a commit in the scratch repository and brings its objects
// into the node's directory under a ref git alone holds, which the table
// never tracks. They are fetched: a push would run the receiving side's
// hooks, the test Forgejo's, which the -c of gitIn does not reach, for
// seconds each.
func (b *symbolicBench) commit(msg string) string {
	sha := commit(b.t, b.work, msg)
	run(b.t, b.dir, "fetch", "-q", b.work, sha+":refs/scratch/"+msg)
	return sha
}

// apply applies one ref transaction of the repository at the next index.
func (b *symbolicBench) apply(updates ...reftable.RefUpdate) {
	b.t.Helper()
	b.idx++
	for i := range updates {
		if v, ok := b.tbl.Get(symbolicRepo, updates[i].Ref); ok {
			updates[i].ExpectRev = v.Rev
		}
	}
	applyAt(b.t, b.tbl, b.idx, reftable.KindRefTxn, &reftable.RefTxn{Repo: symbolicRepo, Format: "sha1", RepoID: 1, Updates: updates})
}

// project runs a few passes and reports whether the repository is
// certified at the last index, with the projector's last failure.
func (b *symbolicBench) project() (bool, string) {
	for range 3 {
		b.p.pass(context.Background())
	}
	b.p.mu.Lock()
	failure := b.p.failures[symbolicRepo]
	b.p.mu.Unlock()
	return b.p.Projected(symbolicRepo) == b.idx, failure
}

// git returns what git holds under name, read one level deep: "ref:" and
// the target for a symbolic ref, the object id for another, "" when git
// holds nothing.
func (b *symbolicBench) git(name string) string {
	if to, err := gitIn(b.dir, "symbolic-ref", "-q", "--no-recurse", name); err == nil {
		return "ref:" + to
	}
	sha, err := gitIn(b.dir, "rev-parse", "-q", "--verify", name)
	if err != nil {
		return ""
	}
	return sha
}

// A symbolic branch the table holds (the alias refs/heads/master ->
// refs/heads/main an operator leaves after renaming a default branch, as in
// #338) whose target Forgejo then renames away: create trunk, delete main,
// HEAD -> trunk. The table keeps master live, pointing at a tombstone, and
// git keeps the same dangling symbolic ref, which for-each-ref lists
// nowhere. The repository is certified, on a node that projects it and on
// one that starts, and stays open to writes (#348).
func TestDanglingSymbolicBranchIsCertified(t *testing.T) {
	for _, alias := range []bool{false, true} {
		name := "without alias"
		if alias {
			name = "with the alias master -> main"
		}
		t.Run(name, func(t *testing.T) {
			b := newSymbolicBench(t)
			c1 := b.commit("one")
			run(t, b.dir, "update-ref", "refs/heads/main", c1)
			run(t, b.dir, "symbolic-ref", "HEAD", "refs/heads/main")
			if alias {
				run(t, b.dir, "symbolic-ref", "refs/heads/master", "refs/heads/main")
			}
			b.apply(reftable.RefUpdate{Ref: "HEAD", Symbolic: "refs/heads/main"}, reftable.RefUpdate{Ref: "refs/heads/main", New: c1})
			if alias {
				// git symbolic-ref refs/heads/master refs/heads/main,
				// through the hook.
				b.apply(reftable.RefUpdate{Ref: "refs/heads/master", Symbolic: "refs/heads/main"})
			}
			certified, failure := b.project()
			require.True(t, certified, "certified before the rename: %s", failure)

			// Forgejo renames the default branch main to trunk, on this node.
			run(t, b.dir, "update-ref", "refs/heads/trunk", c1)
			run(t, b.dir, "update-ref", "-d", "refs/heads/main", c1)
			run(t, b.dir, "symbolic-ref", "HEAD", "refs/heads/trunk")
			b.apply(reftable.RefUpdate{Ref: "refs/heads/trunk", New: c1}, reftable.RefUpdate{Ref: "refs/heads/main", Delete: true, Old: c1})
			b.apply(reftable.RefUpdate{Ref: "HEAD", Symbolic: "refs/heads/trunk"})
			if alias {
				v, ok := b.tbl.Get(symbolicRepo, "refs/heads/master")
				require.True(t, ok && v.Live(), "the table keeps the alias")
				require.Equal(t, "refs/heads/main", v.Symbolic)
				out := run(t, b.dir, "for-each-ref", "--format=%(refname)")
				require.NotContains(t, out, "refs/heads/master", "for-each-ref lists no dangling symbolic ref")
			}

			certified, failure = b.project()
			assert.True(t, certified, "certified at the rename (projected %d, applied %d): %s", b.p.Projected(symbolicRepo), b.idx, failure)
			assert.Empty(t, failure)
			same, err := b.p.Verify(context.Background(), symbolicRepo)
			require.NoError(t, err)
			assert.True(t, same, "Verify finds git as the table holds it")
			if alias {
				assert.Equal(t, "ref:refs/heads/main", b.git("refs/heads/master"), "git keeps the alias the table holds")
			}
			assert.Equal(t, c1, b.git("refs/heads/trunk"))
			assert.Empty(t, b.git("refs/heads/main"))

			// A start of the node: its first reconciliation certifies the
			// repository, which takes writes again.
			p2 := newProjector(b.root, b.tbl, newRegistry(time.Minute, 1, 1))
			p2.unfence()
			for range 3 {
				p2.pass(context.Background())
			}
			assert.True(t, p2.Writable(symbolicRepo), "the repository takes writes after a start of the node")
		})
	}
}

// A node that never held the dangling alias gets it from the table, and
// once it holds it, the projector does not write it again at every pass.
func TestDanglingSymbolicBranchIsProjected(t *testing.T) {
	b := newSymbolicBench(t)
	c1 := b.commit("one")
	b.apply(
		reftable.RefUpdate{Ref: "HEAD", Symbolic: "refs/heads/trunk"},
		reftable.RefUpdate{Ref: "refs/heads/trunk", New: c1},
		reftable.RefUpdate{Ref: "refs/heads/master", Symbolic: "refs/heads/main"},
	)
	certified, failure := b.project()
	assert.True(t, certified, "certified (projected %d, applied %d): %s", b.p.Projected(symbolicRepo), b.idx, failure)
	assert.Equal(t, "ref:refs/heads/main", b.git("refs/heads/master"), "the projector writes the dangling alias")

	// git writes a symbolic ref through a lock file renamed over it: a
	// write would leave another file under the name.
	path := filepath.Join(b.dir, "refs", "heads", "master")
	before, err := os.Lstat(path)
	require.NoError(t, err)
	b.p.markDirty(symbolicRepo)
	certified, failure = b.project()
	assert.True(t, certified, failure)
	after, err := os.Lstat(path)
	require.NoError(t, err)
	assert.True(t, os.SameFile(before, after), "the dangling alias is not written again")
}

// The remedy to #348, the dangling alias deleted on one node: the others
// keep a dangling symbolic ref git lists nowhere, which Forgejo never sees,
// and certify the repository. When its target comes back, the alias
// resolves again: the projector removes it, never the branch it names.
func TestDeletedDanglingSymbolicBranch(t *testing.T) {
	b := newSymbolicBench(t)
	c1 := b.commit("one")
	c2 := b.commit("two")
	b.apply(
		reftable.RefUpdate{Ref: "HEAD", Symbolic: "refs/heads/trunk"},
		reftable.RefUpdate{Ref: "refs/heads/trunk", New: c1},
		reftable.RefUpdate{Ref: "refs/heads/main", New: c1},
		reftable.RefUpdate{Ref: "refs/heads/master", Symbolic: "refs/heads/main"},
	)
	certified, failure := b.project()
	require.True(t, certified, failure)
	b.apply(reftable.RefUpdate{Ref: "refs/heads/main", Delete: true})
	certified, failure = b.project()
	assert.True(t, certified, "certified with the alias dangling (projected %d, applied %d): %s", b.p.Projected(symbolicRepo), b.idx, failure)
	assert.Equal(t, "ref:refs/heads/main", b.git("refs/heads/master"))

	b.apply(reftable.RefUpdate{Ref: "refs/heads/master", Delete: true})
	certified, failure = b.project()
	assert.True(t, certified, "certified once the alias is deleted (projected %d, applied %d): %s", b.p.Projected(symbolicRepo), b.idx, failure)
	same, err := b.p.Verify(context.Background(), symbolicRepo)
	require.NoError(t, err)
	assert.True(t, same)

	b.apply(reftable.RefUpdate{Ref: "refs/heads/main", New: c2})
	certified, failure = b.project()
	assert.True(t, certified, "certified once main is back (projected %d, applied %d): %s", b.p.Projected(symbolicRepo), b.idx, failure)
	assert.Equal(t, c2, b.git("refs/heads/main"), "main as the table holds it")
	assert.Empty(t, b.git("refs/heads/master"), "master as the table holds it")
}

// A symbolic ref whose target is itself symbolic: HEAD -> refs/heads/master
// -> refs/heads/main, and refs/heads/a -> refs/heads/master. The table holds
// each first link, which is what git holds too; for-each-ref and
// symbolic-ref without --no-recurse name the end of the chain, which the
// projector compared with the first link and never certified (#348).
func TestSymbolicChainsAreCertified(t *testing.T) {
	b := newSymbolicBench(t)
	c1 := b.commit("one")
	b.apply(
		reftable.RefUpdate{Ref: "HEAD", Symbolic: "refs/heads/master"},
		reftable.RefUpdate{Ref: "refs/heads/main", New: c1},
		reftable.RefUpdate{Ref: "refs/heads/master", Symbolic: "refs/heads/main"},
		reftable.RefUpdate{Ref: "refs/heads/a", Symbolic: "refs/heads/master"},
	)
	certified, failure := b.project()
	assert.True(t, certified, "certified (projected %d, applied %d): %s", b.p.Projected(symbolicRepo), b.idx, failure)
	assert.Empty(t, failure)
	same, err := b.p.Verify(context.Background(), symbolicRepo)
	require.NoError(t, err)
	assert.True(t, same, "Verify finds git as the table holds it")
	assert.Equal(t, "ref:refs/heads/master", b.git("HEAD"))
	assert.Equal(t, "ref:refs/heads/main", b.git("refs/heads/master"))
	assert.Equal(t, "ref:refs/heads/master", b.git("refs/heads/a"))
	assert.Equal(t, c1, b.git("refs/heads/main"))
}

// The projector replaces or deletes a symbolic ref git holds, never the
// ref it points to: git's update-ref follows a symbolic ref unless told
// not to, so deleting the alias refs/heads/master -> refs/heads/main, the
// remedy to #348, deleted main on every other node, and giving master a
// value of its own moved main there; neither was ever certified again.
func TestSymbolicRefIsReplacedWithoutDereference(t *testing.T) {
	for _, tc := range []struct {
		name   string
		change func(b *symbolicBench, c2 string)
		master func(c2 string) string
	}{
		{
			name:   "deleted",
			change: func(b *symbolicBench, _ string) { b.apply(reftable.RefUpdate{Ref: "refs/heads/master", Delete: true}) },
			master: func(string) string { return "" },
		},
		{
			name:   "given a value of its own",
			change: func(b *symbolicBench, c2 string) { b.apply(reftable.RefUpdate{Ref: "refs/heads/master", New: c2}) },
			master: func(c2 string) string { return c2 },
		},
		{
			name: "dangling, given a value of its own",
			change: func(b *symbolicBench, c2 string) {
				b.apply(reftable.RefUpdate{Ref: "refs/heads/main", Delete: true})
				b.apply(reftable.RefUpdate{Ref: "refs/heads/master", New: c2})
			},
			master: func(c2 string) string { return c2 },
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			b := newSymbolicBench(t)
			c1 := b.commit("one")
			c2 := b.commit("two")
			b.apply(
				reftable.RefUpdate{Ref: "HEAD", Symbolic: "refs/heads/main"},
				reftable.RefUpdate{Ref: "refs/heads/main", New: c1},
				reftable.RefUpdate{Ref: "refs/heads/master", Symbolic: "refs/heads/main"},
			)
			certified, failure := b.project()
			require.True(t, certified, "certified with the alias: %s", failure)
			require.Equal(t, "ref:refs/heads/main", b.git("refs/heads/master"))

			tc.change(b, c2)
			certified, failure = b.project()
			assert.True(t, certified, "certified (projected %d, applied %d): %s", b.p.Projected(symbolicRepo), b.idx, failure)
			want := c1
			if v, _ := b.tbl.Get(symbolicRepo, "refs/heads/main"); !v.Live() {
				want = ""
			}
			assert.Equal(t, want, b.git("refs/heads/main"), "main as the table holds it")
			assert.Equal(t, tc.master(c2), b.git("refs/heads/master"), "master as the table holds it")
		})
	}
}

// forgejo cluster import takes each symbolic ref by its first link, as git
// holds it and as the projector reads it: HEAD -> refs/heads/master ->
// refs/heads/main went into the table as HEAD -> refs/heads/main, the end
// of its chain, and every other node pointed its HEAD past the alias. A
// first link no table holds, refs/heads/h -> HEAD, is imported at the end
// of its chain, as before, never refused (#348).
func TestImportTakesTheFirstLinkOfASymbolicRef(t *testing.T) {
	tc := newTestCluster(t, "node-a", "node-b", "node-c")
	work := t.TempDir()
	run(t, work, "init", "-q")
	c1 := commit(t, work, "one")
	origin := tc.nodes[1]
	const repo = "old/sym.git"
	dir := filepath.Join(origin.r.root, filepath.FromSlash(repo))
	run(t, origin.r.root, "init", "-q", "--bare", dir)
	run(t, dir, "fetch", "-q", work, c1+":refs/heads/main")
	run(t, dir, "symbolic-ref", "refs/heads/master", "refs/heads/main")
	run(t, dir, "symbolic-ref", "HEAD", "refs/heads/master")
	run(t, dir, "symbolic-ref", "refs/heads/h", "HEAD")

	rep, err := origin.r.Import(context.Background(), repo)
	require.NoError(t, err)
	assert.Empty(t, rep.Failures)
	for name, to := range map[string]string{"HEAD": "refs/heads/master", "refs/heads/master": "refs/heads/main", "refs/heads/h": "refs/heads/main"} {
		v, ok := origin.r.table.Get(repo, name)
		require.True(t, ok && v.Live(), "the table holds %s", name)
		assert.Equal(t, to, v.Symbolic, "the table holds %s by its first link", name)
	}
	_, at := origin.r.table.Refs(repo)
	for _, n := range tc.nodes {
		require.Eventually(t, func() bool { return n.r.projector.Projected(repo) >= at }, 15*time.Second, 20*time.Millisecond, "%s certifies the import", n.slot)
		ndir := filepath.Join(n.r.root, filepath.FromSlash(repo))
		assert.Equal(t, "refs/heads/master", run(t, ndir, "symbolic-ref", "--no-recurse", "HEAD"), n.slot)
		assert.Equal(t, "refs/heads/main", run(t, ndir, "symbolic-ref", "--no-recurse", "refs/heads/master"), n.slot)
		assert.Equal(t, c1, run(t, ndir, "rev-parse", "refs/heads/h"), n.slot)
	}
}
