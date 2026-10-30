// Copyright 2026 The Forgejo Authors. All rights reserved.
// SPDX-License-Identifier: GPL-3.0-or-later

package cluster

import (
	"context"
	"path/filepath"
	"testing"

	"forgejo.org/services/cluster/reftable"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// readGitRefs reads each symbolic ref one level deep, a dangling one too
// when the caller names it, and hands git no name as an option. A first
// link no table holds (HEAD, a name outside refs/) stays as for-each-ref
// lists it, at the end of its chain, or unseen when dangling.
func TestReadGitRefsSymbolicOneLevelDeep(t *testing.T) {
	dir := t.TempDir()
	run(t, dir, "init", "-q")
	c1 := commit(t, dir, "one")
	run(t, dir, "symbolic-ref", "refs/heads/master", "refs/heads/main")
	run(t, dir, "symbolic-ref", "refs/heads/a", "refs/heads/master")
	run(t, dir, "symbolic-ref", "refs/heads/old", "refs/heads/gone")
	run(t, dir, "symbolic-ref", "refs/heads/h", "HEAD")
	run(t, dir, "symbolic-ref", "refs/heads/odd", "FOO")
	run(t, dir, "symbolic-ref", "HEAD", "refs/heads/master")
	want := map[string]reftable.ProjectedRef{
		"HEAD":              {Symbolic: "refs/heads/master"},
		"refs/heads/main":   {SHA: c1},
		"refs/heads/master": {Symbolic: "refs/heads/main"},
		"refs/heads/a":      {Symbolic: "refs/heads/master"},
		"refs/heads/h":      {Symbolic: "refs/heads/main"},
	}

	refs, err := readGitRefs(context.Background(), dir)
	require.NoError(t, err)
	assert.Equal(t, want, refs, "a dangling symbolic ref no caller names stays unseen")

	refs, err = readGitRefs(context.Background(), dir, "refs/heads/old", "refs/heads/odd", "refs/heads/none", "refs/heads/main", "--delete", "HEAD")
	require.NoError(t, err)
	want["refs/heads/old"] = reftable.ProjectedRef{Symbolic: "refs/heads/gone"}
	assert.Equal(t, want, refs)
	assert.Equal(t, c1, run(t, dir, "rev-parse", "refs/heads/main"), "reading changes nothing")
}

// readTrackedRefs reads, besides the symbolic refs the table holds, each
// of its tombstones git still holds as a dangling symbolic ref, on both of
// git's ref backends (#359). A tombstone git holds nothing under, or a
// directory of other refs, or one whose path crosses a ref's file, is
// absent.
func TestReadTrackedRefsReadsTombstones(t *testing.T) {
	for _, format := range []string{"files", "reftable"} {
		t.Run(format, func(t *testing.T) {
			work := t.TempDir()
			run(t, work, "init", "-q")
			c1 := commit(t, work, "one")
			dir := filepath.Join(t.TempDir(), "r.git")
			run(t, work, "init", "-q", "--bare", "--ref-format="+format, dir)
			run(t, dir, "fetch", "-q", work, c1+":refs/heads/main")
			run(t, dir, "update-ref", "refs/heads/dir/y", c1)
			run(t, dir, "symbolic-ref", "HEAD", "refs/heads/main")
			run(t, dir, "symbolic-ref", "refs/heads/master", "refs/heads/trunk")
			run(t, dir, "symbolic-ref", "refs/heads/old", "refs/heads/gone")
			run(t, dir, "symbolic-ref", "refs/heads/h", "HEAD")
			sym := func(to string) reftable.RefValue { return reftable.RefValue{Symbolic: to, Rev: 1} }
			sha := func(sha string) reftable.RefValue { return reftable.RefValue{SHA: sha, Rev: 1} }
			tomb := reftable.RefValue{Rev: 2, Tombstone: true}
			table := map[string]reftable.RefValue{
				"HEAD":              sym("refs/heads/main"),
				"refs/heads/main":   sha(c1),
				"refs/heads/dir/y":  sha(c1),
				"refs/heads/master": sym("refs/heads/trunk"),
				"refs/heads/old":    tomb,
				"refs/heads/h":      tomb,
				"refs/heads/none":   tomb,
				"refs/heads/dir":    tomb,
				"refs/heads/main/x": tomb,
				"--delete":          tomb,
			}
			refs, err := readTrackedRefs(context.Background(), dir, table)
			require.NoError(t, err)
			assert.Equal(t, map[string]reftable.ProjectedRef{
				"HEAD":              {Symbolic: "refs/heads/main"},
				"refs/heads/main":   {SHA: c1},
				"refs/heads/dir/y":  {SHA: c1},
				"refs/heads/master": {Symbolic: "refs/heads/trunk"},
				"refs/heads/old":    {Symbolic: "refs/heads/gone"},
				"refs/heads/h":      {Symbolic: "refs/heads/main"},
			}, refs)

			// The same names, read without the table: the dangling ones stay
			// unseen, as for the other readers of git.
			refs, err = readGitRefs(context.Background(), dir)
			require.NoError(t, err)
			assert.NotContains(t, refs, "refs/heads/old")
			assert.NotContains(t, refs, "refs/heads/master")
		})
	}
}

// The certification's failure names the refs git holds otherwise than the
// table, with both values, for an operator to find the cause (#348).
func TestProjectionDiffNamesTheRefs(t *testing.T) {
	have := map[string]reftable.ProjectedRef{
		"HEAD":            {Symbolic: "refs/heads/trunk"},
		"refs/heads/main": {SHA: "1111"},
		"refs/heads/b":    {SHA: "2222"},
	}
	want := map[string]reftable.ProjectedRef{
		"HEAD":              {Symbolic: "refs/heads/trunk"},
		"refs/heads/main":   {SHA: "3333"},
		"refs/heads/master": {Symbolic: "refs/heads/main"},
	}
	assert.Equal(t,
		`"refs/heads/b": git "2222", table nothing; "refs/heads/main": git "1111", table "3333"; "refs/heads/master": git nothing, table "ref:refs/heads/main"`,
		projectionDiff(have, want))
	assert.Empty(t, projectionDiff(want, want))

	many := map[string]reftable.ProjectedRef{}
	for _, n := range []string{"e", "d", "c", "b", "a"} {
		many["refs/heads/"+n] = reftable.ProjectedRef{SHA: "1111"}
	}
	assert.Equal(t,
		`"refs/heads/a": git "1111", table nothing; "refs/heads/b": git "1111", table nothing; "refs/heads/c": git "1111", table nothing; and 2 more`,
		projectionDiff(many, nil))
}
