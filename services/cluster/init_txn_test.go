// Copyright 2026 The Forgejo Authors. All rights reserved.
// SPDX-License-Identifier: GPL-3.0-or-later

package cluster

import (
	"path/filepath"
	"strings"
	"testing"

	"forgejo.org/services/cluster/reftable"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// git gives git init's HEAD and a change of the default branch the same
// zero old value: the directory tells them apart (#136).
func TestInitsRepoTellsInitFromHeadChange(t *testing.T) {
	ctx := t.Context()
	dir := filepath.Join(t.TempDir(), "r.git")
	_, err := gitRun(ctx, t.TempDir(), nil, "init", "-q", "--bare", dir)
	require.NoError(t, err)
	head := []reftable.RefUpdate{{Ref: "HEAD", Symbolic: "refs/heads/dev"}}

	assert.True(t, initsRepo(ctx, dir, head), "an empty directory: git init")

	tree, err := gitRun(ctx, dir, nil, "mktree")
	require.NoError(t, err)
	commit, err := gitRun(ctx, dir, nil, "-c", "user.name=t", "-c", "user.email=t@t", "commit-tree", "-m", "c", strings.TrimSpace(tree))
	require.NoError(t, err)
	_, err = gitRun(ctx, dir, nil, "update-ref", "refs/heads/main", strings.TrimSpace(commit))
	require.NoError(t, err)

	assert.False(t, initsRepo(ctx, dir, head), "a repository with refs: a change of HEAD")
	assert.False(t, initsRepo(ctx, t.TempDir(), []reftable.RefUpdate{{Ref: "refs/heads/main", New: strings.TrimSpace(commit)}}), "not HEAD alone")
	assert.False(t, initsRepo(ctx, dir, nil))
}
