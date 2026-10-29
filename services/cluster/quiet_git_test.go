// Copyright 2026 The Forgejo Authors. All rights reserved.
// SPDX-License-Identifier: GPL-3.0-or-later

package cluster

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestARefusedRepositoryRenameMovesItsDirectoryBack failed in CI under git
// 2.56, at the cleanup of t.TempDir: "unlinkat .../alice/r.git/objects:
// directory not empty", then ".../objects/pack" (#347). Every git fetch,
// commit or push starts "git maintenance run --auto --detach", which goes
// on after the command returned, in the repository of the test, and
// writes under its objects/: its lock, and the packs it rewrites, which
// git 2.54's default strategy, geometric repacking, does once two packs
// break the progression, as the projector's two fetches leave them. Git
// 2.56 turned the detaching off in its own suite for races of that kind
// (its release notes). A cluster node's git home turns that maintenance
// off (modules/git/git.go, syncGitConfig); the suite's did not.
//
// The bench's git now reads quietGitConfig, as the projector's fetch runs
// it (gitRun) and as the tests run theirs (gitIn): a fetch, traced, starts
// no maintenance. Before, it started one with any git.
func TestTheBenchsGitStartsNoMaintenance(t *testing.T) {
	ctx := t.Context()
	work := t.TempDir()
	run(t, work, "init", "-q")
	commit(t, work, "one")
	dir := t.TempDir()
	run(t, dir, "init", "-q", "--bare")
	for _, kv := range quietGitConfig {
		got, err := gitRun(ctx, dir, nil, "config", "--get", kv[0])
		require.NoError(t, err, "the projector's git reads %s", kv[0])
		assert.Equal(t, kv[1], strings.TrimSpace(got), "%s, as the projector's git reads it", kv[0])
		got, err = gitIn(dir, "config", "--get", kv[0])
		require.NoError(t, err, "the tests' git reads %s", kv[0])
		assert.Equal(t, kv[1], got, "%s, as the tests' git reads it", kv[0])
	}

	trace := filepath.Join(t.TempDir(), "trace")
	t.Setenv("GIT_TRACE", trace)
	require.NoError(t, gitFetchInto(ctx, work, dir), "the projector's fetch of the bench")
	out, err := os.ReadFile(trace)
	require.NoError(t, err)
	require.Contains(t, string(out), "fetch", "the fetch is traced")
	assert.NotContains(t, string(out), "maintenance", "the fetch starts no maintenance")
}
