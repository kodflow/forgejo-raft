// Copyright 2026 The Forgejo Authors. All rights reserved.
// SPDX-License-Identifier: GPL-3.0-or-later

package observer

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMain(m *testing.M) {
	if err := quietGit(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	os.Exit(m.Run())
}

// quietGitConfig turns git's automatic maintenance off, as a cluster
// node's git home has it (modules/git/git.go, syncGitConfig), and
// maintenance.autoDetach for whatever git would still maintain, in the
// foreground.
var quietGitConfig = [][2]string{{"gc.auto", "0"}, {"maintenance.auto", "false"}, {"maintenance.autoDetach", "false"}, {"receive.autogc", "false"}}

// quietGit gives quietGitConfig to every git the tests run, through the
// environment they inherit (GIT_CONFIG_COUNT, after the entries already
// there): the observer's git has no home of its own, gitStore.run runs it
// in the environment of the process, and the fake cluster's runs under a
// HOME of each test's. Otherwise git starts "git maintenance run --auto
// --detach" after a fetch, Materialize's into a directory of the test
// among them, or a commit: it goes on after the command returned, and
// writes under objects/ while the cleanup of t.TempDir removes it,
// "directory not empty" (#347).
func quietGit() error {
	n := 0
	if s, ok := os.LookupEnv("GIT_CONFIG_COUNT"); ok {
		var err error
		if n, err = strconv.Atoi(s); err != nil {
			return fmt.Errorf("GIT_CONFIG_COUNT=%q: %w", s, err)
		}
	}
	for _, kv := range quietGitConfig {
		if err := os.Setenv(fmt.Sprintf("GIT_CONFIG_KEY_%d", n), kv[0]); err != nil {
			return err
		}
		if err := os.Setenv(fmt.Sprintf("GIT_CONFIG_VALUE_%d", n), kv[1]); err != nil {
			return err
		}
		n++
	}
	return os.Setenv("GIT_CONFIG_COUNT", strconv.Itoa(n))
}

// The tests' git, as the observer runs it (gitStore.run), reads
// quietGitConfig, and Materialize's fetch, traced, starts no maintenance
// (#347). Before, every fetch started one, detached, which went on in the
// directory of the test after the fetch returned.
func TestTheTestsGitStartsNoMaintenance(t *testing.T) {
	ctx := context.Background()
	c := newFakeCluster(t)
	g := gitStore{root: t.TempDir(), git: "git"}
	src, err := g.ensure(ctx, "alice/demo.git", "sha1")
	require.NoError(t, err)
	one := c.commit("one")
	c.git(src, "fetch", "-q", c.work, "HEAD")
	const cp = "c1050000000000000000000000000000-1-00000000000000000006"
	c.git(src, "update-ref", RefNamespace(cp)+"refs/heads/main", one)
	for _, kv := range quietGitConfig {
		got, err := g.run(ctx, src, nil, "config", "--get", kv[0])
		require.NoError(t, err, "the observer's git reads %s", kv[0])
		assert.Equal(t, kv[1], strings.TrimSpace(got), kv[0])
	}

	trace := filepath.Join(t.TempDir(), "trace")
	t.Setenv("GIT_TRACE", trace)
	target := filepath.Join(t.TempDir(), "alice", "demo.git")
	require.NoError(t, g.Materialize(ctx, "alice/demo.git", cp, target, "sha1", true, nil))
	out, err := os.ReadFile(trace)
	require.NoError(t, err)
	require.Contains(t, string(out), "fetch", "the fetch is traced")
	assert.NotContains(t, string(out), "maintenance", "the fetch starts no maintenance")
}
