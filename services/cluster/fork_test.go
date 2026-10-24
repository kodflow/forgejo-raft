// Copyright 2026 The Forgejo Authors. All rights reserved.
// SPDX-License-Identifier: GPL-3.0-or-later

package cluster

import (
	"bufio"
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"forgejo.org/models/db"
	repo_model "forgejo.org/models/repo"
	"forgejo.org/models/unittest"
	user_model "forgejo.org/models/user"
	"forgejo.org/modules/clusterhook"
	"forgejo.org/modules/git"
	"forgejo.org/modules/setting"
	"forgejo.org/modules/test"
	repo_service "forgejo.org/services/repository"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	testHookSocketEnv = "FORGEJO_CLUSTER_TEST_HOOK_SOCKET"
	testHookRootEnv   = "FORGEJO_CLUSTER_TEST_HOOK_ROOT"
)

// runTestHook is what `forgejo hook reference-transaction` does, run by the
// test binary: one phase of a git ref transaction handed to the replicator
// over its socket, failing closed in "prepared".
func runTestHook(sock string) int {
	if len(os.Args) < 2 || os.Args[1] == "preparing" {
		return 0
	}
	state := os.Args[1]
	gitDir := os.Getenv("GIT_DIR")
	wd, _ := os.Getwd()
	if gitDir == "" {
		gitDir = wd
	}
	if !filepath.IsAbs(gitDir) {
		gitDir = filepath.Join(wd, gitDir)
	}
	if real, err := filepath.EvalSymlinks(gitDir); err == nil {
		gitDir = real
	}
	if rel, err := filepath.Rel(os.Getenv(testHookRootEnv), gitDir); err != nil || strings.HasPrefix(rel, "..") {
		return 0
	}
	req := &clusterhook.Request{Op: state, Nonce: os.Getenv(clusterhook.EnvTxn), GitDir: gitDir, PID: os.Getppid(), CreateOnly: clusterhook.CreateOnlyFromEnv()}
	scanner := bufio.NewScanner(os.Stdin)
	for scanner.Scan() {
		if f := strings.Fields(scanner.Text()); len(f) == 3 {
			req.Lines = append(req.Lines, clusterhook.Line{Old: f[0], New: f[1], Ref: f[2]})
		}
	}
	if len(req.Lines) == 0 {
		return 0
	}
	ctx, cancel := context.WithTimeout(context.Background(), clusterhook.DefaultTimeout)
	defer cancel()
	resp, err := clusterhook.Call(ctx, sock, req)
	if state != clusterhook.OpPrepared {
		return 0
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, "cluster:", err)
		return 1
	}
	if !resp.OK {
		fmt.Fprintln(os.Stderr, "cluster:", resp.Message)
		return 1
	}
	return 0
}

// withGitThroughReplicator runs every git command of Forgejo through node
// n's replicator, as a node of a cluster does: the global hooks path gets
// the reference-transaction hook, Forgejo's git commands carry a nonce,
// deletions and renames go to the table, and the repository root is n's.
func withGitThroughReplicator(t *testing.T, n *testNode) {
	sockDir, err := os.MkdirTemp("", "fjh") //nolint:usetesting // a unix socket path must stay short: 104 bytes on macOS
	require.NoError(t, err)
	t.Cleanup(func() { _ = os.RemoveAll(sockDir) })
	sock := filepath.Join(sockDir, "s")
	require.NoError(t, n.r.serveHookSocket(sock))

	bin, err := os.Executable()
	require.NoError(t, err)
	hooks := filepath.Join(git.HomeDir(), "hooks")
	require.NoError(t, os.MkdirAll(hooks, 0o755))
	hook := filepath.Join(hooks, "reference-transaction")
	_, err = os.Stat(hook)
	require.True(t, os.IsNotExist(err), "a reference-transaction hook is already installed")
	script := fmt.Sprintf("#!/bin/sh\n%s='%s' %s='%s' exec '%s' \"$@\"\n", testHookSocketEnv, sock, testHookRootEnv, n.r.root, bin)
	require.NoError(t, os.WriteFile(hook, []byte(script), 0o755))
	t.Cleanup(func() { _ = os.Remove(hook) })

	clusterhook.SetEnabled(true)
	clusterhook.SetRepoLifecycle(n.r.proposeRepoDelete, n.r.proposeRepoMoves)
	t.Cleanup(func() {
		clusterhook.SetEnabled(false)
		clusterhook.SetRepoLifecycle(nil, nil)
	})
	t.Cleanup(test.MockVariableValue(&setting.RepoRootPath, n.r.root))
}

// A fork in a cluster: the fork's row is not committed while git writes
// its refs, so the other members know its key from the table alone. A
// `git clone` proposes every ref first, and the members refuse the objects
// of a key they do not know: the fork failed every time (R-D3). HEAD alone
// goes first, then the refs, and every member holds the fork.
func TestForkInACluster(t *testing.T) {
	require.NoError(t, unittest.PrepareTestDatabase())
	tc := newTestCluster(t, "node-a", "node-b", "node-c")
	for _, n := range tc.nodes {
		n.r.objects.repoKnown = n.r.repoKnown
	}
	origin := tc.nodes[0]
	// The base repository is on the origin, where the fixtures are.
	require.NoError(t, unittest.CopyDir(filepath.Join(setting.RepoRootPath, "user2", "repo1.git"), filepath.Join(origin.r.root, "user2", "repo1.git")))
	withGitThroughReplicator(t, origin)

	ctx := db.DefaultContext
	base := unittest.AssertExistsAndLoadBean(t, &repo_model.Repository{ID: 1})
	require.NoError(t, base.LoadOwner(ctx))
	owner := unittest.AssertExistsAndLoadBean(t, &user_model.User{ID: 4})
	_, err := repo_service.ForkRepositoryIfNotExists(ctx, owner, owner, repo_service.ForkRepoOptions{BaseRepo: base, Name: "repo1-fork", Description: "fork"})
	require.NoError(t, err)

	key := "user4/repo1-fork.git"
	want, err := readGitRefs(ctx, filepath.Join(origin.r.root, "user2", "repo1.git"))
	require.NoError(t, err)
	require.NotEmpty(t, want)
	for _, n := range tc.nodes {
		head, ok := n.r.table.Get(key, "HEAD")
		require.True(t, ok, "%s: HEAD in the table", n.slot)
		assert.Equal(t, "refs/heads/"+base.DefaultBranch, head.Symbolic, n.slot)
		for ref, v := range want {
			if !strings.HasPrefix(ref, "refs/heads/") && !strings.HasPrefix(ref, "refs/tags/") {
				continue
			}
			got, ok := n.r.table.Get(key, ref)
			require.True(t, ok, "%s: %s in the table", n.slot, ref)
			assert.Equal(t, v.SHA, got.SHA, "%s: %s", n.slot, ref)
			require.Eventually(t, func() bool { return gitRef(t, n, key, ref) == v.SHA }, 10*time.Second, 20*time.Millisecond,
				"%s: %s projected", n.slot, ref)
		}
	}
	_, err = os.Stat(filepath.Join(origin.r.root, "user4", "repo1-fork.git", "FETCH_HEAD"))
	assert.True(t, os.IsNotExist(err), "no FETCH_HEAD left behind")
}
