// Copyright 2026 The Forgejo Authors. All rights reserved.
// SPDX-License-Identifier: GPL-3.0-or-later

package cluster

import (
	"context"
	"fmt"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"forgejo.org/models/db"
	repo_model "forgejo.org/models/repo"
	"forgejo.org/models/unittest"
	user_model "forgejo.org/models/user"
	"forgejo.org/modules/clusterhook"
	"forgejo.org/modules/migration"
	"forgejo.org/modules/setting"
	"forgejo.org/services/cluster/reftable"
	repo_service "forgejo.org/services/repository"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// addTags writes n lightweight tags on the head of dir, without the hook.
func addTags(t *testing.T, dir string, n int) {
	t.Helper()
	head := run(t, dir, "rev-parse", "HEAD")
	var in strings.Builder
	for i := range n {
		fmt.Fprintf(&in, "create refs/tags/big-%05d %s\n", i, head)
	}
	cmd := exec.Command("git", "-c", "core.hooksPath=/dev/null", "update-ref", "--stdin")
	cmd.Dir = dir
	cmd.Stdin = strings.NewReader(in.String())
	out, err := cmd.CombinedOutput()
	require.NoError(t, err, "%s", out)
}

// liveRefs counts the live refs of key in a node's table.
func liveRefs(n *testNode, key string) int {
	count := 0
	refs, _ := n.r.table.Refs(key)
	for _, r := range refs {
		if !r.Tombstone {
			count++
		}
	}
	return count
}

// A fork or a migration of a repository with more refs than the cluster
// takes in one transaction: git 2.54 and later fetch, and every git mirror
// clone, write them all in one, which the cluster refused every time. The
// refs now come by batches, and every member holds them all (#80).
func TestHuntR4ProveBigTxnForkAndMirror(t *testing.T) {
	// As a cluster node's git config: no automatic maintenance, whose
	// pack-refs would rewrite the thousands of refs behind the projector.
	t.Setenv("GIT_CONFIG_COUNT", "2")
	t.Setenv("GIT_CONFIG_KEY_0", "gc.auto")
	t.Setenv("GIT_CONFIG_VALUE_0", "0")
	t.Setenv("GIT_CONFIG_KEY_1", "maintenance.auto")
	t.Setenv("GIT_CONFIG_VALUE_1", "false")
	require.NoError(t, unittest.PrepareTestDatabase())
	tc := newTestCluster(t, "node-a", "node-b", "node-c")
	for _, n := range tc.nodes {
		n.r.objects.repoKnown = n.r.repoKnown
	}
	origin := tc.nodes[0]
	baseDir := filepath.Join(origin.r.root, "user2", "repo1.git")
	require.NoError(t, unittest.CopyDir(filepath.Join(setting.RepoRootPath, "user2", "repo1.git"), baseDir))
	addTags(t, baseDir, reftable.MaxUpdatesPerTxn+100)
	want := len(strings.Fields(run(t, baseDir, "for-each-ref", "--format=%(refname)")))
	require.Greater(t, want, reftable.MaxUpdatesPerTxn)
	withGitThroughReplicator(t, origin)
	ctx := db.DefaultContext

	// The fork: the branches and the tags.
	base := unittest.AssertExistsAndLoadBean(t, &repo_model.Repository{ID: 1})
	require.NoError(t, base.LoadOwner(ctx))
	owner := unittest.AssertExistsAndLoadBean(t, &user_model.User{ID: 4})
	_, err := repo_service.ForkRepositoryIfNotExists(ctx, owner, owner, repo_service.ForkRepoOptions{BaseRepo: base, Name: "big-fork", Description: "fork"})
	require.NoError(t, err, "a fork of more refs than one transaction takes")
	forkWant := len(strings.Fields(run(t, baseDir, "for-each-ref", "--format=%(refname)", "refs/heads/", "refs/tags/")))
	for _, n := range tc.nodes {
		require.Eventually(t, func() bool { return liveRefs(n, "user4/big-fork.git") == forkWant+1 }, 30*time.Second, 50*time.Millisecond,
			"%s holds HEAD and every ref of the fork", n.slot)
	}

	// The migration of a mirror: every ref, and the mirror's remote.
	mirror := unittest.AssertExistsAndLoadBean(t, &repo_model.Repository{ID: 5})
	mirrorOwner := unittest.AssertExistsAndLoadBean(t, &user_model.User{ID: mirror.OwnerID})
	_, err = repo_service.MigrateRepositoryGitData(ctx, mirrorOwner, mirror, migration.MigrateOptions{
		CloneAddr: baseDir, RepoName: mirror.Name, Mirror: true, Releases: true,
	}, nil)
	require.NoError(t, err, "a mirror migration of more refs than one transaction takes")
	key := mirrorOwner.LowerName + "/" + mirror.LowerName + ".git"
	for _, n := range tc.nodes {
		require.Eventually(t, func() bool { return liveRefs(n, key) == want+1 }, 30*time.Second, 50*time.Millisecond,
			"%s holds HEAD and every ref of the mirror", n.slot)
	}
	mirrorDir := filepath.Join(origin.r.root, filepath.FromSlash(key))
	assert.Equal(t, baseDir, run(t, mirrorDir, "config", "--get", "remote.origin.url"))
	assert.Equal(t, "true", run(t, mirrorDir, "config", "--get", "remote.origin.mirror"))
	assert.Contains(t, run(t, mirrorDir, "config", "--get-all", "remote.origin.fetch"), "+refs/*:refs/*")
}

// A transaction past the bound is refused before its objects go anywhere,
// with a reason that names the bound (#80).
func TestOversizedTransactionIsRefusedEarly(t *testing.T) {
	tc := newTestCluster(t, "node-a", "node-b", "node-c")
	n := tc.nodes[0]
	work := t.TempDir()
	run(t, work, "init", "-q")
	c1 := commit(t, work, "one")
	dir := filepath.Join(n.r.root, "big", "txn.git")
	run(t, n.r.root, "init", "-q", "--bare", dir)
	lines := make([]clusterhook.Line, reftable.MaxUpdatesPerTxn+1)
	for i := range lines {
		lines[i] = clusterhook.Line{Old: strings.Repeat("0", 40), New: c1, Ref: fmt.Sprintf("refs/tags/t%05d", i)}
	}
	resp := n.r.Handle(context.Background(), &clusterhook.Request{Op: clusterhook.OpPrepared, Nonce: clusterhook.NewNonce(), GitDir: dir, Lines: lines})
	require.False(t, resp.OK)
	assert.Contains(t, resp.Message, fmt.Sprintf("at most %d", reftable.MaxUpdatesPerTxn))
}
