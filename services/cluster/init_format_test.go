// Copyright 2026 The Forgejo Authors. All rights reserved.
// SPDX-License-Identifier: GPL-3.0-or-later

package cluster

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"forgejo.org/modules/clusterhook"
	"forgejo.org/modules/git"
	"forgejo.org/services/cluster/reftable"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// A repository created as Forgejo creates one in a cluster, in sha256 or
// sha1: `git init --bare --object-format=...` runs in the final directory
// and git's reference-transaction hook hands HEAD to the replicator while
// the directory holds neither HEAD nor objects/ yet. The proposer asked
// `git rev-parse --show-object-format` there, which fails ("not a git
// repository"), and wrote the entry in sha1: the table held HEAD in sha1,
// the other members created the repository in sha1, and the first push
// of sha256 commits was refused at every attempt (#372). The format is now
// the one git writes the hook's lines in, the null id included, and every
// member holds the repository in its own format, which takes its first
// push.
//
// "create" is what CreateRepository, "Initialize repository" and a wiki
// run (git.InitRepository); "fork or migration" is initBareInCluster's
// command, which a fork and a migration run before they fetch the refs.
func TestARepositoryCreatedByGitInitIsHeldInItsObjectFormat(t *testing.T) {
	tc := newTestCluster(t, "node-a", "node-b", "node-c")
	raisePayload(t, tc)
	origin := tc.nodes[0]
	withGitThroughReplicator(t, origin)
	ctx := context.Background()

	cases := []struct {
		name, key, format string
		init              func(dir, format string) error
	}{
		{"create sha256", "o/r256.git", "sha256", func(dir, format string) error {
			return git.InitRepository(ctx, dir, true, format)
		}},
		{"fork or migration sha256", "o/fork256.git", "sha256", func(dir, format string) error {
			if err := os.MkdirAll(dir, os.ModePerm); err != nil {
				return err
			}
			_, _, err := git.NewCommand(ctx, "init", "--bare", "--template", "").
				AddOptionValues("--initial-branch", "main").AddOptionValues("--object-format", format).
				RunStdString(&git.RunOpts{Dir: dir})
			return err
		}},
		{"create sha1", "o/r1.git", "sha1", func(dir, format string) error {
			return git.InitRepository(ctx, dir, true, format)
		}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			dir := filepath.Join(origin.r.root, filepath.FromSlash(c.key))
			require.NoError(t, c.init(dir, c.format))
			requireHeldIn(t, tc, c.key, c.format)
			requireFirstPush(t, tc, origin, c.key, c.format)
		})
	}
}

// A key a proposer without the fix inscribed in sha1 for a sha256
// repository stays so, Apply refusing a HEAD of another format than the
// key's live one: deleting the repository and creating it again repairs
// it. The deletion tombstones its HEAD, and the new git init's HEAD is
// inscribed in the repository's format (#372).
func TestASha256RepositoryInscribedInSha1IsRepairedByItsRecreation(t *testing.T) {
	tc := newTestCluster(t, "node-a", "node-b", "node-c")
	raisePayload(t, tc)
	origin := tc.nodes[0]
	ctx := context.Background()
	key := "o/r.git"
	dir := filepath.Join(origin.r.root, "o", "r.git")

	// What a proposer without the fix wrote: git init's HEAD in sha1 for
	// a sha256 directory.
	run(t, origin.r.root, "init", "-q", "--bare", "--template=", "--object-format=sha256", dir)
	req := &clusterhook.Request{
		Op: clusterhook.OpPrepared, Nonce: clusterhook.NewNonce(), GitDir: dir,
		Lines: []clusterhook.Line{{Old: strings.Repeat("0", 40), New: "ref:refs/heads/main", Ref: "HEAD"}},
	}
	resp := origin.r.Handle(ctx, req)
	require.True(t, resp.OK, resp.Message)
	req.Op = clusterhook.OpCommitted
	origin.r.Handle(ctx, req)
	for _, n := range tc.nodes {
		require.Eventually(t, func() bool { v, ok := n.r.table.Get(key, "HEAD"); return ok && v.Format == "sha1" }, 10*time.Second, 20*time.Millisecond, n.slot)
	}

	withGitThroughReplicator(t, origin)
	settle(t, origin)
	require.NoError(t, os.RemoveAll(dir))
	require.NoError(t, origin.r.proposeRepoDelete(ctx, key, tc.repoID(key)))
	requireGoneEverywhere(t, tc, key)
	// The user's next action comes after the projectors removed the
	// deleted repository: one still at it would remove the new one.
	for _, n := range tc.nodes {
		settle(t, n)
	}
	require.NoError(t, git.InitRepository(ctx, dir, true, "sha256"))
	requireHeldIn(t, tc, key, "sha256")
	requireFirstPush(t, tc, origin, key, "sha256")
}

// requireHeldIn waits until every member's table holds key's HEAD and
// every other member projected it, and requires both in format, the
// format of the origin's directory.
func requireHeldIn(t *testing.T, tc *testCluster, key, format string) {
	t.Helper()
	require.Equal(t, format, run(t, filepath.Join(tc.nodes[0].r.root, filepath.FromSlash(key)), "rev-parse", "--show-object-format"), "the origin's directory")
	for _, n := range tc.nodes {
		require.Eventually(t, func() bool { v, ok := n.r.table.Get(key, "HEAD"); return ok && v.Live() }, 10*time.Second, 20*time.Millisecond, n.slot)
		head, _ := n.r.table.Get(key, "HEAD")
		assert.Equal(t, format, head.Format, "%s: the table's HEAD of %s", n.slot, key)
	}
	for _, n := range tc.nodes[1:] {
		require.Eventually(t, func() bool { return dirExists(n, key) }, 10*time.Second, 20*time.Millisecond, "%s projects %s", n.slot, key)
		assert.Equal(t, format, run(t, filepath.Join(n.r.root, filepath.FromSlash(key)), "rev-parse", "--show-object-format"),
			"%s: the directory of %s", n.slot, key)
	}
}

// requireFirstPush pushes a commit of format to refs/heads/main of key on
// origin, and requires every member to apply it.
func requireFirstPush(t *testing.T, tc *testCluster, origin *testNode, key, format string) {
	t.Helper()
	ctx := context.Background()
	dir := filepath.Join(origin.r.root, filepath.FromSlash(key))
	work := t.TempDir()
	run(t, work, "init", "-q", "--object-format="+format)
	c1 := commit(t, work, "one")
	// The objects land through a receive-pack that runs no hook, as
	// receive-pack's quarantine; the ref goes through the replicator.
	run(t, work, "push", "-q", "--receive-pack=git -c core.hooksPath=/dev/null receive-pack", dir, c1+":refs/tmp/incoming")
	run(t, dir, "update-ref", "-d", "refs/tmp/incoming")
	req := &clusterhook.Request{
		Op: clusterhook.OpPrepared, Nonce: clusterhook.NewNonce(), GitDir: dir, RepoID: tc.repoID(key), PusherID: 2,
		Lines: []clusterhook.Line{{Old: strings.Repeat("0", len(c1)), New: c1, Ref: "refs/heads/main"}},
	}
	unlock := holdRefLock(t, dir, "refs/heads/main", c1)
	resp := origin.r.Handle(ctx, req)
	unlock()
	req.Op = clusterhook.OpAborted
	if resp.OK {
		run(t, dir, "update-ref", "refs/heads/main", c1)
		req.Op = clusterhook.OpCommitted
	}
	origin.r.Handle(ctx, req)
	require.True(t, resp.OK, "the first push of %s: %s", key, resp.Message)
	for _, n := range tc.nodes {
		require.Eventually(t, func() bool { v, ok := n.r.table.Get(key, "refs/heads/main"); return ok && v.Live() && v.SHA == c1 },
			10*time.Second, 20*time.Millisecond, "%s: the first push of %s applied", n.slot, key)
	}
}

// The object format of a transaction whose ids say nothing of it, every
// line symbolic on both sides, is read in the repository's config, where
// git init writes it before HEAD; a directory whose format cannot be read
// is refused rather than written in sha1 (#372).
func TestTheObjectFormatOfATransactionIsTheRepositorys(t *testing.T) {
	r := &Replicator{table: reftable.New()}
	ctx := context.Background()
	symbolic := []clusterhook.Line{{Old: "ref:refs/heads/a", New: "ref:refs/heads/b", Ref: "HEAD"}}
	build := func(dir string, lines []clusterhook.Line) (*reftable.RefTxn, error) {
		tx, _, err := r.buildTxn(ctx, "o/x.git", &clusterhook.Request{Op: clusterhook.OpPrepared, GitDir: dir, Lines: lines})
		return tx, err
	}

	// A directory git init has not finished: its config, no HEAD.
	for _, format := range []string{"sha1", "sha256"} {
		dir := t.TempDir()
		run(t, dir, "init", "-q", "--bare", "--template=", "--object-format="+format)
		require.NoError(t, os.Remove(filepath.Join(dir, "HEAD")))
		tx, err := build(dir, symbolic)
		require.NoError(t, err)
		assert.Equal(t, format, tx.Format, "a transaction without ids in a %s repository", format)
		zero := strings.Repeat("0", map[string]int{"sha1": 40, "sha256": 64}[format])
		tx, err = build(dir, []clusterhook.Line{{Old: zero, New: "ref:refs/heads/main", Ref: "HEAD"}})
		require.NoError(t, err)
		assert.Equal(t, format, tx.Format, "git init's HEAD of a %s repository", format)
		assert.True(t, tx.Init, "git init's HEAD of a %s repository is a creation", format)
	}

	// No config: nothing tells the format.
	_, err := build(t.TempDir(), symbolic)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "nothing was written")
}
