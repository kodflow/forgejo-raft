// Copyright 2026 The Forgejo Authors. All rights reserved.
// SPDX-License-Identifier: GPL-3.0-or-later

package cluster

import (
	"os"
	"strings"
	"testing"
	"time"

	"forgejo.org/models"
	"forgejo.org/models/db"
	git_model "forgejo.org/models/git"
	repo_model "forgejo.org/models/repo"
	"forgejo.org/models/unittest"
	user_model "forgejo.org/models/user"
	"forgejo.org/modules/clusterhook"
	"forgejo.org/modules/gitrepo"
	release_service "forgejo.org/services/release"
	repo_service "forgejo.org/services/repository"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// laggingRepo is the fixtures' repository 1, which the services below
// read from SQL.
const laggingRepo = "user2/repo1.git"

// laggingCluster builds a three-node cluster holding laggingRepo with main
// at c2 and lag-from at c1 on every node, then makes node-a stop
// projecting it: a registered transaction of a live process fences its
// projector on that repository, as a slow fetch, a projector busy
// elsewhere or a restart would. node-a's table keeps applying the
// entries; only its git lags, until release(hold).
func laggingCluster(t *testing.T) (tc *testCluster, a, b *testNode, work, c1, c2, hold string) {
	t.Helper()
	require.NoError(t, unittest.PrepareTestDatabase())
	tc = newTestCluster(t, "node-a", "node-b", "node-c")
	a, b = tc.nodes[0], tc.nodes[1]
	work = t.TempDir()
	run(t, work, "init", "-q")
	c1 = commit(t, work, "one")
	c2 = commit(t, work, "two")
	zero := strings.Repeat("0", 40)
	require.Equal(t, int64(1), tc.repoID(laggingRepo))
	require.True(t, tc.pushVia(a, work, laggingRepo, "refs/heads/main", zero, c2).OK)
	require.True(t, tc.pushVia(a, work, laggingRepo, "refs/heads/lag-from", zero, c1).OK)
	for _, m := range tc.nodes {
		require.Eventually(t, func() bool {
			return gitRef(t, m, laggingRepo, "refs/heads/main") == c2 && gitRef(t, m, laggingRepo, "refs/heads/lag-from") == c1
		}, 15*time.Second, 20*time.Millisecond, "%s: precondition", m.slot)
	}
	hold = clusterhook.NewNonce()
	require.NoError(t, a.r.registry.register(hold, laggingRepo, nil, os.Getpid(), false))
	return tc, a, b, work, c1, c2, hold
}

// pushOnB pushes ref from old to sha through node-b, acknowledged, and
// waits until node-a's table applied it while node-a's git does not show
// it.
func pushOnB(t *testing.T, tc *testCluster, a, b *testNode, work, ref, old, sha string) {
	t.Helper()
	before := gitRef(t, a, laggingRepo, ref)
	resp := tc.pushVia(b, work, laggingRepo, ref, old, sha)
	require.True(t, resp.OK, resp.Message)
	require.Eventually(t, func() bool {
		v, ok := a.r.table.Get(laggingRepo, ref)
		return ok && v.SHA == sha
	}, 10*time.Second, 20*time.Millisecond, "node-a applied the push")
	require.Equal(t, before, gitRef(t, a, laggingRepo, ref), "node-a has not projected it")
}

// freshFrom makes Forgejo's freshness checks ask node n, as they ask the
// replicator of the node that serves the request.
func freshFrom(t *testing.T, n *testNode) {
	t.Helper()
	clusterhook.SetFreshWaiter(n.r.WaitFresh)
	t.Cleanup(func() { clusterhook.SetFreshWaiter(nil) })
}

// caughtUp releases node-a's projection and waits until its git shows ref
// at sha.
func caughtUp(t *testing.T, a *testNode, hold, ref, sha string) {
	t.Helper()
	a.r.registry.release(hold)
	require.Eventually(t, func() bool { return gitRef(t, a, laggingRepo, ref) == sha }, 15*time.Second, 20*time.Millisecond, "node-a catches up")
}

// tableAt checks that every node's table holds ref at sha.
func tableAt(t *testing.T, tc *testCluster, ref, sha, msg string) {
	t.Helper()
	for _, m := range tc.nodes {
		v, ok := m.r.table.Get(laggingRepo, ref)
		assert.True(t, ok && v.Live(), "%s: %s: %s is not live", m.slot, msg, ref)
		assert.Equal(t, sha, v.SHA, "%s: %s", m.slot, msg)
	}
}

// A tag pushed through node-b is acknowledged at c1. Before the consumer
// derives its release row, the same tag is created through the API
// (release_service.CreateNewTag, as POST /repos/{owner}/{repo}/tags does),
// served by node-a, which has not projected it. createTag saw no tag on
// node-a's git and ran `git tag`, whose creation (a null old value and no
// pusher) buildTxn proposed with the table's revision: Apply overwrote the
// pushed tag for the whole cluster (#189). Now the creation waits for
// node-a to serve the repository at the cluster's commit index, and is
// refused while it does not; without that wait, the cluster refuses the
// creation of a tag it holds, which Forgejo reports as a tag that exists.
func TestATagCreatedOnALaggingNodeKeepsAPushedTag(t *testing.T) {
	tc, a, b, work, c1, c2, hold := laggingCluster(t)
	ref := "refs/tags/lag-g1"
	pushOnB(t, tc, a, b, work, ref, strings.Repeat("0", 40), c1)
	ctx := db.DefaultContext
	has, err := repo_model.IsReleaseExist(ctx, 1, "lag-g1")
	require.NoError(t, err)
	require.False(t, has, "the consumer has not derived the push yet")

	withGitThroughReplicator(t, a)
	sqlRepo := unittest.AssertExistsAndLoadBean(t, &repo_model.Repository{ID: 1})
	doer := unittest.AssertExistsAndLoadBean(t, &user_model.User{ID: 2})

	t.Run("node-a waits to serve the repository", func(t *testing.T) {
		freshFrom(t, a)
		err := release_service.CreateNewTag(ctx, doer, sqlRepo, c2, "lag-g1", "")
		require.ErrorIs(t, err, clusterhook.ErrBehind)
		tableAt(t, tc, ref, c1, "the tag pushed and acknowledged at c1 is kept")
		unittest.AssertNotExistsBean(t, &repo_model.Release{RepoID: 1, TagName: "lag-g1"})
	})

	t.Run("the cluster refuses the creation", func(t *testing.T) {
		err := release_service.CreateNewTag(ctx, doer, sqlRepo, c2, "lag-g1", "")
		require.Error(t, err)
		assert.True(t, models.IsErrTagAlreadyExists(err), "%v", err)
		tableAt(t, tc, ref, c1, "the tag pushed and acknowledged at c1 is kept")
		unittest.AssertNotExistsBean(t, &repo_model.Release{RepoID: 1, TagName: "lag-g1"})
	})

	caughtUp(t, a, hold, ref, c1)
	freshFrom(t, a)
	require.NoError(t, release_service.CreateNewTag(ctx, doer, sqlRepo, c2, "lag-g1", ""))
	tableAt(t, tc, ref, c1, "a tag that exists is not moved")
	rel := unittest.AssertExistsAndLoadBean(t, &repo_model.Release{RepoID: 1, TagName: "lag-g1"})
	assert.Equal(t, c1, rel.Sha1)
}

// The usual release automation: a tag is pushed (acknowledged at c1, its
// tag-only release row derived), then a release is created for it through
// the API (POST /repos/{owner}/{repo}/releases with tag_name and
// target_commitish main: the handler turns the tag-only row into a release
// and calls UpdateRelease), served by node-a, which has not projected the
// tag. createTag saw no tag on node-a's git and tagged main, c2: the
// pushed tag moved for the whole cluster (#189). Now the release waits
// for node-a to show the tag, and is built on it.
func TestAReleaseOfAPushedTagOnALaggingNodeKeepsTheTag(t *testing.T) {
	tc, a, b, work, c1, _, hold := laggingCluster(t)
	ref := "refs/tags/lag-g4"
	pushOnB(t, tc, a, b, work, ref, strings.Repeat("0", 40), c1)
	ctx := db.DefaultContext
	require.NoError(t, db.Insert(ctx, &repo_model.Release{RepoID: 1, PublisherID: 2, TagName: "lag-g4", LowerTagName: "lag-g4", Sha1: c1, IsTag: true, NumCommits: 1}))

	withGitThroughReplicator(t, a)
	freshFrom(t, a)
	sqlRepo := unittest.AssertExistsAndLoadBean(t, &repo_model.Repository{ID: 1})
	doer := unittest.AssertExistsAndLoadBean(t, &user_model.User{ID: 2})
	gitRepo, err := gitrepo.OpenRepository(ctx, sqlRepo)
	require.NoError(t, err)
	defer gitRepo.Close()
	// What routers/api/v1/repo/release.go CreateRelease does with an
	// existing tag-only row.
	update := func() error {
		rel, err := repo_model.GetRelease(ctx, 1, "lag-g4")
		require.NoError(t, err)
		rel.Title, rel.IsTag, rel.Repo, rel.Publisher, rel.PublisherID, rel.Target = "v4", false, sqlRepo, doer, doer.ID, "main"
		return release_service.UpdateRelease(ctx, doer, gitRepo, rel, true, nil)
	}
	require.ErrorIs(t, update(), clusterhook.ErrBehind)
	tableAt(t, tc, ref, c1, "the pushed tag acknowledged at c1 is kept")
	rel, err := repo_model.GetRelease(ctx, 1, "lag-g4")
	require.NoError(t, err)
	assert.True(t, rel.IsTag, "the tag-only row is left as it was")

	caughtUp(t, a, hold, ref, c1)
	require.NoError(t, update())
	tableAt(t, tc, ref, c1, "the release is built on the pushed tag")
	rel, err = repo_model.GetRelease(ctx, 1, "lag-g4")
	require.NoError(t, err)
	assert.False(t, rel.IsTag)
	assert.Equal(t, c1, rel.Sha1)
}

// A branch pushed through node-b is acknowledged at c2. Before the
// consumer writes its row, a user renames another branch to that name
// (repo_service.RenameBranch, the web form and the API), served by node-a,
// which has not projected it: IsBranchExist(to) read node-a's git, the SQL
// check found no row, and update-ref's create (a null old value, no
// pusher) overwrote the pushed branch in the table (#189). Now the rename
// waits for node-a to serve the repository, and without that wait the
// cluster refuses the creation: the target exists.
func TestABranchRenamedOnALaggingNodeKeepsAPushedBranch(t *testing.T) {
	tc, a, b, work, c1, c2, hold := laggingCluster(t)
	ref := "refs/heads/lag-to"
	ctx := db.DefaultContext
	require.NoError(t, db.Insert(ctx, &git_model.Branch{RepoID: 1, Name: "lag-from", CommitID: c1, CommitMessage: "one", PusherID: 2}))
	pushOnB(t, tc, a, b, work, ref, strings.Repeat("0", 40), c2)

	withGitThroughReplicator(t, a)
	sqlRepo := unittest.AssertExistsAndLoadBean(t, &repo_model.Repository{ID: 1})
	doer := unittest.AssertExistsAndLoadBean(t, &user_model.User{ID: 2})
	gitRepo, err := gitrepo.OpenRepository(ctx, sqlRepo)
	require.NoError(t, err)
	defer gitRepo.Close()

	t.Run("node-a waits to serve the repository", func(t *testing.T) {
		freshFrom(t, a)
		msg, err := repo_service.RenameBranch(ctx, sqlRepo, doer, gitRepo, "lag-from", "lag-to")
		require.ErrorIs(t, err, clusterhook.ErrBehind)
		assert.Empty(t, msg)
	})

	t.Run("the cluster refuses the new name", func(t *testing.T) {
		msg, err := repo_service.RenameBranch(ctx, sqlRepo, doer, gitRepo, "lag-from", "lag-to")
		require.NoError(t, err)
		assert.Equal(t, "target_exist", msg)
	})

	tableAt(t, tc, ref, c2, "the branch pushed and acknowledged at c2 is kept")
	tableAt(t, tc, "refs/heads/lag-from", c1, "the branch renamed is kept")
	unittest.AssertExistsAndLoadBean(t, &git_model.Branch{RepoID: 1, Name: "lag-from"})
	caughtUp(t, a, hold, ref, c2)
}

// A creation that declares itself one, on a ref the table does not hold,
// is written as before, on a node behind or not: a tag created through the
// API, and a branch renamed to a new name.
func TestACreationOfARefAbsentFromTheClusterIsWritten(t *testing.T) {
	tc, a, _, _, c1, c2, hold := laggingCluster(t)
	ctx := db.DefaultContext
	require.NoError(t, db.Insert(ctx, &git_model.Branch{RepoID: 1, Name: "lag-from", CommitID: c1, CommitMessage: "one", PusherID: 2}))
	withGitThroughReplicator(t, a)
	sqlRepo := unittest.AssertExistsAndLoadBean(t, &repo_model.Repository{ID: 1})
	doer := unittest.AssertExistsAndLoadBean(t, &user_model.User{ID: 2})

	require.NoError(t, release_service.CreateNewTag(ctx, doer, sqlRepo, c2, "lag-new", ""))
	gitRepo, err := gitrepo.OpenRepository(ctx, sqlRepo)
	require.NoError(t, err)
	defer gitRepo.Close()
	msg, err := repo_service.RenameBranch(ctx, sqlRepo, doer, gitRepo, "lag-from", "lag-renamed")
	require.NoError(t, err)
	require.Empty(t, msg)
	a.r.registry.release(hold)
	for _, m := range tc.nodes {
		require.Eventually(t, func() bool {
			tag, _ := m.r.table.Get(laggingRepo, "refs/tags/lag-new")
			to, _ := m.r.table.Get(laggingRepo, "refs/heads/lag-renamed")
			from, _ := m.r.table.Get(laggingRepo, "refs/heads/lag-from")
			return tag.Live() && tag.SHA == c2 && to.Live() && to.SHA == c1 && from.Tombstone
		}, 10*time.Second, 20*time.Millisecond, "%s: the tag and the renamed branch are in the cluster", m.slot)
	}
	rel := unittest.AssertExistsAndLoadBean(t, &repo_model.Release{RepoID: 1, TagName: "lag-new"})
	assert.Equal(t, c2, rel.Sha1)
}
