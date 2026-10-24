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
	issues_model "forgejo.org/models/issues"
	repo_model "forgejo.org/models/repo"
	"forgejo.org/models/unittest"
	user_model "forgejo.org/models/user"
	"forgejo.org/modules/clusterhook"
	"forgejo.org/modules/gitrepo"
	repo_module "forgejo.org/modules/repository"
	pull_service "forgejo.org/services/pull"
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

// freshFrom makes Forgejo's freshness checks and table lookups ask node n,
// as they ask the replicator of the node that serves the request.
func freshFrom(t *testing.T, n *testNode) {
	t.Helper()
	clusterhook.SetFreshWaiter(n.r.WaitFresh)
	clusterhook.SetRefLookup(n.r.refLive)
	t.Cleanup(func() {
		clusterhook.SetFreshWaiter(nil)
		clusterhook.SetRefLookup(nil)
	})
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

// A tag the cluster holds, and its release row, is deleted through the API
// (DELETE /repos/{owner}/{repo}/tags/{tag} calls DeleteReleaseByID with
// delTag), served by node-a, which has not projected the tag yet: `git tag
// -d` failed "not found" on this node, which DeleteReleaseByID took for a
// tag already gone. The row went and the answer was 204, and nothing
// reached the cluster: the tag stayed on every node (#190). Now the
// deletion is refused while node-a does not show the tag, and goes through
// the cluster once it does. A tag that no node holds is still taken for
// gone.
func TestATagDeletedOnALaggingNodeGoesThroughTheCluster(t *testing.T) {
	tc, a, b, work, c1, _, hold := laggingCluster(t)
	ref := "refs/tags/lag-g2"
	pushOnB(t, tc, a, b, work, ref, strings.Repeat("0", 40), c1)
	ctx := db.DefaultContext
	rel := &repo_model.Release{RepoID: 1, PublisherID: 2, TagName: "lag-g2", LowerTagName: "lag-g2", Sha1: c1, IsTag: true, NumCommits: 1}
	require.NoError(t, db.Insert(ctx, rel))

	withGitThroughReplicator(t, a)
	sqlRepo := unittest.AssertExistsAndLoadBean(t, &repo_model.Repository{ID: 1})
	doer := unittest.AssertExistsAndLoadBean(t, &user_model.User{ID: 2})

	t.Run("node-a waits to serve the repository", func(t *testing.T) {
		freshFrom(t, a)
		require.ErrorIs(t, release_service.DeleteReleaseByID(ctx, sqlRepo, rel, doer, true), clusterhook.ErrBehind)
		unittest.AssertExistsAndLoadBean(t, &repo_model.Release{ID: rel.ID})
		tableAt(t, tc, ref, c1, "nothing was deleted")
	})

	t.Run("the table holds the tag", func(t *testing.T) {
		clusterhook.SetRefLookup(a.r.refLive)
		t.Cleanup(func() { clusterhook.SetRefLookup(nil) })
		require.ErrorIs(t, release_service.DeleteReleaseByID(ctx, sqlRepo, rel, doer, true), clusterhook.ErrBehind)
		unittest.AssertExistsAndLoadBean(t, &repo_model.Release{ID: rel.ID})
		tableAt(t, tc, ref, c1, "nothing was deleted")
	})

	caughtUp(t, a, hold, ref, c1)
	freshFrom(t, a)
	require.NoError(t, release_service.DeleteReleaseByID(ctx, sqlRepo, rel, doer, true))
	unittest.AssertNotExistsBean(t, &repo_model.Release{ID: rel.ID})
	for _, m := range tc.nodes {
		require.Eventually(t, func() bool {
			v, _ := m.r.table.Get(laggingRepo, ref)
			return v.Tombstone
		}, 10*time.Second, 20*time.Millisecond, "%s: the tag is deleted in the cluster", m.slot)
	}

	// A row whose tag no node holds goes, as before.
	gone := &repo_model.Release{RepoID: 1, PublisherID: 2, TagName: "lag-nowhere", LowerTagName: "lag-nowhere", Sha1: c1, IsTag: true, NumCommits: 1}
	require.NoError(t, db.Insert(ctx, gone))
	require.NoError(t, release_service.DeleteReleaseByID(ctx, sqlRepo, gone, doer, true))
	unittest.AssertNotExistsBean(t, &repo_model.Release{ID: gone.ID})
}

// The site administration's "Synchronise branches" and "Synchronise tags"
// (routers/web/admin/admin.go, sync_repo_branches and sync_repo_tags) run
// on the node that served the request, from its own git. On node-a, which
// has not projected a branch and a tag pushed through node-b (their rows
// already derived), the branch row was marked deleted and the published
// release turned into a draft (#191). Now a repository node-a is behind on
// is left as it is, and synchronised once node-a serves it.
func TestAnAdminSyncOnALaggingNodeLeavesTheRepositoryAsItIs(t *testing.T) {
	tc, a, b, work, c1, _, hold := laggingCluster(t)
	ctx := db.DefaultContext
	zero := strings.Repeat("0", 40)
	pushOnB(t, tc, a, b, work, "refs/heads/lag-new", zero, c1)
	pushOnB(t, tc, a, b, work, "refs/tags/lag-g3", zero, c1)
	// What the consumer derived for the branch, and the release published
	// on that tag through node-b.
	require.NoError(t, db.Insert(ctx, &git_model.Branch{RepoID: 1, Name: "lag-new", CommitID: c1, CommitMessage: "one", PusherID: 2}))
	rel := &repo_model.Release{RepoID: 1, PublisherID: 2, TagName: "lag-g3", LowerTagName: "lag-g3", Sha1: c1, Title: "release", NumCommits: 1}
	require.NoError(t, db.Insert(ctx, rel))

	withGitThroughReplicator(t, a)
	freshFrom(t, a)
	_, err := repo_module.SyncRepoBranchesFresh(ctx, 1, 0)
	require.ErrorIs(t, err, clusterhook.ErrBehind)
	require.ErrorIs(t, repo_module.SyncRepoTags(ctx, 1), clusterhook.ErrBehind)
	br, err := git_model.GetBranch(ctx, 1, "lag-new")
	require.NoError(t, err)
	assert.False(t, br.IsDeleted, "a live branch is not marked deleted")
	got := unittest.AssertExistsAndLoadBean(t, &repo_model.Release{ID: rel.ID})
	assert.False(t, got.IsDraft, "a published release on a live tag is not turned into a draft")

	caughtUp(t, a, hold, "refs/heads/lag-new", c1)
	require.Eventually(t, func() bool { return gitRef(t, a, laggingRepo, "refs/tags/lag-g3") == c1 }, 15*time.Second, 20*time.Millisecond)
	_, err = repo_module.SyncRepoBranchesFresh(ctx, 1, 0)
	require.NoError(t, err)
	require.NoError(t, repo_module.SyncRepoTags(ctx, 1))
	br, err = git_model.GetBranch(ctx, 1, "lag-new")
	require.NoError(t, err)
	assert.False(t, br.IsDeleted)
	got = unittest.AssertExistsAndLoadBean(t, &repo_model.Release{ID: rel.ID})
	assert.False(t, got.IsDraft)
	assert.Equal(t, c1, got.Sha1)
}

// A pull request created on node-a right after a push to its head branch
// through node-b: NewPullRequest read the head from node-a's git, which
// had not projected the push, and wrote refs/pull/N/head at the older
// commit for the whole cluster. The push's own update of the pull
// requests passes over one created after it (TestPullRequest), and the
// pull request stayed broken until the next push (#191). Now the creation
// waits for node-a to serve the repository, and is refused while it does
// not.
func TestAPullRequestCreatedOnALaggingNodeHasThePushedHead(t *testing.T) {
	tc, a, b, work, c1, _, hold := laggingCluster(t)
	ctx := db.DefaultContext
	zero := strings.Repeat("0", 40)
	head := "refs/heads/lag-feat"
	// The head branch at c1 on every node, then at c3 through node-b.
	a.r.registry.release(hold)
	require.True(t, tc.pushVia(a, work, laggingRepo, head, zero, c1).OK)
	for _, m := range tc.nodes {
		require.Eventually(t, func() bool { return gitRef(t, m, laggingRepo, head) == c1 }, 15*time.Second, 20*time.Millisecond, "%s", m.slot)
	}
	hold = clusterhook.NewNonce()
	require.NoError(t, a.r.registry.register(hold, laggingRepo, nil, os.Getpid(), false))
	c3 := commit(t, work, "three")
	pushOnB(t, tc, a, b, work, head, c1, c3)

	withGitThroughReplicator(t, a)
	freshFrom(t, a)
	// The cluster's repository has no master, the fixtures' default branch,
	// which the review requests of a pull request read.
	_, err := db.GetEngine(ctx).ID(1).Cols("default_branch").Update(&repo_model.Repository{DefaultBranch: "main"})
	require.NoError(t, err)
	sqlRepo := unittest.AssertExistsAndLoadBean(t, &repo_model.Repository{ID: 1})
	doer := unittest.AssertExistsAndLoadBean(t, &user_model.User{ID: 2})
	open := func() (*issues_model.PullRequest, error) {
		issue := &issues_model.Issue{RepoID: 1, Title: "lagging head", PosterID: doer.ID, Poster: doer, IsPull: true}
		pr := &issues_model.PullRequest{
			HeadRepoID: 1, BaseRepoID: 1, HeadBranch: "lag-feat", BaseBranch: "main",
			HeadRepo: sqlRepo, BaseRepo: sqlRepo, Type: issues_model.PullRequestGitea,
		}
		return pr, pull_service.NewPullRequest(ctx, sqlRepo, issue, nil, nil, pr, nil)
	}
	pr, err := open()
	if err == nil {
		v, _ := a.r.table.Get(laggingRepo, pr.GetGitRefName())
		t.Logf("pull request %d created on lagging node-a: %s at %.10s, the head branch at %.10s", pr.Index, pr.GetGitRefName(), v.SHA, c3)
		assert.Equal(t, c3, v.SHA, "the pull request's head is the commit the push acknowledged")
	} else {
		require.ErrorIs(t, err, clusterhook.ErrBehind)
	}

	caughtUp(t, a, hold, head, c3)
	pr, err = open()
	require.NoError(t, err)
	require.Eventually(t, func() bool {
		v, _ := a.r.table.Get(laggingRepo, pr.GetGitRefName())
		return v.SHA == c3
	}, 10*time.Second, 20*time.Millisecond, "the pull request's head is the pushed commit")
}
