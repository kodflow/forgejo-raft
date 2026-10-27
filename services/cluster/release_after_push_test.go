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

	"forgejo.org/models/db"
	repo_model "forgejo.org/models/repo"
	"forgejo.org/models/unittest"
	user_model "forgejo.org/models/user"
	"forgejo.org/modules/clusterhook"
	"forgejo.org/modules/gitrepo"
	"forgejo.org/modules/setting"
	"forgejo.org/modules/test"
	"forgejo.org/services/cluster/consensus"
	"forgejo.org/services/cluster/reftable"
	release_service "forgejo.org/services/release"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// deletePushVia plays a push that deletes ref (git push origin :ref) on
// node n, as tryPushViaWithin plays a creation: git holds an empty lock on
// the ref, the hook's "prepared" phase is asked, and on success git
// deletes the ref and reports "committed". The pusher is user 2.
func deletePushVia(t *testing.T, tc *testCluster, n *testNode, repo, ref, old string) *clusterhook.Response {
	t.Helper()
	dir := filepath.Join(n.r.root, filepath.FromSlash(repo))
	req := &clusterhook.Request{
		Op: clusterhook.OpPrepared, Nonce: clusterhook.NewNonce(), GitDir: dir, RepoID: tc.repoID(repo), PusherID: 2,
		Lines: []clusterhook.Line{{Old: old, New: strings.Repeat("0", 40), Ref: ref}},
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	unlock := holdRefLock(t, dir, ref, "")
	resp := n.r.Handle(ctx, req)
	unlock()
	req.Op = clusterhook.OpAborted
	if resp.OK {
		if _, err := gitIn(dir, "update-ref", "-d", ref); err == nil {
			req.Op = clusterhook.OpCommitted
		}
	}
	n.r.Handle(ctx, req)
	return resp
}

// followerOf is a node that does not lead now.
func followerOf(tc *testCluster, leader *testNode) *testNode {
	for _, n := range tc.nodes {
		if n != leader {
			return n
		}
	}
	return nil
}

// releaseRow is the release row of name in repo1.
func releaseRow(t *testing.T, name string) *repo_model.Release {
	t.Helper()
	rel, err := repo_model.GetRelease(db.DefaultContext, 1, name)
	require.NoError(t, err)
	return rel
}

// projectedEverywhere waits until every node's git shows ref at sha, ""
// for none.
func projectedEverywhere(t *testing.T, tc *testCluster, ref, sha, what string) {
	t.Helper()
	for _, m := range tc.nodes {
		require.Eventually(t, func() bool { return gitRef(t, m, laggingRepo, ref) == sha }, 15*time.Second, 20*time.Millisecond, "%s projects %s", m.slot, what)
	}
}

// derivedOnLeader opens d, on the leader's git.
func derivedOnLeader(t *testing.T, leader *testNode, d *derivation) {
	t.Helper()
	restore := test.MockVariableValue(&setting.RepoRootPath, leader.r.root)
	defer restore()
	d.derived(t)
}

// lastEventOf is the newest event of ref the leader's table recorded with
// the value sha, "" for a deletion.
func lastEventOf(t *testing.T, leader *testNode, ref, sha string) reftable.Event {
	t.Helper()
	var found *reftable.Event
	for _, ev := range leader.r.table.Events() {
		if ev.Repo == laggingRepo && ev.Ref == ref && ev.New == sha {
			e := ev
			found = &e
		}
	}
	require.NotNil(t, found, "the event of %s at %q", ref, sha)
	return *found
}

// tagPushedAndDerived pushes ref at c1 through n, acknowledged, and has
// the leader's consumer derive its tag-only row.
func tagPushedAndDerived(t *testing.T, tc *testCluster, leader, n *testNode, work, ref, c1 string) {
	t.Helper()
	created := holdDerivation(t, tc, ref, c1, nil)
	resp := tc.pushVia(n, work, laggingRepo, ref, strings.Repeat("0", 40), c1)
	require.True(t, resp.OK, resp.Message)
	projectedEverywhere(t, tc, ref, c1, "the tag")
	derivedOnLeader(t, leader, created)
	row := releaseRow(t, strings.TrimPrefix(ref, "refs/tags/"))
	require.True(t, row.IsTag)
	require.Equal(t, c1, row.Sha1)
}

// The rolling-release pattern: a tag is deleted by a push (git push origin
// :refs/tags/rel-roll), acknowledged, then the release of that name is
// created again through the API (POST /repos/{owner}/{repo}/releases with
// tag_name rel-roll, target main), served by node-b, fresh. The API found
// the tag's row, which the leader's consumer had not removed yet, turned
// it into a release and called UpdateRelease: createTag saw no tag and
// tagged main, a write without pusher, and the client read 201. The
// consumer then derived the deletion push: PushUpdateDeleteTagsContext
// turned the published release into a draft, sha1 and num_commits
// cleared, while its tag lived at main on every node, and the tag's own
// event, without pusher, had no work that would publish it again (#298).
// Now the release service waits for the consumer to derive every write the
// cluster confirmed, and refuses while it has not, nothing written: no
// tag, no row changed. Once derived, the API finds no row and creates the
// release and its tag, which stay published.
func TestAReleaseCreatedRightAfterATagDeletionPushStaysPublished(t *testing.T) {
	require.NoError(t, unittest.PrepareTestDatabase())
	tc := newTestCluster(t, "node-a", "node-b", "node-c")
	work := t.TempDir()
	run(t, work, "init", "-q")
	c1 := commit(t, work, "one")
	c2 := commit(t, work, "two")
	zero := strings.Repeat("0", 40)
	const name = "rel-roll"
	ref := "refs/tags/" + name
	require.Equal(t, int64(1), tc.repoID(laggingRepo))

	leader := clusterLeader(t, tc)
	follower := followerOf(tc, leader)
	require.True(t, tc.pushVia(follower, work, laggingRepo, "refs/heads/main", zero, c2).OK)
	tagPushedAndDerived(t, tc, leader, follower, work, ref, c1)

	// The tag is deleted by a push, acknowledged; the leader's consumer
	// has not derived it yet.
	deleted := holdDerivation(t, tc, ref, "", nil)
	resp := deletePushVia(t, tc, follower, laggingRepo, ref, c1)
	require.True(t, resp.OK, resp.Message)
	projectedEverywhere(t, tc, ref, "", "the deletion")

	withGitThroughReplicator(t, follower)
	freshFrom(t, follower)
	ctx := db.DefaultContext
	sqlRepo := unittest.AssertExistsAndLoadBean(t, &repo_model.Repository{ID: 1})
	doer := unittest.AssertExistsAndLoadBean(t, &user_model.User{ID: 2})
	gitRepo, err := gitrepo.OpenRepository(ctx, sqlRepo)
	require.NoError(t, err)
	defer gitRepo.Close()

	// The refusals below wait the read-after-write bound for a derivation
	// the test holds.
	shortWait := test.MockVariableValue(&setting.Cluster.ReadAfterWriteTimeout, 500*time.Millisecond)
	t.Cleanup(shortWait)
	// What routers/api/v1/repo/release.go CreateRelease does with the
	// tag's row, which the consumer has not removed yet.
	rel := releaseRow(t, name)
	require.True(t, rel.IsTag, "the consumer has not removed the tag row yet")
	rel.Title, rel.Note, rel.IsDraft, rel.IsPrerelease = name, "rolling", false, false
	rel.PublisherID, rel.Publisher, rel.IsTag, rel.Repo, rel.Target = doer.ID, doer, false, sqlRepo, "main"
	err = release_service.UpdateRelease(ctx, doer, gitRepo, rel, true, nil)
	if err == nil {
		// Accepted (201 to the client): what the derivation of the
		// deletion push then does to the published release.
		shortWait()
		tagAppliedEverywhere(t, tc, ref, c2, "the release's tag")
		derivedOnLeader(t, leader, deleted)
		row := releaseRow(t, name)
		t.Fatalf("the release was published while the deletion push of its tag was not derived; once derived, it is is_draft=%v sha1=%q num_commits=%d, while its tag lives at %.10s on every node, and the tag's own event, without pusher, has no work that would publish it again", row.IsDraft, row.Sha1, row.NumCommits, c2)
	}
	require.ErrorIs(t, err, clusterhook.ErrBehind, "the release did not wait for the consumer to derive the push")

	// A release or a tag of another name waits as well.
	other := &repo_model.Release{RepoID: 1, Repo: sqlRepo, PublisherID: doer.ID, Publisher: doer, TagName: "rel-other", Target: "main", Title: "other"}
	require.ErrorIs(t, release_service.CreateRelease(gitRepo, other, "", nil), clusterhook.ErrBehind)
	require.ErrorIs(t, release_service.CreateNewTag(ctx, doer, sqlRepo, c2, "rel-other", ""), clusterhook.ErrBehind)

	// Nothing was written: no tag, the rows as they were.
	for _, m := range tc.nodes {
		v, ok := m.r.table.Get(laggingRepo, ref)
		assert.True(t, ok && v.Tombstone, "%s: the refused release created its tag", m.slot)
		_, ok = m.r.table.Get(laggingRepo, "refs/tags/rel-other")
		assert.False(t, ok, "%s: a refused write created rel-other", m.slot)
	}
	assert.Empty(t, gitRef(t, follower, laggingRepo, ref))
	row := releaseRow(t, name)
	assert.True(t, row.IsTag, "the tag row is left as it was")
	assert.Equal(t, c1, row.Sha1)
	unittest.AssertNotExistsBean(t, &repo_model.Release{RepoID: 1, LowerTagName: "rel-other"})
	shortWait()

	// The leader's consumer derives the deletion push: the tag row goes.
	derivedOnLeader(t, leader, deleted)
	unittest.AssertNotExistsBean(t, &repo_model.Release{RepoID: 1, LowerTagName: name})

	// The client retries: the API finds no row, and creates the release
	// and its tag (CreateRelease), on a node up to date.
	rel = &repo_model.Release{RepoID: 1, Repo: sqlRepo, PublisherID: doer.ID, Publisher: doer, TagName: name, Target: "main", Title: name, Note: "rolling"}
	require.NoError(t, release_service.CreateRelease(gitRepo, rel, "", nil))
	tagAppliedEverywhere(t, tc, ref, c2, "the release's tag")
	row = releaseRow(t, name)
	require.False(t, row.IsDraft)
	require.False(t, row.IsTag)
	require.Equal(t, c2, row.Sha1)
	// And a tag alone.
	require.NoError(t, release_service.CreateNewTag(ctx, doer, sqlRepo, c2, "rel-other", ""))
	tagAppliedEverywhere(t, tc, "refs/tags/rel-other", c2, "the tag")
	assert.True(t, releaseRow(t, "rel-other").IsTag)

	// The release's tag creation, as the consumer derives it: no pusher,
	// no work; the release stays published.
	mk := lastEventOf(t, leader, ref, c2)
	assert.Zero(t, mk.PusherID)
	wctx, _ := clusterhook.WithAfterCommit(ctx)
	require.NoError(t, pushWorkWith(wctx, []reftable.Event{mk}, leader.r.table))
	projectedEverywhere(t, tc, ref, c2, "the release's tag")
	row = releaseRow(t, name)
	t.Logf("release %s is_draft=%v is_tag=%v sha1=%.10s; refs: %s at %.10s on every node", name, row.IsDraft, row.IsTag, row.Sha1, ref, c2)
	assert.False(t, row.IsDraft, "the release published through the API is a draft")
	assert.Equal(t, c2, row.Sha1)
	assert.Equal(t, consensus.RoleLeader, leader.r.Engine().Status().Role, "the leader did not change during the test")
}

// tagAppliedEverywhere waits until every node's table holds ref live at
// sha.
func tagAppliedEverywhere(t *testing.T, tc *testCluster, ref, sha, what string) {
	t.Helper()
	for _, m := range tc.nodes {
		require.Eventually(t, func() bool {
			v, ok := m.r.table.Get(laggingRepo, ref)
			return ok && v.Live() && v.SHA == sha
		}, 10*time.Second, 20*time.Millisecond, "%s applied %s", m.slot, what)
	}
}

// tagDeletedEverywhere waits until every node's table holds ref deleted.
func tagDeletedEverywhere(t *testing.T, tc *testCluster, ref string) {
	t.Helper()
	for _, m := range tc.nodes {
		require.Eventually(t, func() bool {
			v, ok := m.r.table.Get(laggingRepo, ref)
			return ok && !v.Live()
		}, 10*time.Second, 20*time.Millisecond, "%s applied the deletion", m.slot)
	}
}

// holdLeaderProjection holds the leader's projector on repo1, a moment
// behind its table (a pass on another repository, a fetch), and returns its
// release.
func holdLeaderProjection(t *testing.T, leader *testNode) func() {
	t.Helper()
	hold := clusterhook.NewNonce()
	require.NoError(t, leader.r.registry.register(hold, laggingRepo, nil, os.Getpid(), false))
	held := true
	release := func() {
		if held {
			held = false
			leader.r.registry.release(hold)
		}
	}
	t.Cleanup(release)
	return release
}

// The tag variant of #267: a tag moved by a push (c1 to c2), acknowledged,
// then deleted through the API (DELETE /repos/{owner}/{repo}/tags/rel-move:
// DeleteReleaseByID with its tag) on node-b, fresh, before the leader's
// consumer derived the move. The deletion removed the row and the tag, a
// write without pusher that leaves the consumer no work; the consumer then
// derived the move on the leader's git, which had not projected the
// deletion yet, and pushUpdateAddTags wrote the row again, for a tag no
// member held: it came back in the tag list (#298). Now the deletion waits
// for the consumer to derive the move, and is refused while it has not,
// nothing deleted; once derived, it deletes the row and the tag, and the
// deletion's event leaves the row deleted.
func TestATagDeletedRightAfterItsMoveStaysDeletedInSQL(t *testing.T) {
	require.NoError(t, unittest.PrepareTestDatabase())
	tc := newTestCluster(t, "node-a", "node-b", "node-c")
	work := t.TempDir()
	run(t, work, "init", "-q")
	c1 := commit(t, work, "one")
	c2 := commit(t, work, "two")
	zero := strings.Repeat("0", 40)
	const name = "rel-move"
	ref := "refs/tags/" + name

	leader := clusterLeader(t, tc)
	follower := followerOf(tc, leader)
	require.True(t, tc.pushVia(follower, work, laggingRepo, "refs/heads/main", zero, c2).OK)
	tagPushedAndDerived(t, tc, leader, follower, work, ref, c1)

	// The tag moves to c2 by a push, acknowledged, not derived yet.
	moved := holdDerivation(t, tc, ref, c2, nil)
	resp := tc.pushVia(follower, work, laggingRepo, ref, c1, c2)
	require.True(t, resp.OK, resp.Message)
	projectedEverywhere(t, tc, ref, c2, "the move")
	releaseProjection := holdLeaderProjection(t, leader)

	withGitThroughReplicator(t, follower)
	freshFrom(t, follower)
	ctx := db.DefaultContext
	sqlRepo := unittest.AssertExistsAndLoadBean(t, &repo_model.Repository{ID: 1})
	doer := unittest.AssertExistsAndLoadBean(t, &user_model.User{ID: 2})

	rel := releaseRow(t, name)
	require.True(t, rel.IsTag)
	shortWait := test.MockVariableValue(&setting.Cluster.ReadAfterWriteTimeout, 500*time.Millisecond)
	err := release_service.DeleteReleaseByID(ctx, sqlRepo, rel, doer, true)
	shortWait()
	if err == nil {
		// Accepted (204 to the client): what the derivation of the move,
		// on the leader's git that does not show the deletion yet, then
		// writes.
		tagDeletedEverywhere(t, tc, ref)
		derivedOnLeader(t, leader, moved)
		if row, err := repo_model.GetRelease(ctx, 1, name); err == nil {
			t.Fatalf("the tag was deleted while its move was not derived; once derived, SQL holds its row again at %.10s (is_tag=%v), while the cluster deleted the tag: it comes back in the tag list", row.Sha1, row.IsTag)
		}
		t.Fatal("the tag was deleted while its move was not derived")
	}
	require.ErrorIs(t, err, clusterhook.ErrBehind, "the deletion did not wait for the consumer to derive the move")
	tableAt(t, tc, ref, c2, "nothing was deleted")
	assert.Equal(t, c2, gitRef(t, follower, laggingRepo, ref), "nothing was deleted")
	assert.Equal(t, c1, releaseRow(t, name).Sha1, "the row is left as it was")

	// The leader's consumer derives the move, on the leader's git.
	derivedOnLeader(t, leader, moved)
	rel = releaseRow(t, name)
	require.Equal(t, c2, rel.Sha1)

	require.NoError(t, release_service.DeleteReleaseByID(ctx, sqlRepo, rel, doer, true))
	tagDeletedEverywhere(t, tc, ref)
	unittest.AssertNotExistsBean(t, &repo_model.Release{RepoID: 1, LowerTagName: name})
	require.Equal(t, c2, gitRef(t, leader, laggingRepo, ref), "the leader has not projected the deletion yet")

	// The leader's projection catches up, and the consumer derives the
	// deletion's event: no pusher, no work, no row.
	releaseProjection()
	projectedEverywhere(t, tc, ref, "", "the deletion")
	del := lastEventOf(t, leader, ref, "")
	assert.Zero(t, del.PusherID)
	wctx, _ := clusterhook.WithAfterCommit(ctx)
	require.NoError(t, pushWorkWith(wctx, []reftable.Event{del}, leader.r.table))
	unittest.AssertNotExistsBean(t, &repo_model.Release{RepoID: 1, LowerTagName: name})
	assert.Equal(t, consensus.RoleLeader, leader.r.Engine().Status().Role, "the leader did not change during the test")
}

// The move of a tag is derived after the tag was deleted without pusher,
// through a request that passed its wait before the move was acknowledged
// (here: the wait is left out), while the leader's git does not show the
// deletion yet. The work of the move read the leader's git, found the
// tag, and wrote its row again, for a tag the cluster deleted: it came
// back in the tag list for good, the deletion having no work of its own
// (#298). Now the work reads the table first: a tag deleted there by a
// later ref transaction has nothing left to land on, as a tag git no
// longer holds (#44).
func TestAMovedTagDeletedBeforeItsDerivationIsNotWrittenBack(t *testing.T) {
	require.NoError(t, unittest.PrepareTestDatabase())
	tc := newTestCluster(t, "node-a", "node-b", "node-c")
	work := t.TempDir()
	run(t, work, "init", "-q")
	c1 := commit(t, work, "one")
	c2 := commit(t, work, "two")
	zero := strings.Repeat("0", 40)
	const name = "rel-gone"
	ref := "refs/tags/" + name

	leader := clusterLeader(t, tc)
	follower := followerOf(tc, leader)
	require.True(t, tc.pushVia(follower, work, laggingRepo, "refs/heads/main", zero, c2).OK)
	tagPushedAndDerived(t, tc, leader, follower, work, ref, c1)

	moved := holdDerivation(t, tc, ref, c2, nil)
	resp := tc.pushVia(follower, work, laggingRepo, ref, c1, c2)
	require.True(t, resp.OK, resp.Message)
	projectedEverywhere(t, tc, ref, c2, "the move")
	releaseProjection := holdLeaderProjection(t, leader)

	withGitThroughReplicator(t, follower)
	freshFrom(t, follower)
	// The deletion passed its wait for the consumer before the move was
	// acknowledged.
	clusterhook.SetDerivedWaiter(nil)
	ctx := db.DefaultContext
	sqlRepo := unittest.AssertExistsAndLoadBean(t, &repo_model.Repository{ID: 1})
	doer := unittest.AssertExistsAndLoadBean(t, &user_model.User{ID: 2})
	require.NoError(t, release_service.DeleteReleaseByID(ctx, sqlRepo, releaseRow(t, name), doer, true))
	tagDeletedEverywhere(t, tc, ref)
	unittest.AssertNotExistsBean(t, &repo_model.Release{RepoID: 1, LowerTagName: name})
	require.Equal(t, c2, gitRef(t, leader, laggingRepo, ref), "the leader has not projected the deletion yet")

	// The consumer derives the move on the leader's git, which still shows
	// the tag.
	derivedOnLeader(t, leader, moved)
	_, err := repo_model.GetRelease(ctx, 1, name)
	if err == nil {
		row := releaseRow(t, name)
		t.Errorf("SQL holds the tag row %s at %.10s (is_tag=%v) while the cluster deleted the tag: it comes back in the tag list", name, row.Sha1, row.IsTag)
	} else {
		require.True(t, repo_model.IsErrReleaseNotExist(err), "%v", err)
	}

	releaseProjection()
	projectedEverywhere(t, tc, ref, "", "the deletion")
	del := lastEventOf(t, leader, ref, "")
	wctx, _ := clusterhook.WithAfterCommit(ctx)
	require.NoError(t, pushWorkWith(wctx, []reftable.Event{del}, leader.r.table))
	unittest.AssertNotExistsBean(t, &repo_model.Release{RepoID: 1, LowerTagName: name})
	assert.Equal(t, consensus.RoleLeader, leader.r.Engine().Status().Role, "the leader did not change during the test")
}
