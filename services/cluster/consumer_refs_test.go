// Copyright 2026 The Forgejo Authors. All rights reserved.
// SPDX-License-Identifier: GPL-3.0-or-later

package cluster

import (
	"context"
	"os/exec"
	"testing"

	cluster_model "forgejo.org/models/cluster"
	"forgejo.org/models/db"
	git_model "forgejo.org/models/git"
	repo_model "forgejo.org/models/repo"
	"forgejo.org/models/unittest"
	"forgejo.org/services/cluster/consensus"
	"forgejo.org/services/cluster/reftable"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	repo1Base = "65f1bf27bc3bf70f64657658635e66094edbcb4d"
	repo1Next = "985f0301dba5e7b34be866819cd15ad3d8f508ee"
)

func repo1Event(i uint64, ref, old, nw string) reftable.Event {
	return reftable.Event{
		Token: consensus.Token{Cluster: consensus.ClusterID{1}, Epoch: 1, Term: 1, Index: i}, Kind: reftable.EventRef,
		Repo: "user2/repo1.git", RepoID: 1, PusherID: 2, PusherName: "user2", Ref: ref, Old: old, New: nw,
	}
}

// git runs git in user2/repo1's directory.
func repo1Git(t *testing.T, args ...string) {
	t.Helper()
	out, err := exec.Command("git", append([]string{"-C", repo_model.RepoPath("user2", "repo1")}, args...)...).CombinedOutput()
	require.NoError(t, err, string(out))
}

// A branch or a tag created then deleted before the consumer derived it
// has no row to mark deleted and nothing to describe: the work is done,
// never failed, so it is not parked for good behind the trim (#44,
// TestHuntR1ProveCrDel).
func TestCreateThenDeleteOfAnUnsyncedBranch(t *testing.T) {
	require.NoError(t, unittest.PrepareTestDatabase())
	ctx := db.DefaultContext
	create := repo1Event(10, "refs/heads/tmp", "", repo1Base)
	del := repo1Event(11, "refs/heads/tmp", repo1Base, "")
	// What an older dispatch grouped, then each entry on its own, as
	// retryParked replays them.
	require.NoError(t, pushWork(ctx, []reftable.Event{create, del}))
	require.NoError(t, pushWork(ctx, []reftable.Event{create}))
	require.NoError(t, pushWork(ctx, []reftable.Event{del}))
	_, err := git_model.GetBranch(ctx, 1, "tmp")
	assert.True(t, git_model.IsErrBranchNotExist(err))

	tagCreate := repo1Event(12, "refs/tags/tmp-tag", "", repo1Base)
	tagDel := repo1Event(13, "refs/tags/tmp-tag", repo1Base, "")
	require.NoError(t, pushWork(ctx, []reftable.Event{tagCreate}))
	require.NoError(t, pushWork(ctx, []reftable.Event{tagDel}))
	require.NoError(t, pushWork(ctx, []reftable.Event{tagCreate, tagDel}))
}

// Two entries of one repository and one pusher, the creation of a branch
// or a tag and its update, are derived each on its own: the new branch and
// the tag's release are inserted once (#76, TestHuntR4ProveDupRefGroupedEntries).
func TestTwoPushesOfANewRefInOneBatch(t *testing.T) {
	require.NoError(t, unittest.PrepareTestDatabase())
	ctx := db.DefaultContext
	repo1Git(t, "update-ref", "refs/heads/dup-new", repo1Next)
	repo1Git(t, "update-ref", "refs/tags/dup-tag", repo1Next)

	c := newConsumer(newReplicator(t.TempDir(), newRegistry(0, 1, 1)))
	c.hasParked = nil
	c.work = func(ctx context.Context, evs []reftable.Event) error { return pushWorkWith(ctx, evs, nil) }
	batch := []reftable.Event{
		repo1Event(10, "refs/heads/dup-new", "", repo1Base),
		repo1Event(11, "refs/heads/dup-new", repo1Base, repo1Next),
		repo1Event(12, "refs/tags/dup-tag", "", repo1Base),
		repo1Event(13, "refs/tags/dup-tag", repo1Base, repo1Next),
	}
	lineage := batch[0].Token.Identity().String()
	require.NoError(t, c.dispatch(ctx, lineage, &cluster_model.Cursor{}, batch))
	b, err := git_model.GetBranch(ctx, 1, "dup-new")
	require.NoError(t, err)
	assert.Equal(t, repo1Next, b.CommitID)
	n, err := db.GetEngine(ctx).Where("repo_id = ? AND lower_tag_name = ?", 1, "dup-tag").Count(new(repo_model.Release))
	require.NoError(t, err)
	assert.EqualValues(t, 1, n)

	// Even grouped by an older dispatch, the branch is inserted once.
	repo1Git(t, "update-ref", "refs/heads/dup-grouped", repo1Next)
	require.NoError(t, pushWork(ctx, []reftable.Event{
		repo1Event(20, "refs/heads/dup-grouped", "", repo1Base),
		repo1Event(21, "refs/heads/dup-grouped", repo1Base, repo1Next),
	}))
}

// The pusher's account deleted before the consumer derived its push: the
// branch rows are written, the notifications that need the account are
// dropped, and the work is done, never parked for good (#95,
// TestHuntR6ProveDeletedPusherParksForever, TestHuntSecpushDeletedPusherParksForever).
func TestPushWorkOfADeletedPusherIsDone(t *testing.T) {
	require.NoError(t, unittest.PrepareTestDatabase())
	ctx := db.DefaultContext
	repo1Git(t, "update-ref", "refs/heads/gone-pusher", repo1Next)
	ev := repo1Event(30, "refs/heads/gone-pusher", "", repo1Next)
	ev.PusherID, ev.PusherName = 424242, "gone"
	require.NoError(t, pushWork(ctx, []reftable.Event{ev}))
	b, err := git_model.GetBranch(ctx, 1, "gone-pusher")
	require.NoError(t, err)
	assert.Equal(t, repo1Next, b.CommitID)
}
