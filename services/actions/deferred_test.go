// Copyright 2026 The Forgejo Authors. All rights reserved.
// SPDX-License-Identifier: GPL-3.0-or-later

package actions

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	actions_model "forgejo.org/models/actions"
	"forgejo.org/models/db"
	issues_model "forgejo.org/models/issues"
	repo_model "forgejo.org/models/repo"
	"forgejo.org/models/unittest"
	"forgejo.org/modules/cache"
	"forgejo.org/modules/clusterhook"
	"forgejo.org/modules/json"
	"forgejo.org/modules/log"
	"forgejo.org/modules/setting"
	api "forgejo.org/modules/structs"
	"forgejo.org/modules/test"
	"forgejo.org/modules/timeutil"
	webhook_module "forgejo.org/modules/webhook"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// requireDeferred notifies input's ref resolution and requires it deferred:
// no git repository opened, no commit resolved.
func requireDeferred(ctx context.Context, t *testing.T, input *NotifyInput) {
	t.Helper()
	gitRepo, commit, _, err := getGitRepoAndCommit(ctx, input)
	require.ErrorIs(t, err, errDeferred)
	assert.Nil(t, gitRepo)
	assert.Nil(t, commit)
}

func countDeferred(t *testing.T) int64 {
	t.Helper()
	n, err := actions_model.CountDeferredNotifications(db.DefaultContext)
	require.NoError(t, err)
	return n
}

func deferredRows(t *testing.T) []*actions_model.ActionDeferredNotification {
	t.Helper()
	rows, err := actions_model.FindDeferredNotifications(db.DefaultContext, 100)
	require.NoError(t, err)
	return rows
}

// deployComment is the notification of the n-th "/deploy" comment on
// repository 1's issue 1.
func deployComment(t *testing.T, n int) *NotifyInput {
	t.Helper()
	return commentInput(t).WithPayload(&api.IssueCommentPayload{
		Action:  api.HookIssueCommentCreated,
		Issue:   &api.Issue{ID: 1, Index: 1},
		Comment: &api.Comment{ID: int64(1000 + n), Body: fmt.Sprintf("/deploy %d", n)},
	})
}

// commentBody is the body of the comment a deferred event carries.
func commentBody(t *testing.T, row *actions_model.ActionDeferredNotification) string {
	t.Helper()
	var p api.IssueCommentPayload
	require.NoError(t, json.Unmarshal([]byte(row.Payload), &p))
	return p.Comment.Body
}

// recordReplays records, in order, the comments the replays commit.
func recordReplays(t *testing.T) *[]string {
	t.Helper()
	replayed := &[]string{}
	t.Cleanup(test.MockVariableValue(&beforeDeferredReplayCommit, func(ctx context.Context, row *actions_model.ActionDeferredNotification) error {
		assert.True(t, db.InTransaction(ctx), "the replay commits with the removal of its row")
		*replayed = append(*replayed, commentBody(t, row))
		return nil
	}))
	return replayed
}

// In a cluster, an event a node cannot resolve fresh within the bound is
// deferred, and so is every later event of the same repository, behind it
// and without waiting, even once the node serves the repository fresh: a
// replay runs them in the order they happened, each once.
func TestDeferredEventsKeepTheirOrder(t *testing.T) {
	require.NoError(t, unittest.PrepareTestDatabase())
	defer test.MockVariableValue(&setting.Cluster.ReadAfterWriteTimeout, 100*time.Millisecond)()
	clusterhook.SetEnabled(true)
	t.Cleanup(func() { clusterhook.SetEnabled(false) })

	calls := fakeFresh(t, true)
	for n := 1; n <= 3; n++ {
		requireDeferred(cache.WithCacheContext(db.DefaultContext), t, deployComment(t, n))
	}
	assert.Len(t, *calls, 1, "the first event waits the bound, the next ones are deferred behind it")

	calls = fakeFresh(t, false)
	requireDeferred(cache.WithCacheContext(db.DefaultContext), t, deployComment(t, 4))
	assert.Empty(t, *calls, "a fresh node defers an event behind the repository's deferred ones")
	require.Equal(t, int64(4), countDeferred(t))

	replayed := recordReplays(t)
	assert.Equal(t, 4, ReplayDeferredNotifications(db.DefaultContext))
	assert.Equal(t, []string{"/deploy 1", "/deploy 2", "/deploy 3", "/deploy 4"}, *replayed, "replayed in order")
	assert.Len(t, *calls, 1, "one freshness check for the repository's events")
	assert.Zero(t, countDeferred(t))

	assert.Zero(t, ReplayDeferredNotifications(db.DefaultContext), "each event is replayed once")
	assert.Len(t, *replayed, 4)

	// Nothing deferred any more: the next event is resolved at once.
	_, commitID := resolve(cache.WithCacheContext(db.DefaultContext), t, deployComment(t, 5))
	assert.Equal(t, repo1Master, commitID)
	assert.Zero(t, countDeferred(t))
}

// A replay stops by the event's runs written and its row removed in one
// transaction. A node that stops before the commit leaves neither: the
// event stays deferred, the repository's later events behind it, and the
// next pass, after a delay that grows with the failures, replays it, its
// runs written once. A replay that read the event before another one
// replayed it finds nothing left to take.
func TestADeferredEventInterruptedBeforeItsCommitRunsOnce(t *testing.T) {
	require.NoError(t, unittest.PrepareTestDatabase())
	defer test.MockVariableValue(&setting.Cluster.ReadAfterWriteTimeout, 100*time.Millisecond)()
	fakeFresh(t, true)
	requireDeferred(db.DefaultContext, t, deployComment(t, 1))
	requireDeferred(db.DefaultContext, t, deployComment(t, 2))
	fakeFresh(t, false)

	// The replay's notification writes a run, as the workflow the comment
	// triggers would.
	index := int64(900000)
	defer test.MockVariableValue(&Notify, func(ctx context.Context, input *NotifyInput) error {
		index++
		p := input.Payload.(*api.IssueCommentPayload)
		return db.Insert(ctx, &actions_model.ActionRun{
			RepoID: input.Repo.ID, OwnerID: input.Repo.OwnerID, Index: index, Event: input.Event,
			WorkflowID: "deploy.yml", Ref: "refs/heads/master", CommitSHA: repo1Master,
			Title: p.Comment.Body, Status: actions_model.StatusWaiting,
		})
	})()
	runsOf := func(body string) int64 {
		n, err := db.GetEngine(db.DefaultContext).Where("repo_id = 1 AND workflow_id = ? AND title = ?", "deploy.yml", body).Count(new(actions_model.ActionRun))
		require.NoError(t, err)
		return n
	}

	stops := 1
	var replayed []string
	defer test.MockVariableValue(&beforeDeferredReplayCommit, func(ctx context.Context, row *actions_model.ActionDeferredNotification) error {
		if stops > 0 {
			stops--
			return errors.New("the node stopped before the commit")
		}
		replayed = append(replayed, commentBody(t, row))
		return nil
	})()

	assert.Zero(t, ReplayDeferredNotifications(db.DefaultContext))
	assert.Zero(t, runsOf("/deploy 1"), "the interrupted replay wrote nothing")
	rows := deferredRows(t)
	require.Len(t, rows, 2, "the event stays deferred, and the next one behind it")
	assert.Equal(t, 1, rows[0].Attempts)
	assert.Contains(t, rows[0].LastError, "the node stopped before the commit")
	assert.Zero(t, rows[1].Attempts, "the next event was not tried")

	assert.Zero(t, ReplayDeferredNotifications(db.DefaultContext), "a failed event is tried again after a delay")

	defer test.MockVariableValue(&deferredReplayEvery, time.Duration(0))()
	assert.Equal(t, 2, ReplayDeferredNotifications(db.DefaultContext), "the next pass, as after a restart, replays both")
	assert.Equal(t, []string{"/deploy 1", "/deploy 2"}, replayed)
	assert.Equal(t, int64(1), runsOf("/deploy 1"), "the event's run is written once")
	assert.Equal(t, int64(1), runsOf("/deploy 2"))

	assert.Zero(t, ReplayDeferredNotifications(db.DefaultContext))
	ran, err := replayDeferred(db.DefaultContext, rows[0])
	require.ErrorIs(t, err, errDeferredGone, "a replay that read the event before finds it gone")
	assert.False(t, ran)
	assert.Equal(t, int64(1), runsOf("/deploy 1"), "and writes nothing")
}

// A repository this node does not serve fresh keeps its deferred events,
// and does not hold back another repository's, deferred after them.
func TestADeferredEventWaitsOnlyForItsRepository(t *testing.T) {
	require.NoError(t, unittest.PrepareTestDatabase())
	defer test.MockVariableValue(&setting.Cluster.ReadAfterWriteTimeout, 50*time.Millisecond)()
	fakeFresh(t, true)
	other := commentInput(t)
	other.Repo = unittest.AssertExistsAndLoadBean(t, &repo_model.Repository{ID: 2})
	requireDeferred(db.DefaultContext, t, other.WithPayload(deployComment(t, 1).Payload))
	requireDeferred(db.DefaultContext, t, deployComment(t, 2))

	clusterhook.SetFreshWaiter(func(ctx context.Context, key string) bool { return key == "user2/repo1.git" })
	replayed := recordReplays(t)
	assert.Equal(t, 1, ReplayDeferredNotifications(db.DefaultContext))
	assert.Equal(t, []string{"/deploy 2"}, *replayed, "repository 1's event runs")
	rows := deferredRows(t)
	require.Len(t, rows, 1, "repository 2's event waits")
	assert.EqualValues(t, 2, rows[0].RepoID)
	assert.Zero(t, rows[0].Attempts, "waiting for a node to serve it fresh is no failure")
}

// A panic in the replay of an event rolls it back like a failure: the event
// stays deferred, and the node goes on.
func TestADeferredEventWhoseReplayPanicsStaysDeferred(t *testing.T) {
	require.NoError(t, unittest.PrepareTestDatabase())
	defer test.MockVariableValue(&setting.Cluster.ReadAfterWriteTimeout, 100*time.Millisecond)()
	fakeFresh(t, true)
	requireDeferred(db.DefaultContext, t, deployComment(t, 1))
	fakeFresh(t, false)
	defer test.MockVariableValue(&Notify, func(context.Context, *NotifyInput) error { panic("boom") })()

	assert.Zero(t, ReplayDeferredNotifications(db.DefaultContext))
	rows := deferredRows(t)
	require.Len(t, rows, 1)
	assert.Contains(t, rows[0].LastError, "panic: boom")
}

// A deferred event carries what its replay needs: its event, its ref, its
// doer, its pull request and its payload, which the replay decodes into
// the payload's type. The replay runs in its transaction, marked as a
// replay, so the ref is resolved at once.
func TestADeferredEventReplaysItsPayloadAndPullRequest(t *testing.T) {
	require.NoError(t, unittest.PrepareTestDatabase())
	defer test.MockVariableValue(&setting.Cluster.ReadAfterWriteTimeout, 100*time.Millisecond)()
	fakeFresh(t, true)
	pr := unittest.AssertExistsAndLoadBean(t, &issues_model.PullRequest{ID: 1})
	payload := &api.PullRequestPayload{
		Action:      api.HookIssueLabelUpdated,
		Index:       2,
		PullRequest: &api.PullRequest{ID: 1, Index: 2, Title: "issue2"},
		Label:       &api.Label{ID: 1, Name: "label1"},
	}
	input := commentInput(t).WithPayload(payload).WithPullRequest(pr)
	input.Event = webhook_module.HookEventPullRequestLabel
	requireDeferred(db.DefaultContext, t, input)
	fakeFresh(t, false)

	var got *NotifyInput
	defer test.MockVariableValue(&Notify, func(ctx context.Context, in *NotifyInput) error {
		assert.True(t, isDeferredReplay(ctx))
		assert.True(t, db.InTransaction(ctx))
		assert.False(t, mayWaitToResolve(ctx), "a replay resolves its ref at once")
		got = in
		return nil
	})()
	assert.Equal(t, 1, ReplayDeferredNotifications(db.DefaultContext))
	require.NotNil(t, got)
	assert.Equal(t, webhook_module.HookEventPullRequestLabel, got.Event)
	assert.EqualValues(t, "refs/pull/2/head", got.Ref)
	assert.EqualValues(t, 1, got.Repo.ID)
	assert.EqualValues(t, 2, got.Doer.ID)
	require.NotNil(t, got.PullRequest)
	assert.EqualValues(t, 1, got.PullRequest.ID)
	assert.NotNil(t, got.PullRequest.Issue, "the pull request's issue is loaded")
	assert.Equal(t, payload, got.Payload)
}

// The queue is bounded: past ACTIONS_DEFERRED_MAX_EVENTS deferred events, an
// event is resolved on the node as it is, as before the queue, with a
// warning; ACTIONS_DEFERRED_MAX_EVENTS = 0 turns the deferral off.
func TestDeferredEventsAreBounded(t *testing.T) {
	require.NoError(t, unittest.PrepareTestDatabase())
	defer test.MockVariableValue(&setting.Cluster.ReadAfterWriteTimeout, 50*time.Millisecond)()
	defer test.MockVariableValue(&setting.Cluster.ActionsDeferredMaxEvents, 2)()
	fakeFresh(t, true)
	requireDeferred(db.DefaultContext, t, deployComment(t, 1))
	requireDeferred(db.DefaultContext, t, deployComment(t, 2))

	lc, cleanup := test.NewLogChecker(log.DEFAULT, log.WARN)
	defer cleanup()
	want := []string{"user2/repo1", "cannot be deferred", "ACTIONS_DEFERRED_MAX_EVENTS"}
	lc.Filter(want...).StopMark("may run a commit before a push another node acknowledged")
	_, commitID := resolve(db.DefaultContext, t, deployComment(t, 3))
	assert.Equal(t, repo1Master, commitID, "past the bound, the event is resolved on the node as it is")
	filtered, stopped := lc.Check(5 * time.Second)
	assert.True(t, stopped, "the warning of an event that could not be deferred")
	for i, part := range want {
		assert.True(t, filtered[i], "the warning names %q", part)
	}
	assert.Equal(t, int64(2), countDeferred(t))

	defer test.MockVariableValue(&setting.Cluster.ActionsDeferredMaxEvents, 0)()
	require.NoError(t, db.TruncateBeans(db.DefaultContext, new(actions_model.ActionDeferredNotification)))
	_, commitID = resolve(db.DefaultContext, t, deployComment(t, 4))
	assert.Equal(t, repo1Master, commitID, "with the deferral off, the event is resolved on the node as it is")
	assert.Zero(t, countDeferred(t))
}

// A deferred event that waited longer than ACTIONS_DEFERRED_MAX_AGE, or
// whose repository or pull request was deleted, is dropped with a warning
// and never runs; the repository's other events are replayed.
func TestDeferredEventsTooOldOrOfADeletedRepositoryAreDropped(t *testing.T) {
	require.NoError(t, unittest.PrepareTestDatabase())
	defer test.MockVariableValue(&setting.Cluster.ActionsDeferredMaxAge, time.Hour)()
	fakeFresh(t, false)
	ctx := db.DefaultContext
	comment := func(n int) string {
		b, err := json.Marshal(deployComment(t, n).Payload)
		require.NoError(t, err)
		return string(b)
	}
	old := &actions_model.ActionDeferredNotification{RepoID: 1, DoerID: 2, Event: webhook_module.HookEventIssueComment, Payload: comment(1)}
	require.NoError(t, actions_model.InsertDeferredNotification(ctx, old))
	_, err := db.GetEngine(ctx).Exec("UPDATE action_deferred_notification SET created_unix = ? WHERE id = ?", timeutil.TimeStampNow().Add(-2*3600), old.ID)
	require.NoError(t, err)
	gone := &actions_model.ActionDeferredNotification{RepoID: 999999, DoerID: 2, Event: webhook_module.HookEventIssueComment, Payload: comment(2)}
	require.NoError(t, actions_model.InsertDeferredNotification(ctx, gone))
	orphan := &actions_model.ActionDeferredNotification{
		RepoID: 1, DoerID: 2, Event: webhook_module.HookEventPullRequestLabel, Ref: "refs/pull/999/head", PullRequestID: 999999,
		Payload: `{"action":"label_updated","number":999}`,
	}
	require.NoError(t, actions_model.InsertDeferredNotification(ctx, orphan))
	live := &actions_model.ActionDeferredNotification{RepoID: 1, DoerID: 2, Event: webhook_module.HookEventIssueComment, Payload: comment(3)}
	require.NoError(t, actions_model.InsertDeferredNotification(ctx, live))

	lc, cleanup := test.NewLogChecker(log.DEFAULT, log.WARN)
	defer cleanup()
	want := []string{"ACTIONS_DEFERRED_MAX_AGE", "its pull request was deleted", "its repository was deleted"}
	// The deleted repository's event is the last one dropped: its
	// repository comes after repository 1's in the queue.
	lc.Filter(want...).StopMark("its repository was deleted")
	replayed := recordReplays(t)
	assert.Equal(t, 1, ReplayDeferredNotifications(ctx))
	assert.Equal(t, []string{"/deploy 3"}, *replayed, "only the live event runs")
	assert.Zero(t, countDeferred(t), "the dropped events left the queue")
	filtered, stopped := lc.Check(5 * time.Second)
	assert.True(t, stopped)
	for i, part := range want {
		assert.True(t, filtered[i], "a warning says %q", part)
	}
}
