// Copyright 2026 The Forgejo Authors. All rights reserved.
// SPDX-License-Identifier: GPL-3.0-or-later

package actions

import (
	"context"
	"testing"
	"time"

	"forgejo.org/models/db"
	issues_model "forgejo.org/models/issues"
	repo_model "forgejo.org/models/repo"
	"forgejo.org/models/unittest"
	user_model "forgejo.org/models/user"
	"forgejo.org/modules/cache"
	"forgejo.org/modules/clusterhook"
	"forgejo.org/modules/git"
	"forgejo.org/modules/log"
	"forgejo.org/modules/setting"
	"forgejo.org/modules/test"
	webhook_module "forgejo.org/modules/webhook"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// The heads of the fixtures' repository 1: master, and the head of its
// pull request 1 (index 2).
const (
	repo1Master = "65f1bf27bc3bf70f64657658635e66094edbcb4d"
	repo1Pull2  = "4a357436d925b5c974181ff12a994538ddc5a269"
)

// freshCall is one call of a fake freshness wait of the cluster.
type freshCall struct {
	key string
	// left is how long the call had before its context's deadline, at
	// the call; doneAtCall is whether the context was already done.
	left       time.Duration
	hasLimit   bool
	doneAtCall bool
}

// fakeFresh registers a freshness wait of the cluster that records its
// calls and answers fresh, or, with behind, waits until its context is
// done and answers that the node may be behind.
func fakeFresh(t *testing.T, behind bool) *[]freshCall {
	t.Helper()
	calls := &[]freshCall{}
	clusterhook.SetFreshWaiter(func(ctx context.Context, key string) bool {
		c := freshCall{key: key, doneAtCall: ctx.Err() != nil}
		if deadline, ok := ctx.Deadline(); ok {
			c.left, c.hasLimit = time.Until(deadline), true
		}
		*calls = append(*calls, c)
		if !behind {
			return true
		}
		<-ctx.Done()
		return false
	})
	t.Cleanup(func() { clusterhook.SetFreshWaiter(nil) })
	return calls
}

func commentInput(t *testing.T) *NotifyInput {
	t.Helper()
	repo := unittest.AssertExistsAndLoadBean(t, &repo_model.Repository{ID: 1})
	doer := unittest.AssertExistsAndLoadBean(t, &user_model.User{ID: 2})
	return NewNotifyInput(repo, doer, webhook_module.HookEventIssueComment)
}

func resolve(ctx context.Context, t *testing.T, input *NotifyInput) (git.RefName, string) {
	t.Helper()
	gitRepo, commit, ref, err := getGitRepoAndCommit(ctx, input)
	require.NoError(t, err)
	defer gitRepo.Close()
	return ref, commit.ID.String()
}

// Outside a cluster an event's ref is resolved at once in the forge's git,
// and nothing is logged about freshness.
func TestNotifyOutsideAClusterResolvesTheRefAtOnce(t *testing.T) {
	require.NoError(t, unittest.PrepareTestDatabase())
	clusterhook.SetFreshWaiter(nil)
	lc, cleanup := test.NewLogChecker(log.DEFAULT, log.WARN)
	defer cleanup()
	lc.StopMark("may still be behind the cluster's last writes")

	ref, commitID := resolve(cache.WithCacheContext(db.DefaultContext), t, commentInput(t))
	assert.Equal(t, git.RefNameFromBranch("master"), ref)
	assert.Equal(t, repo1Master, commitID)
	_, stopped := lc.Check(100 * time.Millisecond)
	assert.False(t, stopped, "no warning about freshness outside a cluster")
}

// In a cluster, an event without a commit of its own (a comment, a pull
// request's label) waits for this node to serve the repository it
// resolves a ref of, the pull request's base for refs/pull/N/head, within
// READ_AFTER_WRITE_TIMEOUT, before the ref is resolved (#335).
func TestNotifyWaitsForTheRepositoryBeforeResolvingARef(t *testing.T) {
	require.NoError(t, unittest.PrepareTestDatabase())
	defer test.MockVariableValue(&setting.Cluster.ReadAfterWriteTimeout, 2*time.Second)()
	calls := fakeFresh(t, false)

	ref, commitID := resolve(cache.WithCacheContext(db.DefaultContext), t, commentInput(t))
	assert.Equal(t, git.RefNameFromBranch("master"), ref)
	assert.Equal(t, repo1Master, commitID)

	pr := unittest.AssertExistsAndLoadBean(t, &issues_model.PullRequest{ID: 1})
	input := commentInput(t)
	input.Event = webhook_module.HookEventPullRequestLabel
	ref, commitID = resolve(cache.WithCacheContext(db.DefaultContext), t, input.WithPullRequest(pr))
	assert.Equal(t, git.RefName("refs/pull/2/head"), ref)
	assert.Equal(t, repo1Pull2, commitID)

	require.Len(t, *calls, 2, "one wait per resolution")
	for _, c := range *calls {
		assert.Equal(t, "user2/repo1.git", c.key)
		assert.False(t, c.doneAtCall)
		assert.True(t, c.hasLimit && c.left <= setting.Cluster.ReadAfterWriteTimeout, "bounded by READ_AFTER_WRITE_TIMEOUT: %v", c.left)
	}
}

// Nothing waits where the commit cannot lag behind the cluster, or where a
// wait must not happen: an event that carries its commit (a push), a ref
// that is a commit ID (a merge), the log consumer's replicated command,
// which must not wait for the writes after the one it derives (#311, as
// for #325), and a database transaction, whose locks a wait would hold.
func TestNotifyDoesNotWaitWithoutARefToResolveOrUnderTheConsumer(t *testing.T) {
	require.NoError(t, unittest.PrepareTestDatabase())
	calls := fakeFresh(t, true)
	ctx := cache.WithCacheContext(db.DefaultContext)

	_, commitID := resolve(ctx, t, commentInput(t).WithCommit(repo1Pull2))
	assert.Equal(t, repo1Pull2, commitID, "an explicit commit")

	merge := commentInput(t)
	merge.Event = webhook_module.HookEventPullRequest
	_, commitID = resolve(ctx, t, merge.WithRef(repo1Master))
	assert.Equal(t, repo1Master, commitID, "a commit ID as the ref")

	_, commitID = resolve(clusterhook.WithCommand(ctx, "1/2/3"), t, commentInput(t))
	assert.Equal(t, repo1Master, commitID, "the consumer's command")

	require.NoError(t, db.WithTx(ctx, func(ctx context.Context) error {
		_, commitID = resolve(ctx, t, commentInput(t))
		return nil
	}))
	assert.Equal(t, repo1Master, commitID, "in a transaction")

	assert.Empty(t, *calls)
	assert.Zero(t, countDeferred(t), "nothing is deferred")
}

// A request that notifies several events (one per label it adds, a
// comment that closes an issue) waits READ_AFTER_WRITE_TIMEOUT in all, not
// once per event: once spent, the next events of the request do not wait.
// Past the bound each event is deferred, for a node that serves the
// repository fresh to replay it (#335, option C), where option A resolved
// it on the node as it is. Outside a request, each event has its own
// bound. The cluster is off here, so that every event reaches the wait: in
// a cluster, the events that follow a deferred one of the same repository
// are deferred behind it without waiting (TestDeferredEventsKeepTheirOrder).
func TestNotifyWaitsOneBoundPerRequest(t *testing.T) {
	require.NoError(t, unittest.PrepareTestDatabase())
	const bound = 200 * time.Millisecond
	defer test.MockVariableValue(&setting.Cluster.ReadAfterWriteTimeout, bound)()
	calls := fakeFresh(t, true)

	request := cache.WithCacheContext(db.DefaultContext)
	start := time.Now()
	for range 3 {
		requireDeferred(request, t, commentInput(t))
	}
	took := time.Since(start)
	require.Len(t, *calls, 3)
	assert.False(t, (*calls)[0].doneAtCall, "the request's first event waits")
	assert.True(t, (*calls)[1].doneAtCall, "the bound is spent: no second wait")
	assert.True(t, (*calls)[2].doneAtCall, "the bound is spent: no third wait")
	assert.Equal(t, int64(3), countDeferred(t), "past the bound, every event is deferred")
	t.Logf("three events of one request on a node behind: %v", took)

	*calls = (*calls)[:0]
	for range 2 {
		requireDeferred(db.DefaultContext, t, commentInput(t))
	}
	require.Len(t, *calls, 2)
	for _, c := range *calls {
		assert.False(t, c.doneAtCall, "outside a request each event has its own bound")
		assert.True(t, c.hasLimit && c.left <= bound)
	}
}
