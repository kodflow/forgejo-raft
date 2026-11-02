// Copyright 2026 The Forgejo Authors. All rights reserved.
// SPDX-License-Identifier: GPL-3.0-or-later

package actions

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	actions_model "forgejo.org/models/actions"
	"forgejo.org/models/db"
	issues_model "forgejo.org/models/issues"
	repo_model "forgejo.org/models/repo"
	user_model "forgejo.org/models/user"
	"forgejo.org/modules/clusterhook"
	"forgejo.org/modules/json"
	"forgejo.org/modules/log"
	"forgejo.org/modules/setting"
	api "forgejo.org/modules/structs"
	webhook_module "forgejo.org/modules/webhook"
)

// In a cluster, an Actions event without a commit of its own resolves its
// ref in the git of the node that serves the request. A node behind on the
// repository would resolve it to the commit before a push another node
// acknowledged, and that commit's run would cancel the acknowledged head's
// through a concurrency group (#335). getGitRepoAndCommit first waits,
// within READ_AFTER_WRITE_TIMEOUT, for the node to serve the repository at
// the cluster's commit index. Past the bound, or without a leader, the
// event is deferred: recorded in action_deferred_notification, in the
// database the nodes share, and replayed by a node that serves the
// repository fresh, at the ref's acknowledged head. Each node replays every
// deferredReplayEvery, and at once when one of its requests defers an
// event. A repository's events keep their order: an event that finds
// earlier ones deferred is deferred behind them, and a replay stops at an
// event it could not replay. The row is removed by the transaction that
// writes the event's runs, so an event runs once, whichever node replays
// it, and a node that stops in the middle of a replay leaves the row to
// the next one.

// deferredReplayEvery is how often a node of a cluster replays the
// deferred events of the repositories it serves fresh.
var deferredReplayEvery = 5 * time.Second

// deferredReplayRepos bounds the repositories one replay visits, and
// deferredReplayBatch the deferred events it reads of each.
const (
	deferredReplayRepos = 500
	deferredReplayBatch = 100
)

// deferredRetryCap bounds the wait before a deferred event whose replay
// failed is tried again.
const deferredRetryCap = 10 * time.Minute

// errDeferred is getGitRepoAndCommit's answer for an event it deferred:
// no run is written now, a replay writes them.
var errDeferred = errors.New("the event is deferred until a node of the cluster serves its repository fresh")

// errDeferredGone is a replay that lost: another replay took the event.
var errDeferredGone = errors.New("the deferred event was already replayed")

// errDeferredNeverRuns is a deferred event that can never run: its
// repository or its pull request was deleted, or its payload cannot be
// read. Its replay drops it.
var errDeferredNeverRuns = errors.New("the deferred event can never run")

// deferredKick wakes this node's replay when one of its requests defers an
// event: the node may serve the repository fresh a moment later.
var deferredKick = make(chan struct{}, 1)

func kickDeferredReplay() {
	select {
	case deferredKick <- struct{}{}:
	default:
	}
}

type deferredReplayKey struct{}

// isDeferredReplay reports whether ctx replays a deferred event: its ref is
// resolved at once, the node serving the repository fresh.
func isDeferredReplay(ctx context.Context) bool {
	_, ok := ctx.Value(deferredReplayKey{}).(bool)
	return ok
}

// beforeDeferredReplayCommit runs in the transaction of a replay once the
// event's runs are written; a test fails it, as a node that stops before
// the commit would.
var beforeDeferredReplayCommit = func(ctx context.Context, row *actions_model.ActionDeferredNotification) error {
	return nil
}

// deferredPayload returns an empty payload of the type the event carries,
// which a deferred event's JSON decodes into; false for an event that is
// never deferred.
func deferredPayload(event webhook_module.HookEventType) (api.Payloader, bool) {
	switch event {
	case webhook_module.HookEventIssues,
		webhook_module.HookEventIssueAssign,
		webhook_module.HookEventIssueLabel,
		webhook_module.HookEventIssueMilestone:
		return &api.IssuePayload{}, true
	case webhook_module.HookEventIssueComment,
		webhook_module.HookEventPullRequestComment:
		return &api.IssueCommentPayload{}, true
	case webhook_module.HookEventPullRequest,
		webhook_module.HookEventPullRequestSync,
		webhook_module.HookEventPullRequestAssign,
		webhook_module.HookEventPullRequestLabel,
		webhook_module.HookEventPullRequestReviewRequest,
		webhook_module.HookEventPullRequestMilestone,
		webhook_module.HookEventPullRequestReviewApproved,
		webhook_module.HookEventPullRequestReviewRejected,
		webhook_module.HookEventPullRequestReviewComment:
		return &api.PullRequestPayload{}, true
	case webhook_module.HookEventRelease:
		return &api.ReleasePayload{}, true
	case webhook_module.HookEventPackage:
		return &api.PackagePayload{}, true
	case webhook_module.HookEventWiki:
		return &api.WikiPayload{}, true
	case webhook_module.HookEventCreate:
		return &api.CreatePayload{}, true
	case webhook_module.HookEventDelete:
		return &api.DeletePayload{}, true
	case webhook_module.HookEventFork:
		return &api.ForkPayload{}, true
	case webhook_module.HookEventRepository:
		return &api.RepositoryPayload{}, true
	case webhook_module.HookEventPush:
		return &api.PushPayload{}, true
	}
	return nil, false
}

// deferNotification records input as a deferred event of its repository,
// why naming the reason, and wakes this node's replay. It refuses, and the
// caller resolves the event on the node as it is, when the deferral is
// off (ACTIONS_DEFERRED_MAX_EVENTS = 0), when ACTIONS_DEFERRED_MAX_EVENTS
// events are deferred already, or when the event cannot be replayed.
func deferNotification(ctx context.Context, input *NotifyInput, why string) error {
	limit := setting.Cluster.ActionsDeferredMaxEvents
	if limit <= 0 {
		return errors.New("the deferral of events is off (ACTIONS_DEFERRED_MAX_EVENTS = 0)")
	}
	if _, ok := deferredPayload(input.Event); !ok {
		return fmt.Errorf("a %s event is never deferred", input.Event)
	}
	count, err := actions_model.CountDeferredNotifications(ctx)
	if err != nil {
		return fmt.Errorf("counting the deferred events: %w", err)
	}
	if count >= int64(limit) {
		return fmt.Errorf("%d events are deferred already, the bound ACTIONS_DEFERRED_MAX_EVENTS sets", count)
	}
	var payload []byte
	if input.Payload != nil {
		if payload, err = json.Marshal(input.Payload); err != nil {
			return fmt.Errorf("encoding the payload: %w", err)
		}
	}
	row := &actions_model.ActionDeferredNotification{
		RepoID:  input.Repo.ID,
		DoerID:  input.Doer.ID,
		Event:   input.Event,
		Ref:     input.Ref.String(),
		Payload: string(payload),
	}
	if input.PullRequest != nil {
		row.PullRequestID = input.PullRequest.ID
	}
	if err := actions_model.InsertDeferredNotification(ctx, row); err != nil {
		return fmt.Errorf("recording the deferred event: %w", err)
	}
	log.Warn("Actions: the %s event of %s is deferred (%d): %s; its workflows run once a node of the cluster serves the repository at the cluster's commit index",
		input.Event, input.Repo.FullName(), row.ID, why)
	kickDeferredReplay()
	return nil
}

// deferredBehind reports whether input's repository has deferred events,
// which input must then wait behind to keep the repository's order. Only
// in a cluster: outside one nothing is ever deferred.
func deferredBehind(ctx context.Context, input *NotifyInput) bool {
	if !clusterhook.Enabled() {
		return false
	}
	has, err := actions_model.HasDeferredNotifications(ctx, input.Repo.ID)
	if err != nil {
		log.Error("Actions: reading the deferred events of %s: %v", input.Repo.FullName(), err)
		return false
	}
	return has
}

// StartDeferredReplayer starts this node's replay of the deferred events:
// a pass at once, then one every deferredReplayEvery and whenever a
// request of this node defers an event. Outside a cluster only the first
// pass runs, for the events a node left deferred when it left the
// cluster. stop ends the replay and waits for it.
func StartDeferredReplayer(ctx context.Context) (stop func()) {
	ctx, cancel := context.WithCancel(ctx)
	var wg sync.WaitGroup
	wg.Go(func() { runDeferredReplayer(ctx) })
	return func() {
		cancel()
		wg.Wait()
	}
}

func runDeferredReplayer(ctx context.Context) {
	t := time.NewTicker(deferredReplayEvery)
	defer t.Stop()
	for first := true; ; first = false {
		if first || clusterhook.Enabled() {
			ReplayDeferredNotifications(ctx)
		}
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		case <-deferredKick:
		}
	}
}

// ReplayDeferredNotifications replays, oldest first, the deferred events of
// the repositories this node serves at the cluster's commit index, and
// returns how many it replayed. The events of a repository deleted since,
// or that waited longer than ACTIONS_DEFERRED_MAX_AGE, are dropped with a
// warning. A repository whose event could not be replayed keeps it, and
// its later events wait behind it, for a later pass.
func ReplayDeferredNotifications(ctx context.Context) int {
	repos, err := actions_model.FindDeferredNotificationRepos(ctx, deferredReplayRepos)
	if err != nil {
		log.Error("Actions: reading the repositories of the deferred events: %v", err)
		return 0
	}
	replayed := 0
	for _, repoID := range repos {
		if ctx.Err() != nil {
			break
		}
		replayed += replayDeferredOfRepo(ctx, repoID)
	}
	return replayed
}

// replayDeferredOfRepo replays, in order, the deferred events of one
// repository read before it asks whether this node serves the repository
// fresh: an event deferred later may follow a write the answer does not
// cover, and waits for the next pass.
func replayDeferredOfRepo(ctx context.Context, repoID int64) int {
	rows, err := actions_model.FindDeferredNotificationsOfRepo(ctx, repoID, deferredReplayBatch)
	if err != nil {
		log.Error("Actions: reading the deferred events of the repository %d: %v", repoID, err)
		return 0
	}
	repo, err := repo_model.GetRepositoryByID(ctx, repoID)
	if repo_model.IsErrRepoNotExist(err) {
		for _, row := range rows {
			dropDeferred(ctx, row, "its repository was deleted")
		}
		return 0
	}
	if err != nil {
		log.Error("Actions: loading the repository %d of deferred events: %v", repoID, err)
		return 0
	}
	maxAge := setting.Cluster.ActionsDeferredMaxAge
	live := make([]*actions_model.ActionDeferredNotification, 0, len(rows))
	for _, row := range rows {
		if maxAge > 0 && time.Since(row.CreatedUnix.AsTime()) > maxAge {
			dropDeferred(ctx, row, fmt.Sprintf("it waited longer than ACTIONS_DEFERRED_MAX_AGE (%v)", maxAge))
			continue
		}
		live = append(live, row)
	}
	if len(live) == 0 {
		return 0
	}
	// The first event of the repository holds the others behind it: while
	// it waits to be tried again, nothing of the repository is replayed.
	if head := live[0]; head.Attempts > 0 && time.Since(head.UpdatedUnix.AsTime()) < deferredRetryDelay(head.Attempts) {
		return 0
	}
	wctx, cancel := context.WithTimeout(ctx, setting.Cluster.ReadAfterWriteTimeout)
	fresh := clusterhook.WaitFresh(wctx, clusterhook.RepoKey(repo.OwnerName, repo.Name, false))
	cancel()
	if !fresh {
		log.Trace("Actions: %d deferred events of %s wait for this node to serve it fresh", len(live), repo.FullName())
		return 0
	}
	replayed := 0
	for _, row := range live {
		ran, err := replayDeferred(ctx, row)
		if errors.Is(err, errDeferredGone) {
			continue
		}
		if err != nil {
			log.Warn("Actions: replaying the deferred %s event %d of %s failed, it is tried again later and the repository's later events wait behind it: %v",
				row.Event, row.ID, repo.FullName(), err)
			if nerr := actions_model.NoteDeferredNotificationFailure(ctx, row.ID, err.Error()); nerr != nil {
				log.Error("Actions: recording the failed replay of the deferred event %d: %v", row.ID, nerr)
			}
			return replayed
		}
		if ran {
			replayed++
		}
	}
	return replayed
}

// deferredRetryDelay is how long a deferred event waits after its attempts
// failed: deferredReplayEvery, doubled at each failure, at most
// deferredRetryCap.
func deferredRetryDelay(attempts int) time.Duration {
	d := deferredReplayEvery
	for i := 1; i < attempts && d < deferredRetryCap; i++ {
		d *= 2
	}
	return min(d, deferredRetryCap)
}

// dropDeferred removes a deferred event that will never run, with a
// warning that names it and why.
func dropDeferred(ctx context.Context, row *actions_model.ActionDeferredNotification, why string) {
	removed, err := actions_model.DeleteDeferredNotification(ctx, row.ID)
	if err != nil {
		log.Error("Actions: dropping the deferred event %d: %v", row.ID, err)
		return
	}
	if removed {
		log.Warn("Actions: the deferred %s event %d of the repository %d (ref %q, deferred at %s, %d failed replays, last error %q) is dropped, its workflows never run: %s",
			row.Event, row.ID, row.RepoID, row.Ref, row.CreatedUnix.AsTime().UTC().Format(time.RFC3339), row.Attempts, row.LastError, why)
	}
}

// replayDeferred replays one deferred event in a transaction that first
// removes its row: another replay that took it already leaves nothing to
// remove (errDeferredGone), and one that is replaying it holds the row
// until it commits. The event's runs commit with the removal, or neither
// does. Notify's own failure is logged and the event consumed, as when a
// request notifies it; any other failure, a panic included, leaves the
// row for a later pass. ran is false for an event dropped because it can
// never run (its pull request deleted, its payload unreadable).
func replayDeferred(ctx context.Context, row *actions_model.ActionDeferredNotification) (ran bool, err error) {
	defer func() {
		if p := recover(); p != nil {
			ran, err = false, fmt.Errorf("panic: %v", p)
		}
	}()
	// A nested transaction that fails, and whose caller logs and swallows
	// the failure, rolls back to its savepoint, never the removal of the
	// row nor the runs written before it. The replay's mark is set outside
	// the transaction: Notify gets the transaction's own context, whose
	// after-commit hooks (a webhook's delivery) wait for the commit.
	replay := context.WithValue(ctx, deferredReplayKey{}, true)
	err = db.WithTx(db.WithNestedSavepoints(replay), func(ctx context.Context) error {
		ran = false
		taken, err := actions_model.DeleteDeferredNotification(ctx, row.ID)
		if err != nil {
			return err
		}
		if !taken {
			return errDeferredGone
		}
		input, err := deferredInput(ctx, row)
		if errors.Is(err, errDeferredNeverRuns) {
			log.Warn("Actions: the deferred %s event %d of the repository %d is dropped, its workflows never run: %v", row.Event, row.ID, row.RepoID, err)
			return nil
		} else if err != nil {
			return err
		}
		log.Info("Actions: replaying the %s event of %s deferred at %s (%d)", row.Event, input.Repo.FullName(), row.CreatedUnix.AsTime().UTC().Format(time.RFC3339), row.ID)
		if err := Notify(ctx, input); err != nil {
			log.Error("an error occurred while replaying the deferred %s event %d: %v", row.Event, row.ID, err)
		}
		ran = true
		return beforeDeferredReplayCommit(ctx, row)
	})
	if err != nil {
		return false, err
	}
	return ran, nil
}

// deferredInput rebuilds the notification of a deferred event, or says,
// wrapping errDeferredNeverRuns, why it can never run.
func deferredInput(ctx context.Context, row *actions_model.ActionDeferredNotification) (*NotifyInput, error) {
	repo, err := repo_model.GetRepositoryByID(ctx, row.RepoID)
	if repo_model.IsErrRepoNotExist(err) {
		return nil, fmt.Errorf("%w: its repository was deleted", errDeferredNeverRuns)
	} else if err != nil {
		return nil, fmt.Errorf("loading the repository: %w", err)
	}
	doer, err := user_model.GetPossibleUserByID(ctx, row.DoerID)
	if user_model.IsErrUserNotExist(err) {
		doer = user_model.NewGhostUser()
	} else if err != nil {
		return nil, fmt.Errorf("loading the doer: %w", err)
	}
	payload, ok := deferredPayload(row.Event)
	if !ok {
		return nil, fmt.Errorf("%w: a %s event cannot be replayed", errDeferredNeverRuns, row.Event)
	}
	if row.Payload == "" {
		payload = nil
	} else if err := json.Unmarshal([]byte(row.Payload), payload); err != nil {
		return nil, fmt.Errorf("%w: its payload cannot be read: %w", errDeferredNeverRuns, err)
	}
	input := NewNotifyInput(repo, doer, row.Event).WithRef(row.Ref).WithPayload(payload)
	if row.PullRequestID != 0 {
		pr, err := issues_model.GetPullRequestByID(ctx, row.PullRequestID)
		if issues_model.IsErrPullRequestNotExist(err) {
			return nil, fmt.Errorf("%w: its pull request was deleted", errDeferredNeverRuns)
		} else if err != nil {
			return nil, fmt.Errorf("loading the pull request: %w", err)
		}
		if err := pr.LoadIssue(ctx); err != nil {
			return nil, fmt.Errorf("loading the pull request's issue: %w", err)
		}
		input.PullRequest = pr
	}
	return input, nil
}
