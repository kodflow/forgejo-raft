// Copyright 2026 The Forgejo Authors. All rights reserved.
// SPDX-License-Identifier: GPL-3.0-or-later

package actions

import (
	"context"

	"forgejo.org/models/db"
	"forgejo.org/modules/timeutil"
	webhook_module "forgejo.org/modules/webhook"
)

// ActionDeferredNotification is an Actions event that a node of a cluster
// could not resolve fresh: the event carries no commit of its own, its ref
// is resolved in the node's git, and the node did not serve the repository
// at the cluster's commit index within READ_AFTER_WRITE_TIMEOUT, or no
// leader answered. Resolved there, the ref could name the commit before a
// push another node acknowledged, whose run would cancel the acknowledged
// head's through a concurrency group (#335). The event waits here instead,
// in the database every node of the cluster shares, until a node that
// serves the repository fresh replays it. The row is removed by the
// transaction that writes the event's runs, so an event runs once, whoever
// replays it and whenever a node restarts.
type ActionDeferredNotification struct {
	ID     int64                        `xorm:"pk autoincr"`
	RepoID int64                        `xorm:"NOT NULL INDEX"`
	DoerID int64                        `xorm:"NOT NULL"`
	Event  webhook_module.HookEventType `xorm:"VARCHAR(255) NOT NULL"`
	// Ref is the event's ref as it was notified, empty for an event of the
	// default branch.
	Ref string `xorm:"TEXT"`
	// PullRequestID is the pull request the event carries, 0 for none.
	PullRequestID int64 `xorm:"NOT NULL DEFAULT 0"`
	// Payload is the event's payload in JSON, as a run records it.
	Payload string `xorm:"LONGTEXT"`
	// Attempts counts the replays that failed, LastError says why the last
	// one did.
	Attempts    int                `xorm:"NOT NULL DEFAULT 0"`
	LastError   string             `xorm:"TEXT"`
	CreatedUnix timeutil.TimeStamp `xorm:"created INDEX"`
	UpdatedUnix timeutil.TimeStamp `xorm:"updated"`
}

// TableName keeps the table's name stable.
func (ActionDeferredNotification) TableName() string { return "action_deferred_notification" }

func init() {
	db.RegisterModel(new(ActionDeferredNotification))
}

// InsertDeferredNotification records a deferred event.
func InsertDeferredNotification(ctx context.Context, n *ActionDeferredNotification) error {
	_, err := db.GetEngine(ctx).Insert(n)
	return err
}

// HasDeferredNotifications reports whether a repository has deferred
// events: a later event of the repository waits behind them, so the
// repository's events keep their order.
func HasDeferredNotifications(ctx context.Context, repoID int64) (bool, error) {
	return db.GetEngine(ctx).Where("repo_id = ?", repoID).Exist(new(ActionDeferredNotification))
}

// CountDeferredNotifications counts the deferred events of every
// repository.
func CountDeferredNotifications(ctx context.Context) (int64, error) {
	return db.GetEngine(ctx).Count(new(ActionDeferredNotification))
}

// FindDeferredNotifications returns at most limit deferred events, oldest
// first: the order they were deferred in.
func FindDeferredNotifications(ctx context.Context, limit int) ([]*ActionDeferredNotification, error) {
	var out []*ActionDeferredNotification
	err := db.GetEngine(ctx).OrderBy("id").Limit(limit).Find(&out)
	return out, err
}

// FindDeferredNotificationRepos returns at most limit repositories that
// have deferred events, the one whose oldest event is the oldest first: a
// repository whose events wait does not hold back the others.
func FindDeferredNotificationRepos(ctx context.Context, limit int) ([]int64, error) {
	var out []int64
	err := db.GetEngine(ctx).Table("action_deferred_notification").Select("repo_id").
		GroupBy("repo_id").OrderBy("MIN(id)").Limit(limit).Find(&out)
	return out, err
}

// FindDeferredNotificationsOfRepo returns at most limit deferred events of
// a repository, oldest first.
func FindDeferredNotificationsOfRepo(ctx context.Context, repoID int64, limit int) ([]*ActionDeferredNotification, error) {
	var out []*ActionDeferredNotification
	err := db.GetEngine(ctx).Where("repo_id = ?", repoID).OrderBy("id").Limit(limit).Find(&out)
	return out, err
}

// DeleteDeferredNotification removes a deferred event, in the transaction
// ctx carries when the caller replays it. It reports false when the row
// was already gone: another replay, on this node or another one, took it.
func DeleteDeferredNotification(ctx context.Context, id int64) (bool, error) {
	n, err := db.GetEngine(ctx).ID(id).Delete(new(ActionDeferredNotification))
	return n == 1, err
}

// NoteDeferredNotificationFailure records a failed replay of a deferred
// event.
func NoteDeferredNotificationFailure(ctx context.Context, id int64, reason string) error {
	_, err := db.GetEngine(ctx).Exec("UPDATE action_deferred_notification SET attempts = attempts + 1, last_error = ?, updated_unix = ? WHERE id = ?",
		reason, timeutil.TimeStampNow(), id)
	return err
}
