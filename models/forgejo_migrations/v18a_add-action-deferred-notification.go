// Copyright 2026 The Forgejo Authors. All rights reserved.
// SPDX-License-Identifier: GPL-3.0-or-later

package forgejo_migrations

import (
	"forgejo.org/modules/timeutil"

	"code.forgejo.org/xorm/xorm"
)

func init() {
	registerMigration(&Migration{
		Description: "add action_deferred_notification, the Actions events a node of a cluster replays once it serves their repository fresh",
		Upgrade:     addActionDeferredNotification,
	})
}

func addActionDeferredNotification(x *xorm.Engine) error {
	type ActionDeferredNotification struct {
		ID            int64              `xorm:"pk autoincr"`
		RepoID        int64              `xorm:"NOT NULL INDEX"`
		DoerID        int64              `xorm:"NOT NULL"`
		Event         string             `xorm:"VARCHAR(255) NOT NULL"`
		Ref           string             `xorm:"TEXT"`
		PullRequestID int64              `xorm:"NOT NULL DEFAULT 0"`
		Payload       string             `xorm:"LONGTEXT"`
		Attempts      int                `xorm:"NOT NULL DEFAULT 0"`
		LastError     string             `xorm:"TEXT"`
		CreatedUnix   timeutil.TimeStamp `xorm:"created INDEX"`
		UpdatedUnix   timeutil.TimeStamp `xorm:"updated"`
	}
	return x.Sync(new(ActionDeferredNotification))
}
