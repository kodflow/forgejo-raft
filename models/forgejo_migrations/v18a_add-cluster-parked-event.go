// Copyright 2026 The Forgejo Authors. All rights reserved.
// SPDX-License-Identifier: GPL-3.0-or-later

package forgejo_migrations

import (
	"forgejo.org/modules/timeutil"

	"code.forgejo.org/xorm/xorm"
)

func init() {
	registerMigration(&Migration{
		Description: "add cluster_parked_event, the ref replicator's events whose work is waiting",
		Upgrade:     addClusterParkedEvent,
	})
}

func addClusterParkedEvent(x *xorm.Engine) error {
	type ClusterParkedEvent struct {
		ID          int64              `xorm:"pk autoincr"`
		Lineage     string             `xorm:"VARCHAR(64) NOT NULL UNIQUE(lineage_index_seq) INDEX(lineage_index)"`
		LogIndex    int64              `xorm:"NOT NULL UNIQUE(lineage_index_seq) INDEX(lineage_index)"`
		Seq         int                `xorm:"NOT NULL DEFAULT 0 UNIQUE(lineage_index_seq)"`
		Repo        string             `xorm:"VARCHAR(255) NOT NULL INDEX"`
		Event       string             `xorm:"LONGTEXT NOT NULL"`
		Reason      string             `xorm:"TEXT"`
		Attempts    int                `xorm:"NOT NULL DEFAULT 0"`
		CreatedUnix timeutil.TimeStamp `xorm:"created"`
		UpdatedUnix timeutil.TimeStamp `xorm:"updated"`
	}
	return x.Sync(new(ClusterParkedEvent))
}
