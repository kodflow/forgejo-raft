// Copyright 2026 The Forgejo Authors. All rights reserved.
// SPDX-License-Identifier: GPL-3.0-or-later

package forgejo_migrations

import (
	"forgejo.org/modules/timeutil"

	"code.forgejo.org/xorm/xorm"
)

func init() {
	registerMigration(&Migration{
		Description: "add cluster_parked_resolution, the audit of parked events an operator resolved",
		Upgrade:     addClusterParkedResolution,
	})
}

func addClusterParkedResolution(x *xorm.Engine) error {
	type ClusterParkedResolution struct {
		ID           int64              `xorm:"pk autoincr"`
		Lineage      string             `xorm:"VARCHAR(64) NOT NULL INDEX(lineage_index)"`
		LogIndex     int64              `xorm:"NOT NULL INDEX(lineage_index)"`
		Seq          int                `xorm:"NOT NULL DEFAULT 0"`
		Repo         string             `xorm:"VARCHAR(255) NOT NULL"`
		Event        string             `xorm:"LONGTEXT NOT NULL"`
		LastError    string             `xorm:"TEXT"`
		Attempts     int                `xorm:"NOT NULL DEFAULT 0"`
		Operator     string             `xorm:"VARCHAR(255) NOT NULL"`
		Reason       string             `xorm:"TEXT NOT NULL"`
		ParkedUnix   timeutil.TimeStamp `xorm:"NOT NULL DEFAULT 0"`
		ResolvedUnix timeutil.TimeStamp `xorm:"created"`
	}
	return x.Sync(new(ClusterParkedResolution))
}
