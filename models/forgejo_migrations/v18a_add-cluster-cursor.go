// Copyright 2026 The Forgejo Authors. All rights reserved.
// SPDX-License-Identifier: GPL-3.0-or-later

package forgejo_migrations

import (
	"code.forgejo.org/xorm/xorm"
)

func init() {
	registerMigration(&Migration{
		Description: "add cluster_cursor, the ref replicator's consumer cursors",
		Upgrade:     addClusterCursor,
	})
}

func addClusterCursor(x *xorm.Engine) error {
	type ClusterCursor struct {
		Name        string `xorm:"pk VARCHAR(64)"`
		Lineage     string `xorm:"VARCHAR(64) NOT NULL"`
		Position    int64  `xorm:"NOT NULL DEFAULT 0"`
		Closed      bool   `xorm:"NOT NULL DEFAULT false"`
		UpdatedUnix int64  `xorm:"updated"`
	}
	return x.Sync(new(ClusterCursor))
}
