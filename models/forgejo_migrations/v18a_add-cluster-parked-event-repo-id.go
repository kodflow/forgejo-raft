// Copyright 2026 The Forgejo Authors. All rights reserved.
// SPDX-License-Identifier: GPL-3.0-or-later

package forgejo_migrations

import (
	"code.forgejo.org/xorm/xorm"
)

func init() {
	registerMigration(&Migration{
		Description: "add repo_id to cluster_parked_event, so a rename keeps a repository's parked order",
		Upgrade:     addClusterParkedEventRepoID,
	})
}

func addClusterParkedEventRepoID(x *xorm.Engine) error {
	type ClusterParkedEvent struct {
		RepoID int64 `xorm:"NOT NULL DEFAULT 0 INDEX"`
	}
	return x.Sync(new(ClusterParkedEvent))
}
