// Copyright 2026 The Forgejo Authors. All rights reserved.
// SPDX-License-Identifier: GPL-3.0-or-later

package forgejo_migrations

import (
	"forgejo.org/modules/timeutil"

	"code.forgejo.org/xorm/xorm"
)

func init() {
	registerMigration(&Migration{
		Description: "add cluster_operator, the cluster-operator permission",
		Upgrade:     addClusterOperator,
	})
}

func addClusterOperator(x *xorm.Engine) error {
	type ClusterOperator struct {
		UserID      int64              `xorm:"pk"`
		GrantedBy   int64              `xorm:"NOT NULL DEFAULT 0"`
		OIDCSubject string             `xorm:"'oidc_subject' VARCHAR(255) NOT NULL DEFAULT ''"`
		CreatedUnix timeutil.TimeStamp `xorm:"created"`
	}
	return x.Sync(new(ClusterOperator))
}
