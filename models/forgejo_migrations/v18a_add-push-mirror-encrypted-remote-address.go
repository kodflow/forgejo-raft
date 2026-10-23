// Copyright 2026 The Forgejo Authors. All rights reserved.
// SPDX-License-Identifier: GPL-3.0-or-later

package forgejo_migrations

import (
	"code.forgejo.org/xorm/xorm"
)

func init() {
	registerMigration(&Migration{
		Description: "add encrypted_remote_address to push_mirror, so every node of a cluster pushes with the mirror's password",
		Upgrade:     addPushMirrorEncryptedRemoteAddress,
	})
}

func addPushMirrorEncryptedRemoteAddress(x *xorm.Engine) error {
	type PushMirror struct {
		EncryptedRemoteAddress []byte `xorm:"BLOB NULL"`
	}
	return x.Sync(new(PushMirror))
}
