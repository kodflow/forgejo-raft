// Copyright 2026 The Forgejo Authors. All rights reserved.
// SPDX-License-Identifier: GPL-3.0-or-later

package admin

import (
	"forgejo.org/modules/updatechecker"
	"forgejo.org/services/context"
)

// prepareRaftNotice tells, next to the upstream notice, whether the
// forgejo-raft build of the recommended official release is published.
func prepareRaftNotice(ctx *context.Context) {
	official, _ := ctx.Data["RemoteVersion"].(string)
	notice, err := updatechecker.GetRaftNotice(ctx, official)
	if err != nil {
		ctx.Data["RaftCheckerError"] = err.Error()
		return
	}
	ctx.Data["RaftNotice"] = notice
}
