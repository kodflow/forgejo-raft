// Copyright 2026 The Forgejo Authors. All rights reserved.
// SPDX-License-Identifier: GPL-3.0-or-later

package repo

import (
	"net/http"

	cluster_service "forgejo.org/services/cluster"
	"forgejo.org/services/context"
)

// clusterWriteRefusal is the cluster's answer to a ref write; tests replace
// it.
var clusterWriteRefusal = cluster_service.WriteRefusal

// refWritesRefused answers 503 with the reason when this node of the
// cluster cannot write refs now (no leader with a majority), and reports
// whether it did. Without it, the write fails deep in git and the client
// reads a 500.
func refWritesRefused(ctx *context.APIContext) bool {
	if err := clusterWriteRefusal(ctx); err != nil {
		ctx.Error(http.StatusServiceUnavailable, "", err.Error())
		return true
	}
	return false
}
