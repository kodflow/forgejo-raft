// Copyright 2026 The Forgejo Authors. All rights reserved.
// SPDX-License-Identifier: GPL-3.0-or-later

package repo

import (
	"errors"
	"net/http"

	"forgejo.org/modules/clusterhook"
	cluster_service "forgejo.org/services/cluster"
	"forgejo.org/services/context"
)

// clusterWriteRefusal is the cluster's answer to a ref write, and
// clusterFresh its wait for this node to serve the repository at the
// cluster's commit index; tests replace them.
var (
	clusterWriteRefusal = cluster_service.WriteRefusal
	clusterFresh        = clusterhook.RequireFresh
)

// refWritesRefused answers 503 with the reason when this node of the
// cluster cannot write refs now (no leader with a majority), or may be
// behind the cluster's last writes to the repository, and reports whether
// it did. Without it, the write fails deep in git and the client reads a
// 500; or a node behind decides from its own git, which does not show yet
// the refs another node wrote (#189, #190).
func refWritesRefused(ctx *context.APIContext) bool {
	if err := clusterWriteRefusal(ctx); err != nil {
		ctx.Error(http.StatusServiceUnavailable, "", err.Error())
		return true
	}
	repo := ctx.Repo().Repository
	if err := clusterFresh(ctx, clusterhook.RepoKey(repo.OwnerName, repo.Name, false)); err != nil {
		ctx.Error(http.StatusServiceUnavailable, "", err.Error())
		return true
	}
	return false
}

// clusterBehind answers 503 when err is a write this node refused because
// it may be behind the cluster's last writes to the repository, and
// reports whether it did: the client retries, through this node or
// another.
func clusterBehind(ctx *context.APIContext, err error) bool {
	if errors.Is(err, clusterhook.ErrBehind) {
		ctx.Error(http.StatusServiceUnavailable, "", err.Error())
		return true
	}
	return false
}
