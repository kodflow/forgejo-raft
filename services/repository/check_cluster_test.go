// Copyright 2026 The Forgejo Authors. All rights reserved.
// SPDX-License-Identifier: GPL-3.0-or-later

package repository

import (
	"testing"

	"forgejo.org/modules/git"

	"github.com/stretchr/testify/assert"
)

// In a cluster, objects reach a member before the ref naming them commits:
// a gc that prunes at once would drop them. Such a prune is ignored; the
// other arguments, and a prune with an age, are kept.
func TestClusterGCArgsKeepUncommittedObjects(t *testing.T) {
	got := clusterGCArgs(git.TrustedCmdArgs{"--auto", "--prune=now", "--aggressive", "--prune=all", "--prune=2.weeks.ago"})
	assert.Equal(t, git.TrustedCmdArgs{"--auto", "--aggressive", "--prune=2.weeks.ago"}, got)
}
