// Copyright 2026 The Forgejo Authors. All rights reserved.
// SPDX-License-Identifier: GPL-3.0-or-later

package repository

import (
	"testing"

	"forgejo.org/modules/git"
	"forgejo.org/modules/setting"
	"forgejo.org/modules/test"

	"github.com/stretchr/testify/assert"
)

// In a cluster, objects reach a member before the ref naming them commits:
// a gc that prunes them early would drop them. Every --prune but never is
// ignored, an age as short as one second included, and git's default expiry
// is passed, whatever [git.config] sets, unless it sets never.
func TestClusterGCArgsKeepUncommittedObjects(t *testing.T) {
	config, got := clusterGCArgs(git.TrustedCmdArgs{"--auto", "--prune=now", "--aggressive", "--prune=all", "--prune=1.second.ago", "--prune=never"})
	assert.Equal(t, git.TrustedCmdArgs{"--auto", "--aggressive", "--prune=never"}, got)
	assert.Equal(t, git.TrustedCmdArgs{"-c", "gc.pruneExpire=2.weeks.ago"}, config)

	defer test.MockVariableValue(&setting.GitConfig.Options, map[string]string{"gc.pruneexpire": "now"})()
	config, _ = clusterGCArgs(nil)
	assert.Equal(t, git.TrustedCmdArgs{"-c", "gc.pruneExpire=2.weeks.ago"}, config, "a short expiry set in [git.config] is overridden")
	setting.GitConfig.Options["gc.pruneexpire"] = "never"
	config, _ = clusterGCArgs(nil)
	assert.Equal(t, git.TrustedCmdArgs{"-c", "gc.pruneExpire=never"}, config)
}
