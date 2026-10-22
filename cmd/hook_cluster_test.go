// Copyright 2026 The Forgejo Authors. All rights reserved.
// SPDX-License-Identifier: GPL-3.0-or-later

package cmd

import (
	"testing"

	"forgejo.org/modules/git"

	"github.com/hashicorp/go-version"
	"github.com/stretchr/testify/assert"
)

// A node of a cluster refuses a git older than 2.48 (#83).
func TestClusterGitVersionError(t *testing.T) {
	for v, ok := range map[string]bool{"2.39.5": false, "2.45.4": false, "2.47.3": false, "2.48.0": true, "2.48.2": true, "2.54.0": true, "2.56.0": true} {
		err := git.ClusterGitVersionError(version.Must(version.NewVersion(v)))
		if ok {
			assert.NoError(t, err, v)
		} else if assert.Error(t, err, v) {
			assert.Contains(t, err.Error(), v)
			assert.Contains(t, err.Error(), git.ClusterRequiredVersion)
		}
	}
	assert.Error(t, git.ClusterGitVersionError(nil))
}
