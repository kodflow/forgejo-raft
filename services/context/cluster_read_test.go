// Copyright 2026 The Forgejo Authors. All rights reserved.
// SPDX-License-Identifier: GPL-3.0-or-later

package context

import (
	"errors"
	"testing"

	repo_model "forgejo.org/models/repo"
	"forgejo.org/modules/clusterhook"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// The web pages and the API ask the cluster about the repository's
// directory and its wiki's, with the database's ID (#82).
func TestClusterReadRefusalAsksForTheRepositoryAndItsWiki(t *testing.T) {
	require.NoError(t, ClusterReadRefusal(&repo_model.Repository{ID: 3, OwnerName: "Alice", Name: "Secret"}), "no cluster")

	asked := map[string]int64{}
	clusterhook.SetReadGuard(func(key string, repoID int64) error {
		asked[key] = repoID
		if key == "alice/secret.wiki.git" {
			return errors.New("another repository")
		}
		return nil
	})
	t.Cleanup(func() { clusterhook.SetReadGuard(nil) })

	require.Error(t, ClusterReadRefusal(&repo_model.Repository{ID: 3, OwnerName: "Alice", Name: "Secret"}))
	assert.Equal(t, map[string]int64{"alice/secret.git": 3, "alice/secret.wiki.git": 3}, asked)
	assert.NoError(t, ClusterReadRefusal(&repo_model.Repository{ID: 4, OwnerName: "bob", Name: "x"}))
}
