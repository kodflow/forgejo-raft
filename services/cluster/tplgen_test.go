// Copyright 2026 The Forgejo Authors. All rights reserved.
// SPDX-License-Identifier: GPL-3.0-or-later

package cluster

import (
	"testing"

	"forgejo.org/models/db"
	repo_model "forgejo.org/models/repo"
	"forgejo.org/models/unittest"
	user_model "forgejo.org/models/user"
	repo_service "forgejo.org/services/repository"

	"github.com/stretchr/testify/require"
)

// A generation from a template that fails after `git init` takes its key
// out of the table as a failed creation does: every member had created the
// directory, and kept the name taken (#102). The template is empty: a push
// of git content runs Forgejo's receive hooks, a whole test binary each in
// this harness, and the cleanup is the same once git wrote the key.
func TestHuntR7ProveTplgenFailedGenerationLeavesNoLiveKey(t *testing.T) {
	require.NoError(t, unittest.PrepareTestDatabase())
	tc := newTestCluster(t, "node-a", "node-b", "node-c")
	for _, n := range tc.nodes {
		n.r.objects.repoKnown = n.r.repoKnown
	}
	withGitThroughReplicator(t, tc.nodes[0])
	ctx := db.DefaultContext
	owner := unittest.AssertExistsAndLoadBean(t, &user_model.User{ID: 4})

	tpl, err := repo_service.CreateRepositoryDirectly(ctx, owner, owner, repo_service.CreateRepoOptions{Name: "tpl", IsTemplate: true})
	require.NoError(t, err)
	requireLiveEverywhere(t, tc, "user4/tpl.git")
	// The template's avatar names a file that is gone: the generation
	// fails after `git init`.
	tpl.Avatar = "an-avatar-whose-file-is-gone"
	_, err = db.GetEngine(ctx).ID(tpl.ID).Cols("avatar").Update(tpl)
	require.NoError(t, err)

	_, err = repo_service.GenerateRepository(ctx, owner, owner, tpl, repo_service.GenerateRepoOptions{Name: "gen", Avatar: true})
	require.Error(t, err)
	unittest.AssertNotExistsBean(t, &repo_model.Repository{OwnerID: 4, LowerName: "gen"})
	requireGoneEverywhere(t, tc, "user4/gen.git")

	// The name is free again, on every member.
	_, err = repo_service.CreateRepositoryDirectly(ctx, owner, owner, repo_service.CreateRepoOptions{Name: "gen"})
	require.NoError(t, err)
	requireLiveEverywhere(t, tc, "user4/gen.git")
}
