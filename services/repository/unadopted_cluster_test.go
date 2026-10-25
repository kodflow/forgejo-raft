// Copyright 2026 The Forgejo Authors. All rights reserved.
// SPDX-License-Identifier: GPL-3.0-or-later

package repository

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"forgejo.org/models/db"
	repo_model "forgejo.org/models/repo"
	"forgejo.org/models/unittest"
	user_model "forgejo.org/models/user"
	"forgejo.org/modules/clusterhook"
	"forgejo.org/modules/setting"
	"forgejo.org/modules/test"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// mockHeldCheck registers, as the replicator does, a check of the keys a
// name holds in the cluster that answers refusal, and records the keys it
// is asked. enabled is clusterhook.Enabled for the test.
func mockHeldCheck(t *testing.T, enabled bool, refusal *error) *[]string {
	t.Helper()
	was := clusterhook.Enabled()
	clusterhook.SetEnabled(enabled)
	asked := &[]string{}
	clusterhook.SetRepoHeldCheck(func(_ context.Context, keys []string) error {
		*asked = append(*asked, keys...)
		return *refusal
	})
	t.Cleanup(func() {
		clusterhook.SetEnabled(was)
		clusterhook.SetRepoHeldCheck(nil)
	})
	t.Cleanup(test.MockVariableValue(&setting.RepoRootPath, t.TempDir()))
	return asked
}

// In a cluster, a creation, a fork or a generation under a name whose key
// the cluster holds is refused as for files on the disk, before anything
// is written: the node's own disk may hold no copy of it (#245). A name
// SQL has keeps its own error, and another refusal of the cluster (no
// quorum) is returned as it is.
func TestCreationUnderANameTheClusterHolds(t *testing.T) {
	require.NoError(t, unittest.PrepareTestDatabase())
	refusal := fmt.Errorf("%w: user4/held.git, on every member", clusterhook.ErrRepoHeld)
	asked := mockHeldCheck(t, true, &refusal)
	ctx := db.DefaultContext
	owner := unittest.AssertExistsAndLoadBean(t, &user_model.User{ID: 4})
	base := unittest.AssertExistsAndLoadBean(t, &repo_model.Repository{ID: 1})
	require.NoError(t, base.LoadOwner(ctx))
	tpl := unittest.AssertExistsAndLoadBean(t, &repo_model.Repository{ID: 44})

	_, err := CreateRepositoryDirectly(ctx, owner, owner, CreateRepoOptions{Name: "held"})
	assert.True(t, repo_model.IsErrRepoFilesAlreadyExist(err), "creation: %v", err)
	assert.Equal(t, []string{"user4/held.git", "user4/held.wiki.git"}, *asked)
	_, err = ForkRepositoryIfNotExists(ctx, owner, owner, ForkRepoOptions{BaseRepo: base, Name: "held"})
	assert.True(t, repo_model.IsErrRepoFilesAlreadyExist(err), "fork: %v", err)
	_, err = GenerateRepository(ctx, owner, owner, tpl, GenerateRepoOptions{Name: "held", GitContent: true})
	assert.True(t, repo_model.IsErrRepoFilesAlreadyExist(err), "generation: %v", err)
	unittest.AssertNotExistsBean(t, &repo_model.Repository{OwnerID: owner.ID, LowerName: "held"})
	assert.NoDirExists(t, repo_model.RepoPath(owner.Name, "held"))

	// A name SQL has: the cluster is not asked.
	*asked = nil
	user2 := unittest.AssertExistsAndLoadBean(t, &user_model.User{ID: 2})
	_, err = CreateRepositoryDirectly(ctx, user2, user2, CreateRepoOptions{Name: "repo1"})
	assert.True(t, repo_model.IsErrRepoAlreadyExist(err), "%v", err)
	assert.Empty(t, *asked)

	// A node that cannot confirm it applied the log.
	refusal = errors.New("the cluster did not record it: this node could not confirm it applied the log")
	_, err = CreateRepositoryDirectly(ctx, owner, owner, CreateRepoOptions{Name: "held"})
	require.ErrorContains(t, err, "could not confirm it applied the log")
	assert.False(t, repo_model.IsErrRepoFilesAlreadyExist(err))
	unittest.AssertNotExistsBean(t, &repo_model.Repository{OwnerID: owner.ID, LowerName: "held"})
}

// Without a cluster the creation does not ask it, and is unchanged.
func TestCreationWithoutAClusterDoesNotAskIt(t *testing.T) {
	require.NoError(t, unittest.PrepareTestDatabase())
	var refusal error = clusterhook.ErrRepoHeld
	asked := mockHeldCheck(t, false, &refusal)
	owner := unittest.AssertExistsAndLoadBean(t, &user_model.User{ID: 4})
	repo, err := CreateRepositoryDirectly(db.DefaultContext, owner, owner, CreateRepoOptions{Name: "plain"})
	require.NoError(t, err)
	assert.Empty(t, *asked)
	assert.DirExists(t, repo.RepoPath())
}

// In a cluster, the files of an unadopted repository the cluster holds are
// not deleted from one node: the refusal names the procedure that deletes
// it from the cluster (#245). Files the cluster does not hold are, and
// without a cluster nothing changes.
func TestDeleteUnadoptedRepositoryInACluster(t *testing.T) {
	require.NoError(t, unittest.PrepareTestDatabase())
	refusal := fmt.Errorf("%w: user2/orphan.git; delete it with `forgejo cluster reconcile --delete-orphan user2/orphan --expect-index 7 --reason \"...\"`", clusterhook.ErrRepoHeld)
	asked := mockHeldCheck(t, true, &refusal)
	ctx := db.DefaultContext
	admin := unittest.AssertExistsAndLoadBean(t, &user_model.User{ID: 1})
	owner := unittest.AssertExistsAndLoadBean(t, &user_model.User{ID: 2})
	dir := filepath.Join(setting.RepoRootPath, "user2", "orphan.git")
	require.NoError(t, os.MkdirAll(dir, 0o755))

	err := DeleteUnadoptedRepository(ctx, admin, owner, "orphan")
	require.ErrorIs(t, err, ErrUnadoptedInCluster)
	require.ErrorContains(t, err, "--delete-orphan user2/orphan")
	assert.DirExists(t, dir)
	assert.Equal(t, []string{"user2/orphan.git"}, *asked)

	refusal = errors.New("the cluster did not record it: this node could not confirm it applied the log")
	err = DeleteUnadoptedRepository(ctx, admin, owner, "orphan")
	require.Error(t, err)
	require.NotErrorIs(t, err, ErrUnadoptedInCluster)
	assert.DirExists(t, dir)

	refusal = nil
	require.NoError(t, DeleteUnadoptedRepository(ctx, admin, owner, "orphan"))
	assert.NoDirExists(t, dir)

	clusterhook.SetRepoHeldCheck(nil)
	clusterhook.SetEnabled(false)
	require.NoError(t, os.MkdirAll(dir, 0o755))
	require.NoError(t, DeleteUnadoptedRepository(ctx, admin, owner, "orphan"))
	assert.NoDirExists(t, dir)
}
