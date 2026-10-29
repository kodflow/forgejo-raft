// Copyright 2026 The Forgejo Authors. All rights reserved.
// SPDX-License-Identifier: GPL-3.0-or-later

package repo

import (
	"context"
	"net/http"
	"testing"

	"forgejo.org/models/db"
	git_model "forgejo.org/models/git"
	repo_model "forgejo.org/models/repo"
	"forgejo.org/models/unittest"
	"forgejo.org/modules/git"
	"forgejo.org/modules/setting"
	api "forgejo.org/modules/structs"
	"forgejo.org/modules/test"
	"forgejo.org/modules/web"
	"forgejo.org/services/contexttest"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// An alias branch of repo1, refs/heads/alias -> refs/heads/master, with the
// row Forgejo writes for it. Renamed through PATCH
// /repos/{owner}/{repo}/branches/{branch} on a node of the cluster, it
// deleted master, the default branch, on every node (#356). It is refused
// with 422 and the reason, as git branch -m refuses it outside a cluster,
// and nothing is renamed.
func TestRenameASymbolicBranchRefusedInACluster(t *testing.T) {
	unittest.PrepareTestEnv(t)
	defer test.MockVariableValue(&clusterWriteRefusal, func(context.Context) error { return nil })()
	defer test.MockVariableValue(&clusterFresh, func(context.Context, string) error { return nil })()
	defer test.MockVariableValue(&setting.Cluster.Enabled, true)()
	dir := unittest.AssertExistsAndLoadBean(t, &repo_model.Repository{ID: 1}).RepoPath()
	master, err := git.GetFullCommitID(t.Context(), dir, "refs/heads/master")
	require.NoError(t, err)
	_, _, err = git.NewCommand(t.Context(), "symbolic-ref", "refs/heads/alias", "refs/heads/master").RunStdString(&git.RunOpts{Dir: dir})
	require.NoError(t, err)
	t.Cleanup(func() {
		_, _, _ = git.NewCommand(context.Background(), "update-ref", "--no-deref", "-d", "refs/heads/alias").RunStdString(&git.RunOpts{Dir: dir})
	})
	require.NoError(t, db.Insert(db.DefaultContext, &git_model.Branch{RepoID: 1, Name: "alias", CommitID: master, CommitMessage: "alias", PusherID: 2}))

	ctx, resp := contexttest.MockAPIContext(t, "PATCH /api/v1/repos/user2/repo1/branches/alias")
	ctx.SetParams("*", "alias")
	contexttest.LoadRepo(t, ctx, 1)
	contexttest.LoadUser(t, ctx, 2)
	contexttest.LoadGitRepo(t, ctx)
	defer ctx.Repo().GitRepo.Close()
	web.SetForm(ctx, &api.UpdateBranchRepoOption{Name: "legacy"})

	UpdateBranch(ctx)

	assert.Equal(t, http.StatusUnprocessableEntity, ctx.Resp.Status())
	assert.Contains(t, resp.Body.String(), "is a symbolic ref to refs/heads/master")
	got, err := git.GetFullCommitID(t.Context(), dir, "refs/heads/master")
	require.NoError(t, err, "master is kept")
	assert.Equal(t, master, got)
	assert.False(t, ctx.Repo().GitRepo.IsBranchExist("legacy"))
	unittest.AssertExistsAndLoadBean(t, &git_model.Branch{RepoID: 1, Name: "alias"})
	unittest.AssertNotExistsBean(t, &git_model.Branch{RepoID: 1, Name: "legacy"})
}
