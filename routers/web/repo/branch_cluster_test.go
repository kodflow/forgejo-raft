// Copyright 2026 The Forgejo Authors. All rights reserved.
// SPDX-License-Identifier: GPL-3.0-or-later

package repo

import (
	"context"
	"net/http"
	"testing"

	git_model "forgejo.org/models/git"
	repo_model "forgejo.org/models/repo"
	"forgejo.org/models/unittest"
	"forgejo.org/modules/clusterhook"
	"forgejo.org/modules/git"
	"forgejo.org/modules/web"
	"forgejo.org/services/contexttest"
	"forgejo.org/services/forms"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// createBranchFromMaster runs the web form "create branch" from the
// branch master of repo1, viewed.
func createBranchFromMaster(t *testing.T, name string) (*http.Response, string, bool) {
	t.Helper()
	ctx, resp := contexttest.MockContext(t, "user2/repo1/branches/_new/branch/master")
	contexttest.LoadUser(t, ctx, 2)
	contexttest.LoadRepo(t, ctx, 1)
	contexttest.LoadGitRepo(t, ctx)
	defer ctx.Repo.GitRepo.Close()
	ctx.Repo.BranchName, ctx.Repo.IsViewBranch = "master", true
	web.SetForm(ctx, &forms.NewBranchForm{NewBranchName: name})

	CreateBranch(ctx)

	return resp.Result(), resp.Body.String(), ctx.Repo.GitRepo.IsBranchExist(name)
}

// The web form's "create branch from branch" reads the source in the
// branch's SQL row. On a node of the cluster whose rows are not derived
// yet from the cluster's last writes, it is refused with 503 and the
// reason, and nothing is written (#226).
func TestCreateBranchFromABranchRefusedWhileItsRowLags(t *testing.T) {
	unittest.PrepareTestEnv(t)
	clusterhook.SetDerivedWaiter(func(context.Context, string) error { return clusterhook.ErrBehind })
	t.Cleanup(func() { clusterhook.SetDerivedWaiter(nil) })

	resp, body, created := createBranchFromMaster(t, "refused-behind")

	assert.Equal(t, http.StatusServiceUnavailable, resp.StatusCode)
	assert.Contains(t, body, "may be behind")
	assert.False(t, created)
}

// Without a cluster the form creates the branch from the row, as before.
func TestCreateBranchFromABranchOutsideACluster(t *testing.T) {
	unittest.PrepareTestEnv(t)

	resp, _, created := createBranchFromMaster(t, "outside-a-cluster")

	require.Equal(t, http.StatusSeeOther, resp.StatusCode)
	assert.True(t, created)
}

// A branch deleted from the branch list on a node of the cluster whose
// branch rows are not derived yet from the cluster's last writes is
// refused, and the page says so where it reads the answer, the redirect's
// flash: nothing is deleted, in git or in SQL (#267).
func TestDeleteBranchRefusedWhileItsRowLags(t *testing.T) {
	unittest.PrepareTestEnv(t)
	clusterhook.SetDerivedWaiter(func(context.Context, string) error { return clusterhook.ErrBehind })
	t.Cleanup(func() { clusterhook.SetDerivedWaiter(nil) })

	ctx, _ := contexttest.MockContext(t, "user2/repo1/branches/delete?name=branch2")
	contexttest.LoadUser(t, ctx, 2)
	contexttest.LoadRepo(t, ctx, 1)
	contexttest.LoadGitRepo(t, ctx)
	defer ctx.Repo.GitRepo.Close()

	func() {
		// A deletion hands its notifications to the push queue, which
		// this test binary does not start: it panics after the deletion.
		defer func() { _ = recover() }()
		DeleteBranchPost(ctx)
	}()

	assert.EqualValues(t, ctx.Tr("repo.branch.deletion_behind", "branch2"), ctx.Flash.ErrorMsg)
	assert.Empty(t, ctx.Flash.SuccessMsg)
	assert.True(t, ctx.Repo.GitRepo.IsBranchExist("branch2"))
	row := unittest.AssertExistsAndLoadBean(t, &git_model.Branch{RepoID: 1, Name: "branch2"})
	assert.False(t, row.IsDeleted)
}

// The "Delete branch" button of a merged pull request deletes its head
// branch only when it holds no commit beyond the merged head. On a node
// of the cluster behind a push to the branch another node acknowledged,
// the comparison read the commit before the push, passed, and the
// deletion, which waits for this node to serve the push, deleted the
// branch with the pushed commit. The comparison now waits too: the push,
// which this node projects while it waits, keeps the branch (#267).
func TestCleanUpPullRequestReadsTheHeadBranchOnceDerived(t *testing.T) {
	unittest.PrepareTestEnv(t)
	repo := unittest.AssertExistsAndLoadBean(t, &repo_model.Repository{ID: 1})
	dir := repo.RepoPath()
	merged, err := git.GetFullCommitID(t.Context(), dir, "branch2")
	require.NoError(t, err)
	pushed, err := git.GetFullCommitID(t.Context(), dir, "master")
	require.NoError(t, err)
	require.NotEqual(t, merged, pushed)
	setRef := func(c context.Context, ref, sha string) error {
		_, _, err := git.NewCommand(c, "update-ref").AddDynamicArguments(ref, sha).RunStdString(&git.RunOpts{Dir: dir})
		return err
	}
	// Pull request 1, index 2 of repo1, merged branch1 at its head.
	require.NoError(t, setRef(t.Context(), "refs/pull/2/head", merged))
	require.NoError(t, setRef(t.Context(), "refs/heads/branch1", merged))
	// The push of branch1 another node acknowledged reaches this node
	// while it waits to serve the repository.
	var moved bool
	clusterhook.SetDerivedWaiter(func(c context.Context, _ string) error {
		if !moved {
			moved = true
			return setRef(c, "refs/heads/branch1", pushed)
		}
		return nil
	})
	t.Cleanup(func() { clusterhook.SetDerivedWaiter(nil) })

	ctx, _ := contexttest.MockContext(t, "user2/repo1/pulls/2/cleanup")
	contexttest.LoadUser(t, ctx, 2)
	contexttest.LoadRepo(t, ctx, 1)
	contexttest.LoadGitRepo(t, ctx)
	defer ctx.Repo.GitRepo.Close()
	ctx.SetParams(":index", "2")

	func() {
		// A deletion hands its notifications to the push queue, which
		// this test binary does not start: it panics after the deletion.
		defer func() { _ = recover() }()
		CleanUpPullRequest(ctx)
	}()

	assert.True(t, moved, "the node waited to serve the repository")
	got, err := git.GetFullCommitID(t.Context(), dir, "refs/heads/branch1")
	require.NoError(t, err, "branch1 was deleted with the pushed commit")
	assert.Equal(t, pushed, got)
	assert.EqualValues(t, ctx.Tr("repo.branch.delete_branch_has_new_commits", "user2/branch1"), ctx.Flash.ErrorMsg)
}
