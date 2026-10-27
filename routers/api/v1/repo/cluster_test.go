// Copyright 2026 The Forgejo Authors. All rights reserved.
// SPDX-License-Identifier: GPL-3.0-or-later

package repo

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"testing"

	"forgejo.org/models/db"
	git_model "forgejo.org/models/git"
	repo_model "forgejo.org/models/repo"
	"forgejo.org/models/unittest"
	"forgejo.org/modules/clusterhook"
	"forgejo.org/modules/git"
	"forgejo.org/modules/gitrepo"
	api "forgejo.org/modules/structs"
	"forgejo.org/modules/test"
	"forgejo.org/modules/web"
	"forgejo.org/services/auth"
	"forgejo.org/services/authz"
	"forgejo.org/services/contexttest"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// A branch created through the API on a node of the cluster that reaches no
// majority is refused with 503 and the reason, not a 500 from deep in git,
// and nothing is written.
func TestCreateBranchRefusedByTheCluster(t *testing.T) {
	unittest.PrepareTestEnv(t)
	defer test.MockVariableValue(&clusterWriteRefusal, func(context.Context) error {
		return errors.New("this node of the cluster reaches no leader with a majority; nothing was written")
	})()

	ctx, resp := contexttest.MockAPIContext(t, "POST /api/v1/repos/user2/repo1/branches")
	contexttest.LoadRepo(t, ctx, 1)
	contexttest.LoadUser(t, ctx, 2)
	contexttest.LoadGitRepo(t, ctx)
	defer ctx.Repo().GitRepo.Close()
	web.SetForm(ctx, &api.CreateBranchRepoOption{BranchName: "refused-by-the-cluster", OldRefName: "master"})

	CreateBranch(ctx)

	assert.Equal(t, http.StatusServiceUnavailable, ctx.Resp.Status())
	assert.Contains(t, resp.Body.String(), "nothing was written")
	assert.False(t, ctx.Repo().GitRepo.IsBranchExist("refused-by-the-cluster"))
}

// behind is the refusal of a node of the cluster behind on the repository.
func behind(_ context.Context, key string) error {
	return fmt.Errorf("%w to %s; nothing was written, retry shortly or through another node", clusterhook.ErrBehind, key)
}

// A tag created through the API on a node of the cluster behind on the
// repository is refused with 503 before its target is read from the
// node's git, and nothing is written (#189).
func TestCreateTagRefusedOnANodeBehind(t *testing.T) {
	unittest.PrepareTestEnv(t)
	defer test.MockVariableValue(&clusterWriteRefusal, func(context.Context) error { return nil })()
	defer test.MockVariableValue(&clusterFresh, behind)()

	ctx, resp := contexttest.MockAPIContext(t, "POST /api/v1/repos/user2/repo1/tags")
	contexttest.LoadRepo(t, ctx, 1)
	contexttest.LoadUser(t, ctx, 2)
	contexttest.LoadGitRepo(t, ctx)
	defer ctx.Repo().GitRepo.Close()
	web.SetForm(ctx, &api.CreateTagOption{TagName: "refused-behind", Target: "master"})

	CreateTag(ctx)

	assert.Equal(t, http.StatusServiceUnavailable, ctx.Resp.Status())
	assert.Contains(t, resp.Body.String(), "may be behind")
	assert.False(t, ctx.Repo().GitRepo.IsTagExist("refused-behind"))
}

// A branch renamed through the API on a node of the cluster behind on the
// repository is refused with 503, and nothing is renamed (#189).
func TestRenameBranchRefusedOnANodeBehind(t *testing.T) {
	unittest.PrepareTestEnv(t)
	defer test.MockVariableValue(&clusterWriteRefusal, func(context.Context) error { return nil })()
	defer test.MockVariableValue(&clusterFresh, behind)()

	ctx, resp := contexttest.MockAPIContext(t, "PATCH /api/v1/repos/user2/repo1/branches/branch2")
	ctx.SetParams("*", "branch2")
	contexttest.LoadRepo(t, ctx, 1)
	contexttest.LoadUser(t, ctx, 2)
	contexttest.LoadGitRepo(t, ctx)
	defer ctx.Repo().GitRepo.Close()
	web.SetForm(ctx, &api.UpdateBranchRepoOption{Name: "renamed-behind"})

	UpdateBranch(ctx)

	assert.Equal(t, http.StatusServiceUnavailable, ctx.Resp.Status())
	assert.Contains(t, resp.Body.String(), "may be behind")
	assert.True(t, ctx.Repo().GitRepo.IsBranchExist("branch2"))
	assert.False(t, ctx.Repo().GitRepo.IsBranchExist("renamed-behind"))
}

// A branch created through the API on a node of the cluster behind on the
// repository is refused with 503 before its source is read from the node's
// git: a source the node does not show yet is not answered 500, and nothing
// is written (#226).
func TestCreateBranchRefusedOnANodeBehind(t *testing.T) {
	unittest.PrepareTestEnv(t)
	defer test.MockVariableValue(&clusterWriteRefusal, func(context.Context) error { return nil })()
	defer test.MockVariableValue(&clusterFresh, behind)()

	ctx, resp := contexttest.MockAPIContext(t, "POST /api/v1/repos/user2/repo1/branches")
	contexttest.LoadRepo(t, ctx, 1)
	contexttest.LoadUser(t, ctx, 2)
	contexttest.LoadGitRepo(t, ctx)
	defer ctx.Repo().GitRepo.Close()
	web.SetForm(ctx, &api.CreateBranchRepoOption{BranchName: "refused-behind", OldRefName: "pushed-through-another-node"})

	CreateBranch(ctx)

	assert.Equal(t, http.StatusServiceUnavailable, ctx.Resp.Status())
	assert.Contains(t, resp.Body.String(), "may be behind")
	assert.False(t, ctx.Repo().GitRepo.IsBranchExist("refused-behind"))
}

// The source of a branch created through the API is read once the node
// serves the repository at the cluster's commit index: the push another
// node acknowledged, which this node projects while it waits, is the
// source, not the commit before it (#226).
func TestCreateBranchReadsItsSourceOnceFresh(t *testing.T) {
	unittest.PrepareTestEnv(t)
	defer test.MockVariableValue(&clusterWriteRefusal, func(context.Context) error { return nil })()

	for _, opt := range []*api.CreateBranchRepoOption{
		{BranchName: "from-ref", OldRefName: "master"},
		{BranchName: "from-branch", OldBranchName: "master"},
		{BranchName: "from-default"},
	} {
		ctx, resp := contexttest.MockAPIContext(t, "POST /api/v1/repos/user2/repo1/branches")
		contexttest.LoadRepo(t, ctx, 1)
		contexttest.LoadUser(t, ctx, 2)
		contexttest.LoadGitRepo(t, ctx)
		// What the API's authorization sets for a token with full access.
		ctx.SetReducer(&authz.AllAccessAuthorizationReducer{})
		gitRepo := ctx.Repo().GitRepo
		dir := ctx.Repo().Repository.RepoPath()
		setMaster := func(c context.Context, sha string) error {
			_, _, err := git.NewCommand(c, "update-ref", "refs/heads/master").AddDynamicArguments(sha).RunStdString(&git.RunOpts{Dir: dir})
			return err
		}
		before, err := gitRepo.GetBranchCommitID("branch2")
		require.NoError(t, err)
		pushed, err := gitRepo.GetBranchCommitID("master")
		require.NoError(t, err)
		require.NotEqual(t, before, pushed)
		// This node's git shows master before the push; the push reaches
		// it while it waits to serve the repository at the cluster's
		// commit index.
		require.NoError(t, setMaster(t.Context(), before))
		restore := test.MockVariableValue(&clusterFresh, func(c context.Context, _ string) error { return setMaster(c, pushed) })
		web.SetForm(ctx, opt)

		CreateBranch(ctx)

		restore()
		require.Equal(t, http.StatusCreated, ctx.Resp.Status(), "%s: %s", opt.BranchName, resp.Body.String())
		got, err := gitRepo.GetBranchCommitID(opt.BranchName)
		require.NoError(t, err)
		assert.Equal(t, pushed, got, "%s starts from the commit the push acknowledged", opt.BranchName)
		gitRepo.Close()
	}
}

// Files committed through the API to a new branch, on a node of the
// cluster behind on the repository, are refused with 503 before the old
// branch is read from the node's git: the new branch would start from the
// commit before a push another node acknowledged. Nothing is written
// (#226).
func TestChangeFilesRefusedOnANodeBehind(t *testing.T) {
	unittest.PrepareTestEnv(t)
	clusterhook.SetFreshWaiter(func(context.Context, string) bool { return false })
	t.Cleanup(func() { clusterhook.SetFreshWaiter(nil) })

	ctx, resp := contexttest.MockAPIContext(t, "POST /api/v1/repos/user2/repo1/contents")
	contexttest.LoadUser(t, ctx, 2)
	contexttest.LoadRepo(t, ctx, 1)
	contexttest.LoadGitRepo(t, ctx)
	defer ctx.Repo().GitRepo.Close()
	web.SetForm(ctx, &api.ChangeFilesOptions{
		BranchName: "master", NewBranchName: "refused-behind",
		Files: []*api.ChangeFileOperation{{Operation: "create", Path: "refused.txt", ContentBase64: "cmVmdXNlZAo="}},
	})

	ChangeFiles(ctx)

	assert.Equal(t, http.StatusServiceUnavailable, ctx.Resp.Status())
	assert.Contains(t, resp.Body.String(), "may be behind")
	assert.False(t, ctx.Repo().GitRepo.IsBranchExist("refused-behind"))
}

// applyDiffPatchToANewBranch applies, through the API, a patch adding a
// file to master of repo1, on the new branch name. It returns the status,
// the body, and the new branch's parent, empty when it was not created.
func applyDiffPatchToANewBranch(t *testing.T, name string) (int, string, string) {
	t.Helper()
	ctx, resp := contexttest.MockAPIContext(t, "POST /api/v1/repos/user2/repo1/diffpatch")
	contexttest.LoadUser(t, ctx, 2)
	contexttest.LoadRepo(t, ctx, 1)
	contexttest.LoadGitRepo(t, ctx)
	defer ctx.Repo().GitRepo.Close()
	web.SetForm(ctx, &api.ApplyDiffPatchFileOptions{
		BranchName: "master", NewBranchName: name,
		Content: "diff --git a/" + name + ".txt b/" + name + ".txt\n" +
			"new file mode 100644\n" +
			"--- /dev/null\n" +
			"+++ b/" + name + ".txt\n" +
			"@@ -0,0 +1 @@\n" +
			"+patch\n",
	})

	ApplyDiffPatch(ctx)

	parent := ""
	if commit, err := ctx.Repo().GitRepo.GetBranchCommit(name); err == nil {
		if id, err := commit.ParentID(0); err == nil {
			parent = id.String()
		}
	}
	return ctx.Resp.Status(), resp.Body.String(), parent
}

// A patch applied through the API to a new branch, on a node of the
// cluster behind on the repository, is refused with 503 before the old
// branch is read from the node's git: the new branch would start from the
// commit before a push another node acknowledged. Nothing is written
// (#239).
func TestApplyDiffPatchRefusedOnANodeBehind(t *testing.T) {
	unittest.PrepareTestEnv(t)
	clusterhook.SetFreshWaiter(func(context.Context, string) bool { return false })
	t.Cleanup(func() { clusterhook.SetFreshWaiter(nil) })

	status, body, parent := applyDiffPatchToANewBranch(t, "refused-behind")

	assert.Equal(t, http.StatusServiceUnavailable, status, body)
	assert.Contains(t, body, "may be behind")
	assert.Empty(t, parent, "the new branch was written")
}

// Without a cluster the patch is applied to the new branch, on master, as
// before.
func TestApplyDiffPatchOutsideACluster(t *testing.T) {
	unittest.PrepareTestEnv(t)

	status, body, parent := applyDiffPatchToANewBranch(t, "outside-a-cluster")

	require.Equal(t, http.StatusCreated, status, body)
	repo := unittest.AssertExistsAndLoadBean(t, &repo_model.Repository{ID: 1})
	gitRepo, err := gitrepo.OpenRepository(t.Context(), repo)
	require.NoError(t, err)
	defer gitRepo.Close()
	master, err := gitRepo.GetBranchCommitID("master")
	require.NoError(t, err)
	assert.Equal(t, master, parent)
}

// A branch deleted through the API on a node of the cluster whose branch
// rows are not derived yet from the cluster's last writes is refused with
// 503 and the reason: deleted then, a branch whose push the consumer
// derived after would come back live in SQL for good. Nothing is deleted,
// in git or in SQL (#267).
func TestDeleteBranchRefusedWhileItsRowLags(t *testing.T) {
	unittest.PrepareTestEnv(t)
	defer test.MockVariableValue(&clusterWriteRefusal, func(context.Context) error { return nil })()
	defer test.MockVariableValue(&clusterFresh, func(context.Context, string) error { return nil })()
	clusterhook.SetDerivedWaiter(func(context.Context, string) error { return clusterhook.ErrBehind })
	t.Cleanup(func() { clusterhook.SetDerivedWaiter(nil) })

	ctx, resp := contexttest.MockAPIContext(t, "DELETE /api/v1/repos/user2/repo1/branches/branch2")
	ctx.SetParams("*", "branch2")
	contexttest.LoadRepo(t, ctx, 1)
	contexttest.LoadUser(t, ctx, 2)
	contexttest.LoadGitRepo(t, ctx)
	defer ctx.Repo().GitRepo.Close()

	func() {
		// A deletion hands its notifications to the push queue, which
		// this test binary does not start: it panics after the deletion.
		defer func() { _ = recover() }()
		DeleteBranch(ctx)
	}()

	assert.Equal(t, http.StatusServiceUnavailable, ctx.Resp.Status())
	assert.Contains(t, resp.Body.String(), "may be behind")
	assert.True(t, ctx.Repo().GitRepo.IsBranchExist("branch2"))
	row := unittest.AssertExistsAndLoadBean(t, &git_model.Branch{RepoID: 1, Name: "branch2"})
	assert.False(t, row.IsDeleted)
}

// The branch rows the API synchronises from this node's git when the
// repository has none are written once the node serves the repository at
// the cluster's commit index: a node behind is refused before (#267).
func TestDeleteBranchRefusedOnANodeBehindBeforeTheBranchSync(t *testing.T) {
	unittest.PrepareTestEnv(t)
	defer test.MockVariableValue(&clusterWriteRefusal, func(context.Context) error { return nil })()
	defer test.MockVariableValue(&clusterFresh, behind)()
	// A repository whose branch rows were never written.
	_, err := db.GetEngine(db.DefaultContext).Where("repo_id = ?", 1).Delete(&git_model.Branch{})
	require.NoError(t, err)

	ctx, resp := contexttest.MockAPIContext(t, "DELETE /api/v1/repos/user2/repo1/branches/branch2")
	ctx.SetParams("*", "branch2")
	contexttest.LoadRepo(t, ctx, 1)
	contexttest.LoadUser(t, ctx, 2)
	contexttest.LoadGitRepo(t, ctx)
	defer ctx.Repo().GitRepo.Close()

	func() {
		defer func() { _ = recover() }()
		DeleteBranch(ctx)
	}()

	assert.Equal(t, http.StatusServiceUnavailable, ctx.Resp.Status())
	assert.Contains(t, resp.Body.String(), "may be behind")
	assert.True(t, ctx.Repo().GitRepo.IsBranchExist("branch2"))
	unittest.AssertCount(t, &git_model.Branch{RepoID: 1}, 0)
}

// A repository generated through the API from a template on a node of the
// cluster that does not serve the template yet is refused with 503 and the
// reason, before anything is written (#261).
func TestGenerateRefusedOnANodeBehind(t *testing.T) {
	unittest.PrepareTestEnv(t)
	clusterhook.SetFreshWaiter(func(context.Context, string) bool { return false })
	t.Cleanup(func() { clusterhook.SetFreshWaiter(nil) })

	ctx, resp := contexttest.MockAPIContext(t, "POST /api/v1/repos/user27/template1/generate")
	contexttest.LoadRepo(t, ctx, 44)
	contexttest.LoadUser(t, ctx, 2)
	// Signed in without a token: no scope restricts the request.
	ctx.SetAuthentication(&auth.UnauthenticatedResult{})
	web.SetForm(ctx, &api.GenerateRepoOption{Owner: "user2", Name: "generated-behind", GitContent: true})

	Generate(ctx)

	assert.Equal(t, http.StatusServiceUnavailable, ctx.Resp.Status())
	assert.Contains(t, resp.Body.String(), "may be behind")
	unittest.AssertNotExistsBean(t, &repo_model.Repository{OwnerID: 2, LowerName: "generated-behind"})
}

// A fork through the API on a node of the cluster that does not serve its
// base yet is refused with 503 and the reason, before anything is written
// (#283).
func TestCreateForkRefusedOnANodeBehind(t *testing.T) {
	unittest.PrepareTestEnv(t)
	clusterhook.SetFreshWaiter(func(context.Context, string) bool { return false })
	t.Cleanup(func() { clusterhook.SetFreshWaiter(nil) })

	ctx, resp := contexttest.MockAPIContext(t, "POST /api/v1/repos/user2/repo1/forks")
	contexttest.LoadRepo(t, ctx, 1)
	contexttest.LoadUser(t, ctx, 4)
	// Signed in without a token: no scope restricts the request.
	ctx.SetAuthentication(&auth.UnauthenticatedResult{})
	name := "forked-behind"
	web.SetForm(ctx, &api.CreateForkOption{Name: &name})

	CreateFork(ctx)

	assert.Equal(t, http.StatusServiceUnavailable, ctx.Resp.Status())
	assert.Contains(t, resp.Body.String(), "may be behind")
	unittest.AssertNotExistsBean(t, &repo_model.Repository{OwnerID: 4, LowerName: "forked-behind"})
}
