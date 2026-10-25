// Copyright 2026 The Forgejo Authors. All rights reserved.
// SPDX-License-Identifier: GPL-3.0-or-later

package repo

import (
	gocontext "context"
	"net/http"
	"os"
	"path"
	"testing"

	"forgejo.org/models/db"
	repo_model "forgejo.org/models/repo"
	"forgejo.org/models/unittest"
	"forgejo.org/modules/clusterhook"
	"forgejo.org/modules/setting"
	"forgejo.org/modules/test"
	"forgejo.org/modules/web"
	"forgejo.org/services/context"
	"forgejo.org/services/contexttest"
	"forgejo.org/services/forms"

	gouuid "github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// newBranchForms are the web forms that build a new branch on master of
// repo1, viewed, through a service that refuses with ErrBehind on a node
// of the cluster behind on the repository: uploaded files, a patch, a
// cherry-picked commit. Each runs its form to the new branch name, and
// returns the response, its body, and the new branch's parent, empty when
// the branch was not created.
func newBranchForms(t *testing.T) map[string]func(name string) (int, string, string) {
	t.Helper()
	t.Cleanup(test.MockVariableValue(&setting.Repository.Upload.TempPath, t.TempDir()))

	run := func(name string, handler func(*context.Context), form any, params map[string]string) (int, string, string) {
		ctx, resp := contexttest.MockContext(t, "user2/repo1/_new/master")
		contexttest.LoadUser(t, ctx, 2)
		contexttest.LoadRepo(t, ctx, 1)
		contexttest.LoadGitRepo(t, ctx)
		defer ctx.Repo.GitRepo.Close()
		contexttest.LoadRepoCommit(t, ctx)
		ctx.Repo.BranchName, ctx.Repo.IsViewBranch = "master", true
		ctx.Repo.CommitID = ctx.Repo.Commit.ID.String()
		for k, v := range params {
			ctx.SetParams(k, v)
		}
		web.SetForm(ctx, form)

		handler(ctx)

		parent := ""
		if commit, err := ctx.Repo.GitRepo.GetBranchCommit(name); err == nil {
			if id, err := commit.ParentID(0); err == nil {
				parent = id.String()
			}
		}
		return resp.Code, resp.Body.String(), parent
	}

	return map[string]func(name string) (int, string, string){
		"UploadFilePost": func(name string) (int, string, string) {
			up := &repo_model.Upload{UUID: gouuid.New().String(), Name: name + ".txt"}
			require.NoError(t, os.MkdirAll(path.Dir(up.LocalPath()), 0o755))
			require.NoError(t, os.WriteFile(up.LocalPath(), []byte(name+"\n"), 0o644))
			require.NoError(t, db.Insert(db.DefaultContext, up))
			return run(name, UploadFilePost, &forms.UploadRepoFileForm{
				CommitChoice: frmCommitChoiceNewBranch, NewBranchName: name, CommitMailID: -1,
				Files: []string{up.UUID}, FullPaths: []string{up.Name},
			}, nil)
		},
		"NewDiffPatchPost": func(name string) (int, string, string) {
			return run(name, NewDiffPatchPost, &forms.EditRepoFileForm{
				TreePath: "patch", CommitChoice: frmCommitChoiceNewBranch, NewBranchName: name, CommitMailID: -1,
				Content: "diff --git a/" + name + ".txt b/" + name + ".txt\n" +
					"new file mode 100644\n" +
					"--- /dev/null\n" +
					"+++ b/" + name + ".txt\n" +
					"@@ -0,0 +1 @@\n" +
					"+patch\n",
			}, nil)
		},
		// The commit picked is "a change", master's child on branch2.
		"CherryPickPost": func(name string) (int, string, string) {
			return run(name, CherryPickPost, &forms.CherryPickForm{
				CommitChoice: frmCommitChoiceNewBranch, NewBranchName: name, CommitMailID: -1,
			}, map[string]string{":sha": changeOnBranch2(t)})
		},
	}
}

// changeOnBranch2 returns the parent of branch2's head in repo1: a commit
// whose own parent is master's head.
func changeOnBranch2(t *testing.T) string {
	t.Helper()
	ctx, _ := contexttest.MockContext(t, "user2/repo1")
	contexttest.LoadRepo(t, ctx, 1)
	contexttest.LoadGitRepo(t, ctx)
	defer ctx.Repo.GitRepo.Close()
	branch2, err := ctx.Repo.GitRepo.GetBranchCommit("branch2")
	require.NoError(t, err)
	change, err := branch2.ParentID(0)
	require.NoError(t, err)
	return change.String()
}

// On a node of the cluster behind on the repository, each form is refused
// with 503 and the reason, as the API and the other web writes are, and
// not rendered again with 200: nothing is written, the client retries
// (#239).
func TestNewBranchFormsRefusedOnANodeBehind(t *testing.T) {
	unittest.PrepareTestEnv(t)
	clusterhook.SetFreshWaiter(func(gocontext.Context, string) bool { return false })
	t.Cleanup(func() { clusterhook.SetFreshWaiter(nil) })

	for handler, submit := range newBranchForms(t) {
		status, body, parent := submit("refused-" + handler)

		assert.Equal(t, http.StatusServiceUnavailable, status, "%s: %s", handler, body)
		assert.Contains(t, body, "may be behind", handler)
		assert.Empty(t, parent, "%s wrote its branch", handler)
	}
}

// Without a cluster each form builds its new branch on master, and
// redirects, as before.
func TestNewBranchFormsOutsideACluster(t *testing.T) {
	unittest.PrepareTestEnv(t)
	ctx, _ := contexttest.MockContext(t, "user2/repo1")
	contexttest.LoadRepo(t, ctx, 1)
	contexttest.LoadGitRepo(t, ctx)
	master, err := ctx.Repo.GitRepo.GetBranchCommitID("master")
	require.NoError(t, err)
	ctx.Repo.GitRepo.Close()

	for handler, submit := range newBranchForms(t) {
		status, body, parent := submit("outside-" + handler)

		require.Equal(t, http.StatusSeeOther, status, "%s: %s", handler, body)
		assert.Equal(t, master, parent, handler)
	}
}
