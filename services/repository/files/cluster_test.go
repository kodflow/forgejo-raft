// Copyright 2026 The Forgejo Authors. All rights reserved.
// SPDX-License-Identifier: GPL-3.0-or-later

package files

import (
	"context"
	"os"
	"path"
	"testing"

	"forgejo.org/models/db"
	repo_model "forgejo.org/models/repo"
	"forgejo.org/models/unittest"
	user_model "forgejo.org/models/user"
	"forgejo.org/modules/clusterhook"
	"forgejo.org/modules/gitrepo"
	"forgejo.org/modules/setting"
	"forgejo.org/modules/test"

	gouuid "github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// newBranchWriters are the services that build a new branch on the old
// branch they clone from this node's git: a patch, uploaded files, a
// cherry-picked commit, each onto master of repo1, on the branch name.
func newBranchWriters(t *testing.T) map[string]func(name string) error {
	t.Helper()
	ctx := db.DefaultContext
	repo := unittest.AssertExistsAndLoadBean(t, &repo_model.Repository{ID: 1})
	doer := unittest.AssertExistsAndLoadBean(t, &user_model.User{ID: 2})
	gitRepo, err := gitrepo.OpenRepository(ctx, repo)
	require.NoError(t, err)
	defer gitRepo.Close()
	master, err := gitRepo.GetBranchCommitID("master")
	require.NoError(t, err)
	// "a change", master's child on branch2.
	branch2, err := gitRepo.GetBranchCommit("branch2")
	require.NoError(t, err)
	change, err := branch2.ParentID(0)
	require.NoError(t, err)
	t.Cleanup(test.MockVariableValue(&setting.Repository.Upload.TempPath, t.TempDir()))

	return map[string]func(name string) error{
		"ApplyDiffPatch": func(name string) error {
			_, err := ApplyDiffPatch(ctx, repo, doer, &ApplyDiffPatchOptions{
				OldBranch: "master", NewBranch: name, Message: "apply " + name,
				Content: "diff --git a/" + name + ".txt b/" + name + ".txt\n" +
					"new file mode 100644\n" +
					"--- /dev/null\n" +
					"+++ b/" + name + ".txt\n" +
					"@@ -0,0 +1 @@\n" +
					"+patch\n",
			})
			return err
		},
		// LastCommitID is what the web form passes: master's head.
		"UploadRepoFiles": func(name string) error {
			up := &repo_model.Upload{UUID: gouuid.New().String(), Name: name + ".txt"}
			require.NoError(t, os.MkdirAll(path.Dir(up.LocalPath()), 0o755))
			require.NoError(t, os.WriteFile(up.LocalPath(), []byte(name+"\n"), 0o644))
			require.NoError(t, db.Insert(ctx, up))
			return UploadRepoFiles(ctx, repo, doer, &UploadRepoFileOptions{
				LastCommitID: master, OldBranch: "master", NewBranch: name, Message: "upload " + name,
				Files: []string{up.UUID}, FullPaths: []string{up.Name},
			})
		},
		"CherryPick": func(name string) error {
			_, err := CherryPick(ctx, repo, doer, false, &ApplyDiffPatchOptions{
				OldBranch: "master", NewBranch: name, Message: "pick " + name, Content: change.String(),
			})
			return err
		},
	}
}

// parentOf returns the parent of branch name of repo1, empty when the
// branch does not exist.
func parentOf(t *testing.T, name string) string {
	t.Helper()
	repo := unittest.AssertExistsAndLoadBean(t, &repo_model.Repository{ID: 1})
	gitRepo, err := gitrepo.OpenRepository(db.DefaultContext, repo)
	require.NoError(t, err)
	defer gitRepo.Close()
	commit, err := gitRepo.GetBranchCommit(name)
	if err != nil {
		return ""
	}
	id, err := commit.ParentID(0)
	require.NoError(t, err)
	return id.String()
}

// On a node of the cluster behind on the repository, each service is
// refused with ErrBehind before it reads the old branch in the node's git:
// the new branch would start from the commit before a push another node
// acknowledged, for the whole cluster. Nothing is written (#239).
func TestNewBranchWritersRefusedOnANodeBehind(t *testing.T) {
	unittest.PrepareTestEnv(t)
	var asked []string
	clusterhook.SetFreshWaiter(func(_ context.Context, key string) bool {
		asked = append(asked, key)
		return false
	})
	t.Cleanup(func() { clusterhook.SetFreshWaiter(nil) })

	for name, write := range newBranchWriters(t) {
		err := write("refused-" + name)
		require.ErrorIs(t, err, clusterhook.ErrBehind, name)
		assert.Contains(t, err.Error(), "user2/repo1.git", name)
		assert.Empty(t, parentOf(t, "refused-"+name), "%s wrote its branch", name)
	}
	assert.Equal(t, []string{"user2/repo1.git", "user2/repo1.git", "user2/repo1.git"}, asked)
}

// Without a cluster each service builds its new branch on master, as
// before.
func TestNewBranchWritersOutsideACluster(t *testing.T) {
	unittest.PrepareTestEnv(t)
	repo := unittest.AssertExistsAndLoadBean(t, &repo_model.Repository{ID: 1})
	gitRepo, err := gitrepo.OpenRepository(db.DefaultContext, repo)
	require.NoError(t, err)
	master, err := gitRepo.GetBranchCommitID("master")
	require.NoError(t, err)
	gitRepo.Close()

	for name, write := range newBranchWriters(t) {
		require.NoError(t, write("outside-"+name), name)
		assert.Equal(t, master, parentOf(t, "outside-"+name), name)
	}
}
