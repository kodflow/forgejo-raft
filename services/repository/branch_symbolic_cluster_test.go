// Copyright 2026 The Forgejo Authors. All rights reserved.
// SPDX-License-Identifier: GPL-3.0-or-later

package repository

import (
	"os"
	"os/exec"
	"strings"
	"testing"

	"forgejo.org/models/db"
	git_model "forgejo.org/models/git"
	repo_model "forgejo.org/models/repo"
	"forgejo.org/models/unittest"
	user_model "forgejo.org/models/user"
	"forgejo.org/modules/git"
	"forgejo.org/modules/gitrepo"
	"forgejo.org/modules/setting"
	"forgejo.org/modules/test"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// refOneLevel returns what git holds in dir under name, one level deep:
// "ref:" and the target of a symbolic ref, the object id of another, ""
// when git holds nothing.
func refOneLevel(dir, name string) string {
	read := func(args ...string) (string, bool) {
		cmd := exec.Command("git", append([]string{"-c", "core.hooksPath=/dev/null"}, args...)...)
		cmd.Dir = dir
		cmd.Env = append(os.Environ(), "HOME="+setting.Git.HomePath)
		out, err := cmd.Output()
		return strings.TrimSpace(string(out)), err == nil
	}
	if to, ok := read("symbolic-ref", "-q", "--no-recurse", "--end-of-options", name); ok {
		return "ref:" + to
	}
	sha, _ := read("rev-parse", "-q", "--verify", "--end-of-options", name)
	return sha
}

// repo1 holds the alias refs/heads/alias -> refs/heads/master, and the
// branch row Forgejo writes for it, since for-each-ref lists it; master is
// the default branch, protected. In a cluster, renaming the alias through
// RenameBranch, as the web form and PATCH /repos/{o}/{r}/branches/{b} do,
// deleted master in git, on every node, where git branch -m refuses: the
// protection was read on the alias alone. SQL renamed the alias's row and
// kept master as the default branch, its row live, which git no longer
// held (#356). The rename is now refused with git.ErrSymbolicBranch, the
// SQL transaction rolled back: nothing changes, in git or in SQL.
func TestRenameASymbolicBranchChangesNothingInACluster(t *testing.T) {
	unittest.PrepareTestEnv(t)
	defer test.MockVariableValue(&setting.Cluster.Enabled, true)()
	ctx := db.DefaultContext
	repo := unittest.AssertExistsAndLoadBean(t, &repo_model.Repository{ID: 1})
	doer := unittest.AssertExistsAndLoadBean(t, &user_model.User{ID: 2})
	require.Equal(t, "master", repo.DefaultBranch)
	dir := repo.RepoPath()
	master := gitIn(t, dir, "", "rev-parse", "refs/heads/master")
	gitIn(t, dir, "", "symbolic-ref", "refs/heads/alias", "refs/heads/master")
	t.Cleanup(func() { gitIn(t, dir, "", "update-ref", "--no-deref", "-d", "refs/heads/alias") })
	require.NoError(t, db.Insert(ctx, &git_model.Branch{RepoID: 1, Name: "alias", CommitID: master, CommitMessage: "alias", PusherID: 2}))
	require.NoError(t, git_model.UpdateProtectBranch(ctx, repo, &git_model.ProtectedBranch{RepoID: 1, RuleName: "master"}, git_model.WhitelistOptions{}))
	masterRow := unittest.AssertExistsAndLoadBean(t, &git_model.Branch{RepoID: 1, Name: "master"})
	gitRepo, err := gitrepo.OpenRepository(ctx, repo)
	require.NoError(t, err)
	defer gitRepo.Close()
	require.True(t, gitRepo.IsBranchExist("alias"), "Forgejo sees the alias as a branch")

	msg, err := RenameBranch(ctx, repo, doer, gitRepo, "alias", "legacy")
	t.Logf("RenameBranch(alias, legacy): %q, %v", msg, err)
	t.Logf("git after: master=%q alias=%q legacy=%q HEAD=%q", refOneLevel(dir, "refs/heads/master"), refOneLevel(dir, "refs/heads/alias"), refOneLevel(dir, "refs/heads/legacy"), refOneLevel(dir, "HEAD"))

	assert.True(t, git.IsErrSymbolicBranch(err), "the rename of a symbolic branch is refused, and says why: %v", err)
	assert.Empty(t, msg)
	// git: master, the alias and HEAD as they were.
	assert.Equal(t, master, refOneLevel(dir, "refs/heads/master"), "master, the default and protected branch, is kept")
	assert.Equal(t, "ref:refs/heads/master", refOneLevel(dir, "refs/heads/alias"))
	assert.Empty(t, refOneLevel(dir, "refs/heads/legacy"))
	assert.Equal(t, "ref:refs/heads/master", refOneLevel(dir, "HEAD"))
	// SQL: the rows of master and of the alias, the default branch, the
	// protection and the renamed branches as they were.
	row := unittest.AssertExistsAndLoadBean(t, &git_model.Branch{RepoID: 1, Name: "master"})
	assert.Equal(t, masterRow.CommitID, row.CommitID)
	assert.False(t, row.IsDeleted, "the row of master stays live")
	aliasRow := unittest.AssertExistsAndLoadBean(t, &git_model.Branch{RepoID: 1, Name: "alias"})
	assert.False(t, aliasRow.IsDeleted)
	unittest.AssertNotExistsBean(t, &git_model.Branch{RepoID: 1, Name: "legacy"})
	unittest.AssertNotExistsBean(t, &git_model.RenamedBranch{RepoID: 1, From: "alias"})
	assert.Equal(t, "master", unittest.AssertExistsAndLoadBean(t, &repo_model.Repository{ID: 1}).DefaultBranch)
	rule, err := git_model.GetProtectedBranchRuleByName(ctx, 1, "master")
	require.NoError(t, err)
	assert.NotNil(t, rule, "the protection of master is kept")
}
