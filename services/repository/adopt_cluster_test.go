// Copyright 2026 The Forgejo Authors. All rights reserved.
// SPDX-License-Identifier: GPL-3.0-or-later

package repository

import (
	"fmt"
	"os"
	"path"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	"forgejo.org/models/db"
	"forgejo.org/models/unittest"
	user_model "forgejo.org/models/user"
	"forgejo.org/modules/clusterhook"
	"forgejo.org/modules/git"
	"forgejo.org/modules/setting"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// In a cluster, the directory of a repository whose row SQL lost keeps the
// lost row's ID in the table and every stamp, and reads and pushes are
// refused while it differs from the database's. The adoption writes HEAD
// again under the new row's ID, for the repository and its wiki, which the
// cluster binds on every node (#150).
func TestAdoptRepositoryGivesTheClusterItsID(t *testing.T) {
	require.NoError(t, unittest.PrepareTestDatabase())
	defer clusterhook.SetEnabled(clusterhook.Enabled())
	clusterhook.SetEnabled(true)

	name := "lost-in-sql"
	dir := path.Join(setting.RepoRootPath, "user2", name+".git")
	for _, d := range []string{dir, path.Join(setting.RepoRootPath, "user2", name+".wiki.git")} {
		require.NoError(t, unittest.CopyDir("../../modules/git/tests/repos/repo1_bare", d))
	}
	t.Cleanup(func() {
		_ = os.RemoveAll(dir)
		_ = os.RemoveAll(strings.TrimSuffix(dir, ".git") + ".wiki.git")
	})

	// A reference-transaction hook, as the cluster's, writes down what
	// each prepared transaction carries.
	seen := filepath.Join(t.TempDir(), "seen")
	hook := filepath.Join(git.HomeDir(), "hooks", "reference-transaction")
	_, err := os.Stat(hook)
	require.True(t, os.IsNotExist(err), "a reference-transaction hook is already installed")
	require.NoError(t, os.MkdirAll(filepath.Dir(hook), 0o755))
	script := fmt.Sprintf("#!/bin/sh\n[ \"$1\" = prepared ] || exit 0\nwhile read -r old new ref; do echo \"$GITEA_REPO_ID $GITEA_REPO_IS_WIKI $ref $new $(basename \"$(pwd -P)\")\" >> '%s'; done\n", seen)
	require.NoError(t, os.WriteFile(hook, []byte(script), 0o755))
	t.Cleanup(func() { _ = os.Remove(hook) })

	doer := unittest.AssertExistsAndLoadBean(t, &user_model.User{ID: 1})
	owner := unittest.AssertExistsAndLoadBean(t, &user_model.User{ID: 2})
	repo, err := AdoptRepository(db.DefaultContext, doer, owner, CreateRepoOptions{Name: name})
	require.NoError(t, err)

	b, err := os.ReadFile(seen)
	require.NoError(t, err, "the adoption wrote no ref")
	id := strconv.FormatInt(repo.ID, 10)
	lines := strings.Split(strings.TrimSpace(string(b)), "\n")
	assert.Contains(t, lines, id+" false HEAD ref:refs/heads/master "+name+".git")
	assert.Contains(t, lines, id+" true HEAD ref:refs/heads/master "+name+".wiki.git")
}
