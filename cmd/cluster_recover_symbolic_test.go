// Copyright 2026 The Forgejo Authors. All rights reserved.
// SPDX-License-Identifier: GPL-3.0-or-later

package cmd

import (
	"bytes"
	"maps"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"forgejo.org/models/db"
	repo_model "forgejo.org/models/repo"
	"forgejo.org/models/unittest"
	"forgejo.org/modules/gitrepo"
	repo_module "forgejo.org/modules/repository"
	"forgejo.org/modules/setting"
	"forgejo.org/modules/test"
	"forgejo.org/services/cluster/observer"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// symbolicGit runs git in dir, a directory of the test, hooks off.
func symbolicGit(t *testing.T, dir string, args ...string) string {
	t.Helper()
	cmd := exec.Command("git", append([]string{"-c", "core.hooksPath=/dev/null"}, args...)...)
	cmd.Dir = dir
	cmd.Env = append(os.Environ(), "GIT_CONFIG_NOSYSTEM=1")
	out, err := cmd.CombinedOutput()
	require.NoError(t, err, "git %v: %s", args, out)
	return string(out)
}

// symbolicManifest is the checkpoint as the observer writes it
// (observer.go fetchObjects): every repository's branches from SQL's rows,
// but user2/repo1.git's from its repository, as the table holds them: a
// symbolic ref goes into Symbolic, every other into Refs.
func symbolicManifest(t *testing.T, repoDir string, symbolic map[string]string) *observer.Manifest {
	t.Helper()
	m := checkpointOfSQL(t)
	rm := observer.RepoManifest{Format: "sha1", Refs: map[string]string{}, Symbolic: map[string]string{"HEAD": "refs/heads/master"}}
	for line := range strings.SplitSeq(strings.TrimSpace(symbolicGit(t, repoDir, "for-each-ref", "--format=%(refname) %(objectname) %(symref)", "refs/heads/")), "\n") {
		f := strings.Fields(line)
		if len(f) == 3 {
			continue
		}
		rm.Refs[f[0]] = f[1]
	}
	// git lists a symbolic ref at the ref it resolves to, not the one it
	// names, and a dangling one not at all: the table holds the target.
	maps.Copy(rm.Symbolic, symbolic)
	m.Repos["user2/repo1.git"] = rm
	return m
}

// masterRowAs is the statement writing a row of user2/repo1's branch name,
// a copy of master's.
func masterRowAs(name string) string {
	const cols = "is_deleted, commit_id, commit_message, pusher_id, deleted_by_id, deleted_unix, commit_time, created_unix, updated_unix"
	return "INSERT INTO branch (repo_id, name, " + cols + ") SELECT repo_id, '" + name + "', " + cols + " FROM branch WHERE repo_id = 1 AND name = 'master'"
}

// Step 6 of recover --from-observer compares SQL's branch rows with the
// checkpoint's refs. Forgejo lists a symbolic branch as a branch at the
// commit of the ref it resolves to (git for-each-ref refs/heads/ in
// GetBranchNames, then SyncRepoBranchesWithRepo), so SQL holds a row for
// it whenever the repository's branches were synchronised: by the site
// administration, an adoption, the API's listing of a repository with no
// row, or step 6 itself, deriving the rows of a repository whose rows lag
// behind its refs (#159). The checkpoint holds the symbolic branch in
// Symbolic, not in Refs: its row was "in SQL, not in the refs" for good,
// deriving again writing it again, and the step refused the checkpoint
// after the old cluster was fenced (#338). Such a row is now judged as the
// forge reads it, at the commit its symbolic ref resolves to in the
// checkpoint, and needs not be there.
func TestRecoverVerifiesTheRowsOfSymbolicBranches(t *testing.T) {
	zero := strings.Repeat("0", 40)
	for _, tc := range []struct {
		name string
		// symbolic are the symbolic branches of user2/repo1.git, name to
		// target, in its repository and in the checkpoint.
		symbolic map[string]string
		// sql runs on the SQL at L, after the fixtures.
		sql []string
		// reconciled is whether the step derives the rows of user2/repo1.
		reconciled bool
	}{
		{name: "without a symbolic branch, the rows matching"},
		{
			name:       "without a symbolic branch, master's row lagging",
			sql:        []string{"UPDATE branch SET commit_id = '" + zero + "' WHERE repo_id = 1 AND name = 'master'"},
			reconciled: true,
		},
		{
			// The alias a forge keeps after renaming its default branch,
			// and no row of it: nothing synchronised the repository's
			// branches since it was made.
			name:     "a symbolic branch without a row",
			symbolic: map[string]string{"refs/heads/main": "refs/heads/master"},
		},
		{
			name:     "a symbolic branch whose row is at its target's commit",
			symbolic: map[string]string{"refs/heads/main": "refs/heads/master"},
			sql:      []string{masterRowAs("main")},
		},
		{
			// The issue's scenario: deriving the lagging row of master
			// writes the alias's row too.
			name:       "a symbolic branch, master's row lagging",
			symbolic:   map[string]string{"refs/heads/main": "refs/heads/master"},
			sql:        []string{"UPDATE branch SET commit_id = '" + zero + "' WHERE repo_id = 1 AND name = 'master'"},
			reconciled: true,
		},
		{
			// Its row written before its target moved.
			name:     "a symbolic branch whose row lags behind its target",
			symbolic: map[string]string{"refs/heads/main": "refs/heads/master"},
			sql: []string{
				masterRowAs("main"),
				"UPDATE branch SET commit_id = '" + zero + "' WHERE repo_id = 1 AND name = 'main'",
			},
			reconciled: true,
		},
		{
			name:     "a chain of symbolic branches, master's row lagging",
			symbolic: map[string]string{"refs/heads/main": "refs/heads/master", "refs/heads/trunk": "refs/heads/main"},
			sql:      []string{"UPDATE branch SET commit_id = '" + zero + "' WHERE repo_id = 1 AND name = 'master'"},
			// Derived, the rows hold main and trunk at master's commit.
			reconciled: true,
		},
		{
			// Its target deleted: git lists no such branch, and the row
			// left from before is removed by the derivation.
			name:     "a dangling symbolic branch holding a row",
			symbolic: map[string]string{"refs/heads/old": "refs/heads/gone"},
			sql: []string{
				masterRowAs("old"),
			},
			reconciled: true,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			require.NoError(t, unittest.PrepareTestDatabase())
			unittest.PrepareTestEnv(t)
			t.Cleanup(func() { unittest.PrepareTestEnv(t) })
			ctx := db.DefaultContext
			e := db.GetEngine(ctx)

			// The restored repository root, in the test's own directory.
			root := t.TempDir()
			repoDir := filepath.Join(root, "user2", "repo1.git")
			require.NoError(t, unittest.CopyDir(filepath.Join(setting.RepoRootPath, "user2", "repo1.git"), repoDir))
			defer test.MockVariableValue(&setting.RepoRootPath, root)()
			// The rows of user2/repo1 at its refs, before its symbolic
			// branches: the fixtures hold some of its branches only.
			repo := unittest.AssertExistsAndLoadBean(t, &repo_model.Repository{ID: 1})
			gitRepo, err := gitrepo.OpenRepository(ctx, repo)
			require.NoError(t, err)
			_, err = repo_module.SyncRepoBranchesWithRepo(ctx, repo, gitRepo, 0)
			gitRepo.Close()
			require.NoError(t, err)
			for name, to := range tc.symbolic {
				symbolicGit(t, repoDir, "symbolic-ref", name, to)
			}
			m := symbolicManifest(t, repoDir, tc.symbolic)
			t.Logf("manifest of user2/repo1.git: symbolic=%v", m.Repos["user2/repo1.git"].Symbolic)
			for _, q := range tc.sql {
				_, err := e.Exec(q)
				require.NoError(t, err)
			}

			var out bytes.Buffer
			err = verifyBranches(ctx, m, &out, false)
			t.Logf("step 6 output:\n%s", out.String())
			require.NoError(t, err, "step 6 passes a checkpoint whose refs SQL's rows derive from")
			assert.Equal(t, tc.reconciled, strings.Contains(out.String(), "reconciled: user2/repo1.git"), out.String())
			assert.Contains(t, out.String(), "refs and SQL rows match")

			// Run again, as the operator would: nothing left to derive.
			out.Reset()
			require.NoError(t, verifyBranches(ctx, m, &out, false), out.String())
			assert.NotContains(t, out.String(), "reconciled", "the derived rows match")
			mismatch, _, _, err := branchMismatch(ctx, m)
			require.NoError(t, err)
			assert.Empty(t, mismatch)
		})
	}
}

// The row of a symbolic branch is still compared: one behind the commit
// its alias resolves to, which nothing derives (here the repository is not
// where the restore wrote it), is refused, named with that commit.
func TestRecoverRefusesTheRowOfASymbolicBranchThatDoesNotDerive(t *testing.T) {
	require.NoError(t, unittest.PrepareTestDatabase())
	unittest.PrepareTestEnv(t)
	t.Cleanup(func() { unittest.PrepareTestEnv(t) })
	ctx := db.DefaultContext
	e := db.GetEngine(ctx)

	root := t.TempDir()
	repoDir := filepath.Join(root, "user2", "repo1.git")
	require.NoError(t, unittest.CopyDir(filepath.Join(setting.RepoRootPath, "user2", "repo1.git"), repoDir))
	defer test.MockVariableValue(&setting.RepoRootPath, root)()
	repo := unittest.AssertExistsAndLoadBean(t, &repo_model.Repository{ID: 1})
	gitRepo, err := gitrepo.OpenRepository(ctx, repo)
	require.NoError(t, err)
	_, err = repo_module.SyncRepoBranchesWithRepo(ctx, repo, gitRepo, 0)
	gitRepo.Close()
	require.NoError(t, err)
	symbolic := map[string]string{"refs/heads/main": "refs/heads/master"}
	symbolicGit(t, repoDir, "symbolic-ref", "refs/heads/main", "refs/heads/master")
	m := symbolicManifest(t, repoDir, symbolic)
	master := m.Repos["user2/repo1.git"].Refs["refs/heads/master"]
	require.NotEmpty(t, master)
	zero := strings.Repeat("0", 40)
	_, err = e.Exec(masterRowAs("main"))
	require.NoError(t, err)
	_, err = e.Exec("UPDATE branch SET commit_id = ? WHERE repo_id = 1 AND name = 'main'", zero)
	require.NoError(t, err)
	// Moved aside, in the test's directory: nothing to derive the rows from.
	require.NoError(t, os.Rename(repoDir, repoDir+".aside"))

	var out bytes.Buffer
	err = verifyBranches(ctx, m, &out, false)
	require.ErrorContains(t, err, "user2/repo1.git refs/heads/main: refs "+master+" through its symbolic ref, SQL \""+zero+"\"")
	assert.NotContains(t, out.String(), "reconciled")
}
