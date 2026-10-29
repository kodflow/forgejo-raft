// Copyright 2026 The Forgejo Authors. All rights reserved.
// SPDX-License-Identifier: GPL-3.0-or-later

package cmd

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"forgejo.org/models/db"
	repo_model "forgejo.org/models/repo"
	"forgejo.org/models/unittest"
	"forgejo.org/modules/setting"
	"forgejo.org/modules/test"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/urfave/cli/v3"
)

// releasesOf returns the release rows of a repository, by tag.
func releasesOf(t *testing.T, repoID int64) map[string]repo_model.Release {
	t.Helper()
	var rels []*repo_model.Release
	require.NoError(t, db.GetEngine(db.DefaultContext).Where("repo_id = ?", repoID).Find(&rels))
	out := make(map[string]repo_model.Release, len(rels))
	for _, rel := range rels {
		out[rel.TagName] = *rel
	}
	return out
}

// `forgejo admin repo-sync-releases` rewrote every repository's releases
// from the git of the node it was started on, without the wait for
// freshness of the site administration's synchronisation (#191): on a node
// behind the cluster, a release another node published turned into a draft
// and a moved tag's release went back to its old commit, in the database
// every node shares (#341). On a node of a cluster it now refuses before
// it opens anything, and names the synchronisation that waits.
func TestRepoSyncReleasesRefusesInACluster(t *testing.T) {
	defer test.MockVariableValue(&cli.OsExiter, func(code int) {})()
	defer test.MockVariableValue(&setting.Cluster.Enabled, true)()
	require.NoError(t, unittest.PrepareTestDatabase())
	before := releasesOf(t, 1)
	require.NotEmpty(t, before)

	err := subcmdRepoSyncReleases().Run(context.Background(), []string{"repo-sync-releases"})
	require.ErrorIs(t, err, errRepoSyncReleasesInCluster)
	assert.Contains(t, err.Error(), "nothing was written")
	assert.Contains(t, err.Error(), `"Sync tags from Git data to database"`, "the refusal names the synchronisation that waits")
	assert.Contains(t, err.Error(), "forgejo cluster reconcile --apply")
	assert.Equal(t, before, releasesOf(t, 1))
}

// Outside a cluster the command synchronises as it always did: a release
// whose tag git lacks becomes a draft, a tag-only row whose tag git lacks
// goes, and a release at its tag's commit stays.
func TestRepoSyncReleasesOutsideACluster(t *testing.T) {
	defer test.MockVariableValue(&setting.Cluster.Enabled, false)()
	require.NoError(t, unittest.PrepareTestDatabase())
	before := releasesOf(t, 1)
	// The fixtures' repository 1 holds v1.1 in git, neither v1.0 nor
	// delete-tag.
	require.False(t, before["v1.0"].IsDraft)
	require.True(t, before["delete-tag"].IsTag)
	require.False(t, before["v1.1"].IsDraft)

	require.NoError(t, syncAllReposReleases(db.DefaultContext))
	after := releasesOf(t, 1)
	assert.True(t, after["v1.0"].IsDraft, "a release whose tag git lacks becomes a draft")
	assert.Empty(t, after["v1.0"].Sha1)
	assert.NotContains(t, after, "delete-tag", "a tag-only row whose tag git lacks goes")
	assert.Equal(t, before["v1.1"], after["v1.1"], "a release at its tag's commit stays")
	assert.Equal(t, before["draft-release"], after["draft-release"])
}

// Through the command line as an operator runs it, the [cluster] section
// of the configuration file, loaded before the command, makes it refuse
// before it opens the database: initDB would stop the process here, the
// file naming no installed instance and no database. The settings that
// load replaces, and the later tests of this package read, are restored.
func TestRepoSyncReleasesRefusesWithAClusterConfiguration(t *testing.T) {
	defer test.MockProtect(&setting.Cluster)()
	defer test.MockProtect(&setting.CfgProvider)()
	defer test.MockProtect(&setting.InstallLock)()
	defer test.MockProtect(&setting.Git)()
	defer test.MockProtect(&setting.AppWorkPath)()
	defer test.MockProtect(&setting.CustomPath)()
	defer test.MockProtect(&setting.CustomConf)()
	defer test.MockProtect(&setting.AppDataPath)()
	defer test.MockProtect(&setting.RepoRootPath)()
	dir := t.TempDir()
	conf := filepath.Join(dir, "app.ini")
	require.NoError(t, os.WriteFile(conf, []byte("[cluster]\nENABLED = true\n"), 0o600))

	r, err := runTestApp(NewMainApp("version", "version-extra"), "./forgejo", "--work-path", dir, "--config", conf, "admin", "repo-sync-releases")
	require.ErrorIs(t, err, errRepoSyncReleasesInCluster)
	assert.Equal(t, 1, r.ExitCode)
	assert.Contains(t, r.Stderr, "repo-sync-releases does not run on a node of a cluster")
	assert.True(t, setting.Cluster.Enabled, "the command read the [cluster] section of its configuration")
}
