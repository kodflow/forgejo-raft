// Copyright 2026 The Forgejo Authors. All rights reserved.
// SPDX-License-Identifier: GPL-3.0-or-later

package mirror

import (
	"context"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	repo_model "forgejo.org/models/repo"
	"forgejo.org/modules/clusterhook"
	"forgejo.org/modules/git"
	"forgejo.org/modules/setting"
	"forgejo.org/modules/test"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// The wiki of a pull mirror is found by asking the remote with the
// mirror's credentials, which a private wiki requires, and its remote is
// written without them, as the repository's (CodeRabbit on #123).
func TestPullWikiRemoteAsksWithTheCredentialsAndKeepsThemOut(t *testing.T) {
	defer test.MockVariableValue(&setting.RepoRootPath, t.TempDir())()
	defer test.MockVariableValue(&setting.Git.HomePath, t.TempDir())()
	require.NoError(t, git.InitFull(context.Background()))
	was := clusterhook.Enabled()
	clusterhook.SetEnabled(true)
	defer clusterhook.SetEnabled(was)
	repo := &repo_model.Repository{OwnerName: "o", Name: "r"}
	out, err := exec.Command("git", "init", "-q", "--bare", repo.WikiPath()).CombinedOutput()
	require.NoError(t, err, string(out))
	var asked []string
	defer test.MockVariableValue(&wikiRemoteURL, func(_ context.Context, remote string) string {
		asked = append(asked, remote)
		return strings.TrimSuffix(remote, ".git") + ".wiki.git"
	})()
	m := &repo_model.Mirror{Repo: repo}
	require.NoError(t, ensurePullWikiRemote(context.Background(), m, "https://u:secret@example.com/o/r.git", "https://u@example.com/o/r.git", nil))
	assert.Equal(t, []string{"https://u:secret@example.com/o/r.git"}, asked)
	url, err := remoteURLOf(context.Background(), repo.WikiPath(), m.GetRemoteName())
	require.NoError(t, err)
	assert.Equal(t, "https://u@example.com/o/r.wiki.git", url)
	assert.Equal(t, filepath.Join(setting.RepoRootPath, "o", "r.wiki.git"), repo.WikiPath())

	// Present, it is not asked again.
	require.NoError(t, ensurePullWikiRemote(context.Background(), m, "https://u:secret@example.com/o/r.git", "https://u@example.com/o/r.git", nil))
	assert.Len(t, asked, 1)
}
