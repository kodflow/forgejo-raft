// Copyright 2026 The Forgejo Authors. All rights reserved.
// SPDX-License-Identifier: GPL-3.0-or-later

package integration

import (
	"fmt"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"

	auth_model "forgejo.org/models/auth"
	"forgejo.org/modules/clusterhook"
	"forgejo.org/modules/git"
	"forgejo.org/modules/setting"
	cluster_service "forgejo.org/services/cluster"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// [cluster] EXPECTED_NODES = 1 is the standard product. The integration
// configurations set it (tests/*.ini.tmpl), so every integration test runs
// with it, in the server and in the hooks git runs; this one checks what it
// means for a push: no replicator, the hooks upstream generates, and what
// is pushed is read back.
func TestClusterSizeOneIsTheStandardProduct(t *testing.T) {
	onApplicationRun(t, func(t *testing.T, u *url.URL) {
		require.Equal(t, 1, setting.Cluster.ExpectedNodes, "the integration configuration sets [cluster] EXPECTED_NODES = 1")
		require.False(t, setting.Cluster.Enabled)
		require.NoError(t, setting.Cluster.ConfigError)
		assert.Nil(t, cluster_service.Current())
		assert.False(t, clusterhook.Enabled())

		ctx := NewAPITestContext(t, "user2", "cluster-size-one", auth_model.AccessTokenScopeWriteRepository, auth_model.AccessTokenScopeWriteUser)
		t.Run("CreateRepo", doAPICreateRepository(ctx, nil, git.Sha1ObjectFormat))

		// Upstream's hooks (core.hooksPath), and no reference-transaction hook.
		hooks := filepath.Join(git.HomeDir(), "hooks")
		for _, h := range []string{"pre-receive", "update", "post-receive"} {
			assert.FileExists(t, filepath.Join(hooks, h))
		}
		assert.NoFileExists(t, filepath.Join(hooks, "reference-transaction"))
		assert.NoDirExists(t, filepath.Join(setting.AppDataPath, "cluster"), "no consensus data directory")

		dst := t.TempDir()
		u.Path = ctx.GitPath()
		u.User = url.UserPassword("user2", userPassword)
		t.Run("Clone", doGitClone(dst, u))
		content := fmt.Sprintf("pushed on a forge of one node, %s", t.Name())
		require.NoError(t, os.WriteFile(filepath.Join(dst, "one.txt"), []byte(content), 0o644))
		require.NoError(t, git.AddChanges(dst, true))
		sig := git.Signature{Email: "user2@example.com", Name: "User Two"}
		require.NoError(t, git.CommitChanges(dst, git.CommitChangesOptions{Committer: &sig, Author: &sig, Message: "one node"}))
		branch, _, err := git.NewCommand(t.Context(), "rev-parse", "--abbrev-ref", "HEAD").RunStdString(&git.RunOpts{Dir: dst})
		require.NoError(t, err)
		branch = strings.TrimSpace(branch)
		t.Run("Push", doGitPushTestRepository(dst, "origin", branch))

		req := NewRequest(t, "GET", fmt.Sprintf("/user2/cluster-size-one/raw/branch/%s/one.txt", branch))
		resp := ctx.Session.MakeRequest(t, req, http.StatusOK)
		assert.Equal(t, content, resp.Body.String())
	})
}
