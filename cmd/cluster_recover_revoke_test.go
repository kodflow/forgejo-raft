// Copyright 2026 The Forgejo Authors. All rights reserved.
// SPDX-License-Identifier: GPL-3.0-or-later

package cmd

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"

	auth_model "forgejo.org/models/auth"
	"forgejo.org/models/db"
	org_model "forgejo.org/models/organization"
	"forgejo.org/models/unittest"
	user_model "forgejo.org/models/user"
	"forgejo.org/modules/optional"
	"forgejo.org/modules/setting"
	"forgejo.org/modules/test"
	"forgejo.org/modules/timeutil"
	"forgejo.org/services/cluster/observer"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// checkpointOfRestoredSQL is a checkpoint whose refs are SQL's branch
// rows, for the repositories whose directories the fixtures hold; the rows
// of the others go, so that the steps before the revocation pass. The
// stored files are checked elsewhere (TestStoredPresentNamesTheMissingFile)
// and the 2FA secrets too: their storages are not local and two_factor is
// emptied.
func checkpointOfRestoredSQL(t *testing.T) *observer.Manifest {
	t.Helper()
	e := db.GetEngine(db.DefaultContext)
	rows, err := e.Query(`SELECT b.repo_id AS id, r.owner_name AS owner, r.lower_name AS repo, b.name AS branch, b.commit_id AS sha
		FROM branch b JOIN repository r ON r.id = b.repo_id WHERE b.is_deleted = ?`, false)
	require.NoError(t, err)
	m := &observer.Manifest{Repos: map[string]observer.RepoManifest{}}
	for _, r := range rows {
		key := strings.ToLower(string(r["owner"])) + "/" + string(r["repo"]) + ".git"
		if _, err := os.Stat(filepath.Join(setting.RepoRootPath, filepath.FromSlash(key), "HEAD")); err != nil {
			_, err := e.Exec("DELETE FROM branch WHERE repo_id = ?", string(r["id"]))
			require.NoError(t, err)
			continue
		}
		rm := m.Repos[key]
		if rm.Refs == nil {
			rm.Refs = map[string]string{}
		}
		rm.Refs["refs/heads/"+string(r["branch"])] = string(r["sha"])
		m.Repos[key] = rm
	}
	t.Cleanup(test.MockVariableValue(&setting.LFS.Storage, &setting.Storage{Type: setting.MinioStorageType}))
	t.Cleanup(test.MockVariableValue(&setting.Attachment.Storage, &setting.Storage{Type: setting.MinioStorageType}))
	t.Cleanup(test.MockVariableValue(&setting.Packages.Storage, &setting.Storage{Type: setting.MinioStorageType}))
	_, err = e.Exec("DELETE FROM two_factor")
	require.NoError(t, err)
	return m
}

// Step 6 revokes every token: a recovered forge trusts none it held. The
// long-term authentication cookie ("remember me", forgejo_auth_token), its
// SSO form, and the codes the old forge mailed to reset a password or to
// activate an account or an address are tokens as well: one the old forge
// issued, captured while its members were compromised, signed its holder
// in again on the recovered forge, administrators included (#256), and so
// is an invitation to a team mailed with a link. An access token
// restricted to some repositories has rows in
// access_token_resource_repo, which reference it: they are revoked with
// it, or the foreign key refuses the revocation after the old cluster was
// fenced. Only SQL rows change here.
func TestRecoverRevokesEveryToken(t *testing.T) {
	require.NoError(t, unittest.PrepareTestDatabase())
	unittest.PrepareTestEnv(t)
	t.Cleanup(func() { unittest.PrepareTestEnv(t) })
	ctx := db.DefaultContext
	e := db.GetEngine(ctx)
	m := checkpointOfRestoredSQL(t)

	// On the old forge, before the checkpoint: user2 checked "remember me",
	// with a password and through an SSO source, asked for a password
	// reset, and user5 for its activation and an address's.
	user2 := unittest.AssertExistsAndLoadBean(t, &user_model.User{ID: 2})
	user5 := unittest.AssertExistsAndLoadBean(t, &user_model.User{ID: 5})
	t.Cleanup(test.MockVariableValue(&setting.Service.ActiveCodeLives, 180))
	codes := map[auth_model.AuthorizationPurpose]string{}
	for _, p := range []auth_model.AuthorizationPurpose{auth_model.LongTermAuthorization, auth_model.LongTermAuthorizationSSO} {
		lookup, validator, err := auth_model.GenerateAuthToken(ctx, user2.ID, optional.None[int64](), timeutil.TimeStampNow().Add(30*86400), p)
		require.NoError(t, err)
		codes[p] = lookup + ":" + validator
	}
	var err error
	codes[auth_model.PasswordReset], err = user2.GenerateEmailAuthorizationCode(ctx, auth_model.PasswordReset)
	require.NoError(t, err)
	codes[auth_model.UserActivation], err = user5.GenerateEmailAuthorizationCode(ctx, auth_model.UserActivation)
	require.NoError(t, err)
	codes[auth_model.EmailActivation(user5.Email)], err = user5.GenerateEmailAuthorizationCode(ctx, auth_model.EmailActivation(user5.Email))
	require.NoError(t, err)
	for p, code := range codes {
		u, _, _, err := user_model.VerifyUserAuthorizationToken(ctx, code, p)
		require.NoError(t, err)
		require.NotNil(t, u, p)
	}
	// And an access token restricted to one repository.
	scoped := &auth_model.AccessToken{UID: user2.ID, Name: "scoped", Scope: auth_model.AccessTokenScopeAll, ResourceAllRepos: false}
	require.NoError(t, auth_model.NewAccessToken(ctx, scoped))
	_, err = e.Insert(&auth_model.AccessTokenResourceRepo{TokenID: scoped.ID, RepoID: 1})
	require.NoError(t, err)
	// And an invitation to org3's owners, mailed with a link any account
	// holding it accepts.
	team := unittest.AssertExistsAndLoadBean(t, &org_model.Team{ID: 1})
	invite, err := org_model.CreateTeamInviteByEmail(ctx, user2, team, "invited@example.com")
	require.NoError(t, err)

	var out bytes.Buffer
	require.NoError(t, verifyRecovered(ctx, m, &out, false), out.String())
	t.Logf("step 6:\n%s", out.String())

	for _, bean := range []any{new(auth_model.AccessToken), new(auth_model.AccessTokenResourceRepo), new(auth_model.AuthorizationToken), new(org_model.TeamInvite)} {
		n, err := e.Count(bean)
		require.NoError(t, err)
		assert.Zero(t, n, "%T rows are revoked", bean)
	}
	assert.Contains(t, out.String(), "rows of forgejo_auth_token")
	for p, code := range codes {
		u, _, _, err := user_model.VerifyUserAuthorizationToken(ctx, code, p)
		require.NoError(t, err)
		assert.Nil(t, u, "a %s code the old forge issued opens nothing on the recovered forge", p)
	}
	_, err = org_model.GetInviteByToken(ctx, invite.Token)
	assert.True(t, org_model.IsErrTeamInviteNotFound(err), "the invitation the old forge mailed lets no one in: %v", err)
}

// Every table holding a foreign key to a table step 6 revokes is revoked
// too, before it: a row left referencing a revoked one makes the
// revocation fail, after the old cluster was fenced. A table a later
// release adds with such a key fails here first.
func TestRecoverRevokesReferencingTablesFirst(t *testing.T) {
	pos := map[string]int{}
	for i, table := range revokedTables {
		pos[table] = i
	}
	beans, err := db.NamesToBean()
	require.NoError(t, err)
	for _, bean := range beans {
		info, err := db.TableInfo(bean)
		require.NoError(t, err)
		for _, fk := range info.ForeignKeys {
			target, ok := pos[fk.TargetTableName]
			if !ok {
				continue
			}
			at, ok := pos[info.Name]
			if assert.True(t, ok, "%s references %s, which step 6 revokes, and is not revoked", info.Name, fk.TargetTableName) {
				assert.Less(t, at, target, "%s references %s: it is revoked first", info.Name, fk.TargetTableName)
			}
		}
	}
}
