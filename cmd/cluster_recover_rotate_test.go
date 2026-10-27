// Copyright 2026 The Forgejo Authors. All rights reserved.
// SPDX-License-Identifier: GPL-3.0-or-later

package cmd

import (
	"crypto/ed25519"
	"crypto/rand"
	"encoding/base64"
	"maps"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"forgejo.org/models/unittest"
	"forgejo.org/modules/generate"
	"forgejo.org/modules/jwtx"
	"forgejo.org/modules/lfs"
	"forgejo.org/modules/setting"
	"forgejo.org/modules/test"
	"forgejo.org/services/auth"
	auth_method "forgejo.org/services/auth/method"

	"github.com/golang-jwt/jwt/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Step 2 hands its gate every secret it wrote into rotated.ini, on the run
// that writes it as on the runs that read it back (#284).
func TestRecoverRotationReturnsWhatItWrote(t *testing.T) {
	path := filepath.Join(t.TempDir(), rotatedFile)
	recipient := "nas=" + strings.Repeat("ab", 32)
	// Drawn here, never written in the test: they stand for the
	// checkpoint's SECRET_KEY.
	_, checkpointKey := generate.NewJwtSecret()
	_, otherKey := generate.NewJwtSecret()
	first, err := rotatedSecrets(path, checkpointKey, recipient)
	require.NoError(t, err)
	again, err := rotatedSecrets(path, otherKey, "other="+strings.Repeat("cd", 32))
	require.NoError(t, err)
	assert.Equal(t, again, first, "the first run's secrets are the file's")
	for _, k := range []string{"SECRET_KEY", "JWT_SECRET", "LFS_JWT_SECRET", "OBSERVER_RECIPIENTS"} {
		assert.NotEmpty(t, first[k], k)
	}
	assert.Equal(t, checkpointKey, first["SECRET_KEY"])
	assert.Equal(t, recipient, first["OBSERVER_RECIPIENTS"])
	assert.NotEqual(t, first["JWT_SECRET"], first["LFS_JWT_SECRET"])
}

// Step 2 writes a new LFS_JWT_SECRET into rotated.ini and stops until the
// node's configuration carries the rotated secrets. It stopped for
// SECRET_KEY, JWT_SECRET and the recipient only: a node that kept the old
// cluster's LFS_JWT_SECRET, shared by every member and compromised with
// them, went on, and the recovered forge accepted every LFS token that
// secret signs, for any user and any repository (#284).
func TestRecoverRotationRefusesTheOldLFSSecret(t *testing.T) {
	require.NoError(t, unittest.PrepareTestDatabase())
	dir := t.TempDir()
	recipientKey := strings.Repeat("ab", 32)
	recipient := "nas=" + recipientKey
	rot, err := rotatedSecrets(filepath.Join(dir, rotatedFile), setting.SecretKey, recipient)
	require.NoError(t, err)
	rotatedLFS, err := generate.DecodeJwtSecret(rot["LFS_JWT_SECRET"])
	require.NoError(t, err)
	// The node carries the rotated JWT_SECRET: the one this process signs
	// with stands for it, as no test reloads [oauth2].
	rot["JWT_SECRET"] = base64.RawURLEncoding.EncodeToString(setting.GetGeneralTokenSigningSecret())
	defer test.MockVariableValue(&setting.Cluster.ObserverRecipients, map[string]string{"nas": recipientKey})()
	oldLFS, oldLFSText := generate.NewJwtSecret()

	// node configures its LFS_JWT_SECRET as configured says, and loads the
	// LFS server's key from it as the server does when on.
	node := func(t *testing.T, on bool, configured string, key jwtx.SigningKey) {
		ini := "[server]\n"
		if configured != "" {
			ini += configured + "\n"
		}
		cfg, err := setting.NewConfigProviderFromData(ini)
		require.NoError(t, err)
		t.Cleanup(test.MockVariableValue(&setting.CfgProvider, cfg))
		t.Cleanup(test.MockVariableValue(&setting.LFS.StartServer, on))
		t.Cleanup(test.MockVariableValue(&setting.LFS.SigningKey, key))
	}
	hs256 := func(secret []byte) jwtx.SigningKey {
		k, err := jwtx.CreateSigningKey("HS256", secret)
		require.NoError(t, err)
		return k
	}
	mint := func(secret []byte, userID, repoID int64) string {
		now := time.Now()
		s, err := jwt.NewWithClaims(jwt.SigningMethodHS256, lfs.Claims{
			ExpiresAt: jwt.NewNumericDate(now.Add(time.Hour)), NotBefore: jwt.NewNumericDate(now),
			RepoID: repoID, Op: "download", UserID: userID,
		}).SignedString(secret)
		require.NoError(t, err)
		return s
	}
	verify := func(tok string) auth.MethodOutput {
		req := httptest.NewRequest("GET", "https://example.org/user2/repo2.git/info/lfs/objects/batch", nil)
		req.Header.Set("Authorization", "Bearer "+tok)
		return (&auth_method.LFSToken{}).Verify(req, nil, nil)
	}
	lfsName := "[server] LFS_JWT_SECRET"

	t.Run("old LFS secret", func(t *testing.T) {
		node(t, true, "LFS_JWT_SECRET = "+oldLFSText, hs256(oldLFS))
		pending, err := rotationPending(rot, recipient)
		require.NoError(t, err)
		assert.Equal(t, []string{lfsName}, pending, "SECRET_KEY, JWT_SECRET and the recipient are rotated, LFS_JWT_SECRET is not")
		// What the stop prevents: user1, a site administrator, on user2's
		// private repository, with a token the old secret signs.
		ok, isOK := verify(mint(oldLFS, 1, 2)).(*auth.AuthenticationSuccess)
		require.True(t, isOK, "the node honours a token of the old cluster's LFS secret")
		assert.EqualValues(t, 1, ok.Result.User().ID)
	})
	t.Run("rotated LFS secret", func(t *testing.T) {
		node(t, true, "LFS_JWT_SECRET = "+rot["LFS_JWT_SECRET"], hs256(rotatedLFS))
		pending, err := rotationPending(rot, recipient)
		require.NoError(t, err)
		assert.Empty(t, pending)
		assert.IsType(t, &auth.AuthenticationAttemptedIncorrectCredential{}, verify(mint(oldLFS, 1, 2)), "the old cluster's LFS tokens are refused")
		assert.IsType(t, &auth.AuthenticationSuccess{}, verify(mint(rotatedLFS, 1, 2)))
	})
	t.Run("rotated LFS secret from its URI", func(t *testing.T) {
		f := filepath.Join(dir, "lfs-jwt-secret")
		require.NoError(t, os.WriteFile(f, []byte(rot["LFS_JWT_SECRET"]+"\n"), 0o600))
		node(t, true, "LFS_JWT_SECRET_URI = file:"+f, hs256(rotatedLFS))
		pending, err := rotationPending(rot, recipient)
		require.NoError(t, err)
		assert.Empty(t, pending)
	})
	t.Run("asymmetric LFS key", func(t *testing.T) {
		// The rotated secret configured, unused: the key file signs.
		_, priv, err := ed25519.GenerateKey(rand.Reader)
		require.NoError(t, err)
		k, err := jwtx.CreateSigningKey("EdDSA", priv)
		require.NoError(t, err)
		node(t, true, "LFS_JWT_SECRET = "+rot["LFS_JWT_SECRET"], k)
		pending, err := rotationPending(rot, recipient)
		require.NoError(t, err)
		require.Len(t, pending, 1)
		assert.Contains(t, pending[0], "LFS_JWT_SIGNING_ALGORITHM EdDSA")
	})
	t.Run("asymmetric LFS algorithm, server off", func(t *testing.T) {
		// Turned on later, the server would sign with the key file the
		// configuration names, which no step rotates.
		node(t, false, "LFS_JWT_SIGNING_ALGORITHM = EdDSA\nLFS_JWT_SECRET = "+rot["LFS_JWT_SECRET"], nil)
		pending, err := rotationPending(rot, recipient)
		require.NoError(t, err)
		require.Len(t, pending, 1)
		assert.Contains(t, pending[0], "LFS_JWT_SIGNING_ALGORITHM EdDSA")
	})
	t.Run("LFS server off", func(t *testing.T) {
		// Turned on later, the server would sign with what the
		// configuration carries.
		for configured, want := range map[string][]string{
			"LFS_JWT_SECRET = " + oldLFSText: {lfsName},
			"":                               {lfsName},
			"LFS_JWT_SECRET = " + rot["LFS_JWT_SECRET"]: nil,
		} {
			node(t, false, configured, nil)
			pending, err := rotationPending(rot, recipient)
			require.NoError(t, err)
			assert.Equal(t, want, pending, configured)
		}
	})
	t.Run("every other secret", func(t *testing.T) {
		node(t, true, "LFS_JWT_SECRET = "+rot["LFS_JWT_SECRET"], hs256(rotatedLFS))
		other := maps.Clone(rot)
		_, other["SECRET_KEY"] = generate.NewJwtSecret()
		_, other["JWT_SECRET"] = generate.NewJwtSecret()
		pending, err := rotationPending(other, "nas="+strings.Repeat("cd", 32))
		require.NoError(t, err)
		assert.Equal(t, []string{"[security] SECRET_KEY", "[oauth2] JWT_SECRET", "[cluster] OBSERVER_RECIPIENTS"}, pending)

		delete(other, "LFS_JWT_SECRET")
		_, err = rotationPending(other, recipient)
		require.ErrorContains(t, err, "LFS_JWT_SECRET", "a rotated.ini without it stops the recovery")
	})
}

// The shipped deployments set [oauth2] JWT_SECRET and [server]
// LFS_JWT_SECRET from their shared store (#297), and SECRET_KEY: FORGEJO__*
// variables that the image's setup writes into app.ini at every start
// (environment-to-ini). Step 2 read app.ini alone: run in the deployment's
// container, with app.ini carrying the rotated secrets, it passed, and the
// node's first start put the store's values back, the old cluster's,
// compromised with its members (#305). The gate now names what this
// process's environment sets to another value than rotated.ini's.
func TestRecoverRotationRefusesWhatTheEnvironmentSetsBack(t *testing.T) {
	dir := t.TempDir()
	recipientKey := strings.Repeat("ab", 32)
	recipient := "nas=" + recipientKey
	rot, err := rotatedSecrets(filepath.Join(dir, rotatedFile), setting.SecretKey, recipient)
	require.NoError(t, err)
	// The node carries the rotated JWT_SECRET: the one this process signs
	// with stands for it, as no test reloads [oauth2].
	rot["JWT_SECRET"] = base64.RawURLEncoding.EncodeToString(setting.GetGeneralTokenSigningSecret())
	rotatedLFS, err := generate.DecodeJwtSecret(rot["LFS_JWT_SECRET"])
	require.NoError(t, err)
	defer test.MockVariableValue(&setting.Cluster.ObserverRecipients, map[string]string{"nas": recipientKey})()
	// app.ini as the operator left it: every rotated secret.
	cfg, err := setting.NewConfigProviderFromData("[server]\nLFS_JWT_SECRET = " + rot["LFS_JWT_SECRET"] + "\n")
	require.NoError(t, err)
	defer test.MockVariableValue(&setting.CfgProvider, cfg)()
	defer test.MockVariableValue(&setting.LFS.StartServer, true)()
	key, err := jwtx.CreateSigningKey("HS256", rotatedLFS)
	require.NoError(t, err)
	defer test.MockVariableValue(&setting.LFS.SigningKey, key)()
	pending, err := rotationPending(rot, recipient)
	require.NoError(t, err)
	require.Empty(t, pending, "app.ini alone carries the rotated secrets")

	// The old cluster's values, as the deployment's store keeps them:
	// drawn here, never written in the test.
	_, oldLFS := generate.NewJwtSecret()
	_, oldJWT := generate.NewJwtSecret()
	_, oldKey := generate.NewJwtSecret()
	file := func(name, content string) string {
		f := filepath.Join(dir, name)
		require.NoError(t, os.WriteFile(f, []byte(content+"\n"), 0o600))
		return f
	}
	for _, c := range []struct {
		name string
		env  map[string]string
		want []string
	}{
		{"LFS secret", map[string]string{"FORGEJO__server__LFS_JWT_SECRET": oldLFS}, []string{"[server] LFS_JWT_SECRET"}},
		{"LFS secret from a file", map[string]string{"FORGEJO__server__LFS_JWT_SECRET__FILE": file("lfs", oldLFS)}, []string{"[server] LFS_JWT_SECRET"}},
		{"LFS secret from its URI", map[string]string{"FORGEJO__server__LFS_JWT_SECRET_URI": "file:" + file("lfs-uri", oldLFS)}, []string{"[server] LFS_JWT_SECRET"}},
		{"LFS algorithm", map[string]string{"FORGEJO__server__LFS_JWT_SIGNING_ALGORITHM": "EdDSA"}, []string{"[server] LFS_JWT_SECRET (LFS_JWT_SIGNING_ALGORITHM EdDSA signs with a key file the recovery does not rotate: recover with HS256, the default)"}},
		{"JWT secret, GITEA__", map[string]string{"GITEA__oauth2__JWT_SECRET": oldJWT}, []string{"[oauth2] JWT_SECRET"}},
		{"JWT secret from a file", map[string]string{"FORGEJO__oauth2__JWT_SECRET__FILE": file("jwt", oldJWT)}, []string{"[oauth2] JWT_SECRET"}},
		{"JWT secret from its URI", map[string]string{"FORGEJO__oauth2__JWT_SECRET_URI": "file:" + file("jwt-uri", oldJWT)}, []string{"[oauth2] JWT_SECRET"}},
		{"SECRET_KEY", map[string]string{"FORGEJO__security__SECRET_KEY": oldKey}, []string{"[security] SECRET_KEY"}},
		{"recipients", map[string]string{"FORGEJO__cluster__OBSERVER_RECIPIENTS": "old=" + strings.Repeat("cd", 32)}, []string{"[cluster] OBSERVER_RECIPIENTS"}},
		{"all of them", map[string]string{
			"FORGEJO__security__SECRET_KEY": oldKey, "FORGEJO__oauth2__JWT_SECRET": oldJWT, "FORGEJO__server__LFS_JWT_SECRET": oldLFS,
		}, []string{"[security] SECRET_KEY", "[oauth2] JWT_SECRET", "[server] LFS_JWT_SECRET"}},
		{"the rotated values", map[string]string{
			"FORGEJO__security__SECRET_KEY": rot["SECRET_KEY"], "FORGEJO__oauth2__JWT_SECRET": rot["JWT_SECRET"],
			"FORGEJO__server__LFS_JWT_SECRET": rot["LFS_JWT_SECRET"], "FORGEJO__cluster__OBSERVER_RECIPIENTS": "nas=" + strings.ToUpper(recipientKey),
		}, nil},
		{"other settings", map[string]string{"FORGEJO__server__ROOT_URL": "https://example.org/", "FORGEJO__server__LFS_START_SERVER": "true"}, nil},
	} {
		t.Run(c.name, func(t *testing.T) {
			for k, v := range c.env {
				t.Setenv(k, v)
			}
			pending, err := rotationPending(rot, recipient)
			require.NoError(t, err)
			assert.Equal(t, c.want, pending)
		})
	}

	t.Run("the variables named", func(t *testing.T) {
		pending, vars, err := rotationGate(rot, recipient, []string{
			"FORGEJO__server__LFS_JWT_SECRET=" + oldLFS, "FORGEJO__server__ROOT_URL=https://example.org/",
			"GITEA__oauth2__JWT_SECRET__FILE=" + file("jwt-vars", oldJWT), "FORGEJO__oauth2__JWT_SECRET=" + oldJWT,
		})
		require.NoError(t, err)
		assert.Equal(t, []string{"[oauth2] JWT_SECRET", "[server] LFS_JWT_SECRET"}, pending)
		assert.Equal(t, []string{"GITEA__oauth2__JWT_SECRET__FILE", "FORGEJO__oauth2__JWT_SECRET", "FORGEJO__server__LFS_JWT_SECRET"}, vars)
	})
	t.Run("named once", func(t *testing.T) {
		// app.ini carries the old LFS secret too, as the setup rendered it.
		old, err := generate.DecodeJwtSecret(oldLFS)
		require.NoError(t, err)
		cfg, err := setting.NewConfigProviderFromData("[server]\nLFS_JWT_SECRET = " + oldLFS + "\n")
		require.NoError(t, err)
		defer test.MockVariableValue(&setting.CfgProvider, cfg)()
		k, err := jwtx.CreateSigningKey("HS256", old)
		require.NoError(t, err)
		defer test.MockVariableValue(&setting.LFS.SigningKey, k)()
		pending, vars, err := rotationGate(rot, recipient, []string{"FORGEJO__server__LFS_JWT_SECRET=" + oldLFS})
		require.NoError(t, err)
		assert.Equal(t, []string{"[server] LFS_JWT_SECRET"}, pending)
		assert.Equal(t, []string{"FORGEJO__server__LFS_JWT_SECRET"}, vars)
	})
	t.Run("cleared after the start", func(t *testing.T) {
		// An installed instance clears its FORGEJO__* variables while it
		// reads its configuration: the gate reads them as the process
		// started.
		defer test.MockVariableValue(&startEnviron, []string{"FORGEJO__server__LFS_JWT_SECRET=" + oldLFS})()
		pending, err := rotationPending(rot, recipient)
		require.NoError(t, err)
		assert.Equal(t, []string{"[server] LFS_JWT_SECRET"}, pending)
	})
}
