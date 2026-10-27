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
