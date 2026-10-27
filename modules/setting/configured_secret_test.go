// Copyright 2026 The Forgejo Authors. All rights reserved.
// SPDX-License-Identifier: GPL-3.0-or-later

package setting

import (
	"os"
	"path/filepath"
	"testing"

	"forgejo.org/modules/generate"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// A secret a configuration carries reads as loadSecret reads it, verbatim
// or from its URI, and what would stop the server is an error.
func TestConfiguredSecret(t *testing.T) {
	read := func(ini string) (string, error) {
		cfg, err := NewConfigProviderFromData(ini)
		require.NoError(t, err)
		return ConfiguredSecret(cfg.Section("oauth2"), "JWT_SECRET_URI", "JWT_SECRET")
	}
	// Drawn here, never written in the test.
	_, verbatim := generate.NewJwtSecret()
	_, inFile := generate.NewJwtSecret()
	got, err := read("[oauth2]\nJWT_SECRET = " + verbatim + "\n")
	require.NoError(t, err)
	assert.Equal(t, verbatim, got)

	got, err = read("[oauth2]\n")
	require.NoError(t, err)
	assert.Empty(t, got)

	f := filepath.Join(t.TempDir(), "jwt-secret")
	require.NoError(t, os.WriteFile(f, []byte(inFile+"\n"), 0o600))
	got, err = read("[oauth2]\nJWT_SECRET_URI = file:" + f + "\n")
	require.NoError(t, err)
	assert.Equal(t, inFile, got)

	_, err = read("[oauth2]\nJWT_SECRET_URI = file:" + f + "\nJWT_SECRET = " + verbatim + "\n")
	require.ErrorContains(t, err, "cannot specify both JWT_SECRET_URI and JWT_SECRET")
	_, err = read("[oauth2]\nJWT_SECRET_URI = file:" + filepath.Join(t.TempDir(), "missing") + "\n")
	require.Error(t, err)
	_, err = read("[oauth2]\nJWT_SECRET_URI = https://example.org/secret\n")
	require.Error(t, err, "only file URIs")
}
