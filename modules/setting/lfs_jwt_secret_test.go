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

// The LFS secret a configuration carries reads as the server reads it,
// with the LFS server off too, and writes nothing.
func TestLFSJWTSecretConfigured(t *testing.T) {
	read := func(ini string) (string, error) {
		cfg, err := NewConfigProviderFromData(ini)
		require.NoError(t, err)
		return LFSJWTSecretConfigured(cfg)
	}
	// Drawn here, never written in the test.
	_, verbatim := generate.NewJwtSecret()
	_, inFile := generate.NewJwtSecret()
	got, err := read("[server]\nLFS_START_SERVER = false\nLFS_JWT_SECRET = " + verbatim + "\n")
	require.NoError(t, err)
	assert.Equal(t, verbatim, got)

	got, err = read("[server]\n")
	require.NoError(t, err)
	assert.Empty(t, got, "none configured, none created")

	f := filepath.Join(t.TempDir(), "lfs-secret")
	require.NoError(t, os.WriteFile(f, []byte(" "+inFile+" \n"), 0o600))
	got, err = read("[server]\nLFS_JWT_SECRET_URI = file:" + f + "\n")
	require.NoError(t, err)
	assert.Equal(t, inFile, got)

	_, err = read("[server]\nLFS_JWT_SECRET_URI = file:" + f + "\nLFS_JWT_SECRET = " + verbatim + "\n")
	require.ErrorContains(t, err, "both")
	_, err = read("[server]\nLFS_JWT_SECRET_URI = file:" + filepath.Join(t.TempDir(), "missing") + "\n")
	require.Error(t, err)
}
