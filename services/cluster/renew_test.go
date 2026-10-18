// Copyright 2026 The Forgejo Authors. All rights reserved.
// SPDX-License-Identifier: GPL-3.0-or-later

package cluster

import (
	"encoding/pem"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Vault or Nomad behind a private PKI is reached with its CA file, and
// refused without it.
func TestHTTPClientTrustsAPrivateCA(t *testing.T) {
	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {}))
	defer srv.Close()
	ca := filepath.Join(t.TempDir(), "ca.pem")
	require.NoError(t, os.WriteFile(ca, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: srv.Certificate().Raw}), 0o600))

	c, err := httpClientFor(srv.URL, ca, 5*time.Second)
	require.NoError(t, err)
	resp, err := c.Get(srv.URL)
	require.NoError(t, err)
	resp.Body.Close()

	c, err = httpClientFor(srv.URL, "", 5*time.Second)
	require.NoError(t, err)
	_, err = c.Get(srv.URL)
	require.Error(t, err, "the system roots do not know it")

	_, err = httpClientFor(srv.URL, filepath.Join(t.TempDir(), "missing.pem"), time.Second)
	assert.Error(t, err)
}
