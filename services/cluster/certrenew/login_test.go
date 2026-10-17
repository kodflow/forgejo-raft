// Copyright 2026 The Forgejo Authors. All rights reserved.
// SPDX-License-Identifier: GPL-3.0-or-later

package certrenew

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"encoding/pem"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"forgejo.org/modules/json"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// fakeVault answers the JWT login of one role and the sign endpoint, as a
// Vault with jwt-consensus and pki-consensus would.
type fakeVault struct {
	mu      sync.Mutex
	logins  []string // the JWTs logged in with
	issued  map[string]bool
	revoked map[string]bool
	ttl     int64
	ca      *testCA
}

func (f *fakeVault) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	f.mu.Lock()
	defer f.mu.Unlock()
	switch r.URL.Path {
	case "/v1/auth/jwt-consensus/login":
		var in struct{ Role, JWT string }
		_ = json.NewDecoder(r.Body).Decode(&in)
		if in.Role != "node-a" || in.JWT == "" || in.JWT == "forged" {
			http.Error(w, `{"errors":["permission denied"]}`, http.StatusBadRequest)
			return
		}
		f.logins = append(f.logins, in.JWT)
		tok := fmt.Sprintf("hvs.%d", len(f.logins))
		f.issued[tok] = true
		_ = json.NewEncoder(w).Encode(map[string]any{"auth": map[string]any{"client_token": tok, "lease_duration": f.ttl}})
	case "/v1/pki-consensus/sign/node-a":
		tok := r.Header.Get("X-Vault-Token")
		if !f.issued[tok] || f.revoked[tok] {
			http.Error(w, `{"errors":["permission denied"]}`, http.StatusForbidden)
			return
		}
		var in struct {
			CSR     string `json:"csr"`
			URISans string `json:"uri_sans"`
		}
		_ = json.NewDecoder(r.Body).Decode(&in)
		s := &localSigner{ca: f.ca, life: time.Hour}
		cert, err := s.Sign(r.Context(), []byte(in.CSR), in.URISans)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"data": map[string]string{"certificate": string(cert)}})
	default:
		http.NotFound(w, r)
	}
}

func TestJWTLoginRenewsBeforeTheTTLEnds(t *testing.T) {
	f := &fakeVault{issued: map[string]bool{}, revoked: map[string]bool{}, ttl: 3600}
	srv := httptest.NewServer(f)
	defer srv.Close()
	jwt := filepath.Join(t.TempDir(), "nomad_consensus.jwt")
	require.NoError(t, os.WriteFile(jwt, []byte("jwt-1\n"), 0o600))
	now := time.Unix(1_800_000_000, 0)
	l := &JWTLogin{Addr: srv.URL, Mount: "jwt-consensus", Role: "node-a", JWTFile: jwt, Now: func() time.Time { return now }}
	ctx := context.Background()

	tok, err := l.Token(ctx)
	require.NoError(t, err)
	assert.Equal(t, "hvs.1", tok)
	now = now.Add(39 * time.Minute)
	tok, _ = l.Token(ctx)
	assert.Equal(t, "hvs.1", tok, "kept before two thirds of the TTL")

	// Nomad rotated the JWT; the next login reads the new one.
	require.NoError(t, os.WriteFile(jwt, []byte("jwt-2"), 0o600))
	now = now.Add(2 * time.Minute)
	tok, _ = l.Token(ctx)
	assert.Equal(t, "hvs.2", tok, "logged in again at two thirds")
	assert.Equal(t, []string{"jwt-1", "jwt-2"}, f.logins)

	// A JWT Vault refuses is an error, never an empty token.
	require.NoError(t, os.WriteFile(jwt, []byte("forged"), 0o600))
	l.Forget()
	_, err = l.Token(ctx)
	require.Error(t, err)
}

// The renewer signs through the login; a token Vault revoked is dropped and
// the signature retried with a fresh login; the token file is the fallback.
func TestVaultSignsThroughTheWorkloadIdentity(t *testing.T) {
	dir := t.TempDir()
	f := &fakeVault{issued: map[string]bool{}, revoked: map[string]bool{}, ttl: 3600, ca: newCA(t, dir)}
	srv := httptest.NewServer(f)
	defer srv.Close()
	jwt := filepath.Join(dir, "nomad_consensus.jwt")
	require.NoError(t, os.WriteFile(jwt, []byte("jwt-1"), 0o600))
	login := &JWTLogin{Addr: srv.URL, Mount: "jwt-consensus", Role: "node-a", JWTFile: jwt}
	var fellBack []error
	v := &Vault{
		Addr: srv.URL, Mount: "pki-consensus", Role: "node-a", Denied: login.Forget,
		Token: FirstOf(func(err error) { fellBack = append(fellBack, err) }, login.Token),
	}

	r := &Renewer{CAFile: f.ca.file, Cluster: cluster, Slot: "node-a", Signer: v}
	r.CertFile = filepath.Join(dir, "node.pem")
	r.KeyFile = r.CertFile
	writeKey(t, r.CertFile)
	_, err := r.RenewNow(context.Background())
	require.NoError(t, err)

	f.mu.Lock()
	f.revoked["hvs.1"] = true
	f.mu.Unlock()
	_, err = r.RenewNow(context.Background())
	require.NoError(t, err, "a revoked token is replaced by a new login")
	assert.Len(t, f.logins, 2)
	assert.Empty(t, fellBack)

	// The identity is gone and Vault was given a token file: it is used, and
	// the fallback is reported.
	require.NoError(t, os.Remove(jwt))
	login.Forget()
	tokFile := filepath.Join(dir, "token")
	require.NoError(t, os.WriteFile(tokFile, []byte("hvs.2\n"), 0o600))
	v.Token = FirstOf(func(err error) { fellBack = append(fellBack, err) }, login.Token, TokenFile(tokFile))
	_, err = r.RenewNow(context.Background())
	require.NoError(t, err)
	require.Len(t, fellBack, 1)
	assert.True(t, errors.Is(fellBack[0], os.ErrNotExist))
}

func writeKey(t *testing.T, file string) {
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)
	kb, err := x509.MarshalECPrivateKey(key)
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(file, pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: kb}), 0o600))
}
