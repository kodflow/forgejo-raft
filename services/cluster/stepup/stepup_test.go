// Copyright 2026 The Forgejo Authors. All rights reserved.
// SPDX-License-Identifier: GPL-3.0-or-later

package stepup

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"encoding/base64"
	"math/big"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"
	"time"

	"forgejo.org/modules/json"

	"github.com/golang-jwt/jwt/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// fakeIDP is an OpenID provider in process: discovery, keys, and a token
// endpoint whose ID token the test shapes.
type fakeIDP struct {
	srv      *httptest.Server
	key      *rsa.PrivateKey
	claims   jwt.MapClaims
	verifier string
}

func newIDP(t *testing.T) *fakeIDP {
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	require.NoError(t, err)
	f := &fakeIDP{key: key}
	mux := http.NewServeMux()
	f.srv = httptest.NewServer(mux)
	t.Cleanup(f.srv.Close)
	mux.HandleFunc("/.well-known/openid-configuration", func(w http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]string{
			"issuer": f.srv.URL, "authorization_endpoint": f.srv.URL + "/authorize",
			"token_endpoint": f.srv.URL + "/token", "jwks_uri": f.srv.URL + "/jwks",
		})
	})
	mux.HandleFunc("/jwks", func(w http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{"keys": []map[string]string{{
			"kty": "RSA", "kid": "k1", "n": base64.RawURLEncoding.EncodeToString(key.N.Bytes()),
			"e": base64.RawURLEncoding.EncodeToString(big.NewInt(int64(key.E)).Bytes()),
		}}})
	})
	mux.HandleFunc("/token", func(w http.ResponseWriter, r *http.Request) {
		_ = r.ParseForm()
		f.verifier = r.Form.Get("code_verifier")
		tok := jwt.NewWithClaims(jwt.SigningMethodRS256, f.claims)
		tok.Header["kid"] = "k1"
		s, err := tok.SignedString(key)
		require.NoError(t, err)
		_ = json.NewEncoder(w).Encode(map[string]string{"id_token": s, "access_token": "x", "token_type": "Bearer"})
	})
	return f
}

func (f *fakeIDP) provider(t *testing.T) *Provider {
	p, err := Discover(context.Background(), f.srv.URL+"/.well-known/openid-configuration", "forgejo", "secret")
	require.NoError(t, err)
	return p
}

func request(acr string) Request {
	return Request{State: "st", Nonce: "n-123", Verifier: "verifier-0123456789-0123456789-0123456789", Redirect: "https://forge/admin/cluster/stepup/callback", ACR: acr, Started: time.Now().Unix()}
}

func (f *fakeIDP) good(r Request) jwt.MapClaims {
	return jwt.MapClaims{
		"iss": f.srv.URL, "aud": "forgejo", "sub": "user-42", "nonce": r.Nonce,
		"auth_time": time.Now().Unix(), "exp": time.Now().Add(time.Minute).Unix(), "iat": time.Now().Unix(),
		"acr": "urn:example:acr:default",
	}
}

func TestAuthURLAsksForAFreshLogin(t *testing.T) {
	f := newIDP(t)
	p := f.provider(t)
	r := request("webauthn")
	u, err := url.Parse(p.AuthURL(r))
	require.NoError(t, err)
	q := u.Query()
	assert.Equal(t, "login", q.Get("prompt"))
	assert.Equal(t, "0", q.Get("max_age"))
	assert.Equal(t, "webauthn", q.Get("acr_values"))
	assert.Equal(t, "S256", q.Get("code_challenge_method"))
	sum := sha256.Sum256([]byte(r.Verifier))
	assert.Equal(t, base64.RawURLEncoding.EncodeToString(sum[:]), q.Get("code_challenge"))
	assert.Equal(t, r.Nonce, q.Get("nonce"))
}

func TestVerifyAcceptsAFreshLoginOnly(t *testing.T) {
	f := newIDP(t)
	p := f.provider(t)
	ctx := context.Background()
	r := request("")

	f.claims = f.good(r)
	tok, err := p.Exchange(ctx, r, "code")
	require.NoError(t, err)
	assert.Equal(t, r.Verifier, f.verifier, "PKCE verifier sent")
	c, err := p.Verify(ctx, r, tok)
	require.NoError(t, err)
	assert.Equal(t, "user-42", c.Subject)

	cases := map[string]func(jwt.MapClaims){
		"another nonce":    func(m jwt.MapClaims) { m["nonce"] = "other" },
		"another audience": func(m jwt.MapClaims) { m["aud"] = "someone-else" },
		"another issuer":   func(m jwt.MapClaims) { m["iss"] = "https://evil" },
		"an old login":     func(m jwt.MapClaims) { m["auth_time"] = time.Now().Add(-10 * time.Minute).Unix() },
		"no auth_time":     func(m jwt.MapClaims) { delete(m, "auth_time") },
		"expired":          func(m jwt.MapClaims) { m["exp"] = time.Now().Add(-10 * time.Minute).Unix() },
	}
	for name, mutate := range cases {
		t.Run(name, func(t *testing.T) {
			f.claims = f.good(r)
			mutate(f.claims)
			tok, err := p.Exchange(ctx, r, "code")
			require.NoError(t, err)
			_, err = p.Verify(ctx, r, tok)
			assert.Error(t, err)
		})
	}

	t.Run("a required class", func(t *testing.T) {
		r := request("webauthn")
		f.claims = f.good(r)
		tok, _ := p.Exchange(ctx, r, "code")
		_, err := p.Verify(ctx, r, tok)
		require.Error(t, err, "the class differs")
		f.claims["acr"] = "webauthn"
		tok, _ = p.Exchange(ctx, r, "code")
		_, err = p.Verify(ctx, r, tok)
		require.NoError(t, err)
	})

	t.Run("a token signed by another key", func(t *testing.T) {
		other, _ := rsa.GenerateKey(rand.Reader, 2048)
		tok := jwt.NewWithClaims(jwt.SigningMethodRS256, f.good(r))
		tok.Header["kid"] = "k1"
		s, _ := tok.SignedString(other)
		_, err := p.Verify(ctx, r, s)
		assert.Error(t, err)
	})
}

// An EC key published by the provider verifies an ES256 ID token.
func TestVerifyWithAnECKey(t *testing.T) {
	ek, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)
	mux := http.NewServeMux()
	srv := httptest.NewServer(mux)
	defer srv.Close()
	pad := func(b []byte) string {
		out := make([]byte, 32)
		copy(out[32-len(b):], b)
		return base64.RawURLEncoding.EncodeToString(out)
	}
	raw, _ := ek.PublicKey.Bytes()
	mux.HandleFunc("/jwks", func(w http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{"keys": []map[string]string{{
			"kty": "EC", "kid": "e1", "crv": "P-256", "x": pad(raw[1:33]), "y": pad(raw[33:]),
		}}})
	})
	p := &Provider{Issuer: srv.URL, JWKSURL: srv.URL + "/jwks", ClientID: "forgejo"}
	r := request("")
	tok := jwt.NewWithClaims(jwt.SigningMethodES256, jwt.MapClaims{
		"iss": srv.URL, "aud": "forgejo", "sub": "u", "nonce": r.Nonce,
		"auth_time": time.Now().Unix(), "exp": time.Now().Add(time.Minute).Unix(),
	})
	tok.Header["kid"] = "e1"
	s, err := tok.SignedString(ek)
	require.NoError(t, err)
	c, err := p.Verify(context.Background(), r, s)
	require.NoError(t, err)
	assert.Equal(t, "u", c.Subject)
}
