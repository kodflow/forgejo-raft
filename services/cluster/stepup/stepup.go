// Copyright 2026 The Forgejo Authors. All rights reserved.
// SPDX-License-Identifier: GPL-3.0-or-later

// Package stepup re-authenticates a cluster operator through the instance's
// OpenID Connect provider before an online operation (kitsunium ADR 0152
// D19, R7-27, R8-33), for example one that enforces WebAuthn. The request
// asks for a fresh login (prompt=login, max_age=0) and, when
// configured, an authentication context class (acr_values); the answer is
// accepted only when its ID token is signed by the provider, names this
// client, carries our nonce, reports an authentication time after the
// request, and the required class.
package stepup

import (
	"context"
	"crypto"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rsa"
	"crypto/sha256"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"math/big"
	"net/http"
	"net/url"
	"strings"
	"time"

	"forgejo.org/modules/json"

	"github.com/golang-jwt/jwt/v5"
)

// Provider is an OpenID Connect provider as discovery describes it, with
// this instance's client.
type Provider struct {
	Issuer       string `json:"issuer"`
	AuthEndpoint string `json:"authorization_endpoint"`
	TokenURL     string `json:"token_endpoint"`
	JWKSURL      string `json:"jwks_uri"`
	ClientID     string `json:"-"`
	ClientSecret string `json:"-"`
	Client       *http.Client
}

func (p *Provider) client() *http.Client {
	if p.Client != nil {
		return p.Client
	}
	return &http.Client{Timeout: 15 * time.Second}
}

// Discover reads a provider's discovery document.
func Discover(ctx context.Context, discoveryURL, clientID, clientSecret string) (*Provider, error) {
	p := &Provider{ClientID: clientID, ClientSecret: clientSecret}
	if err := p.getJSON(ctx, discoveryURL, p); err != nil {
		return nil, fmt.Errorf("OpenID discovery: %w", err)
	}
	if p.Issuer == "" || p.AuthEndpoint == "" || p.TokenURL == "" || p.JWKSURL == "" {
		return nil, errors.New("OpenID discovery: an endpoint is missing")
	}
	return p, nil
}

func (p *Provider) getJSON(ctx context.Context, u string, v any) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	if err != nil {
		return err
	}
	resp, err := p.client().Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("%s answered %s", u, resp.Status)
	}
	return json.NewDecoder(io.LimitReader(resp.Body, 1<<20)).Decode(v)
}

// Request is one step-up in flight, kept in the operator's session.
type Request struct {
	State    string
	Nonce    string
	Verifier string
	Redirect string
	ACR      string
	Started  int64
}

// AuthURL is where the operator is sent: a fresh login is required, with
// PKCE, our nonce, and the authentication class when one is configured.
func (p *Provider) AuthURL(r Request) string {
	sum := sha256.Sum256([]byte(r.Verifier))
	q := url.Values{
		"response_type":         {"code"},
		"client_id":             {p.ClientID},
		"redirect_uri":          {r.Redirect},
		"scope":                 {"openid"},
		"state":                 {r.State},
		"nonce":                 {r.Nonce},
		"prompt":                {"login"},
		"max_age":               {"0"},
		"code_challenge":        {base64.RawURLEncoding.EncodeToString(sum[:])},
		"code_challenge_method": {"S256"},
	}
	if r.ACR != "" {
		q.Set("acr_values", r.ACR)
	}
	sep := "?"
	if strings.Contains(p.AuthEndpoint, "?") {
		sep = "&"
	}
	return p.AuthEndpoint + sep + q.Encode()
}

// Exchange trades the code for the ID token.
func (p *Provider) Exchange(ctx context.Context, r Request, code string) (string, error) {
	form := url.Values{
		"grant_type":    {"authorization_code"},
		"code":          {code},
		"redirect_uri":  {r.Redirect},
		"client_id":     {p.ClientID},
		"code_verifier": {r.Verifier},
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, p.TokenURL, strings.NewReader(form.Encode()))
	if err != nil {
		return "", err
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.SetBasicAuth(url.QueryEscape(p.ClientID), url.QueryEscape(p.ClientSecret))
	resp, err := p.client().Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	var out struct {
		IDToken string `json:"id_token"`
		Error   string `json:"error"`
	}
	if err := json.NewDecoder(io.LimitReader(resp.Body, 1<<20)).Decode(&out); err != nil {
		return "", err
	}
	if resp.StatusCode != http.StatusOK || out.IDToken == "" {
		return "", fmt.Errorf("token endpoint answered %s %s", resp.Status, out.Error)
	}
	return out.IDToken, nil
}

// Claims is what a verified step-up proves.
type Claims struct {
	Subject  string
	AuthTime time.Time
	ACR      string
}

// skew tolerates clocks a little apart.
const skew = 30 * time.Second

// Verify checks the ID token against the request (see the package comment).
func (p *Provider) Verify(ctx context.Context, r Request, idToken string) (*Claims, error) {
	keys, err := p.keys(ctx)
	if err != nil {
		return nil, err
	}
	tok, err := jwt.Parse(idToken, func(t *jwt.Token) (any, error) {
		kid, _ := t.Header["kid"].(string)
		if k, ok := keys[kid]; ok {
			return k, nil
		}
		if len(keys) == 1 && kid == "" {
			for _, k := range keys {
				return k, nil
			}
		}
		return nil, fmt.Errorf("no provider key %q", kid)
	}, jwt.WithValidMethods([]string{"RS256", "RS384", "RS512", "ES256", "ES384", "ES512", "PS256"}),
		jwt.WithIssuer(p.Issuer), jwt.WithAudience(p.ClientID), jwt.WithExpirationRequired(), jwt.WithLeeway(skew))
	if err != nil {
		return nil, fmt.Errorf("the ID token does not verify: %w", err)
	}
	mc, _ := tok.Claims.(jwt.MapClaims)
	if nonce, _ := mc["nonce"].(string); nonce == "" || nonce != r.Nonce {
		return nil, errors.New("the ID token does not carry this request's nonce")
	}
	at, ok := mc["auth_time"].(float64)
	if !ok {
		return nil, errors.New("the ID token has no auth_time: the provider did not report a fresh login")
	}
	authTime := time.Unix(int64(at), 0)
	if authTime.Before(time.Unix(r.Started, 0).Add(-skew)) {
		return nil, errors.New("the provider reused an older login: a fresh one is required")
	}
	acr, _ := mc["acr"].(string)
	if r.ACR != "" && acr != r.ACR {
		return nil, fmt.Errorf("the login's authentication class is %q, %q is required", acr, r.ACR)
	}
	sub, _ := mc["sub"].(string)
	if sub == "" {
		return nil, errors.New("the ID token names no subject")
	}
	return &Claims{Subject: sub, AuthTime: authTime, ACR: acr}, nil
}

type jwk struct {
	Kty string `json:"kty"`
	Kid string `json:"kid"`
	N   string `json:"n"`
	E   string `json:"e"`
	Crv string `json:"crv"`
	X   string `json:"x"`
	Y   string `json:"y"`
}

func (p *Provider) keys(ctx context.Context) (map[string]crypto.PublicKey, error) {
	var set struct {
		Keys []jwk `json:"keys"`
	}
	if err := p.getJSON(ctx, p.JWKSURL, &set); err != nil {
		return nil, fmt.Errorf("provider keys: %w", err)
	}
	out := map[string]crypto.PublicKey{}
	for _, k := range set.Keys {
		switch k.Kty {
		case "RSA":
			n, err1 := base64.RawURLEncoding.DecodeString(k.N)
			e, err2 := base64.RawURLEncoding.DecodeString(k.E)
			if err1 != nil || err2 != nil {
				continue
			}
			out[k.Kid] = &rsa.PublicKey{N: new(big.Int).SetBytes(n), E: int(new(big.Int).SetBytes(e).Int64())}
		case "EC":
			var c elliptic.Curve
			switch k.Crv {
			case "P-256":
				c = elliptic.P256()
			case "P-384":
				c = elliptic.P384()
			case "P-521":
				c = elliptic.P521()
			default:
				continue
			}
			x, err1 := base64.RawURLEncoding.DecodeString(k.X)
			y, err2 := base64.RawURLEncoding.DecodeString(k.Y)
			size := (c.Params().BitSize + 7) / 8
			if err1 != nil || err2 != nil || len(x) > size || len(y) > size {
				continue
			}
			// The uncompressed point, coordinates left-padded to the curve size.
			point := make([]byte, 1+2*size)
			point[0] = 4
			copy(point[1+size-len(x):1+size], x)
			copy(point[1+2*size-len(y):], y)
			pub, err := ecdsa.ParseUncompressedPublicKey(c, point)
			if err != nil {
				continue
			}
			out[k.Kid] = pub
		}
	}
	if len(out) == 0 {
		return nil, errors.New("the provider publishes no usable key")
	}
	return out, nil
}
