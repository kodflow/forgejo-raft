// Copyright 2026 The Forgejo Authors. All rights reserved.
// SPDX-License-Identifier: GPL-3.0-or-later

package certrenew

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"time"

	"forgejo.org/modules/json"
)

// Vault signs through Vault's PKI secrets engine: POST
// <Addr>/v1/<Mount>/sign/<Role> (D20: one role per slot, pki-consensus).
// The role sets require_cn=false, since the certificate carries a URI SAN
// only, and allowed_uri_sans to the slot's exact URI.
type Vault struct {
	Addr  string
	Mount string
	Role  string
	TTL   time.Duration
	// Token returns the Vault token; production logs in with the
	// allocation's workload identity, the lab with a dev root token.
	Token func(ctx context.Context) (string, error)
	// Denied, when set, is called when Vault refuses the token (403); the
	// signature is tried once more with a fresh token.
	Denied func()
	Client *http.Client
}

// TokenFile reads a token from a file at each call, so a token renewed on
// disk is picked up.
func TokenFile(path string) func(context.Context) (string, error) {
	return func(context.Context) (string, error) {
		b, err := os.ReadFile(path)
		if err != nil {
			return "", err
		}
		return strings.TrimSpace(string(b)), nil
	}
}

// Sign implements Signer.
func (v *Vault) Sign(ctx context.Context, csrPEM []byte, uriSAN string) ([]byte, error) {
	cert, err := v.sign(ctx, csrPEM, uriSAN)
	if errors.Is(err, errDenied) && v.Denied != nil {
		v.Denied()
		cert, err = v.sign(ctx, csrPEM, uriSAN)
	}
	return cert, err
}

var errDenied = errors.New("vault refused the token")

func (v *Vault) sign(ctx context.Context, csrPEM []byte, uriSAN string) ([]byte, error) {
	tok, err := v.Token(ctx)
	if err != nil {
		return nil, fmt.Errorf("vault token: %w", err)
	}
	body := map[string]any{"csr": string(csrPEM), "uri_sans": uriSAN, "format": "pem"}
	if v.TTL > 0 {
		body["ttl"] = v.TTL.String()
	}
	b, err := json.Marshal(body)
	if err != nil {
		return nil, err
	}
	u := strings.TrimRight(v.Addr, "/") + "/v1/" + strings.Trim(v.Mount, "/") + "/sign/" + v.Role
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, u, bytes.NewReader(b))
	if err != nil {
		return nil, err
	}
	req.Header.Set("X-Vault-Token", tok)
	req.Header.Set("Content-Type", "application/json")
	c := v.Client
	if c == nil {
		c = &http.Client{Timeout: 30 * time.Second}
	}
	resp, err := c.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	raw, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return nil, err
	}
	if resp.StatusCode == http.StatusForbidden {
		return nil, fmt.Errorf("%w: %s", errDenied, strings.TrimSpace(string(raw)))
	}
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("vault %s: %s", resp.Status, strings.TrimSpace(string(raw)))
	}
	var out struct {
		Data struct {
			Certificate string `json:"certificate"`
		} `json:"data"`
	}
	if err := json.Unmarshal(raw, &out); err != nil {
		return nil, err
	}
	if out.Data.Certificate == "" {
		return nil, errors.New("vault returned no certificate")
	}
	return []byte(out.Data.Certificate), nil
}
