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
	"sync"
	"time"

	"forgejo.org/modules/json"
)

// JWTLogin logs in to Vault with the allocation's workload identity (ADR
// 0152 D17, R8-13): POST <Addr>/v1/auth/<Mount>/login with the role of the
// slot and the JWT Nomad writes into the task's secrets directory. The
// token is kept and logged in again at two thirds of its TTL, so it never
// expires under the renewer; the JWT file is read at each login, so a JWT
// Nomad rotated is picked up.
type JWTLogin struct {
	Addr    string
	Mount   string
	Role    string
	JWTFile string
	Client  *http.Client
	// Now is the clock, time.Now when nil.
	Now func() time.Time

	mu      sync.Mutex
	token   string
	relogin time.Time
}

func (l *JWTLogin) now() time.Time {
	if l.Now != nil {
		return l.Now()
	}
	return time.Now()
}

// Token returns a valid Vault token, logging in when none is held or the
// one held reached two thirds of its TTL.
func (l *JWTLogin) Token(ctx context.Context) (string, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.token != "" && l.now().Before(l.relogin) {
		return l.token, nil
	}
	tok, ttl, err := l.login(ctx)
	if err != nil {
		return "", err
	}
	l.token = tok
	if ttl <= 0 {
		// A token without TTL never needs a new login; one a day still
		// picks up a rotated role.
		ttl = 36 * time.Hour
	}
	l.relogin = l.now().Add(ttl * 2 / 3)
	return tok, nil
}

// Forget drops the token held, so the next call logs in again: Vault
// refused it (revoked, or its role changed).
func (l *JWTLogin) Forget() {
	l.mu.Lock()
	l.token = ""
	l.mu.Unlock()
}

func (l *JWTLogin) login(ctx context.Context) (string, time.Duration, error) {
	raw, err := os.ReadFile(l.JWTFile)
	if err != nil {
		return "", 0, fmt.Errorf("workload identity: %w", err)
	}
	jwt := strings.TrimSpace(string(raw))
	if jwt == "" {
		return "", 0, errors.New("workload identity: " + l.JWTFile + " is empty")
	}
	b, err := json.Marshal(map[string]string{"role": l.Role, "jwt": jwt})
	if err != nil {
		return "", 0, err
	}
	u := strings.TrimRight(l.Addr, "/") + "/v1/auth/" + strings.Trim(l.Mount, "/") + "/login"
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, u, bytes.NewReader(b))
	if err != nil {
		return "", 0, err
	}
	req.Header.Set("Content-Type", "application/json")
	c := l.Client
	if c == nil {
		c = &http.Client{Timeout: 30 * time.Second}
	}
	resp, err := c.Do(req)
	if err != nil {
		return "", 0, err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return "", 0, err
	}
	if resp.StatusCode != http.StatusOK {
		return "", 0, fmt.Errorf("vault login %s: %s", resp.Status, strings.TrimSpace(string(body)))
	}
	var out struct {
		Auth struct {
			ClientToken   string `json:"client_token"`
			LeaseDuration int64  `json:"lease_duration"`
		} `json:"auth"`
	}
	if err := json.Unmarshal(body, &out); err != nil {
		return "", 0, err
	}
	if out.Auth.ClientToken == "" {
		return "", 0, errors.New("vault login returned no token")
	}
	return out.Auth.ClientToken, time.Duration(out.Auth.LeaseDuration) * time.Second, nil
}

// FirstOf returns the first source that gives a token: the workload
// identity login, then the token file kept as a fallback. onFallback is told
// why the first failed.
func FirstOf(onFallback func(error), sources ...func(context.Context) (string, error)) func(context.Context) (string, error) {
	return func(ctx context.Context) (string, error) {
		var errs []error
		for i, src := range sources {
			if src == nil {
				continue
			}
			tok, err := src(ctx)
			if err == nil {
				if i > 0 && len(errs) > 0 && onFallback != nil {
					onFallback(errors.Join(errs...))
				}
				return tok, nil
			}
			errs = append(errs, err)
		}
		if len(errs) == 0 {
			return "", errors.New("no Vault token source configured")
		}
		return "", errors.Join(errs...)
	}
}
