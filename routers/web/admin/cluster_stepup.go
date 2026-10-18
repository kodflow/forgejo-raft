// Copyright 2026 The Forgejo Authors. All rights reserved.
// SPDX-License-Identifier: GPL-3.0-or-later

package admin

import (
	go_context "context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"net/http"
	"time"

	auth_model "forgejo.org/models/auth"
	cluster_model "forgejo.org/models/cluster"
	"forgejo.org/modules/json"
	"forgejo.org/modules/log"
	"forgejo.org/modules/setting"
	"forgejo.org/services/auth/source/oauth2"
	cluster_service "forgejo.org/services/cluster"
	"forgejo.org/services/cluster/stepup"
	"forgejo.org/services/context"
)

// Step-up through the instance's OpenID Connect provider ([cluster]
// STEP_UP_OIDC_SOURCE): once it is set, every operator re-authenticates
// there, as the subject pinned on the operator's grant, with a fresh login
// and, when STEP_UP_ACR is set, that authentication class. The requirement
// lives in the grant, not in the account's external login links, which the
// account itself may delete or replace: neither lowers it. It fails closed
// when the source is missing or no subject is pinned. Only the accounts
// listed in STEP_UP_PASSWORD_OPERATORS (break-glass, set in app.ini) keep the
// password and TOTP step-up.

const (
	sessionStepUpRequest = "cluster_step_up_request"
	sessionPendingOp     = "cluster_pending_operation"
)

// discover is the OpenID discovery, replaced in tests.
var discover = stepup.Discover

type stepUpSource struct {
	source     *auth_model.Source
	externalID string
}

// errStepUpUnavailable refuses an operation when the OpenID Connect step-up
// is required and cannot happen.
var errStepUpUnavailable = errors.New("the OpenID Connect step-up is required and unavailable")

// oidcStepUp returns the provider and the pinned subject the operator
// re-authenticates as; nil and no error when the password step-up applies;
// an error when the OpenID Connect step-up is required and cannot happen.
func oidcStepUp(ctx *context.Context) (*stepUpSource, error) {
	if setting.Cluster.StepUpOIDCSource == "" || ctx.Doer == nil {
		return nil, nil //nolint:nilnil // no provider: the password step-up applies
	}
	if setting.Cluster.StepUpPasswordOperators.Contains(ctx.Doer.LowerName) {
		return nil, nil //nolint:nilnil // break-glass: the password step-up applies
	}
	src, err := auth_model.GetActiveOAuth2SourceByName(ctx, setting.Cluster.StepUpOIDCSource)
	if err != nil {
		log.Error("cluster step-up: source %q: %v", setting.Cluster.StepUpOIDCSource, err)
		return nil, errStepUpUnavailable
	}
	cfg, ok := src.Cfg.(*oauth2.Source)
	if !ok || cfg.OpenIDConnectAutoDiscoveryURL == "" {
		log.Error("cluster step-up: source %q is not an OpenID Connect source", src.Name)
		return nil, errStepUpUnavailable
	}
	op, has, err := cluster_model.GetOperator(ctx, ctx.Doer.ID)
	if err != nil || !has || op.OIDCSubject == "" {
		log.Warn("cluster step-up: %s has no OpenID subject pinned; run `forgejo cluster operator pin` on a node", ctx.Doer.Name)
		return nil, errStepUpUnavailable
	}
	return &stepUpSource{source: src, externalID: op.OIDCSubject}, nil
}

func (s *stepUpSource) provider(ctx go_context.Context) (*stepup.Provider, error) {
	cfg := s.source.Cfg.(*oauth2.Source)
	return discover(ctx, cfg.OpenIDConnectAutoDiscoveryURL, cfg.ClientID, cfg.ClientSecret)
}

func randomHex(n int) string {
	b := make([]byte, n)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}

func callbackURL() string { return setting.AppURL + "admin/cluster/stepup/callback" }

// ClusterStepUp keeps the operation in the session and sends the operator
// to the provider.
func ClusterStepUp(ctx *context.Context) {
	if !isClusterOperator(ctx) {
		ctx.Error(http.StatusForbidden, "cluster operator permission required")
		return
	}
	s, err := oidcStepUp(ctx)
	if err != nil {
		ctx.Flash.Error(ctx.Tr("admin.cluster.op.step_up_oidc_unavailable"))
		ctx.Redirect(setting.AppSubURL + "/admin/cluster")
		return
	}
	if s == nil {
		ctx.Redirect(setting.AppSubURL + "/admin/cluster")
		return
	}
	o := clusterOperation(ctx)
	if !o.Action.Valid() || o.Target == "" {
		ctx.Flash.Error(ctx.Tr("admin.cluster.op.invalid"))
		ctx.Redirect(setting.AppSubURL + "/admin/cluster")
		return
	}
	p, err := s.provider(ctx)
	if err != nil {
		ctx.ServerError("step-up provider", err)
		return
	}
	req := stepup.Request{
		State: randomHex(16), Nonce: randomHex(16), Verifier: randomHex(32),
		Redirect: callbackURL(), ACR: setting.Cluster.StepUpACR, Started: time.Now().Unix(),
	}
	rb, _ := json.Marshal(req)
	ob, _ := json.Marshal(o)
	if err := ctx.Session.Set(sessionStepUpRequest, string(rb)); err != nil {
		ctx.ServerError("Session.Set", err)
		return
	}
	if err := ctx.Session.Set(sessionPendingOp, string(ob)); err != nil {
		ctx.ServerError("Session.Set", err)
		return
	}
	ctx.Redirect(p.AuthURL(req))
}

// ClusterStepUpCallback checks the provider's answer and, when it proves a
// fresh login of this very account, records the step-up and returns to the
// pending operation.
func ClusterStepUpCallback(ctx *context.Context) {
	if !isClusterOperator(ctx) {
		ctx.Error(http.StatusForbidden, "cluster operator permission required")
		return
	}
	err := func() error {
		s, err := oidcStepUp(ctx)
		if err != nil {
			return err
		}
		if s == nil {
			return errors.New("no OpenID Connect step-up applies to this account")
		}
		raw, _ := ctx.Session.Get(sessionStepUpRequest).(string)
		_ = ctx.Session.Delete(sessionStepUpRequest)
		var req stepup.Request
		if raw == "" || json.Unmarshal([]byte(raw), &req) != nil {
			return errors.New("no step-up in progress")
		}
		if ctx.FormString("state") != req.State {
			return errors.New("the answer belongs to another step-up")
		}
		if e := ctx.FormString("error"); e != "" {
			return errors.New("the provider refused: " + e)
		}
		p, err := s.provider(ctx)
		if err != nil {
			return err
		}
		tok, err := p.Exchange(ctx, req, ctx.FormString("code"))
		if err != nil {
			return err
		}
		claims, err := p.Verify(ctx, req, tok)
		if err != nil {
			return err
		}
		if claims.Subject != s.externalID {
			return errors.New("the provider authenticated another account")
		}
		log.Info("cluster step-up: %s re-authenticated through %s at %s", ctx.Doer.Name, s.source.Name, claims.AuthTime)
		return ctx.Session.Set(sessionClusterStepUp, time.Now().Unix())
	}()
	if err != nil {
		log.Warn("cluster step-up for %s refused: %v", ctx.Doer.Name, err)
		ctx.Flash.Error(ctx.Tr("admin.cluster.op.step_up_failed"))
		ctx.Redirect(setting.AppSubURL + "/admin/cluster")
		return
	}
	ctx.Redirect(setting.AppSubURL + "/admin/cluster/operate/pending")
}

// ClusterOperatePending shows the confirmation of the operation kept before
// the step-up.
func ClusterOperatePending(ctx *context.Context) {
	if !isClusterOperator(ctx) {
		ctx.Error(http.StatusForbidden, "cluster operator permission required")
		return
	}
	raw, _ := ctx.Session.Get(sessionPendingOp).(string)
	var o cluster_service.Operation
	if raw == "" || json.Unmarshal([]byte(raw), &o) != nil || !o.Action.Valid() {
		ctx.Redirect(setting.AppSubURL + "/admin/cluster")
		return
	}
	_ = ctx.Session.Delete(sessionPendingOp)
	renderConfirm(ctx, o, "", "")
}
