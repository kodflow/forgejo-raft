// Copyright 2026 The Forgejo Authors. All rights reserved.
// SPDX-License-Identifier: GPL-3.0-or-later

package admin

import (
	go_context "context"
	"crypto/rand"
	"crypto/rsa"
	"encoding/base64"
	"math/big"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	auth_model "forgejo.org/models/auth"
	cluster_model "forgejo.org/models/cluster"
	"forgejo.org/models/db"
	"forgejo.org/models/unittest"
	user_model "forgejo.org/models/user"
	"forgejo.org/modules/container"
	"forgejo.org/modules/json"
	"forgejo.org/modules/setting"
	"forgejo.org/modules/test"
	"forgejo.org/services/auth/source/oauth2"
	"forgejo.org/services/cluster"
	"forgejo.org/services/cluster/stepup"
	"forgejo.org/services/contexttest"

	"github.com/golang-jwt/jwt/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// An operator linked to the OpenID source steps up there: no password is
// accepted, the provider's fresh login of this very account is, and the
// pending operation is then confirmed by its typed target alone.
func TestClusterStepUpThroughOIDC(t *testing.T) {
	unittest.PrepareTestEnv(t)
	mem, reps := cluster.NewForTesting(t.TempDir(), "node-a", "node-b", "node-c")
	defer cluster.SetCurrentForTesting(reps[0])()
	require.NoError(t, cluster_model.AddOperator(db.DefaultContext, 1, 0, ""))
	target := reps[1].Engine().Status().ID.String()

	// The provider: keys and a token endpoint answering for the last nonce.
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	require.NoError(t, err)
	var nonce string
	sub := "idp-user-1"
	mux := http.NewServeMux()
	idp := httptest.NewServer(mux)
	defer idp.Close()
	mux.HandleFunc("/.well-known/openid-configuration", func(w http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]string{
			"issuer": idp.URL, "authorization_endpoint": idp.URL + "/authorize",
			"token_endpoint": idp.URL + "/token", "jwks_uri": idp.URL + "/jwks", "userinfo_endpoint": idp.URL + "/userinfo",
		})
	})
	mux.HandleFunc("/jwks", func(w http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{"keys": []map[string]string{{
			"kty": "RSA", "kid": "k", "n": base64.RawURLEncoding.EncodeToString(key.N.Bytes()),
			"e": base64.RawURLEncoding.EncodeToString(big.NewInt(int64(key.E)).Bytes()),
		}}})
	})
	mux.HandleFunc("/token", func(w http.ResponseWriter, _ *http.Request) {
		tok := jwt.NewWithClaims(jwt.SigningMethodRS256, jwt.MapClaims{
			"iss": idp.URL, "aud": "forgejo", "sub": sub, "nonce": nonce,
			"auth_time": time.Now().Unix(), "exp": time.Now().Add(time.Minute).Unix(),
		})
		tok.Header["kid"] = "k"
		s, _ := tok.SignedString(key)
		_ = json.NewEncoder(w).Encode(map[string]string{"id_token": s})
	})
	defer test.MockVariableValue(&discover, func(_ go_context.Context, _, id, secret string) (*stepup.Provider, error) {
		return &stepup.Provider{Issuer: idp.URL, AuthEndpoint: idp.URL + "/authorize", TokenURL: idp.URL + "/token", JWKSURL: idp.URL + "/jwks", ClientID: id, ClientSecret: secret}, nil
	})()

	src := &auth_model.Source{Type: auth_model.OAuth2, Name: "oidc-stepup", IsActive: false, Cfg: &oauth2.Source{
		Provider: "openidConnect", ClientID: "forgejo", ClientSecret: "s", OpenIDConnectAutoDiscoveryURL: idp.URL + "/.well-known/openid-configuration",
	}}
	require.NoError(t, auth_model.CreateSource(db.DefaultContext, src))
	src.IsActive = true
	require.NoError(t, auth_model.UpdateSource(db.DefaultContext, src))
	u := unittest.AssertExistsAndLoadBean(t, &user_model.User{ID: 1})
	require.NoError(t, user_model.LinkExternalToUser(db.DefaultContext, u, &user_model.ExternalLoginUser{ExternalID: sub, UserID: 1, LoginSourceID: src.ID, Provider: "openidConnect"}))
	defer test.MockVariableValue(&setting.Cluster.StepUpOIDCSource, "oidc-stepup")()

	sess := newSession()
	op := url.Values{"action": {"transfer-leadership"}, "target": {target}, "confirm": {target}, "password": {"password"}}
	refused := func(what string) {
		t.Helper()
		_, run := clusterContext(t, "POST /admin/cluster/operate/confirm", op, newSession())
		_, errMsg, _ := run()
		assert.NotEmpty(t, errMsg, what)
		assert.NotEqual(t, reps[1].Engine().Status().ID, mem.Leader().Status().ID, what)
	}

	// With a step-up source, the password is refused, and with no subject
	// pinned on the grant the operation fails closed.
	refused("no subject pinned")
	ctx, resp := contexttest.MockContext(t, "POST /admin/cluster/stepup")
	ctx.Doer, ctx.Session, ctx.Req.Form = u, newSession(), op
	ClusterStepUp(ctx)
	assert.Equal(t, "/admin/cluster", resp.Header().Get("Location"), "no provider round trip without a pinned subject")

	// The grant pins the subject; the link the account holds no longer
	// matters: deleting it lowers nothing.
	require.Equal(t, sub, cluster.StepUpSubjectOf(db.DefaultContext, 1))
	require.NoError(t, cluster_model.PinOperatorSubject(db.DefaultContext, 1, sub))
	_, err = user_model.RemoveAccountLink(db.DefaultContext, u, src.ID)
	require.NoError(t, err)
	refused("the link was deleted: the password is still refused")

	// A source gone fails closed too.
	src.IsActive = false
	require.NoError(t, auth_model.UpdateSource(db.DefaultContext, src))
	refused("the source is inactive")
	src.IsActive = true
	require.NoError(t, auth_model.UpdateSource(db.DefaultContext, src))

	// The step-up starts: to the provider, a fresh login asked for.
	ctx, resp = contexttest.MockContext(t, "POST /admin/cluster/stepup")
	ctx.Doer, ctx.Session, ctx.Req.Form = u, sess, op
	ClusterStepUp(ctx)
	require.Equal(t, http.StatusSeeOther, resp.Code)
	loc, err := url.Parse(resp.Header().Get("Location"))
	require.NoError(t, err)
	assert.True(t, strings.HasPrefix(loc.String(), idp.URL+"/authorize"))
	assert.Equal(t, "login", loc.Query().Get("prompt"))
	assert.Equal(t, "0", loc.Query().Get("max_age"))
	nonce = loc.Query().Get("nonce")

	callback := func(state string) (int, string) {
		ctx, resp := contexttest.MockContext(t, "GET /admin/cluster/stepup/callback")
		ctx.Doer, ctx.Session = u, sess
		ctx.Req.Form = url.Values{"state": {state}, "code": {"c"}}
		ClusterStepUpCallback(ctx)
		return resp.Code, resp.Header().Get("Location")
	}
	// Another state is refused and does not step up.
	_, where := callback("forged")
	assert.Equal(t, "/admin/cluster", where)
	assert.Nil(t, sess.Get(sessionClusterStepUp))

	// The provider authenticated another account: refused.
	ctx, resp = contexttest.MockContext(t, "POST /admin/cluster/stepup")
	ctx.Doer, ctx.Session, ctx.Req.Form = u, sess, op
	ClusterStepUp(ctx)
	loc, _ = url.Parse(resp.Header().Get("Location"))
	nonce, state := loc.Query().Get("nonce"), loc.Query().Get("state")
	sub = "someone-else"
	_, where = callback(state)
	assert.Equal(t, "/admin/cluster", where)
	assert.Nil(t, sess.Get(sessionClusterStepUp))
	sub = "idp-user-1"

	// The right login steps up and returns to the pending operation.
	ctx, resp = contexttest.MockContext(t, "POST /admin/cluster/stepup")
	ctx.Doer, ctx.Session, ctx.Req.Form = u, sess, op
	ClusterStepUp(ctx)
	loc, _ = url.Parse(resp.Header().Get("Location"))
	nonce, state = loc.Query().Get("nonce"), loc.Query().Get("state")
	_, where = callback(state)
	assert.Equal(t, "/admin/cluster/operate/pending", where)
	assert.NotNil(t, sess.Get(sessionClusterStepUp))

	_, run := clusterContext(t, "POST /admin/cluster/operate/confirm", url.Values{"action": {"transfer-leadership"}, "target": {target}, "confirm": {target}}, sess)
	_, errMsg, okMsg := run()
	assert.Empty(t, errMsg)
	assert.NotEmpty(t, okMsg)
	assert.Equal(t, reps[1].Engine().Status().ID, mem.Leader().Status().ID)

	// A break-glass login listed in app.ini keeps the password step-up.
	var code int
	defer test.MockVariableValue(&setting.Cluster.StepUpPasswordOperators, container.SetOf("user1"))()
	back := reps[0].Engine().Status().ID.String()
	_, run = clusterContext(t, "POST /admin/cluster/operate/confirm", url.Values{"action": {"transfer-leadership"}, "target": {back}, "confirm": {back}, "password": {"password"}}, newSession())
	code, errMsg, _ = run()
	assert.Equal(t, http.StatusSeeOther, code, errMsg)
	assert.Equal(t, reps[0].Engine().Status().ID, mem.Leader().Status().ID)
}
