// Copyright 2026 The Forgejo Authors. All rights reserved.
// SPDX-License-Identifier: GPL-3.0-or-later

package admin

import (
	"net/http"
	"net/url"
	"testing"
	"time"

	auth_model "forgejo.org/models/auth"
	cluster_model "forgejo.org/models/cluster"
	"forgejo.org/models/db"
	"forgejo.org/models/unittest"
	user_model "forgejo.org/models/user"
	"forgejo.org/services/cluster"
	"forgejo.org/services/contexttest"

	"code.forgejo.org/go-chi/session"
	"github.com/pquerna/otp/totp"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// mapSession is the part of a session the cluster pages use.
type mapSession struct {
	session.Store
	m map[any]any
}

func newSession() *mapSession            { return &mapSession{m: map[any]any{}} }
func (s *mapSession) Get(k any) any      { return s.m[k] }
func (s *mapSession) Set(k, v any) error { s.m[k] = v; return nil }
func (s *mapSession) Delete(k any) error { delete(s.m, k); return nil }

func clusterContext(t *testing.T, path string, form url.Values, sess *mapSession) (*user_model.User, func() (int, string, string)) {
	t.Helper()
	ctx, resp := contexttest.MockContext(t, path)
	u := unittest.AssertExistsAndLoadBean(t, &user_model.User{ID: 1})
	require.True(t, u.IsAdmin)
	ctx.Doer = u
	ctx.Session = sess
	ctx.Req.Form = form
	return u, func() (int, string, string) {
		switch path {
		case "POST /admin/cluster/operate":
			ClusterOperate(ctx)
		case "POST /admin/cluster/operate/confirm":
			ClusterOperateConfirm(ctx)
		default:
			Cluster(ctx)
		}
		return resp.Code, ctx.Flash.ErrorMsg, ctx.Flash.SuccessMsg
	}
}

func TestClusterAdministration(t *testing.T) {
	unittest.PrepareTestEnv(t)
	mem, reps := cluster.NewForTesting(t.TempDir(), "node-a", "node-b", "node-c")
	defer cluster.SetCurrentForTesting(reps[0])()
	target := reps[1].Engine().Status().ID.String()
	op := url.Values{"action": {"transfer-leadership"}, "target": {target}}

	t.Run("the page shows the node to any site administrator", func(t *testing.T) {
		_, run := clusterContext(t, "GET /admin/cluster", url.Values{}, newSession())
		code, _, _ := run()
		assert.Equal(t, http.StatusOK, code)
	})

	t.Run("a site administrator without the cluster permission cannot operate", func(t *testing.T) {
		_, run := clusterContext(t, "POST /admin/cluster/operate/confirm", op, newSession())
		code, _, _ := run()
		assert.Equal(t, http.StatusForbidden, code)
		assert.NotEqual(t, reps[1].Engine().Status().ID, mem.Leader().Status().ID)
	})

	require.NoError(t, cluster_model.AddOperator(db.DefaultContext, 1, 0, ""))

	t.Run("a wrong confirmation does nothing", func(t *testing.T) {
		f := url.Values{"action": {"transfer-leadership"}, "target": {target}, "confirm": {"nope"}, "password": {"password"}}
		_, run := clusterContext(t, "POST /admin/cluster/operate/confirm", f, newSession())
		code, errMsg, _ := run()
		assert.Equal(t, http.StatusOK, code)
		assert.NotEmpty(t, errMsg)
		assert.NotEqual(t, reps[1].Engine().Status().ID, mem.Leader().Status().ID)
	})

	t.Run("a wrong password does nothing", func(t *testing.T) {
		f := url.Values{"action": {"transfer-leadership"}, "target": {target}, "confirm": {target}, "password": {"wrong"}}
		_, run := clusterContext(t, "POST /admin/cluster/operate/confirm", f, newSession())
		_, errMsg, _ := run()
		assert.NotEmpty(t, errMsg)
		assert.NotEqual(t, reps[1].Engine().Status().ID, mem.Leader().Status().ID)
	})

	sess := newSession()
	t.Run("an operator who re-authenticates runs it, audited", func(t *testing.T) {
		f := url.Values{"action": {"transfer-leadership"}, "target": {target}, "confirm": {target}, "password": {"password"}}
		_, run := clusterContext(t, "POST /admin/cluster/operate/confirm", f, sess)
		code, errMsg, okMsg := run()
		assert.Equal(t, http.StatusSeeOther, code)
		assert.Empty(t, errMsg)
		assert.NotEmpty(t, okMsg)
		assert.Equal(t, reps[1].Engine().Status().ID, mem.Leader().Status().ID)
		audit := reps[2].Table().Audit()
		require.NotEmpty(t, audit)
		assert.Equal(t, "forgejo-user:1", audit[0].Operator)
		assert.Equal(t, "transfer-leadership", audit[0].Action)
		assert.Equal(t, "ok", audit[0].Outcome)
	})

	t.Run("a fresh step-up needs only the typed confirmation", func(t *testing.T) {
		back := reps[0].Engine().Status().ID.String()
		f := url.Values{"action": {"transfer-leadership"}, "target": {back}, "confirm": {back}}
		_, run := clusterContext(t, "POST /admin/cluster/operate/confirm", f, sess)
		_, errMsg, _ := run()
		assert.Empty(t, errMsg)
		assert.Equal(t, reps[0].Engine().Status().ID, mem.Leader().Status().ID)
	})

	t.Run("an unknown operation is refused", func(t *testing.T) {
		f := url.Values{"action": {"bootstrap"}, "target": {target}}
		_, run := clusterContext(t, "POST /admin/cluster/operate", f, newSession())
		_, errMsg, _ := run()
		assert.NotEmpty(t, errMsg)
	})

	t.Run("the permission is granted to site administrators only, and the last one stays", func(t *testing.T) {
		grant := func(action, login string) string {
			f := url.Values{"action": {action}, "target": {login}, "confirm": {login}}
			_, run := clusterContext(t, "POST /admin/cluster/operate/confirm", f, sess)
			_, errMsg, _ := run()
			return errMsg
		}
		assert.NotEmpty(t, grant("grant-operator", "user2"), "user2 is not a site administrator")
		assert.NotEmpty(t, grant("grant-operator", "no-such-login-x9"), "an unknown login is refused")
		assert.NotEmpty(t, grant("revoke-operator", "user1"), "the last operator stays")
		// The replicated audit names the accounts by id, never by login, even
		// for a login that does not exist.
		var unknown bool
		for _, a := range reps[0].Table().Audit() {
			if a.Target == "unknown-account" {
				unknown = true
				if a.Outcome == "failed" {
					assert.Equal(t, "unknown user", a.Detail)
				}
				continue
			}
			if a.Action == "grant-operator" || a.Action == "revoke-operator" {
				assert.Regexp(t, `^forgejo-user:[0-9]+$`, a.Target)
				assert.NotContains(t, a.Detail, "user2", a.Detail)
				assert.NotContains(t, a.Detail, "no-such-login-x9", a.Detail)
			}
		}
		assert.True(t, unknown, "the unknown login was audited, without its name")
		ok, err := cluster_model.IsOperator(db.DefaultContext, 1)
		require.NoError(t, err)
		assert.True(t, ok)
	})
}

// A TOTP passcode steps up once: replayed inside its window, it is refused.
func TestClusterStepUpPasscodeCountsOnce(t *testing.T) {
	unittest.PrepareTestEnv(t)
	mem, reps := cluster.NewForTesting(t.TempDir(), "node-a", "node-b", "node-c")
	defer cluster.SetCurrentForTesting(reps[0])()
	require.NoError(t, cluster_model.AddOperator(db.DefaultContext, 1, 0, ""))
	key, err := totp.Generate(totp.GenerateOpts{Issuer: "forgejo", AccountName: "user1"})
	require.NoError(t, err)
	require.NoError(t, auth_model.NewTwoFactor(db.DefaultContext, &auth_model.TwoFactor{UID: 1}, key.Secret()))
	passcode, err := totp.GenerateCode(key.Secret(), time.Now())
	require.NoError(t, err)

	step := func(target string) string {
		f := url.Values{"action": {"transfer-leadership"}, "target": {target}, "confirm": {target}, "password": {"password"}, "passcode": {passcode}}
		_, run := clusterContext(t, "POST /admin/cluster/operate/confirm", f, newSession())
		_, errMsg, _ := run()
		return errMsg
	}
	to := reps[1].Engine().Status().ID.String()
	require.Empty(t, step(to), "the first use steps up")
	require.Equal(t, reps[1].Engine().Status().ID, mem.Leader().Status().ID)
	back := reps[0].Engine().Status().ID.String()
	assert.NotEmpty(t, step(back), "the same passcode again is refused")
	assert.Equal(t, reps[1].Engine().Status().ID, mem.Leader().Status().ID)
}
