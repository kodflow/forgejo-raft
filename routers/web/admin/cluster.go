// Copyright 2026 The Forgejo Authors. All rights reserved.
// SPDX-License-Identifier: GPL-3.0-or-later

package admin

import (
	go_context "context"
	"errors"
	"net/http"
	"strings"
	"time"

	auth_model "forgejo.org/models/auth"
	cluster_model "forgejo.org/models/cluster"
	user_model "forgejo.org/models/user"
	"forgejo.org/modules/base"
	"forgejo.org/modules/setting"
	cluster_service "forgejo.org/services/cluster"
	"forgejo.org/services/context"
)

const (
	tplCluster        base.TplName = "admin/cluster/status"
	tplClusterConfirm base.TplName = "admin/cluster/confirm"

	sessionClusterStepUp = "cluster_step_up_at"
)

// The cluster's online operations (kitsunium ADR 0152 D17) are web only:
// no REST route and no API token reaches them. Each needs a site
// administrator who also holds the cluster-operator permission
// (the cluster_operator table), a fresh re-authentication, and a typed
// confirmation. The operator recorded is the session's own account, never
// an impersonated one: the web has no sudo.

func isClusterOperator(ctx *context.Context) bool {
	return cluster_service.IsOperator(ctx, ctx.Doer)
}

// Cluster shows the node's view of the cluster.
func Cluster(ctx *context.Context) {
	ctx.Data["Title"] = ctx.Tr("admin.cluster")
	ctx.Data["PageIsAdminCluster"] = true
	ctx.Data["IsClusterOperator"] = isClusterOperator(ctx)
	ctx.Data["ClusterActions"] = cluster_service.Actions
	r := cluster_service.Current()
	if r == nil {
		ctx.Data["ClusterDisabled"] = true
		ctx.HTML(http.StatusOK, tplCluster)
		return
	}
	view := r.View(ctx)
	ctx.Data["View"] = view
	ops, err := cluster_model.ListOperators(ctx)
	if err != nil {
		ctx.ServerError("ListOperators", err)
		return
	}
	var names []string
	for _, o := range ops {
		if u, err := user_model.GetUserByID(ctx, o.UserID); err == nil {
			names = append(names, u.Name)
		}
	}
	ctx.Data["ClusterOperators"] = names
	ctx.HTML(http.StatusOK, tplCluster)
}

func clusterOperation(ctx *context.Context) cluster_service.Operation {
	return cluster_service.Operation{
		Action:  cluster_service.Action(ctx.FormString("action")),
		Target:  strings.TrimSpace(ctx.FormString("target")),
		Address: strings.TrimSpace(ctx.FormString("address")),
	}
}

// confirmWord is what the operator must type: the node id, or for a new
// member its slot, or the alarm id.
func confirmWord(o cluster_service.Operation) string {
	if o.Action == cluster_service.ActionGrantOperator || o.Action == cluster_service.ActionRevokeOperator {
		return o.Target
	}
	if o.Action == cluster_service.ActionAuthorizeJoin {
		slot, _, _ := strings.Cut(o.Target, ":")
		return slot
	}
	if o.Action == cluster_service.ActionAddLearner {
		parts := strings.SplitN(o.Target, ":", 4)
		if len(parts) >= 3 {
			return parts[2]
		}
	}
	return o.Target
}

func stepUpFresh(ctx *context.Context) bool {
	at, ok := ctx.Session.Get(sessionClusterStepUp).(int64)
	return ok && time.Since(time.Unix(at, 0)) < setting.Cluster.StepUpMaxAge
}

// ClusterOperate shows the confirmation step of an operation.
func ClusterOperate(ctx *context.Context) {
	if !isClusterOperator(ctx) {
		ctx.Error(http.StatusForbidden, "cluster operator permission required")
		return
	}
	o := clusterOperation(ctx)
	if !o.Action.Valid() || o.Target == "" {
		ctx.Flash.Error(ctx.Tr("admin.cluster.op.invalid"))
		ctx.Redirect(setting.AppSubURL + "/admin/cluster")
		return
	}
	renderConfirm(ctx, o, "", "")
}

// The fields an error can point at.
const (
	errFieldConfirm  = "confirm"
	errFieldPassword = "password"
)

func renderConfirm(ctx *context.Context, o cluster_service.Operation, errField, errMsg string) {
	ctx.Data["Title"] = ctx.Tr("admin.cluster")
	ctx.Data["PageIsAdminCluster"] = true
	ctx.Data["Operation"] = o
	ctx.Data["ConfirmWord"] = confirmWord(o)
	ctx.Data["NeedStepUp"] = !stepUpFresh(ctx)
	ctx.Data["NeedPasscode"], _ = auth_model.HasTwoFactorByUID(ctx, ctx.Doer.ID)
	ctx.Data["HasPassword"] = ctx.Doer.IsPasswordSet()
	if s, err := oidcStepUp(ctx); err != nil {
		ctx.Data["StepUpBlocked"] = true
	} else if s != nil {
		ctx.Data["StepUpOIDC"] = s.source.Name
	}
	if errMsg != "" {
		switch errField {
		case errFieldConfirm:
			ctx.Data["Err_Confirm"] = true
		case errFieldPassword:
			ctx.Data["Err_Password"] = true
		}
		ctx.Flash.Error(errMsg, true)
	}
	ctx.HTML(http.StatusOK, tplClusterConfirm)
}

// ClusterOperateConfirm re-authenticates the operator, checks the typed
// confirmation, and runs the operation on this node, which forwards it to
// the leader.
func ClusterOperateConfirm(ctx *context.Context) {
	if !isClusterOperator(ctx) {
		ctx.Error(http.StatusForbidden, "cluster operator permission required")
		return
	}
	o := clusterOperation(ctx)
	if !o.Action.Valid() || o.Target == "" {
		ctx.Flash.Error(ctx.Tr("admin.cluster.op.invalid"))
		ctx.Redirect(setting.AppSubURL + "/admin/cluster")
		return
	}
	if ctx.FormString("confirm") != confirmWord(o) {
		renderConfirm(ctx, o, errFieldConfirm, ctx.Locale.TrString("admin.cluster.op.confirm_mismatch"))
		return
	}
	if !stepUpFresh(ctx) {
		// With a step-up source, an operator steps up at the provider, never
		// with a password, and nothing happens when that is impossible.
		if s, err := oidcStepUp(ctx); err != nil {
			renderConfirm(ctx, o, "", ctx.Locale.TrString("admin.cluster.op.step_up_oidc_unavailable"))
			return
		} else if s != nil {
			renderConfirm(ctx, o, "", ctx.Locale.TrString("admin.cluster.op.step_up_oidc_required"))
			return
		}
	}
	if !stepUpFresh(ctx) {
		if !ctx.Doer.IsPasswordSet() {
			renderConfirm(ctx, o, "", ctx.Locale.TrString("admin.cluster.op.step_up_no_password"))
			return
		}
		if !ctx.Doer.ValidatePassword(ctx, ctx.FormString("password")) {
			renderConfirm(ctx, o, errFieldPassword, ctx.Locale.TrString("admin.cluster.op.step_up_failed"))
			return
		}
		if has, _ := auth_model.HasTwoFactorByUID(ctx, ctx.Doer.ID); has {
			twofa, err := auth_model.GetTwoFactorByUID(ctx, ctx.Doer.ID)
			if err != nil {
				ctx.ServerError("GetTwoFactorByUID", err)
				return
			}
			passcode := ctx.FormString("passcode")
			ok, err := twofa.ValidateTOTP(passcode)
			// A passcode counts once, as at login: a replay inside its
			// window does not step up again.
			if err != nil || !ok || twofa.LastUsedPasscode == passcode {
				renderConfirm(ctx, o, errFieldPassword, ctx.Locale.TrString("admin.cluster.op.step_up_failed"))
				return
			}
			twofa.LastUsedPasscode = passcode
			if err := auth_model.UpdateTwoFactor(ctx, twofa); err != nil {
				ctx.ServerError("UpdateTwoFactor", err)
				return
			}
		}
		if err := ctx.Session.Set(sessionClusterStepUp, time.Now().Unix()); err != nil {
			ctx.ServerError("Session.Set", err)
			return
		}
	}

	r := cluster_service.Current()
	if r == nil {
		ctx.Flash.Error(ctx.Tr("admin.cluster.disabled"))
		ctx.Redirect(setting.AppSubURL + "/admin/cluster")
		return
	}
	opCtx, cancel := go_context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	if err := r.Operate(opCtx, cluster_service.OperatorFor(ctx.Doer.ID), o); err != nil {
		reason := err.Error()
		switch {
		case errors.Is(err, cluster_service.ErrLastOperator):
			reason = ctx.Locale.TrString("admin.cluster.op.last_operator")
		case errors.Is(err, cluster_service.ErrNotSiteAdmin):
			reason = ctx.Locale.TrString("admin.cluster.op.not_site_admin", o.Target)
		case user_model.IsErrUserNotExist(err):
			reason = ctx.Locale.TrString("admin.cluster.op.no_such_user", o.Target)
		}
		ctx.Flash.Error(ctx.Tr("admin.cluster.op.failed", ctx.Locale.TrString("admin.cluster.op."+string(o.Action)), reason))
	} else {
		ctx.Flash.Success(ctx.Tr("admin.cluster.op.done", ctx.Locale.TrString("admin.cluster.op."+string(o.Action)), o.Target))
	}
	ctx.Redirect(setting.AppSubURL + "/admin/cluster")
}
