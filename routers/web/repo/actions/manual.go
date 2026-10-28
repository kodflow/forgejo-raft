// Copyright 2024 The Forgejo Authors.
// SPDX-License-Identifier: GPL-3.0-or-later

package actions

import (
	"errors"
	"net/http"
	"net/url"

	"forgejo.org/modules/clusterhook"
	actions_service "forgejo.org/services/actions"
	app_context "forgejo.org/services/context"
)

func ManualRunWorkflow(ctx *app_context.Context) {
	workflowID := ctx.FormString("workflow")
	if len(workflowID) == 0 {
		ctx.ServerError("workflow", nil)
		return
	}

	ref := ctx.FormString("ref")
	if len(ref) == 0 {
		ctx.ServerError("ref", nil)
		return
	}

	if empty, err := ctx.Repo.GitRepo.IsEmpty(); err != nil {
		ctx.ServerError("IsEmpty", err)
		return
	} else if empty {
		ctx.NotFound("IsEmpty", nil)
		return
	}

	workflow, err := actions_service.GetWorkflowToDispatch(ctx, ctx.Repo.Repository, ctx.Repo.GitRepo, ref, workflowID)
	if err != nil {
		if errors.Is(err, clusterhook.ErrBehind) {
			// A node of the cluster behind on the repository would run the
			// commit before the last push another node acknowledged, and
			// cancel the run of that push's head: nothing was written, the
			// user retries (#325).
			ctx.Error(http.StatusServiceUnavailable, err.Error())
			return
		}
		ctx.ServerError("GetWorkflowFromCommit", err)
		return
	}

	location := ctx.Repo.RepoLink + "/actions?workflow=" + url.QueryEscape(workflowID) +
		"&actor=" + url.QueryEscape(ctx.FormString("actor")) +
		"&status=" + url.QueryEscape(ctx.FormString("status"))

	formKeyGetter := func(key string) string {
		formKey := "inputs[" + key + "]"
		return ctx.Req.PostFormValue(formKey)
	}

	_, _, err = workflow.Dispatch(ctx, formKeyGetter, ctx.Repo.Repository, ctx.Doer)
	if err != nil {
		if actions_service.IsInputRequiredErr(err) {
			ctx.Flash.Error(ctx.Locale.Tr("actions.workflow.dispatch.input_required", err.(actions_service.InputRequiredErr).Name))
			ctx.Redirect(location)
			return
		}
		ctx.ServerError("workflow.Dispatch", err)
		return
	}

	// forward to the page of the run which was just created
	ctx.Flash.Info(ctx.Locale.Tr("actions.workflow.dispatch.success"))
	ctx.Redirect(location)
}
