// Copyright 2026 The Forgejo Authors. All rights reserved.
// SPDX-License-Identifier: GPL-3.0-or-later

package setting

import (
	"context"
	"net/http"
	"sync/atomic"
	"testing"

	actions_model "forgejo.org/models/actions"
	"forgejo.org/models/db"
	repo_model "forgejo.org/models/repo"
	unit_model "forgejo.org/models/unit"
	"forgejo.org/models/unittest"
	"forgejo.org/modules/clusterhook"
	"forgejo.org/modules/web"
	"forgejo.org/services/contexttest"
	"forgejo.org/services/forms"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// archiveRepo1 archives repo1, as the settings do before it can be
// unarchived.
func archiveRepo1(t *testing.T) {
	t.Helper()
	repo := unittest.AssertExistsAndLoadBean(t, &repo_model.Repository{ID: 1})
	require.NoError(t, repo_model.SetArchiveRepoState(db.DefaultContext, repo, true))
}

// postUnarchive is the settings button "Unarchive this repository" of
// repo1.
func postUnarchive(t *testing.T) (int, string) {
	t.Helper()
	ctx, resp := contexttest.MockContext(t, "user2/repo1/settings")
	contexttest.LoadUser(t, ctx, 2)
	contexttest.LoadRepo(t, ctx, 1)
	ctx.Req.Form.Set("action", "unarchive")
	web.SetForm(ctx, &forms.RepoSettingForm{})

	SettingsPost(ctx)
	return ctx.Resp.Status(), resp.Body.String()
}

// postUnits is the units settings form of repo1 with the units of form
// enabled, and the others disabled.
func postUnits(t *testing.T, form forms.RepoUnitSettingForm) (int, string) {
	t.Helper()
	ctx, resp := contexttest.MockContext(t, "user2/repo1/settings/units")
	contexttest.LoadUser(t, ctx, 2)
	contexttest.LoadRepo(t, ctx, 1)
	web.SetForm(ctx, &form)

	UnitsPost(ctx)
	return ctx.Resp.Status(), resp.Body.String()
}

// unitsWithoutWiki enables every unit repo1 has but the wiki, and the
// Actions unit when actions is true.
func unitsWithoutWiki(actions bool) forms.RepoUnitSettingForm {
	return forms.RepoUnitSettingForm{
		EnableCode:     true,
		EnableIssues:   true,
		EnablePulls:    true,
		EnableReleases: true,
		EnableProjects: true,
		EnablePackages: true,
		EnableActions:  actions,
	}
}

// keptSchedule registers a scheduled workflow of repo1, which a refused
// request must keep.
func keptSchedule(t *testing.T) *actions_model.ActionSchedule {
	t.Helper()
	kept := &actions_model.ActionSchedule{RepoID: 1, OwnerID: 2, WorkflowID: "kept.yml", Ref: "refs/heads/master", CommitSHA: "65f1bf27bc3bf70f64657658635e66094edbcb4d"}
	require.NoError(t, db.Insert(db.DefaultContext, kept))
	return kept
}

// Lifting the archiving registers the scheduled workflows of the default
// branch's head read in this node's git. On a node of the cluster behind
// on the repository, that head is the one before the last push another
// node acknowledged: it is refused with 503 and the reason, and the
// repository stays archived (#313).
func TestUnarchiveRefusedOnANodeBehind(t *testing.T) {
	unittest.PrepareTestEnv(t)
	archiveRepo1(t)
	clusterhook.SetFreshWaiter(func(context.Context, string) bool { return false })
	t.Cleanup(func() { clusterhook.SetFreshWaiter(nil) })

	status, body := postUnarchive(t)

	assert.Equal(t, http.StatusServiceUnavailable, status)
	assert.Contains(t, body, "may be behind")
	assert.True(t, unittest.AssertExistsAndLoadBean(t, &repo_model.Repository{ID: 1}).IsArchived)
}

// A node of the cluster that serves the repository at the cluster's
// commit index lifts the archiving, having asked once.
func TestUnarchiveOnAFreshNode(t *testing.T) {
	unittest.PrepareTestEnv(t)
	archiveRepo1(t)
	var asked atomic.Int32
	clusterhook.SetFreshWaiter(func(_ context.Context, key string) bool {
		assert.Equal(t, "user2/repo1.git", key)
		asked.Add(1)
		return true
	})
	t.Cleanup(func() { clusterhook.SetFreshWaiter(nil) })

	status, _ := postUnarchive(t)

	assert.Equal(t, http.StatusSeeOther, status)
	assert.EqualValues(t, 1, asked.Load())
	assert.False(t, unittest.AssertExistsAndLoadBean(t, &repo_model.Repository{ID: 1}).IsArchived)
}

// Saving the units with Actions enabled cleans the registered scheduled
// workflows, then registers those of the default branch's head read in
// this node's git. On a node of the cluster behind on the repository, it
// is refused with 503 and the reason, and nothing is written: the units
// and the schedules registered are kept (#313).
func TestUnitsWithActionsRefusedOnANodeBehind(t *testing.T) {
	unittest.PrepareTestEnv(t)
	kept := keptSchedule(t)
	clusterhook.SetFreshWaiter(func(context.Context, string) bool { return false })
	t.Cleanup(func() { clusterhook.SetFreshWaiter(nil) })

	status, body := postUnits(t, unitsWithoutWiki(true))

	assert.Equal(t, http.StatusServiceUnavailable, status)
	assert.Contains(t, body, "may be behind")
	unittest.AssertExistsAndLoadBean(t, &repo_model.RepoUnit{RepoID: 1, Type: unit_model.TypeWiki})
	unittest.AssertExistsAndLoadBean(t, &actions_model.ActionSchedule{ID: kept.ID})
}

// A node of the cluster that serves the repository at the cluster's
// commit index saves the units with Actions enabled, having asked once.
// Units saved without Actions read no git, and do not ask.
func TestUnitsOnAFreshNode(t *testing.T) {
	unittest.PrepareTestEnv(t)
	var asked atomic.Int32
	clusterhook.SetFreshWaiter(func(_ context.Context, key string) bool {
		assert.Equal(t, "user2/repo1.git", key)
		asked.Add(1)
		return true
	})
	t.Cleanup(func() { clusterhook.SetFreshWaiter(nil) })

	status, _ := postUnits(t, unitsWithoutWiki(true))
	assert.Equal(t, http.StatusSeeOther, status)
	assert.EqualValues(t, 1, asked.Load())
	unittest.AssertNotExistsBean(t, &repo_model.RepoUnit{RepoID: 1, Type: unit_model.TypeWiki})
	unittest.AssertExistsAndLoadBean(t, &repo_model.RepoUnit{RepoID: 1, Type: unit_model.TypeActions})

	status, _ = postUnits(t, unitsWithoutWiki(false))
	assert.Equal(t, http.StatusSeeOther, status)
	assert.EqualValues(t, 1, asked.Load(), "units saved without Actions do not ask")
	unittest.AssertNotExistsBean(t, &repo_model.RepoUnit{RepoID: 1, Type: unit_model.TypeActions})
}
