// Copyright 2026 The Forgejo Authors. All rights reserved.
// SPDX-License-Identifier: GPL-3.0-or-later

package repo

import (
	"context"
	"net/http"
	"testing"

	actions_model "forgejo.org/models/actions"
	"forgejo.org/models/db"
	repo_model "forgejo.org/models/repo"
	unit_model "forgejo.org/models/unit"
	"forgejo.org/models/unittest"
	api "forgejo.org/modules/structs"
	"forgejo.org/modules/test"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Lifting the archiving through the API registers the scheduled workflows
// of the default branch's head read in this node's git. On a node of the
// cluster behind on the repository, that head is the one before the last
// push another node acknowledged: the request is refused with 503 and the
// reason before anything is written, the rename of the same request
// included, and the repository stays archived (#313).
func TestEditUnarchiveRefusedOnANodeBehind(t *testing.T) {
	unittest.PrepareTestEnv(t)
	repo := unittest.AssertExistsAndLoadBean(t, &repo_model.Repository{ID: 1})
	require.NoError(t, repo_model.SetArchiveRepoState(db.DefaultContext, repo, true))
	defer test.MockVariableValue(&clusterFresh, behind)()
	name, archived := "renamed-behind", false

	status, body := editRepo1(t, api.EditRepoOption{Name: &name, Archived: &archived})

	assert.Equal(t, http.StatusServiceUnavailable, status)
	assert.Contains(t, body, "may be behind")
	repo = unittest.AssertExistsAndLoadBean(t, &repo_model.Repository{ID: 1})
	assert.Equal(t, "repo1", repo.Name)
	assert.True(t, repo.IsArchived)
}

// Enabling the Actions unit through the API cleans the registered
// scheduled workflows, then registers those of the default branch's head
// read in this node's git. On a node of the cluster behind on the
// repository, the request is refused with 503 before anything is written:
// neither the description nor the schedules registered change (#313).
func TestEditActionsRefusedOnANodeBehind(t *testing.T) {
	unittest.PrepareTestEnv(t)
	kept := &actions_model.ActionSchedule{RepoID: 1, OwnerID: 2, WorkflowID: "kept.yml", Ref: "refs/heads/master", CommitSHA: "65f1bf27bc3bf70f64657658635e66094edbcb4d"}
	require.NoError(t, db.Insert(db.DefaultContext, kept))
	defer test.MockVariableValue(&clusterFresh, behind)()
	description, actions := "changed behind", true

	status, body := editRepo1(t, api.EditRepoOption{Description: &description, HasActions: &actions})

	assert.Equal(t, http.StatusServiceUnavailable, status)
	assert.Contains(t, body, "may be behind")
	assert.NotEqual(t, description, unittest.AssertExistsAndLoadBean(t, &repo_model.Repository{ID: 1}).Description)
	unittest.AssertExistsAndLoadBean(t, &actions_model.ActionSchedule{ID: kept.ID})
}

// A node of the cluster that serves the repository at the cluster's
// commit index enables the Actions unit and lifts the archiving, having
// asked once for each; an edit that registers no scheduled workflow, the
// archiving and the Actions unit disabled included, does not ask.
func TestEditActionsAndUnarchiveOnAFreshNode(t *testing.T) {
	unittest.PrepareTestEnv(t)
	asked := 0
	defer test.MockVariableValue(&clusterFresh, func(_ context.Context, key string) error {
		assert.Equal(t, "user2/repo1.git", key)
		asked++
		return nil
	})()
	yes, no := true, false

	description := "nothing read"
	status, _ := editRepo1(t, api.EditRepoOption{Description: &description})
	assert.Equal(t, http.StatusOK, status)
	assert.Zero(t, asked)

	status, _ = editRepo1(t, api.EditRepoOption{HasActions: &yes})
	assert.Equal(t, http.StatusOK, status)
	assert.Equal(t, 1, asked)
	unittest.AssertExistsAndLoadBean(t, &repo_model.RepoUnit{RepoID: 1, Type: unit_model.TypeActions})

	status, _ = editRepo1(t, api.EditRepoOption{Archived: &yes})
	assert.Equal(t, http.StatusOK, status)
	assert.Equal(t, 1, asked, "archiving reads no git")
	assert.True(t, unittest.AssertExistsAndLoadBean(t, &repo_model.Repository{ID: 1}).IsArchived)

	status, _ = editRepo1(t, api.EditRepoOption{Archived: &no})
	assert.Equal(t, http.StatusOK, status)
	assert.Equal(t, 2, asked)
	assert.False(t, unittest.AssertExistsAndLoadBean(t, &repo_model.Repository{ID: 1}).IsArchived)

	status, _ = editRepo1(t, api.EditRepoOption{HasActions: &no})
	assert.Equal(t, http.StatusOK, status)
	assert.Equal(t, 2, asked, "disabling Actions reads no git")
	unittest.AssertNotExistsBean(t, &repo_model.RepoUnit{RepoID: 1, Type: unit_model.TypeActions})
}
