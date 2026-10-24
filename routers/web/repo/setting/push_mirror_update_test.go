// Copyright 2026 The Forgejo Authors. All rights reserved.
// SPDX-License-Identifier: GPL-3.0-or-later

package setting

import (
	"net/http"
	"strconv"
	"testing"
	"time"

	"forgejo.org/models/db"
	repo_model "forgejo.org/models/repo"
	"forgejo.org/models/unittest"
	"forgejo.org/modules/web"
	"forgejo.org/services/contexttest"
	"forgejo.org/services/forms"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// The interval of a push mirror was written on its own, before the branch
// filter's transaction: a filter the git config refused gave a 500 with
// the new interval kept and the old filter, half an update (#201). The two
// are one update now, kept or refused together.
func TestPushMirrorUpdateIsKeptOrRefusedWhole(t *testing.T) {
	unittest.PrepareTestEnv(t)
	ctx, _ := contexttest.MockContext(t, "user2/repo1/settings?action=push-mirror-update")
	contexttest.LoadUser(t, ctx, 2)
	contexttest.LoadRepo(t, ctx, 1)

	// A remote the repository's git config does not hold: outside a cluster
	// nothing recreates it, and the filter cannot reach it.
	pm := &repo_model.PushMirror{
		RepoID:        ctx.Repo.Repository.ID,
		RemoteName:    "remote_mirror_absent",
		RemoteAddress: "https://example.com/user2/repo1.git",
		Interval:      8 * time.Hour,
	}
	require.NoError(t, db.Insert(db.DefaultContext, pm))

	web.SetForm(ctx, &forms.RepoSettingForm{
		PushMirrorID:           strconv.FormatInt(pm.ID, 10),
		PushMirrorInterval:     "10h",
		PushMirrorBranchFilter: "master",
	})
	SettingsPost(ctx)
	assert.Equal(t, http.StatusInternalServerError, ctx.Resp.Status())

	stored := unittest.AssertExistsAndLoadBean(t, &repo_model.PushMirror{ID: pm.ID})
	assert.Equal(t, 8*time.Hour, stored.Interval, "the interval was kept without the filter")
	assert.Empty(t, stored.BranchFilter)
}
