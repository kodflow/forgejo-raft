// Copyright 2026 The Forgejo Authors. All rights reserved.
// SPDX-License-Identifier: GPL-3.0-or-later

package repo

import (
	gocontext "context"
	"net/http"
	"testing"

	issues_model "forgejo.org/models/issues"
	"forgejo.org/models/unittest"
	"forgejo.org/modules/clusterhook"
	"forgejo.org/services/contexttest"

	"github.com/stretchr/testify/assert"
)

// updateTarget posts, as the pull request page does, a new target branch
// for pull request 2 (branch2 into master, index 3 of repo1).
func updateTarget(t *testing.T, target string) (int, string) {
	t.Helper()
	ctx, resp := contexttest.MockContext(t, "POST user2/repo1/pulls/3/target_branch?target_branch="+target)
	contexttest.LoadUser(t, ctx, 2)
	ctx.IsSigned = true
	contexttest.LoadRepo(t, ctx, 1)
	ctx.SetParams(":index", "3")

	UpdatePullRequestTarget(ctx)

	return resp.Code, resp.Body.String()
}

// A change of the target branch posted to a node of the cluster behind on
// the pull request's repositories is refused with 503 and the reason: the
// patch tested on the node's git would be that of the commit before a push
// another node acknowledged, and the merge checks trust what it writes
// (#300). Nothing is written, the user retries.
func TestUpdatePullRequestTargetRefusedOnANodeBehind(t *testing.T) {
	unittest.PrepareTestEnv(t)
	clusterhook.SetFreshWaiter(func(gocontext.Context, string) bool { return false })
	t.Cleanup(func() { clusterhook.SetFreshWaiter(nil) })

	code, body := updateTarget(t, "develop")

	assert.Equal(t, http.StatusServiceUnavailable, code, body)
	assert.Contains(t, body, "may be behind")
	assert.Equal(t, "master", unittest.AssertExistsAndLoadBean(t, &issues_model.PullRequest{ID: 2}).BaseBranch, "the target is unchanged")
}

// On a node of the cluster that serves the repositories, the target
// changes as before.
func TestUpdatePullRequestTargetOnAFreshNode(t *testing.T) {
	unittest.PrepareTestEnv(t)
	clusterhook.SetFreshWaiter(func(gocontext.Context, string) bool { return true })
	t.Cleanup(func() { clusterhook.SetFreshWaiter(nil) })

	code, body := updateTarget(t, "develop")

	assert.Equal(t, http.StatusOK, code, body)
	assert.Equal(t, "develop", unittest.AssertExistsAndLoadBean(t, &issues_model.PullRequest{ID: 2}).BaseBranch)
}
