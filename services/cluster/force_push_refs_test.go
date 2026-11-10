// Copyright 2026 The Forgejo Authors. All rights reserved.
// SPDX-License-Identifier: GPL-3.0-or-later

package cluster

import (
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"forgejo.org/models/db"
	issues_model "forgejo.org/models/issues"
	"forgejo.org/models/unittest"
	user_model "forgejo.org/models/user"
	"forgejo.org/modules/clusterhook"
	"forgejo.org/modules/gitrepo"
	"forgejo.org/modules/setting"
	"forgejo.org/modules/test"
	issue_service "forgejo.org/services/issue"
	pull_service "forgejo.org/services/pull"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// A force push to an open pull request keeps the replaced and the new head
// under refs/pull/N/comment/M/{old,new}, written by Forgejo itself with git
// update-ref (forgejo#15162). In a cluster they go through the replicator
// as every ref does: every node's table and git hold them. Deleting the
// pull request lists refs/pull/N/ from the git of the node that serves it,
// then deletes each ref: on a node behind, a force-push ref another node
// wrote would be missed and kept for good. The deletion waits for the node
// to serve the repository, is refused with nothing deleted while it does
// not, and deletes every ref of the pull request once it does.
func TestForcePushRefsOfAPullRequestAreReplicatedAndDeleted(t *testing.T) {
	defer test.MockVariableValue(&setting.Repository.PullRequest.KeepForcePushRefs, true)()
	tc, a, b, work, c1, c2, hold := laggingCluster(t)
	a.r.registry.release(hold)
	withGitThroughReplicator(t, a)
	freshFrom(t, a)
	ctx := db.DefaultContext
	doer := unittest.AssertExistsAndLoadBean(t, &user_model.User{ID: 2})
	pr := unittest.AssertExistsAndLoadBean(t, &issues_model.PullRequest{ID: 2})
	require.NoError(t, pr.LoadBaseRepo(ctx))
	require.Equal(t, int64(1), pr.BaseRepoID)

	// main moved from c2 back to c1: c2 is not an ancestor, a force push.
	comment, err := pull_service.CreatePushPullComment(ctx, doer, pr, c2, c1)
	require.NoError(t, err)
	require.NotNil(t, comment)
	oldRef, newRef := comment.ForcePushOldCommitGitRefName(), comment.ForcePushNewCommitGitRefName()
	require.Equal(t, fmt.Sprintf("refs/pull/%d/comment/%d/old", pr.Index, comment.ID), oldRef)
	for _, m := range tc.nodes {
		require.Eventually(t, func() bool {
			return gitRef(t, m, laggingRepo, oldRef) == c2 && gitRef(t, m, laggingRepo, newRef) == c1
		}, 15*time.Second, 20*time.Millisecond, "%s: the force-push refs are replicated", m.slot)
	}
	tableAt(t, tc, oldRef, c2, "the replaced head")
	tableAt(t, tc, newRef, c1, "the new head")

	// node-a stops projecting the repository, and a force-push ref is
	// written through node-b: node-a's git does not list it.
	lag := clusterhook.NewNonce()
	require.NoError(t, a.r.registry.register(lag, laggingRepo, nil, os.Getpid(), false))
	t.Cleanup(func() { a.r.registry.release(lag) })
	unseen := fmt.Sprintf("refs/pull/%d/comment/%d/new", pr.Index, comment.ID+1000)
	pushOnB(t, tc, a, b, work, unseen, strings.Repeat("0", 40), c2)

	require.NoError(t, pr.LoadIssue(ctx))
	gitRepo, err := gitrepo.OpenRepository(ctx, pr.BaseRepo)
	require.NoError(t, err)
	defer gitRepo.Close()
	err = issue_service.DeleteIssue(ctx, doer, gitRepo, pr.Issue)
	require.ErrorIs(t, err, clusterhook.ErrBehind)
	unittest.AssertExistsAndLoadBean(t, &issues_model.Issue{ID: pr.IssueID})
	tableAt(t, tc, unseen, c2, "a refused deletion deletes nothing")
	tableAt(t, tc, oldRef, c2, "a refused deletion deletes nothing")

	caughtUp(t, a, lag, unseen, c2)
	require.NoError(t, issue_service.DeleteIssue(ctx, doer, gitRepo, pr.Issue))
	unittest.AssertNotExistsBean(t, &issues_model.Issue{ID: pr.IssueID})
	for _, m := range tc.nodes {
		for _, ref := range []string{oldRef, newRef, unseen} {
			require.Eventually(t, func() bool { return gitRef(t, m, laggingRepo, ref) == "" },
				15*time.Second, 20*time.Millisecond, "%s: %s is deleted", m.slot, ref)
			v, ok := m.r.table.Get(laggingRepo, ref)
			assert.False(t, ok && v.Live(), "%s: %s is still live in the table", m.slot, ref)
		}
	}
}
