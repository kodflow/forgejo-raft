// Copyright 2026 The Forgejo Authors. All rights reserved.
// SPDX-License-Identifier: GPL-3.0-or-later

package automerge

import (
	"context"
	"strings"
	"sync/atomic"
	"testing"

	issues_model "forgejo.org/models/issues"
	"forgejo.org/models/unittest"
	user_model "forgejo.org/models/user"
	"forgejo.org/modules/clusterhook"
	"forgejo.org/modules/setting"
	"forgejo.org/modules/test"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// laggingNotifier is mergedNotifier, and a push to the repository another
// node of the cluster acknowledges right after the merge, which this node
// does not serve yet: behind holds from then on.
type laggingNotifier struct {
	mergedNotifier
	behind *atomic.Bool
}

func (n *laggingNotifier) AutoMergePullRequest(ctx context.Context, doer *user_model.User, pr *issues_model.PullRequest) {
	n.mergedNotifier.AutoMergePullRequest(ctx, doer, pr)
	n.behind.Store(true)
}

// childTarget returns the target branch of pull request 5 (pr-to-update
// into branch2), the child of pull request 2 (branch2 into master).
func childTarget(t *testing.T) string {
	t.Helper()
	return unittest.AssertExistsAndLoadBean(t, &issues_model.PullRequest{ID: 5}).BaseBranch
}

// A merge scheduled with the head branch deleted after it first retargets
// the pull requests based on that branch (RetargetChildrenOnMerge). Since
// #300, a node of a cluster behind on a child's repositories refuses the
// child's retarget with ErrBehind, which the errors of the children hid
// from errors.Is: the deletion took the refusal for a failure, logged it
// and dropped it, and nothing ever deleted the branch, which #279 had
// fixed for a node behind. The deletion is now handed back to the queue,
// nothing deleted nor retargeted while the node lags, and done, the child
// retargeted first, once the node serves the repositories.
func TestAutoMergeDeletesItsHeadBranchOnceItsChildIsRetargeted(t *testing.T) {
	defer test.MockVariableValue(&setting.Repository.PullRequest.RetargetChildrenOnMerge, true)()
	var behind atomic.Bool
	item, master := scheduleMergeWith(t, &laggingNotifier{behind: &behind}, true)
	head := strings.TrimPrefix(item, "2_")
	clusterhook.SetFreshWaiter(func(context.Context, string) bool { return !behind.Load() })
	t.Cleanup(func() { clusterhook.SetFreshWaiter(nil) })

	deferred := handled(item)
	merged(t, master)
	assert.Equal(t, head, branch2(t), "nothing is deleted while the child's retarget is refused")
	assert.Equal(t, "branch2", childTarget(t), "the child keeps its target")
	require.Len(t, deferred, 1, "the deletion refused right after the merge is handed back to the queue, not dropped")
	assert.NotEqual(t, item, deferred[0], "the merge is not tried again")

	assert.Equal(t, deferred, handled(deferred...), "still behind: handed back again")
	assert.Equal(t, head, branch2(t), "nothing is deleted while the node lags")
	assert.Equal(t, "branch2", childTarget(t))

	behind.Store(false)
	handled(deferred...)
	assert.Equal(t, "master", childTarget(t), "the child is retargeted")
	assert.Empty(t, branch2(t), "the head branch is deleted once its child is retargeted")
}
