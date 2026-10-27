// Copyright 2026 The Forgejo Authors. All rights reserved.
// SPDX-License-Identifier: GPL-3.0-or-later

package cluster

import (
	"strings"
	"testing"
	"time"

	"forgejo.org/models/db"
	git_model "forgejo.org/models/git"
	repo_model "forgejo.org/models/repo"
	"forgejo.org/models/unittest"
	user_model "forgejo.org/models/user"
	"forgejo.org/modules/clusterhook"
	repo_service "forgejo.org/services/repository"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// The base user27/template1 (44, public) and a private repository that
// extends its history, user27/payroll (900): its master, p1, is a child of
// t1, the base's first master, which a fork of the base holds. The base
// then moves to t2, a child of t1 too. A sync of the fork through node-a
// pushed the base's branch from node-a's directory under the base's name
// right into the fork: the base renamed away and the private repository
// renamed under its name, projected on node-a, put p1, a fast-forward of
// the fork's master, into the fork, public, on every member. And on a node
// behind a push of the base, it pushed the branch from before the push
// (#283, as the fork itself). The sync, and the answer of whether the fork
// may be synced, now read the base's directory between two checks that it
// holds the base, the sync once node-a serves the base, into a staging
// copy the push goes from: they are refused before anything is pushed.
func TestSyncForkFromALaggingNodeNeverPushesAnotherRepository(t *testing.T) {
	require.NoError(t, unittest.PrepareTestDatabase())
	tc := newTestCluster(t, "node-a", "node-b", "node-c")
	for _, n := range tc.nodes {
		n.r.objects.repoKnown = n.r.repoKnown
	}
	raisePayload(t, tc)
	a, b := tc.nodes[0], tc.nodes[1]
	zero := strings.Repeat("0", 40)
	const payroll, tmp = "user27/payroll.git", "user27/template-tmp.git"
	ctx := db.DefaultContext

	pub := t.TempDir()
	run(t, pub, "init", "-q")
	t1 := commit(t, pub, "public content")
	priv := t.TempDir()
	run(t, priv, "init", "-q")
	run(t, priv, "pull", "-q", pub, "main")
	p1 := commit(t, priv, "PRIVATE: payroll of the whole company")
	require.True(t, tplGenPushVia(t, b, priv, tplGenTemplate, "refs/heads/master", zero, p1, 900).OK)
	waitAll(t, tc, func(n *testNode) bool { return gitRef(t, n, tplGenTemplate, "refs/heads/master") == p1 })
	settle(t, a)
	renameOn(t, b, tplGenTemplate, payroll)
	tc.createVia(b, tplGenTemplate)
	resp := tplGenPushVia(t, b, pub, tplGenTemplate, "refs/heads/master", zero, t1, tplGenTemplateID)
	require.True(t, resp.OK, resp.Message)
	waitApplied(t, b, a)
	require.Eventually(t, func() bool { return gitRef(t, a, tplGenTemplate, "refs/heads/master") == t1 }, 15*time.Second, 20*time.Millisecond, "node-a serves the base")
	settle(t, a)

	tplGenServe(t, a)
	owner := unittest.AssertExistsAndLoadBean(t, &user_model.User{ID: 2})
	created, err := forkFor(t, forkBase(t, tplGenTemplateID), owner.ID, "synced-fork")
	require.NoError(t, err)
	forkKey := clusterhook.RepoKey(created.OwnerName, created.Name, false)
	waitAll(t, tc, func(n *testNode) bool { return gitRef(t, n, forkKey, "refs/heads/master") == t1 })

	// The base moves to t2 through node-b, and its branch row with it, as
	// the cluster's consumer derives it.
	t2 := commit(t, pub, "public content, v2")
	require.True(t, tplGenPushVia(t, b, pub, tplGenTemplate, "refs/heads/master", t1, t2, tplGenTemplateID).OK)
	waitApplied(t, b, a)
	require.Eventually(t, func() bool { return gitRef(t, a, tplGenTemplate, "refs/heads/master") == t2 }, 15*time.Second, 20*time.Millisecond, "node-a serves t2")
	settle(t, a)
	require.NoError(t, db.Insert(ctx, &git_model.Branch{RepoID: tplGenTemplateID, Name: "master", CommitID: t2, CommitMessage: "public content, v2"}))

	fork := func(t *testing.T) *repo_model.Repository {
		t.Helper()
		return unittest.AssertExistsAndLoadBean(t, &repo_model.Repository{ID: created.ID})
	}
	// forkAt checks that every member's table and git hold sha as the
	// fork's master.
	forkAt := func(t *testing.T, sha, msg string) {
		t.Helper()
		for _, n := range tc.nodes {
			waitApplied(t, a, n)
			v, ok := n.r.table.Get(forkKey, "refs/heads/master")
			require.True(t, ok && v.Live(), n.slot)
			assert.Equal(t, sha, v.SHA, "%s: %s", n.slot, msg)
		}
	}
	moves := func(t *testing.T, key, sha string, pairs ...[2]string) {
		t.Helper()
		for _, p := range pairs {
			renameOn(t, b, p[0], p[1])
		}
		waitApplied(t, b, a)
		require.Eventually(t, func() bool { return gitRef(t, a, key, "refs/heads/master") == sha }, 15*time.Second, 20*time.Millisecond, "node-a projects the renames")
		settle(t, a)
	}

	info, err := repo_service.GetSyncForkInfo(ctx, fork(t), "master")
	require.NoError(t, err)
	require.True(t, info.Allowed, "node-a serves the base: the fork may be synced")
	assert.Equal(t, 1, info.CommitsBehind)

	// The base renamed away and the private repository renamed under its
	// name, both projected on node-a.
	t.Run("node-a refuses to read the repository a rename put under the base's name", func(t *testing.T) {
		moves(t, tplGenTemplate, p1, [2]string{tplGenTemplate, tmp}, [2]string{payroll, tplGenTemplate})
		defer moves(t, tplGenTemplate, t2, [2]string{tplGenTemplate, payroll}, [2]string{tmp, tplGenTemplate})

		info, err := repo_service.GetSyncForkInfo(ctx, fork(t), "master")
		if err == nil {
			t.Logf("node-a answers allowed=%v behind=%d", info.Allowed, info.CommitsBehind)
		}
		require.ErrorIs(t, err, clusterhook.ErrBehind, "the answer reads another repository's directory")

		err = repo_service.SyncFork(ctx, owner, fork(t), "master")
		for _, n := range tc.nodes {
			waitApplied(t, a, n)
			v, _ := n.r.table.Get(forkKey, "refs/heads/master")
			t.Logf("%s: fork master=%.10s (base t1=%.10s, private p1=%.10s)", n.slot, v.SHA, t1, p1)
		}
		require.ErrorIs(t, err, clusterhook.ErrBehind)
		forkAt(t, t1, "the public fork holds the private repository's history")
	})

	// The base moves to t3 through node-b, acknowledged; node-a applied
	// it, its git still shows t2.
	release := holdOn(t, a, tplGenTemplate)
	t3 := commit(t, pub, "public content, v3")
	require.True(t, tplGenPushVia(t, b, pub, tplGenTemplate, "refs/heads/master", t2, t3, tplGenTemplateID).OK)
	waitApplied(t, b, a)
	require.Equal(t, t2, gitRef(t, a, tplGenTemplate, "refs/heads/master"), "node-a has not projected the push")

	t.Run("node-a waits to serve the base", func(t *testing.T) {
		err := repo_service.SyncFork(ctx, owner, fork(t), "master")
		require.ErrorIs(t, err, clusterhook.ErrBehind)
		forkAt(t, t1, "nothing was pushed")
	})

	release()
	require.Eventually(t, func() bool { return gitRef(t, a, tplGenTemplate, "refs/heads/master") == t3 }, 15*time.Second, 20*time.Millisecond, "node-a catches up")
	settle(t, a)
	require.NoError(t, repo_service.SyncFork(ctx, owner, fork(t), "master"))
	forkAt(t, t3, "the fork holds the push acknowledged before the sync")
	waitAll(t, tc, func(n *testNode) bool { return gitRef(t, n, forkKey, "refs/heads/master") == t3 })
	for _, n := range tc.nodes {
		assert.Equal(t, p1, gitRef(t, n, payroll, "refs/heads/master"), "%s: the private repository is kept under its name", n.slot)
	}
}
