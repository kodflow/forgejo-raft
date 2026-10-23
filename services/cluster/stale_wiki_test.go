// Copyright 2026 The Forgejo Authors. All rights reserved.
// SPDX-License-Identifier: GPL-3.0-or-later

package cluster

import (
	"path/filepath"
	"strings"
	"testing"
	"time"

	"forgejo.org/models/db"
	repo_model "forgejo.org/models/repo"
	"forgejo.org/models/unittest"
	"forgejo.org/modules/clusterhook"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// A repository's deletion whose wiki entry was lost (a leader change, an
// outcome never settled) left the wiki's key alive beside the deleted
// repository. reconcile examined a wiki alone only when its repository's
// key was absent from the table, never when it was deleted there, and
// `--delete-orphan` refused once SQL knew the name again: a repository
// created again under it was refused on every node, the wiki's stamp
// naming the deleted repository's ID (#147), with nothing to repair it
// (#163).
func TestReconcileOffersTheWikiOfADeletedRepository(t *testing.T) {
	require.NoError(t, unittest.PrepareTestDatabase())
	tc := newTestCluster(t, "node-a", "node-b", "node-c")
	origin := tc.nodes[0]
	ctx := db.DefaultContext
	work := t.TempDir()
	run(t, work, "init", "-q")
	c1 := commit(t, work, "one")
	zero := strings.Repeat("0", 40)

	const lostID = 41
	tc.repoIDsMu.Lock()
	tc.repoIDs = map[string]int64{}
	for _, name := range []string{"wikigone", "wikistale", "wikilost"} {
		tc.repoIDs["user4/"+name+".git"] = lostID
		tc.repoIDs["user4/"+name+".wiki.git"] = lostID
	}
	tc.repoIDsMu.Unlock()
	for _, name := range []string{"wikigone", "wikistale", "wikilost"} {
		for _, key := range []string{"user4/" + name + ".git", "user4/" + name + ".wiki.git"} {
			require.True(t, tc.pushVia(origin, work, key, "refs/heads/main", zero, c1).OK, key)
		}
	}
	for _, name := range []string{"wikigone", "wikistale"} {
		// The repository's entry committed, its wiki's was lost.
		require.NoError(t, origin.r.proposeRepoDelete(ctx, "user4/"+name+".git", lostID))
		requireGoneEverywhere(t, tc, "user4/"+name+".git")
		requireMainEverywhere(t, tc, "user4/"+name+".wiki.git")
	}
	report := func() []Mismatch {
		ms, err := origin.r.Reconcile(ctx, true)
		require.NoError(t, err)
		return ms
	}
	find := func(ms []Mismatch, repo string) (Mismatch, bool) {
		for _, m := range ms {
			if m.Repo == repo && m.Category == RepoLifecycle {
				return m, true
			}
		}
		return Mismatch{}, false
	}
	del := func(repo string, index uint64) *clusterhook.Response {
		return origin.r.Handle(ctx, &clusterhook.Request{Op: clusterhook.OpReconcile, Repo: repo, Index: int64(index), Reason: "wiki entry lost", Operator: "tester"})
	}

	// SQL does not know the name: the wiki is reported with its
	// repository, as an orphan, and deleted by --delete-orphan.
	m, ok := find(report(), "user4/wikigone.git")
	require.True(t, ok, "the wiki of a deleted repository is reported")
	assert.Contains(t, m.Action, "--delete-orphan")
	assert.NotZero(t, m.Index)
	resp := del("user4/wikigone", m.Index)
	require.True(t, resp.OK, resp.Message)
	requireGoneEverywhere(t, tc, "user4/wikigone.wiki.git")

	// The name is taken again: a new row, a new ID, a push under it. Every
	// node refuses the new repository while the old wiki stays.
	stale := &repo_model.Repository{OwnerID: 4, OwnerName: "user4", Name: "wikistale", LowerName: "wikistale"}
	require.NoError(t, db.Insert(ctx, stale))
	require.NotEqual(t, int64(lostID), stale.ID)
	tc.repoIDsMu.Lock()
	tc.repoIDs["user4/wikistale.git"] = stale.ID
	tc.repoIDsMu.Unlock()
	// git init of the new repository, through the hook, then a push.
	dir := filepath.Join(origin.r.root, "user4", "wikistale.git")
	require.Eventually(t, func() bool { return !dirExists(origin, "user4/wikistale.git") }, 10*time.Second, 20*time.Millisecond)
	run(t, origin.r.root, "init", "-q", "--bare", dir)
	initReq := &clusterhook.Request{
		Op: clusterhook.OpPrepared, Nonce: clusterhook.NewNonce(), GitDir: dir, RepoID: stale.ID,
		Lines: []clusterhook.Line{{Old: zero, New: "ref:refs/heads/main", Ref: "HEAD"}},
	}
	resp = origin.r.Handle(ctx, initReq)
	require.True(t, resp.OK, resp.Message)
	initReq.Op = clusterhook.OpCommitted
	origin.r.Handle(ctx, initReq)
	resp = tc.pushVia(origin, work, "user4/wikistale.git", "refs/heads/main", zero, c1)
	require.True(t, resp.OK, resp.Message)
	requireMainEverywhere(t, tc, "user4/wikistale.git")
	for _, n := range tc.nodes {
		require.Eventually(t, func() bool { return readRefusal(n, "user4/wikistale.wiki.git", stale.ID) != nil }, 10*time.Second, 20*time.Millisecond, n.slot)
	}
	_, ok = find(report(), "user4/wikistale.git")
	assert.False(t, ok, "the new repository is no orphan")
	m, ok = find(report(), "user4/wikistale.wiki.git")
	require.True(t, ok, "the old wiki is reported")
	assert.Equal(t, "wiki", m.Kind)
	assert.Contains(t, m.Action, "--delete-orphan <owner>/<repo>.wiki")
	resp = del("user4/wikistale", m.Index)
	require.False(t, resp.OK, "the repository SQL has is never deleted")
	resp = del("user4/wikistale.wiki", m.Index-1)
	require.False(t, resp.OK)
	assert.Contains(t, resp.Message, "changed at index")
	resp = del("user4/wikistale.wiki", m.Index)
	require.True(t, resp.OK, resp.Message)
	requireGoneEverywhere(t, tc, "user4/wikistale.wiki.git")
	requireMainEverywhere(t, tc, "user4/wikistale.git")
	for _, n := range tc.nodes {
		require.Eventually(t, func() bool { return readRefusal(n, "user4/wikistale.wiki.git", stale.ID) == nil }, 10*time.Second, 20*time.Millisecond, n.slot)
	}

	// A repository SQL lost and gave a new row (adoption pending): its
	// wiki keeps the same ID as its repository's key, and is not stale.
	lost := &repo_model.Repository{OwnerID: 4, OwnerName: "user4", Name: "wikilost", LowerName: "wikilost"}
	require.NoError(t, db.Insert(ctx, lost))
	_, ok = find(report(), "user4/wikilost.wiki.git")
	assert.False(t, ok, "the wiki of the repository SQL lost is not stale")
	_, last := origin.r.table.RepoHash("user4/wikilost.wiki.git")
	resp = del("user4/wikilost.wiki", last)
	require.False(t, resp.OK, resp.Message)
	assert.Contains(t, resp.Message, "no orphan")
	requireMainEverywhere(t, tc, "user4/wikilost.wiki.git")
}

// requireMainEverywhere waits until every member holds key's main branch
// alive, projected.
func requireMainEverywhere(t *testing.T, tc *testCluster, key string) {
	t.Helper()
	for _, n := range tc.nodes {
		require.Eventually(t, func() bool {
			v, ok := n.r.table.Get(key, "refs/heads/main")
			return ok && v.Live() && dirExists(n, key)
		}, 10*time.Second, 20*time.Millisecond, "%s: %s projected", n.slot, key)
	}
}
