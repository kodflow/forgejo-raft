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
	user_model "forgejo.org/models/user"
	"forgejo.org/modules/git"
	repo_service "forgejo.org/services/repository"
	wiki_service "forgejo.org/services/wiki"

	"github.com/stretchr/testify/require"
)

func deleteWikiSetup(t *testing.T, name string) (*testCluster, *testNode, *repo_model.Repository, string, string, string) {
	t.Helper()
	require.NoError(t, unittest.PrepareTestDatabase())
	tc := newTestCluster(t, "node-a", "node-b", "node-c")
	origin := tc.nodes[0]
	withGitThroughReplicator(t, origin)
	ctx := db.DefaultContext
	owner := unittest.AssertExistsAndLoadBean(t, &user_model.User{ID: 4})
	repo, err := repo_service.CreateRepositoryDirectly(ctx, owner, owner, repo_service.CreateRepoOptions{Name: name})
	require.NoError(t, err)
	require.NoError(t, wiki_service.AddWikiPage(ctx, owner, repo, "Home", "secret wiki content", "add"))
	wiki := "user4/" + name + ".wiki.git"
	branch := "refs/heads/" + repo.GetWikiBranchName()
	for _, n := range tc.nodes {
		require.Eventually(t, func() bool {
			v, ok := n.r.table.Get(wiki, branch)
			return ok && v.Live() && gitRef(t, n, wiki, branch) == v.SHA
		}, 15*time.Second, 20*time.Millisecond, "%s holds the wiki", n.slot)
	}
	v, _ := origin.r.table.Get(wiki, branch)
	return tc, origin, repo, wiki, branch, v.SHA
}

func logWikiState(t *testing.T, tc *testCluster, wiki, branch, when string) {
	for _, n := range tc.nodes {
		v, ok := n.r.table.Get(wiki, branch)
		_, deleted := n.r.table.Deleted(wiki)
		t.Logf("%s %s: table live=%v sha=%s deleted=%v dir=%v git=%q", when, n.slot, ok && v.Live(), v.SHA, deleted, dirExists(n, wiki), gitRef(t, n, wiki, branch))
	}
}

// Settings > Danger zone > "Delete wiki data" in a three-member cluster: it
// removed the origin's files alone, the wiki's key stayed live, the other
// members kept it and the origin's projector brought it back (#53).
func TestDeleteWikiDataDeletesTheWikiOnEveryMember(t *testing.T) {
	tc, origin, repo, wiki, branch, oldSHA := deleteWikiSetup(t, "delwiki")
	ctx := db.DefaultContext

	require.NoError(t, wiki_service.DeleteWiki(ctx, repo))
	// Several projector ticks (2 s) with no other trigger.
	time.Sleep(5 * time.Second)
	logWikiState(t, tc, wiki, branch, "after DeleteWiki")
	for _, n := range tc.nodes {
		_, deleted := n.r.table.Deleted(wiki)
		if !deleted {
			t.Errorf("%s: the wiki key %s is not deleted in the replicated table", n.slot, wiki)
		}
		if dirExists(n, wiki) {
			t.Errorf("%s: the wiki directory still exists after Delete wiki data", n.slot)
		}
	}

	// A restart of the origin: startupBarrier calls unfence (replicator.go:364),
	// which schedules every key of the table.
	origin.r.projector.unfence()
	time.Sleep(5 * time.Second)
	logWikiState(t, tc, wiki, branch, "after origin restart (unfence)")
	if dirExists(origin, wiki) {
		got := gitRef(t, origin, wiki, branch)
		out, _, err := git.NewCommand(ctx, "show").AddDynamicArguments(oldSHA + ":Home.md").RunStdString(&git.RunOpts{Dir: filepath.Join(origin.r.root, filepath.FromSlash(wiki))})
		t.Logf("origin wiki recreated: %s=%s, Home.md=%q err=%v", branch, got, strings.TrimSpace(out), err)
		t.Errorf("node-a: the deleted wiki came back on the origin after a restart (git %s = %s, old %s)", branch, got, oldSHA)
	}
}

// Delete wiki data, then create the wiki again, as Forgejo allows: the new
// wiki starts from nothing (#53).
func TestAWikiCreatedAgainAfterDeleteWikiData(t *testing.T) {
	tc, _, repo, wiki, branch, oldSHA := deleteWikiSetup(t, "delwiki2")
	ctx := db.DefaultContext
	owner := unittest.AssertExistsAndLoadBean(t, &user_model.User{ID: 4})

	require.NoError(t, wiki_service.DeleteWiki(ctx, repo))
	time.Sleep(time.Second)
	err := wiki_service.AddWikiPage(ctx, owner, repo, "Home", "a brand new wiki", "new")
	t.Logf("AddWikiPage after DeleteWiki: err=%v", err)
	time.Sleep(5 * time.Second)
	logWikiState(t, tc, wiki, branch, "after re-creation")
	if err != nil {
		t.Errorf("creating the wiki again after Delete wiki data fails: %v", err)
	}
	for _, n := range tc.nodes {
		v, ok := n.r.table.Get(wiki, branch)
		if ok && v.Live() {
			anc := git.NewCommand(ctx, "merge-base", "--is-ancestor").AddDynamicArguments(oldSHA, v.SHA)
			_, _, aerr := anc.RunStdString(&git.RunOpts{Dir: filepath.Join(n.r.root, filepath.FromSlash(wiki))})
			if v.SHA == oldSHA || aerr == nil {
				t.Errorf("%s: the table's wiki %s = %s still holds the deleted history (old %s)", n.slot, branch, v.SHA, oldSHA)
			}
		}
	}
}
