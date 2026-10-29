// Copyright 2026 The Forgejo Authors. All rights reserved.
// SPDX-License-Identifier: GPL-3.0-or-later

package cluster

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"forgejo.org/models/db"
	repo_model "forgejo.org/models/repo"
	"forgejo.org/models/unittest"
	"forgejo.org/modules/clusterhook"
	"forgejo.org/modules/gitrepo"
	repo_module "forgejo.org/modules/repository"
	"forgejo.org/modules/setting"
	"forgejo.org/modules/test"
	"forgejo.org/services/cluster/consensus"
	"forgejo.org/services/cluster/reftable"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// releaseMismatches returns the release and tag-only rows reconcile found
// behind their tag, by tag name.
func releaseMismatches(ms []Mismatch, repo string) map[string]Mismatch {
	out := map[string]Mismatch{}
	for _, m := range ms {
		if m.Repo == repo && m.Category == RefsAheadOfSQL && (m.Kind == "release" || m.Kind == "tag") && m.Action != "" {
			out[m.Ref] = m
		}
	}
	return out
}

// releaseBench is a repository whose bare git and table hold the same
// tags, annotated and lightweight, for reconcile without a running
// cluster.
type releaseBench struct {
	r      *Replicator
	root   string
	c1, c2 string
}

func newReleaseBench(t *testing.T, light map[string]string) *releaseBench {
	t.Helper()
	require.NoError(t, unittest.PrepareTestDatabase())
	root, work := t.TempDir(), t.TempDir()
	run(t, work, "init", "-q")
	b := &releaseBench{root: root}
	b.c1 = commit(t, work, "one")
	b.c2 = commit(t, work, "two")
	var updates []reftable.RefUpdate
	for _, name := range []string{"ann-ok", "ann-old"} {
		run(t, work, "tag", "-a", "-m", "annotated", name, b.c1)
		obj := run(t, work, "rev-parse", "refs/tags/"+name)
		require.NotEqual(t, b.c1, obj, "an annotated tag's value is its tag object")
		updates = append(updates, reftable.RefUpdate{Ref: "refs/tags/" + name, New: obj})
	}
	for name, at := range light {
		sha := b.c1
		if at == "c2" {
			sha = b.c2
		}
		run(t, work, "tag", name, sha)
		updates = append(updates, reftable.RefUpdate{Ref: "refs/tags/" + name, New: sha})
	}
	run(t, root, "clone", "-q", "--bare", work, filepath.Join(root, "user2", "repo1.git"))

	b.r = newReplicator(root, newRegistry(0, 1, 1))
	data, err := reftable.Encode(reftable.KindRefTxn, &reftable.RefTxn{Repo: laggingRepo, RepoID: 1, Format: "sha1", Updates: updates})
	require.NoError(t, err)
	res := b.r.table.Apply(consensus.Entry{Term: 1, Index: 1, Data: data}).(reftable.Result)
	require.True(t, res.OK, res.Reason)

	_, err = db.GetEngine(db.DefaultContext).Where("repo_id = ?", 1).Delete(new(repo_model.Release))
	require.NoError(t, err)
	return b
}

// insert writes release rows of repository 1, by tag name.
func (b *releaseBench) insert(t *testing.T, rels ...*repo_model.Release) map[string]int64 {
	t.Helper()
	ids := map[string]int64{}
	for _, rel := range rels {
		rel.RepoID, rel.PublisherID, rel.LowerTagName, rel.Title = 1, 2, strings.ToLower(rel.TagName), rel.TagName
		require.NoError(t, db.Insert(db.DefaultContext, rel))
		ids[rel.TagName] = rel.ID
	}
	return ids
}

// A synchronisation of the tags from a git behind the cluster leaves a
// release a draft, its commit cleared, while the table holds its tag, or at
// another commit than its tag's (#341). Reconcile reports both. The second
// is regenerable. The first is not: a draft its author saved on an
// existing tag has no commit either (createTag sets the commit of a
// published release only), and no column tells the two apart. A release at
// its annotated tag's commit, the tag's value in the table being the tag
// object, is in place, and so are a published release its author turned
// draft, which keeps its commit, and a draft whose tag is gone.
func TestReconcileReportsReleasesBehindTheirTag(t *testing.T) {
	b := newReleaseBench(t, map[string]string{
		"light-ok": "c2", "light-old": "c2", "tag-old": "c2",
		"drafted": "c1", "saved-draft": "c1", "authored-draft": "c1",
	})
	b.insert(t,
		&repo_model.Release{TagName: "ann-ok", Sha1: b.c1},
		&repo_model.Release{TagName: "ann-old", Sha1: b.c2},
		&repo_model.Release{TagName: "light-ok", Sha1: b.c2},
		&repo_model.Release{TagName: "light-old", Sha1: b.c1},
		&repo_model.Release{TagName: "tag-old", Sha1: b.c1, IsTag: true},
		// As PushUpdateDeleteTag leaves a release.
		&repo_model.Release{TagName: "drafted", IsDraft: true},
		// As CreateRelease saves a draft on an existing tag.
		&repo_model.Release{TagName: "saved-draft", IsDraft: true},
		// As its author leaves a published release turned draft.
		&repo_model.Release{TagName: "authored-draft", Sha1: b.c1, IsDraft: true},
		// A tag deleted for good leaves its release so.
		&repo_model.Release{TagName: "deleted", IsDraft: true},
	)

	ms, err := b.r.Reconcile(db.DefaultContext, false)
	require.NoError(t, err)
	got := releaseMismatches(ms, laggingRepo)
	want := map[string]Mismatch{
		"ann-old":     {Repo: laggingRepo, Category: RefsAheadOfSQL, Kind: "release", Ref: "ann-old", Regenerable: true, Action: ActionReleaseOldCommit},
		"light-old":   {Repo: laggingRepo, Category: RefsAheadOfSQL, Kind: "release", Ref: "light-old", Regenerable: true, Action: ActionReleaseOldCommit},
		"tag-old":     {Repo: laggingRepo, Category: RefsAheadOfSQL, Kind: "tag", Ref: "tag-old", Regenerable: true, Action: ActionReleaseOldCommit},
		"drafted":     {Repo: laggingRepo, Category: RefsAheadOfSQL, Kind: "release", Ref: "drafted", Action: ActionReleaseDraftTagHeld},
		"saved-draft": {Repo: laggingRepo, Category: RefsAheadOfSQL, Kind: "release", Ref: "saved-draft", Action: ActionReleaseDraftTagHeld},
	}
	assert.Equal(t, want, got)
	for _, m := range ms {
		assert.False(t, m.Applied, "a dry run changes nothing: %+v", m)
	}
}

// With apply, reconcile writes again the rows it rebuilds, and only those:
// a release at another commit than its tag takes the tag's commit, a tag
// the table holds without a row gets one, a tag-only row whose tag is gone
// goes. It no longer runs SyncReleasesWithTags for the tag-only rows, which
// published every draft whose tag git holds, a draft its author saved
// included, and turned into a draft a release whose tag is gone, a row
// reconcile reports and never changes (#341). A row changed since
// reconcile read it, a release its author turned draft meanwhile, is not
// written.
func TestReconcileApplyRebuildsOnlyTheRowsBehindTheirTag(t *testing.T) {
	b := newReleaseBench(t, map[string]string{
		"light-old": "c2", "tag-old": "c2", "saved-draft": "c1", "plain": "c2", "turned": "c2",
	})
	ctx := db.DefaultContext
	defer test.MockVariableValue(&setting.RepoRootPath, b.root)()
	ids := b.insert(t,
		&repo_model.Release{TagName: "ann-ok", Sha1: b.c1},
		&repo_model.Release{TagName: "ann-old", Sha1: b.c2},
		&repo_model.Release{TagName: "light-old", Sha1: b.c1},
		&repo_model.Release{TagName: "tag-old", Sha1: b.c1, IsTag: true},
		&repo_model.Release{TagName: "saved-draft", IsDraft: true},
		&repo_model.Release{TagName: "gone-release", Sha1: b.c1},
		&repo_model.Release{TagName: "gone-tag", Sha1: b.c1, IsTag: true},
		&repo_model.Release{TagName: "turned", Sha1: b.c1},
	)
	repo, err := repo_model.GetRepositoryByID(ctx, 1)
	require.NoError(t, err)
	refs, _ := b.r.table.Refs(laggingRepo)

	// "turned" read at c1, then turned draft by its author: not written.
	asRead := unittest.AssertExistsAndLoadBean(t, &repo_model.Release{ID: ids["turned"]})
	_, err = db.GetEngine(ctx).ID(asRead.ID).Cols("is_draft").Update(&repo_model.Release{IsDraft: true})
	require.NoError(t, err)
	stale := []Mismatch{{Repo: laggingRepo, Category: RefsAheadOfSQL, Kind: "release", Ref: "turned", Regenerable: true, Action: ActionReleaseOldCommit}}
	b.r.rebuildReleases(ctx, laggingRepo, repo, stale, []*repo_model.Release{asRead})
	assert.False(t, stale[0].Applied, "a row changed since it was read is not written")
	turned := unittest.AssertExistsAndLoadBean(t, &repo_model.Release{ID: asRead.ID})
	assert.True(t, turned.IsDraft, "the draft its author made stays a draft")
	assert.Equal(t, b.c1, turned.Sha1)

	ms, err := b.r.reconcileNonRegenerable(ctx, laggingRepo, repo, refs, true)
	require.NoError(t, err)
	byRef := map[string]Mismatch{}
	for _, m := range ms {
		if m.Kind == "release" || m.Kind == "tag" {
			byRef[m.Ref] = m
		}
	}
	for _, ref := range []string{"ann-old", "light-old", "tag-old", "plain", "gone-tag"} {
		assert.True(t, byRef[ref].Regenerable && byRef[ref].Applied, "%s is rebuilt: %+v", ref, byRef[ref])
	}
	assert.Equal(t, ActionReleaseDraftTagHeld, byRef["saved-draft"].Action)
	assert.Equal(t, ActionReleaseNoTag, byRef["gone-release"].Action)
	for _, ref := range []string{"saved-draft", "gone-release"} {
		assert.False(t, byRef[ref].Regenerable || byRef[ref].Applied, "%s is reported, never changed: %+v", ref, byRef[ref])
	}
	assert.NotContains(t, byRef, "turned", "a release its author turned draft keeps its commit")

	rows := releasesOf(t, 1)
	assert.Equal(t, b.c1, rows["ann-ok"].Sha1)
	assert.Equal(t, b.c1, rows["ann-old"].Sha1, "a release on an annotated tag takes the commit the tag object names")
	assert.Equal(t, b.c2, rows["light-old"].Sha1)
	assert.Equal(t, b.c2, rows["tag-old"].Sha1)
	assert.True(t, rows["tag-old"].IsTag)
	assert.True(t, rows["plain"].IsTag, "a tag the table holds without a row gets one")
	assert.Equal(t, b.c2, rows["plain"].Sha1)
	assert.NotContains(t, rows, "gone-tag", "a tag-only row whose tag is gone goes")
	assert.True(t, rows["saved-draft"].IsDraft, "a draft its author saved on an existing tag is not published")
	assert.Empty(t, rows["saved-draft"].Sha1)
	assert.False(t, rows["gone-release"].IsDraft, "a release whose tag is gone is reported, not turned into a draft")
	assert.Equal(t, b.c1, rows["gone-release"].Sha1)
	assert.True(t, rows["turned"].IsDraft)
	for _, name := range []string{"ann-old", "light-old", "saved-draft", "gone-release", "turned"} {
		assert.Equal(t, name, rows[name].Title, "the release itself is kept")
	}
}

// releasesOf returns the release rows of a repository, by tag name.
func releasesOf(t *testing.T, repoID int64) map[string]repo_model.Release {
	t.Helper()
	var rels []*repo_model.Release
	require.NoError(t, db.GetEngine(db.DefaultContext).Where("repo_id = ?", repoID).Find(&rels))
	out := make(map[string]repo_model.Release, len(rels))
	for _, rel := range rels {
		out[rel.TagName] = *rel
	}
	return out
}

// `forgejo admin repo-sync-releases` ran SyncReleasesWithTags on the node it
// was started on, from that node's own git, in a process without the
// replicator, hence without the wait for freshness the site
// administration's "Synchronise tags" has (#191). On node-a, which has not
// projected a tag pushed through node-b and a tag moved through node-b,
// the published release of the first became a draft and the release of
// the second went back to the tag's old commit. Once node-a caught up,
// neither the consumer, its work done, nor `forgejo cluster reconcile
// --apply` brought them back or reported them (#341). The command now
// refuses to run on a node of a cluster (cmd). On rows a run before this
// fix left, reconcile reports both; with apply it writes the moved tag's
// release again from the tag, and leaves the draft as it is, reported: a
// draft its author saved on an existing tag reads exactly the same. Apply
// rebuilds a lost tag-only row beside them without publishing that draft.
func TestReconcileAfterATagSyncFromALaggingNode(t *testing.T) {
	tc, a, b, work, c1, c2, hold := laggingCluster(t)
	ctx := db.DefaultContext
	zero := strings.Repeat("0", 40)
	// Only this test's release rows are judged.
	_, err := db.GetEngine(ctx).Where("repo_id = ?", 1).Delete(new(repo_model.Release))
	require.NoError(t, err)

	// lag-mv, lag-plain, lag-saved and the annotated lag-ann at c1 on every
	// node first.
	run(t, work, "tag", "-a", "-m", "annotated", "lag-ann", c1)
	ann := run(t, work, "rev-parse", "refs/tags/lag-ann")
	a.r.registry.release(hold)
	first := map[string]string{"refs/tags/lag-mv": c1, "refs/tags/lag-ann": ann, "refs/tags/lag-plain": c1, "refs/tags/lag-saved": c1}
	for ref, sha := range first {
		require.True(t, tc.pushVia(b, work, laggingRepo, ref, zero, sha).OK, ref)
	}
	for _, m := range tc.nodes {
		require.Eventually(t, func() bool {
			for ref, sha := range first {
				if gitRef(t, m, laggingRepo, ref) != sha {
					return false
				}
			}
			return true
		}, 15*time.Second, 20*time.Millisecond, "%s: the first tags at c1", m.slot)
	}
	hold = clusterhook.NewNonce()
	require.NoError(t, a.r.registry.register(hold, laggingRepo, nil, os.Getpid(), false))

	// Acknowledged through node-b, not projected on node-a yet.
	pushOnB(t, tc, a, b, work, "refs/tags/lag-cli", zero, c1)
	pushOnB(t, tc, a, b, work, "refs/tags/lag-mv", c1, c2)
	// The rows the consumer and the release service wrote for them.
	cli := &repo_model.Release{RepoID: 1, PublisherID: 2, TagName: "lag-cli", LowerTagName: "lag-cli", Sha1: c1, Title: "cli", NumCommits: 1}
	mv := &repo_model.Release{RepoID: 1, PublisherID: 2, TagName: "lag-mv", LowerTagName: "lag-mv", Sha1: c2, Title: "mv", NumCommits: 2}
	annRel := &repo_model.Release{RepoID: 1, PublisherID: 2, TagName: "lag-ann", LowerTagName: "lag-ann", Sha1: c1, Title: "ann", NumCommits: 1}
	require.NoError(t, db.Insert(ctx, cli))
	require.NoError(t, db.Insert(ctx, mv))
	require.NoError(t, db.Insert(ctx, annRel))

	defer test.MockVariableValue(&setting.RepoRootPath, a.r.root)()
	// The site administration's path refuses on node-a (#191).
	freshFrom(t, a)
	require.ErrorIs(t, repo_module.SyncRepoTags(ctx, 1), clusterhook.ErrBehind)

	// What repo-sync-releases did for each repository before it refused
	// to run in a cluster, in a process where no fresh waiter is
	// registered.
	clusterhook.SetFreshWaiter(nil)
	repo, err := repo_model.GetRepositoryByID(ctx, 1)
	require.NoError(t, err)
	gitRepo, err := gitrepo.OpenRepository(ctx, repo)
	require.NoError(t, err)
	require.NoError(t, repo_module.SyncReleasesWithTags(ctx, repo, gitRepo))
	gitRepo.Close()
	gotCli := unittest.AssertExistsAndLoadBean(t, &repo_model.Release{ID: cli.ID})
	gotMv := unittest.AssertExistsAndLoadBean(t, &repo_model.Release{ID: mv.ID})
	require.True(t, gotCli.IsDraft, "the stale synchronisation took lag-cli for deleted")
	require.Empty(t, gotCli.Sha1)
	require.Equal(t, c1, gotMv.Sha1, "the stale synchronisation wrote lag-mv's old commit")
	gotAnn := unittest.AssertExistsAndLoadBean(t, &repo_model.Release{ID: annRel.ID})
	require.False(t, gotAnn.IsDraft)
	require.Equal(t, c1, gotAnn.Sha1, "lag-ann did not move: its row is kept")
	// A row at another commit than its annotated tag's, as a stale
	// synchronisation leaves an annotated tag moved.
	_, err = db.GetEngine(ctx).ID(annRel.ID).Cols("sha1").Update(&repo_model.Release{Sha1: c2})
	require.NoError(t, err)
	// lag-plain's tag-only row lost, as a database restore loses it, and
	// a draft its author saved on lag-saved, which exists.
	_, err = db.GetEngine(ctx).Where("repo_id = ?", 1).In("lower_tag_name", "lag-plain", "lag-saved").Delete(new(repo_model.Release))
	require.NoError(t, err)
	saved := &repo_model.Release{RepoID: 1, PublisherID: 2, TagName: "lag-saved", LowerTagName: "lag-saved", Title: "saved", IsDraft: true}
	require.NoError(t, db.Insert(ctx, saved))

	// node-a catches up: every node's table and git hold the tags.
	caughtUp(t, a, hold, "refs/tags/lag-cli", c1)
	require.Eventually(t, func() bool { return gitRef(t, a, laggingRepo, "refs/tags/lag-mv") == c2 },
		15*time.Second, 20*time.Millisecond, "node-a projects lag-mv at c2")
	tableAt(t, tc, "refs/tags/lag-cli", c1, "lag-cli")
	tableAt(t, tc, "refs/tags/lag-mv", c2, "lag-mv")
	tableAt(t, tc, "refs/tags/lag-ann", ann, "lag-ann")
	tableAt(t, tc, "refs/tags/lag-saved", c1, "lag-saved")
	_, at := a.r.table.Refs(laggingRepo)
	require.Eventually(t, func() bool { return a.r.projector.Projected(laggingRepo) >= at },
		15*time.Second, 20*time.Millisecond, "node-a certifies the repository")

	// The report names the rows, and changes nothing.
	ms, err := a.r.Reconcile(ctx, false)
	require.NoError(t, err)
	got := releaseMismatches(ms, laggingRepo)
	require.Len(t, got, 4, "%+v", got)
	assert.Equal(t, ActionReleaseDraftTagHeld, got["lag-cli"].Action)
	assert.Equal(t, ActionReleaseDraftTagHeld, got["lag-saved"].Action)
	assert.Equal(t, ActionReleaseOldCommit, got["lag-mv"].Action)
	assert.Equal(t, ActionReleaseOldCommit, got["lag-ann"].Action)
	for _, m := range got {
		assert.Equal(t, "release", m.Kind)
		assert.Equal(t, m.Action == ActionReleaseOldCommit, m.Regenerable, "%+v", m)
		assert.False(t, m.Applied)
	}
	plain := Mismatch{Repo: laggingRepo, Category: RefsAheadOfSQL, Kind: "tag", Ref: "lag-plain", Regenerable: true}
	assert.Contains(t, ms, plain)
	unittest.AssertExistsAndLoadBean(t, &repo_model.Release{ID: cli.ID, IsDraft: true})

	// The documented repair, on node-a now fresh.
	ms, err = a.r.Reconcile(ctx, true)
	require.NoError(t, err)
	got = releaseMismatches(ms, laggingRepo)
	require.Len(t, got, 4, "%+v", got)
	for _, m := range got {
		assert.Equal(t, m.Regenerable, m.Applied, "%+v", m)
	}
	plain.Applied = true
	assert.Contains(t, ms, plain)
	rows := releasesOf(t, 1)
	assert.True(t, rows["lag-plain"].IsTag, "the lost tag-only row is written again")
	assert.Equal(t, c1, rows["lag-plain"].Sha1)
	gotCli = unittest.AssertExistsAndLoadBean(t, &repo_model.Release{ID: cli.ID})
	gotMv = unittest.AssertExistsAndLoadBean(t, &repo_model.Release{ID: mv.ID})
	gotAnn = unittest.AssertExistsAndLoadBean(t, &repo_model.Release{ID: annRel.ID})
	gotSaved := unittest.AssertExistsAndLoadBean(t, &repo_model.Release{ID: saved.ID})
	assert.True(t, gotCli.IsDraft, "a draft without a commit is not published: nothing tells it from a draft its author saved")
	assert.True(t, gotSaved.IsDraft, "a draft its author saved on an existing tag stays a draft")
	assert.Empty(t, gotSaved.Sha1)
	assert.False(t, gotMv.IsDraft)
	assert.Equal(t, c2, gotMv.Sha1, "a release stays at its tag's old commit while every node holds the tag at c2")
	assert.False(t, gotAnn.IsDraft)
	assert.Equal(t, c1, gotAnn.Sha1, "a release on an annotated tag takes the commit the tag object names")
	assert.Equal(t, "mv", gotMv.Title, "the release itself is kept")

	ms, err = a.r.Reconcile(ctx, false)
	require.NoError(t, err)
	got = releaseMismatches(ms, laggingRepo)
	assert.Len(t, got, 2, "only the drafts are left, reported: %+v", got)
	assert.Contains(t, got, "lag-cli")
	assert.Contains(t, got, "lag-saved")
	assert.NotContains(t, ms, Mismatch{Repo: laggingRepo, Category: RefsAheadOfSQL, Kind: "tag", Ref: "lag-plain", Regenerable: true})
}
