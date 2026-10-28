// Copyright 2026 The Forgejo Authors. All rights reserved.
// SPDX-License-Identifier: GPL-3.0-or-later

package release

import (
	"context"
	"testing"

	"forgejo.org/models/db"
	repo_model "forgejo.org/models/repo"
	"forgejo.org/models/unittest"
	user_model "forgejo.org/models/user"
	"forgejo.org/modules/clusterhook"
	"forgejo.org/modules/git"
	"forgejo.org/modules/gitrepo"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// In a cluster, a write of a release or a tag waits for the consumer to
// derive the repository's last writes, and is refused while it has not:
// nothing is written, no row and no tag. A release deleted with its tag
// kept does not wait: the derivation of a push to its tag converges on the
// tag row it becomes (#298).
func TestReleaseWritesWaitForTheRowsToBeDerived(t *testing.T) {
	require.NoError(t, unittest.PrepareTestDatabase())
	doer := unittest.AssertExistsAndLoadBean(t, &user_model.User{ID: 2})
	repo := unittest.AssertExistsAndLoadBean(t, &repo_model.Repository{ID: 1})
	gitRepo, err := gitrepo.OpenRepository(git.DefaultContext, repo)
	require.NoError(t, err)
	defer gitRepo.Close()
	var asked []string
	clusterhook.SetDerivedWaiter(func(_ context.Context, key string) error {
		asked = append(asked, key)
		return clusterhook.ErrBehind
	})
	t.Cleanup(func() { clusterhook.SetDerivedWaiter(nil) })

	err = CreateRelease(gitRepo, &repo_model.Release{RepoID: 1, Repo: repo, PublisherID: doer.ID, Publisher: doer, TagName: "refused-release", Target: "master", Title: "refused"}, "", nil)
	require.ErrorIs(t, err, clusterhook.ErrBehind)
	require.ErrorIs(t, CreateNewTag(db.DefaultContext, doer, repo, "master", "refused-tag", ""), clusterhook.ErrBehind)
	tagRow := unittest.AssertExistsAndLoadBean(t, &repo_model.Release{ID: 3})
	tagRow.IsTag, tagRow.Title, tagRow.Target, tagRow.Repo = false, "refused", "master", repo
	require.ErrorIs(t, UpdateRelease(db.DefaultContext, doer, gitRepo, tagRow, true, nil), clusterhook.ErrBehind)
	tagRow = unittest.AssertExistsAndLoadBean(t, &repo_model.Release{ID: 3})
	require.ErrorIs(t, DeleteReleaseByID(db.DefaultContext, repo, tagRow, doer, true), clusterhook.ErrBehind)
	assert.Equal(t, []string{"user2/repo1.git", "user2/repo1.git", "user2/repo1.git", "user2/repo1.git"}, asked)

	for _, name := range []string{"refused-release", "refused-tag", "delete-tag"} {
		assert.False(t, gitRepo.IsTagExist(name), "a refused write created the tag %s", name)
	}
	unittest.AssertNotExistsBean(t, &repo_model.Release{RepoID: 1, LowerTagName: "refused-release"})
	unittest.AssertNotExistsBean(t, &repo_model.Release{RepoID: 1, LowerTagName: "refused-tag"})
	tagRow = unittest.AssertExistsAndLoadBean(t, &repo_model.Release{ID: 3})
	assert.True(t, tagRow.IsTag, "the tag row is left as it was")
	assert.NotEqual(t, "refused", tagRow.Title)

	draft := unittest.AssertExistsAndLoadBean(t, &repo_model.Release{ID: 4})
	require.NoError(t, DeleteReleaseByID(db.DefaultContext, repo, draft, doer, false))
	assert.True(t, unittest.AssertExistsAndLoadBean(t, &repo_model.Release{ID: 4}).IsTag, "the release is kept as a tag")
}

// The routes wait before they read the release they update; a push
// acknowledged between that wait and the service's own is derived during
// the latter, and may remove the row the caller read: the tag-only row of
// a tag the push deleted. Updated then, the release was written in no
// row, after its tag was created again, and the client told it exists.
// It is refused instead, nothing written (#298).
func TestUpdateReleaseOfARowRemovedWhileItsRowsWereDerived(t *testing.T) {
	require.NoError(t, unittest.PrepareTestDatabase())
	doer := unittest.AssertExistsAndLoadBean(t, &user_model.User{ID: 2})
	repo := unittest.AssertExistsAndLoadBean(t, &repo_model.Repository{ID: 1})
	gitRepo, err := gitrepo.OpenRepository(git.DefaultContext, repo)
	require.NoError(t, err)
	defer gitRepo.Close()
	clusterhook.SetEnabled(true)
	t.Cleanup(func() { clusterhook.SetEnabled(false) })

	rel := unittest.AssertExistsAndLoadBean(t, &repo_model.Release{ID: 3})
	require.True(t, rel.IsTag)
	// The consumer derives the push that deleted the tag while the request
	// waits: the tag-only row goes.
	clusterhook.SetDerivedWaiter(func(ctx context.Context, _ string) error {
		_, err := db.DeleteByID[repo_model.Release](ctx, rel.ID)
		return err
	})
	t.Cleanup(func() { clusterhook.SetDerivedWaiter(nil) })

	rel.IsTag, rel.Title, rel.Target, rel.Repo, rel.Publisher, rel.PublisherID = false, "published", "master", repo, doer, doer.ID
	err = UpdateRelease(db.DefaultContext, doer, gitRepo, rel, true, nil)
	require.ErrorIs(t, err, clusterhook.ErrBehind)
	assert.Contains(t, err.Error(), "was removed while its rows were derived")
	assert.False(t, gitRepo.IsTagExist("delete-tag"), "a tag was created for a release written in no row")
	unittest.AssertNotExistsBean(t, &repo_model.Release{ID: rel.ID})
}
