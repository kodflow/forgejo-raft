// Copyright 2026 The Forgejo Authors. All rights reserved.
// SPDX-License-Identifier: GPL-3.0-or-later

package cluster

import (
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"forgejo.org/models/db"
	git_model "forgejo.org/models/git"
	repo_model "forgejo.org/models/repo"
	"forgejo.org/models/unittest"
	user_model "forgejo.org/models/user"
	"forgejo.org/modules/setting"
	"forgejo.org/modules/test"
	"forgejo.org/services/cluster/consensus"
	"forgejo.org/services/cluster/reftable"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// A push through a deploy key is credited to the repository's owner
// (routers/private/serv.go: results.UserID = repo.OwnerID, UserName =
// OwnerName; cmd/serv.go exports them as GITEA_PUSHER_ID/NAME), an
// organization for an organization's repository. The organization was
// renamed before the consumer derived the push, and another account took
// its old name, removing the redirect. pusherRenamedSince holds that an
// organization never pushed, so the push was taken for a reissued ID at
// every attempt: parked for good, the repository's later events behind it
// and the trim of the queue stopped below it (#367, a rest of #235). The
// organization still owns the very repository the event was written for:
// it is the pusher, and the push is derived.
func TestDeployKeyPushOfARenamedOrganizationWhoseOldNameWasTakenIsDerived(t *testing.T) {
	require.NoError(t, unittest.PrepareTestDatabase())
	ctx := db.DefaultContext
	org := unittest.AssertExistsAndLoadBean(t, &user_model.User{ID: 3})
	require.True(t, org.IsOrganization())
	tok := consensus.Token{Cluster: consensus.ClusterID{0x46, 0x53}, Epoch: 1, Term: 1, Index: 7}
	// org3's deploy key pushes a new branch of org3/repo3 at master's
	// commit; no pull request names it.
	ev := reftable.Event{
		Token: tok, Kind: reftable.EventRef, Repo: "org3/repo3.git", RepoID: 3, PusherID: org.ID, PusherName: org.Name,
		Ref: "refs/heads/deploy-key", New: org3Repo3Head, Time: time.Now().UnixNano(),
	}
	require.False(t, pusherReissued(ctx, org, ev), "the pusher, under its name")

	renameAccount(t, org, "org3-renamed")
	require.False(t, pusherReissued(ctx, org, ev), "renamed: the old name redirects to it")

	// The rename moves the owner's repositories, as services/user
	// RenameUser does: their owner_name in SQL, their directory on disk
	// (here a copy, in a root of the test's own), and their keys in the
	// table (proposeOwnerRename, after the push).
	require.NoError(t, repo_model.UpdateRepositoryOwnerNames(ctx, org.ID, org.Name))
	root := t.TempDir()
	moved := filepath.Join(root, "org3-renamed", "repo3.git")
	require.NoError(t, unittest.CopyDir(repo_model.RepoPath("org3", "repo3"), moved))
	out, err := exec.Command("git", "-C", moved, "update-ref", "refs/heads/deploy-key", org3Repo3Head).CombinedOutput()
	require.NoError(t, err, string(out))
	defer test.MockVariableValue(&setting.RepoRootPath, root)()
	tbl := ownerRenamedTable{from: "org3/repo3.git", to: "org3-renamed/repo3.git", at: tok.Index + 1}

	taker := &user_model.User{Name: "org3", Email: "org3-taker@example.com"}
	require.NoError(t, user_model.CreateUser(ctx, taker))
	_, err = user_model.GetUserRedirect(ctx, "org3")
	require.True(t, user_model.IsErrUserRedirectNotExist(err), "the new account removed the redirect")

	require.NoError(t, pushWorkWith(ctx, []reftable.Event{ev}, tbl), "the organization that pushed through its deploy key is still ID 3")
	b, err := git_model.GetBranch(ctx, 3, "deploy-key")
	require.NoError(t, err)
	assert.Equal(t, org3Repo3Head, b.CommitID)
	assert.Equal(t, org.ID, b.PusherID, "credited to the organization that pushed")
}

// ownerRenamedTable is a table that moved one repository at an owner's
// rename.
type ownerRenamedTable struct {
	from, to string
	at       uint64
}

func (o ownerRenamedTable) RenamedFromBefore(repo string, before uint64) (string, uint64, bool) {
	if repo == o.to && before > o.at {
		return o.from, o.at, true
	}
	return "", 0, false
}

func (o ownerRenamedTable) RenamedAfter(from string, after uint64) (string, uint64, bool) {
	if from == o.from && after < o.at {
		return o.to, o.at, true
	}
	return "", 0, false
}

func (ownerRenamedTable) Deleted(string) (uint64, bool) { return 0, false }

// The owner rule credits only the account owning the very repository the
// event was written for, under the name that repository's owner had then:
// a reissued ID is still refused (#235). An account that took the pusher's
// ID owns none of the lost pusher's repositories.
func TestPushWorkStillRefusesAReissuedPusherIDThatOwnsAnotherRepository(t *testing.T) {
	require.NoError(t, unittest.PrepareTestDatabase())
	ctx := db.DefaultContext
	org := unittest.AssertExistsAndLoadBean(t, &user_model.User{ID: 3})
	repo3 := unittest.AssertExistsAndLoadBean(t, &repo_model.Repository{ID: 3})
	repo1 := unittest.AssertExistsAndLoadBean(t, &repo_model.Repository{ID: 1})
	require.Equal(t, org.ID, repo3.OwnerID)
	require.NotEqual(t, org.ID, repo1.OwnerID)

	// user2 pushed to her own user2/repo1; ID 3 now names org3, which owns
	// another repository.
	ev := reftable.Event{
		Token: consensus.Token{Index: 9}, Kind: reftable.EventRef, Repo: "user2/repo1.git", RepoID: 1, PusherID: 3,
		PusherName: "user2", Ref: "refs/tags/v1", New: repo1Base, Time: time.Now().UnixNano(),
	}
	assert.False(t, pusherOwnsRepo(org, ev, repo1), "the event's repository is another account's")
	err := pushWork(ctx, []reftable.Event{ev})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "pusher ID 3")

	// A push to org3/repo3 by an account that is not its owner: a member's
	// name, not the owner segment of the key.
	member := reftable.Event{Repo: "org3/repo3.git", RepoID: 3, PusherID: 3, PusherName: "lost-member"}
	assert.False(t, pusherOwnsRepo(org, member, repo3), "the pusher's name is not the owner's")
	owner := reftable.Event{Repo: "org3/repo3.git", RepoID: 3, PusherID: 3, PusherName: "ORG3"}
	assert.True(t, pusherOwnsRepo(org, owner, repo3), "the owner, names compare without case")
	owner.RepoID = 0
	assert.False(t, pusherOwnsRepo(org, owner, repo3), "no repository ID: pushWork did not check the key leads to it")
	owner.RepoID = 1
	assert.False(t, pusherOwnsRepo(org, owner, repo3), "the repository is not the event's")
	owner.RepoID, owner.PusherName = 3, ""
	assert.False(t, pusherOwnsRepo(org, owner, repo3), "an event written before payload version 4 names no pusher")
}

// The rest of #367 that pusherOwnsRepo left (#377): org3's deploy key
// pushes to org3/repo3, and before the consumer derives the push org3
// gives repo3 away to user5, then org3 is renamed and another account
// takes its old name, removing the redirect. org3 owns repo3 no more, and
// pusherRenamedSince held that an organization never pushes: the push was
// taken for a reissued ID at every attempt, parked for good. The
// organization predates the push, its old name is another account's, as
// a renamed user's push asks (#235), and the repository redirect its
// transfer left shows it owned repo3: it is the pusher. Everything is
// written under t.TempDir() (the copy of the fixture repository) and in
// the test database.
func TestDeployKeyPushOfARepositoryARenamedOrganizationGaveAwayIsDerived(t *testing.T) {
	require.NoError(t, unittest.PrepareTestDatabase())
	ctx := db.DefaultContext
	org := unittest.AssertExistsAndLoadBean(t, &user_model.User{ID: 3})
	require.True(t, org.IsOrganization())
	tok := consensus.Token{Cluster: consensus.ClusterID{0x46, 0x53}, Epoch: 1, Term: 1, Index: 7}
	ev := reftable.Event{
		Token: tok, Kind: reftable.EventRef, Repo: "org3/repo3.git", RepoID: 3, PusherID: org.ID, PusherName: org.Name,
		Ref: "refs/heads/deploy-key", New: org3Repo3Head, Time: time.Now().UnixNano(),
	}

	// The transfer of repo3 to user5: owner in SQL, directory on disk (a
	// copy in a root of the test's own), key in the table after the push.
	_, err := db.GetEngine(ctx).ID(int64(3)).Cols("owner_id", "owner_name").Update(&repo_model.Repository{OwnerID: 5, OwnerName: "user5"})
	require.NoError(t, err)
	root := t.TempDir()
	moved := filepath.Join(root, "user5", "repo3.git")
	require.NoError(t, unittest.CopyDir(repo_model.RepoPath("org3", "repo3"), moved))
	out, err := exec.Command("git", "-C", moved, "update-ref", "refs/heads/deploy-key", org3Repo3Head).CombinedOutput()
	require.NoError(t, err, string(out))
	defer test.MockVariableValue(&setting.RepoRootPath, root)()
	tbl := ownerRenamedTable{from: "org3/repo3.git", to: "user5/repo3.git", at: tok.Index + 1}

	// Then org3 is renamed, and another account takes its old name.
	renameAccount(t, org, "org3-renamed")
	taker := &user_model.User{Name: "org3", Email: "org3-taker@example.com"}
	require.NoError(t, user_model.CreateUser(ctx, taker))
	_, err = user_model.GetUserRedirect(ctx, "org3")
	require.True(t, user_model.IsErrUserRedirectNotExist(err), "the new account removed the redirect")

	repo3 := unittest.AssertExistsAndLoadBean(t, &repo_model.Repository{ID: 3})
	require.False(t, pusherOwnsRepo(org, ev, repo3), "repo3 is user5's now")
	assert.True(t, pusherReissued(ctx, org, ev), "without the transfer's redirect, nothing shows org3 owned repo3")
	// The transfer leaves a redirect from org3's ID to repo3, in its
	// transaction (services/repository/transfer.go).
	require.NoError(t, repo_model.NewRedirect(ctx, org.ID, repo3.ID, repo3.Name, repo3.Name))
	assert.False(t, pusherReissued(ctx, org, ev), "the organization predates the push, owned repo3, and its old name was taken")
	require.NoError(t, pushWorkWith(ctx, []reftable.Event{ev}, tbl), "org3, still ID 3, pushed through its deploy key before it gave the repository away")
	b, err := git_model.GetBranch(ctx, 3, "deploy-key")
	require.NoError(t, err)
	assert.Equal(t, org3Repo3Head, b.CommitID)
	assert.Equal(t, org.ID, b.PusherID, "credited to the organization that pushed")
}

// The organization rule leaves a reissued ID caught (#235): an
// organization holding the pusher's ID now is taken for the one that
// pushed only through a deploy key, under the owner segment of the
// event's key, if SQL shows it owned the event's repository, it predates
// the push beyond the clock skew, and the name it pushed under has a
// reason to have lost its redirect (#377).
func TestPushWorkStillRefusesAReissuedOrganizationID(t *testing.T) {
	require.NoError(t, unittest.PrepareTestDatabase())
	ctx := db.DefaultContext
	org := unittest.AssertExistsAndLoadBean(t, &user_model.User{ID: 3})
	require.True(t, org.IsOrganization())
	created := time.Unix(int64(org.CreatedUnix), 0)
	// user2/repo1 under the name of an account that exists: the event
	// reads as org3's deploy key pushing to its repository when it was
	// named user2, then renamed and its name taken.
	ev := reftable.Event{
		Token: consensus.Token{Index: 9}, Kind: reftable.EventRef, Repo: "user2/repo1.git", RepoID: 1, PusherID: 3,
		PusherName: "user2", Ref: "refs/tags/v1", New: repo1Base, Time: created.Add(time.Hour).UnixNano(),
	}
	assert.True(t, pusherReissued(ctx, org, ev), "org3 never owned repo1: nothing shows it pushed")
	err := pushWork(ctx, []reftable.Event{ev})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "pusher ID 3")

	// org3 owned repo1 once and gave it away: the redirect of its transfer.
	require.NoError(t, repo_model.NewRedirect(ctx, org.ID, 1, "repo1", "repo1"))
	assert.False(t, pusherReissued(ctx, org, ev), "a deploy-key push of the organization, which gave the repository away and was renamed since, its old name taken")

	late := ev
	late.Time = created.Add(-time.Hour).UnixNano()
	assert.True(t, pusherReissued(ctx, org, late), "created after the push")
	err = pushWork(ctx, []reftable.Event{late})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "pusher ID 3")
	late.Time = created.Add(time.Minute).UnixNano()
	assert.True(t, pusherReissued(ctx, org, late), "created within the clock skew before the push")

	member := ev
	member.Repo = "org3/repo3.git"
	assert.True(t, pusherReissued(ctx, org, member), "an organization never pushes under a name that is not the owner segment of the key")
	member.Repo = ""
	assert.True(t, pusherReissued(ctx, org, member), "no key, no owner segment")
	noID := ev
	noID.RepoID = 0
	assert.True(t, pusherReissued(ctx, org, noID), "no repository ID: pushWork did not check the key leads to it")

	nobody := ev
	nobody.Repo, nobody.PusherName = "nobody-took-it/repo1.git", "nobody-took-it"
	assert.True(t, pusherReissued(ctx, org, nobody), "an old name with neither an account nor a redirect")
}
