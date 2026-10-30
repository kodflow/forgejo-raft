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
