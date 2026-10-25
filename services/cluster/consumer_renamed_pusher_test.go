// Copyright 2026 The Forgejo Authors. All rights reserved.
// SPDX-License-Identifier: GPL-3.0-or-later

package cluster

import (
	"os/exec"
	"testing"
	"time"

	cluster_model "forgejo.org/models/cluster"
	"forgejo.org/models/db"
	git_model "forgejo.org/models/git"
	repo_model "forgejo.org/models/repo"
	"forgejo.org/models/unittest"
	user_model "forgejo.org/models/user"
	"forgejo.org/modules/setting"
	"forgejo.org/modules/test"
	"forgejo.org/modules/timeutil"
	"forgejo.org/services/cluster/consensus"
	"forgejo.org/services/cluster/reftable"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// org3Repo3Head is the master commit of the fixture repository org3/repo3.
const org3Repo3Head = "2a47ca4b614a9f5a43abbd5ad851a54a616ffee6"

// renameAccount renames u in SQL as services/user RenameUser does, without
// its moves of the account's repositories on disk.
func renameAccount(t *testing.T, u *user_model.User, to string) {
	t.Helper()
	ctx := db.DefaultContext
	require.NoError(t, user_model.NewUserRedirect(ctx, u.ID, u.Name, to))
	u.Name, u.LowerName = to, to
	require.NoError(t, user_model.UpdateUserCols(ctx, u, "name", "lower_name"))
}

// A rename leaves a redirect from the old name, which Forgejo removes as
// soon as another account takes that name: an account created under it
// (createUser DeleteUserRedirect) or renamed into it (NewUserRedirect),
// with USERNAME_COOLDOWN_PERIOD 0 by default. An event of user2 still to
// derive when she renamed herself and someone registered "user2" was taken
// for a reissued ID at every attempt: parked for good, the repository's
// later events behind it and the trim of the queue stopped below it
// (#235). It is derived, credited to her account.
func TestPushOfARenamedPusherWhoseOldNameWasTakenIsDerived(t *testing.T) {
	require.NoError(t, unittest.PrepareTestDatabase())
	ctx := db.DefaultContext
	repo3 := repo_model.RepoPath("org3", "repo3")
	out, err := exec.Command("git", "-C", repo3, "update-ref", "refs/heads/renamed-pusher", org3Repo3Head).CombinedOutput()
	require.NoError(t, err, string(out))
	t.Cleanup(func() {
		out, err := exec.Command("git", "-C", repo3, "update-ref", "-d", "refs/heads/renamed-pusher").CombinedOutput()
		assert.NoError(t, err, string(out))
	})
	tok := consensus.Token{Cluster: consensus.ClusterID{0x46, 0x53}, Epoch: 1, Term: 1, Index: 7}
	lineage := tok.Identity().String()
	// user2 pushes to org3/repo3, another owner's repository that her
	// rename does not move.
	ev := reftable.Event{
		Token: tok, Kind: reftable.EventRef, Repo: "org3/repo3.git", RepoID: 3, PusherID: 2, PusherName: "user2",
		Ref: "refs/heads/renamed-pusher", New: org3Repo3Head, Time: time.Now().UnixNano(),
	}
	u := unittest.AssertExistsAndLoadBean(t, &user_model.User{ID: 2})
	require.False(t, pusherReissued(ctx, u, ev), "the pusher, under her name")

	renameAccount(t, u, "user2-renamed")
	require.False(t, pusherReissued(ctx, u, ev), "renamed: the old name redirects to her")

	// Someone registers the freed name, as the sign-up form does.
	taker := &user_model.User{Name: "user2", Email: "renamed-pusher-taker@example.com"}
	require.NoError(t, user_model.CreateUser(ctx, taker))
	_, err = user_model.GetUserRedirect(ctx, "user2")
	require.True(t, user_model.IsErrUserRedirectNotExist(err), "the new account removed the redirect")
	assert.False(t, pusherReissued(ctx, u, ev), "the account that pushed is still ID 2, renamed: not a reissued ID")

	// The consumer's dispatch with the real work, at its first attempt.
	c := newConsumer(newReplicator(t.TempDir(), newRegistry(0, 1, 1)))
	require.NoError(t, c.dispatch(ctx, lineage, &cluster_model.Cursor{}, []reftable.Event{ev}))
	rows, err := cluster_model.ListParked(ctx)
	require.NoError(t, err)
	assert.Empty(t, rows, "a legitimate push is not parked")
	b, err := git_model.GetBranch(ctx, 3, "renamed-pusher")
	require.NoError(t, err)
	assert.Equal(t, org3Repo3Head, b.CommitID)
	assert.EqualValues(t, 2, b.PusherID, "credited to the account that pushed")

	// The new "user2" renames itself away in turn: the old name now
	// redirects to it, not to the pusher.
	renameAccount(t, taker, "user2-taker")
	r, err := user_model.GetUserRedirect(ctx, "user2")
	require.NoError(t, err)
	require.Equal(t, taker.ID, r.RedirectUserID)
	assert.False(t, pusherReissued(ctx, u, ev), "the old name's redirect goes to the account that took it, then left it")

	// Another account renamed into the old name removes the redirect too.
	require.NoError(t, unittest.PrepareTestDatabase())
	u = unittest.AssertExistsAndLoadBean(t, &user_model.User{ID: 2})
	renameAccount(t, u, "user2-renamed")
	renameAccount(t, unittest.AssertExistsAndLoadBean(t, &user_model.User{ID: 4}), "user2")
	_, err = user_model.GetUserRedirect(ctx, "user2")
	require.True(t, user_model.IsErrUserRedirectNotExist(err))
	assert.False(t, pusherReissued(ctx, u, ev), "renamed into: the old name is another account's")
}

// MAX_USER_REDIRECTS keeps the newest redirects of an account: a pusher who
// renamed herself past it since the push lost the redirect from the name
// she pushed under, with no account on that name. Her push is derived; a
// redirect list that only shows renames from before the push, or a limit
// that drops nothing, proves no such loss (#235).
func TestPushOfAPusherRenamedPastMaxUserRedirects(t *testing.T) {
	require.NoError(t, unittest.PrepareTestDatabase())
	ctx := db.DefaultContext
	defer test.MockVariableValue(&setting.Service.MaxUserRedirects, int64(1))()
	ev := reftable.Event{
		Token: consensus.Token{Index: 9}, Kind: reftable.EventRef, Repo: "user2/repo1.git", RepoID: 1, PusherID: 2,
		PusherName: "user2", Ref: "refs/tags/v-limit", New: repo1Base, Time: time.Now().Add(-2 * time.Hour).UnixNano(),
	}
	u := unittest.AssertExistsAndLoadBean(t, &user_model.User{ID: 2})
	renameAccount(t, u, "user2-mid")
	// The first rename, an hour after the push, is the older redirect.
	_, err := db.GetEngine(ctx).Exec("UPDATE user_redirect SET created_unix = ? WHERE lower_name = ?",
		timeutil.TimeStamp(time.Now().Add(-time.Hour).Unix()), "user2")
	require.NoError(t, err)
	renameAccount(t, u, "user2-renamed")
	require.NoError(t, user_model.LimitUserRedirects(ctx, u.ID, setting.Service.MaxUserRedirects))
	_, err = user_model.GetUserRedirect(ctx, "user2")
	require.True(t, user_model.IsErrUserRedirectNotExist(err), "the limit dropped the redirect from the pushed name")
	taken, err := user_model.IsUserExist(ctx, u.ID, "user2")
	require.NoError(t, err)
	require.False(t, taken)
	assert.False(t, pusherReissued(ctx, u, ev), "renamed past MAX_USER_REDIRECTS: not a reissued ID")

	before := ev
	before.Time = time.Now().Add(time.Hour).UnixNano()
	assert.True(t, pusherReissued(ctx, u, before), "every redirect kept predates the push: none made since was dropped")
	defer test.MockVariableValue(&setting.Service.MaxUserRedirects, int64(0))()
	assert.True(t, pusherReissued(ctx, u, ev), "no limit drops a redirect")
}

// The rename rule leaves a reissued ID caught. SQL that lost the pusher's
// row hands its ID to an account created after the loss, so after the
// push, and the old name may well be taken again since: the README asks an
// operator to recreate a lost owner. An account created within the clock
// skew of the push is not shown to predate it, an old name nobody took
// gives the redirect no reason to be gone, and an organization never
// pushed.
func TestPushWorkStillRefusesAReissuedPusherIDWhoseNameWasTaken(t *testing.T) {
	require.NoError(t, unittest.PrepareTestDatabase())
	ctx := db.DefaultContext
	u := unittest.AssertExistsAndLoadBean(t, &user_model.User{ID: 2})
	created := time.Unix(int64(u.CreatedUnix), 0)
	ev := reftable.Event{
		Token: consensus.Token{Index: 9}, Kind: reftable.EventRef, Repo: "user2/repo1.git", RepoID: 1, PusherID: 2,
		PusherName: "lost-pusher", Ref: "refs/tags/v1", New: repo1Base, Time: created.Add(-time.Hour).UnixNano(),
	}
	require.NoError(t, user_model.CreateUser(ctx, &user_model.User{Name: "lost-pusher", Email: "lost-pusher@example.com"}))
	assert.True(t, pusherReissued(ctx, u, ev), "created after the push, the old name recreated since")
	err := pushWork(ctx, []reftable.Event{ev})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "pusher ID 2")

	ev.Time = created.Add(-time.Minute).UnixNano()
	assert.True(t, pusherReissued(ctx, u, ev), "created within the clock skew after the push")
	ev.Time = created.Add(time.Minute).UnixNano()
	assert.True(t, pusherReissued(ctx, u, ev), "created within the clock skew before the push")

	ev.PusherName, ev.Time = "nobody-took-it", created.Add(time.Hour).UnixNano()
	assert.True(t, pusherReissued(ctx, u, ev), "an old name with neither an account nor a redirect")

	org := unittest.AssertExistsAndLoadBean(t, &user_model.User{ID: 3})
	require.True(t, org.IsOrganization())
	orgEv := reftable.Event{PusherID: 3, PusherName: "user2", Time: time.Now().UnixNano()}
	assert.True(t, pusherReissued(ctx, org, orgEv), "an organization holding the ID never pushed")
}
