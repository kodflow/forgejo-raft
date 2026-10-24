// Copyright 2017 The Gitea Authors. All rights reserved.
// SPDX-License-Identifier: MIT

package repository

import (
	"context"
	"strings"
	"sync/atomic"
	"testing"

	"forgejo.org/models/db"
	repo_model "forgejo.org/models/repo"
	"forgejo.org/models/unittest"
	user_model "forgejo.org/models/user"
	"forgejo.org/modules/git"
	"forgejo.org/modules/setting"
	"forgejo.org/modules/test"
	"forgejo.org/modules/util"

	"code.forgejo.org/xorm/xorm/contexts"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestForkRepository(t *testing.T) {
	require.NoError(t, unittest.PrepareTestDatabase())

	// user 13 has already forked repo10
	user := unittest.AssertExistsAndLoadBean(t, &user_model.User{ID: 13})
	repo := unittest.AssertExistsAndLoadBean(t, &repo_model.Repository{ID: 10})

	fork, err := ForkRepositoryAndUpdates(git.DefaultContext, user, user, ForkRepoOptions{
		BaseRepo:    repo,
		Name:        "test",
		Description: "test",
	})
	assert.Nil(t, fork)
	require.Error(t, err)
	assert.True(t, IsErrForkAlreadyExist(err))

	// user not reached maximum limit of repositories
	assert.False(t, repo_model.IsErrReachLimitOfRepo(err))

	// change AllowForkWithoutMaximumLimit to false for the test
	defer test.MockVariableValue(&setting.Repository.AllowForkWithoutMaximumLimit, false)()
	// user has reached maximum limit of repositories
	user.MaxRepoCreation = 0
	fork2, err := ForkRepositoryAndUpdates(git.DefaultContext, user, user, ForkRepoOptions{
		BaseRepo:    repo,
		Name:        "test",
		Description: "test",
	})
	assert.Nil(t, fork2)
	assert.True(t, repo_model.IsErrReachLimitOfRepo(err))
}

// forkCountProbe runs seen when the fork count of a base repository is
// incremented, while on is set.
type forkCountProbe struct {
	on   atomic.Bool
	seen func()
}

func (p *forkCountProbe) BeforeProcess(c *contexts.ContextHook) (context.Context, error) {
	if p.on.Load() && strings.Contains(c.SQL, "num_forks=num_forks+1") {
		p.seen()
	}
	return c.Ctx, nil
}

func (p *forkCountProbe) AfterProcess(*contexts.ContextHook) error { return nil }

// A fork counts itself on its base last, once its repository is cloned:
// the update locks the base's row until the fork's transaction commits,
// and made first it held it during the whole `git clone` (#177). A fork
// that fails counts nothing and leaves no repository behind.
func TestForkCountsItselfOnItsBaseOnceCloned(t *testing.T) {
	require.NoError(t, unittest.PrepareTestDatabase())
	ctx := db.DefaultContext
	base := unittest.AssertExistsAndLoadBean(t, &repo_model.Repository{ID: 1})
	require.NoError(t, base.LoadOwner(ctx))
	owner := unittest.AssertExistsAndLoadBean(t, &user_model.User{ID: 4})

	var counted, cloned []string
	probe := &forkCountProbe{}
	eng, err := db.GetMasterEngine(db.GetEngine(ctx))
	require.NoError(t, err)
	eng.AddHook(probe)
	t.Cleanup(func() { probe.on.Store(false) })

	for _, name := range []string{"broken-fork", "repo1-fork"} {
		probe.seen = func() {
			counted = append(counted, name)
			if git.IsBranchExist(ctx, repo_model.RepoPath(owner.Name, name), base.DefaultBranch) {
				cloned = append(cloned, name)
			}
		}
		probe.on.Store(true)
		opts := ForkRepoOptions{BaseRepo: base, Name: name, Description: "fork"}
		if name == "broken-fork" {
			// The clone fails: the branch does not exist.
			opts.SingleBranch = "no-such-branch"
		}
		_, err := ForkRepositoryIfNotExists(ctx, owner, owner, opts)
		probe.on.Store(false)
		if name == "broken-fork" {
			require.Error(t, err)
			unittest.AssertNotExistsBean(t, &repo_model.Repository{OwnerID: owner.ID, LowerName: name})
			exists, err := util.IsExist(repo_model.RepoPath(owner.Name, name))
			require.NoError(t, err)
			assert.False(t, exists, "the failed fork left no repository behind")
			assert.Equal(t, base.NumForks, unittest.AssertExistsAndLoadBean(t, &repo_model.Repository{ID: base.ID}).NumForks, "the failed fork is not counted")
			continue
		}
		require.NoError(t, err)
	}
	assert.Equal(t, []string{"repo1-fork"}, counted, "the base counts the fork that succeeded, once")
	assert.Equal(t, []string{"repo1-fork"}, cloned, "the fork was cloned when the base counted it")
	assert.Equal(t, base.NumForks+1, unittest.AssertExistsAndLoadBean(t, &repo_model.Repository{ID: base.ID}).NumForks)
}
