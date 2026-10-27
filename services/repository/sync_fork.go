// Copyright 2025 The Forgejo Authors. All rights reserved.
// SPDX-License-Identifier: MIT

package repository

import (
	"context"
	"fmt"
	"slices"
	"time"

	git_model "forgejo.org/models/git"
	repo_model "forgejo.org/models/repo"
	user_model "forgejo.org/models/user"
	"forgejo.org/modules/clusterhook"
	"forgejo.org/modules/git"
	"forgejo.org/modules/log"
	repo_module "forgejo.org/modules/repository"
	api "forgejo.org/modules/structs"
)

// SyncFork syncs a branch of a fork with the base repo
func SyncFork(ctx context.Context, doer *user_model.User, repo *repo_model.Repository, branch string) error {
	err := repo.MustNotBeArchived()
	if err != nil {
		return err
	}

	err = repo.GetBaseRepo(ctx)
	if err != nil {
		return err
	}

	// In a cluster, the branch is pushed from a staging copy of it, read
	// from the base's directory on this node once this node serves the
	// base, between two checks that it holds the base (#283).
	from, removeStaging, err := stageSyncForkBranch(ctx, repo.BaseRepo, branch)
	if err != nil {
		return err
	}
	defer removeStaging()

	err = git.Push(ctx, from, git.PushOptions{
		Remote: repo.RepoPath(),
		Branch: fmt.Sprintf("%s:%s", branch, branch),
		Env:    repo_module.PushingEnvironment(doer, repo),
	})

	return err
}

// stageSyncForkBranch returns the directory a sync of a fork pushes branch
// from, and its removal. Without a cluster it is the base's directory.
//
// In a cluster the push went from the base's directory on this node right
// into the fork, whose ref transaction sends the objects it carries to the
// other members. On a node behind the cluster, the directory showed the
// base's branch from before a push another node acknowledged. Or it held
// another repository, a private one, renamed under the base's name and
// projected on this node meanwhile: its branch, which may extend the
// fork's, went into the fork, public as its base, on every member (#283,
// as for a fork). The sync now first waits for this node to serve the base
// at the commit index the cluster confirms at the call
// (clusterhook.RequireFresh), then reads the branch once, by `git clone
// --bare --single-branch` into a staging copy outside the repository root,
// between two checks that the directory holds the base, and nothing else
// (readSourceDirectory). Every refusal wraps clusterhook.ErrBehind and
// comes before anything is pushed: the push goes from that copy.
func stageSyncForkBranch(ctx context.Context, base *repo_model.Repository, branch string) (string, func(), error) {
	if err := clusterhook.RequireFresh(ctx, clusterhook.RepoKey(base.OwnerName, base.Name, false)); err != nil {
		return "", nil, err
	}
	from := base.RepoPath()
	if !clusterhook.Enabled() {
		return from, func() {}, nil
	}
	staging, err := repo_module.CreateTemporaryPath("sync-fork")
	if err != nil {
		return "", nil, err
	}
	remove := func() {
		if err := repo_module.RemoveTemporaryPath(staging); err != nil {
			log.Error("Failed to remove the staging copy of %s's branch %s: %v", base.FullName(), branch, err)
		}
	}
	if err := readSourceDirectory(ctx, base, "base repository", syncForkRefused, func() error {
		if stdout, _, err := git.NewCommand(ctx, "clone", "--bare", "--quiet", "--no-tags", "--single-branch", "--branch").AddDynamicArguments(branch, from, staging).
			SetDescription(fmt.Sprintf("stageSyncForkBranch(git clone): %s %s", base.FullName(), branch)).
			RunStdString(&git.RunOpts{Timeout: 10 * time.Minute}); err != nil {
			return fmt.Errorf("git clone: %w: %s", err, stdout)
		}
		return nil
	}); err != nil {
		remove()
		return "", nil, err
	}
	return staging, remove, nil
}

// syncForkRefused is a refusal of a sync of a fork, wrapping
// clusterhook.ErrBehind: the caller retries.
func syncForkRefused(err error) error {
	return fmt.Errorf("%w: %w; the fork was not synced", clusterhook.ErrBehind, err)
}

// CanSyncFork returns information about syncing a fork
func GetSyncForkInfo(ctx context.Context, repo *repo_model.Repository, branch string) (*api.SyncForkInfo, error) {
	info := new(api.SyncForkInfo)

	if !repo.IsFork {
		return info, nil
	}

	if repo.IsArchived {
		return info, nil
	}

	err := repo.GetBaseRepo(ctx)
	if err != nil {
		return nil, err
	}

	forkBranch, err := git_model.GetBranch(ctx, repo.ID, branch)
	if err != nil {
		return nil, err
	}

	info.ForkCommit = forkBranch.CommitID

	baseBranch, err := git_model.GetBranch(ctx, repo.BaseRepo.ID, branch)
	if err != nil {
		if git_model.IsErrBranchNotExist(err) {
			// If the base repo don't have the branch, we don't need to continue
			return info, nil
		}
		return nil, err
	}

	info.BaseCommit = baseBranch.CommitID

	// If both branches has the same latest commit, we don't need to sync
	if forkBranch.CommitID == baseBranch.CommitID {
		return info, nil
	}

	// Check if the latest commit of the fork is also in the base. In a
	// cluster, the base's directory on this node is read between two
	// checks that it holds the base, and nothing else (#283): a directory
	// that held another repository meanwhile refuses the answer, wrapping
	// clusterhook.ErrBehind, rather than tell what that one holds.
	allowed, behind := false, 0
	if err := readSourceDirectory(ctx, repo.BaseRepo, "base repository", syncForkRefused, func() error {
		gitRepo, err := git.OpenRepository(ctx, repo.BaseRepo.RepoPath())
		if err != nil {
			return err
		}
		defer gitRepo.Close()

		commit, err := gitRepo.GetCommit(forkBranch.CommitID)
		if err != nil {
			if git.IsErrNotExist(err) {
				return nil
			}
			return err
		}

		branchList, err := commit.GetAllBranches()
		if err != nil {
			return err
		}

		if !slices.Contains(branchList, branch) {
			return nil
		}

		diff, err := git.GetDivergingCommits(ctx, repo.BaseRepo.RepoPath(), baseBranch.CommitID, forkBranch.CommitID, nil)
		if err != nil {
			return err
		}
		allowed, behind = true, diff.Behind
		return nil
	}); err != nil {
		return nil, err
	}

	info.Allowed = allowed
	info.CommitsBehind = behind

	return info, nil
}
