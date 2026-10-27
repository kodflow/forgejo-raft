// Copyright 2019 The Gitea Authors. All rights reserved.
// SPDX-License-Identifier: MIT

package repository

import (
	"context"
	"fmt"
	"strings"
	"time"

	"forgejo.org/models/db"
	git_model "forgejo.org/models/git"
	repo_model "forgejo.org/models/repo"
	user_model "forgejo.org/models/user"
	"forgejo.org/modules/clusterhook"
	"forgejo.org/modules/git"
	"forgejo.org/modules/gitrepo"
	"forgejo.org/modules/log"
	repo_module "forgejo.org/modules/repository"
	"forgejo.org/modules/structs"
	"forgejo.org/modules/util"
	notify_service "forgejo.org/services/notify"
)

// ErrForkAlreadyExist represents a "ForkAlreadyExist" kind of error.
type ErrForkAlreadyExist struct {
	Uname    string
	RepoName string
	ForkName string
}

// IsErrForkAlreadyExist checks if an error is an ErrForkAlreadyExist.
func IsErrForkAlreadyExist(err error) bool {
	_, ok := err.(ErrForkAlreadyExist)
	return ok
}

func (err ErrForkAlreadyExist) Error() string {
	return fmt.Sprintf("repository is already forked by user [uname: %s, repo path: %s, fork path: %s]", err.Uname, err.RepoName, err.ForkName)
}

func (err ErrForkAlreadyExist) Unwrap() error {
	return util.ErrAlreadyExist
}

// ForkRepoOptions contains the fork repository options
type ForkRepoOptions struct {
	BaseRepo     *repo_model.Repository
	Name         string
	Description  string
	SingleBranch string
}

// ForkRepositoryIfNotExists creates a fork of a repository if it does not already exists and fails otherwise
func ForkRepositoryIfNotExists(ctx context.Context, doer, owner *user_model.User, opts ForkRepoOptions) (*repo_model.Repository, error) {
	// Fork is prohibited, if user has reached maximum limit of repositories
	if !doer.IsAdmin && !owner.CanForkRepo() {
		return nil, repo_model.ErrReachLimitOfRepo{
			Limit: owner.MaxRepoCreation,
		}
	}

	forkedRepo, err := repo_model.GetUserFork(ctx, opts.BaseRepo.ID, owner.ID)
	if err != nil {
		return nil, err
	}
	if forkedRepo != nil {
		return nil, ErrForkAlreadyExist{
			Uname:    owner.Name,
			RepoName: opts.BaseRepo.FullName(),
			ForkName: forkedRepo.FullName(),
		}
	}

	// In a cluster, a name whose key the cluster holds is refused before
	// anything is written, as a creation's (#245).
	if err := clusterCreateRefusal(ctx, owner, opts.Name); err != nil {
		return nil, err
	}
	// And the base's directory on this node is read before anything is
	// written too: once this node serves the base at the cluster's commit
	// index, between two checks that it holds the base, into a staging
	// copy the fork's refs then come from (#283).
	staging, removeStaging, err := stageForkBase(ctx, opts.BaseRepo)
	if err != nil {
		return nil, err
	}
	defer removeStaging()

	defaultBranch := opts.BaseRepo.DefaultBranch
	if opts.SingleBranch != "" {
		defaultBranch = opts.SingleBranch
	}
	repo := &repo_model.Repository{
		OwnerID:          owner.ID,
		Owner:            owner,
		OwnerName:        owner.Name,
		Name:             opts.Name,
		LowerName:        strings.ToLower(opts.Name),
		Description:      opts.Description,
		DefaultBranch:    defaultBranch,
		IsPrivate:        opts.BaseRepo.IsPrivate || opts.BaseRepo.Owner.Visibility == structs.VisibleTypePrivate,
		IsEmpty:          opts.BaseRepo.IsEmpty,
		IsFork:           true,
		ForkID:           opts.BaseRepo.ID,
		ObjectFormatName: opts.BaseRepo.ObjectFormatName,
	}

	oldRepoPath := opts.BaseRepo.RepoPath()

	needsRollback := false
	rollbackFn := func(cause error) {
		if !needsRollback {
			return
		}

		repoPath := repo_model.RepoPath(owner.Name, repo.Name)

		if exists, _ := util.IsExist(repoPath); !exists {
			return
		}

		// As the transaction will be failed and hence database changes will be destroyed we only need
		// to delete the related repository on the filesystem, and from the cluster's table
		if errDelete := removeAbandonedRepo(ctx, owner.Name, repo.Name, cause); errDelete != nil {
			log.Error("Failed to remove fork repo: %v", errDelete)
		}
	}

	// In a cluster, the fork's refs go into the replicated table during
	// the transaction that commits its repository row: both fall on the
	// same side of a checkpoint, the removal of an abandoned fork
	// included, or a restore finds refs SQL does not name (#159). It is
	// released last, after any undo.
	releaseCut, err := clusterhook.HoldCut(ctx)
	if err != nil {
		return nil, err
	}
	defer releaseCut()

	needsRollbackInPanic := true
	defer func() {
		panicErr := recover()
		if panicErr == nil {
			return
		}

		if needsRollbackInPanic {
			rollbackFn(nil)
		}
		panic(panicErr)
	}()

	err = db.WithTx(ctx, func(txCtx context.Context) error {
		if err = repo_module.CreateRepositoryByExample(txCtx, doer, owner, repo, false, true); err != nil {
			return err
		}

		// copy lfs files failure should not be ignored
		if err = git_model.CopyLFS(txCtx, repo, opts.BaseRepo); err != nil {
			return err
		}

		needsRollback = true

		repoPath := repo_model.RepoPath(owner.Name, repo.Name)
		if clusterhook.Enabled() {
			if err := copyForkInCluster(txCtx, opts, staging, repoPath, defaultBranch, repo.ObjectFormatName); err != nil {
				log.Error("Fork Repository (git init and fetch) Failed for %v (from %v): %v", repo, opts.BaseRepo, err)
				return err
			}
		} else {
			cloneCmd := git.NewCommand(txCtx, "clone", "--bare")
			if opts.SingleBranch != "" {
				cloneCmd.AddArguments("--single-branch", "--branch").AddDynamicArguments(opts.SingleBranch)
			}
			if stdout, _, err := cloneCmd.AddDynamicArguments(oldRepoPath, repoPath).
				SetDescription(fmt.Sprintf("ForkRepositoryIfNotExists(git clone): %s to %s", opts.BaseRepo.FullName(), repo.FullName())).
				RunStdBytes(&git.RunOpts{Timeout: 10 * time.Minute}); err != nil {
				log.Error("Fork Repository (git clone) Failed for %v (from %v):\nStdout: %s\nError: %v", repo, opts.BaseRepo, stdout, err)
				return fmt.Errorf("git clone: %w", err)
			}
		}

		if err := repo_module.CheckDaemonExportOK(txCtx, repo); err != nil {
			return fmt.Errorf("checkDaemonExportOK: %w", err)
		}

		if stdout, _, err := git.NewCommand(txCtx, "update-server-info").
			SetDescription(fmt.Sprintf("ForkRepositoryIfNotExists(git update-server-info): %s", repo.FullName())).
			RunStdString(&git.RunOpts{Dir: repoPath}); err != nil {
			log.Error("Fork Repository (git update-server-info) failed for %v:\nStdout: %s\nError: %v", repo, stdout, err)
			return fmt.Errorf("git update-server-info: %w", err)
		}

		gitRepo, err := gitrepo.OpenRepository(txCtx, repo)
		if err != nil {
			return fmt.Errorf("OpenRepository: %w", err)
		}
		defer gitRepo.Close()

		if _, err = repo_module.SyncRepoBranchesWithRepo(txCtx, repo, gitRepo, doer.ID); err != nil {
			return err
		}

		// The base repository's row is locked from this update to the
		// commit. Made last, it is not held while the fork's objects are
		// copied, and in a cluster sent to a majority, for minutes: a push
		// of the base waited behind it on the leader's log consumer, and
		// with it the SQL of every repository of the cluster (#177). The
		// transaction still commits the count with the fork, or neither.
		return repo_model.IncrementRepoForkNum(txCtx, opts.BaseRepo.ID)
	})
	needsRollbackInPanic = false
	if err != nil {
		rollbackFn(err)
		return nil, err
	}

	return repo, nil
}

// copyForkInCluster takes into the fork the refs `git clone --bare` would:
// the branches, or the one branch asked, and the tags, with HEAD on the
// default branch. In a cluster a clone cannot do it: its first ref
// transaction carries every ref, whose objects go to the other members for
// a key they know neither from the replicated table nor from SQL, where
// the fork's row is not committed yet, and they refuse them. `git init`
// writes HEAD alone first, which ships no object and makes the key known
// to the table; the refs then follow with `git fetch`, by batches that
// stay within the cluster's bound of refs per transaction (#80). from is
// the staging copy of the base stageForkBase read and checked, never the
// base's directory: each batch sends its refs to the other members as it
// lands, and the projector may move another repository under the base's
// name between two of them (#283).
func copyForkInCluster(ctx context.Context, opts ForkRepoOptions, from, to, defaultBranch, objectFormat string) error {
	if strings.ContainsAny(opts.SingleBranch, "*:") {
		return fmt.Errorf("invalid branch name %q", opts.SingleBranch)
	}
	if err := initBareInCluster(ctx, to, defaultBranch, objectFormat); err != nil {
		return err
	}

	var refs []string
	if opts.SingleBranch != "" {
		// Named even when absent: the fetch fails as the clone would.
		refs = []string{git.BranchPrefix + opts.SingleBranch}
	} else {
		heads, err := listRefs(ctx, from, git.BranchPrefix)
		if err != nil {
			return err
		}
		refs = heads
	}
	tags, err := listRefs(ctx, from, git.TagPrefix)
	if err != nil {
		return err
	}
	return fetchRefsInBatches(ctx, from, to, append(refs, tags...), 10*time.Minute)
}

// stageForkBase reads, in a cluster, the base's directory on this node
// into a staging copy, outside the repository root and so not replicated,
// and returns its path and its removal. The fork's refs come from that
// copy (copyForkInCluster), never from the base's directory itself.
//
// The fork read the base's directory by the name the request loaded at its
// start, past the one guard RepoAssignment made, and fetched from it right
// into the fork, whose refs, and the objects they carry, each fetch sends
// to the other members. On a node behind the cluster, the directory showed
// the base from before a push another node acknowledged: the fork started
// from the commit before it, on every member and for good. Or it held
// another repository, a private one, renamed under the base's name and
// projected on this node meanwhile: its history went into the fork, public
// as its base, on every member (#283, as #261 for a generation).
//
// The fork now first waits for this node to serve the base at the commit
// index the cluster confirms at the call (clusterhook.RequireFresh), as a
// generation from a template does. The base's directory is then read once,
// by `git clone --bare` into the staging copy, between two checks that it
// holds the base, and nothing else (readSourceDirectory). Every refusal
// wraps clusterhook.ErrBehind and comes before anything of the fork is
// written, its row, its key or any ref: what the fork takes from this
// copy, batch by batch, was read and checked whole before. The copy takes
// what `git clone --bare` would, the base's branches and tags.
//
// Without a cluster it does not read the base: the fork clones the base's
// directory itself, as before, and the path is empty.
func stageForkBase(ctx context.Context, base *repo_model.Repository) (string, func(), error) {
	if err := clusterhook.RequireFresh(ctx, clusterhook.RepoKey(base.OwnerName, base.Name, false)); err != nil {
		return "", nil, err
	}
	if !clusterhook.Enabled() {
		return "", func() {}, nil
	}
	staging, err := repo_module.CreateTemporaryPath("fork")
	if err != nil {
		return "", nil, err
	}
	remove := func() {
		if err := repo_module.RemoveTemporaryPath(staging); err != nil {
			log.Error("Failed to remove the staging copy of the fork's base %s: %v", base.FullName(), err)
		}
	}
	from := base.RepoPath()
	if err := readSourceDirectory(ctx, base, "base repository", forkRefused, func() error {
		if stdout, _, err := git.NewCommand(ctx, "clone", "--bare", "--quiet").AddDynamicArguments(from, staging).
			SetDescription(fmt.Sprintf("stageForkBase(git clone): %s", base.FullName())).
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

// forkRefused is a refusal of the fork, wrapping clusterhook.ErrBehind: the
// caller retries.
func forkRefused(err error) error {
	return fmt.Errorf("%w: %w; no fork was created", clusterhook.ErrBehind, err)
}

// ForkRepositoryAndUpdates forks a repository. On success it updates metadata (size, stats, etc.) and send a notification.
func ForkRepositoryAndUpdates(ctx context.Context, doer, owner *user_model.User, opts ForkRepoOptions) (*repo_model.Repository, error) {
	repo, err := ForkRepositoryIfNotExists(ctx, doer, owner, opts)
	if err != nil {
		return nil, err
	}

	// even if below operations failed, it could be ignored. And they will be retried
	if err := repo_module.UpdateRepoSize(ctx, repo); err != nil {
		log.Error("Failed to update size for repository: %v", err)
	}
	if err := repo_model.CopyLanguageStat(ctx, opts.BaseRepo, repo); err != nil {
		log.Error("Copy language stat from oldRepo failed: %v", err)
	}

	gitRepo, err := gitrepo.OpenRepository(ctx, repo)
	if err != nil {
		log.Error("Open created git repository failed: %v", err)
	} else {
		defer gitRepo.Close()
		if err := repo_module.SyncReleasesWithTags(ctx, repo, gitRepo); err != nil {
			log.Error("Sync releases from git tags failed: %v", err)
		}
	}

	notify_service.ForkRepository(ctx, doer, opts.BaseRepo, repo)

	return repo, nil
}

// ConvertForkToNormalRepository convert the provided repo from a forked repo to normal repo
func ConvertForkToNormalRepository(ctx context.Context, repo *repo_model.Repository) error {
	err := db.WithTx(ctx, func(ctx context.Context) error {
		repo, err := repo_model.GetRepositoryByID(ctx, repo.ID)
		if err != nil {
			return err
		}

		if !repo.IsFork {
			return nil
		}

		if err := repo_model.DecrementRepoForkNum(ctx, repo.ForkID); err != nil {
			log.Error("Unable to decrement repo fork num for old root repo %d of repository %-v whilst converting from fork. Error: %v", repo.ForkID, repo, err)
			return err
		}

		repo.IsFork = false
		repo.ForkID = 0

		if err := repo_module.UpdateRepository(ctx, repo, false); err != nil {
			log.Error("Unable to update repository %-v whilst converting from fork. Error: %v", repo, err)
			return err
		}

		return nil
	})

	return err
}
