// Copyright 2020 The Gitea Authors. All rights reserved.
// SPDX-License-Identifier: MIT

package repository

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"forgejo.org/models/db"
	repo_model "forgejo.org/models/repo"
	system_model "forgejo.org/models/system"
	"forgejo.org/modules/clusterhook"
	"forgejo.org/modules/git"
	"forgejo.org/modules/log"
	repo_module "forgejo.org/modules/repository"
	"forgejo.org/modules/setting"
	"forgejo.org/modules/util"

	"xorm.io/builder"
)

// GitFsckRepos calls 'git fsck' to check repository health.
func GitFsckRepos(ctx context.Context, timeout time.Duration, args git.TrustedCmdArgs) error {
	log.Trace("Doing: GitFsck")

	if err := db.Iterate(
		ctx,
		builder.Expr("id>0 AND is_fsck_enabled=?", true),
		func(ctx context.Context, repo *repo_model.Repository) error {
			select {
			case <-ctx.Done():
				return db.ErrCancelledf("before fsck of %s", repo.FullName())
			default:
			}
			return GitFsckRepo(ctx, repo, timeout, args)
		},
	); err != nil {
		log.Trace("Error: GitFsck: %v", err)
		return err
	}

	log.Trace("Finished: GitFsck")
	return nil
}

// GitFsckRepo calls 'git fsck' to check an individual repository's health.
func GitFsckRepo(ctx context.Context, repo *repo_model.Repository, timeout time.Duration, args git.TrustedCmdArgs) error {
	log.Trace("Running health check on repository %-v", repo)
	repoPath := repo.RepoPath()
	if err := git.Fsck(ctx, repoPath, timeout, args); err != nil {
		log.Warn("Failed to health check repository (%-v): %v", repo, err)
		if err = system_model.CreateRepositoryNotice("Failed to health check repository (%s): %v", repo.FullName(), err); err != nil {
			log.Error("CreateRepositoryNotice: %v", err)
		}
	}
	return nil
}

// GitGcRepos calls 'git gc' to remove unnecessary files and optimize the local repository
func GitGcRepos(ctx context.Context, timeout time.Duration, args git.TrustedCmdArgs) error {
	log.Trace("Doing: GitGcRepos")

	if err := db.Iterate(
		ctx,
		builder.Gt{"id": 0},
		func(ctx context.Context, repo *repo_model.Repository) error {
			select {
			case <-ctx.Done():
				return db.ErrCancelledf("before GC of %s", repo.FullName())
			default:
			}
			// we can ignore the error here because it will be logged in GitGCRepo
			_ = GitGcRepo(ctx, repo, timeout, args)
			return nil
		},
	); err != nil {
		return err
	}

	log.Trace("Finished: GitGcRepos")
	return nil
}

// clusterGCArgs keeps unreachable objects for git's default expiry at least:
// in a cluster, objects arrive on a member before the ref that names them
// commits, shipped to a majority that this node counts in, and the gc gate
// is read once before a gc that may run for minutes. Pruned meanwhile, the
// committed ref would name objects this node told the proposer it holds.
// A --prune with any other value than never is dropped (now, all, or an
// age as short as one second), and gc.pruneExpire, which [git.config] may
// set, is passed explicitly: git's default, or never when configured so.
func clusterGCArgs(args git.TrustedCmdArgs) (config, gc git.TrustedCmdArgs) {
	out := make(git.TrustedCmdArgs, 0, len(args))
	for _, a := range args {
		if v, ok := strings.CutPrefix(string(a), "--prune="); ok && v != "never" {
			log.Warn("git gc: %s is ignored in a cluster: objects shipped for a write not yet committed would be pruned", a)
			continue
		}
		out = append(out, a)
	}
	config = git.TrustedCmdArgs{"-c", "gc.pruneExpire=2.weeks.ago"}
	for k, v := range setting.GitConfig.Options {
		if strings.EqualFold(k, "gc.pruneExpire") && v == "never" {
			config = git.TrustedCmdArgs{"-c", "gc.pruneExpire=never"}
		}
	}
	return config, out
}

// GitGcRepo calls 'git gc' to remove unnecessary files and optimize the local repository
func GitGcRepo(ctx context.Context, repo *repo_model.Repository, timeout time.Duration, args git.TrustedCmdArgs) error {
	// In a cluster the projector owns garbage collection: no gc while the
	// repository is not projected or still has unconsumed events, whose
	// objects are pinned (kitsunium ADR 0152 D19, R8-27).
	if !clusterhook.MayCollectGarbage(clusterhook.RepoKey(repo.OwnerName, repo.Name, false)) {
		log.Info("Skipping git gc on %-v: the cluster has work pending on it", repo)
		return nil
	}
	command := git.NewCommand(ctx)
	if setting.Cluster.Enabled {
		var config git.TrustedCmdArgs
		config, args = clusterGCArgs(args)
		command.AddArguments(config...)
	}
	if setting.Cluster.Enabled || clusterhook.Enabled() {
		// gc packs the refs (pack-refs --all --prune), which runs the
		// reference-transaction hook: one transaction writing each packed
		// ref into packed-refs, then one deleting each pruned loose ref.
		// The hook would propose those deletions, and every member would
		// delete every branch and tag (#72). Packing changes no ref's
		// value, and git's locks guard it from concurrent writes: the
		// cluster is left out of it, as the projector's own git is.
		command.AddArguments("-c", "core.hooksPath=/dev/null")
	}
	log.Trace("Running git gc on %-v", repo)
	command.AddArguments("gc").AddArguments(args...).
		SetDescription(fmt.Sprintf("Repository Garbage Collection: %s", repo.FullName()))
	var stdout string
	var err error
	stdout, _, err = command.RunStdString(&git.RunOpts{Timeout: timeout, Dir: repo.RepoPath()})
	if err != nil {
		log.Error("Repository garbage collection failed for %-v. Stdout: %s\nError: %v", repo, stdout, err)
		desc := fmt.Sprintf("Repository garbage collection failed for %s. Stdout: %s\nError: %v", repo.RepoPath(), stdout, err)
		if err := system_model.CreateRepositoryNotice(desc); err != nil {
			log.Error("CreateRepositoryNotice: %v", err)
		}
		return fmt.Errorf("Repository garbage collection failed in repo: %s: Error: %w", repo.FullName(), err)
	}

	// Now update the size of the repository
	if err := repo_module.UpdateRepoSize(ctx, repo); err != nil {
		log.Error("Updating size as part of garbage collection failed for %-v. Stdout: %s\nError: %v", repo, stdout, err)
		desc := fmt.Sprintf("Updating size as part of garbage collection failed for %s. Stdout: %s\nError: %v", repo.RepoPath(), stdout, err)
		if err := system_model.CreateRepositoryNotice(desc); err != nil {
			log.Error("CreateRepositoryNotice: %v", err)
		}
		return fmt.Errorf("Updating size as part of garbage collection failed in repo: %s: Error: %w", repo.FullName(), err)
	}

	return nil
}

func gatherMissingRepoRecords(ctx context.Context) (repo_model.RepositoryList, error) {
	repos := make([]*repo_model.Repository, 0, 10)
	if err := db.Iterate(
		ctx,
		builder.Gt{"id": 0},
		func(ctx context.Context, repo *repo_model.Repository) error {
			select {
			case <-ctx.Done():
				return db.ErrCancelledf("during gathering missing repo records before checking %s", repo.FullName())
			default:
			}
			isDir, err := util.IsDir(repo.RepoPath())
			if err != nil {
				return fmt.Errorf("Unable to check dir for %s. %w", repo.FullName(), err)
			}
			if !isDir {
				repos = append(repos, repo)
			}
			return nil
		},
	); err != nil {
		if strings.HasPrefix(err.Error(), "Aborted gathering missing repo") {
			return nil, err
		}
		if err2 := system_model.CreateRepositoryNotice("gatherMissingRepoRecords: %v", err); err2 != nil {
			log.Error("CreateRepositoryNotice: %v", err2)
		}
		return nil, err
	}
	return repos, nil
}

// ErrMissingReposInCluster refuses the maintenance of repositories whose
// Git files are missing on a node of a cluster.
var ErrMissingReposInCluster = errors.New("in a cluster, a repository missing from this node's disk is not missing: the cluster projects it again from the other members. Deleting or reinitializing it here would act on every member (forgejo cluster reconcile compares the database with the cluster instead)")

// missingReposRefused refuses, on a node of a cluster, to judge a
// repository missing from the local disk alone: a member that lags, was
// rebuilt on an empty disk or has not projected a new repository yet would
// delete it, or rewrite its HEAD, on every member (#51).
func missingReposRefused() error {
	if setting.Cluster.Enabled || clusterhook.Enabled() {
		return ErrMissingReposInCluster
	}
	return nil
}

// DeleteMissingRepositories deletes all repository records that lost Git files.
func DeleteMissingRepositories(ctx context.Context) error {
	if err := missingReposRefused(); err != nil {
		return err
	}
	repos, err := gatherMissingRepoRecords(ctx)
	if err != nil {
		return err
	}

	if len(repos) == 0 {
		return nil
	}

	for _, repo := range repos {
		select {
		case <-ctx.Done():
			return db.ErrCancelledf("during DeleteMissingRepositories before %s", repo.FullName())
		default:
		}
		log.Trace("Deleting %d/%d...", repo.OwnerID, repo.ID)
		if err := DeleteRepositoryDirectly(ctx, repo.ID, DeleteRepositoryOpts{}); err != nil {
			log.Error("Failed to DeleteRepository %-v: Error: %v", repo, err)
			if err2 := system_model.CreateRepositoryNotice("Failed to DeleteRepository %s [%d]: Error: %v", repo.FullName(), repo.ID, err); err2 != nil {
				log.Error("CreateRepositoryNotice: %v", err)
			}
		}
	}
	return nil
}

// ReinitMissingRepositories reinitializes all repository records that lost Git files.
func ReinitMissingRepositories(ctx context.Context) error {
	if err := missingReposRefused(); err != nil {
		return err
	}
	repos, err := gatherMissingRepoRecords(ctx)
	if err != nil {
		return err
	}

	if len(repos) == 0 {
		return nil
	}

	for _, repo := range repos {
		select {
		case <-ctx.Done():
			return db.ErrCancelledf("during ReinitMissingRepositories before %s", repo.FullName())
		default:
		}
		log.Trace("Initializing %d/%d...", repo.OwnerID, repo.ID)
		if err := git.InitRepository(ctx, repo.RepoPath(), true, repo.ObjectFormatName); err != nil {
			log.Error("Unable (re)initialize repository %d at %s. Error: %v", repo.ID, repo.RepoPath(), err)
			if err2 := system_model.CreateRepositoryNotice("InitRepository [%d]: %v", repo.ID, err); err2 != nil {
				log.Error("CreateRepositoryNotice: %v", err2)
			}
		}
	}
	return nil
}
