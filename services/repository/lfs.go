// Copyright 2022 The Gitea Authors. All rights reserved.
// SPDX-License-Identifier: MIT

package repository

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
	"time"

	git_model "forgejo.org/models/git"
	repo_model "forgejo.org/models/repo"
	"forgejo.org/modules/clusterhook"
	"forgejo.org/modules/git"
	"forgejo.org/modules/gitrepo"
	"forgejo.org/modules/lfs"
	"forgejo.org/modules/log"
	"forgejo.org/modules/setting"
	"forgejo.org/modules/timeutil"
)

// GarbageCollectLFSMetaObjectsOptions provides options for GarbageCollectLFSMetaObjects function
type GarbageCollectLFSMetaObjectsOptions struct {
	LogDetail               func(format string, v ...any)
	AutoFix                 bool
	OlderThan               time.Time
	UpdatedLessRecentlyThan time.Time
}

// GarbageCollectLFSMetaObjects garbage collects LFS objects for all repositories
func GarbageCollectLFSMetaObjects(ctx context.Context, opts GarbageCollectLFSMetaObjectsOptions) error {
	log.Trace("Doing: GarbageCollectLFSMetaObjects")
	defer log.Trace("Finished: GarbageCollectLFSMetaObjects")

	if opts.LogDetail == nil {
		opts.LogDetail = log.Debug
	}

	if !setting.LFS.StartServer {
		opts.LogDetail("LFS support is disabled")
		return nil
	}
	if err := lfsCollectionRefused(opts.AutoFix); err != nil {
		return err
	}
	if setting.Cluster.Enabled && !clusterhook.Enabled() {
		opts.LogDetail("This process runs no replicator of the cluster: the orphans reported below are judged from this node's git alone, which may lag behind the cluster")
	}

	return git_model.IterateRepositoryIDsWithLFSMetaObjects(ctx, func(ctx context.Context, repoID, count int64) error {
		repo, err := repo_model.GetRepositoryByID(ctx, repoID)
		if err != nil {
			return err
		}

		return GarbageCollectLFSMetaObjectsForRepo(ctx, repo, opts)
	})
}

// ErrLFSCollectionWithoutReplicator refuses the deletions of the LFS
// garbage collection in a process of a cluster node that runs no
// replicator, `forgejo doctor` among them.
var ErrLFSCollectionWithoutReplicator = errors.New("in a cluster, a pointer missing from this node's git does not make an LFS object orphaned: a node behind the cluster's last writes, stopped for a while, or projecting a repository again lacks it, and deleting the object's row and file would delete it for every member. This process runs no replicator that could tell, and deletes nothing: run the gc_lfs cron task instead (\"Garbage collect LFS meta objects\" in Site administration > Monitoring > Cron, or [cron.gc_lfs]), on a running node, which skips the repositories that node does not serve at the cluster's last writes")

// lfsCollectionRefused refuses, in a process of a cluster node that runs
// no replicator, to delete what the collection finds orphaned: nothing
// there knows whether this node's git holds a repository as the cluster
// does, and clusterhook's gates answer yes without a replicator (#349).
// A report, which deletes nothing, is let through.
func lfsCollectionRefused(autoFix bool) error {
	if autoFix && setting.Cluster.Enabled && !clusterhook.Enabled() {
		return ErrLFSCollectionWithoutReplicator
	}
	return nil
}

// lfsRepoUnsettled says why this node must not judge the LFS objects of a
// repository from its git now, or "". In a cluster the projector may have
// the repository's directory in the making (projected again: initialized
// first, its objects fetched after), objects pinned by an unconsumed event,
// or the node may be behind the writes the cluster acknowledged: a pointer
// missing here is then not missing, and the deletion of the object's row
// and file, in the SQL and the storage every member shares, would lose it
// for the whole cluster (#349). Without a cluster both gates answer at
// once, and nothing is skipped.
func lfsRepoUnsettled(ctx context.Context, key string) string {
	if !clusterhook.MayCollectGarbage(key) {
		return "the cluster has work pending on it on this node"
	}
	if err := clusterhook.RequireFresh(ctx, key); err != nil {
		return err.Error()
	}
	return ""
}

// errLFSPassEnded ends a repository's pass early; the objects not examined
// yet are kept.
var errLFSPassEnded = errors.New("LFS garbage collection pass ended")

// lfsPointerInGit looks the pointer blob sha up in gitRepo: true when git
// holds it, false only when git answers that it is missing, and an error
// when git gave no answer (a cat-file that failed to start or died, a
// cancelled context), which tells nothing about the object.
func lfsPointerInGit(gitRepo *git.Repository, sha string) (bool, error) {
	var found bool
	err := gitRepo.WithCatFileBatchCheck(gitRepo.Ctx, func(wr io.Writer, rd *bufio.Reader) error {
		if _, err := wr.Write([]byte(sha + "\n")); err != nil {
			return err
		}
		got, typ, _, err := git.ReadBatchLine(rd)
		switch {
		case err == nil && string(got) == sha:
			found = true
			return nil
		case git.IsErrNotExist(err) && string(got) == sha && strings.TrimSpace(typ) == "missing":
			// "<sha> missing": the whole answer was read, the batch
			// stays usable.
			return nil
		case err == nil:
			return fmt.Errorf("git cat-file answered %q for %s", got, sha)
		default:
			return err
		}
	})
	return found, err
}

// GarbageCollectLFSMetaObjectsForRepo garbage collects LFS objects for a specific repository
func GarbageCollectLFSMetaObjectsForRepo(ctx context.Context, repo *repo_model.Repository, opts GarbageCollectLFSMetaObjectsOptions) error {
	opts.LogDetail("Checking %s", repo.FullName())
	if err := lfsCollectionRefused(opts.AutoFix); err != nil {
		return err
	}
	key := clusterhook.RepoKey(repo.OwnerName, repo.Name, false)
	if why := lfsRepoUnsettled(ctx, key); why != "" {
		log.Info("Skipping LFS garbage collection of %-v: %s", repo, why)
		opts.LogDetail("Skipping %s on this node of the cluster: %s", repo.FullName(), why)
		return nil
	}
	total, orphaned, collected, deleted := int64(0), 0, 0, 0
	defer func() {
		if orphaned == 0 {
			opts.LogDetail("Found %d total LFSMetaObjects in %s", total, repo.FullName())
		} else if !opts.AutoFix {
			opts.LogDetail("Found %d/%d orphaned LFSMetaObjects in %s", orphaned, total, repo.FullName())
		} else {
			opts.LogDetail("Collected %d/%d orphaned/%d total LFSMetaObjects in %s. %d removed from storage.", collected, orphaned, total, repo.FullName(), deleted)
		}
	}()

	gitRepo, err := gitrepo.OpenRepository(ctx, repo)
	if err != nil {
		log.Error("Unable to open git repository %s: %v", repo.FullName(), err)
		return err
	}
	defer gitRepo.Close()

	store := lfs.NewContentStore()
	objectFormat := git.ObjectFormatFromName(repo.ObjectFormatName)

	var ended string
	err = git_model.IterateLFSMetaObjectsForRepo(ctx, repo.ID, func(ctx context.Context, metaObject *git_model.LFSMetaObject) error {
		total++
		pointerSha := git.ComputeBlobHash(objectFormat, []byte(metaObject.StringContent()))

		found, err := lfsPointerInGit(gitRepo, pointerSha.String())
		if err != nil {
			// Unknown is not absent: the object is kept, and so are
			// the ones after it, which the same git would not answer
			// for either.
			ended = fmt.Sprintf("git could not look up the pointer of %s: %v", metaObject.Oid, err)
			return errLFSPassEnded
		}
		if found {
			return git_model.MarkLFSMetaObject(ctx, metaObject.ID)
		}
		orphaned++

		if !opts.AutoFix {
			return nil
		}
		// The gate again, right before the deletion: the projector may
		// have taken the repository up since the pass began (#349).
		if !clusterhook.MayCollectGarbage(key) {
			ended = "the cluster has work pending on it on this node"
			return errLFSPassEnded
		}
		// Non-existent pointer file
		_, err = git_model.RemoveLFSMetaObjectByOidFn(ctx, repo.ID, metaObject.Oid, func(count int64) error {
			if count > 0 {
				return nil
			}

			if err := store.Delete(metaObject.RelativePath()); err != nil {
				log.Error("Unable to remove lfs metaobject %s from store: %v", metaObject.Oid, err)
			}
			deleted++
			return nil
		})
		if err != nil {
			return fmt.Errorf("unable to remove meta-object %s in %s: %w", metaObject.Oid, repo.FullName(), err)
		}
		collected++

		return nil
	}, &git_model.IterateLFSMetaObjectsForRepoOptions{
		// Only attempt to garbage collect lfs meta objects older than a week as the order of git lfs upload
		// and git object upload is not necessarily guaranteed. It's possible to imagine a situation whereby
		// an LFS object is uploaded but the git branch is not uploaded immediately, or there are some rapid
		// changes in new branches that might lead to lfs objects becoming temporarily unassociated with git
		// objects.
		//
		// It is likely that a week is potentially excessive but it should definitely be enough that any
		// unassociated LFS object is genuinely unassociated.
		OlderThan:               timeutil.TimeStamp(opts.OlderThan.Unix()),
		UpdatedLessRecentlyThan: timeutil.TimeStamp(opts.UpdatedLessRecentlyThan.Unix()),
	})
	if errors.Is(err, errLFSPassEnded) {
		log.Warn("LFS garbage collection of %-v stopped, the objects left kept: %s", repo, ended)
		opts.LogDetail("Stopped in %s, the objects left kept: %s", repo.FullName(), ended)
		return nil
	}
	if err != nil {
		return err
	}
	return nil
}
