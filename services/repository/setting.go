// Copyright 2023 The Gitea Authors. All rights reserved.
// SPDX-License-Identifier: MIT

package repository

import (
	"context"
	"fmt"
	"slices"

	"forgejo.org/models/db"
	repo_model "forgejo.org/models/repo"
	"forgejo.org/models/unit"
	"forgejo.org/modules/clusterhook"
	"forgejo.org/modules/log"
	actions_service "forgejo.org/services/actions"
)

// UpdateRepositoryUnits updates a repository's units
func UpdateRepositoryUnits(ctx context.Context, repo *repo_model.Repository, units []repo_model.RepoUnit, deleteUnitTypes []unit.Type) (err error) {
	// Saving the Actions unit cleans the scheduled workflows registered,
	// then registers those of the default branch's head read in this
	// node's git. In a cluster, a node behind on the repository would
	// replace them for good with those of the head before the last push
	// another node acknowledged, which nothing detects again until the
	// next push to the default branch (#313). The save waits until this
	// node serves the repository at the commit index the cluster confirms
	// now, and is refused while it does not, before anything is written,
	// outside the transaction.
	if slices.ContainsFunc(units, func(u repo_model.RepoUnit) bool { return u.Type == unit.TypeActions }) {
		if err := requireFreshSchedules(ctx, repo); err != nil {
			return err
		}
	}

	ctx, committer, err := db.TxContext(ctx)
	if err != nil {
		return err
	}
	defer committer.Close()

	// Delete existing settings of units before adding again
	for _, u := range units {
		deleteUnitTypes = append(deleteUnitTypes, u.Type)
	}

	if slices.Contains(deleteUnitTypes, unit.TypeActions) {
		if err := actions_service.CleanRepoScheduleTasks(ctx, repo, true); err != nil {
			log.Error("CleanRepoScheduleTasks: %v", err)
		}
	}

	for _, u := range units {
		if u.Type == unit.TypeActions {
			if err := actions_service.DetectAndHandleSchedules(ctx, repo); err != nil {
				log.Error("DetectAndHandleSchedules: %v", err)
			}
			break
		}
	}

	if _, err = db.GetEngine(ctx).Where("repo_id = ?", repo.ID).In("type", deleteUnitTypes).Delete(new(repo_model.RepoUnit)); err != nil {
		return err
	}

	if len(units) > 0 {
		if err = db.Insert(ctx, units); err != nil {
			return err
		}
	}

	return committer.Commit()
}

// requireFreshSchedules waits until this node serves repo at the commit
// index the cluster confirms now, before the scheduled workflows of its
// default branch are read in this node's git and registered, and returns
// clusterhook.ErrBehind while it does not. Without a cluster it returns
// nil at once.
func requireFreshSchedules(ctx context.Context, repo *repo_model.Repository) error {
	return clusterhook.RequireFresh(ctx, clusterhook.RepoKey(repo.OwnerName, repo.Name, false))
}

// UnarchiveRepository lifts the archiving of repo, then registers the
// scheduled workflows of its default branch when its Actions unit is
// enabled, as archiving removed them. It returns clusterhook.ErrBehind,
// nothing written, on a node of a cluster that may be behind on repo.
func UnarchiveRepository(ctx context.Context, repo *repo_model.Repository) error {
	// The scheduled workflows are those of the default branch's head read
	// in this node's git. In a cluster, a node behind on the repository
	// would register those of the head before the last push another node
	// acknowledged, or none, which nothing detects again until the next
	// push to the default branch (#313). The repository stays archived
	// while this node does not serve it at the commit index the cluster
	// confirms now.
	actions := repo.UnitEnabled(ctx, unit.TypeActions)
	if actions {
		if err := requireFreshSchedules(ctx, repo); err != nil {
			return err
		}
	}

	if err := repo_model.SetArchiveRepoState(ctx, repo, false); err != nil {
		return fmt.Errorf("SetArchiveRepoState: %w", err)
	}

	if actions {
		if err := actions_service.DetectAndHandleSchedules(ctx, repo); err != nil {
			log.Error("DetectAndHandleSchedules for un-archived repo %s/%s: %v", repo.OwnerName, repo.Name, err)
		}
	}
	return nil
}
