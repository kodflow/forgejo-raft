// Copyright 2019 The Gitea Authors. All rights reserved.
// SPDX-License-Identifier: MIT

package repository

import (
	"context"
	"fmt"

	"forgejo.org/models/db"
	git_model "forgejo.org/models/git"
	issues_model "forgejo.org/models/issues"
	repo_model "forgejo.org/models/repo"
	user_model "forgejo.org/models/user"
	"forgejo.org/modules/clusterhook"
	"forgejo.org/modules/log"
	notify_service "forgejo.org/services/notify"
)

// GenerateIssueLabels generates issue labels from a template repository
func GenerateIssueLabels(ctx context.Context, templateRepo, generateRepo *repo_model.Repository) error {
	templateLabels, err := issues_model.GetLabelsByRepoID(ctx, templateRepo.ID, "", db.ListOptions{})
	if err != nil {
		return err
	}
	// Prevent insert being called with an empty slice which would result in
	// err "no element on slice when insert".
	if len(templateLabels) == 0 {
		return nil
	}

	newLabels := make([]*issues_model.Label, 0, len(templateLabels))
	for _, templateLabel := range templateLabels {
		newLabels = append(newLabels, &issues_model.Label{
			RepoID:      generateRepo.ID,
			Name:        templateLabel.Name,
			Exclusive:   templateLabel.Exclusive,
			Description: templateLabel.Description,
			Color:       templateLabel.Color,
		})
	}
	return db.Insert(ctx, newLabels)
}

func GenerateProtectedBranch(ctx context.Context, templateRepo, generateRepo *repo_model.Repository) error {
	templateBranches, err := git_model.FindRepoProtectedBranchRules(ctx, templateRepo.ID)
	if err != nil {
		return err
	}
	// Prevent insert being called with an empty slice which would result in
	// err "no element on slice when insert".
	if len(templateBranches) == 0 {
		return nil
	}

	newBranches := make([]*git_model.ProtectedBranch, 0, len(templateBranches))
	for _, templateBranch := range templateBranches {
		templateBranch.ID = 0
		templateBranch.RepoID = generateRepo.ID
		templateBranch.UpdatedUnix = 0
		templateBranch.CreatedUnix = 0
		newBranches = append(newBranches, templateBranch)
	}
	return db.Insert(ctx, newBranches)
}

// clusterTemplateRefusal refuses, in a cluster, a generation that reads the
// template's directory on this node, for its git content or its git hooks,
// while this node may not serve the template as the cluster holds it. The
// template is loaded by its ID, by the web form outside RepoAssignment and
// its guard (#82): the directory under its name may still hold another
// repository, a private one renamed away whose rename this node has not
// projected yet, whose tree the generation committed into a new repository
// of the requester, replicated to every member. Or it may show the
// template's branches from before a push another node acknowledged, on
// which the new repository was built for good (#261). The generation first
// waits for this node to serve the template at the commit index the cluster
// confirms at the call, as the services that write from this node's git do
// (#189, #226, #239), then checks that the directory holds the template, as
// git's reads do (templateReadRefusal). Either refusal wraps
// clusterhook.ErrBehind and comes before anything is written: the caller
// retries. A generation that reads only SQL is not refused. Without a
// cluster it returns nil.
func clusterTemplateRefusal(ctx context.Context, templateRepo *repo_model.Repository, opts GenerateRepoOptions) error {
	readsDirectory := (opts.GitContent && !templateRepo.IsEmpty) || opts.GitHooks
	if !readsDirectory {
		return nil
	}
	if err := clusterhook.RequireFresh(ctx, clusterhook.RepoKey(templateRepo.OwnerName, templateRepo.Name, false)); err != nil {
		return err
	}
	return templateReadRefusal(templateRepo)
}

// templateReadRefusal refuses, wrapping clusterhook.ErrBehind, a read of the
// template's directory on this node while it holds another repository than
// the template (#82, #261). The generation asks it before anything is
// written; the read itself is checked again around git's
// (readTemplateDirectory). Without a cluster it returns nil.
func templateReadRefusal(templateRepo *repo_model.Repository) error {
	if err := clusterhook.ReadRefusal(clusterhook.RepoKey(templateRepo.OwnerName, templateRepo.Name, false), templateRepo.ID); err != nil {
		return templateRefused(err)
	}
	return nil
}

// templateRefused is a refusal of the generation, wrapping
// clusterhook.ErrBehind: the caller retries.
func templateRefused(err error) error {
	return fmt.Errorf("%w: %w; no repository was generated", clusterhook.ErrBehind, err)
}

// readTemplateDirectory runs read, which reads the template's directory on
// this node, git's clone of its content or its git hooks, between two
// checks that it holds the template, and nothing else (#261): see
// readSourceDirectory. A refusal wraps clusterhook.ErrBehind, and the new
// repository is removed as for any failed generation (#102).
func readTemplateDirectory(ctx context.Context, templateRepo *repo_model.Repository, read func() error) error {
	return readSourceDirectory(ctx, templateRepo, "template", templateRefused, read)
}

// readSourceDirectory runs read, which reads the directory on this node of
// source, the repository a write copies from (a template's, a fork's base),
// between two checks that it holds source, and nothing else (#261, #283).
// A check made before anything was written leaves the time the write takes
// until git reads, and nothing keeps the projector's moves out of the
// directory while git reads it: source renamed away and another
// repository, a private one, renamed or created under its name, both
// projected right after the last check, and git read that one's directory,
// whose content the write copied into a repository replicated to every
// member.
//
// Right before the read, source's row must still give the directory's
// name, and the directory must hold source (clusterhook.CheckRead). Once
// git is done, before anything read leaves the caller, the directory must
// still hold it, no rename, transfer or deletion that took source's key or
// freed it may lie above the index this node had projected the key at when
// the read began, even one undone since, and the row must still give the
// name. Any of them refuses the write with refused, which wraps
// clusterhook.ErrBehind: the caller retries. noun names source in the
// refusal. The checks are clusterhook.ReadBetweenChecks, which a push
// mirror's sync shares (#294). Without a cluster it only runs read.
func readSourceDirectory(ctx context.Context, source *repo_model.Repository, noun string, refused func(error) error, read func() error) error {
	key := clusterhook.RepoKey(source.OwnerName, source.Name, false)
	return clusterhook.ReadBetweenChecks(key, source.ID, func() error {
		return sourceStillNamed(ctx, source, key, noun, refused)
	}, refused, read)
}

// sourceStillNamed refuses with refused, which wraps clusterhook.ErrBehind,
// a write from source whose row no longer gives key: renamed, transferred
// or deleted, it or its owner, since it was loaded. Another repository may
// take the name meanwhile, and then its directory on this node (#261).
func sourceStillNamed(ctx context.Context, source *repo_model.Repository, key, noun string, refused func(error) error) error {
	current, err := repo_model.GetRepositoryByID(ctx, source.ID)
	if repo_model.IsErrRepoNotExist(err) {
		return refused(fmt.Errorf("the %s %s was deleted", noun, key))
	}
	if err != nil {
		return err
	}
	if now := clusterhook.RepoKey(current.OwnerName, current.Name, false); now != key {
		return refused(fmt.Errorf("the %s %s is %s now", noun, key, now))
	}
	return nil
}

// GenerateRepository generates a repository from a template
func GenerateRepository(ctx context.Context, doer, owner *user_model.User, templateRepo *repo_model.Repository, opts GenerateRepoOptions) (_ *repo_model.Repository, err error) {
	if !doer.IsAdmin && !owner.CanCreateRepo() {
		return nil, repo_model.ErrReachLimitOfRepo{
			Limit: owner.MaxRepoCreation,
		}
	}

	// In a cluster, a name whose key the cluster holds is refused before
	// anything is written, as a creation's (#245).
	if err := clusterCreateRefusal(ctx, owner, opts.Name); err != nil {
		return nil, err
	}
	// And so is a generation that would read the template's directory on
	// a node that does not serve the template yet (#261).
	if err := clusterTemplateRefusal(ctx, templateRepo, opts); err != nil {
		return nil, err
	}

	var generateRepo *repo_model.Repository
	// initialized is set once git may have written the repository: a
	// failed generation then removes it, from the cluster's table too,
	// as a failed creation does (#102).
	initialized := false

	// The generated refs and the repository's row fall on the same side
	// of a checkpoint, as a creation's do (#159).
	releaseCut, err := clusterhook.HoldCut(ctx)
	if err != nil {
		return nil, err
	}
	defer releaseCut()
	if err = db.WithTx(ctx, func(ctx context.Context) error {
		generateRepo, err = generateRepository(ctx, doer, owner, templateRepo, opts)
		// A repository is returned once its directory was found absent and
		// `git init` ran; files found in place are never this call's.
		initialized = generateRepo != nil && !repo_model.IsErrRepoFilesAlreadyExist(err)
		if err != nil {
			return err
		}

		// Git Content, read from the template's directory between two
		// checks (readTemplateDirectory, #261).
		if opts.GitContent && !templateRepo.IsEmpty {
			if err = GenerateGitContent(ctx, templateRepo, generateRepo); err != nil {
				return err
			}
		}

		// Topics
		if opts.Topics {
			if err = repo_model.GenerateTopics(ctx, templateRepo, generateRepo); err != nil {
				return err
			}
		}

		// Git Hooks, read from the template's directory between two
		// checks, and written once the second one passed (#261).
		if opts.GitHooks {
			if err = GenerateGitHooks(ctx, templateRepo, generateRepo); err != nil {
				return err
			}
		}

		// Webhooks
		if opts.Webhooks {
			if err = GenerateWebhooks(ctx, templateRepo, generateRepo); err != nil {
				return err
			}
		}

		// Avatar
		if opts.Avatar && len(templateRepo.Avatar) > 0 {
			if err = generateAvatar(ctx, templateRepo, generateRepo); err != nil {
				return err
			}
		}

		// Issue Labels
		if opts.IssueLabels {
			if err = GenerateIssueLabels(ctx, templateRepo, generateRepo); err != nil {
				return err
			}
		}

		if opts.ProtectedBranch {
			if err = GenerateProtectedBranch(ctx, templateRepo, generateRepo); err != nil {
				return err
			}
		}

		return nil
	}); err != nil {
		if initialized {
			if errDelete := removeAbandonedRepo(ctx, owner.Name, generateRepo.Name, err); errDelete != nil {
				log.Error("GenerateRepository: %v", errDelete)
				return nil, fmt.Errorf("%w; and the repository's files were not removed: %v", err, errDelete)
			}
		}
		return nil, err
	}

	notify_service.CreateRepository(ctx, doer, owner, generateRepo)

	return generateRepo, nil
}
