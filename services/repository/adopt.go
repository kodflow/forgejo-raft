// Copyright 2020 The Gitea Authors. All rights reserved.
// SPDX-License-Identifier: MIT

package repository

import (
	"context"
	"fmt"
	"os"
	"path"
	"path/filepath"
	"strconv"
	"strings"

	"forgejo.org/models/db"
	git_model "forgejo.org/models/git"
	repo_model "forgejo.org/models/repo"
	user_model "forgejo.org/models/user"
	"forgejo.org/modules/clusterhook"
	"forgejo.org/modules/container"
	"forgejo.org/modules/git"
	"forgejo.org/modules/gitrepo"
	"forgejo.org/modules/log"
	"forgejo.org/modules/optional"
	repo_module "forgejo.org/modules/repository"
	"forgejo.org/modules/setting"
	"forgejo.org/modules/util"
	notify_service "forgejo.org/services/notify"

	"github.com/gobwas/glob"
)

// AdoptRepository adopts pre-existing repository files for the user/organization.
func AdoptRepository(ctx context.Context, doer, u *user_model.User, opts CreateRepoOptions) (*repo_model.Repository, error) {
	if !doer.IsAdmin && !u.CanCreateRepo() {
		return nil, repo_model.ErrReachLimitOfRepo{
			Limit: u.MaxRepoCreation,
		}
	}

	repo := &repo_model.Repository{
		OwnerID:                         u.ID,
		Owner:                           u,
		OwnerName:                       u.Name,
		Name:                            opts.Name,
		LowerName:                       strings.ToLower(opts.Name),
		Description:                     opts.Description,
		OriginalURL:                     opts.OriginalURL,
		OriginalServiceType:             opts.GitServiceType,
		IsPrivate:                       opts.IsPrivate,
		IsFsckEnabled:                   !opts.IsMirror,
		CloseIssuesViaCommitInAnyBranch: setting.Repository.DefaultCloseIssuesViaCommitsInAnyBranch,
		Status:                          opts.Status,
		IsEmpty:                         !opts.AutoInit,
	}

	// In a cluster, the adopted repository's ID goes into the replicated
	// table during the transaction that commits its row: both fall on the
	// same side of a checkpoint (#167). Taken before the transaction opens,
	// never inside it: the checkpoint's exclusive cut would wait on it.
	releaseCut, err := clusterhook.HoldCut(ctx)
	if err != nil {
		return nil, err
	}
	defer releaseCut()

	if err := db.WithTx(ctx, func(ctx context.Context) error {
		repoPath := repo_model.RepoPath(u.Name, repo.Name)
		isExist, err := util.IsExist(repoPath)
		if err != nil {
			log.Error("Unable to check if %s exists. Error: %v", repoPath, err)
			return err
		}
		if !isExist {
			return repo_model.ErrRepoNotExist{
				OwnerName: u.Name,
				Name:      repo.Name,
			}
		}

		if err := repo_module.CreateRepositoryByExample(ctx, doer, u, repo, true, false); err != nil {
			return err
		}

		// Re-fetch the repository from database before updating it (else it would
		// override changes that were done earlier with sql)
		if repo, err = repo_model.GetRepositoryByID(ctx, repo.ID); err != nil {
			return fmt.Errorf("getRepositoryByID: %w", err)
		}

		if err := adoptRepository(ctx, repoPath, repo, opts.DefaultBranch); err != nil {
			return fmt.Errorf("adoptRepository: %w", err)
		}

		if err := repo_module.CheckDaemonExportOK(ctx, repo); err != nil {
			return fmt.Errorf("checkDaemonExportOK: %w", err)
		}

		// Initialize Issue Labels if selected
		if len(opts.IssueLabels) > 0 {
			if err := repo_module.InitializeLabels(ctx, repo.ID, opts.IssueLabels, false); err != nil {
				return fmt.Errorf("InitializeLabels: %w", err)
			}
		}

		if stdout, _, err := git.NewCommand(ctx, "update-server-info").
			SetDescription(fmt.Sprintf("CreateRepository(git update-server-info): %s", repoPath)).
			RunStdString(&git.RunOpts{Dir: repoPath}); err != nil {
			log.Error("CreateRepository(git update-server-info) in %v: Stdout: %s\nError: %v", repo, stdout, err)
			return fmt.Errorf("CreateRepository(git update-server-info): %w", err)
		}
		// Last, inside the transaction: when the cluster does not take the
		// ID, the row is rolled back with it and the repository stays
		// unadopted, to adopt again (#162).
		return bindAdoptedID(ctx, repo)
	}); err != nil {
		return nil, err
	}
	releaseCut()

	notify_service.AdoptRepository(ctx, doer, u, repo)

	return repo, nil
}

// bindAdoptedID gives the cluster the database ID of an adopted repository,
// and of its wiki when there is one. A directory the cluster replicated
// keeps the ID of the row it was written under, in the table and in every
// node's stamp, until a write carries another; reads and pushes are
// refused while the stamp names another ID than the database's (#82). The
// adoption's own writes carry none: a repository whose row SQL lost
// (failover, restore), adopted as `forgejo cluster reconcile` prescribes,
// was refused on every node for good (#150). HEAD is written again as it
// is, under the new ID, before the row is committed: a write the cluster
// refuses or does not answer (no quorum, a leader change, which often
// follows the failover) rolls the row back, and the repository stays
// unadopted, offered for adoption again. Committed first, the row left
// the repository refused on every node, no longer unadopted, with nothing
// left to bind it but its deletion (#162). A transaction that fails after
// the write leaves the stamps on an ID no row holds: the repository is
// unadopted, and its next adoption binds the next ID. The write carries
// no pusher, so the consumer derives nothing from it. Without a cluster
// it does nothing.
func bindAdoptedID(ctx context.Context, repo *repo_model.Repository) error {
	if !clusterhook.Enabled() {
		return nil
	}
	for _, wiki := range []bool{false, true} {
		dir := repo.RepoPath()
		if wiki {
			dir = repo.WikiPath()
		}
		if isDir, err := util.IsDir(dir); err != nil {
			return fmt.Errorf("bindAdoptedID: %w", err)
		} else if !isDir {
			continue
		}
		head, _, err := git.NewCommand(ctx, "symbolic-ref", "HEAD").RunStdString(&git.RunOpts{Dir: dir})
		if err != nil {
			return fmt.Errorf("bindAdoptedID(git symbolic-ref HEAD): %s: %w", dir, err)
		}
		env := append(os.Environ(),
			repo_module.EnvRepoID+"="+strconv.FormatInt(repo.ID, 10),
			repo_module.EnvRepoIsWiki+"="+strconv.FormatBool(wiki),
		)
		if _, _, err := git.NewCommand(ctx, "symbolic-ref", "HEAD").AddDynamicArguments(strings.TrimSpace(head)).
			SetDescription(fmt.Sprintf("bindAdoptedID(git symbolic-ref HEAD): %s", dir)).
			RunStdString(&git.RunOpts{Dir: dir, Env: env}); err != nil {
			return fmt.Errorf("the cluster did not take the ID %d for %s: the adoption is rolled back, adopt the repository again once the cluster has a leader: %w", repo.ID, dir, err)
		}
	}
	return nil
}

func adoptRepository(ctx context.Context, repoPath string, repo *repo_model.Repository, defaultBranch string) (err error) {
	isExist, err := util.IsExist(repoPath)
	if err != nil {
		log.Error("Unable to check if %s exists. Error: %v", repoPath, err)
		return err
	}
	if !isExist {
		return fmt.Errorf("adoptRepository: path does not already exist: %s", repoPath)
	}

	repo.IsEmpty = false

	if len(defaultBranch) > 0 {
		repo.DefaultBranch = defaultBranch

		if err = gitrepo.SetDefaultBranch(ctx, repo, repo.DefaultBranch); err != nil {
			return fmt.Errorf("setDefaultBranch: %w", err)
		}
	} else {
		repo.DefaultBranch, err = gitrepo.GetDefaultBranch(ctx, repo)
		if err != nil {
			repo.DefaultBranch = setting.Repository.DefaultBranch
			if err = gitrepo.SetDefaultBranch(ctx, repo, repo.DefaultBranch); err != nil {
				return fmt.Errorf("setDefaultBranch: %w", err)
			}
		}
	}

	// Don't bother looking this repo in the context it won't be there
	gitRepo, err := gitrepo.OpenRepository(ctx, repo)
	if err != nil {
		return fmt.Errorf("openRepository: %w", err)
	}
	defer gitRepo.Close()

	if _, err = repo_module.SyncRepoBranchesWithRepo(ctx, repo, gitRepo, 0); err != nil {
		return fmt.Errorf("SyncRepoBranchesWithRepo: %w", err)
	}

	if err = repo_module.SyncReleasesWithTags(ctx, repo, gitRepo); err != nil {
		return fmt.Errorf("SyncReleasesWithTags: %w", err)
	}

	branches, _ := git_model.FindBranchNames(ctx, git_model.FindBranchOptions{
		RepoID:          repo.ID,
		ListOptions:     db.ListOptionsAll,
		IsDeletedBranch: optional.Some(false),
	})

	found := false
	hasDefault := false
	hasMaster := false
	hasMain := false
	for _, branch := range branches {
		if branch == repo.DefaultBranch {
			found = true
			break
		} else if branch == setting.Repository.DefaultBranch {
			hasDefault = true
		} else if branch == "master" {
			hasMaster = true
		} else if branch == "main" {
			hasMain = true
		}
	}
	if !found {
		if hasDefault {
			repo.DefaultBranch = setting.Repository.DefaultBranch
		} else if hasMaster {
			repo.DefaultBranch = "master"
		} else if hasMain {
			repo.DefaultBranch = "main"
		} else if len(branches) > 0 {
			repo.DefaultBranch = branches[0]
		} else {
			repo.IsEmpty = true
			repo.DefaultBranch = setting.Repository.DefaultBranch
		}

		if err = gitrepo.SetDefaultBranch(ctx, repo, repo.DefaultBranch); err != nil {
			return fmt.Errorf("setDefaultBranch: %w", err)
		}
	}
	if err = repo_module.UpdateRepository(ctx, repo, false); err != nil {
		return fmt.Errorf("updateRepository: %w", err)
	}

	return nil
}

// DeleteUnadoptedRepository deletes unadopted repository files from the filesystem
func DeleteUnadoptedRepository(ctx context.Context, doer, u *user_model.User, repoName string) error {
	if err := repo_model.IsUsableRepoName(repoName); err != nil {
		return err
	}

	repoPath := repo_model.RepoPath(u.Name, repoName)
	isExist, err := util.IsExist(repoPath)
	if err != nil {
		log.Error("Unable to check if %s exists. Error: %v", repoPath, err)
		return err
	}
	if !isExist {
		return repo_model.ErrRepoNotExist{
			OwnerName: u.Name,
			Name:      repoName,
		}
	}

	if exist, err := repo_model.IsRepositoryModelExist(ctx, u, repoName); err != nil {
		return err
	} else if exist {
		return repo_model.ErrRepoAlreadyExist{
			Uname: u.Name,
			Name:  repoName,
		}
	}

	return util.RemoveAll(repoPath)
}

type unadoptedRepositories struct {
	repositories []string
	index        int
	start        int
	end          int
}

func (unadopted *unadoptedRepositories) add(repository string) {
	if unadopted.index >= unadopted.start && unadopted.index < unadopted.end {
		unadopted.repositories = append(unadopted.repositories, repository)
	}
	unadopted.index++
}

func checkUnadoptedRepositories(ctx context.Context, userName string, repoNamesToCheck []string, unadopted *unadoptedRepositories) error {
	if len(repoNamesToCheck) == 0 {
		return nil
	}
	ctxUser, err := user_model.GetUserByName(ctx, userName)
	if err != nil {
		if user_model.IsErrUserNotExist(err) {
			log.Debug("Missing user: %s", userName)
			return nil
		}
		return err
	}
	repos, _, err := repo_model.GetUserRepositories(ctx, &repo_model.SearchRepoOptions{
		Actor:   ctxUser,
		Private: true,
		ListOptions: db.ListOptions{
			Page:     1,
			PageSize: len(repoNamesToCheck),
		}, LowerNames: repoNamesToCheck,
	})
	if err != nil {
		return err
	}
	if len(repos) == len(repoNamesToCheck) {
		return nil
	}
	repoNames := make(container.Set[string], len(repos))
	for _, repo := range repos {
		repoNames.Add(repo.LowerName)
	}
	for _, repoName := range repoNamesToCheck {
		if !repoNames.Contains(repoName) {
			unadopted.add(path.Join(userName, repoName)) // These are not used as filepaths - but as reponames - therefore use path.Join not filepath.Join
		}
	}
	return nil
}

// ListUnadoptedRepositories lists all the unadopted repositories that match the provided query
func ListUnadoptedRepositories(ctx context.Context, query string, opts *db.ListOptions) ([]string, int, error) {
	globUser, _ := glob.Compile("*")
	globRepo, _ := glob.Compile("*")

	// repositories are stored by the lower case user name and lower case repo
	// name
	query = strings.ToLower(query)
	qsplit := strings.SplitN(query, "/", 2)
	if len(qsplit) > 0 && len(query) > 0 {
		var err error
		globUser, err = glob.Compile(qsplit[0])
		if err != nil {
			log.Info("Invalid glob expression '%s' (skipped): %v", qsplit[0], err)
		}
		if len(qsplit) > 1 {
			globRepo, err = glob.Compile(qsplit[1])
			if err != nil {
				log.Info("Invalid glob expression '%s' (skipped): %v", qsplit[1], err)
			}
		}
	}
	var repoNamesToCheck []string

	start := (opts.Page - 1) * opts.PageSize
	unadopted := &unadoptedRepositories{
		repositories: make([]string, 0, opts.PageSize),
		start:        start,
		end:          start + opts.PageSize,
		index:        0,
	}

	var userName string

	// We're going to iterate by pagesize.
	root := filepath.Clean(setting.RepoRootPath)
	if err := filepath.WalkDir(root, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if !d.IsDir() || path == root {
			return nil
		}

		name := d.Name()

		if !strings.ContainsRune(path[len(root)+1:], filepath.Separator) {
			// Got a new user
			if err = checkUnadoptedRepositories(ctx, userName, repoNamesToCheck, unadopted); err != nil {
				return err
			}
			repoNamesToCheck = repoNamesToCheck[:0]

			if !globUser.Match(name) {
				return filepath.SkipDir
			}

			userName = name
			return nil
		}

		if !strings.HasSuffix(name, ".git") {
			return filepath.SkipDir
		}
		name = name[:len(name)-4]
		if repo_model.IsUsableRepoName(name) != nil || strings.ToLower(name) != name || !globRepo.Match(name) {
			return filepath.SkipDir
		}

		repoNamesToCheck = append(repoNamesToCheck, name)
		if len(repoNamesToCheck) >= setting.Database.IterateBufferSize {
			if err = checkUnadoptedRepositories(ctx, userName, repoNamesToCheck, unadopted); err != nil {
				return err
			}
			repoNamesToCheck = repoNamesToCheck[:0]
		}
		return filepath.SkipDir
	}); err != nil {
		return nil, 0, err
	}

	if err := checkUnadoptedRepositories(ctx, userName, repoNamesToCheck, unadopted); err != nil {
		return nil, 0, err
	}

	return unadopted.repositories, unadopted.index, nil
}
