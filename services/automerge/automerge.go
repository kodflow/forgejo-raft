// Copyright 2021 Gitea. All rights reserved.
// SPDX-License-Identifier: MIT

package automerge

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"forgejo.org/models"
	"forgejo.org/models/db"
	issues_model "forgejo.org/models/issues"
	access_model "forgejo.org/models/perm/access"
	pull_model "forgejo.org/models/pull"
	repo_model "forgejo.org/models/repo"
	user_model "forgejo.org/models/user"
	"forgejo.org/modules/clusterhook"
	"forgejo.org/modules/git"
	"forgejo.org/modules/gitrepo"
	"forgejo.org/modules/graceful"
	"forgejo.org/modules/log"
	"forgejo.org/modules/process"
	"forgejo.org/modules/queue"
	"forgejo.org/modules/util"
	notify_service "forgejo.org/services/notify"
	pull_service "forgejo.org/services/pull"
	repo_service "forgejo.org/services/repository"
	shared_automerge "forgejo.org/services/shared/automerge"
)

// Init runs the task queue to that handles auto merges
func Init() error {
	notify_service.RegisterNotifier(NewNotifier())

	shared_automerge.PRAutoMergeQueue = queue.CreateUniqueQueue(graceful.GetManager().ShutdownContext(), "pr_auto_merge", handler)
	if shared_automerge.PRAutoMergeQueue == nil {
		return errors.New("unable to create pr_auto_merge queue")
	}
	go graceful.GetManager().RunWithCancel(shared_automerge.PRAutoMergeQueue)
	return nil
}

// branchDeletionItem prefixes the queue items of a merged pull request's
// head branch deletion, which a node of a cluster defers: the pull
// request's ID, the scheduled doer's ID and the head the merge took follow
// it.
const branchDeletionItem = "delete-branch_"

// Handle handles items of the pr_auto_merge queue, as the queue's workers
// do, and returns those to hand back to the queue. The cluster's test
// bench drives the queue of a node with it.
func Handle(items ...string) []string {
	return handler(items...)
}

// handle passed PR IDs and test the PRs. The items it returns are pushed
// back to the queue, after a few seconds when none was handled: a merge a
// node of a cluster defers until it serves the pull request's repositories,
// and the deletion of a merged pull request's head branch it defers until
// its consumer derived the head repository (#279).
func handler(items ...string) []string {
	var deferred []string
	for _, s := range items {
		if rest, ok := strings.CutPrefix(s, branchDeletionItem); ok {
			var id, doerID int64
			var mergedHead string
			if _, err := fmt.Sscanf(rest, "%d_%d_%s", &id, &doerID, &mergedHead); err != nil {
				log.Error("could not parse data from pr_auto_merge queue (%v): %v", s, err)
				continue
			}
			if handleBranchDeletion(id, doerID, mergedHead) {
				deferred = append(deferred, s)
			}
			continue
		}
		var id int64
		var sha string
		if _, err := fmt.Sscanf(s, "%d_%s", &id, &sha); err != nil {
			log.Error("could not parse data from pr_auto_merge queue (%v): %v", s, err)
			continue
		}
		retry, deletion := handlePullRequestAutoMerge(id, sha)
		if retry {
			deferred = append(deferred, s)
		}
		if deletion != "" {
			deferred = append(deferred, deletion)
		}
	}
	return deferred
}

// ScheduleAutoMerge if schedule is false and no error, pull can be merged directly
func ScheduleAutoMerge(ctx context.Context, doer *user_model.User, pull *issues_model.PullRequest, style repo_model.MergeStyle, message string, deleteBranch bool) (scheduled bool, err error) {
	err = db.WithTx(ctx, func(ctx context.Context) error {
		if err := pull_model.ScheduleAutoMerge(ctx, doer, pull.ID, style, message, deleteBranch); err != nil {
			return err
		}
		scheduled = true

		_, err = issues_model.CreateAutoMergeComment(ctx, issues_model.CommentTypePRScheduledToAutoMerge, pull, doer)
		return err
	})
	return scheduled, err
}

// RemoveScheduledAutoMerge cancels a previously scheduled pull request
func RemoveScheduledAutoMerge(ctx context.Context, doer *user_model.User, pull *issues_model.PullRequest, repoPerms access_model.Permission) error {
	exist, autoMerge, err := pull_model.GetScheduledMergeByPullID(ctx, pull.ID)
	if err != nil {
		return err
	}
	if !exist {
		return db.ErrNotExist{Resource: "auto_merge", ID: pull.ID}
	}

	if doer.ID != autoMerge.DoerID {
		allowed, err := pull_service.IsUserAllowedToMerge(ctx, pull, repoPerms, doer)
		if err != nil {
			return err
		}
		if !allowed {
			return util.ErrPermissionDenied
		}
	}

	return db.WithTx(ctx, func(ctx context.Context) error {
		if err := pull_model.DeleteScheduledAutoMerge(ctx, pull.ID); err != nil {
			return err
		}

		_, err := issues_model.CreateAutoMergeComment(ctx, issues_model.CommentTypePRUnScheduledToAutoMerge, pull, doer)
		return err
	})
}

// StartPRCheckAndAutoMergeBySHA start an automerge check and auto merge task for all pull requests of repository and SHA
func StartPRCheckAndAutoMergeBySHA(ctx context.Context, sha string, repo *repo_model.Repository) error {
	return shared_automerge.StartPRCheckAndAutoMergeBySHA(ctx, sha, repo)
}

// StartPRCheckAndAutoMerge start an automerge check and auto merge task for a pull request
func StartPRCheckAndAutoMerge(ctx context.Context, pull *issues_model.PullRequest) {
	shared_automerge.StartPRCheckAndAutoMerge(ctx, pull)
}

// handlePullRequestAutoMerge merge the pull request if all checks are successful.
// It reports whether the merge is to be tried again later, its schedule
// kept: on a node of a cluster behind the cluster's last writes to the head
// or the base repository, which it reads from its own git (#247). Once the
// merge is done, deletion is the queue item of the head branch's deletion
// the node deferred, "" for none (#279).
func handlePullRequestAutoMerge(pullID int64, sha string) (retry bool, deletion string) {
	ctx, _, finished := process.GetManager().AddContext(graceful.GetManager().HammerContext(),
		fmt.Sprintf("Handle AutoMerge of PR[%d] with sha[%s]", pullID, sha))
	defer finished()

	pr, err := issues_model.GetPullRequestByID(ctx, pullID)
	if err != nil {
		log.Error("GetPullRequestByID[%d]: %v", pullID, err)
		return false, ""
	}

	// Check if there is a scheduled pr in the db
	exists, scheduledPRM, err := pull_model.GetScheduledMergeByPullID(ctx, pr.ID)
	if err != nil {
		log.Error("%-v GetScheduledMergeByPullID: %v", pr, err)
		return false, ""
	}
	if !exists {
		log.Trace("GetScheduledMergeByPullID found nothing for PR %d", pullID)
		return false, ""
	}

	if err = pr.LoadBaseRepo(ctx); err != nil {
		log.Error("%-v LoadBaseRepo: %v", pr, err)
		return false, ""
	}

	// In a cluster, refs/pull/N/head, the head branch and the base are read
	// below from this node's git: behind a push another node acknowledged,
	// the head compared to sha, and then merged, is the commit before it.
	// The merge waits until this node serves both repositories, and is
	// deferred, its schedule kept, while it does not.
	if err := pull_service.RequireFreshHeadAndBase(ctx, pr); err != nil {
		if errors.Is(err, clusterhook.ErrBehind) {
			log.Warn("Auto merge of %-v deferred: %v", pr, err)
			return true, ""
		}
		log.Error("%-v RequireFreshHeadAndBase: %v", pr, err)
		return false, ""
	}

	// check the sha is the same as pull request head commit id
	baseGitRepo, err := gitrepo.OpenRepository(ctx, pr.BaseRepo)
	if err != nil {
		log.Error("OpenRepository: %v", err)
		return false, ""
	}
	defer baseGitRepo.Close()

	headCommitID, err := baseGitRepo.GetRefCommitID(pr.GetGitRefName())
	if err != nil {
		log.Error("GetRefCommitID: %v", err)
		return false, ""
	}
	if headCommitID != sha {
		log.Warn("Head commit id of auto merge %-v does not match sha [%s], it may means the head branch has been updated. Just ignore this request because a new request expected in the queue", pr, sha)
		return false, ""
	}

	// Get all checks for this pr
	// We get the latest sha commit hash again to handle the case where the check of a previous push
	// did not succeed or was not finished yet.
	if err = pr.LoadHeadRepo(ctx); err != nil {
		log.Error("%-v LoadHeadRepo: %v", pr, err)
		return false, ""
	}

	var headGitRepo *git.Repository
	if pr.BaseRepoID == pr.HeadRepoID {
		headGitRepo = baseGitRepo
	} else {
		headGitRepo, err = gitrepo.OpenRepository(ctx, pr.HeadRepo)
		if err != nil {
			log.Error("OpenRepository %-v: %v", pr.HeadRepo, err)
			return false, ""
		}
		defer headGitRepo.Close()
	}

	switch pr.Flow {
	case issues_model.PullRequestFlowGithub:
		headBranchExist := headGitRepo.IsBranchExist(pr.HeadBranch)
		if pr.HeadRepo == nil || !headBranchExist {
			log.Warn("Head branch of auto merge %-v does not exist [HeadRepoID: %d, Branch: %s]", pr, pr.HeadRepoID, pr.HeadBranch)
			return false, ""
		}
		// The merge takes the head branch, which a push moves before the
		// pull request's work moves refs/pull/N/head to it and its checks
		// follow: until then, the checks passed are those of sha, not of
		// the commit the merge would take (#247). That work queues a new
		// request, with the schedule kept.
		headBranchCommitID, err := headGitRepo.GetBranchCommitID(pr.HeadBranch)
		if err != nil {
			log.Error("%-v GetBranchCommitID: %v", pr, err)
			return false, ""
		}
		if headBranchCommitID != sha {
			log.Warn("Head branch of auto merge %-v moved past sha [%s] to %s, which its checks did not judge yet. Just ignore this request because a new request expected in the queue", pr, sha, headBranchCommitID)
			return false, ""
		}
	case issues_model.PullRequestFlowAGit:
		headBranchExist := baseGitRepo.IsReferenceExist(pr.GetGitRefName())
		if !headBranchExist {
			log.Warn("Head branch of auto merge %-v does not exist [HeadRepoID: %d, Branch(Agit): %s]", pr, pr.HeadRepoID, pr.HeadBranch)
			return false, ""
		}
	default:
		log.Error("wrong flow type %d", pr.Flow)
		return false, ""
	}

	// Check if all checks succeeded
	pass, err := pull_service.IsPullCommitStatusPass(ctx, pr)
	if err != nil {
		log.Error("%-v IsPullCommitStatusPass: %v", pr, err)
		return false, ""
	}
	if !pass {
		log.Info("Scheduled auto merge %-v has unsuccessful status checks", pr)
		return false, ""
	}

	// Merge if all checks succeeded
	doer, err := user_model.GetUserByID(ctx, scheduledPRM.DoerID)
	if err != nil {
		log.Error("Unable to get scheduled User[%d]: %v", scheduledPRM.DoerID, err)
		return false, ""
	}

	perm, err := access_model.GetUserRepoPermission(ctx, pr.BaseRepo, doer)
	if err != nil {
		log.Error("GetUserRepoPermission %-v: %v", pr.BaseRepo, err)
		return false, ""
	}

	if err := pull_service.CheckPullMergeable(ctx, doer, &perm, pr, pull_service.MergeCheckTypeGeneral, false, scheduledPRM.MergeStyle); err != nil {
		if errors.Is(err, pull_service.ErrUserNotAllowedToMerge) {
			log.Info("%-v was scheduled to automerge by an unauthorized user", pr)
			return false, ""
		}
		log.Error("%-v CheckPullMergeable: %v", pr, err)
		return false, ""
	}

	if err := pull_model.DeleteScheduledAutoMerge(ctx, pr.ID); err != nil && !db.IsErrNotExist(err) {
		log.Error("DeleteScheduledAutoMerge[%d]: %v", pr.ID, err)
	}

	// The head merged is the one the checks above passed, sha: Merge reads
	// a branch's pull request's head from its head branch, which a push
	// moves before the pull request's refs/pull/N/head and its statuses
	// follow it (#247).
	if err := pull_service.Merge(ctx, pr, doer, baseGitRepo, scheduledPRM.MergeStyle, sha, scheduledPRM.Message, true); err != nil {
		switch {
		case errors.Is(err, clusterhook.ErrBehind):
			// Merge waits again, for the cluster's commit index at its own
			// call: a push acknowledged since the wait above, which this
			// node does not serve yet, refuses it before anything is read
			// or written. The schedule deleted above is restored, and the
			// merge deferred.
			log.Warn("Auto merge of %-v deferred: %v", pr, err)
			return restoreSchedule(ctx, doer, pr, scheduledPRM), ""
		case models.IsErrSHADoesNotMatch(err):
			// The head moved past sha: nothing was merged, and the new
			// head's checks queue the merge again, as for a moved
			// refs/pull/N/head above, once the schedule is restored.
			log.Warn("Head of auto merge %-v moved past sha [%s]: %v", pr, sha, err)
			restoreSchedule(ctx, doer, pr, scheduledPRM)
			return false, ""
		}
		log.Error("pull_service.Merge: %v", err)
		// FIXME: if merge failed, we should display some error message to the pull request page.
		// The resolution is add a new column on automerge table named `error_message` to store the error message and displayed
		// on the pull request page. But this should not be finished in a bug fix PR which will be backport to release branch.
		return false, ""
	}

	// The schedule was deleted before the merge, and the post-receive hook
	// of the merge's push deletes it too: a deletion the node defers is
	// handed back to the queue as an item of its own, which carries what
	// it needs, sha the head the merge took among them (#279).
	if scheduledPRM.DeleteBranchAfterMerge && deleteHeadBranch(ctx, doer, pr, headGitRepo, sha) {
		return false, fmt.Sprintf("%s%d_%d_%s", branchDeletionItem, pr.ID, doer.ID, sha)
	}
	return false, ""
}

// deleteHeadBranch deletes the head branch of pr, which a scheduled merge
// merged at mergedHead, as its schedule asked. A branch that moved past
// mergedHead since is kept: the commits pushed to it would be lost. It
// reports whether the deletion is deferred: a node of a cluster whose
// consumer has not derived the head repository's last writes refuses it
// before anything is written, which keeps the branch row from staying
// live in SQL for good (#267), and does it once it derived them (#279).
func deleteHeadBranch(ctx context.Context, doer *user_model.User, pr *issues_model.PullRequest, headGitRepo *git.Repository, mergedHead string) (deferred bool) {
	err := repo_service.DeleteBranchAfterMerge(ctx, doer, pr, headGitRepo, mergedHead)
	switch {
	case err == nil:
		return false
	case errors.Is(err, clusterhook.ErrBehind):
		log.Warn("Deletion of the head branch of auto merged %-v deferred: %v", pr, err)
		return true
	case errors.Is(err, repo_service.ErrBranchHasNewCommits):
		log.Warn("Head branch of auto merged %-v kept: %v", pr, err)
		return false
	case git.IsErrNotExist(err) || git.IsErrBranchNotExist(err):
		log.Info("Head branch of auto merged %-v is already deleted: %v", pr, err)
		return false
	}
	log.Error("%d repo_service.DeleteBranchAfterMerge: %v", pr.ID, err)
	return false
}

// handleBranchDeletion deletes the head branch of the pull request
// pullID, which a scheduled merge by the user doerID merged at mergedHead,
// once the node derived the head repository: the deletion the node
// deferred right after the merge (#279). It reports whether the deletion
// is to be tried again later, while the node still defers it.
func handleBranchDeletion(pullID, doerID int64, mergedHead string) (retry bool) {
	ctx, _, finished := process.GetManager().AddContext(graceful.GetManager().HammerContext(),
		fmt.Sprintf("Delete the head branch of auto merged PR[%d] at sha[%s]", pullID, mergedHead))
	defer finished()

	pr, err := issues_model.GetPullRequestByID(ctx, pullID)
	if err != nil {
		log.Error("GetPullRequestByID[%d]: %v", pullID, err)
		return false
	}
	if !pr.HasMerged {
		log.Error("%-v is not merged: its head branch is not deleted", pr)
		return false
	}
	doer, err := user_model.GetUserByID(ctx, doerID)
	if err != nil {
		log.Error("Unable to get scheduled User[%d] of %-v: %v", doerID, pr, err)
		return false
	}
	if err := pr.LoadIssue(ctx); err != nil {
		log.Error("%-v LoadIssue: %v", pr, err)
		return false
	}
	if err := pr.LoadBaseRepo(ctx); err != nil {
		log.Error("%-v LoadBaseRepo: %v", pr, err)
		return false
	}
	if err := pr.LoadHeadRepo(ctx); err != nil {
		log.Error("%-v LoadHeadRepo: %v", pr, err)
		return false
	}
	if pr.HeadRepo == nil {
		log.Warn("Head repository of auto merged %-v was deleted: no head branch to delete", pr)
		return false
	}
	headGitRepo, err := gitrepo.OpenRepository(ctx, pr.HeadRepo)
	if err != nil {
		log.Error("OpenRepository %-v: %v", pr.HeadRepo, err)
		return false
	}
	defer headGitRepo.Close()

	return deleteHeadBranch(ctx, doer, pr, headGitRepo, mergedHead)
}

// restoreSchedule schedules again the merge handlePullRequestAutoMerge
// deleted before a Merge that wrote nothing, and reports whether the item
// is to be tried again: not when the schedule could not be restored. A
// schedule set meanwhile, by the user, is kept as it is.
func restoreSchedule(ctx context.Context, doer *user_model.User, pr *issues_model.PullRequest, scheduled *pull_model.AutoMerge) bool {
	if err := pull_model.ScheduleAutoMerge(ctx, doer, pr.ID, scheduled.MergeStyle, scheduled.Message, scheduled.DeleteBranchAfterMerge); err != nil && !pull_model.IsErrAlreadyScheduledToAutoMerge(err) {
		log.Error("%-v ScheduleAutoMerge: %v", pr, err)
		return false
	}
	return true
}
