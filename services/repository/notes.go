// Copyright 2026 The Forgejo Authors. All rights reserved.
// SPDX-License-Identifier: GPL-3.0-or-later

package repository

import (
	"context"

	repo_model "forgejo.org/models/repo"
	user_model "forgejo.org/models/user"
	"forgejo.org/modules/clusterhook"
	"forgejo.org/modules/git"
)

// notesFresh waits until this node serves repo at the commit index the
// cluster confirms at the call, and refuses with clusterhook.ErrBehind
// while it does not, before anything is written. git notes builds the new
// notes commit on this node's refs/notes/commits: on a node behind on the
// repository it was built on notes older than those another node
// acknowledged, and dropped them for the whole cluster (#324). git.SetNote
// now has the cluster refuse such an update rather than take it; the wait
// builds the note on the acknowledged notes instead, and leaves that
// refusal to a note another node writes between the wait and the update.
// Without a cluster it returns nil.
func notesFresh(ctx context.Context, repo *repo_model.Repository) error {
	return clusterhook.RequireFresh(ctx, clusterhook.RepoKey(repo.OwnerName, repo.Name, false))
}

// SetNote sets the git note of commitID in repo to message, written by
// doer. In a cluster it waits first for this node to serve the repository
// (notesFresh).
func SetNote(ctx context.Context, doer *user_model.User, repo *repo_model.Repository, gitRepo *git.Repository, commitID, message string) error {
	if err := notesFresh(ctx, repo); err != nil {
		return err
	}
	return git.SetNote(ctx, gitRepo, commitID, message, doer.Name, doer.GetEmail())
}

// RemoveNote removes the git note of commitID in repo. In a cluster it
// waits first for this node to serve the repository (notesFresh).
func RemoveNote(ctx context.Context, repo *repo_model.Repository, gitRepo *git.Repository, commitID string) error {
	if err := notesFresh(ctx, repo); err != nil {
		return err
	}
	return git.RemoveNote(ctx, gitRepo, commitID)
}
