// Copyright 2026 The Forgejo Authors. All rights reserved.
// SPDX-License-Identifier: GPL-3.0-or-later

package cluster

import (
	"context"
	"fmt"
	"slices"
	"strings"

	repo_model "forgejo.org/models/repo"
	user_model "forgejo.org/models/user"
	"forgejo.org/modules/clusterhook"
	"forgejo.org/modules/log"
	"forgejo.org/services/cluster/reftable"
)

// rekeyByRedirects follows, through SQL, the repository of a parked entry
// of another lineage than the table's (#238). The post-receive work checks
// that the repository its ID names now is the one the event was written
// for by following the table's renames from the event's index (eventLeadsTo).
// A table that does not hold the event's lineage cannot answer: a recovery
// from an observer starts a new table, with none of the old lineage's
// renames and its indexes counted again from 1; a restore starts from a
// snapshot cut before the renames that followed it. A push the old lineage
// parked, its repository renamed or transferred since, then failed at every
// retry as a reused ID, and every later push of the repository was parked
// behind it, the trim of the new lineage held below them.
//
// SQL keeps what the table lost: a rename or a transfer leaves a redirect
// from the repository's old name under its old owner, and an owner's
// rename one from the owner's old name. When they lead the event's key to
// the repository its ID names, the event is given that repository's key
// now, and the work's own check passes. A reissued ID leads there only if
// the repository holding it had the event's key itself, which the table's
// renames accept as well. The events of the table's own lineage never come
// here: the table alone judges them.
//
// What the redirects do not lead (another repository or account took the
// old name since, or the ID was reissued) keeps its key: the work refuses
// it, never landing on another repository, and doubt says why the table
// cannot settle it and how the operator gives it up.
func rekeyByRedirects(ctx context.Context, lineage string, evs []reftable.Event) ([]reftable.Event, string, error) {
	out := slices.Clone(evs)
	var doubt string
	for i := range out {
		ev := &out[i]
		if ev.Kind != reftable.EventRef || ev.IsWiki || ev.PusherID == 0 || ev.RepoID <= 0 || ev.Trigger == TriggerImport {
			continue // no work, or no ID to check (pushWorkWith)
		}
		repo, err := repo_model.GetRepositoryByID(ctx, ev.RepoID)
		if repo_model.IsErrRepoNotExist(err) {
			doubt = lineageDoubt(lineage, ev, "no repository holds its ID any more, and the table may not hold the deletion")
			continue
		}
		if err != nil {
			return evs, "", err
		}
		key := clusterhook.RepoKey(repo.OwnerName, repo.Name, false)
		if key == ev.Repo {
			continue
		}
		led, err := redirectsLead(ctx, ev.Repo, repo)
		if err != nil {
			return evs, "", err
		}
		if !led {
			doubt = lineageDoubt(lineage, ev, fmt.Sprintf("the table may not hold the renames that led it to %s, and SQL's redirects do not lead it there either", key))
			continue
		}
		log.Info("cluster consumer: the parked event of %s at %d is of lineage %s, whose renames this table may not hold: SQL's redirects lead it to %s (ID %d), where its work runs", ev.Repo, ev.Token.Index, lineage, key, ev.RepoID)
		ev.Repo = key
	}
	return out, doubt, nil
}

// lineageDoubt is why a parked event of another lineage than the table's
// may never succeed, and the operator's way out.
func lineageDoubt(lineage string, ev *reftable.Event, why string) string {
	return fmt.Sprintf("%s is an event of lineage %s, not of this table's: %s; if it never succeeds, `forgejo cluster parked resolve %d --lineage %s --reason \"...\"` gives up its work (push actions, webhooks, CI, pull request updates)",
		ev.Repo, lineage, why, ev.Token.Index, lineage)
}

// redirectsLead reports whether SQL's redirects lead key, the repository an
// event was written for, to repo: its owner, under that name or the
// redirect from it, holds repo under the event's name or a redirect from
// it. Forgejo removes a redirect once another repository, or another
// account, takes its name: nothing in SQL then says where key went.
func redirectsLead(ctx context.Context, key string, repo *repo_model.Repository) (bool, error) {
	owner, name, ok := strings.Cut(strings.TrimSuffix(key, ".git"), "/")
	if !ok || owner == "" || name == "" {
		return false, nil
	}
	var owners []int64
	switch u, err := user_model.GetUserByName(ctx, owner); {
	case err == nil:
		owners = append(owners, u.ID)
	case !user_model.IsErrUserNotExist(err):
		return false, err
	}
	switch r, err := user_model.GetUserRedirect(ctx, owner); {
	case err == nil:
		owners = append(owners, r.RedirectUserID)
	case !user_model.IsErrUserRedirectNotExist(err):
		return false, err
	}
	for _, id := range owners {
		if id == repo.OwnerID && name == repo.LowerName {
			return true, nil
		}
		to, err := repo_model.GetRedirect(ctx, id, name)
		switch {
		case err == nil && to == repo.ID:
			return true, nil
		case err != nil && !repo_model.IsErrRedirectNotExist(err):
			return false, err
		}
	}
	return false, nil
}
