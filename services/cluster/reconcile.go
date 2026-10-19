// Copyright 2026 The Forgejo Authors. All rights reserved.
// SPDX-License-Identifier: GPL-3.0-or-later

package cluster

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"sort"
	"strings"
	"time"

	cluster_model "forgejo.org/models/cluster"
	"forgejo.org/models/db"
	git_model "forgejo.org/models/git"
	issues_model "forgejo.org/models/issues"
	repo_model "forgejo.org/models/repo"
	"forgejo.org/modules/gitrepo"
	"forgejo.org/modules/log"
	"forgejo.org/modules/optional"
	repo_module "forgejo.org/modules/repository"
	"forgejo.org/services/cluster/reftable"
	repo_service "forgejo.org/services/repository"
)

// ObjectsReport is what `forgejo cluster objects verify|backfill` found.
type ObjectsReport struct {
	Repos   int            `json:"repos"`
	Missing map[string]int `json:"missing,omitempty"`
	Fetched int            `json:"fetched"`
	Errors  []string       `json:"errors,omitempty"`
}

// VerifyObjects checks that this node holds every object the table's refs
// name, repository by repository (ADR 0152 D17, R3-13); with backfill it
// fetches what is missing from the other members.
func (r *Replicator) VerifyObjects(ctx context.Context, backfill bool) (ObjectsReport, error) {
	rep := ObjectsReport{Missing: map[string]int{}}
	for _, key := range r.table.Repos() {
		if ctx.Err() != nil {
			return rep, ctx.Err()
		}
		if _, deleted := r.table.Deleted(key); deleted {
			continue
		}
		refs, _ := r.table.Refs(key)
		var shas []string
		for _, v := range refs {
			if v.Live() && v.SHA != "" {
				shas = append(shas, v.SHA)
			}
		}
		if len(shas) == 0 {
			continue
		}
		rep.Repos++
		dir := filepath.Join(r.root, filepath.FromSlash(key))
		missing, err := missingObjects(ctx, dir, shas)
		if err != nil {
			rep.Errors = append(rep.Errors, fmt.Sprintf("%s: %v", key, err))
			continue
		}
		if len(missing) == 0 {
			continue
		}
		if backfill {
			if err := r.objects.fetchMissing(ctx, key, dir, missing); err != nil {
				rep.Errors = append(rep.Errors, fmt.Sprintf("%s: %v", key, err))
			} else {
				rep.Fetched += len(missing)
				continue
			}
		}
		rep.Missing[key] = len(missing)
	}
	return rep, nil
}

// Mismatch is one difference between the table and SQL.
type Mismatch struct {
	Repo     string `json:"repo"`
	Category string `json:"category"`
	// Kind says which SQL rows: branch, tag, release, pull-request.
	Kind string `json:"kind,omitempty"`
	Ref  string `json:"ref,omitempty"`
	// Regenerable rows are rebuilt from the table; the others are reported,
	// with Action naming what the operator does (R7-30).
	Regenerable bool   `json:"regenerable"`
	Action      string `json:"action,omitempty"`
	Applied     bool   `json:"applied,omitempty"`
}

// Mismatch categories (ADR 0152 D17, R4-26, R5-08).
const (
	RefsAheadOfSQL = "refs-ahead-of-sql"
	SQLAheadOfRefs = "sql-ahead-of-refs"
	RepoLifecycle  = "repository-lifecycle"
	// ParkedEvent is an event whose post-receive work waits parked: with
	// apply, it is retried; it stays until its work commits.
	ParkedEvent = "parked-event"
)

// Actions on non-regenerable rows, for the operator.
const (
	ActionDemotePR     = "demote: the pull request is marked merged, its merge commit is not in the base branch the table holds; reopen it or merge it again"
	ActionPRHeadGone   = "the head branch of the open pull request is not in the table: close it or push the branch again"
	ActionReleaseNoTag = "the release names a tag the table does not hold: push the tag again or delete the release"
)

// Reconcile compares the table with SQL's branch rows. Branch rows are
// REGENERABLE: with apply, they are rebuilt from the table and git, once the
// repository's projection reached the table (R8-26). Non-regenerable rows,
// such as a pull request's merge state, are not touched here.
func (r *Replicator) Reconcile(ctx context.Context, apply bool) ([]Mismatch, error) {
	if db.DefaultContext == nil {
		return nil, errors.New("database not ready")
	}
	if apply {
		// The rows are rebuilt from this node's table: one that lags the
		// cluster would restore a branch the cluster deleted. A commit
		// boundary a quorum confirmed is applied here first.
		if r.Engine() == nil {
			return nil, errors.New("the cluster is not running: nothing is rebuilt")
		}
		if _, err := r.confirm(ctx); err != nil {
			return nil, fmt.Errorf("this node's table could not be confirmed current (%w): nothing is rebuilt", err)
		}
	}
	out, err := r.reconcileParked(ctx, apply)
	if err != nil {
		return out, err
	}
	keys := r.table.Repos()
	inTable := make(map[string]bool, len(keys))
	for _, key := range keys {
		inTable[key] = true
	}
	orphans := map[string]bool{}
	for _, key := range keys {
		if ctx.Err() != nil {
			return out, ctx.Err()
		}
		if wiki, ok := strings.CutSuffix(key, ".wiki.git"); ok {
			// A wiki is reconciled with its repository, or alone when the
			// table holds no repository key for it.
			if main := wiki + ".git"; !inTable[main] {
				if known, err := repoKnownInDB(ctx, main); err == nil && !known {
					if m, ok := r.reconcileOrphan(ctx, main, apply, orphans); ok {
						out = append(out, m)
					}
				}
			}
			continue
		}
		_, deleted := r.table.Deleted(key)
		repo, err := lookupRepo(ctx, reftable.Event{Repo: key})
		if err != nil {
			if !deleted {
				m := Mismatch{Repo: key, Category: RepoLifecycle}
				if repo_model.IsErrRepoNotExist(err) {
					if o, ok := r.reconcileOrphan(ctx, key, apply, orphans); ok {
						m = o
					}
				}
				out = append(out, m)
			}
			continue
		}
		if deleted {
			out = append(out, Mismatch{Repo: key, Category: RepoLifecycle})
			continue
		}
		refs, at := r.table.Refs(key)
		table := map[string]bool{}
		for name, v := range refs {
			if v.Live() && v.SHA != "" && strings.HasPrefix(name, "refs/heads/") {
				table[strings.TrimPrefix(name, "refs/heads/")] = true
			}
		}
		rows, err := git_model.FindBranchNames(ctx, git_model.FindBranchOptions{RepoID: repo.ID, IsDeletedBranch: optional.Some(false)})
		if err != nil {
			return out, err
		}
		sqlSet := map[string]bool{}
		for _, b := range rows {
			sqlSet[b] = true
		}
		var stale []string
		var repoMismatches []Mismatch
		for b := range table {
			if !sqlSet[b] {
				repoMismatches = append(repoMismatches, Mismatch{Repo: key, Category: RefsAheadOfSQL, Kind: "branch", Ref: b, Regenerable: true})
				stale = append(stale, b)
			}
		}
		for b := range sqlSet {
			if !table[b] {
				repoMismatches = append(repoMismatches, Mismatch{Repo: key, Category: SQLAheadOfRefs, Kind: "branch", Ref: b, Regenerable: true})
			}
		}
		sort.Slice(repoMismatches, func(i, j int) bool { return repoMismatches[i].Ref < repoMismatches[j].Ref })
		if apply && len(stale) > 0 && r.projector.Projected(key) >= at {
			gitRepo, err := gitrepo.OpenRepository(ctx, repo)
			if err == nil {
				err = repo_service.SyncBranchesToDB(ctx, repo.ID, 0, stale, gitRepo.GetBranchCommit)
				gitRepo.Close()
			}
			if err == nil {
				for i := range repoMismatches {
					if repoMismatches[i].Category == RefsAheadOfSQL {
						repoMismatches[i].Applied = true
					}
				}
			}
		}
		out = append(out, repoMismatches...)
		more, err := r.reconcileNonRegenerable(ctx, key, repo, refs, apply && r.projector.Projected(key) >= at)
		if err != nil {
			return out, err
		}
		out = append(out, more...)
	}
	r.forgetOrphans(orphans)
	return out, nil
}

// orphanGrace is how long a repository the table holds alive, and SQL
// does not know, must stay unchanged in the table before `reconcile
// --apply` deletes it from the cluster. A creation or a fork writes its
// refs before its row commits, a fork's copy taking up to ten minutes.
var orphanGrace = 30 * time.Minute

// orphanMark is when reconcile first saw an orphaned key at an index.
type orphanMark struct {
	last  uint64
	since time.Time
}

// reconcileOrphan handles a repository the table holds alive and SQL does
// not know, with its wiki: the files of a deletion or of a failed creation
// the cluster did not record (R-D4, R-D5). Every member keeps them, and a
// repository created again under the name would inherit their refs. With
// apply, once the keys stayed unchanged for orphanGrace since this node's
// reconcile first saw them, and SQL still has no row, their deletion is
// proposed and every member removes the files. It reports false when the
// table holds no live ref under either key: nothing is left to delete.
func (r *Replicator) reconcileOrphan(ctx context.Context, key string, apply bool, seen map[string]bool) (Mismatch, bool) {
	wiki := strings.TrimSuffix(key, ".git") + ".wiki.git"
	var live []string
	var last uint64
	for _, k := range []string{key, wiki} {
		refs, _ := r.table.Refs(k)
		for _, v := range refs {
			if v.Live() {
				live = append(live, k)
				_, at := r.table.RepoHash(k)
				last = max(last, at)
				break
			}
		}
	}
	if len(live) == 0 {
		return Mismatch{}, false
	}
	seen[key] = true
	r.orphansMu.Lock()
	if r.orphans == nil {
		r.orphans = map[string]orphanMark{}
	}
	mark, ok := r.orphans[key]
	if !ok || mark.last != last {
		mark = orphanMark{last: last, since: time.Now()}
		r.orphans[key] = mark
	}
	r.orphansMu.Unlock()

	m := Mismatch{Repo: key, Category: RepoLifecycle}
	if wait := orphanGrace - time.Since(mark.since); wait > 0 {
		m.Action = fmt.Sprintf("the table holds this repository, SQL has no row for it: `forgejo cluster reconcile --apply` deletes it from the cluster once it stayed unchanged for %s, from %s on", orphanGrace, mark.since.Add(orphanGrace).UTC().Format(time.RFC3339))
		return m, true
	}
	m.Action = "the table holds this repository, SQL has no row for it: `forgejo cluster reconcile --apply` deletes it from the cluster"
	if !apply {
		return m, true
	}
	// SQL is read again last: a creation that committed meanwhile keeps
	// its repository.
	if known, err := repoKnownInDB(ctx, key); err != nil || known {
		return m, true
	}
	for _, k := range live {
		if err := r.proposeRepoDelete(ctx, k, 0); err != nil {
			log.Error("cluster reconcile: deleting %s, which SQL does not know: %v", k, err)
			return m, true
		}
	}
	log.Warn("cluster reconcile: deleted %s from the cluster: the table held it alive, unchanged since %s, and SQL has no row for it", strings.Join(live, ", "), mark.since.UTC().Format(time.RFC3339))
	m.Applied = true
	return m, true
}

// forgetOrphans drops, after a whole pass, the marks of the keys it did
// not find orphaned.
func (r *Replicator) forgetOrphans(seen map[string]bool) {
	r.orphansMu.Lock()
	defer r.orphansMu.Unlock()
	for k := range r.orphans {
		if !seen[k] {
			delete(r.orphans, k)
		}
	}
}

// reconcileParked lists the parked events and, with apply, retries them
// first: those whose work committed are reported applied.
func (r *Replicator) reconcileParked(ctx context.Context, apply bool) ([]Mismatch, error) {
	before, err := cluster_model.ListParked(ctx)
	if err != nil {
		return nil, err
	}
	still := map[string]bool{}
	if apply && len(before) > 0 && r.consumer != nil {
		if _, err := r.consumer.retryParked(ctx); err != nil {
			return nil, err
		}
		after, err := cluster_model.ListParked(ctx)
		if err != nil {
			return nil, err
		}
		for _, p := range after {
			still[fmt.Sprintf("%s@%d", p.Lineage, p.LogIndex)] = true
		}
	}
	var out []Mismatch
	seen := map[string]bool{}
	for _, p := range before {
		key := fmt.Sprintf("%s@%d", p.Lineage, p.LogIndex)
		if seen[key] {
			continue
		}
		seen[key] = true
		out = append(out, Mismatch{
			Repo: p.Repo, Category: ParkedEvent, Kind: "event", Ref: key, Regenerable: true,
			Action: p.Reason, Applied: apply && r.consumer != nil && !still[key],
		})
	}
	return out, nil
}

// reconcileNonRegenerable checks the rows the table cannot rebuild: pull
// requests marked merged whose merge commit the base branch does not hold,
// open pull requests whose head branch is gone, releases whose tag is gone.
// They are reported with the operator's action, never changed. Tag-only
// release rows are regenerable and, with apply, rebuilt from git.
func (r *Replicator) reconcileNonRegenerable(ctx context.Context, key string, repo *repo_model.Repository, refs map[string]reftable.RefValue, apply bool) ([]Mismatch, error) {
	var out []Mismatch
	live := func(ref string) (reftable.RefValue, bool) {
		v, ok := refs[ref]
		return v, ok && v.Live() && v.SHA != ""
	}
	dir := filepath.Join(r.root, filepath.FromSlash(key))

	var prs []*issues_model.PullRequest
	if err := db.GetEngine(ctx).Where("base_repo_id = ?", repo.ID).Find(&prs); err != nil {
		return out, err
	}
	for _, pr := range prs {
		if pr.HasMerged {
			base, ok := live("refs/heads/" + pr.BaseBranch)
			if pr.MergedCommitID == "" {
				continue
			}
			if !ok || !isAncestor(ctx, dir, pr.MergedCommitID, base.SHA) {
				out = append(out, Mismatch{Repo: key, Category: SQLAheadOfRefs, Kind: "pull-request", Ref: fmt.Sprintf("#%d", pr.Index), Action: ActionDemotePR})
			}
			continue
		}
		if pr.HeadRepoID == repo.ID && pr.Flow == issues_model.PullRequestFlowGithub {
			if _, ok := live("refs/heads/" + pr.HeadBranch); !ok {
				closed, err := isClosed(ctx, pr)
				if err == nil && !closed {
					out = append(out, Mismatch{Repo: key, Category: SQLAheadOfRefs, Kind: "pull-request", Ref: fmt.Sprintf("#%d", pr.Index), Action: ActionPRHeadGone})
				}
			}
		}
	}

	var rels []*repo_model.Release
	if err := db.GetEngine(ctx).Where("repo_id = ?", repo.ID).Find(&rels); err != nil {
		return out, err
	}
	sqlTags := map[string]bool{}
	for _, rel := range rels {
		sqlTags[rel.TagName] = true
		if rel.IsDraft {
			continue
		}
		if _, ok := live("refs/tags/" + rel.TagName); !ok {
			m := Mismatch{Repo: key, Category: SQLAheadOfRefs, Kind: "tag", Ref: rel.TagName, Regenerable: rel.IsTag}
			if !rel.IsTag {
				m.Kind, m.Action = "release", ActionReleaseNoTag
			}
			out = append(out, m)
		}
	}
	missingTags := false
	for name, v := range refs {
		if tag, ok := strings.CutPrefix(name, "refs/tags/"); ok && v.Live() && !sqlTags[tag] {
			out = append(out, Mismatch{Repo: key, Category: RefsAheadOfSQL, Kind: "tag", Ref: tag, Regenerable: true})
			missingTags = true
		}
	}
	regenerableTags := missingTags
	for _, m := range out {
		if m.Kind == "tag" && m.Category == SQLAheadOfRefs {
			regenerableTags = true
		}
	}
	if apply && regenerableTags {
		gitRepo, err := gitrepo.OpenRepository(ctx, repo)
		if err == nil {
			err = repo_module.SyncReleasesWithTags(ctx, repo, gitRepo)
			gitRepo.Close()
		}
		if err == nil {
			for i := range out {
				if out[i].Kind == "tag" {
					out[i].Applied = true
				}
			}
		}
	}
	sort.SliceStable(out, func(i, j int) bool { return out[i].Kind+out[i].Ref < out[j].Kind+out[j].Ref })
	return out, nil
}

func isAncestor(ctx context.Context, dir, commit, tip string) bool {
	_, err := gitRun(ctx, dir, nil, "merge-base", "--is-ancestor", commit, tip)
	return err == nil
}

func isClosed(ctx context.Context, pr *issues_model.PullRequest) (bool, error) {
	if err := pr.LoadIssue(ctx); err != nil {
		return false, err
	}
	return pr.Issue.IsClosed, nil
}
