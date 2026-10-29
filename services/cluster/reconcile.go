// Copyright 2026 The Forgejo Authors. All rights reserved.
// SPDX-License-Identifier: GPL-3.0-or-later

package cluster

import (
	"context"
	"errors"
	"fmt"
	"math"
	"path/filepath"
	"sort"
	"strings"
	"time"

	cluster_model "forgejo.org/models/cluster"
	"forgejo.org/models/db"
	git_model "forgejo.org/models/git"
	issues_model "forgejo.org/models/issues"
	repo_model "forgejo.org/models/repo"
	"forgejo.org/modules/git"
	"forgejo.org/modules/gitrepo"
	"forgejo.org/modules/log"
	"forgejo.org/modules/optional"
	repo_module "forgejo.org/modules/repository"
	"forgejo.org/services/cluster/consensus"
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
			if err := r.backfill(ctx, key, dir, missing); err != nil {
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

// backfill fetches the objects missing from key's directory for
// VerifyObjects. The fetch writes under that directory, as the consumer's
// does: while Forgejo moves it on this node, the repository's or its
// owner's rename, it is refused, and the hold such a move takes cancels a
// fetch in flight there and waits for its end; outside the hold, a
// member's answer arriving after the move created the directory again
// under the old name (#275).
func (r *Replicator) backfill(ctx context.Context, key, dir string, missing []string) error {
	var holds *dirHolds
	if r.projector != nil {
		holds = r.projector.holds
	}
	fctx, leave, ok := holds.enterCtx(ctx, key)
	if !ok {
		return errors.New("its directory moves with a rename on this node; retry")
	}
	defer leave()
	return r.objects.fetchMissing(fctx, key, dir, missing)
}

// Mismatch is one difference between the table and SQL.
type Mismatch struct {
	Repo     string `json:"repo"`
	Category string `json:"category"`
	// Kind says which SQL rows: branch, tag, release, pull-request.
	Kind string `json:"kind,omitempty"`
	Ref  string `json:"ref,omitempty"`
	// Regenerable rows are rebuilt from the table; the others are reported,
	// with Action naming what the operator does (R7-30). A regenerable
	// row's Action, when set, says what its rebuild changes.
	Regenerable bool   `json:"regenerable"`
	Action      string `json:"action,omitempty"`
	Applied     bool   `json:"applied,omitempty"`
	// Index and Since are, for a repository the table holds and SQL does
	// not know, the index of its last change and since when this node saw
	// it unchanged: what `reconcile --delete-orphan` checks.
	Index uint64 `json:"index,omitempty"`
	Since string `json:"since,omitempty"`
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
	// SQL does not tell the two apart: a draft saved as such has no
	// commit either, so the row is never published here.
	ActionReleaseDraftTagHeld = "the release is a draft without a commit while the table holds its tag: either a draft its author saved on an existing tag, or a published release that a synchronisation of the tags from a git behind the cluster took for deleted. Nothing is changed: if it was published, publish it again from its edit page"
)

// ActionReleaseOldCommit says what the rebuild of a regenerable release row
// at another commit than its tag changes, for the operator.
const ActionReleaseOldCommit = "the release names another commit than its tag in the table: a synchronisation of the tags from a git behind the cluster wrote the tag's older commit. --apply writes it again at the tag's commit, as a push of the tag does"

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
			// table holds no live repository key for it: a deletion whose
			// wiki entry was lost left it alive beside the deleted
			// repository (#163).
			main := wiki + ".git"
			if _, deleted := r.table.Deleted(main); !inTable[main] || deleted {
				if known, err := repoKnownInDB(ctx, main); err == nil && !known {
					if m, ok := r.reconcileOrphan(main, orphans); ok {
						out = append(out, m)
					}
					continue
				}
			}
			stale, last, err := r.staleWiki(ctx, key)
			if err != nil {
				return out, err
			}
			if stale {
				out = append(out, Mismatch{
					Repo: key, Category: RepoLifecycle, Kind: "wiki", Action: actionStaleWiki,
					Index: last, Since: r.markOrphan(key, last, orphans),
				})
			}
			continue
		}
		_, deleted := r.table.Deleted(key)
		repo, err := lookupRepo(ctx, reftable.Event{Repo: key})
		if err != nil {
			if !deleted {
				m := Mismatch{Repo: key, Category: RepoLifecycle}
				if repo_model.IsErrRepoNotExist(err) {
					if o, ok := r.reconcileOrphan(key, orphans); ok {
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
		table := map[string]string{}
		for name, v := range refs {
			if v.Live() && v.SHA != "" && strings.HasPrefix(name, "refs/heads/") {
				table[strings.TrimPrefix(name, "refs/heads/")] = v.SHA
			}
		}
		rows, err := branchCommits(ctx, repo.ID)
		if err != nil {
			return out, err
		}
		var stale []string
		var repoMismatches []Mismatch
		for b, sha := range table {
			// A row at another commit than the table's is as stale as a
			// missing one: a push derived under a branch's old name, the
			// branch renamed meanwhile, left the row at the commit before
			// the push, and nothing else revisits it (#225).
			if commit, ok := rows[b]; !ok || commit != sha {
				repoMismatches = append(repoMismatches, Mismatch{Repo: key, Category: RefsAheadOfSQL, Kind: "branch", Ref: b, Regenerable: true})
				stale = append(stale, b)
			}
		}
		for b := range rows {
			if _, ok := table[b]; !ok {
				repoMismatches = append(repoMismatches, Mismatch{Repo: key, Category: SQLAheadOfRefs, Kind: "branch", Ref: b, Regenerable: true})
			}
		}
		sort.Slice(repoMismatches, func(i, j int) bool { return repoMismatches[i].Ref < repoMismatches[j].Ref })
		if apply && len(stale) > 0 && r.projector.Projected(key) >= at {
			synced := map[string]bool{}
			gitRepo, err := gitrepo.OpenRepository(ctx, repo)
			if err == nil {
				err = repo_service.SyncBranchesToDB(ctx, repo.ID, 0, stale, r.branchCommitAsTable(key, gitRepo, synced))
				gitRepo.Close()
			}
			if err == nil {
				for i := range repoMismatches {
					if repoMismatches[i].Category == RefsAheadOfSQL && synced[repoMismatches[i].Ref] {
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

// branchCommits returns the commit of each branch row of a repository not
// marked deleted, by name.
func branchCommits(ctx context.Context, repoID int64) (map[string]string, error) {
	var rows []*git_model.Branch
	cond := git_model.FindBranchOptions{RepoID: repoID, IsDeletedBranch: optional.Some(false)}.ToConds()
	if err := db.GetEngine(ctx).Cols("name", "commit_id").Where(cond).Find(&rows); err != nil {
		return nil, err
	}
	out := make(map[string]string, len(rows))
	for _, b := range rows {
		out[b.Name] = b.CommitID
	}
	return out, nil
}

// branchCommitAsTable reads a branch's commit in this node's git for the
// rebuild of its row, and records it in synced, only while the table holds
// the branch at that same commit. A write the table applied after
// reconcile read it, which this node's git may not show yet, would
// otherwise bring a row the consumer derived since back to an older
// commit. Such a branch reads as removed, which the rebuild skips, and is
// left to the next pass.
func (r *Replicator) branchCommitAsTable(key string, gitRepo *git.Repository, synced map[string]bool) func(string) (*git.Commit, error) {
	return func(name string) (*git.Commit, error) {
		commit, err := gitRepo.GetBranchCommit(name)
		if err != nil {
			return nil, err
		}
		if v, ok := r.table.Get(key, git.BranchPrefix+name); !ok || !v.Live() || v.SHA != commit.ID.String() {
			return nil, git.ErrNotExist{ID: name}
		}
		synced[name] = true
		return commit, nil
	}
}

// orphanMark is when this node's reconcile first saw an orphaned
// repository unchanged at an index.
type orphanMark struct {
	last  uint64
	since time.Time
}

// ActionDeleteOrphan is the audit action of `reconcile --delete-orphan`.
const ActionDeleteOrphan Action = "delete-orphan"

// actionOrphan is what the operator does with a repository the table holds
// alive and SQL does not know.
const actionOrphan = "the table holds this repository alive, SQL has no row for it. After a loss on the SQL side (a PostgreSQL failover, a point-in-time or backup restore) the table is its only copy: restore it, by adopting it (Site administration > Repositories > Unadopted, or POST /api/v1/admin/unadopted/{owner}/{repo}) once its owner exists. Only a deletion or a failed creation the cluster did not record is deleted, explicitly: `forgejo cluster reconcile --delete-orphan <owner>/<repo> --expect-index <index> --reason \"...\"`"

// reconcileOrphan reports a repository the table holds alive and SQL does
// not know, with its wiki: the files of a deletion or of a failed creation
// the cluster did not record (R-D4, R-D5), or a repository SQL lost. They
// look the same, and the second is only in the table: reconcile never
// deletes it, even with apply. The report carries the index of the keys'
// last change and since when this node saw them unchanged, for
// DeleteOrphan. It reports false when the table holds no live ref under
// either key.
func (r *Replicator) reconcileOrphan(key string, seen map[string]bool) (Mismatch, bool) {
	live, last := r.orphanKeys(key)
	if len(live) == 0 {
		return Mismatch{}, false
	}
	return Mismatch{
		Repo: key, Category: RepoLifecycle, Action: actionOrphan,
		Index: last, Since: r.markOrphan(key, last, seen),
	}, true
}

// markOrphan returns since when this node's reconcile saw key unchanged at
// last, and marks key seen in this pass.
func (r *Replicator) markOrphan(key string, last uint64, seen map[string]bool) string {
	seen[key] = true
	r.orphansMu.Lock()
	defer r.orphansMu.Unlock()
	if r.orphans == nil {
		r.orphans = map[string]orphanMark{}
	}
	mark, ok := r.orphans[key]
	if !ok || mark.last != last {
		mark = orphanMark{last: last, since: time.Now()}
		r.orphans[key] = mark
	}
	return mark.since.UTC().Format(time.RFC3339)
}

// actionStaleWiki is what the operator does with a wiki the table holds
// alive under the name of a repository SQL has, which belongs to another.
const actionStaleWiki = "the table holds this wiki alive, and it belongs to a deleted repository, not to the repository SQL has under the name: the deletion of the wiki was not recorded. Every node refuses the repository while it stays. Delete it, explicitly: `forgejo cluster reconcile --delete-orphan <owner>/<repo>.wiki --expect-index <index> --reason \"...\"`"

// staleWiki reports whether the wiki key holds, alive, the wiki of another
// repository than the one SQL has under its name, and the index of its
// last change: one whose repository key was deleted after it, or that was
// written under another repository ID than SQL's and than its repository
// key's (#163). A wiki and its repository that both keep a lost row's ID
// are the repository SQL lost, restored by adoption, not a stale wiki. It
// reports false when SQL has no row for the name: reconcileOrphan covers
// it.
func (r *Replicator) staleWiki(ctx context.Context, wikiKey string) (bool, uint64, error) {
	refs, wikiID, _ := r.table.RefsWithID(wikiKey)
	if _, deleted := r.table.Deleted(wikiKey); deleted || !anyLive(refs) {
		return false, 0, nil
	}
	_, last := r.table.RepoHash(wikiKey)
	main := strings.TrimSuffix(wikiKey, ".wiki.git") + ".git"
	repo, err := lookupRepo(ctx, reftable.Event{Repo: main})
	if repo_model.IsErrRepoNotExist(err) {
		return false, last, nil
	}
	if err != nil {
		return false, last, err
	}
	if d, ok := r.table.DeletedBefore(main, math.MaxUint64); ok && d > last {
		return true, last, nil
	}
	_, mainID, _ := r.table.RefsWithID(main)
	return wikiID > 0 && wikiID != repo.ID && mainID != wikiID, last, nil
}

// orphanKeys returns the keys of a repository and its wiki that hold a
// live ref, and the index of their last change.
func (r *Replicator) orphanKeys(key string) ([]string, uint64) {
	var live []string
	var last uint64
	for _, k := range []string{key, strings.TrimSuffix(key, ".git") + ".wiki.git"} {
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
	return live, last
}

// OrphanDeletion is what `reconcile --delete-orphan` deleted.
type OrphanDeletion struct {
	Deleted []string `json:"deleted"`
	Index   uint64   `json:"index"`
}

// DeleteOrphan deletes from the cluster a repository the table holds alive
// and SQL does not know, wiki included: an operator's explicit decision,
// for one repository, never implied by reconcile. It refuses unless this
// node's table is confirmed current, the keys did not change since the
// index the operator read in reconcile's report, and SQL, read last, still
// has no row for the repository. The decision is in the audit with its
// reason.
func (r *Replicator) DeleteOrphan(ctx context.Context, op consensus.Operator, repo string, expect uint64, reason string) (*OrphanDeletion, error) {
	if db.DefaultContext == nil {
		return nil, errors.New("database not ready")
	}
	if strings.TrimSpace(reason) == "" {
		return nil, errors.New("a reason is required: it is kept in the audit")
	}
	base, wikiOnly := strings.CutSuffix(strings.ToLower(strings.TrimSuffix(repo, ".git")), ".wiki")
	key := base + ".git"
	if err := reftable.ValidRepoKey(key); err != nil {
		return nil, fmt.Errorf("repository %q: %w", repo, err)
	}
	if r.Engine() == nil {
		return nil, errors.New("the cluster is not running: nothing is deleted")
	}
	if _, err := r.confirm(ctx); err != nil {
		return nil, fmt.Errorf("this node's table could not be confirmed current (%w): nothing is deleted", err)
	}
	if wikiOnly {
		return r.deleteStaleWiki(ctx, op, base+".wiki.git", expect, reason)
	}
	live, last := r.orphanKeys(key)
	if len(live) == 0 {
		return nil, fmt.Errorf("the table holds nothing alive under %s: nothing is deleted", key)
	}
	if last != expect {
		return nil, fmt.Errorf("%s changed at index %d, after the index %d given: run `forgejo cluster reconcile` again; nothing is deleted", key, last, expect)
	}
	known, err := repoKnownInDB(ctx, key)
	if err != nil {
		return nil, fmt.Errorf("reading SQL: %w; nothing is deleted", err)
	}
	if known {
		return nil, fmt.Errorf("SQL has a row for %s: it is no orphan; nothing is deleted", key)
	}
	res := &OrphanDeletion{Index: last}
	for _, k := range live {
		if err := r.proposeRepoDelete(ctx, k, 0); err != nil {
			r.audit(ctx, op, Operation{Action: ActionDeleteOrphan, Target: key}, "failed", fmt.Sprintf("%s (index %d, deleted %v): %v", reason, last, res.Deleted, err))
			return res, fmt.Errorf("deleting %s: %w", k, err)
		}
		res.Deleted = append(res.Deleted, k)
	}
	log.Warn("cluster: %s deleted %s, which SQL does not know, at index %d: %s", op, strings.Join(live, ", "), last, reason)
	r.audit(ctx, op, Operation{Action: ActionDeleteOrphan, Target: key}, "deleted", fmt.Sprintf("%s (index %d)", reason, last))
	return res, nil
}

// deleteStaleWiki is DeleteOrphan of a wiki alone, `<owner>/<repo>.wiki`:
// one the table holds alive while SQL has no row for its repository, or
// one staleWiki reports, the wiki of a deleted repository under the name
// of another (#163). SQL is read last, after the table was confirmed
// current. The repository's key is never touched.
func (r *Replicator) deleteStaleWiki(ctx context.Context, op consensus.Operator, wikiKey string, expect uint64, reason string) (*OrphanDeletion, error) {
	main := strings.TrimSuffix(wikiKey, ".wiki.git") + ".git"
	refs, _ := r.table.Refs(wikiKey)
	if _, deleted := r.table.Deleted(wikiKey); deleted || !anyLive(refs) {
		return nil, fmt.Errorf("the table holds nothing alive under %s: nothing is deleted", wikiKey)
	}
	if _, last := r.table.RepoHash(wikiKey); last != expect {
		return nil, fmt.Errorf("%s changed at index %d, after the index %d given: run `forgejo cluster reconcile` again; nothing is deleted", wikiKey, last, expect)
	}
	stale, last, err := r.staleWiki(ctx, wikiKey)
	if err != nil {
		return nil, fmt.Errorf("reading SQL: %w; nothing is deleted", err)
	}
	if !stale {
		known, err := repoKnownInDB(ctx, main)
		if err != nil {
			return nil, fmt.Errorf("reading SQL: %w; nothing is deleted", err)
		}
		if known {
			return nil, fmt.Errorf("%s is the wiki of the repository SQL has under the name: it is no orphan; nothing is deleted", wikiKey)
		}
	}
	res := &OrphanDeletion{Index: last}
	if err := r.proposeRepoDelete(ctx, wikiKey, 0); err != nil {
		r.audit(ctx, op, Operation{Action: ActionDeleteOrphan, Target: wikiKey}, "failed", fmt.Sprintf("%s (index %d): %v", reason, last, err))
		return res, fmt.Errorf("deleting %s: %w", wikiKey, err)
	}
	res.Deleted = append(res.Deleted, wikiKey)
	log.Warn("cluster: %s deleted %s, which no repository SQL has owns, at index %d: %s", op, wikiKey, last, reason)
	r.audit(ctx, op, Operation{Action: ActionDeleteOrphan, Target: wikiKey}, "deleted", fmt.Sprintf("%s (index %d)", reason, last))
	return res, nil
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
// open pull requests whose head branch is gone, releases whose tag is gone,
// drafts without a commit whose tag the table holds. They are reported
// with the operator's action, never changed. Tag-only release rows are
// regenerable and, with apply, rebuilt from git. So is a published release
// or tag-only row a synchronisation from a git behind left at another
// commit than its tag in the table (#341), rebuilt from the tag once git
// shows it at the table's value.
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
	var behind []*repo_model.Release
	for _, rel := range rels {
		sqlTags[rel.TagName] = true
		v, ok := live("refs/tags/" + rel.TagName)
		if rel.IsDraft {
			// A draft without a commit is how the deletion of its tag
			// leaves a release (PushUpdateDeleteTag): while the table
			// holds the tag, a synchronisation from a git behind may have
			// taken it for deleted (#341). It is also how its author saves
			// a draft, on an existing tag or not (createTag sets the
			// commit of a published release only), and nothing in the row
			// tells the two apart: it is reported, never published. A
			// published release its author turned draft keeps its commit.
			if ok && !rel.IsTag && rel.Sha1 == "" {
				out = append(out, Mismatch{Repo: key, Category: RefsAheadOfSQL, Kind: "release", Ref: rel.TagName, Action: ActionReleaseDraftTagHeld})
			}
			continue
		}
		if !ok {
			m := Mismatch{Repo: key, Category: SQLAheadOfRefs, Kind: "tag", Ref: rel.TagName, Regenerable: rel.IsTag}
			if !rel.IsTag {
				m.Kind, m.Action = "release", ActionReleaseNoTag
			}
			out = append(out, m)
			continue
		}
		if rel.Sha1 != v.SHA {
			// Equal, the tag is lightweight and the row at its commit;
			// else the tag may be annotated, its object peeled below.
			behind = append(behind, rel)
		}
	}
	stale, behindRows := releasesBehindTags(ctx, key, dir, refs, behind)
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
		r.rebuildTagRows(ctx, key, repo, out)
	}
	if apply && len(stale) > 0 {
		r.rebuildReleases(ctx, key, repo, stale, behindRows)
	}
	out = append(out, stale...)
	sort.SliceStable(out, func(i, j int) bool { return out[i].Kind+out[i].Ref < out[j].Kind+out[j].Ref })
	return out, nil
}

// rebuildTagRows writes the regenerable tag-only rows reported in out, and
// only those: a tag the table holds without a row is added as a push of it
// does (PushUpdateAddTag), read in git only while the table holds it at
// that same value; a tag-only row whose tag the table does not hold goes,
// as a deletion of the tag does. SyncReleasesWithTags would also publish
// every draft whose tag git holds, a draft its author saved included, and
// turn into a draft a release whose tag is gone, which reconcile reports
// and never changes (#341).
func (r *Replicator) rebuildTagRows(ctx context.Context, key string, repo *repo_model.Repository, out []Mismatch) {
	gitRepo, err := gitrepo.OpenRepository(ctx, repo)
	if err != nil {
		log.Warn("cluster: reconcile of %s: %v", key, err)
		return
	}
	defer gitRepo.Close()
	for i := range out {
		if out[i].Kind != "tag" || !out[i].Regenerable {
			continue
		}
		ref := git.TagPrefix + out[i].Ref
		v, held := r.table.Get(key, ref)
		held = held && v.Live()
		switch out[i].Category {
		case RefsAheadOfSQL:
			sha, err := gitRepo.GetTagID(out[i].Ref)
			if err != nil || !held || v.SHA != sha {
				continue
			}
			// A row written since, a draft its author saved on the tag
			// above all, would be published.
			if exists, err := repo_model.IsReleaseExist(ctx, repo.ID, out[i].Ref); err != nil || exists {
				continue
			}
			if err := repo_module.PushUpdateAddTag(ctx, repo, gitRepo, out[i].Ref, sha, ref); err != nil {
				log.Warn("cluster: reconcile of %s: the row of %s: %v", key, ref, err)
				continue
			}
		case SQLAheadOfRefs:
			if held {
				continue
			}
			// A release made of the row since would be turned into a
			// draft: only a row still tag-only goes.
			if rel, err := repo_model.GetRelease(ctx, repo.ID, out[i].Ref); err != nil || !rel.IsTag {
				continue
			}
			if err := repo_model.PushUpdateDeleteTag(ctx, repo, out[i].Ref); err != nil {
				log.Warn("cluster: reconcile of %s: the row of %s: %v", key, ref, err)
				continue
			}
		default:
			continue
		}
		out[i].Applied = true
	}
}

// releasesBehindTags reports, among rels, the published release and
// tag-only rows whose commit is not the one their tag's value in the table
// peels to, as a synchronisation of the tags from a git behind the cluster
// leaves a moved tag's row (#341), with the rows reported, in the same
// order. Neither the consumer, its work done, nor a later push revisits
// them. The tag's value is peeled in this node's git; a tag whose object
// it lacks is not compared, `forgejo cluster objects verify` reports it.
func releasesBehindTags(ctx context.Context, key, dir string, refs map[string]reftable.RefValue, rels []*repo_model.Release) ([]Mismatch, []*repo_model.Release) {
	if len(rels) == 0 {
		return nil, nil
	}
	shas := make([]string, 0, len(rels))
	for _, rel := range rels {
		shas = append(shas, refs[git.TagPrefix+rel.TagName].SHA)
	}
	commits, err := peelCommits(ctx, dir, shas)
	if err != nil {
		log.Warn("cluster: reconcile of %s: the commits of its tags: %v", key, err)
	}
	var out []Mismatch
	var rows []*repo_model.Release
	for _, rel := range rels {
		commit, ok := commits[refs[git.TagPrefix+rel.TagName].SHA]
		if !ok || commit == rel.Sha1 {
			continue
		}
		m := Mismatch{Repo: key, Category: RefsAheadOfSQL, Kind: "release", Ref: rel.TagName, Regenerable: true, Action: ActionReleaseOldCommit}
		if rel.IsTag {
			m.Kind = "tag"
		}
		out = append(out, m)
		rows = append(rows, rel)
	}
	return out, rows
}

// peelCommits returns the commit each of shas, a tag object or a commit,
// peels to in dir, by sha. A sha whose object dir lacks, or that peels to
// no commit, is absent.
func peelCommits(ctx context.Context, dir string, shas []string) (map[string]string, error) {
	commits := make(map[string]string, len(shas))
	if len(shas) == 0 {
		return commits, nil
	}
	var in strings.Builder
	for _, sha := range shas {
		in.WriteString(sha + "^{commit}\n")
	}
	out, err := gitRun(ctx, dir, strings.NewReader(in.String()), "cat-file", "--batch-check=%(objectname)")
	if err != nil {
		return commits, err
	}
	// One line per line of input, in its order: the commit, or the input
	// followed by "missing".
	lines := strings.Split(strings.TrimSuffix(out, "\n"), "\n")
	for i, sha := range shas {
		if i < len(lines) {
			if f := strings.Fields(lines[i]); len(f) == 1 {
				commits[sha] = f[0]
			}
		}
	}
	return commits, nil
}

// rebuildReleases writes again the release rows releasesBehindTags
// reported, rows, from this node's git, as a push of their tag does
// (PushUpdateAddTag): the row takes its tag's commit. A tag is read in git
// only while the table holds it at that same value, as branchCommitAsTable
// reads a branch: a write the table applied after reconcile read it, which
// this node's git may not show yet, would otherwise bring a row the
// consumer derived since back to an older commit. A row changed since it
// was read, turned into a draft by its author above all, which
// PushUpdateAddTag would publish, is not written either. Such a row is
// left to the next pass.
func (r *Replicator) rebuildReleases(ctx context.Context, key string, repo *repo_model.Repository, stale []Mismatch, rows []*repo_model.Release) {
	gitRepo, err := gitrepo.OpenRepository(ctx, repo)
	if err != nil {
		log.Warn("cluster: reconcile of %s: %v", key, err)
		return
	}
	defer gitRepo.Close()
	for i := range stale {
		ref := git.TagPrefix + stale[i].Ref
		sha, err := gitRepo.GetTagID(stale[i].Ref)
		if err != nil {
			continue
		}
		if v, ok := r.table.Get(key, ref); !ok || !v.Live() || v.SHA != sha {
			continue
		}
		now, err := repo_model.GetReleaseByID(ctx, rows[i].ID)
		if err != nil || now.IsDraft || now.IsTag != rows[i].IsTag || now.Sha1 != rows[i].Sha1 || now.TagName != rows[i].TagName {
			continue
		}
		if err := repo_module.PushUpdateAddTag(ctx, repo, gitRepo, stale[i].Ref, sha, ref); err != nil {
			log.Warn("cluster: reconcile of %s: the release of %s: %v", key, ref, err)
			continue
		}
		stale[i].Applied = true
	}
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
