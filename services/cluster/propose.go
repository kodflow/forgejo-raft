// Copyright 2026 The Forgejo Authors. All rights reserved.
// SPDX-License-Identifier: GPL-3.0-or-later

package cluster

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"strings"
	"time"

	"forgejo.org/modules/clusterhook"
	"forgejo.org/modules/log"
	"forgejo.org/services/cluster/consensus"
	"forgejo.org/services/cluster/reftable"
)

// prepared is the hook's "prepared" phase: the whole git transaction becomes
// one entry, proposed after its objects are on a majority, and git publishes
// nothing unless the entry committed and Apply accepted it (ADR 0152 D19,
// R1-03, R1-05). The write git performs afterwards is a pre-write the
// projector reconciles.
func (r *Replicator) prepared(ctx context.Context, req *clusterhook.Request) *clusterhook.Response {
	repo, err := r.repoKey(req.GitDir)
	if err != nil {
		return fail(err)
	}
	tx, refs := r.buildTxn(ctx, repo, req)
	if len(tx.Updates) == 0 {
		// Only pseudo-refs outside refs/, which the table does not hold.
		return &clusterhook.Response{OK: true}
	}
	if err := r.registry.register(req.Nonce, repo, refs, req.PID); err != nil {
		return fail(err)
	}
	// A checkpoint reading PostgreSQL's position holds this briefly.
	r.fence.RLock()
	resp := r.propose(ctx, repo, req, tx)
	r.fence.RUnlock()
	if !resp.OK {
		// A refusal deregisters before answering git.
		r.registry.release(req.Nonce)
	}
	return resp
}

// buildTxn turns the hook's lines into a proposal, reading each ref's
// revision from the table.
func (r *Replicator) buildTxn(ctx context.Context, repo string, req *clusterhook.Request) (*reftable.RefTxn, []string) {
	tx := &reftable.RefTxn{
		Repo: repo, RepoID: req.RepoID, PusherID: req.PusherID, Time: time.Now().UnixNano(),
		Nonce: req.Nonce, Trigger: req.Trigger, PRID: req.PRID, IsWiki: req.IsWiki, PushOptions: req.PushOptions,
	}
	var refs []string
	for _, l := range req.Lines {
		if l.Ref != "HEAD" && !strings.HasPrefix(l.Ref, "refs/") {
			continue
		}
		u := reftable.RefUpdate{Ref: l.Ref}
		switch {
		case strings.HasPrefix(l.New, "ref:"):
			u.Symbolic = strings.TrimPrefix(l.New, "ref:")
		case reftable.IsZeroSHA(l.New):
			u.Delete = true
		default:
			u.New = l.New
			if len(l.New) == 64 {
				tx.Format = "sha256"
			}
		}
		if !strings.HasPrefix(l.Old, "ref:") && !reftable.IsZeroSHA(l.Old) {
			u.Old = l.Old
		}
		if v, ok := r.table.Get(repo, l.Ref); ok {
			u.ExpectRev = v.Rev
		}
		tx.Updates = append(tx.Updates, u)
		refs = append(refs, l.Ref)
	}
	if tx.Format == "" {
		cur, _ := r.table.Refs(repo)
		for _, v := range cur {
			if v.Format != "" {
				tx.Format = v.Format
				break
			}
		}
	}
	if tx.Format == "" {
		// A new repository's first entry is often HEAD alone: ask git.
		tx.Format = "sha1"
		if out, err := gitRun(ctx, req.GitDir, nil, "rev-parse", "--show-object-format"); err == nil && strings.TrimSpace(out) == "sha256" {
			tx.Format = "sha256"
		}
	}
	return tx, refs
}

// errNoLeader is what a push reads on a node whose cluster cannot commit:
// a majority of its members is out of its reach.
var errNoLeader = errors.New("this node of the cluster reaches no leader with a majority: the cluster takes no write here until a majority is back; nothing was written")

// writeLeaderWait is how long a push waits for the cluster to commit before
// it is turned away: an election after a lost leader takes a few seconds,
// and a push must ride it out rather than fail.
var writeLeaderWait = 15 * time.Second

// WriteRefusal says why this node cannot take a push now, or nil. The
// pre-receive hook asks it before git updates anything: the reference-
// transaction hook refuses the same push later, but git then dies, and the
// client of a smart HTTP push reads a disconnect instead of the reason. The
// pre-receive hook's answer reaches it. The test is a barrier, which only a
// leader answered by a majority completes: a node that still believes in a
// leader just killed or cut off would pass a mere "leader known". A push
// that passes may still fail later, at the proposal; this only turns away
// the pushes that cannot succeed.
func (r *Replicator) WriteRefusal(ctx context.Context) error {
	deadline := time.Now().Add(writeLeaderWait)
	for {
		eng := r.Engine()
		if eng == nil || !r.projector.Ready() {
			return errors.New("this node of the cluster is not ready; retry shortly")
		}
		if h := eng.Health(); h.Stopped {
			return fmt.Errorf("this node of the cluster stopped (%s); nothing was written", h.StopReason)
		}
		if probeLeader(ctx, eng) {
			return nil
		}
		if ctx.Err() != nil || !time.Now().Before(deadline) {
			return errNoLeader
		}
		select {
		case <-ctx.Done():
			return errNoLeader
		case <-time.After(100 * time.Millisecond):
		}
	}
}

func (r *Replicator) propose(ctx context.Context, repo string, req *clusterhook.Request, tx *reftable.RefTxn) *clusterhook.Response {
	eng := r.Engine()
	if eng == nil || !r.projector.Ready() {
		return fail(errors.New("this node of the cluster is not ready; retry shortly"))
	}
	if r.table.EventBytes() > reftable.MaxEventBytes*9/10 {
		return fail(errors.New("the cluster's event queue is nearly full; the push is refused before anything is written"))
	}
	// Before the objects travel: a learner no leader answers, and a leader
	// whose writer hook has not completed, take no write.
	if err := r.learnerGate(ctx, eng); err != nil {
		return fail(err)
	}
	if err := r.writerGate(eng); err != nil {
		return fail(err)
	}

	// Objects before refs: written and synced on a majority first.
	var wants, haves []string
	for _, u := range tx.Updates {
		if u.New != "" {
			wants = append(wants, u.New)
		}
	}
	if len(wants) > 0 {
		cur, _ := r.table.Refs(repo)
		for _, v := range cur {
			if v.Live() && v.SHA != "" {
				haves = append(haves, v.SHA)
			}
		}
		dir := filepath.Join(r.root, filepath.FromSlash(repo))
		if err := r.objects.shipToMajority(ctx, repo, dir, tx.Format, wants, haves); err != nil {
			objectShipsTotal.WithLabelValues("failed").Inc()
			return fail(fmt.Errorf("%w; nothing was written", err))
		}
		objectShipsTotal.WithLabelValues("ok").Inc()
	}

	data, err := reftable.Encode(reftable.KindRefTxn, tx)
	if err != nil {
		return fail(err)
	}
	return r.proposeWithRetry(ctx, repo, req.Nonce, tx, data)
}

// retryDelay paces the retries of a proposal while a leader is elected.
const retryDelay = 150 * time.Millisecond

// proposeWithRetry proposes a ref transaction and keeps at it, within the
// request's budget, while the cluster has a majority but no leader yet: a
// leader lost to a stopped node must not fail a push the two survivors can
// commit. Retrying is safe because the proposal names the revisions it
// expects: a copy of an attempt that did commit is refused by Apply, and
// the table then shows our values, which is success. An unknown outcome is
// resolved from the table after a barrier, never by retrying blindly (R1-04).
func (r *Replicator) proposeWithRetry(ctx context.Context, repo, nonce string, tx *reftable.RefTxn, data []byte) *clusterhook.Response {
	eng := r.Engine()
	maybeCommitted := false
	var lastErr error
	for {
		tok, res, err := eng.Propose(ctx, data)
		if err == nil {
			result, _ := res.(reftable.Result)
			if result.OK || (maybeCommitted && r.committed(repo, tx)) {
				proposalsTotal.WithLabelValues("committed").Inc()
				return r.proposed(repo, nonce, tok)
			}
			proposalsTotal.WithLabelValues("refused").Inc()
			return fail(fmt.Errorf("the cluster refused the write: %s", result.Reason))
		}
		lastErr = err
		switch {
		case retryable(err):
			// Certainly not appended: wait for a leader and propose again.
		case consensus.Certain(err):
			proposalsTotal.WithLabelValues("error").Inc()
			return fail(fmt.Errorf("the cluster refused the write (%v); nothing was written", err))
		default:
			// Appended or possibly delivered: settle it from the table.
			proposalsTotal.WithLabelValues("unknown").Inc()
			maybeCommitted = true
			tok, cerr := r.confirm(ctx)
			if cerr != nil {
				log.Error("cluster: outcome of a write to %s unknown and unresolved: %v (%v)", repo, err, cerr)
				return fail(fmt.Errorf("the outcome of the write is unknown (%v); retry, the cluster will refuse a duplicate", err))
			}
			if r.committed(repo, tx) {
				proposalsTotal.WithLabelValues("committed").Inc()
				return r.proposed(repo, nonce, tok)
			}
			// Absent after a confirmed barrier: it never commits, propose again.
		}
		select {
		case <-ctx.Done():
			return fail(fmt.Errorf("the cluster found no leader in time (%v); nothing was written", lastErr))
		case <-time.After(retryDelay):
		}
	}
}

// retryable says nothing was appended and a leader may come.
func retryable(err error) bool {
	return errors.Is(err, consensus.ErrNotLeader) || errors.Is(err, consensus.ErrLeadershipUnconfirmed) ||
		errors.Is(err, consensus.ErrEntrySuperseded) || errors.Is(err, consensus.ErrOverloaded)
}

// confirm applies a commit boundary confirmed by a quorum, retrying while a
// leader is elected, within ctx.
func (r *Replicator) confirm(ctx context.Context) (consensus.Token, error) {
	eng := r.Engine()
	for {
		tok, err := eng.Barrier(ctx)
		if err == nil {
			err = eng.WaitApplied(ctx, tok)
		}
		if err == nil {
			return tok, nil
		}
		select {
		case <-ctx.Done():
			return consensus.Token{}, err
		case <-time.After(retryDelay):
		}
	}
}

// committed reports whether every ref of tx holds its new value at the
// revision it expected plus one.
func (r *Replicator) committed(repo string, tx *reftable.RefTxn) bool {
	for _, u := range tx.Updates {
		v, ok := r.table.Get(repo, u.Ref)
		if !ok || v.Rev != u.ExpectRev+1 {
			return false
		}
		switch {
		case u.Delete && !v.Tombstone,
			u.Symbolic != "" && v.Symbolic != u.Symbolic,
			u.New != "" && v.SHA != u.New:
			return false
		}
	}
	return true
}

func (r *Replicator) proposed(repo, nonce string, tok consensus.Token) *clusterhook.Response {
	r.registry.markProposed(nonce)
	r.recordRaw(nonce, repo, tok)
	r.signalConsumer()
	return &clusterhook.Response{OK: true}
}

// finished releases a transaction after git committed or aborted it, and
// has the projector check git against the table.
func (r *Replicator) finished(req *clusterhook.Request) {
	t := r.registry.release(req.Nonce)
	repo := ""
	if t != nil {
		repo = t.repo
	} else if k, err := r.repoKey(req.GitDir); err == nil {
		repo = k
	}
	if repo != "" {
		r.projector.markDirty(repo)
	}
}

// proposeRepoDelete tombstones a deleted repository's refs; every member's
// projector then removes it.
func (r *Replicator) proposeRepoDelete(ctx context.Context, key string, repoID int64) error {
	return r.proposeLifecycle(ctx, reftable.KindRepoDelete, &reftable.RepoDelete{Repo: key, RepoID: repoID, Time: time.Now().UnixNano()},
		func() bool { _, deleted := r.table.Deleted(key); return deleted })
}

// proposeRepoRename moves a repository's refs to its new key; every
// projector, the origin's included, moves the directory.
func (r *Replicator) proposeRepoRename(ctx context.Context, from, to string) error {
	return r.proposeRepoMoves(ctx, [][2]string{{from, to}})
}

// proposeRepoMoves moves several repositories in ONE entry, all or none: a
// repository and its wiki, or an owner's repositories.
func (r *Replicator) proposeRepoMoves(ctx context.Context, moves [][2]string) error {
	if len(moves) == 0 {
		return nil
	}
	rn := &reftable.RepoRename{From: moves[0][0], To: moves[0][1], Time: time.Now().UnixNano(), Also: moves[1:]}
	return r.proposeLifecycle(ctx, reftable.KindRepoRename, rn, func() bool {
		for _, m := range moves {
			if f, _, ok := r.table.RenamedFrom(m[1]); !ok || f != m[0] {
				return false
			}
		}
		return true
	})
}

func (r *Replicator) proposeLifecycle(ctx context.Context, kind reftable.Kind, body any, landed func() bool) error {
	data, err := reftable.Encode(kind, body)
	if err != nil {
		return err
	}
	r.fence.RLock()
	_, result, err := r.proposeCommand(ctx, data, landed)
	r.fence.RUnlock()
	if err != nil {
		return fmt.Errorf("the cluster did not record it: %w", err)
	}
	if !result.OK {
		return fmt.Errorf("the cluster refused it: %s", result.Reason)
	}
	r.signalConsumer()
	return nil
}

// proposeOwnerRename moves every repository the table holds under an owner
// in one entry: they all move, or none does and Forgejo moves the directory
// back.
func (r *Replicator) proposeOwnerRename(ctx context.Context, oldOwner, newOwner string) error {
	prefix := oldOwner + "/"
	var moves [][2]string
	for _, key := range r.table.Repos() {
		if !strings.HasPrefix(key, prefix) {
			continue
		}
		if _, deleted := r.table.Deleted(key); deleted {
			continue
		}
		moves = append(moves, [2]string{key, newOwner + "/" + strings.TrimPrefix(key, prefix)})
	}
	return r.proposeRepoMoves(ctx, moves)
}

// commandBudget bounds a command's retries when its caller set no deadline.
const commandBudget = 30 * time.Second

// proposeCommand proposes a command and keeps at it through a leader
// election, as proposeWithRetry does for ref transactions. landed reports,
// from the table, whether the command already took effect: it settles an
// unknown outcome, and a copy refused because the first attempt committed.
// A nil landed marks an idempotent command, which is simply proposed again.
func (r *Replicator) proposeCommand(ctx context.Context, data []byte, landed func() bool) (consensus.Token, reftable.Result, error) {
	if _, ok := ctx.Deadline(); !ok {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, commandBudget)
		defer cancel()
	}
	eng := r.Engine()
	if eng == nil {
		return consensus.Token{}, reftable.Result{}, errors.New("the cluster is not running")
	}
	maybeCommitted := false
	for {
		tok, res, err := eng.Propose(ctx, data)
		if err == nil {
			result, _ := res.(reftable.Result)
			if !result.OK && maybeCommitted && landed != nil && landed() {
				result = reftable.Result{OK: true}
			}
			return tok, result, nil
		}
		switch {
		case retryable(err):
		case consensus.Certain(err):
			return tok, reftable.Result{}, err
		default:
			maybeCommitted = true
			ctok, cerr := r.confirm(ctx)
			if cerr != nil {
				return tok, reftable.Result{}, fmt.Errorf("%w (unresolved: %v)", err, cerr)
			}
			if landed != nil && landed() {
				return ctok, reftable.Result{OK: true}, nil
			}
		}
		select {
		case <-ctx.Done():
			return tok, reftable.Result{}, fmt.Errorf("no leader in time: %w", err)
		case <-time.After(retryDelay):
		}
	}
}
