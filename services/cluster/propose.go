// Copyright 2026 The Forgejo Authors. All rights reserved.
// SPDX-License-Identifier: GPL-3.0-or-later

package cluster

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
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
	nested := r.registry.takeNested(req.Nonce)
	if !nested && req.Nonce != "" && packedRefsTxn(req.GitDir, req.Lines) {
		// The files backend's own transaction on packed-refs: the one that
		// follows carries the deletion.
		r.registry.markNested(req.Nonce)
		return &clusterhook.Response{OK: true}
	}
	if err := r.pushTargetGone(repo, req); err != nil {
		return fail(err)
	}
	tx, refs, err := r.buildTxn(ctx, repo, req)
	if err != nil {
		return fail(err)
	}
	if len(tx.Updates) == 0 {
		// Only pseudo-refs outside refs/, which the table does not hold.
		return &clusterhook.Response{OK: true}
	}
	if len(tx.Updates) > reftable.MaxUpdatesPerTxn {
		// Apply would refuse it, after its objects went to a majority for
		// nothing (#80).
		return fail(fmt.Errorf("the transaction updates %d refs and the cluster takes at most %d in one; push them in several smaller pushes; nothing was written", len(tx.Updates), reftable.MaxUpdatesPerTxn))
	}
	if req.PusherID != 0 {
		if err := gitWillApply(ctx, req.GitDir, req.Lines); err != nil {
			return fail(err)
		}
	}
	if err := r.registry.register(req.Nonce, repo, refs, req.PID, nested); err != nil {
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

// packedRefsTxn reports whether the hook's lines are the nested transaction
// git's files backend runs on packed-refs when it deletes a packed ref:
// every line with two null ids, each ref in packed-refs. git runs it inside
// the transaction that deletes the refs, and calls the hook for it first:
// preparing(X->0), preparing(0->0), prepared(0->0), prepared(X->0),
// committed(0->0), committed(X->0) (git 2.48 to 2.56, update-ref -d,
// branch -D, branch -m and a push alike). It carries no value: proposed,
// its 0->0 would tombstone the ref with no check of its old value, before
// the deletion git itself checks (#37). A deletion without an old value
// (update-ref -d, branch -D) shows the same lines in its own transaction,
// which comes right after the nested one: the registry tells them apart by
// their order under the nonce (markNested, takeNested). One of a ref only
// loose, which packed-refs does not hold, is aborted by git before it is
// prepared.
func packedRefsTxn(gitDir string, lines []clusterhook.Line) bool {
	if len(lines) == 0 {
		return false
	}
	for _, l := range lines {
		if !reftable.IsZeroSHA(l.Old) || !reftable.IsZeroSHA(l.New) || !strings.HasPrefix(l.Ref, "refs/") {
			return false
		}
	}
	b, err := os.ReadFile(filepath.Join(gitDir, "packed-refs"))
	if err != nil {
		return false
	}
	packed := map[string]bool{}
	for line := range strings.SplitSeq(string(b), "\n") {
		if _, name, ok := strings.Cut(line, " "); ok && line[0] != '#' && line[0] != '^' {
			packed[name] = true
		}
	}
	for _, l := range lines {
		if !packed[l.Ref] {
			return false
		}
	}
	return true
}

// allZeroLines reports whether every line carries two null ids.
func allZeroLines(lines []clusterhook.Line) bool {
	for _, l := range lines {
		if !reftable.IsZeroSHA(l.Old) || !reftable.IsZeroSHA(l.New) {
			return false
		}
	}
	return len(lines) > 0
}

// pushTargetGone refuses a push into a key the table deleted or renamed
// away: a push still running when the repository was deleted or renamed on
// another node. Its new branch would bring the key back to life under a
// name the database no longer has, reported a success and then lost, its
// objects moved or removed with the directory. Forgejo creating a
// repository again under a freed name is no push: `git init` writes HEAD
// without a pusher, which revives the key. A deletion or rename this node
// has not applied yet is not seen here; this closes the window down to the
// proposal's own latency.
func (r *Replicator) pushTargetGone(repo string, req *clusterhook.Request) error {
	if req.PusherID == 0 {
		return nil
	}
	if _, deleted := r.table.Deleted(repo); deleted {
		return fmt.Errorf("the repository %s was deleted in the cluster; nothing was written", repo)
	}
	if to, moved := r.table.MovedAway(repo); moved {
		return fmt.Errorf("the repository %s was renamed to %s in the cluster: push there; nothing was written", repo, to)
	}
	return nil
}

// gitWillApply refuses a push transaction git is about to reject in part.
// From git 2.54, receive-pack runs a non-atomic push as ONE transaction that
// tolerates a failure per ref, and the hook reads the rejected refs too, in
// "prepared" and in "committed" (measured with git 2.54.0 and 2.56.0): a ref
// the client reads "[remote rejected]" would still be committed by the
// cluster, then projected on every member and announced to webhooks. In
// "prepared" git holds the lock of every ref it updates, written with the
// new value, and has not moved the ref yet. A line whose ref does not hold
// its old value, or whose lock is missing or holds another value (another
// writer's, or a lock left behind), is one git rejects: the whole
// transaction is refused then, nothing written, and the client reads every
// ref refused. Only pushes are checked: receive-pack always names the old
// value, Forgejo's own writes do not. A deletion's lock is empty, so a lock
// holding a value is another writer's; a lock older than staleLockAge was
// left behind. The reftable backend keeps no lock per ref: the old value
// and the directory and file conflicts with this node's refs are checked.
// receive-pack also runs a nested transaction on packed-refs for a deletion,
// whose lines carry two null ids: it is not checked.
func gitWillApply(ctx context.Context, dir string, lines []clusterhook.Line) error {
	var checked []clusterhook.Line
	for _, l := range lines {
		if !strings.HasPrefix(l.Ref, "refs/") || strings.HasPrefix(l.Old, "ref:") || strings.HasPrefix(l.New, "ref:") ||
			(reftable.IsZeroSHA(l.Old) && reftable.IsZeroSHA(l.New)) {
			continue
		}
		if err := reftable.ValidRefName(l.Ref); err != nil {
			return fmt.Errorf("%s: %w; nothing was written", l.Ref, err)
		}
		checked = append(checked, l)
	}
	if len(checked) == 0 {
		return nil
	}
	cur, err := readGitRefs(ctx, dir)
	if err != nil {
		return fmt.Errorf("reading the refs git is about to update: %w; nothing was written", err)
	}
	_, statErr := os.Stat(filepath.Join(dir, "reftable"))
	perRefLocks := errors.Is(statErr, os.ErrNotExist)
	for _, l := range checked {
		v, exists := cur[l.Ref]
		switch {
		case reftable.IsZeroSHA(l.Old) && exists:
			return errGitRejects{ref: l.Ref, why: "it already exists on this node"}
		case !reftable.IsZeroSHA(l.Old) && (!exists || v.SHA != l.Old):
			return errGitRejects{ref: l.Ref, why: "it does not hold the value the push expects on this node"}
		}
		if conflict := gitNameConflict(cur, checked, l); conflict != "" {
			return errGitRejects{ref: l.Ref, why: "it conflicts with " + conflict + " on this node"}
		}
		if !perRefLocks {
			continue
		}
		lock := filepath.Join(dir, filepath.FromSlash(l.Ref)) + ".lock"
		b, err := os.ReadFile(lock)
		if err != nil {
			return errGitRejects{ref: l.Ref, why: "git holds no lock on it"}
		}
		held := strings.TrimSuffix(string(b), "\n")
		switch {
		case reftable.IsZeroSHA(l.New) && held != "":
			// git's lock for a deletion is empty: a value is another
			// writer's update.
			return errGitRejects{ref: l.Ref, why: "another writer holds its lock"}
		case !reftable.IsZeroSHA(l.New) && held != l.New:
			return errGitRejects{ref: l.Ref, why: "another writer holds its lock"}
		}
		if st, err := os.Stat(lock); err != nil || time.Since(st.ModTime()) > staleLockAge {
			// git took its locks moments before calling the hook: an older
			// lock was left behind, and git, which could not take it,
			// rejects the ref, an empty one for a deletion included.
			return errGitRejects{ref: l.Ref, why: "a lock left behind blocks it"}
		}
	}
	return nil
}

// staleLockAge is how old a ref lock may be and still be git's own: git
// locks every ref of the transaction, then runs the hook. A stranger's lock
// younger than that, empty for a deletion, cannot be told apart.
var staleLockAge = 2 * time.Minute

// gitNameConflict names a ref of this node's git that a created or updated
// ref of the push conflicts with as a directory and a file
// (refs/heads/a against refs/heads/a/b), once the push's own deletions are
// gone. git rejects such a ref in part, the reftable backend included,
// which keeps no lock per ref; the table checks its own state, which a
// node behind on its projection does not share.
func gitNameConflict(cur map[string]reftable.ProjectedRef, lines []clusterhook.Line, l clusterhook.Line) string {
	if reftable.IsZeroSHA(l.New) {
		return ""
	}
	deleted := map[string]bool{}
	for _, x := range lines {
		if reftable.IsZeroSHA(x.New) {
			deleted[x.Ref] = true
		}
	}
	for i := strings.IndexByte(l.Ref, '/'); i > 0; {
		if p := l.Ref[:i]; !deleted[p] {
			if _, ok := cur[p]; ok {
				return p
			}
		}
		j := strings.IndexByte(l.Ref[i+1:], '/')
		if j < 0 {
			break
		}
		i += 1 + j
	}
	conflict := ""
	for name := range cur {
		if strings.HasPrefix(name, l.Ref+"/") && !deleted[name] && (conflict == "" || name < conflict) {
			conflict = name
		}
	}
	return conflict
}

// errGitRejects is a push git would reject in part: the cluster refuses
// all of it rather than commit a ref the client reads as rejected.
type errGitRejects struct{ ref, why string }

func (e errGitRejects) Error() string {
	return fmt.Sprintf("the push is refused: git on this node rejects %s (%s), and the cluster commits a push whole or not at all; nothing was written, push again", e.ref, e.why)
}

// errStaleCreate refuses a push that creates a ref the table already holds:
// git on this node had not projected it yet, and the cluster must not take
// a creation for an overwrite.
type errStaleCreate struct{ ref string }

func (e errStaleCreate) Error() string {
	return fmt.Sprintf("%s already exists in the cluster, this node had not shown it yet: fetch, then push again; nothing was written", e.ref)
}

// buildTxn turns the hook's lines into a proposal, reading each ref's
// revision from the table.
//
// The entry's nonce names this transaction, not the git process: a git
// before 2.54 runs one transaction per ref of a non-atomic push under one
// process nonce, and committed() would take a sister transaction's commit
// for this one's (TxnNonce). The registry and the read-after-write cookie
// keep the process nonce.
//
// A push (a pusher is named) that creates a ref gives git a zero old value,
// which receive-pack reads as "must not exist" and checks against THIS
// node's git only. When the table already holds a live value, git was
// behind the table: proposing with the table's revision would overwrite a
// branch the pusher never saw, a non-fast-forward no one forced. Such a
// line is refused. The revision read here is the one Apply checks, so a
// ref that becomes live after this check is refused by Apply instead.
// Forgejo's own updates without an old value (update-ref, a forced
// rewrite) carry no pusher and keep their meaning.
func (r *Replicator) buildTxn(ctx context.Context, repo string, req *clusterhook.Request) (*reftable.RefTxn, []string, error) {
	// Read before the revisions: Apply refuses the transaction when the
	// repository left the key after this index (reftable.RefTxn.ReadAt).
	read, _ := r.table.Applied()
	tx := &reftable.RefTxn{
		Repo: repo, RepoID: req.RepoID, PusherID: req.PusherID, PusherName: req.PusherName, Time: time.Now().UnixNano(),
		Nonce: clusterhook.TxnNonce(req.Nonce, req.Lines), Trigger: req.Trigger, PRID: req.PRID, IsWiki: req.IsWiki, PushOptions: req.PushOptions,
		Push: req.PusherID != 0, ReadAt: read.Index,
	}
	var refs []string
	for _, l := range req.Lines {
		if l.Ref != "HEAD" && !strings.HasPrefix(l.Ref, "refs/") {
			continue
		}
		// Apply refuses an invalid name too; a name that is not valid
		// UTF-8 would reach it already changed by JSON, under another name.
		if err := reftable.ValidRefName(l.Ref); err != nil {
			return nil, nil, fmt.Errorf("%q: %w; nothing was written", l.Ref, err)
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
			if req.PusherID != 0 && reftable.IsZeroSHA(l.Old) && !u.Delete && v.Live() {
				return nil, nil, errStaleCreate{ref: l.Ref}
			}
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
	tx.Init = initsRepo(ctx, req.GitDir, tx.Updates)
	if tx.Format == "" {
		// A new repository's first entry is often HEAD alone: ask git.
		tx.Format = "sha1"
		if out, err := gitRun(ctx, req.GitDir, nil, "rev-parse", "--show-object-format"); err == nil && strings.TrimSpace(out) == "sha256" {
			tx.Format = "sha256"
		}
	}
	return tx, refs, nil
}

// initsRepo reports whether a transaction of HEAD alone is git init's:
// git gives a change of the default branch the same zero old value, so
// the directory tells them apart, which holds no ref before git init's
// HEAD. A directory that cannot be read keeps the reading the table had
// before InitVersion, a creation (#136).
func initsRepo(ctx context.Context, gitDir string, updates []reftable.RefUpdate) bool {
	if len(updates) == 0 {
		return false
	}
	for _, u := range updates {
		if u.Ref != "HEAD" || u.Symbolic == "" {
			return false
		}
	}
	out, err := gitRun(ctx, gitDir, nil, "for-each-ref", "--count=1", "--format=%(refname)")
	if err != nil {
		log.Warn("cluster: reading the refs of %s to tell git init from a change of HEAD: %v", gitDir, err)
		return true
	}
	return strings.TrimSpace(out) == ""
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
	// The last probe ends at the deadline too: a client or a proxy that
	// gives up first would read a disconnect again.
	ctx, cancel := context.WithTimeout(ctx, writeLeaderWait)
	defer cancel()
	for {
		eng := r.Engine()
		// The fence only: a repository still behind refuses its own writes,
		// at the proposal, and holds no other one (#39).
		if eng == nil || r.projector.Fenced() {
			return errors.New("this node of the cluster is not ready; retry shortly")
		}
		if h := eng.Health(); h.Stopped {
			return fmt.Errorf("this node of the cluster stopped (%s); nothing was written", h.StopReason)
		}
		forming := formingRefusal(r.auto, eng.Membership())
		if forming == nil && probeLeader(ctx, eng) {
			return nil
		}
		if ctx.Err() == nil {
			select {
			case <-ctx.Done():
			case <-time.After(100 * time.Millisecond):
			}
		}
		if ctx.Err() != nil {
			if forming != nil {
				return forming
			}
			return errNoLeader
		}
	}
}

// WriteRefusal is this node's refusal of a ref write now, nil when it can
// take one or when the cluster is off: the API asks it before a write that
// would otherwise fail in the reference-transaction hook with a bare 500.
func WriteRefusal(ctx context.Context) error {
	if r := Current(); r != nil {
		return r.WriteRefusal(ctx)
	}
	return nil
}

func (r *Replicator) propose(ctx context.Context, repo string, req *clusterhook.Request, tx *reftable.RefTxn) *clusterhook.Response {
	eng := r.Engine()
	if eng == nil || r.projector.Fenced() {
		return fail(errors.New("this node of the cluster is not ready; retry shortly"))
	}
	if !r.projector.Writable(repo) {
		return fail(fmt.Errorf("the repository %s is not reconciled on this node since its start; retry shortly or through another node", repo))
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
	if err := r.waitFormed(ctx, eng); err != nil {
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

	// From payload version 2, Apply itself refuses a push into a key
	// deleted or renamed away by an entry this node had not applied yet
	// when pushTargetGone looked.
	// The pusher's name is written from payload version 4 only, once every
	// member applies it: an older one drops it, and its table would hold
	// other events than the others'.
	v := r.table.WriteVersion()
	if v < reftable.PusherVersion || tx.PusherID == 0 || len(tx.PusherName) > reftable.MaxPusherName {
		tx.PusherName = ""
	}
	// Init likewise from InitVersion only.
	if v < reftable.InitVersion {
		tx.Init = false
	}
	data, err := reftable.EncodeVersion(v, reftable.KindRefTxn, tx)
	if err != nil {
		return fail(err)
	}
	return r.proposeWithRetry(ctx, repo, req.Nonce, tx, data)
}

// waitFormed waits, within writeLeaderWait, until the autonomous cluster
// has three voters (formingRefusal): the autopilot promotes the members
// that joined within seconds of the formation.
func (r *Replicator) waitFormed(ctx context.Context, eng consensus.Engine) error {
	ctx, cancel := context.WithTimeout(ctx, writeLeaderWait)
	defer cancel()
	for {
		err := formingRefusal(r.auto, eng.Membership())
		if err == nil {
			return nil
		}
		select {
		case <-ctx.Done():
			return err
		case <-time.After(100 * time.Millisecond):
		}
	}
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

// committed reports whether tx committed: its nonce among the accepted
// transactions, or else every ref of tx holding its new value at the
// revision it expected plus one. The nonce answers even when a later write
// already moved a ref, where the values alone would call a committed push
// refused.
func (r *Replicator) committed(repo string, tx *reftable.RefTxn) bool {
	if _, ok := r.table.CommittedNonce(tx.Nonce); ok {
		return true
	}
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
	if allZeroLines(req.Lines) && r.registry.nestedFinished(req.Nonce) {
		// The nested packed-refs transaction ends before the one that
		// deletes the refs, which stays registered until git publishes.
		return
	}
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
	v := r.table.WriteVersion()
	d := &reftable.RepoDelete{Repo: key, RepoID: repoID, Time: time.Now().UnixNano()}
	landed := func() bool { _, deleted := r.table.Deleted(key); return deleted }
	if v >= reftable.GenerationVersion {
		d.Nonce = clusterhook.NewNonce()
		landed = r.nonceLanded(d.Nonce)
	}
	return r.proposeLifecycle(ctx, v, reftable.KindRepoDelete, d, landed)
}

// nonceLanded is the landed test of a lifecycle command written at
// reftable.GenerationVersion: its own nonce among the committed ones. A
// state of the table cannot tell THIS command from an older one: after a
// round trip a to b and back, b's rename mark already names a, and a third
// a to b whose outcome was unknown read as committed when nothing was
// (#74).
func (r *Replicator) nonceLanded(nonce string) func() bool {
	return func() bool { _, ok := r.table.CommittedNonce(nonce); return ok }
}

// proposeRepoRename moves a repository's refs to its new key; every
// projector, the origin's included, moves the directory.
func (r *Replicator) proposeRepoRename(ctx context.Context, from, to string) error {
	return r.proposeRepoMoves(ctx, [][2]string{{from, to}})
}

// proposeRepoMoves moves several repositories in ONE entry, all or none: a
// repository and its wiki, or an owner's repositories.
func (r *Replicator) proposeRepoMoves(ctx context.Context, moves [][2]string) error {
	return r.proposeMoves(ctx, moves, [2]string{})
}

// proposeMoves is proposeRepoMoves; owner names an owner's rename, old and
// new names, which Apply checks from reftable.GenerationVersion.
func (r *Replicator) proposeMoves(ctx context.Context, moves [][2]string, owner [2]string) error {
	if len(moves) == 0 {
		return nil
	}
	v, rn := r.renameCommand(moves, owner)
	landed := func() bool {
		for _, m := range moves {
			if f, _, ok := r.table.RenamedFrom(m[1]); !ok || f != m[0] {
				return false
			}
		}
		return true
	}
	if v >= reftable.GenerationVersion {
		landed = r.nonceLanded(rn.Nonce)
	}
	return r.proposeLifecycle(ctx, v, reftable.KindRepoRename, rn, landed)
}

// renameCommand is the command moving moves, at the version the table
// writes; owner names an owner's rename.
func (r *Replicator) renameCommand(moves [][2]string, owner [2]string) (uint8, *reftable.RepoRename) {
	v := r.table.WriteVersion()
	rn := &reftable.RepoRename{From: moves[0][0], To: moves[0][1], Time: time.Now().UnixNano(), Also: moves[1:]}
	if v >= reftable.GenerationVersion {
		rn.Nonce, rn.Owner = clusterhook.NewNonce(), owner
	}
	return v, rn
}

// tooLarge refuses a command of size bytes the engine would refuse as
// ENTRY_TOO_LARGE, before it is proposed.
func (r *Replicator) tooLarge(size int) error {
	bound := r.maxEntry
	if bound <= 0 {
		bound = consensus.MaxEntryBytes
	}
	if size <= bound {
		return nil
	}
	return fmt.Errorf("%w (%d bytes, the bound is %d)", clusterhook.ErrTooLarge, size, bound)
}

func (r *Replicator) proposeLifecycle(ctx context.Context, v uint8, kind reftable.Kind, body any, landed func() bool) error {
	data, err := reftable.EncodeVersion(v, kind, body)
	if err != nil {
		return err
	}
	// Refused by the engine, it would be refused at every attempt: it is
	// refused here, with certainty, the callers moving the directories
	// back (#182).
	if err := r.tooLarge(len(data)); err != nil {
		return fmt.Errorf("the cluster cannot record it: %w", err)
	}
	// As a push: while the autonomous cluster forms, a deletion or a
	// rename committed on the founder's disk alone, already done in SQL,
	// would be lost with that disk.
	if eng := r.Engine(); eng != nil {
		if err := r.waitFormed(ctx, eng); err != nil {
			return fmt.Errorf("the cluster did not record it: %w", err)
		}
	}
	// The caller already moved or removed the directories: a request that
	// ends, its client gone, must not leave the outcome open (#46).
	// Its deadline still bounds the proposal.
	detached := context.WithoutCancel(ctx)
	pctx := detached
	if dl, ok := ctx.Deadline(); ok {
		var cancel context.CancelFunc
		pctx, cancel = context.WithDeadline(detached, dl)
		defer cancel()
	}
	r.fence.RLock()
	_, result, err := r.proposeCommand(pctx, data, landed)
	r.fence.RUnlock()
	if clusterhook.IsOutcomeUnresolved(err) && landed != nil {
		if ok, rerr := r.resolveLanded(detached, landed); rerr == nil {
			if !ok {
				// Certain: a commit boundary a quorum confirmed was applied
				// without it, so the callers move the directories back.
				return fmt.Errorf("the cluster did not record it: not applied at a commit boundary after %v", err)
			}
			result, err = reftable.Result{OK: true}, nil
		}
	}
	if err != nil {
		return fmt.Errorf("the cluster did not record it: %w", err)
	}
	if !result.OK {
		return fmt.Errorf("the cluster refused it: %s", result.Reason)
	}
	r.signalConsumer()
	return nil
}

// lifecycleResolveBudget bounds how long a lifecycle command whose outcome
// stayed unknown is resolved, a commit boundary at a time, before its
// caller is told ErrOutcomeUnresolved.
var lifecycleResolveBudget = clusterhook.DefaultTimeout

// resolveLanded applies a commit boundary confirmed by a quorum and then
// asks landed, within lifecycleResolveBudget.
func (r *Replicator) resolveLanded(ctx context.Context, landed func() bool) (bool, error) {
	ctx, cancel := context.WithTimeout(ctx, lifecycleResolveBudget)
	defer cancel()
	if _, err := r.confirm(ctx); err != nil {
		return false, err
	}
	return landed(), nil
}

// proposeOwnerRename moves every repository the table holds under an owner
// in one entry: they all move, or none does and Forgejo moves the directory
// back. The list is read once this node applied a commit boundary a quorum
// confirmed: a node behind the log would leave a repository created just
// before under the old owner, while the database renamed it (#56). From
// reftable.GenerationVersion, Apply also refuses a list that misses one.
func (r *Replicator) proposeOwnerRename(ctx context.Context, oldOwner, newOwner string) error {
	if r.Engine() != nil {
		cctx, cancel := context.WithTimeout(ctx, lifecycleResolveBudget)
		_, err := r.confirm(cctx)
		cancel()
		if err != nil {
			return fmt.Errorf("the cluster did not record it: this node could not confirm it applied the log: %w", err)
		}
	}
	moves := r.ownerMoves(oldOwner, newOwner)
	err := r.proposeMoves(ctx, moves, [2]string{oldOwner, newOwner})
	if errors.Is(err, clusterhook.ErrTooLarge) {
		return ownerTooLarge(oldOwner, len(moves), err)
	}
	return err
}

// ownerRenameRefusal is clusterhook.OwnerRenameRefusal: an owner's rename
// whose entry, as this node's table lists its moves now, the engine would
// refuse is refused before Forgejo writes anything (#182). The rename is
// still checked once proposed, its moves read again after a commit
// boundary a quorum confirmed.
func (r *Replicator) ownerRenameRefusal(oldOwner, newOwner string) error {
	moves := r.ownerMoves(oldOwner, newOwner)
	if len(moves) == 0 {
		return nil
	}
	v, rn := r.renameCommand(moves, [2]string{oldOwner, newOwner})
	data, err := reftable.EncodeVersion(v, reftable.KindRepoRename, rn)
	if err != nil {
		return err
	}
	if err := r.tooLarge(len(data)); err != nil {
		return ownerTooLarge(oldOwner, len(moves), err)
	}
	return nil
}

// ownerTooLarge explains the refusal of an owner's rename too large for
// one entry, which every attempt meets again.
func ownerTooLarge(owner string, keys int, err error) error {
	return fmt.Errorf("renaming the owner %s moves its %d repositories, wikis included, at once: %w; move some of its repositories to another owner first", owner, keys, err)
}

// ownerMoves lists the moves of an owner's rename: every key under the old
// owner that holds a repository, a live ref at least, the keys Apply's
// check of an owner's rename requires. A key a rename moved away holds
// none, nor the key of a wiki never created, which a repository's rename
// moves with it: moved too, each one got a rename mark from a key holding
// no repository, which a node behind the log followed to move another
// repository's directory there (#181).
func (r *Replicator) ownerMoves(oldOwner, newOwner string) [][2]string {
	prefix := oldOwner + "/"
	var moves [][2]string
	for _, key := range r.table.Repos() {
		if !strings.HasPrefix(key, prefix) {
			continue
		}
		if _, deleted := r.table.Deleted(key); deleted {
			continue
		}
		if refs, _ := r.table.Refs(key); !anyLive(refs) {
			continue
		}
		moves = append(moves, [2]string{key, newOwner + "/" + strings.TrimPrefix(key, prefix)})
	}
	return moves
}

// RaisePayloadVersion makes the cluster write reftable.PayloadVersion from
// now on. The raise is a version 1 Audit entry, which a member running an
// older binary applies as a plain record: it must be proposed only once
// every member runs a binary that applies the new version, or the first
// entry written at it stops the members that cannot read it (D18: stopped,
// never forked). The autonomous mode raises it by itself once every member
// says it can (autopilot); an operator raises it otherwise.
func (r *Replicator) RaisePayloadVersion(ctx context.Context, op consensus.Operator) error {
	if r.table.WriteVersion() >= reftable.PayloadVersion {
		return nil
	}
	data, err := reftable.Encode(reftable.KindAudit, &reftable.Audit{
		ID: clusterhook.NewNonce(), Operator: string(op), Action: reftable.AuditPayloadVersion,
		Target: strconv.Itoa(reftable.PayloadVersion), Outcome: reftable.AuditRaised, Time: time.Now().Unix(),
	})
	if err != nil {
		return err
	}
	tok, result, err := r.proposeCommand(ctx, data, func() bool { return r.table.WriteVersion() >= reftable.PayloadVersion })
	if err != nil {
		return err
	}
	if !result.OK {
		return fmt.Errorf("the cluster refused it: %s", result.Reason)
	}
	// A proposal forwarded from a follower returns once the leader applied
	// it: what follows the raise here, the pending revocations' flush,
	// reads this node's table.
	if eng := r.Engine(); eng != nil {
		if err := eng.WaitApplied(ctx, tok); err != nil {
			return fmt.Errorf("the payload version is raised, but this node did not apply it yet: %w", err)
		}
	}
	return nil
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
				return tok, reftable.Result{}, fmt.Errorf("%w: %w (unresolved: %v)", clusterhook.ErrOutcomeUnresolved, err, cerr)
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
