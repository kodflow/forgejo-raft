// Copyright 2026 The Forgejo Authors. All rights reserved.
// SPDX-License-Identifier: GPL-3.0-or-later

package mirror

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"time"

	repo_model "forgejo.org/models/repo"
	"forgejo.org/modules/clusterhook"
	"forgejo.org/modules/git"
	"forgejo.org/modules/log"
	"forgejo.org/modules/optional"
	"forgejo.org/modules/process"
	repo_module "forgejo.org/modules/repository"
	"forgejo.org/modules/setting"
	"forgejo.org/modules/timeutil"
	"forgejo.org/modules/util"
)

// In a cluster every node holds its own copy of a repository and runs the
// update_mirrors cron, and a push mirror syncs from any node (#79, #146).
// Its push is `--mirror --force` from the node's disk: from a node behind
// the cluster it rewound the remote and deleted there a branch an
// acknowledged push created, and from a directory that still held another
// repository it pushed that repository's history to the remote (#172).
// Two fresh nodes that pushed at once finished in any order, and the one
// whose state was older could land last, until the next sync.
//
// A node of a cluster therefore holds a push mirror's sync in SQL before
// it reads the repository: last_update holds the hold's expiry, ahead of
// every clock, until the sync writes its outcome there. Under the hold,
// each push starts once the node serves its repository at least at the
// commit index the cluster confirms then (WaitFresh), from a copy of its
// directory read between two checks that the directory holds the
// repository the database describes, under the name the sync loaded
// (stagePush, #294): it carries every write acknowledged before the hold,
// and so every write the previous hold's pushes carried, and nothing of
// another repository. The hold, not the leadership, keeps the
// pushes apart: a node that led when its push started may still be
// pushing when the next leader does, and nothing fences a push at the
// remote; the hold needs SQL only, and works on whichever node runs the
// sync. The cron does not pick a mirror under a hold, which is not due.

// pushHoldSkew is the clock skew between the nodes of a cluster that the
// hold of a push mirror's sync tolerates: a hold is the expiry its node
// wrote, read against each node's own clock.
const pushHoldSkew = time.Minute

// pushHoldPoll is how often a sync waiting for another node's hold reads
// it again.
var pushHoldPoll = time.Second

// errPushHoldLost is a hold that expired and that another node took over:
// that node syncs the mirror, and this one writes nothing.
var errPushHoldLost = errors.New("another node of the cluster took over the sync of this push mirror")

// errPushCarried is a sync that another one, started since on any node,
// carried through: this one has nothing left to push.
var errPushCarried = errors.New("carried by a sync of this push mirror started since")

// pushRefused is a push this node refused to make: it may be behind the
// cluster, its directory holds another repository, or the repository was
// renamed since the sync loaded it. The sync is not
// written as done, and the next round of update_mirrors, on any node,
// retries it.
type pushRefused struct{ error }

func (e pushRefused) Unwrap() error { return e.error }

// pushTimeoutInCluster is the timeout of a push mirror's git push:
// [git.timeout] MIRROR, or git's own default when it is not set, as
// modules/git applies it. The hold outlasts it.
func pushTimeoutInCluster() time.Duration {
	for _, s := range []int{setting.Git.Timeout.Mirror, setting.Git.Timeout.Default} {
		if s > 0 {
			return time.Duration(s) * time.Second
		}
	}
	return 360 * time.Second
}

// pushHoldSpan is how long a hold lasts. What follows its last renewal is
// bounded: for the repository and its wiki, a fresh wait, then the copy
// brought up to the directory and the push, which git runs within timeout
// together (pushFresh), its process group killed past the process
// manager's grace; the clock skew counts on both sides.
func pushHoldSpan(timeout time.Duration) time.Duration {
	return 2*(setting.Cluster.ReadAfterWriteTimeout+timeout+process.TerminateGraceTimeout) + 2*pushHoldSkew
}

// heldAt reports whether lastUpdate is a hold that has not expired at now.
// A sync's time, written by a node whose clock is ahead by less than
// pushHoldSkew, is not one.
func heldAt(lastUpdate, now timeutil.TimeStamp) bool {
	return lastUpdate > now.AddDuration(pushHoldSkew)
}

func pushMirrorState(ctx context.Context, id int64) (timeutil.TimeStamp, string, error) {
	last, lastError, exist, err := repo_model.GetPushMirrorSyncState(ctx, id)
	if err == nil && !exist {
		err = fmt.Errorf("push mirror %d is gone", id)
	}
	return last, lastError, err
}

// pushHold is this node's hold of a push mirror's sync.
type pushHold struct {
	id    int64
	span  time.Duration
	token timeutil.TimeStamp // the hold's expiry, which last_update holds
	prev  timeutil.TimeStamp // last_update before the hold
}

// holdPushMirror takes the hold of the push mirror id's sync, and waits
// for one another node keeps. A hold taken after this call started, by
// any node, carries this sync: its fresh waits come after it, so its
// pushes carry every write acknowledged before this call. Once that sync
// succeeded, holdPushMirror returns errPushCarried; a failed one, maybe on
// a node behind, does not carry it, and this node syncs itself. A hold
// whose node stopped is taken over once it expired.
func holdPushMirror(ctx context.Context, id int64, span time.Duration) (*pushHold, error) {
	seen, _, err := pushMirrorState(ctx, id)
	if err != nil {
		return nil, err
	}
	// cover is a hold taken after this call started.
	var cover timeutil.TimeStamp
	trustCover := true
	for {
		now := timeutil.TimeStampNow()
		if heldAt(seen, now) {
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-time.After(pushHoldPoll):
			}
		} else {
			if cover != 0 && seen != cover {
				// The hold that carries this sync ended with its outcome.
				_, lastError, err := pushMirrorState(ctx, id)
				if err != nil {
					return nil, err
				}
				if lastError == "" {
					return nil, errPushCarried
				}
				cover, trustCover = 0, false
			}
			token := now.AddDuration(span)
			ok, err := repo_model.SwapPushMirrorLastUpdate(ctx, id, seen, token, optional.None[string]())
			if err != nil {
				return nil, err
			}
			if ok {
				return &pushHold{id: id, span: span, token: token, prev: seen}, nil
			}
		}
		cur, _, err := pushMirrorState(ctx, id)
		if err != nil {
			return nil, err
		}
		if trustCover && cur != seen && heldAt(cur, timeutil.TimeStampNow()) {
			// Written since this call read last_update: a hold taken, or
			// renewed, after this call started.
			cover = cur
		}
		seen = cur
	}
}

// renew extends the hold right before the pushes, which it then outlasts:
// what comes before them, the LFS objects above all, has no bound. A hold
// that expired meanwhile and that another node took over is lost.
func (h *pushHold) renew(ctx context.Context) error {
	token := max(timeutil.TimeStampNow().AddDuration(h.span), h.token+1)
	ok, err := repo_model.SwapPushMirrorLastUpdate(ctx, h.id, h.token, token, optional.None[string]())
	if err != nil {
		return err
	}
	if !ok {
		return errPushHoldLost
	}
	h.token = token
	return nil
}

// release ends the hold with the sync's outcome, also on a canceled sync,
// so that the next one does not wait for the hold to expire. A hold
// another node took over is left to it.
func (h *pushHold) release(ctx context.Context, lastUpdate timeutil.TimeStamp, lastError string) error {
	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), time.Minute)
	defer cancel()
	if lastUpdate == h.token {
		lastUpdate--
	}
	ok, err := repo_model.SwapPushMirrorLastUpdate(ctx, h.id, h.token, lastUpdate, optional.Some(lastError))
	if err == nil && !ok {
		err = errPushHoldLost
	}
	return err
}

// syncPushMirrorInCluster is SyncPushMirror on a node of a cluster. A sync
// done, or failed as a sync fails without a cluster, writes its time in
// last_update; a sync this node refused to push puts last_update back as
// it was, so the mirror stays due.
func syncPushMirrorInCluster(ctx context.Context, m *repo_model.PushMirror) bool {
	timeout := pushTimeoutInCluster()
	hold, err := holdPushMirror(ctx, m.ID, pushHoldSpan(timeout))
	if errors.Is(err, errPushCarried) {
		log.Trace("SyncPushMirror [mirror: %d][repo: %-v]: %v", m.ID, m.Repo, err)
		return true
	}
	if err != nil {
		log.Error("SyncPushMirror [mirror: %d][repo: %-v]: holding the sync in the cluster: %v", m.ID, m.Repo, err)
		return false
	}
	err = runPushSyncHeld(ctx, m, hold, timeout)
	if errors.Is(err, errPushHoldLost) {
		log.Warn("SyncPushMirror [mirror: %d][repo: %-v]: %v", m.ID, m.Repo, err)
		return false
	}
	lastUpdate, lastError := timeutil.TimeStampNow(), ""
	if err != nil {
		log.Error("SyncPushMirror [mirror: %d][repo: %-v]: %v", m.ID, m.Repo, err)
		lastError = stripExitStatus.ReplaceAllLiteralString(err.Error(), "")
		if errors.As(err, new(pushRefused)) {
			lastUpdate = hold.prev
		}
	}
	if err := hold.release(ctx, lastUpdate, lastError); err != nil {
		log.Error("UpdatePushMirror [%d]: %v", m.ID, err)
		return false
	}
	log.Trace("SyncPushMirror [mirror: %d][repo: %-v]: Finished", m.ID, m.Repo)
	return err == nil
}

// runPushSyncHeld is runPushSync under the hold. Each push, of the
// repository and of its wiki, comes from a staging copy of its directory
// on this node, read between two checks that it holds the repository the
// sync loaded (stagePush). Both pushes come last, after one renewal of the
// hold, so that every write of the hold, taken or renewed, is followed by
// the fresh waits of both, and their copies brought up to the directory,
// before they push. A wiki that fails keeps the repository's push, which
// runPushSync makes first. The wiki is waited for before the node looks
// for it: one created lately may not be projected here yet, and would be
// skipped.
func runPushSyncHeld(ctx context.Context, m *repo_model.PushMirror, hold *pushHold, timeout time.Duration) error {
	if err := mayPushFrom(ctx, m, false); err != nil {
		return err
	}
	repoStage, err := stagePush(ctx, m, false, timeout)
	if err != nil {
		return err
	}
	defer repoStage.remove()
	repoPush, err := repoStage.prepare(ctx, m)
	if err != nil {
		return err
	}
	var wikiStage *pushStage
	var wikiPush *preparedPush
	wikiErr := mayPushFrom(ctx, m, true)
	if wikiErr == nil && m.Repo.HasWiki() {
		wikiStage, wikiErr = stagePush(ctx, m, true, timeout)
		switch {
		case errors.Is(wikiErr, errNoPushRemote):
			log.Trace("Skipping wiki: No remote configured")
			wikiErr = nil
		case wikiErr == nil:
			defer wikiStage.remove()
			wikiPush, wikiErr = wikiStage.prepare(ctx, m)
		}
	}
	if err := hold.renew(ctx); err != nil {
		return err
	}
	if err := repoStage.pushFresh(ctx, m, repoPush, timeout); err != nil {
		return err
	}
	if wikiErr != nil || wikiPush == nil {
		return wikiErr
	}
	return wikiStage.pushFresh(ctx, m, wikiPush, timeout)
}

// mayPushFrom refuses a push from this node, of the repository or of its
// wiki, unless the node serves it at least at the commit index the
// cluster confirms now, which a node without a leader cannot tell: before
// anything reads the directory, and again right before the push. A read
// past the bound is served as the node has it; a push `--mirror --force`
// from it is not made. That the directory holds the repository the
// database describes (#82, #294) is checked around each read of it
// (readPushSource).
func mayPushFrom(ctx context.Context, m *repo_model.PushMirror, isWiki bool) error {
	key := clusterhook.RepoKey(m.Repo.OwnerName, m.Repo.Name, isWiki)
	if !clusterhook.WaitFresh(ctx, key) {
		return pushRefused{fmt.Errorf("this node of the cluster may be behind the cluster's last writes to %s; the push mirror's sync is retried", key)}
	}
	return nil
}

// readPushSource runs read, which reads the directory on this node of the
// push mirror's repository, or of its wiki, between two checks that it
// holds that repository and nothing else, as a fork's base and a template
// are read (#283): the repository's row, read again by its ID, must still
// give the name the sync loaded, and the directory must hold the
// repository (clusterhook.ReadBetweenChecks). A refusal is a pushRefused:
// the sync is not written as done, and the next round of update_mirrors
// retries it, with the repository loaded again, under its new name if it
// was renamed.
func readPushSource(ctx context.Context, m *repo_model.PushMirror, isWiki bool, read func() error) error {
	key := clusterhook.RepoKey(m.Repo.OwnerName, m.Repo.Name, isWiki)
	return clusterhook.ReadBetweenChecks(key, m.Repo.ID, func() error {
		return pushSourceStillNamed(ctx, m.Repo, key, isWiki)
	}, func(err error) error { return pushRefused{err} }, read)
}

// pushSourceStillNamed refuses, as a pushRefused, a push of repo whose row
// no longer gives key: renamed, transferred or deleted, it or its owner,
// since the sync loaded it. Another repository may take the name
// meanwhile, and then its directory on this node, whose stamp may carry no
// repository ID for the read guard to compare (#294).
func pushSourceStillNamed(ctx context.Context, repo *repo_model.Repository, key string, isWiki bool) error {
	return mirrorRepoStillNamed(ctx, repo, key, isWiki, "push mirror", func(err error) error { return pushRefused{err} })
}

// mirrorRepoStillNamed refuses with refused the sync of a mirror, whose
// kind mirror names, of repo whose row no longer gives key: renamed,
// transferred or deleted, it or its owner, since the sync loaded it. It
// returns the database's error as it is.
func mirrorRepoStillNamed(ctx context.Context, repo *repo_model.Repository, key string, isWiki bool, mirror string, refused func(error) error) error {
	current, err := repo_model.GetRepositoryByID(ctx, repo.ID)
	if repo_model.IsErrRepoNotExist(err) {
		return refused(fmt.Errorf("the repository of %s was deleted; the %s's sync is retried", key, mirror))
	}
	if err != nil {
		return err
	}
	if now := clusterhook.RepoKey(current.OwnerName, current.Name, isWiki); now != key {
		return refused(fmt.Errorf("the repository of %s is %s now; the %s's sync is retried under its new name", key, now, mirror))
	}
	return nil
}

// errNoPushRemote is a wiki without a remote to push to: its push is
// skipped, as runPushSync skips it.
var errNoPushRemote = errors.New("the wiki has no remote to push to")

// pushStage is the staging copy, outside the repository root and so never
// replicated, that a push mirror's sync pushes from in a cluster: of the
// directory of the repository, or of its wiki, on this node.
type pushStage struct {
	from   string // the directory on this node
	dir    string // the copy
	key    string
	isWiki bool
	// cred is the address of the directory's remote with its password,
	// which the copy's remote does not hold: the push passes it through a
	// credential helper.
	cred string
}

// stagePush reads, in a cluster, the directory on this node of the push
// mirror's repository, or of its wiki, into a staging copy, and returns it
// with the push mirror's remote configured in the copy. A wiki without a
// remote to push to gives none, and errNoPushRemote.
//
// The sync loaded its repository once, at its start, and read the
// directory under that name past one check, ReadRefusal, that it held the
// repository. A directory whose stamp carries no repository ID, that of a
// repository written only by a fork, a migration or a pull mirror's sync,
// is judged by the replicated table alone, which rightly holds, under the
// name, the repository a rename put there: the check passed, the remote
// was recreated from SQL in that repository's git config (#79), its LFS
// objects went to the remote and git pushed its history there `--mirror
// --force` (#294, after #172). The window ran from the start of the sync,
// the wait for another node's hold included, to the push.
//
// The directory is now read once, by `git clone --mirror`, between two
// checks that it holds the repository the sync loaded, and nothing else
// (readPushSource); its LFS objects and the push come from the copy only.
// The copy's remote is the one the directory's git config holds, read with
// it, or the one SQL gives (pushRemoteAddress), with the push refspecs of
// SQL's branch filter, and without a password. Only once the read was
// checked does the directory's own remote follow SQL, as before (#79,
// #183): nothing of the sync writes into a directory it did not check.
func stagePush(ctx context.Context, m *repo_model.PushMirror, isWiki bool, timeout time.Duration) (*pushStage, error) {
	from := m.Repo.RepoPath()
	if isWiki {
		from = m.Repo.WikiPath()
	}
	dir, err := repo_module.CreateTemporaryPath("push-mirror")
	if err != nil {
		return nil, err
	}
	s := &pushStage{from: from, dir: dir, key: clusterhook.RepoKey(m.Repo.OwnerName, m.Repo.Name, isWiki), isWiki: isWiki}
	if err := s.configure(ctx, m, timeout); err != nil {
		s.remove()
		if errors.As(err, new(pushRefused)) || errors.Is(err, errNoPushRemote) {
			return nil, err
		}
		err = util.SanitizeErrorCredentialURLs(err)
		log.Error("SyncPushMirror [repo: %-v]: staging the push mirror's copy of %s: %v", m.Repo, s.key, err)
		return nil, err
	}
	return s, nil
}

// configure reads the directory into the copy, between two checks, and
// sets the push mirror's remote in the copy, then in the directory. A wiki
// without a remote to push to is errNoPushRemote.
func (s *pushStage) configure(ctx context.Context, m *repo_model.PushMirror, timeout time.Duration) error {
	var held string
	var exists bool
	if err := readPushSource(ctx, m, s.isWiki, func() error {
		var err error
		if held, exists, err = remoteURLIn(ctx, s.from, m.RemoteName); err != nil {
			return err
		}
		if stdout, _, err := git.NewCommand(ctx, "clone", "--mirror", "--quiet").AddDynamicArguments(s.from, s.dir).
			SetDescription(fmt.Sprintf("stagePush(git clone): %s", s.key)).
			RunStdString(&git.RunOpts{Timeout: timeout}); err != nil {
			return fmt.Errorf("git clone: %w: %s", err, stdout)
		}
		return nil
	}); err != nil {
		return err
	}
	// The copy pushes to the push mirror's remote only: the clone's own
	// remote, the directory, goes, its refs kept.
	if _, _, err := git.NewCommand(ctx, "config", "--remove-section", "remote.origin").RunStdString(&git.RunOpts{Dir: s.dir}); err != nil {
		return err
	}

	addr := held
	if !exists {
		var err error
		if addr, err = pushRemoteAddress(ctx, m, s.isWiki); err != nil {
			return err
		}
	}
	if addr == "" {
		if s.isWiki {
			return errNoPushRemote
		}
		// The repository without any remote fails at the push's
		// preparation, as without a cluster.
		return nil
	}

	// The node that created the push mirror holds its remote with the
	// password: the copy's remote never does, and the push passes it
	// through a credential helper. A push mirror from before
	// EncryptedRemoteAddress has it stored from there, as
	// pushCredentialURL does, for the other nodes.
	copyAddr := addr
	if u, err := url.Parse(addr); err == nil && hasPassword(u) {
		if m.EncryptedRemoteAddress == nil && !s.isWiki {
			if err := m.SetEncryptedRemoteAddress(ctx, addr); err != nil {
				return err
			}
		}
		s.cred = addr
		u.User = url.User(u.User.Username())
		copyAddr = u.String()
	}
	if err := addPushRemoteAndConfig(ctx, m, copyAddr, s.dir); err != nil {
		return err
	}

	// The directory's own remote follows SQL, now that the read was
	// checked, as ensurePushMirrorRemote brings it.
	if m.RemoteAddress == "" {
		return nil
	}
	if exists {
		if err := alignPushRefSpecs(ctx, m, s.from); err != nil {
			return err
		}
	} else if err := addPushRemoteAndConfig(ctx, m, addr, s.from); err != nil {
		return err
	}
	// The fetch refspec `git remote add` writes for a branch filter goes,
	// as preparePushFrom removes it from the copy.
	if _, _, err := git.NewCommand(ctx, "config", "--unset-all").AddDynamicArguments("remote." + m.RemoteName + ".fetch").RunStdString(&git.RunOpts{Dir: s.from}); err != nil && !git.IsErrorExitCode(err, 5) {
		return err
	}
	return nil
}

// prepare does what comes before the push of the copy: its LFS objects,
// which are the copy's, the remote's checks and its credentials.
func (s *pushStage) prepare(ctx context.Context, m *repo_model.PushMirror) (*preparedPush, error) {
	return preparePushFrom(ctx, m, s.dir, s.isWiki, s.cred)
}

// pushFresh pushes the copy p was prepared from once this node serves the
// repository, or its wiki, fresh (mayPushFrom), the copy first brought up
// to the directory, read again between two checks (readPushSource): the
// push carries every write this node serves then, and nothing of another
// repository. Both run within timeout, the bound pushHoldSpan counts for
// a push.
func (s *pushStage) pushFresh(ctx context.Context, m *repo_model.PushMirror, p *preparedPush, timeout time.Duration) error {
	if err := mayPushFrom(ctx, m, s.isWiki); err != nil {
		return err
	}
	deadline := time.Now().Add(timeout)
	if err := readPushSource(ctx, m, s.isWiki, func() error {
		if stdout, _, err := git.NewCommand(ctx, "fetch", "--prune", "--quiet").AddDynamicArguments(s.from, "+refs/*:refs/*").
			SetDescription(fmt.Sprintf("stagePush(git fetch): %s", s.key)).
			RunStdString(&git.RunOpts{Dir: s.dir, Timeout: timeout}); err != nil {
			return fmt.Errorf("git fetch: %w: %s", err, stdout)
		}
		return nil
	}); err != nil {
		return err
	}
	left := time.Until(deadline)
	if left <= 0 {
		return fmt.Errorf("bringing the copy of %s up to date took the push's whole timeout", s.key)
	}
	return p.push(ctx, m, left)
}

// remove removes the copy.
func (s *pushStage) remove() {
	if err := repo_module.RemoveTemporaryPath(s.dir); err != nil {
		log.Error("Failed to remove the push mirror's copy of %s: %v", s.key, err)
	}
}
