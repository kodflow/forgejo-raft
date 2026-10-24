// Copyright 2026 The Forgejo Authors. All rights reserved.
// SPDX-License-Identifier: GPL-3.0-or-later

package mirror

import (
	"context"
	"errors"
	"fmt"
	"time"

	repo_model "forgejo.org/models/repo"
	"forgejo.org/modules/clusterhook"
	"forgejo.org/modules/git"
	"forgejo.org/modules/log"
	"forgejo.org/modules/optional"
	"forgejo.org/modules/process"
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
// commit index the cluster confirms then (WaitFresh), from a directory
// that holds the repository the database describes (ReadRefusal): it
// carries every write acknowledged before the hold, and so every write the
// previous hold's pushes carried. The hold, not the leadership, keeps the
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
// cluster, or its directory holds another repository. The sync is not
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
// bounded: for the repository and its wiki, a fresh wait and a push, which
// git kills past timeout, its process group past the process manager's
// grace; the clock skew counts on both sides.
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

// runPushSyncHeld is runPushSync under the hold. Both pushes come last,
// after one renewal of the hold, so that every write of the hold, taken
// or renewed, is followed by the fresh waits of both before they push. A
// wiki that fails keeps the repository's push, which runPushSync makes
// first. The wiki is waited for before the node looks for it: one created
// lately may not be projected here yet, and would be skipped.
func runPushSyncHeld(ctx context.Context, m *repo_model.PushMirror, hold *pushHold, timeout time.Duration) error {
	if err := mayPushFrom(ctx, m, false); err != nil {
		return err
	}
	repoPush, err := preparePush(ctx, m, m.Repo, false)
	if err != nil {
		return err
	}
	var wikiPush *preparedPush
	wikiErr := mayPushFrom(ctx, m, true)
	if wikiErr == nil && m.Repo.HasWiki() {
		var has bool
		if has, wikiErr = wikiPushRemote(ctx, m); has {
			wikiPush, wikiErr = preparePush(ctx, m, m.Repo, true)
		}
	}
	if err := hold.renew(ctx); err != nil {
		return err
	}
	if err := mayPushFrom(ctx, m, false); err != nil {
		return err
	}
	if err := repoPush.push(ctx, m, timeout); err != nil {
		return err
	}
	if wikiErr != nil || wikiPush == nil {
		return wikiErr
	}
	if err := mayPushFrom(ctx, m, true); err != nil {
		return err
	}
	return wikiPush.push(ctx, m, timeout)
}

// wikiPushRemote recreates the remote of the wiki's push mirror when this
// node lacks it, and reports whether the wiki has one to push to, as
// runPushSync does.
func wikiPushRemote(ctx context.Context, m *repo_model.PushMirror) (bool, error) {
	if err := ensurePushMirrorRemote(ctx, m, m.Repo.WikiPath(), true); err != nil {
		log.Error("SyncPushMirror [repo: %-v]: recreating the remote of the wiki's push mirror: %v", m.Repo, err)
		return false, util.SanitizeErrorCredentialURLs(err)
	}
	if _, err := git.GetRemoteAddress(ctx, m.Repo.WikiPath(), m.RemoteName); err != nil {
		log.Trace("Skipping wiki: No remote configured")
		return false, nil
	}
	return true, nil
}

// mayPushFrom refuses a push from this node, of the repository or of its
// wiki, unless the node serves it at least at the commit index the
// cluster confirms now, which a node without a leader cannot tell, and
// its directory holds the repository the database describes (#82): the
// checks git's reads make (routers/web/repo/githttp.go), before anything
// reads or changes the directory. A read past the bound is served as the
// node has it; a push `--mirror --force` from it is not made.
func mayPushFrom(ctx context.Context, m *repo_model.PushMirror, isWiki bool) error {
	key := clusterhook.RepoKey(m.Repo.OwnerName, m.Repo.Name, isWiki)
	if !clusterhook.WaitFresh(ctx, key) {
		return pushRefused{fmt.Errorf("this node of the cluster may be behind the cluster's last writes to %s; the push mirror's sync is retried", key)}
	}
	if err := clusterhook.ReadRefusal(key, m.Repo.ID); err != nil {
		return pushRefused{err}
	}
	return nil
}
