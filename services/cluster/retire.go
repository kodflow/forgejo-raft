// Copyright 2026 The Forgejo Authors. All rights reserved.
// SPDX-License-Identifier: GPL-3.0-or-later

package cluster

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"time"

	"forgejo.org/modules/log"
	"forgejo.org/services/cluster/consensus"
)

// A node the autopilot removed while it was out of reach comes back with an
// id the cluster retired:
// the engine never admits it again (R8-11). It rejoins under a new id, by
// itself, and only once a quorum confirmed that its id is retired:
//
//  1. a majority of the voters of this node's own committed membership,
//     each authenticated by its slot and key, say this node's id is
//     retired;
//  2. the consensus data directory is RENAMED to a dated directory, kept
//     for the operator, never deleted;
//  3. a new id is stamped, the node joins through the CA admission and
//     catches up as a learner; the autopilot then makes it a voter.
//
// Before anything moves, the node gets a new key (newkey.go): the leader
// that removed it marked its key, which never joins again (#209).
//
// The repositories are not touched: the projector and the object channel
// reconcile them with the table it catches up on.

// Member kinds as the status shows them.
const (
	kindVoter   = "voter"
	kindLearner = "learner"
	kindRemoved = "removed"
)

// memberRecord is what this node saw of a member, and since when.
type memberRecord struct {
	Slot  string
	Kind  string
	Since time.Time
}

// observe records each member's kind, and the retired ids, the first time
// this node sees them so.
func (r *Replicator) observe(m consensus.Membership) {
	now := r.auto.now()
	r.seenMu.Lock()
	defer r.seenMu.Unlock()
	if r.seen == nil {
		r.seen = map[consensus.NodeID]memberRecord{}
	}
	set := func(id consensus.NodeID, slot, kind string) {
		rec, ok := r.seen[id]
		if slot == "" {
			slot = rec.Slot
		}
		if !ok || rec.Kind != kind {
			rec.Since = now
		}
		rec.Slot, rec.Kind = slot, kind
		r.seen[id] = rec
	}
	for _, x := range m.Members {
		kind := kindLearner
		if x.Voter {
			kind = kindVoter
		}
		set(x.ID, x.Slot, kind)
	}
	for _, id := range m.Retired {
		set(id, "", kindRemoved)
	}
}

// record returns what this node saw of a member.
func (r *Replicator) record(id consensus.NodeID) (memberRecord, bool) {
	r.seenMu.Lock()
	defer r.seenMu.Unlock()
	rec, ok := r.seen[id]
	return rec, ok
}

// removedRecords lists the retired members this node saw, oldest first.
func (r *Replicator) removedRecords(m consensus.Membership) []RemovedView {
	r.seenMu.Lock()
	defer r.seenMu.Unlock()
	var out []RemovedView
	for _, id := range m.Retired {
		rec := r.seen[id]
		out = append(out, RemovedView{ID: id.String(), Slot: rec.Slot, Since: rec.Since})
	}
	return out
}

// retiredConfirmed reports whether a quorum says self is retired: a
// majority of the voters of this node's own committed membership, the one
// view it can trust, each answered under its own key that self is retired.
// Nothing an answer's body says of the membership (its node id, its index,
// its voters) is believed: one forged answer, from any holder of a
// certificate of the CA, would otherwise retire a healthy member (#49).
// trusted says whether an answer came from a member, by its slot and the
// key the handshake proved, under a key nobody revoked.
func retiredConfirmed(self consensus.NodeID, own consensus.Membership, seen []hello, trusted func(consensus.Member, hello) bool) bool {
	voters, confirmed := 0, 0
	for _, m := range own.Members {
		if !m.Voter {
			continue
		}
		voters++
		if m.ID == self {
			continue
		}
		for _, h := range seen {
			if h.State == helloMember && trusted(m, h) && h.retires(self.String()) {
				confirmed++
				break
			}
		}
	}
	return voters > 0 && confirmed >= voters/2+1
}

// joinerRetired reports whether the members that answered a joining node,
// one that never held a membership to count against, say by a majority
// that its id is retired: an answer under a revoked key does not count. A
// forged answer can at most make such a node stamp another id, and it holds
// no data a quorum committed.
func joinerRetired(self consensus.NodeID, seen []hello, revoked func([32]byte) bool) bool {
	members, retired := 0, 0
	for _, h := range seen {
		if h.State != helloMember || revoked(h.SPKI) {
			continue
		}
		members++
		if h.retires(self.String()) {
			retired++
		}
	}
	return retired > 0 && retired > members/2
}

// trustedAnswer reports whether a hello came from member m: its slot, and
// its key, which the handshake of the object channel proved (a node's
// object channel and consensus share their certificate), never revoked.
// The body's node id is not compared: a member's machine that rejoined
// under a new id still speaks for that key.
func (r *Replicator) trustedAnswer(m consensus.Member, h hello) bool {
	if m.SPKI == ([32]byte{}) || h.Slot != m.Slot {
		return false
	}
	return h.SPKI == m.SPKI && !r.keyRevoked(m.SPKI)
}

// keyRevoked reports whether this node's table holds the key revoked.
func (r *Replicator) keyRevoked(spki [32]byte) bool {
	return r.table != nil && r.table.KeyRevoked(hex.EncodeToString(spki[:]))
}

// revokedOrAsked reports whether this node's table holds the key revoked,
// or asked for revocation and not written yet.
func (r *Replicator) revokedOrAsked(spki [32]byte) bool {
	return r.keyRevoked(spki) || (r.table != nil && r.revocationAsked(hex.EncodeToString(spki[:])))
}

// checkRetired runs on a node that does not lead: when it is not in its own
// membership any more, or no leader answers it, it asks the others, and
// rejoins under a new id once a quorum confirms its id is retired.
func (r *Replicator) checkRetired(ctx context.Context, eng consensus.Engine) {
	self := eng.Status().ID
	if self == 0 || r.auto.rejoin == nil {
		return
	}
	if h := eng.Health(); h.Stopped && h.StopReason != "" {
		// A stopped engine lists no membership, which reads as retired;
		// its stop belongs to the operator, and a rejoin would move its
		// marker aside with the data directory (#67). An engine closed
		// without a reason, by a rejoin that failed, may rejoin again.
		if r.auto.stopNoted.CompareAndSwap(false, true) {
			log.Error("cluster: the consensus node stopped (%s): it does not rejoin under a new id; an operator clears the stop", h.StopReason)
		}
		return
	}
	own := eng.Membership()
	if len(own.Members) > 0 {
		r.auto.lastOwn = own
	} else {
		// An engine a failed rejoin closed lists nobody: the membership
		// it held last is still the one this node trusts.
		own = r.auto.lastOwn
	}
	if Rank(own, self) > 0 && probeLeader(ctx, eng) {
		return
	}
	seen := r.auto.round(ctx)
	switch {
	case len(own.Members) == 0 && r.auto.neverMember(self):
		// A joining node that no membership ever listed: the leader may
		// have added it as the replacement of a lost voter (#186), then
		// removed it before it was ever heard. Its JOIN under a retired id
		// is refused for good, and it holds no data to keep.
		if !joinerRetired(self, seen, r.keyRevoked) {
			return
		}
	case !retiredConfirmed(self, own, seen, r.trustedAnswer):
		return
	}
	log.Warn("cluster: a quorum says this node's id %s is retired: it rejoins under a new id", self)
	err := r.auto.rejoin(ctx)
	switch {
	case errors.Is(err, errNeedsNewKey):
		// Retried at each pass, logged once: only an operator's new key
		// lets it through.
		if r.auto.keyNoted.CompareAndSwap(false, true) {
			log.Error("cluster: this node does not rejoin under a new id: %v", err)
		}
	case err != nil:
		log.Error("cluster: rejoining under a new id: %v; retried", err)
	default:
		r.auto.keyNoted.Store(false)
	}
}

// refuseStoppedDir refuses to move aside a data directory that holds a
// stop marker or a quarantine: only an operator clears them, and a rejoin
// would reopen the node past them (#67).
func refuseStoppedDir(driver consensus.Driver, dir string) error {
	rep, err := driver.Inspect(dir)
	if err != nil {
		return fmt.Errorf("inspecting the data directory before a rejoin: %w", err)
	}
	if len(rep.Markers) > 0 || rep.Quarantined {
		return fmt.Errorf("the data directory %s holds the stop markers %v (quarantined: %t): an operator clears them before the node rejoins", dir, rep.Markers, rep.Quarantined)
	}
	return nil
}

// retireDataDir moves a retired node's consensus data directory aside,
// under a dated name, for the operator; nothing is deleted.
func retireDataDir(dir string, now time.Time) (string, error) {
	to := fmt.Sprintf("%s.retired-%s", dir, now.UTC().Format("20060102T150405.000000000Z"))
	if err := os.Rename(dir, to); err != nil {
		return "", fmt.Errorf("moving the retired data directory aside: %w", err)
	}
	return to, nil
}
