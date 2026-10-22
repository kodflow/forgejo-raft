// Copyright 2026 The Forgejo Authors. All rights reserved.
// SPDX-License-Identifier: GPL-3.0-or-later

package cluster

import (
	"context"
	"fmt"
	"os"
	"slices"
	"time"

	"forgejo.org/modules/log"
	"forgejo.org/services/cluster/consensus"
)

// A node the autopilot removed while it was out of reach comes back with an
// id the cluster retired:
// the engine never admits it again (R8-11). It rejoins under a new id, by
// itself, and only once a quorum confirmed that its id is retired:
//
//  1. a majority of the voters of the newest committed membership any
//     member reports list this node's id among the retired ones;
//  2. the consensus data directory is RENAMED to a dated directory, kept
//     for the operator, never deleted;
//  3. a new id is stamped, the node joins through the CA admission and
//     catches up as a learner; the autopilot then makes it a voter.
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
// majority of the voters of the newest membership among the answers each
// list it as retired. Answers from other nodes only: this node's own view
// may be stale either way.
func retiredConfirmed(self consensus.NodeID, seen []hello) bool {
	var newest *hello
	for i := range seen {
		h := &seen[i]
		if h.State == helloMember && (newest == nil || h.Index > newest.Index) {
			newest = h
		}
	}
	if newest == nil || len(newest.Voters) == 0 {
		return false
	}
	confirmed := 0
	for _, h := range seen {
		if slices.Contains(newest.Voters, h.Node) && slices.Contains(h.Retired, self.String()) {
			confirmed++
		}
	}
	return confirmed >= len(newest.Voters)/2+1
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
	if Rank(eng.Membership(), self) > 0 && probeLeader(ctx, eng) {
		return
	}
	if !retiredConfirmed(self, r.auto.round(ctx)) {
		return
	}
	log.Warn("cluster: a quorum says this node's id %s is retired: it rejoins under a new id", self)
	if err := r.auto.rejoin(ctx); err != nil {
		log.Error("cluster: rejoining under a new id: %v; retried", err)
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
