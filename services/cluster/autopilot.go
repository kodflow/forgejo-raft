// Copyright 2026 The Forgejo Authors. All rights reserved.
// SPDX-License-Identifier: GPL-3.0-or-later

package cluster

import (
	"context"
	"errors"
	"fmt"
	"time"

	"forgejo.org/modules/log"
	"forgejo.org/services/cluster/consensus"
	"forgejo.org/services/cluster/reftable"
)

// autopilotOperator is the identity the autonomous mode records in the log
// for what it does without an operator: an admission and a promotion.
const autopilotOperator consensus.Operator = "forgejo-autopilot"

// admits is the autonomous mode's half of a JOIN without a grant: the
// engine already refused a slot that has a member and a retired id, and
// the transport verified the certificate's chain and slot.
func (a *autoMode) admits(slot string, eng consensus.Engine) error {
	if len(a.peers) > 0 {
		if _, ok := a.peers[slot]; !ok && slot != a.self {
			return fmt.Errorf("JOIN_REFUSED: slot %s is not in PEERS", slot)
		}
	}
	if eng == nil {
		return errors.New("JOIN_REFUSED: the consensus node is not open")
	}
	if n := len(eng.Membership().Members); n > a.expected {
		return fmt.Errorf("JOIN_REFUSED: the cluster has %d members for %d expected nodes; remove one first", n, a.expected)
	}
	return nil
}

// autopilot keeps the voters at the rule (voters.go) from the leader, and
// keeps each node's own membership honest: every pass, it records what the
// node sees (the status's "since"), and
//
//   - on the leader, removes a member out of reach for DEAD_MEMBER_TIMEOUT
//     while three voters remain, or promotes the eldest learner, one change
//     per pass;
//   - elsewhere, rejoins under a new id once a quorum says this node's id
//     is retired.
//
// Every change is a committed membership change, refused by the engine
// unless the new voter set answers by a majority, so it is safe at any
// time; a learner not caught up yet is refused and retried. A cluster that
// lost its majority has no leader, so nothing is removed and nothing is
// written until a member returns.
func (r *Replicator) autopilot(ctx context.Context) {
	t := time.NewTicker(r.auto.tick)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
		r.autopilotStep(ctx)
	}
}

// autopilotStep is one pass of the autopilot.
func (r *Replicator) autopilotStep(ctx context.Context) {
	eng := r.Engine()
	if eng == nil {
		return
	}
	r.stepWriter(ctx, eng)
	m := eng.Membership()
	r.observe(m)
	st := eng.Status()
	if st.Role != consensus.RoleLeader {
		r.dead.reset()
		r.checkRetired(ctx, eng)
		return
	}
	if !eng.Health().QuorumReachable {
		// A leader that no majority of the current voters answers changes
		// nothing: a minority never shrinks the cluster to itself.
		r.dead.reset()
		return
	}
	pctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	if id, ok := r.deadMember(eng, m); ok {
		r.autoChange(pctx, ActionRemove, id, "out of reach for DEAD_MEMBER_TIMEOUT", eng.Remove)
		return
	}
	if id, ok := NextPromotion(m); ok {
		r.autoChange(pctx, ActionPromote, id, "every live member votes", eng.Promote)
		return
	}
	r.maybeRaisePayload(pctx, m)
	r.flushRevocations(pctx)
}

// payloadProbeEvery paces the leader's question "does every member apply
// the new command version" while the cluster still writes the old one.
const payloadProbeEvery = 30 * time.Second

// maybeRaisePayload raises the written command version once EVERY member
// of the membership answers that its binary applies it: a rolling upgrade
// raises it after the last member restarted on the new binary, and never
// while one still runs the old one (it would stop on the first new entry).
func (r *Replicator) maybeRaisePayload(ctx context.Context, m consensus.Membership) {
	if r.table.WriteVersion() >= reftable.PayloadVersion || r.auto.now().Before(r.payloadProbe) {
		return
	}
	r.payloadProbe = r.auto.now().Add(payloadProbeEvery)
	if !everyMemberApplies(m, r.selfID(), r.auto.round(ctx)) {
		return
	}
	if err := r.RaisePayloadVersion(ctx, autopilotOperator); err != nil {
		log.Debug("cluster: autopilot: raising the payload version: %v; retried", err)
		return
	}
	log.Warn("cluster: autopilot: every member applies payload version %d: the cluster writes it from now on", reftable.PayloadVersion)
}

// everyMemberApplies reports whether every member other than self answered
// as itself (its node id) with a binary that applies reftable.PayloadVersion.
func everyMemberApplies(m consensus.Membership, self consensus.NodeID, seen []hello) bool {
	ok := map[string]bool{}
	for _, h := range seen {
		if h.Payload >= reftable.PayloadVersion {
			ok[h.Node] = true
		}
	}
	for _, x := range m.Members {
		if x.ID != self && !ok[x.ID.String()] {
			return false
		}
	}
	return true
}

// autoChange runs one membership change of the autopilot, logged and
// audited; a refusal is retried at the next pass.
func (r *Replicator) autoChange(ctx context.Context, action Action, id consensus.NodeID, why string, change func(context.Context, consensus.Operator, consensus.NodeID) error) {
	if err := change(ctx, autopilotOperator, id); err != nil {
		log.Debug("cluster: autopilot: %s %s: %v; retried", action, id, err)
		return
	}
	log.Warn("cluster: autopilot: %s %s: %s", action, id, why)
	r.audit(ctx, autopilotOperator, Operation{Action: action, Target: id.String()}, "ok", why)
}

// deadClock is when the leader first found each member out of reach. Only
// the autopilot's goroutine touches it.
type deadClock map[consensus.NodeID]time.Time

func (d *deadClock) reset() { *d = nil }

// deadMember names a member the leader has not heard from for
// DEAD_MEMBER_TIMEOUT, by the autopilot's clock, when three voters remain
// without it. A member heard again starts over.
func (r *Replicator) deadMember(eng consensus.Engine, m consensus.Membership) (consensus.NodeID, bool) {
	now := r.auto.now()
	if r.dead == nil {
		r.dead = deadClock{}
	}
	down := map[consensus.NodeID]bool{}
	for _, p := range eng.Health().Peers {
		if p.Unreachable || p.LastContact > r.auto.downAfter {
			down[p.ID] = true
		}
	}
	for id := range r.dead {
		if !down[id] || Rank(m, id) == 0 {
			delete(r.dead, id)
		}
	}
	var dead consensus.NodeID
	for _, x := range m.Members {
		if !down[x.ID] {
			continue
		}
		since, ok := r.dead[x.ID]
		if !ok {
			r.dead[x.ID] = now
			log.Warn("cluster: autopilot: %s (%s) is out of reach; removed after %s unless it answers", x.ID, x.Slot, r.auto.deadTimeout)
			continue
		}
		if dead == 0 && now.Sub(since) >= r.auto.deadTimeout && Removable(m, x.ID) {
			dead = x.ID
		}
	}
	return dead, dead != 0
}

// errReadOnly refuses a write on a learner whose leader does not answer.
var errReadOnly = errors.New("this node is a learner and no leader answers: it is read-only until a voter is back; nothing was written")

// leaderProbe bounds the check that the leader answers.
const leaderProbe = 1500 * time.Millisecond

// probeLeader asks the leader for a read barrier: the one way a learner
// knows its leader answers now, since a learner keeps the last leader it
// heard of and never campaigns.
func probeLeader(ctx context.Context, eng consensus.Engine) bool {
	pctx, cancel := context.WithTimeout(ctx, leaderProbe)
	defer cancel()
	_, err := eng.Barrier(pctx)
	return err == nil
}

// learnerGate refuses a write up front on a learner that no leader answers,
// rather than letting it wait for a leader that cannot come from it.
func (r *Replicator) learnerGate(ctx context.Context, eng consensus.Engine) error {
	if r.auto == nil || eng == nil {
		return nil
	}
	m := eng.Membership()
	if i := Rank(m, eng.Status().ID); i > 0 && m.Members[i-1].Voter {
		return nil
	}
	if probeLeader(ctx, eng) {
		return nil
	}
	return errReadOnly
}
