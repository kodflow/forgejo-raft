// Copyright 2026 The Forgejo Authors. All rights reserved.
// SPDX-License-Identifier: GPL-3.0-or-later

package cluster

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"net"
	"slices"
	"time"

	"forgejo.org/modules/log"
	"forgejo.org/services/cluster/consensus"
	"forgejo.org/services/cluster/reftable"
)

// autopilotOperator is the identity the autonomous mode records in the log
// for what it does without an operator: an admission and a promotion, and
// in both modes the raise of the engine's cluster version.
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
//     while three voters remain (a learner always), and marks its key;
//     adds the new id of a lost voter of three, under its slot; or
//     promotes the eldest learner ready to vote, two at once from one
//     voter;
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
	if r.yieldIfStuck(pctx, eng, m) {
		return
	}
	if id, ok := r.deadMember(eng, m); ok {
		r.removeDead(pctx, eng, id)
		return
	}
	if id, ok := r.revokedLearner(m); ok {
		r.autoChange(pctx, ActionRemove, id, "its key was revoked", eng.Remove)
		return
	}
	if r.replaceLost(pctx, eng, m) {
		return
	}
	if ids := NextPromotion(m, r.promotable(pctx, eng)); len(ids) > 0 {
		// From one voter, both learners in this pass: a refusal of the
		// second leaves two voters until the next pass, never by design.
		for _, id := range ids {
			if !r.autoChange(pctx, ActionPromote, id, "every live member votes", eng.Promote) {
				break
			}
		}
		return
	}
	if r.maybeYieldForCheckpoints(pctx, eng, m) {
		return
	}
	r.maybeRaisePayload(pctx, m)
	r.maybeRaiseCluster(pctx, eng)
	_ = r.flushRevocations(pctx) // logged; the next pass retries it
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
	if !everyMemberApplies(m, r.selfID(), r.auto.round(ctx), r.trustedAnswer) {
		return
	}
	if err := r.RaisePayloadVersion(ctx, autopilotOperator); err != nil {
		log.Debug("cluster: autopilot: raising the payload version: %v; retried", err)
		return
	}
	log.Warn("cluster: autopilot: every member applies payload version %d: the cluster writes it from now on", reftable.PayloadVersion)
}

// everyMemberApplies reports whether every member other than self answered
// as itself, under its slot and its key (trusted) and with its node id,
// with a binary that applies reftable.PayloadVersion. The node id alone is
// the body's word: any holder of a certificate of the CA could claim to be
// the member still on an older binary (#49).
func everyMemberApplies(m consensus.Membership, self consensus.NodeID, seen []hello, trusted func(consensus.Member, hello) bool) bool {
	for _, x := range m.Members {
		if x.ID == self {
			continue
		}
		if !slices.ContainsFunc(seen, func(h hello) bool {
			return h.Node == x.ID.String() && h.Payload >= reftable.PayloadVersion && trusted(x, h)
		}) {
			return false
		}
	}
	return true
}

// autoChange runs one membership change of the autopilot, logged and
// audited, and reports whether it was made; a refusal is retried at the
// next pass.
func (r *Replicator) autoChange(ctx context.Context, action Action, id consensus.NodeID, why string, change func(context.Context, consensus.Operator, consensus.NodeID) error) bool {
	if err := change(ctx, autopilotOperator, id); err != nil {
		log.Debug("cluster: autopilot: %s %s: %v; retried", action, id, err)
		return false
	}
	log.Warn("cluster: autopilot: %s %s: %s", action, id, why)
	r.audit(ctx, autopilotOperator, Operation{Action: action, Target: id.String()}, "ok", why)
	return true
}

// deadWhy is what the autopilot records of the removal of a dead member.
const deadWhy = "out of reach for DEAD_MEMBER_TIMEOUT"

// removeDead removes a member out of reach for DEAD_MEMBER_TIMEOUT, and
// marks its key as one never admitted again (#209): the member comes back
// under a new id with a new key (newkey.go), never with this one. The mark
// is an operator's revocation, asked the same way (revokeMember): the
// request commits before the change, so the JOIN refuses the key from the
// moment the member is gone, and the revocation is written once no member
// holds the key (flushRevocations), so a member left in place is never cut
// off (#64). A change the engine refuses for certain withdraws its request
// and leaves no effect (#171); one whose outcome is unknown keeps it, as it
// may have been made (#165).
//
// Below reftable.WithdrawVersion a refused change cannot withdraw its
// request, since some member's binary does not apply the withdrawal: the
// removal then asks nothing, as before, and the node's own rule still
// keeps the key from coming back (keyForRejoin).
func (r *Replicator) removeDead(ctx context.Context, eng consensus.Engine, id consensus.NodeID) bool {
	if r.table.WriteVersion() < reftable.WithdrawVersion {
		return r.autoChange(ctx, ActionRemove, id, deadWhy, eng.Remove)
	}
	marked := false
	removed := r.autoChange(ctx, ActionRemove, id, deadWhy+"; its key is not admitted again", func(ctx context.Context, op consensus.Operator, id consensus.NodeID) error {
		m := eng.Membership()
		if i := Rank(m, id) - 1; i >= 0 && r.revokedOrAsked(m.Members[i].SPKI) {
			// A request already open for the key, an operator's or one of
			// an earlier pass whose outcome was unknown, refuses it at the
			// JOIN: the removal records none of its own, so a removal
			// retried at each pass never piles requests up, and withdraws
			// none it did not record.
			if err := eng.Remove(ctx, op, id); err != nil {
				return err
			}
			marked = true
			return nil
		}
		k, asked, request, err := r.revokeMember(ctx, eng, op, id)
		if err != nil {
			return err
		}
		if err := eng.Remove(ctx, op, id); err != nil {
			if asked {
				r.settleRefusedRevocation(ctx, eng, ActionRemove, id, k, request, err)
			}
			return err
		}
		marked = asked
		return nil
	})
	if removed && marked {
		// Written now that no member holds the key, else at a later pass.
		if err := r.flushRevocations(ctx); err != nil {
			log.Debug("cluster: autopilot: the revocation of the key of %s is written at a later pass: %v", id, err)
		}
	}
	return removed
}

// replaceLost is the autonomous repair of a voter whose data directory was
// lost while the cluster holds three voters (#186, decision A). The node
// comes back under a new id, its old id holding the slot: the engine
// refuses its JOIN (one slot, one member), and the floor of three voters
// keeps the old id from being removed (Removable), so the cluster would
// stay at a fault tolerance of 0 for good. The leader adds the new id
// itself, as a learner under the same slot, once
//
//   - the node at the slot's address in PEERS answers as joining, over the
//     object channel under a certificate of the cluster's CA for that slot,
//     with the id and incarnation of its stamp (replaceVoter);
//   - the voter holding the slot has been out of the leader's reach for
//     DEAD_MEMBER_TIMEOUT;
//   - its key is neither revoked nor asked for revocation, no member holds
//     it, and it passes what a JOIN through the CA passes (replacement).
//
// The autopilot then promotes it, four voters, and removes the old id at a
// later pass, three again: the membership never lists fewer than three
// voters, and no format changes. The joiner's JOIN, then answered as a
// member's, completes. Its key is a new one (newkey.go): the old id holds
// its old key, and marks it once removed (removeDead).
func (r *Replicator) replaceLost(ctx context.Context, eng consensus.Engine, m consensus.Membership) bool {
	if len(m.Members) > r.auto.expected || r.auto.probe == nil {
		// A replacement already stands by, or a member above the size: it
		// is promoted, and the old id removed, before anything else.
		return false
	}
	now := r.auto.now()
	for _, old := range m.Members {
		since, ok := r.dead[old.ID]
		if !ok || !old.Voter || now.Sub(since) < r.auto.deadTimeout || Removable(m, old.ID) {
			continue
		}
		if r.replaceVoter(ctx, eng, m, old) {
			return true
		}
	}
	return false
}

// replaceVoter asks the node at the address PEERS gives the slot of old, a
// lost voter, who it is, and adds it as old's replacement when it is one.
// Only that slot is asked: the voter that does not answer is the one whose
// address may keep the probe waiting, at each pass.
func (r *Replicator) replaceVoter(ctx context.Context, eng consensus.Engine, m consensus.Membership, old consensus.Member) bool {
	addr, ok := r.auto.peers[old.Slot]
	if !ok {
		r.noteReplace(fmt.Sprintf("cluster: autopilot: %s, out of reach for DEAD_MEMBER_TIMEOUT, holds slot %s, which PEERS does not name: it is not replaced by itself", old.ID, old.Slot))
		return false
	}
	pctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	h, err := r.auto.probe(pctx, addr, old.Slot)
	cancel()
	if err != nil || h.Slot != old.Slot || h.State != helloJoining {
		// Nothing joins under that slot yet, or the certificate at that
		// address names another slot.
		return false
	}
	if h.Address == "" {
		h.Address = addr
	}
	x, err := r.replacement(ctx, eng, m, h)
	if err != nil {
		r.noteReplace(fmt.Sprintf("cluster: autopilot: the node joining slot %s does not replace %s, out of reach for DEAD_MEMBER_TIMEOUT: %v", h.Slot, old.ID, err))
		return false
	}
	why := fmt.Sprintf("replaces %s of slot %s, out of reach for DEAD_MEMBER_TIMEOUT", old.ID, old.Slot)
	var refused error
	if r.autoChange(ctx, ActionAddLearner, x.ID, why, func(ctx context.Context, op consensus.Operator, _ consensus.NodeID) error {
		refused = eng.AddLearner(ctx, op, x)
		return refused
	}) {
		r.auto.replaceNoted = ""
		return true
	}
	r.noteReplace(fmt.Sprintf("cluster: autopilot: adding %s, the node joining slot %s, in place of %s: %v; retried", x.ID, h.Slot, old.ID, refused))
	return false
}

// replacement checks a joining hello for the replacement of a lost voter,
// as authorizeJoin checks a JOIN admitted on the CA, and returns the
// learner to add.
func (r *Replicator) replacement(ctx context.Context, eng consensus.Engine, m consensus.Membership, h hello) (consensus.Member, error) {
	id, err := ParseNodeID(h.Node)
	if err != nil {
		return consensus.Member{}, fmt.Errorf("its hello names no node id: %w", err)
	}
	var inc consensus.Incarnation
	b, err := hex.DecodeString(h.Incarnation)
	if err != nil || len(b) != len(inc) {
		return consensus.Member{}, errors.New("its hello carries no incarnation: its binary does not replace a lost voter by itself, or its key changed since its consensus node opened and it waits for a restart")
	}
	copy(inc[:], b)
	if Rank(m, id) > 0 || slices.Contains(m.Retired, id) {
		return consensus.Member{}, fmt.Errorf("node id %s is a member, or retired", id)
	}
	if _, _, err := net.SplitHostPort(h.Address); err != nil {
		return consensus.Member{}, fmt.Errorf("its address: %w", err)
	}
	spki := hex.EncodeToString(h.SPKI[:])
	if h.SPKI == ([32]byte{}) || holdsKey(m, spki) {
		return consensus.Member{}, errors.New("it presents the key of a member, its old id's: a node that takes a new id needs a new key, made by the renewer (RENEW_VAULT_ADDR) before its new id is stamped, or installed by an operator, the node then restarted")
	}
	// The revocations are read once this node's table applied a commit
	// boundary a quorum confirmed, as for a JOIN (#70).
	if err := r.appliedToBoundary(ctx); err != nil {
		return consensus.Member{}, fmt.Errorf("the cluster state cannot be confirmed on this node: %w", err)
	}
	if r.table.KeyRevoked(spki) || r.revocationAsked(spki) {
		return consensus.Member{}, errors.New("its key was revoked, or its revocation is asked")
	}
	if g, ok := r.table.JoinGrant(h.Slot); ok && g.SPKI != "" && g.SPKI != spki {
		return consensus.Member{}, fmt.Errorf("slot %s was granted to another key", h.Slot)
	}
	if err := r.auto.admits(h.Slot, eng); err != nil {
		return consensus.Member{}, err
	}
	if v := r.table.WriteVersion(); v > reftable.BaseVersion && !appliesVersion(h, v) {
		return consensus.Member{}, fmt.Errorf("its binary applies payload version %d, the cluster writes %d", max(h.Payload, reftable.BaseVersion), v)
	}
	if gate := r.nomadGate(); gate != nil {
		if err := gate.CheckJoin(ctx, h.Slot, h.Address); err != nil {
			return consensus.Member{}, err
		}
	}
	return consensus.Member{ID: id, Incarnation: inc, Slot: h.Slot, SPKI: h.SPKI, Address: h.Address}, nil
}

// noteReplace logs why a lost voter is not replaced, once for each reason.
func (r *Replicator) noteReplace(why string) {
	if why == r.auto.replaceNoted {
		return
	}
	r.auto.replaceNoted = why
	log.Warn("%s", why)
}

// revokedLearner names a learner whose key this node's table holds revoked:
// one admitted by a member whose table had not applied the revocation yet
// (#70). It is removed, never promoted. So is a learner whose key is asked
// for revocation: a refused change withdraws its request (#171), and one
// left open is an operator's Remove or DenyPin that may have been made.
func (r *Replicator) revokedLearner(m consensus.Membership) (consensus.NodeID, bool) {
	for _, x := range m.Members {
		if !x.Voter && r.revokedOrAsked(x.SPKI) {
			return x.ID, true
		}
	}
	return 0, false
}

// promoteLag is how far behind the commit a learner may be and still be
// promoted: the engine's own bound (PromoteLag), which refuses the rest.
const promoteLag = 1024

// promotable says whether a learner is ready to vote: the leader does not
// count it out of reach (downAfter), it receives no snapshot and is caught up
// (#92, #94), and, once the cluster writes a payload version above the
// base, its binary applies that version: an older binary made a voter would
// stop on the first entry it cannot read, or lead and lose what it cannot
// keep (#50). Its hello is asked once per pass, and only then.
func (r *Replicator) promotable(ctx context.Context, eng consensus.Engine) func(consensus.Member) bool {
	peers := map[consensus.NodeID]consensus.PeerHealth{}
	for _, p := range eng.Health().Peers {
		peers[p.ID] = p
	}
	var seen []hello
	asked := false
	return func(x consensus.Member) bool {
		p, ok := peers[x.ID]
		if !ok || r.revokedOrAsked(x.SPKI) || p.Unreachable || p.Snapshot || p.Lag > promoteLag || p.LastContact > r.auto.downAfter {
			return false
		}
		v := r.table.WriteVersion()
		if v <= reftable.BaseVersion {
			return true
		}
		if !asked {
			seen, asked = r.auto.round(ctx), true
		}
		return slices.ContainsFunc(seen, func(h hello) bool {
			return h.Node == x.ID.String() && appliesVersion(h, v) && r.trustedAnswer(x, h)
		})
	}
}

// appliesVersion reports whether the binary that answered applies payload
// version v; a binary older than the field applies the base version only.
func appliesVersion(h hello, v uint8) bool {
	return max(h.Payload, reftable.BaseVersion) >= int(v)
}

// The leader that cannot derive the SQL gives the leadership away: only
// the leader runs the consumer, and nothing in the election knows about
// the database, so a leader whose writer hook keeps failing, or whose
// consumer keeps failing while events wait, would otherwise freeze the SQL
// of the whole cluster, then every write once the event queue is full
// (#47). It yields to the voter least behind, at most once per
// yieldBackoff, so a database down for every node does not make the
// leadership spin.
var (
	yieldAfterPromotions = 5
	yieldAfterPasses     = 60
	yieldBackoff         = 5 * time.Minute
)

// yieldIfStuck transfers the leadership when this leader cannot derive the
// SQL, and reports whether it did.
func (r *Replicator) yieldIfStuck(ctx context.Context, eng consensus.Engine, m consensus.Membership) bool {
	var why string
	switch {
	case r.writerHook != nil && r.hookFails >= yieldAfterPromotions:
		why = fmt.Sprintf("its writer hook failed %d times in a row", r.hookFails)
	case int(r.consumerFails.Load()) >= yieldAfterPasses && r.table.EventCount() > 0:
		why = fmt.Sprintf("its consumer failed %d passes in a row with %d events waiting", r.consumerFails.Load(), r.table.EventCount())
	default:
		return false
	}
	now := r.auto.now()
	if !r.yielded.IsZero() && now.Sub(r.yielded) < yieldBackoff {
		return false
	}
	to, ok := successor(eng, m)
	if !ok {
		return false
	}
	r.yielded = now
	if err := eng.TransferLeadership(ctx, autopilotOperator, to); err != nil {
		log.Warn("cluster: autopilot: this leader cannot derive the SQL (%s); transferring the leadership to %s: %v", why, to, err)
		return false
	}
	r.sqlYield.Store(now.Unix())
	log.Error("cluster: autopilot: this leader cannot derive the SQL (%s): the leadership goes to %s", why, to)
	r.audit(ctx, autopilotOperator, Operation{Action: ActionTransfer, Target: to.String()}, "ok", "the leader cannot derive the SQL: "+why)
	return true
}

// successor is the voter other than the leader the leader hears and that
// is least behind.
func successor(eng consensus.Engine, m consensus.Membership) (consensus.NodeID, bool) {
	self := eng.Status().ID
	var best consensus.PeerHealth
	found := false
	for _, p := range eng.Health().Peers {
		i := Rank(m, p.ID) - 1
		if p.ID == self || i < 0 || !m.Members[i].Voter || p.Unreachable || p.Snapshot {
			continue
		}
		if !found || p.Lag < best.Lag {
			best, found = p, true
		}
	}
	return best.ID, found
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
// rather than letting it wait for a leader that cannot come from it. One
// failed probe is not that answer: the engine fails every read in flight
// when the node's role changes, its own promotion from learner included,
// leader and term unchanged (#199). A failed probe is asked again, as
// WriteRefusal does, within writeLeaderWait or the caller's deadline, and
// the membership is read again before each probe and once more before the
// refusal: a node promoted meanwhile passes as a voter, a leader that
// answers a later probe passes, and only a learner that no leader answered
// for the whole wait is refused.
func (r *Replicator) learnerGate(ctx context.Context, eng consensus.Engine) error {
	if r.auto == nil || eng == nil {
		return nil
	}
	ctx, cancel := context.WithTimeout(ctx, writeLeaderWait)
	defer cancel()
	for {
		m := eng.Membership()
		if i := Rank(m, eng.Status().ID); i > 0 && m.Members[i-1].Voter {
			return nil
		}
		if ctx.Err() != nil {
			return errReadOnly
		}
		if probeLeader(ctx, eng) {
			return nil
		}
		select {
		case <-ctx.Done():
		case <-time.After(100 * time.Millisecond):
		}
	}
}
