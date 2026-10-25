// Copyright 2026 The Forgejo Authors. All rights reserved.
// SPDX-License-Identifier: GPL-3.0-or-later

package cluster

import (
	"context"
	"fmt"
	"slices"
	"strings"
	"sync"
	"time"

	cluster_model "forgejo.org/models/cluster"
	"forgejo.org/modules/log"
	"forgejo.org/modules/setting"
	"forgejo.org/services/cluster/consensus"
)

// Only the leader takes the checkpoints, and only when its own [cluster]
// CHECKPOINT_INTERVAL is set; the members may set it differently. Once a
// checkpoint closed, its backup cursor holds the trim of the event queue
// until the next one (R8-23, R9-14): a leader that takes none would let the
// queue fill until every write is refused (#176). The leader is therefore a
// member that takes them whenever a voter does: one that takes none hands
// the leadership to one that does, caught up, at most once per
// yieldBackoff; one that takes them never hands it over for them, so two
// members never pass it back and forth.

// What a hello says of the answering member's CHECKPOINT_INTERVAL. An older
// binary says nothing, which is neither.
const (
	checkpointsOn  = "on"
	checkpointsOff = "off"
)

// checkpointsWord is what this node's hello says of its checkpoints.
func (r *Replicator) checkpointsWord() string {
	if r.checkpoints.Load() != nil {
		return checkpointsOn
	}
	return checkpointsOff
}

// checkpointYieldLag is how far behind the commit a member that takes the
// checkpoints may be and still be handed the leadership: the transfer
// itself brings it the rest.
const checkpointYieldLag = 64

// checkpointHandBack is how long a member that gave the leadership away
// because it could not derive the SQL (yieldIfStuck) is not handed it back
// for its checkpoints: it would give it away again, and the leadership
// would go back and forth between the two. Past it, the member is tried
// again, at most once in that time.
var checkpointHandBack = time.Hour

// In the manual mode no autopilot runs: nothing makes a leader that cannot
// derive the SQL give the leadership away, and an operator's transfer is the
// remedy (#47). yieldIfStuck, whose time the hello says (sql_yield), never
// runs there either, so the pilot of the leader handed the leadership back,
// for its checkpoints, to the very taker the operator had moved it away
// from, and the SQL stayed frozen for the whole cluster (#217). Each member
// therefore says it in the manual mode: from the pass of its consumer, as
// the leader, that makes yieldAfterPasses failed in a row while events wait,
// until one of its passes, as the leader again, goes through while events
// are there, its hello's sql_yield is the time of its last failed pass or of
// its pilot's last tick. Only the leader runs the consumer, so a follower
// cannot know better, and its pilot keeps the time fresh. takerHold then
// keeps the leadership away from it for as long as it says so, on a leader
// of this binary or of the previous one alike, and the operator's transfer
// stands. The memory is the process's: a member that restarts is handed the
// leadership again, and so is one the operator transfers it to that derives
// the SQL. The autonomous mode's sql_yield stays yieldIfStuck's alone.

// sqlState is whether this node, when it last led, could not derive the
// SQL. The consumer's passes set and clear it, the pilot reads it; the
// manual mode's writes of sql_yield are made under mu, so a pilot's refresh
// never lands after the pass that cleared it.
type sqlState struct {
	mu    sync.Mutex
	stuck bool
}

// cannotDeriveSQL reports whether this node's consumer failed
// yieldAfterPasses passes in a row while events wait, as yieldIfStuck
// counts them.
func (r *Replicator) cannotDeriveSQL() bool {
	return int(r.consumerFails.Load()) >= yieldAfterPasses && r.table.EventCount() > 0
}

// noteLeaderPass is, in the manual mode, the word of the consumer's pass on
// this node, as the leader, that ended with err (consumerFails counted): it
// cannot derive the SQL once yieldAfterPasses failed in a row while events
// wait, and it can again once a pass goes through while events are there.
func (r *Replicator) noteLeaderPass(err error) {
	if r.auto != nil {
		return
	}
	s := &r.ckState.sql
	s.mu.Lock()
	defer s.mu.Unlock()
	switch {
	case err != nil && r.cannotDeriveSQL():
		if !s.stuck {
			log.Error("cluster: this leader cannot derive the SQL (its consumer failed %d passes in a row with %d events waiting): in the manual mode only an operator's transfer moves the leadership away from it; until it derives the SQL again as the leader, its hello says so, and it is not handed the leadership for its checkpoints", r.consumerFails.Load(), r.table.EventCount())
		}
		s.stuck = true
		r.sqlYield.Store(r.clock().Unix())
	case err == nil && s.stuck && r.table.EventCount() > 0:
		s.stuck = false
		r.sqlYield.Store(0)
		log.Warn("cluster: this leader derives the SQL again: its hello no longer says it cannot")
	}
}

// keepSQLYield is, in the manual mode, the pilot's tick on every member: it
// keeps sql_yield at now while this node could not derive the SQL when it
// last led, so the hold of takerHold never lapses before it can.
func (r *Replicator) keepSQLYield(now time.Time) {
	if r.auto != nil {
		return
	}
	s := &r.ckState.sql
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.stuck {
		r.sqlYield.Store(now.Unix())
	}
}

// checkpointProbeEvery paces the leader's question "do you take the
// checkpoints", as payloadProbeEvery paces the payload's.
var checkpointProbeEvery = payloadProbeEvery

// checkpointView is what the leader last found of the checkpoints: when it
// asked, the voters that take them and those that did not say, by slot, and
// for each voter of On it would not hand the leadership to, "slot: why"
// (takerHold).
type checkpointView struct {
	At      time.Time
	On      []string
	Unknown []string
	Held    []string
}

// checkpointViewFresh is how long the leader's view of the takers stands.
const checkpointViewFresh = 2 * payloadProbeEvery

// checkpointState is the leader's view of the takers, and this node's SQL
// as the manual mode's hand-over reads it (sqlState).
type checkpointState struct {
	mu   sync.Mutex
	view checkpointView
	sql  sqlState
}

func (s *checkpointState) set(v checkpointView) {
	s.mu.Lock()
	s.view = v
	s.mu.Unlock()
}

// fresh is the view, when it is recent enough to be told.
func (s *checkpointState) fresh(now time.Time) (checkpointView, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	v := s.view
	return v, !v.At.IsZero() && now.Sub(v.At) <= checkpointViewFresh
}

// clock is the autopilot's clock in the autonomous mode, the wall clock
// otherwise.
func (r *Replicator) clock() time.Time {
	if r.auto != nil && r.auto.now != nil {
		return r.auto.now()
	}
	return time.Now()
}

// memberAnswers asks the members who they are: in the autonomous mode the
// slots PEERS names, as the autopilot does, the members at their addresses
// otherwise.
func (r *Replicator) memberAnswers(ctx context.Context, eng consensus.Engine) []hello {
	if r.auto != nil {
		return r.auto.round(ctx)
	}
	return r.askMembers(ctx, eng.Membership(), eng.Status().ID)
}

// answerOf is the trusted answer of member x among seen.
func (r *Replicator) answerOf(x consensus.Member, seen []hello) (hello, bool) {
	i := slices.IndexFunc(seen, func(h hello) bool { return h.Node == x.ID.String() && r.trustedAnswer(x, h) })
	if i < 0 {
		return hello{}, false
	}
	return seen[i], true
}

// maybeYieldForCheckpoints is the leader's pass over the checkpoints, at
// most once per checkpointProbeEvery: it asks the members whether they take
// them, keeps what it found for the health, and hands the leadership to one
// that does when this leader takes none. It reports whether it did.
func (r *Replicator) maybeYieldForCheckpoints(ctx context.Context, eng consensus.Engine, m consensus.Membership) bool {
	now := r.clock()
	if eng.Status().Role != consensus.RoleLeader || now.Before(r.checkpointProbe) {
		return false
	}
	r.checkpointProbe = now.Add(checkpointProbeEvery)
	if r.checkpoints.Load() != nil {
		r.ckState.set(checkpointView{At: now, On: []string{eng.Status().Slot}})
		return false
	}
	seen := r.memberAnswers(ctx, eng)
	self := eng.Status().ID
	peers := peerHealth(eng)
	v := checkpointView{At: now}
	for _, x := range m.Members {
		if !x.Voter || x.ID == self {
			continue
		}
		h, ok := r.answerOf(x, seen)
		switch {
		case !ok || h.Checkpoints == "":
			v.Unknown = append(v.Unknown, x.Slot)
		case h.Checkpoints == checkpointsOn:
			v.On = append(v.On, x.Slot)
			p, known := peers[x.ID]
			if why := takerHold(h, p, known, now); why != "" {
				v.Held = append(v.Held, x.Slot+": "+why)
			}
		}
	}
	r.ckState.set(v)
	return r.yieldForCheckpoints(ctx, eng, m, seen)
}

// peerHealth is the engine's replication health of each peer, by id.
func peerHealth(eng consensus.Engine) map[consensus.NodeID]consensus.PeerHealth {
	peers := map[consensus.NodeID]consensus.PeerHealth{}
	for _, p := range eng.Health().Peers {
		peers[p.ID] = p
	}
	return peers
}

// takerHold is why the leader does not hand the leadership, for the
// checkpoints, to a voter that takes them and answered h, "" when it would:
// it could not derive the SQL within checkpointHandBack (it gave the
// leadership away for it, or, in the manual mode, says it has not derived
// it since it last led: noteLeaderPass), or its replication health p
// (known when the engine has one) says it is unreachable, receives a
// snapshot, lags more than checkpointYieldLag or was last heard of more
// than checkpointContact ago.
func takerHold(h hello, p consensus.PeerHealth, known bool, now time.Time) string {
	switch {
	case h.SQLYield != 0 && now.Sub(time.Unix(h.SQLYield, 0)) < checkpointHandBack:
		return fmt.Sprintf("could not derive the SQL as of %s, not handed the leadership back within %s of it", time.Unix(h.SQLYield, 0).UTC().Format(time.RFC3339), checkpointHandBack)
	case !known:
		return "no replication health for it"
	case p.Unreachable:
		return "unreachable"
	case p.Snapshot:
		return "receiving a snapshot"
	case p.Lag > checkpointYieldLag:
		return fmt.Sprintf("%d entries behind, at most %d", p.Lag, checkpointYieldLag)
	case p.LastContact > checkpointContact:
		return fmt.Sprintf("last heard of %s ago, at most %s", p.LastContact.Round(time.Millisecond), checkpointContact)
	}
	return ""
}

// yieldForCheckpoints hands the leadership of this leader, which takes no
// checkpoint, to the voter that takes them, answers, receives no snapshot,
// is at most checkpointYieldLag behind, and has not given the leadership
// away for want of the SQL within checkpointHandBack: the least behind,
// the eldest on a tie. At most once per yieldBackoff, as yieldIfStuck. It
// reports whether it did.
func (r *Replicator) yieldForCheckpoints(ctx context.Context, eng consensus.Engine, m consensus.Membership, seen []hello) bool {
	if r.checkpoints.Load() != nil {
		return false
	}
	now := r.clock()
	if !r.checkpointYielded.IsZero() && now.Sub(r.checkpointYielded) < yieldBackoff {
		return false
	}
	peers := peerHealth(eng)
	self := eng.Status().ID
	var to consensus.Member
	var best consensus.PeerHealth
	found := false
	for _, x := range m.Members {
		if !x.Voter || x.ID == self {
			continue
		}
		h, ok := r.answerOf(x, seen)
		if !ok || h.Checkpoints != checkpointsOn {
			continue
		}
		p, known := peers[x.ID]
		if takerHold(h, p, known, now) != "" {
			continue
		}
		if !found || p.Lag < best.Lag {
			to, best, found = x, p, true
		}
	}
	if !found {
		return false
	}
	r.checkpointYielded = now
	if err := eng.TransferLeadership(ctx, autopilotOperator, to.ID); err != nil {
		log.Warn("cluster: this leader takes no checkpoint; transferring the leadership to %s (%s), which does: %v", to.ID, to.Slot, err)
		return false
	}
	why := "this leader takes no checkpoint ([cluster] CHECKPOINT_INTERVAL is 0 here), " + to.Slot + " does"
	log.Warn("cluster: the leadership goes to %s (%s): %s", to.ID, to.Slot, why)
	r.audit(ctx, autopilotOperator, Operation{Action: ActionTransfer, Target: to.ID.String()}, "ok", why)
	return true
}

// checkpointContact bounds the last contact of a member handed the
// leadership for its checkpoints.
const checkpointContact = 10 * time.Second

// checkpointsRefusal is why a voter does not start: its CHECKPOINT_INTERVAL
// is 0, the backup cursor of its lineage exists, and every other voter
// answered that it takes no checkpoint either. No checkpoint would ever
// close again, and the event queue would fill until every write is refused
// (#176). It refuses on certainties only: this node's own configuration,
// the cursor read from SQL, and an answer from every other voter, each from
// a binary that says whether it takes them. Anything it cannot read, a
// voter that does not answer, a binary that does not say, lets the node
// start: the health and the log say the rest. It returns nil to start.
func (r *Replicator) checkpointsRefusal(ctx context.Context, eng consensus.Engine) error {
	if setting.Cluster.CheckpointInterval > 0 || r.checkpoints.Load() != nil || r.consumer == nil || r.consumer.cursor == nil {
		return nil
	}
	st := eng.Status()
	m := eng.Membership()
	if i := Rank(m, st.ID); i == 0 || !m.Members[i-1].Voter {
		return nil
	}
	lineage := st.Identity.String()
	b, err := r.consumer.cursor(ctx, cluster_model.CursorBackup)
	if err != nil || b == nil || b.Lineage != lineage {
		return nil
	}
	actx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	seen := r.memberAnswers(actx, eng)
	var off []string
	for _, x := range m.Members {
		if !x.Voter || x.ID == st.ID {
			continue
		}
		h, ok := r.answerOf(x, seen)
		if !ok || h.Checkpoints != checkpointsOff {
			// Unknown, or a voter that takes them: no certainty.
			return nil
		}
		off = append(off, x.Slot)
	}
	where := "on this voter, the only one"
	if len(off) > 0 {
		where = "on this voter and on every other one (" + strings.Join(off, ", ") + ")"
	}
	return fmt.Errorf("[cluster] CHECKPOINT_INTERVAL is 0 %s, while the backup cursor of lineage %s stands at %d: no checkpoint would close again, and the event queue would not be trimmed past it until every write is refused (#176); set CHECKPOINT_INTERVAL on this node, or on another voter first", where, lineage, b.Position)
}

// RefuseWithoutCheckpoints is the start's refusal of checkpointsRefusal,
// once the database is up: the replicator starts before it and does not
// depend on it (routers/init.go), while the backup cursor is read from SQL;
// read before, the start panicked on a database not opened yet. A refused
// voter closes its consensus node, which hands the leadership over first,
// and the start fails with why. It returns nil to go on.
func RefuseWithoutCheckpoints(ctx context.Context) error {
	r := Current()
	if r == nil {
		return nil
	}
	eng := r.Engine()
	if eng == nil {
		return nil
	}
	err := r.checkpointsRefusal(ctx, eng)
	if err == nil {
		return nil
	}
	cctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if cerr := eng.Close(cctx); cerr != nil {
		log.Error("cluster: closing the consensus node: %v", cerr)
	}
	return err
}
