// Copyright 2026 The Forgejo Authors. All rights reserved.
// SPDX-License-Identifier: GPL-3.0-or-later

package cluster

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"time"

	auth_model "forgejo.org/models/auth"
	cluster_model "forgejo.org/models/cluster"
	"forgejo.org/models/db"
	user_model "forgejo.org/models/user"
	"forgejo.org/modules/clusterhook"
	"forgejo.org/modules/log"
	"forgejo.org/modules/setting"
	"forgejo.org/services/cluster/consensus"
	"forgejo.org/services/cluster/reftable"
)

// MemberView is one member as this node sees it.
type MemberView struct {
	ID          string
	Slot        string
	Address     string
	SPKI        string
	Voter       bool
	Joining     bool
	Self        bool
	Leader      bool
	Match       uint64
	Lag         uint64
	LastContact time.Duration
	Unreachable bool
	Pinned      bool
	// Rank is the member's seniority, 1 for the eldest: the order of the
	// committed membership, which the voter rule reads.
	Rank int
	// Since is when this node first saw the member as it is now (voter or
	// learner), in the autonomous mode; zero otherwise.
	Since time.Time
	// Known is set when this node knows the member's progress: on the
	// leader, for every other member.
	Known    bool
	Snapshot bool
	// LastContactText is LastContact rounded for display, empty if never.
	LastContactText string
}

// RemovedView is a member the cluster removed, and since when this node
// knows it; its slot is empty when this node never saw it as a member.
type RemovedView struct {
	ID    string
	Slot  string
	Since time.Time
}

// AlarmView is a replicated alarm.
type AlarmView struct {
	ID    uint64
	Kind  string
	Nodes []string
}

// View is what the administration and `forgejo cluster status` show.
type View struct {
	Slot           string
	Ready          bool
	Fenced         bool
	Role           string
	Term           uint64
	Leader         string
	Commit         uint64
	Applied        uint64
	Identity       string
	ClusterVersion uint32
	// PayloadVersion is the command version the cluster writes, and
	// PayloadMax the highest this binary applies.
	PayloadVersion   uint8
	PayloadMax       uint8
	FailureTolerance int
	QuorumReachable  bool
	Stopped          bool
	StopReason       string
	CertExpiry       time.Time
	SPKI             string
	Members          []MemberView
	Retired          []string
	Alarms           []AlarmView
	Root             string
	Repos            int
	Events           int
	EventBytes       int64
	TxnsInFlight     int
	ConsumerLag      uint64
	// ParkedEvents are the events whose post-receive work waits for a
	// retry; -1 when the database cannot say.
	ParkedEvents int64
	// SQLDurable and SQLDurability are the last verdict on whether SQL
	// survives a failover of the database; the event queue is trimmed only
	// while it does. SQLDurabilityChecked is zero before the first check.
	SQLDurable           bool
	SQLDurability        string
	SQLDurabilityChecked time.Time
	// Parked lists the first parked entries with their error.
	Parked []ParkedView
	// BackupLag is how far the PostgreSQL backup lags the cursor while it
	// holds the trim beyond its bound; 0 otherwise.
	BackupLag uint64
	// EventQueueMax is the bound past which pushes are refused;
	// EventQueueAlarm is set past EventQueueAlarmShare of it.
	EventQueueMax   int64
	EventQueueAlarm bool
	ApplyLagEntries uint64
	// SnapshotBytes is the newest consensus snapshot's size, SnapshotLimit
	// the size its cuts are held to; SnapshotNearLimit past four fifths.
	SnapshotBytes     int64
	SnapshotLimit     int64
	SnapshotNearLimit bool
	Lag               []RepoLag
	Audit             []reftable.AuditRecord
	Started           time.Time
	// ExpectedNodes is [cluster] EXPECTED_NODES, 0 in the manual mode, and
	// VoterTarget the voters the rule wants with the members present.
	ExpectedNodes int
	VoterTarget   int
	// Removed are the retired members, in the autonomous mode.
	Removed []RemovedView
}

// View collects the node's status.
func (r *Replicator) View(ctx context.Context) View {
	v := View{
		Slot: setting.Cluster.Slot, Fenced: r.projector.Fenced(), Ready: r.Ready(),
		Root: r.table.Root().String(), Repos: len(r.table.Repos()), Events: r.table.EventCount(),
		EventBytes: r.table.EventBytes(), TxnsInFlight: r.registry.count(), Started: r.started,
		Audit: r.table.Audit(),
	}
	if r.objects != nil && r.objects.id != nil {
		v.CertExpiry = r.objects.id.Expiry()
		spki := r.objects.id.SPKI()
		v.SPKI = hex.EncodeToString(spki[:])
	}
	if len(v.Audit) > 100 {
		v.Audit = v.Audit[:100]
	}
	v.Lag = r.projector.Lag()
	if len(v.Lag) > 50 {
		v.Lag = v.Lag[:50]
	}
	ch := r.ConsumerHealth(ctx)
	v.SQLDurable, v.SQLDurability, v.SQLDurabilityChecked, v.ParkedEvents = ch.SQLDurable, ch.SQLDurability, ch.SQLDurabilityChecked, ch.ParkedEvents
	v.BackupLag, v.EventQueueMax, v.EventQueueAlarm = ch.BackupLag, ch.EventQueueMax, ch.EventQueueAlarm
	if v.ParkedEvents > 0 {
		if ps, err := r.ListParked(ctx); err == nil {
			if len(ps) > 100 {
				ps = ps[:100]
			}
			v.Parked = ps
		}
	}
	if r.consumer != nil {
		v.ConsumerLag = r.consumer.Lag()
	}
	eng := r.Engine()
	if eng == nil {
		return v
	}
	st := eng.Status()
	h := eng.Health()
	v.Role, v.Term, v.Commit, v.Applied = st.Role.String(), st.Term, st.Commit, st.Applied
	if st.Leader != 0 {
		v.Leader = st.Leader.String()
	}
	v.Identity, v.ClusterVersion = st.Identity.String(), st.ClusterVersion
	v.PayloadVersion, v.PayloadMax = r.table.WriteVersion(), reftable.PayloadVersion
	v.FailureTolerance, v.QuorumReachable, v.Stopped, v.StopReason = h.FailureTolerance, h.QuorumReachable, h.Stopped, h.StopReason
	v.ApplyLagEntries = h.ApplyLagEntries
	v.SnapshotBytes, v.SnapshotLimit, v.SnapshotNearLimit = h.SnapshotBytes, h.SnapshotLimit, h.SnapshotNearLimit
	for _, a := range h.Alarms {
		av := AlarmView{ID: uint64(a.ID), Kind: a.Kind}
		for _, n := range a.Nodes {
			av.Nodes = append(av.Nodes, n.String())
		}
		v.Alarms = append(v.Alarms, av)
	}
	peers := map[consensus.NodeID]consensus.PeerHealth{}
	for _, p := range h.Peers {
		peers[p.ID] = p
	}
	ms := eng.Membership()
	if r.auto != nil {
		v.ExpectedNodes = r.auto.expected
		v.VoterTarget = VoterTarget(len(ms.Members))
		v.Removed = r.removedRecords(ms)
	}
	for i, m := range ms.Members {
		mv := MemberView{
			ID: m.ID.String(), Slot: m.Slot, Address: m.Address, SPKI: hex.EncodeToString(m.SPKI[:]),
			Voter: m.Voter, Joining: m.Joining, Self: m.ID == st.ID, Leader: m.ID == st.Leader,
			Pinned: m.SPKI != [32]byte{}, Rank: i + 1,
		}
		if r.auto != nil {
			if rec, ok := r.record(m.ID); ok {
				mv.Since = rec.Since
			}
		}
		if p, ok := peers[m.ID]; ok {
			mv.Match, mv.Lag, mv.LastContact, mv.Unreachable = p.Match, p.Lag, p.LastContact, p.Unreachable
			mv.Known, mv.Snapshot = true, p.Snapshot
			if p.LastContact >= 0 {
				mv.LastContactText = p.LastContact.Round(time.Millisecond).String()
			}
		}
		v.Members = append(v.Members, mv)
	}
	for _, id := range ms.Retired {
		v.Retired = append(v.Retired, id.String())
	}
	return v
}

// ConsumerHealth is the part of View the health check reads: SQL's
// durability and the parked events, without the engine's status.
func (r *Replicator) ConsumerHealth(ctx context.Context) View {
	var v View
	if r.consumer != nil {
		d := r.consumer.Durability()
		v.SQLDurable, v.SQLDurability, v.SQLDurabilityChecked = d.OK, d.Reason, d.Checked
	}
	v.ParkedEvents = -1
	if db.DefaultContext != nil {
		if n, err := cluster_model.CountParked(ctx); err == nil {
			v.ParkedEvents = n
		}
	}
	if r.consumer != nil {
		v.BackupLag = r.consumer.BackupLag()
	}
	if r.table != nil {
		v.EventBytes, v.EventQueueMax = r.table.EventBytes(), reftable.MaxEventBytes
		v.EventQueueAlarm = v.EventBytes >= int64(float64(reftable.MaxEventBytes)*EventQueueAlarmShare)
	}
	return v
}

// EventQueueAlarmShare is the share of the event queue's bound at which the
// health check and the administration warn, before pushes are refused.
const EventQueueAlarmShare = 0.5

// Action names an online operation of ADR 0152 D17.
type Action string

const (
	ActionAddLearner     Action = "add-learner"
	ActionPromote        Action = "promote"
	ActionRemove         Action = "remove"
	ActionTransfer       Action = "transfer-leadership"
	ActionDenyPin        Action = "deny-pin"
	ActionClearNoSpace   Action = "clear-no-space"
	ActionUpdateAddress  Action = "update-address"
	ActionAuthorizeJoin  Action = "authorize-join"
	ActionGrantOperator  Action = "grant-operator"
	ActionRevokeOperator Action = "revoke-operator"
	// ActionRaisePayload makes the cluster write the command version this
	// binary applies (Target), once every member runs it.
	ActionRaisePayload Action = "raise-payload-version"
)

// Actions lists the operations the administration offers.
var Actions = []Action{ActionAddLearner, ActionPromote, ActionRemove, ActionTransfer, ActionDenyPin, ActionClearNoSpace, ActionUpdateAddress, ActionAuthorizeJoin, ActionGrantOperator, ActionRevokeOperator, ActionRaisePayload}

// Valid reports whether a is an operation.
func (a Action) Valid() bool {
	return slices.Contains(Actions, a)
}

// Operation is one request of the administration.
type Operation struct {
	Action Action
	// Target is a node id for most actions, a member spec
	// "id:incarnation:slot:spki:address" for add-learner, an alarm id for
	// clear-no-space.
	Target string
	// Address is the new address of update-address.
	Address string
}

// OperatorFor is the opaque operator identity of a Forgejo account: its id,
// never its name or address, since the log keeps it (R3-24).
func OperatorFor(userID int64) consensus.Operator {
	return consensus.Operator("forgejo-user:" + strconv.FormatInt(userID, 10))
}

func operatorOrLocal(s string) consensus.Operator {
	op := consensus.Operator(s)
	if op.Validate() != nil {
		op = consensus.Operator("local-uid:" + strconv.Itoa(os.Getuid()))
	}
	return op
}

// Operate runs one online operation on behalf of op. It can be submitted on
// any node: the engine forwards it to the leader (D17, Q8). The request and
// its outcome are written to the replicated audit and logged at Warn; an
// audit that cannot be written does not block the operation, so clearing a
// no-space alarm stays possible while ordinary entries are refused.
func (r *Replicator) Operate(ctx context.Context, op consensus.Operator, o Operation) error {
	if err := op.Validate(); err != nil {
		return err
	}
	if !o.Action.Valid() {
		return fmt.Errorf("unknown operation %q", o.Action)
	}
	eng := r.Engine()
	if eng == nil {
		return errors.New("the consensus node is not open")
	}
	log.Warn("cluster: operator %s requests %s %s", op, o.Action, o.Target)
	// The replicated log keeps what it is given: an account is recorded by
	// its id, never by its login (R3-24), like the operator itself.
	recorded := o
	if o.Action == ActionGrantOperator || o.Action == ActionRevokeOperator {
		recorded.Target = "unknown-account"
		if u, err := user_model.GetUserByName(ctx, o.Target); err == nil {
			recorded.Target = string(OperatorFor(u.ID))
		}
	}
	r.audit(ctx, op, recorded, "requested", "")
	err := r.operate(ctx, eng, op, o)
	outcome, detail := "ok", ""
	switch {
	case errors.Is(err, ErrRevocationPending):
		detail = err.Error()
	case err != nil:
		outcome, detail = "failed", err.Error()
	}
	log.Warn("cluster: operator %s %s %s: %s %s", op, o.Action, o.Target, outcome, detail)
	if err != nil && recorded.Target != o.Target {
		detail = accountErrorDetail(err)
	}
	r.audit(ctx, op, recorded, outcome, detail)
	return err
}

// accountErrorDetail is what the replicated audit keeps of a failed grant
// or withdrawal: a fixed text, never the error's own, which may name the
// login (ErrUserNotExist formats it). The node's log has the full error.
func accountErrorDetail(err error) string {
	switch {
	case user_model.IsErrUserNotExist(err):
		return "unknown user"
	case errors.Is(err, ErrNotSiteAdmin):
		return ErrNotSiteAdmin.Error()
	case errors.Is(err, ErrLastOperator):
		return ErrLastOperator.Error()
	default:
		return "failed; see the node's log"
	}
}

func (r *Replicator) operate(ctx context.Context, eng consensus.Engine, op consensus.Operator, o Operation) error {
	if o.Action == ActionAddLearner {
		m, err := ParseMember(o.Target)
		if err != nil {
			return err
		}
		if r.table.KeyRevoked(hex.EncodeToString(m.SPKI[:])) {
			return errors.New("this key was revoked: the node needs a new key")
		}
		return eng.AddLearner(ctx, op, m)
	}
	if o.Action == ActionAuthorizeJoin {
		return r.grantJoin(ctx, op, o.Target)
	}
	if o.Action == ActionGrantOperator || o.Action == ActionRevokeOperator {
		return changeOperator(ctx, op, o)
	}
	if o.Action == ActionRaisePayload {
		if o.Target != strconv.Itoa(reftable.PayloadVersion) {
			return fmt.Errorf("this binary raises the payload version to %d only", reftable.PayloadVersion)
		}
		// In the autonomous mode every member says which version its
		// binary applies: the raise waits for all of them, as the
		// autopilot's does, since a member on an older binary stops on the
		// first entry of the new version, and two of three lose the
		// quorum (#50).
		// Outside it, every member answers a member that asks: an older
		// binary does not, and the raise waits for it all the same.
		var seen []hello
		if r.auto != nil {
			seen = r.auto.round(ctx)
		} else {
			seen = r.askMembers(ctx, eng.Membership(), eng.Status().ID)
		}
		if !everyMemberApplies(eng.Membership(), eng.Status().ID, seen, r.trustedAnswer) {
			return fmt.Errorf("some member did not answer, or runs a binary that does not apply payload version %d: the version is not raised", reftable.PayloadVersion)
		}
		if err := r.RaisePayloadVersion(ctx, op); err != nil {
			return err
		}
		// Outside the autonomous mode nothing else writes them: a failure
		// is the operator's to see, and raising again retries it.
		if err := r.flushRevocations(ctx); err != nil {
			return fmt.Errorf("the payload version is raised, but a pending revocation is not written: %w", err)
		}
		return nil
	}
	if o.Action == ActionClearNoSpace {
		id, err := strconv.ParseUint(o.Target, 10, 64)
		if err != nil {
			return fmt.Errorf("alarm id: %w", err)
		}
		// The precondition names the alarm and its lineage, so a stale
		// click cannot clear an alarm raised since (R4-06, R5-09).
		h := eng.Health()
		return eng.ClearNoSpace(ctx, op, consensus.Precondition{Identity: h.Identity, AlarmID: consensus.AlarmID(id)})
	}
	id, err := ParseNodeID(o.Target)
	if err != nil {
		return err
	}
	switch o.Action {
	case ActionPromote:
		return eng.Promote(ctx, op, id)
	case ActionRemove, ActionDenyPin:
		if o.Action == ActionRemove {
			if err := r.removable(eng, id); err != nil {
				return err
			}
		}
		pending, deferred, err := r.revokeMember(ctx, eng, op, id)
		if err != nil {
			return err
		}
		if o.Action == ActionRemove {
			err = eng.Remove(ctx, op, id)
		} else {
			err = eng.DenyPin(ctx, op, id)
		}
		if err != nil {
			if deferred {
				// Nothing was recorded: the key stays usable, as the
				// member still holds it (#64).
				return err
			}
			log.Error("cluster: the key of %s was revoked, and the %s that followed was refused: %v; the member keeps its place with a revoked key until it is removed", id, o.Action, err)
			return err
		}
		if deferred {
			// Recorded once the change is made, so a refused change never
			// leaves a revocation for the next raise to write (#64).
			if err := r.recordPendingRevocation(ctx, pending); err != nil {
				return fmt.Errorf("recording the revocation of %s: %w", id, err)
			}
			log.Warn("cluster: the key of %s (slot %s) is not revoked yet: the cluster writes payload version %d, the revocation waits for %d", id, pending.Slot, r.table.WriteVersion(), reftable.RevokeVersion)
			return ErrRevocationPending
		}
		return nil
	case ActionTransfer:
		return eng.TransferLeadership(ctx, op, id)
	case ActionUpdateAddress:
		if _, _, err := net.SplitHostPort(o.Address); err != nil {
			return fmt.Errorf("address: %w", err)
		}
		return eng.UpdateAddress(ctx, op, id, o.Address)
	}
	return fmt.Errorf("unknown operation %q", o.Action)
}

func (r *Replicator) audit(ctx context.Context, op consensus.Operator, o Operation, outcome, detail string) {
	if len(detail) > 1024 {
		detail = detail[:1024]
	}
	target := o.Target
	if o.Address != "" {
		target += " -> " + o.Address
	}
	if len(target) > 256 {
		target = target[:256]
	}
	if len(outcome) > 256 {
		outcome = outcome[:256]
	}
	id := clusterhook.NewNonce()
	data, err := reftable.Encode(reftable.KindAudit, &reftable.Audit{
		ID: id, Operator: string(op), Action: string(o.Action), Target: target, Outcome: outcome, Detail: detail, Time: time.Now().Unix(),
	})
	if err != nil {
		return
	}
	actx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	// The record carries its own id, so an unknown outcome is settled by
	// finding it, and a retry never writes it twice.
	tok, _, err := r.proposeCommand(actx, data, func() bool { return r.table.HasAudit(id) })
	if err != nil {
		log.Warn("cluster: the audit of %s by %s could not be replicated: %v", o.Action, op, err)
		return
	}
	// A proposal forwarded from a follower answers once the leader applied
	// it; the page that follows reads this node's table, so wait for it.
	if err := r.Engine().WaitApplied(actx, tok); err != nil {
		log.Warn("cluster: the audit of %s by %s is committed and not yet applied here: %v", o.Action, op, err)
	}
}

// ParseNodeID reads a node id as printed: 16 hex digits.
func ParseNodeID(s string) (consensus.NodeID, error) {
	v, err := strconv.ParseUint(strings.TrimSpace(s), 16, 64)
	if err != nil || v == 0 {
		return 0, fmt.Errorf("node id %q must be 16 hex digits, not zero", s)
	}
	return consensus.NodeID(v), nil
}

// ParseMember reads "id:incarnation:slot:spki:address", what
// `forgejo cluster init` prints (D8, R9-04). The SPKI pin is required.
func ParseMember(s string) (consensus.Member, error) {
	parts := strings.SplitN(strings.TrimSpace(s), ":", 5)
	if len(parts) != 5 {
		return consensus.Member{}, errors.New("member must be id:incarnation:slot:spki:host:port")
	}
	id, err := ParseNodeID(parts[0])
	if err != nil {
		return consensus.Member{}, err
	}
	var m consensus.Member
	m.ID = id
	inc, err := hex.DecodeString(parts[1])
	if err != nil || len(inc) != 16 {
		return m, errors.New("incarnation must be 32 hex digits")
	}
	copy(m.Incarnation[:], inc)
	m.Slot = parts[2]
	if !slotOK(m.Slot) {
		return m, errors.New("slot must match [a-z0-9-]{1,32}")
	}
	spki, err := hex.DecodeString(parts[3])
	if err != nil || len(spki) != 32 || [32]byte(spki) == ([32]byte{}) {
		return m, errors.New("SPKI must be 64 hex digits, never empty")
	}
	copy(m.SPKI[:], spki)
	if _, _, err := net.SplitHostPort(parts[4]); err != nil {
		return m, fmt.Errorf("address: %w", err)
	}
	m.Address = parts[4]
	return m, nil
}

func slotOK(s string) bool {
	if len(s) == 0 || len(s) > 32 {
		return false
	}
	for i := 0; i < len(s); i++ {
		c := s[i]
		if !(c >= 'a' && c <= 'z' || c >= '0' && c <= '9' || c == '-') {
			return false
		}
	}
	return true
}

// grantJoin lets a new node join under a slot by itself for JOIN_GRANT_TTL:
// target is "slot" or "slot:spki" to admit one key only. The grant is a
// replicated entry, so every member answers a JOIN the same way.
func (r *Replicator) grantJoin(ctx context.Context, op consensus.Operator, target string) error {
	slot, spki, _ := strings.Cut(target, ":")
	g := &reftable.JoinGrant{Slot: slot, SPKI: strings.ToLower(spki), Expires: time.Now().Add(setting.Cluster.JoinGrantTTL).Unix(), Operator: string(op)}
	data, err := reftable.Encode(reftable.KindJoinGrant, g)
	if err != nil {
		return err
	}
	_, result, err := r.proposeCommand(ctx, data, nil)
	if err != nil {
		return err
	}
	if !result.OK {
		return errors.New(result.Reason)
	}
	return nil
}

// bindGrant replaces an unbound grant with the same grant bound to spki.
// From reftable.GenerationVersion Apply refuses it once another member
// bound the same grant to another key: two joiners admitted at once under
// one grant, the second is refused (#70).
func (r *Replicator) bindGrant(ctx context.Context, g reftable.JoinGrant, spki [32]byte) error {
	g.SPKI = hex.EncodeToString(spki[:])
	data, err := reftable.EncodeVersion(r.table.WriteVersion(), reftable.KindJoinGrant, &g)
	if err != nil {
		return err
	}
	_, result, err := r.proposeCommand(ctx, data, func() bool {
		cur, ok := r.table.JoinGrant(g.Slot)
		return ok && cur == g
	})
	if err != nil {
		return err
	}
	if !result.OK {
		return errors.New(result.Reason)
	}
	return nil
}

// ErrRevocationPending says an operation was done while the key it
// revokes is not revoked yet: the cluster writes a revocation once every
// member applies payload version reftable.RevokeVersion, and the node
// writes it then by itself.
var ErrRevocationPending = errors.New("the key is not revoked yet: some members run a binary that cannot apply a key revocation; it is written by itself once every member runs this version")

// revokeMember revokes the key of member id before an operator removes it
// or denies its pin: a removed node presenting that key, under any id, is
// refused at the JOIN and on the object channel. The autopilot's removal of
// a dead member does not come here, so that member returns.
//
// A revocation is written at payload version reftable.RevokeVersion only,
// which an older binary stops on: while the cluster writes an older
// version, nothing is written here and the revocation is returned as
// pending, for the caller to record once the change it precedes is made;
// flushRevocations writes it once the version is raised.
func (r *Replicator) revokeMember(ctx context.Context, eng consensus.Engine, op consensus.Operator, id consensus.NodeID) (pending reftable.RevokeKey, deferred bool, err error) {
	for _, m := range eng.Membership().Members {
		if m.ID != id || m.SPKI == ([32]byte{}) {
			continue
		}
		k := reftable.RevokeKey{SPKI: hex.EncodeToString(m.SPKI[:]), Slot: m.Slot, Operator: string(op), Time: time.Now().Unix()}
		if r.table.WriteVersion() < reftable.RevokeVersion {
			return k, true, nil
		}
		return k, false, r.writeRevocation(ctx, k)
	}
	return reftable.RevokeKey{}, false, nil
}

// removable refuses up front a removal the engine would refuse after the
// key was revoked: the leader itself (the engine never removes its leader;
// the leadership is transferred first), and a voter below the floor of
// voters. A revocation written before a refused change would leave the
// member in place with its key revoked, its object channel refused on every
// peer (#64).
func (r *Replicator) removable(eng consensus.Engine, id consensus.NodeID) error {
	if st := eng.Status(); st.Leader == id || (st.Role == consensus.RoleLeader && st.ID == id) {
		return fmt.Errorf("%w: %s leads the cluster: transfer the leadership to another voter first", consensus.ErrMembershipInvalid, id)
	}
	m := eng.Membership()
	i := Rank(m, id) - 1
	if i < 0 || !m.Members[i].Voter {
		return nil
	}
	// The engine's floor is three voters in both modes: the autonomous
	// mode sets it, and the engine defaults to it otherwise.
	if left := m.Voters() - 1; left < autoMinVoters {
		return fmt.Errorf("%w: removing %s would leave %d voters, the floor is %d", consensus.ErrMembershipInvalid, id, left, autoMinVoters)
	}
	if v, ok := strandedBy(m, eng.Health(), eng.Status().ID, id); ok {
		return fmt.Errorf("%w: %s does not answer or lags behind the membership: removing %s, which answers, may leave it no member it knows to vouch for the leader once it is back; bring %s back, or remove it first", consensus.ErrMembershipInvalid, v, id, v)
	}
	return nil
}

// voteWindow is the engine's default ElectionTimeoutMin: a peer the leader
// heard within it answers, for the engine's guard below.
const voteWindow = time.Second

// strandedBy names a voter the engine's leader may refuse to strand by the
// removal of target, a voter, as it refuses MEMBERSHIP_INVALID the removal
// of a member that answers when a voter of the new configuration might list
// neither the leader nor a voter that stays (kitsunium/sdk#603). The leader
// alone knows its peers; elsewhere the engine decides alone.
//
// It errs on the side of refusing: a voter that lags behind the membership
// may hold an older configuration, and a silent one listed before the
// leader may hold one from before the leader joined. A voter caught up
// with the membership lists the leader, which stays.
func strandedBy(m consensus.Membership, h consensus.Health, leader, target consensus.NodeID) (consensus.NodeID, bool) {
	if len(h.Peers) == 0 {
		return 0, false
	}
	peers := make(map[consensus.NodeID]consensus.PeerHealth, len(h.Peers))
	for _, p := range h.Peers {
		peers[p.ID] = p
	}
	silent := func(id consensus.NodeID) bool {
		p, ok := peers[id]
		return !ok || p.Unreachable || p.LastContact >= voteWindow
	}
	if silent(target) {
		// A member that does not answer vouches for no one now: the
		// engine never refuses its removal on this ground.
		return 0, false
	}
	for _, x := range m.Members {
		if !x.Voter || x.ID == leader || x.ID == target {
			continue
		}
		p, ok := peers[x.ID]
		if !ok || p.Match < m.At.Index || (silent(x.ID) && Rank(m, x.ID) < Rank(m, leader)) {
			return x.ID, true
		}
	}
	return 0, false
}

// recordPendingRevocation records a revocation the cluster cannot write
// yet, as a version 1 audit record every binary applies.
func (r *Replicator) recordPendingRevocation(ctx context.Context, k reftable.RevokeKey) error {
	id := clusterhook.NewNonce()
	data, err := reftable.Encode(reftable.KindAudit, &reftable.Audit{
		ID: id, Operator: k.Operator, Action: reftable.AuditRevokeKey, Target: k.Slot + ":" + k.SPKI,
		Outcome: reftable.AuditPending, Time: k.Time,
	})
	if err != nil {
		return err
	}
	_, result, err := r.proposeCommand(ctx, data, func() bool { return r.table.HasAudit(id) })
	if err != nil {
		return err
	}
	if !result.OK {
		return errors.New(result.Reason)
	}
	return nil
}

// writeRevocation writes one revocation, at reftable.RevokeVersion.
func (r *Replicator) writeRevocation(ctx context.Context, k reftable.RevokeKey) error {
	if r.table.WriteVersion() < reftable.RevokeVersion {
		return ErrRevocationPending
	}
	data, err := reftable.EncodeVersion(reftable.RevokeVersion, reftable.KindRevokeKey, &k)
	if err != nil {
		return err
	}
	_, result, err := r.proposeCommand(ctx, data, func() bool { return r.table.KeyRevoked(k.SPKI) })
	if err != nil {
		return fmt.Errorf("revoking the key of slot %s: %w", k.Slot, err)
	}
	if !result.OK {
		return fmt.Errorf("revoking the key of slot %s: %s", k.Slot, result.Reason)
	}
	return nil
}

// flushRevocations writes the pending revocations once the cluster writes
// reftable.RevokeVersion; before, it does nothing. The leader's autopilot
// calls it at each pass, and a raise of the payload version right after,
// which returns its error to the operator.
func (r *Replicator) flushRevocations(ctx context.Context) error {
	if r.table.WriteVersion() < reftable.RevokeVersion {
		return nil
	}
	var held map[string]string
	if eng := r.Engine(); eng != nil {
		held = map[string]string{}
		for _, m := range eng.Membership().Members {
			held[hex.EncodeToString(m.SPKI[:])] = m.Slot
		}
	}
	for _, k := range r.table.PendingRevocations() {
		if slot, ok := held[k.SPKI]; ok {
			// A member still holds the key: its removal was refused after
			// the revocation was recorded, by an older binary. Writing it
			// would cut a healthy member off the object channel (#64).
			log.Error("cluster: the pending revocation of slot %s is not written: member %s still holds the key; remove that member first", k.Slot, slot)
			continue
		}
		if err := r.writeRevocation(ctx, k); err != nil {
			log.Warn("cluster: writing the pending revocation of slot %s: %v", k.Slot, err)
			return err
		}
		log.Warn("cluster: the key of slot %s is revoked, the cluster now writes payload version %d", k.Slot, reftable.RevokeVersion)
	}
	return nil
}

// authorizeJoin is Forgejo's JoinAuthorizer (R8-09). The engine has already
// refused a slot that still has a member; Forgejo adds the fence decision:
// an operator granted the slot, the grant has not expired, and when it names
// a key, the joiner presents that key. The operator recorded is the one who
// granted it. Without a grant, a new node is added from the administration.
// Under Nomad, the joiner's Nomad node must not be draining, ineligible or
// down (ADR 0152 R8-09), read with a node:read token.
//
// In the autonomous mode, a slot nobody granted is admitted on the cluster's
// CA alone, which the transport verified with the slot: the slot must be one
// PEERS names when PEERS is set, and the cluster may hold at most one member
// above its size (a replacement standing by). A grant bound to another key
// still refuses, and the Nomad check still applies.
func (r *Replicator) authorizeJoin(ctx context.Context, req consensus.JoinRequest) (consensus.Operator, error) {
	slot, spki := req.Slot, req.SPKI
	// The engine refused a slot that has a member on the membership it
	// appended, which runs ahead of the table: the revocation a Remove
	// commits before its membership change may not be applied here yet,
	// after a restart or behind a snapshot cut. The table is read once it
	// applied a commit boundary a quorum confirmed (#70).
	if err := r.appliedToBoundary(ctx); err != nil {
		return "", fmt.Errorf("JOIN_REFUSED: the cluster state cannot be confirmed on this node: %w", err)
	}
	// A key an operator revoked (Remove, DenyPin) never joins again,
	// whatever the grant or the CA says.
	if r.table.KeyRevoked(hex.EncodeToString(spki[:])) {
		return "", fmt.Errorf("JOIN_REFUSED: the key presented for slot %s was revoked", slot)
	}
	g, ok := r.table.JoinGrant(slot)
	if ok && g.SPKI != "" && g.SPKI != hex.EncodeToString(spki[:]) {
		return "", fmt.Errorf("JOIN_REFUSED: slot %s was granted to another key", slot)
	}
	granted := ok && time.Now().Unix() <= g.Expires
	operator := consensus.Operator(g.Operator)
	switch {
	case granted:
	case r.auto != nil:
		if err := r.auto.admits(slot, r.Engine()); err != nil {
			return "", err
		}
		operator = autopilotOperator
	case !ok:
		return "", fmt.Errorf("JOIN_REFUSED: no operator granted slot %s", slot)
	default:
		return "", fmt.Errorf("JOIN_REFUSED: the grant for slot %s expired", slot)
	}
	if err := r.joinerApplies(ctx, req); err != nil {
		log.Warn("cluster: a join under slot %s refused: %v", slot, err)
		return "", err
	}
	if gate := r.nomadGate(); gate != nil {
		if err := gate.CheckJoin(ctx, slot, req.Address); err != nil {
			log.Warn("cluster: a join under slot %s refused: %v", slot, err)
			return "", err
		}
	}
	if granted && g.SPKI == "" {
		// The grant is used: it is bound to the key it admits, so a retry
		// of this node passes and no other key joins under it.
		if err := r.bindGrant(ctx, g, spki); err != nil {
			return "", fmt.Errorf("JOIN_REFUSED: the grant for slot %s could not be bound to the joiner's key: %w", slot, err)
		}
	}
	log.Warn("cluster: a node joins under slot %s, admitted by %s", slot, operator)
	return operator, nil
}

// appliedToBoundary waits until this node's table applied a commit boundary
// a quorum confirmed, within a few seconds.
func (r *Replicator) appliedToBoundary(ctx context.Context) error {
	eng := r.Engine()
	if eng == nil {
		return errors.New("the consensus node is not open")
	}
	bctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	tok, err := eng.Barrier(bctx)
	if err != nil {
		return err
	}
	return eng.WaitApplied(bctx, tok)
}

// joinerApplies refuses, in the autonomous mode, a joiner whose binary does
// not apply the payload version the cluster writes: it would restore the
// table's snapshot without what it cannot read (a revocation among them),
// then become a voter, perhaps the leader (#50). The joiner says it in its
// hello, under the certificate of its slot; a joiner that does not answer
// is refused, and retries. While the cluster writes the base version, every
// binary applies it, and nothing is asked.
func (r *Replicator) joinerApplies(ctx context.Context, req consensus.JoinRequest) error {
	v := r.table.WriteVersion()
	if r.auto == nil || r.auto.probe == nil || v <= reftable.BaseVersion {
		return nil
	}
	pctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	h, err := r.auto.probe(pctx, req.Address, req.Slot)
	if err != nil {
		return fmt.Errorf("JOIN_REFUSED: the joiner of slot %s does not say which payload version it applies: %w", req.Slot, err)
	}
	if h.Slot != req.Slot || !appliesVersion(h, v) {
		return fmt.Errorf("JOIN_REFUSED: the joiner of slot %s runs a binary that applies payload version %d, the cluster writes %d", req.Slot, max(h.Payload, reftable.BaseVersion), v)
	}
	return nil
}

// SaveSnapshot writes the ref snapshot and its SHA-256 side by side (D17,
// R3-26). It is reachable only through the private socket.
func (r *Replicator) SaveSnapshot(ctx context.Context, op consensus.Operator, path string) (consensus.SnapshotMeta, error) {
	eng := r.Engine()
	if eng == nil {
		return consensus.SnapshotMeta{}, errors.New("the consensus node is not open")
	}
	if path == "" {
		return consensus.SnapshotMeta{}, errors.New("a path is required")
	}
	// The data, its metadata and its digest are written aside, then put in
	// place digest last: a .sha256 beside the file means the three belong
	// together. A .partial left by a crash is ours and is overwritten.
	tmp := path + ".partial"
	f, err := os.OpenFile(tmp, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o600)
	if err != nil {
		return consensus.SnapshotMeta{}, err
	}
	h := sha256.New()
	meta, err := eng.SaveSnapshot(ctx, op, io.MultiWriter(f, h))
	if err == nil {
		err = f.Sync()
	}
	if cerr := f.Close(); err == nil {
		err = cerr
	}
	if err != nil {
		os.Remove(tmp)
		return meta, err
	}
	sum := hex.EncodeToString(h.Sum(nil))
	// The digest names the file by its base name, so `sha256sum -c` works
	// wherever the pair is copied; a second line binds the metadata.
	digests := sum + "  " + filepath.Base(path) + "\n"
	if len(meta.Raw) > 0 {
		ms := sha256.Sum256(meta.Raw)
		digests += hex.EncodeToString(ms[:]) + "  " + filepath.Base(path) + ".meta\n"
	}
	if err := writeFileSynced(path+".sha256.partial", []byte(digests)); err != nil {
		return meta, err
	}
	// The engine's metadata travels with the snapshot: a restore needs it.
	if len(meta.Raw) > 0 {
		if err := writeFileSynced(path+".meta.partial", meta.Raw); err != nil {
			return meta, err
		}
	}
	if err := os.Remove(path + ".sha256"); err != nil && !os.IsNotExist(err) {
		return meta, err
	}
	if err := os.Rename(tmp, path); err != nil {
		return meta, err
	}
	if len(meta.Raw) > 0 {
		if err := os.Rename(path+".meta.partial", path+".meta"); err != nil {
			return meta, err
		}
	}
	if err := os.Rename(path+".sha256.partial", path+".sha256"); err != nil {
		return meta, err
	}
	// The renames are durable before the operator is told the snapshot is
	// saved.
	if err := syncDirFn(filepath.Dir(path)); err != nil {
		return meta, err
	}
	log.Warn("cluster: operator %s saved the ref snapshot at %d to %s (sha256 %s)", op, meta.Position.Index, path, sum)
	return meta, nil
}

// OpenSavedSnapshot opens a snapshot `snapshot save` wrote, for a restore.
// sum is the digest of the file as `snapshot save` logged it and wrote on
// the first line of path.sha256: the file is read whole and refused when
// it differs, then rewound. The metadata beside it is checked against the
// line path.sha256 holds for it, when it holds one (snapshots saved before
// it was written have none). The engine checks the decoded stream against
// the metadata itself.
func OpenSavedSnapshot(path string, sum [32]byte) (*os.File, []byte, error) {
	meta, err := os.ReadFile(path + ".meta")
	if err != nil {
		return nil, nil, fmt.Errorf("the snapshot's metadata, written beside it by `snapshot save`: %w", err)
	}
	if lines, err := os.ReadFile(path + ".sha256"); err == nil {
		for line := range strings.SplitSeq(string(lines), "\n") {
			want, name, ok := strings.Cut(line, "  ")
			if !ok || name != filepath.Base(path)+".meta" {
				continue
			}
			if got := sha256.Sum256(meta); hex.EncodeToString(got[:]) != want {
				return nil, nil, fmt.Errorf("%s.meta does not match its digest in %s.sha256", path, path)
			}
		}
	} else if !os.IsNotExist(err) {
		return nil, nil, err
	}
	f, err := os.Open(path)
	if err != nil {
		return nil, nil, err
	}
	h := sha256.New()
	if _, err := io.Copy(h, f); err != nil {
		f.Close()
		return nil, nil, err
	}
	if !bytes.Equal(h.Sum(nil), sum[:]) {
		f.Close()
		return nil, nil, fmt.Errorf("%s does not match the digest given: it is not the file `snapshot save` wrote", path)
	}
	if _, err := f.Seek(0, io.SeekStart); err != nil {
		f.Close()
		return nil, nil, err
	}
	return f, meta, nil
}

// writeFileSynced writes b to path and syncs it before closing.
func writeFileSynced(path string, b []byte) error {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o600)
	if err != nil {
		return err
	}
	_, err = f.Write(b)
	if err == nil {
		err = f.Sync()
	}
	if cerr := f.Close(); err == nil {
		err = cerr
	}
	return err
}

// The refusals of the operator management, which the administration
// translates.
var (
	ErrNotSiteAdmin = errors.New("the account is not a site administrator")
	ErrLastOperator = errors.New("the last cluster operator cannot be withdrawn here; use `forgejo cluster operator remove` on a node")
)

// changeOperator grants or withdraws the cluster-operator permission. It is
// held by site administrators only, and the last one cannot be withdrawn
// from the administration: `forgejo cluster operator` on a node is the way
// back in.
func changeOperator(ctx context.Context, op consensus.Operator, o Operation) error {
	u, err := user_model.GetUserByName(ctx, o.Target)
	if err != nil {
		return err
	}
	by, _ := strconv.ParseInt(strings.TrimPrefix(string(op), "forgejo-user:"), 10, 64)
	if o.Action == ActionGrantOperator {
		if !u.IsAdmin {
			return ErrNotSiteAdmin // the login stays out of the replicated audit
		}
		return cluster_model.AddOperator(ctx, u.ID, by, StepUpSubjectOf(ctx, u.ID))
	}
	if err := cluster_model.RemoveOperatorUnlessLast(ctx, u.ID); errors.Is(err, cluster_model.ErrLastOperator) {
		return ErrLastOperator
	} else if err != nil {
		return err
	}
	return nil
}

// StepUpSubjectOf is the subject an account is linked as at [cluster]
// STEP_UP_OIDC_SOURCE, pinned on its grant; empty without either. A grant
// is itself a stepped-up operation, so the link it reads is trusted then.
func StepUpSubjectOf(ctx context.Context, userID int64) string {
	if setting.Cluster.StepUpOIDCSource == "" {
		return ""
	}
	src, err := auth_model.GetActiveOAuth2SourceByName(ctx, setting.Cluster.StepUpOIDCSource)
	if err != nil {
		return ""
	}
	ext := &user_model.ExternalLoginUser{UserID: userID, LoginSourceID: src.ID}
	if has, err := user_model.GetExternalLogin(ctx, ext); err != nil || !has {
		return ""
	}
	return ext.ExternalID
}

// IsOperator reports whether an account may change the cluster: a site
// administrator holding the cluster-operator permission.
func IsOperator(ctx context.Context, u *user_model.User) bool {
	if u == nil || !u.IsAdmin {
		return false
	}
	ok, err := cluster_model.IsOperator(ctx, u.ID)
	return err == nil && ok
}

// SeedOperators gives the permission to the [cluster] OPERATORS accounts
// when nobody holds it yet: the bootstrap of a new forge. Afterwards the
// database is the authority and the setting is ignored.
func SeedOperators(ctx context.Context) error {
	if !setting.Cluster.Enabled || len(setting.Cluster.Operators) == 0 {
		return nil
	}
	n, err := cluster_model.CountOperators(ctx)
	if err != nil || n > 0 {
		return err
	}
	for name := range setting.Cluster.Operators {
		u, err := user_model.GetUserByName(ctx, name)
		if err != nil || !u.IsAdmin {
			log.Warn("cluster: [cluster] OPERATORS names %q, which is not a site administrator; skipped", name)
			continue
		}
		if err := cluster_model.AddOperator(ctx, u.ID, 0, StepUpSubjectOf(ctx, u.ID)); err != nil {
			return err
		}
		log.Warn("cluster: seeded the cluster-operator permission for %s from [cluster] OPERATORS", u.Name)
	}
	return nil
}
