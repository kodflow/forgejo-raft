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
	// ProjectionWait is, on the leader, the parked entry its retries have
	// found waiting longest for its repository's projection on this node,
	// which no retry nor reconcile changes while it leads: another member
	// that projects the repository applies it once it leads (#249). Nil
	// otherwise. ProjectionYield says that the autopilot hands the
	// leadership over for it (the autonomous mode).
	ProjectionWait  *ProjectionWaitView
	ProjectionYield bool
	// BackupLag is how far the PostgreSQL backup lags the cursor, and
	// BackupHeld the bytes of the event queue it holds behind it, while it
	// holds the trim beyond its bound; 0 otherwise.
	BackupLag  uint64
	BackupHeld int64
	// PushRefusedBytes is the size of the queue past which pushes are
	// refused.
	PushRefusedBytes int64
	// CheckpointsHere is set when this node takes the checkpoints while it
	// leads ([cluster] CHECKPOINT_INTERVAL is set). On the leader, Backup
	// is the backup cursor of its lineage, when one exists (HasBackup), and
	// CheckpointsOff says that this leader takes no checkpoint while that
	// cursor holds the trim: nothing moves it until a member that takes
	// them leads (#176).
	CheckpointsHere bool
	HasBackup       bool
	Backup          uint64
	CheckpointsOff  bool
	// CheckpointTakers are the voters the leader last found to take the
	// checkpoints, and CheckpointUnknown those that did not say, by slot;
	// CheckpointsAsked is when it asked, zero when it did not lately.
	// CheckpointTakersHeld are the takers it would not hand the leadership
	// to then, each as "slot: why".
	CheckpointTakers     []string
	CheckpointUnknown    []string
	CheckpointsAsked     time.Time
	CheckpointTakersHeld []string
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

// ProjectionWaitView is a parked entry waiting for its repository's
// projection on the leader, and since when its retries found it so.
type ProjectionWaitView struct {
	Lineage string
	Index   int64
	Repo    string
	Since   time.Time
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
	v.BackupHeld, v.PushRefusedBytes = ch.BackupHeld, ch.PushRefusedBytes
	v.CheckpointsHere, v.HasBackup, v.Backup, v.CheckpointsOff = ch.CheckpointsHere, ch.HasBackup, ch.Backup, ch.CheckpointsOff
	v.ProjectionWait, v.ProjectionYield = ch.ProjectionWait, ch.ProjectionYield
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
		v.BackupLag, v.BackupHeld = r.consumer.BackupLag(), r.consumer.BackupHeld()
	}
	v.PushRefusedBytes = pushRefusedBytes
	v.CheckpointsHere = r.checkpoints.Load() != nil
	if eng := r.Engine(); eng != nil && r.consumer != nil {
		if st := eng.Status(); st.Role == consensus.RoleLeader {
			v.Backup, v.HasBackup = r.consumer.backup(st.Identity.String())
			v.CheckpointsOff = v.HasBackup && !v.CheckpointsHere
			if ck, ok := r.ckState.fresh(r.clock()); ok {
				v.CheckpointTakers, v.CheckpointUnknown, v.CheckpointsAsked = ck.On, ck.Unknown, ck.At
				v.CheckpointTakersHeld = ck.Held
			}
			if ws := r.consumer.waitingProjection(); len(ws) > 0 {
				w := ws[0]
				v.ProjectionWait = &ProjectionWaitView{Lineage: w.Lineage, Index: w.Index, Repo: w.Repo, Since: w.Since}
				v.ProjectionYield = r.auto != nil
			}
		}
	}
	if r.table != nil {
		v.EventBytes, v.EventQueueMax = r.table.EventBytes(), reftable.MaxEventBytes
		v.EventQueueAlarm = v.EventBytes >= int64(float64(reftable.MaxEventBytes)*EventQueueAlarmShare)
	}
	return v
}

// CheckpointsUntaken reports whether the trim the backup holds waits for a
// leader that takes the checkpoints, rather than for a checkpoint of this
// one that closes: the health check and the status page say the way out
// from it.
func (v View) CheckpointsUntaken() bool {
	return v.CheckpointsOff || v.HasBackup && !v.CheckpointsHere
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
// any node: the engine forwards it to the leader (D17, Q8). A Remove or a
// DenyPin first waits for this node to apply a commit boundary a quorum
// confirmed, and is refused MEMBERSHIP_INVALID when the node does not list
// its target then: the key it revokes is read here (#337). The request and
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
		if k := hex.EncodeToString(m.SPKI[:]); r.table.KeyRevoked(k) || r.revocationAsked(k) {
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
		// The membership this node reads is the configuration it last
		// appended. On a node behind the log (after a restart or a snapshot
		// install, the operator's confirmation routed there by a load
		// balancer), it may not list a member added since, which the
		// operator saw on another node's page: the engine forwarded the
		// change to the leader, which made it, while no key was read here
		// to ask the revocation of, and the removed key joined again
		// (#337). Once this node applied a commit boundary a quorum
		// confirmed, it lists every member whose addition committed before
		// the request, as authorizeJoin reads the table then (#70); a
		// target it still does not list is refused (revokeMember).
		if err := r.appliedToBoundary(ctx); err != nil {
			return fmt.Errorf("the membership cannot be confirmed on this node, nothing is changed: %w", err)
		}
		change := eng.DenyPin
		if o.Action == ActionRemove {
			if err := r.removable(eng, id); err != nil {
				return err
			}
			change = eng.Remove
		} else if err := deniable(eng, id); err != nil {
			return err
		}
		k, asked, request, err := r.revokeMember(ctx, eng, op, id)
		if err != nil {
			return err
		}
		if err := change(ctx, op, id); err != nil {
			if asked {
				// Only the request is recorded: the member keeps its key,
				// its object channel and its place (#64, #157, #158).
				r.settleRefusedRevocation(ctx, eng, o.Action, id, k, request, err)
			}
			return err
		}
		if !asked {
			return nil
		}
		if r.table.WriteVersion() < reftable.RevokeVersion {
			log.Warn("cluster: the key of %s (slot %s) is not revoked yet: the cluster writes payload version %d, the revocation waits for %d", id, k.Slot, r.table.WriteVersion(), reftable.RevokeVersion)
			return ErrRevocationPending
		}
		// The change committed: no member holds the key any more, and the
		// request already refuses it at the JOIN. A failure here leaves it
		// requested, for the next flush to write.
		if err := r.flushRevocations(ctx); err != nil {
			return fmt.Errorf("the %s of %s is made, its key is refused at the JOIN, but the revocation is not written yet: %w", o.Action, id, err)
		}
		return nil
	case ActionTransfer:
		return eng.TransferLeadership(ctx, op, id)
	case ActionUpdateAddress:
		if _, _, err := net.SplitHostPort(o.Address); err != nil {
			return fmt.Errorf("address: %w", err)
		}
		if err := movable(eng, id); err != nil {
			return err
		}
		err := eng.UpdateAddress(ctx, op, id, o.Address)
		if errors.Is(err, consensus.ErrMembershipInvalid) {
			// The engine's refusal names no cause: say which ones it has.
			return fmt.Errorf("%w: the leader refused to move %s: either it is not a member, or a majority of the voters does not answer, or a voter that does not answer or lags behind the membership may know %s only at its current address (kitsunium/sdk#761); bring that voter back, then move %s again", err, id, id, id)
		}
		return err
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

// revokeMember asks for the revocation of the key of member id before an
// operator removes it or denies its pin, and reports whether it did: a
// removed node presenting that key, under any id, is refused at the JOIN.
// The autopilot's removal of a dead member comes here too, from
// reftable.WithdrawVersion (removeDead): that member returns under a new id
// with a new key, never with this one (#209).
//
// Only the request is written here, as a pending revocation (a version 1
// audit record every binary applies alike): the JOIN refuses its key from
// the moment it commits, the object channel does not, and the revocation
// itself is written by flushRevocations once no member holds the key. The
// engine decides the change on the leader, with what it alone knows, and
// may refuse it after the request committed: a revocation written before
// would leave the member in place with its key revoked, refused on every
// peer's object channel, and nothing undoes a revocation (#64, #157,
// #158). Written after the change alone, the removed node could join
// again, on the CA in the autonomous mode, before it lands, or for good
// if this node stopped in between: the request committed first closes
// that window.
//
// From reftable.WithdrawVersion the change records its own request, even
// when another one is open for the key, and request is the index the table
// numbered it by: a change the engine refuses for certain withdraws it,
// and only it (settleRefusedRevocation). Below, an open request is reused
// and request is 0: it stays, as before (#171).
//
// An id the membership does not list is refused MEMBERSHIP_INVALID, and
// the caller makes no change: no key is read to ask the revocation of, and
// the engine would forward the change to a leader that may list the
// member, remove it or deny its pin, and leave its key usable (#337). The
// operator's Remove and DenyPin read the membership once the node applied
// a confirmed commit boundary; the autopilot reads its own, the leader's.
// A member without a pin, its pin denied already, holds no key: nothing is
// asked, and the change is made.
func (r *Replicator) revokeMember(ctx context.Context, eng consensus.Engine, op consensus.Operator, id consensus.NodeID) (k reftable.RevokeKey, asked bool, request uint64, err error) {
	ms := eng.Membership()
	if Rank(ms, id) == 0 {
		if slices.Contains(ms.Retired, id) {
			return k, false, 0, fmt.Errorf("%w: %s is retired, removed already: nothing is changed", consensus.ErrMembershipInvalid, id)
		}
		return k, false, 0, fmt.Errorf("%w: %s is not a member on this node, so its key cannot be read to revoke it: nothing is changed", consensus.ErrMembershipInvalid, id)
	}
	for _, m := range ms.Members {
		if m.ID != id || m.SPKI == ([32]byte{}) {
			continue
		}
		k = reftable.RevokeKey{SPKI: hex.EncodeToString(m.SPKI[:]), Slot: m.Slot, Operator: string(op), Time: time.Now().Unix()}
		if r.table.KeyRevoked(k.SPKI) {
			return k, false, 0, nil
		}
		numbered := r.table.WriteVersion() >= reftable.WithdrawVersion
		if !numbered && r.revocationAsked(k.SPKI) {
			return k, true, 0, nil
		}
		index, err := r.recordPendingRevocation(ctx, k)
		if err != nil {
			return k, false, 0, fmt.Errorf("recording the revocation of %s: %w", id, err)
		}
		// The table numbers a request by its commit only once it applied
		// the raise; one committed before is numbered 0 and stays.
		if !slices.Contains(r.table.RevocationRequests(k.SPKI), index) {
			index = 0
		}
		return k, true, index, nil
	}
	return reftable.RevokeKey{}, false, 0, nil
}

// changeRefused reports whether the engine refused a membership change
// for certain, before appending it: on this node (consensus.Certain), or
// in the leader's admission (MEMBERSHIP_INVALID, MEMBERSHIP_CHANGE_PENDING).
// Any other answer, an unknown outcome or a context that ended among them,
// may have let the change commit.
func changeRefused(err error) bool {
	return consensus.Certain(err) || errors.Is(err, consensus.ErrMembershipInvalid) || errors.Is(err, consensus.ErrMembershipPending)
}

// settleRefusedRevocation decides what becomes of the revocation request a
// Remove or DenyPin recorded (revokeMember) once the engine refused the
// change. A change refused for certain, of a member that still holds the
// key, leaves no effect: its request is withdrawn, by the index the table
// numbered it, and the key is then only what the member's next departure
// makes of it (#171). Any other refusal
// keeps the request, which is written once no member holds the key: the
// change may have committed (#64, #165). A request numbered 0, committed
// before reftable.WithdrawVersion, stays too. The withdrawal is proposed
// past the end of the operator's request, which may be what ended.
func (r *Replicator) settleRefusedRevocation(ctx context.Context, eng consensus.Engine, action Action, id consensus.NodeID, k reftable.RevokeKey, request uint64, refusal error) {
	keep := func(why string) {
		log.Warn("cluster: the %s of %s was refused: %v; the revocation of its key stays requested (%s), and is written once no member holds the key", action, id, refusal, why)
	}
	switch {
	case !changeRefused(refusal):
		keep("the change may have been made")
		return
	case request == 0:
		keep("the request was not recorded by this change, or before payload version " + strconv.Itoa(reftable.WithdrawVersion))
		return
	case !holdsKey(eng.Membership(), k.SPKI):
		// The member left meanwhile, by another path: the request the
		// operator made stays, so its key does not join again.
		keep("the member left meanwhile")
		return
	}
	wctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), commandBudget)
	defer cancel()
	if err := r.withdrawRevocation(wctx, k, request); err != nil {
		keep("withdrawing it failed: " + err.Error())
		return
	}
	log.Warn("cluster: the %s of %s was refused: %v; the revocation request of its key is withdrawn", action, id, refusal)
}

// holdsKey reports whether a member of m holds the key spki, 64 hex digits.
func holdsKey(m consensus.Membership, spki string) bool {
	for _, x := range m.Members {
		if hex.EncodeToString(x.SPKI[:]) == spki {
			return true
		}
	}
	return false
}

// withdrawRevocation withdraws the revocation request of k numbered
// request, from reftable.WithdrawVersion.
func (r *Replicator) withdrawRevocation(ctx context.Context, k reftable.RevokeKey, request uint64) error {
	v := r.table.WriteVersion()
	if v < reftable.WithdrawVersion {
		return fmt.Errorf("the cluster writes payload version %d, a withdrawal needs %d", v, reftable.WithdrawVersion)
	}
	data, err := reftable.EncodeVersion(v, reftable.KindWithdrawRevocation, &reftable.WithdrawRevocation{
		SPKI: k.SPKI, Index: request, Operator: k.Operator, Time: time.Now().Unix(),
	})
	if err != nil {
		return err
	}
	open := func() bool { return slices.Contains(r.table.RevocationRequests(k.SPKI), request) }
	_, result, err := r.proposeCommand(ctx, data, func() bool { return !open() })
	if err != nil {
		return err
	}
	if !result.OK {
		return errors.New(result.Reason)
	}
	if r.table.KeyRevoked(k.SPKI) {
		// A flush wrote it between the refusal and the withdrawal.
		return errors.New("the revocation was written before its request was withdrawn")
	}
	return nil
}

// revocationAsked reports whether an operator asked for the revocation of
// spki, 64 hex digits, and it is not written yet.
func (r *Replicator) revocationAsked(spki string) bool {
	for _, k := range r.table.PendingRevocations() {
		if k.SPKI == spki {
			return true
		}
	}
	return false
}

// deniable refuses up front a DenyPin the engine always refuses: the
// leader's own pin. Nothing else needs checking here: the revocation is
// written only once the change committed.
func deniable(eng consensus.Engine, id consensus.NodeID) error {
	if st := eng.Status(); st.Leader == id || (st.Role == consensus.RoleLeader && st.ID == id) {
		return fmt.Errorf("%w: %s leads the cluster: transfer the leadership to another voter first", consensus.ErrMembershipInvalid, id)
	}
	return nil
}

// removable refuses up front a removal the engine would refuse: the leader
// itself (the engine never removes its leader; the leadership is
// transferred first), a voter below the floor of voters, and on the leader
// a voter the removal may strand. It answers the operator before anything
// is written; the engine still decides, on the leader, and its refusal
// leaves the member its key (revokeMember), so a check made away from the
// leader or with an older view is never what keeps a key usable (#64,
// #157).
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

// movable refuses up front a move of a voter's address the engine's leader
// refuses MEMBERSHIP_INVALID as one that may strand a voter
// (kitsunium/sdk#761): a voter that knows target only at its current
// address sends its PreVote there, and once target serves elsewhere nothing
// answers it. It names that voter, which the engine's refusal does not. The
// leader's own move is never refused on this ground: it dials every voter
// that lists it. As for removable, the engine still decides, on the leader.
func movable(eng consensus.Engine, id consensus.NodeID) error {
	st := eng.Status()
	if st.Leader == id || (st.Role == consensus.RoleLeader && st.ID == id) {
		return nil
	}
	m := eng.Membership()
	if i := Rank(m, id) - 1; i < 0 || !m.Members[i].Voter {
		return nil
	}
	if v, ok := strandedBy(m, eng.Health(), st.ID, id); ok {
		return fmt.Errorf("%w: %s does not answer or lags behind the membership: moving %s, which answers, may leave it no member it knows at its address to vouch for the leader once it is back; bring %s back first", consensus.ErrMembershipInvalid, v, id, v)
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
// yet, as a version 1 audit record every binary applies, and returns the
// index it committed at.
func (r *Replicator) recordPendingRevocation(ctx context.Context, k reftable.RevokeKey) (uint64, error) {
	id := clusterhook.NewNonce()
	data, err := reftable.Encode(reftable.KindAudit, &reftable.Audit{
		ID: id, Operator: k.Operator, Action: reftable.AuditRevokeKey, Target: k.Slot + ":" + k.SPKI,
		Outcome: reftable.AuditPending, Time: k.Time,
	})
	if err != nil {
		return 0, err
	}
	tok, result, err := r.proposeCommand(ctx, data, func() bool { return r.table.HasAudit(id) })
	if err != nil {
		return 0, err
	}
	if !result.OK {
		return 0, errors.New(result.Reason)
	}
	// A proposal forwarded from a follower returns once the leader applied
	// it: the index is read from this node's table once it applied it too.
	// Unknown, it is 0, and the request is never withdrawn.
	if eng := r.Engine(); eng != nil && !tok.IsZero() {
		if err := eng.WaitApplied(ctx, tok); err != nil {
			return 0, nil
		}
	}
	index, _ := r.table.AuditIndex(id)
	return index, nil
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
// calls it at each pass, an operator's Remove or DenyPin once its change
// committed, and a raise of the payload version right after; the last two
// return its error to the operator.
//
// A key a member holds is never written: that member would be cut off its
// peers' object channels while it keeps its place (#64, #157, #158). The
// membership the node reads is the one it last appended, which a change of
// leader may still drop; a barrier commits what was appended before it,
// so the membership is read once it is at or below the barrier.
func (r *Replicator) flushRevocations(ctx context.Context) error {
	if r.table.WriteVersion() < reftable.RevokeVersion || len(r.table.PendingRevocations()) == 0 {
		return nil
	}
	eng := r.Engine()
	if eng == nil {
		return errors.New("the consensus node is not open")
	}
	if _, ok := ctx.Deadline(); !ok {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, commandBudget)
		defer cancel()
	}
	tok, err := r.confirm(ctx)
	if err != nil {
		return fmt.Errorf("confirming the membership before writing a revocation: %w", err)
	}
	m := eng.Membership()
	if m.At.Index > tok.Index {
		// A change appended since the barrier may not commit: the next
		// flush reads the membership again.
		return fmt.Errorf("the membership changed while the revocations were checked; retried")
	}
	held := map[string]string{}
	for _, x := range m.Members {
		held[hex.EncodeToString(x.SPKI[:])] = x.Slot
	}
	for _, k := range r.table.PendingRevocations() {
		if slot, ok := held[k.SPKI]; ok {
			// Its removal or the denial of its pin was refused, or is not
			// made yet: the request waits for the member to leave.
			log.Debug("cluster: the pending revocation of slot %s is not written: member %s still holds the key", k.Slot, slot)
			continue
		}
		if err := r.writeRevocation(ctx, k); err != nil {
			log.Warn("cluster: writing the pending revocation of slot %s: %v", k.Slot, err)
			return err
		}
		log.Warn("cluster: the key of slot %s is revoked", k.Slot)
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
	// whatever the grant or the CA says, from the moment the revocation is
	// asked: it is written only once its member left (revokeMember).
	if k := hex.EncodeToString(spki[:]); r.table.KeyRevoked(k) || r.revocationAsked(k) {
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
