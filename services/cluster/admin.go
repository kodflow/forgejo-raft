// Copyright 2026 The Forgejo Authors. All rights reserved.
// SPDX-License-Identifier: GPL-3.0-or-later

package cluster

import (
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
	Lag             []RepoLag
	Audit           []reftable.AuditRecord
	Started         time.Time
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
	if err != nil {
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
		return r.RaisePayloadVersion(ctx, op)
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
	case ActionRemove:
		return eng.Remove(ctx, op, id)
	case ActionTransfer:
		return eng.TransferLeadership(ctx, op, id)
	case ActionDenyPin:
		return eng.DenyPin(ctx, op, id)
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
	if gate := r.nomadGate(); gate != nil {
		if err := gate.CheckJoin(ctx, slot, req.Address); err != nil {
			log.Warn("cluster: a join under slot %s refused: %v", slot, err)
			return "", err
		}
	}
	log.Warn("cluster: a node joins under slot %s, admitted by %s", slot, operator)
	return operator, nil
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
	// wherever the pair is copied.
	if err := os.WriteFile(path+".sha256.partial", []byte(sum+"  "+filepath.Base(path)+"\n"), 0o600); err != nil {
		return meta, err
	}
	// The engine's metadata travels with the snapshot: a restore needs it.
	if len(meta.Raw) > 0 {
		if err := os.WriteFile(path+".meta.partial", meta.Raw, 0o600); err != nil {
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
	log.Warn("cluster: operator %s saved the ref snapshot at %d to %s (sha256 %s)", op, meta.Position.Index, path, sum)
	return meta, nil
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
