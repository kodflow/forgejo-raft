// Copyright 2026 The Forgejo Authors. All rights reserved.
// SPDX-License-Identifier: GPL-3.0-or-later

// Package kitsunium binds Forgejo's consensus boundary to the kitsunium SDK's
// engine, github.com/kitsunium/sdk/pkg/v1/app/consensus (ADR 0152). It is
// the "kitsunium" driver of [cluster] DRIVER.
package kitsunium

import (
	"context"
	"crypto/ed25519"
	"errors"
	"fmt"
	"io"
	"sort"
	"time"

	"forgejo.org/modules/log"
	"forgejo.org/services/cluster/consensus"

	sdk "github.com/kitsunium/sdk/pkg/v1/app/consensus"
)

func init() {
	consensus.Register(driver{})
}

// RuntimeDir is the boot-scoped directory of the engine's second
// STORAGE_FAILED marker (kitsunium/sdk#562); empty is the SDK's own,
// /run/kitsunium-consensus on Linux, which the shipped deployments provide.
// Only a test sets it, to a directory an unprivileged process can create.
var RuntimeDir string

// openDataDir opens a data directory with the marker directory above.
func openDataDir(dir string) (sdk.DataDir, error) {
	return sdk.NewDataDirWith(dir, sdk.DataDirOptions{RuntimeDir: RuntimeDir})
}

type driver struct{}

func (driver) Name() string { return "kitsunium" }

// Open opens the shipped data directory under its lock, the mutual-TLS TCP
// transport from the node's PEM files, and the engine over both. The data
// directory must have been prepared by `forgejo cluster init` and
// `forgejo cluster bootstrap` (or a JOIN): NewRaft never bootstraps.
func (driver) Open(ctx context.Context, cfg consensus.Config) (consensus.Engine, error) {
	d, err := openDataDir(cfg.DataDir)
	if err != nil {
		return nil, mapErr(err)
	}
	rep, err := sdk.Inspect(d)
	if err != nil {
		_ = d.Close()
		return nil, mapErr(err)
	}
	if rep.Stamp.State == 0 {
		_ = d.Close()
		return nil, fmt.Errorf("%w: %s is not initialised; run `forgejo cluster init`, then `forgejo cluster bootstrap`", consensus.ErrMisconfigured, cfg.DataDir)
	}
	if rep.Stamp.Slot != cfg.Slot {
		_ = d.Close()
		return nil, fmt.Errorf("%w: the data directory belongs to slot %q, not %q", consensus.ErrMisconfigured, rep.Stamp.Slot, cfg.Slot)
	}
	tr, err := sdk.NewTCPTransportFromPEM(cfg.TLS.CertFile, cfg.TLS.KeyFile, cfg.TLS.CAFile, &sdk.TransportConfig{ListenAddress: cfg.ListenAddress})
	if err != nil {
		_ = d.Close()
		return nil, mapErr(err)
	}
	sc := sdk.Config{
		Cluster: rep.Stamp.Log.Cluster, ID: rep.Stamp.ID, Machine: Machine(cfg.Machine),
		Durability: sdk.DurabilityVolatile, Log: d.Log(), Snapshots: d.Snapshots(), Transport: tr,
		MaxSnapshotBytes: cfg.MaxSnapshotBytes, MaxLogBytes: cfg.MaxLogBytes, SnapshotPausesApply: true,
		Meter: newEngineMeter(), Logger: engineLogger(), MinVoters: cfg.MinVoters,
	}
	if cfg.JoinAuthorizer != nil {
		sc.JoinAuthorizer = joinAuthorizer(cfg.JoinAuthorizer)
	}
	n, err := sdk.NewNode(sc)
	if err != nil {
		_ = tr.Close()
		_ = d.Close()
		return nil, mapErr(err)
	}
	e := Wrap(n, cfg.Slot)
	e.closers = []func() error{tr.Close, d.Close}
	e.refused = tr.RefusedBeforeHandshake
	if rep.Stamp.State == sdk.StampJoining {
		// The loop lives as long as the engine, not as the caller's
		// context: Close stops it before the transport closes.
		jctx, stop := context.WithCancel(context.WithoutCancel(ctx))
		done := make(chan struct{})
		go func() {
			defer close(done)
			joinLoop(jctx, tr, rep.Stamp, cfg)
		}()
		e.closers = append([]func() error{func() error { stop(); <-done; return nil }}, e.closers...)
	}
	return e, nil
}

// joinLoop sends the pre-admission JOIN of a joining node to the seeds until
// a member admits it as a learner (ADR 0152 D4, D19). Admission needs an
// operator's grant for the slot, or the autonomous mode; until then every
// member refuses, and the node keeps asking.
func joinLoop(ctx context.Context, tr *sdk.TCPTransport, stamp sdk.StampValue, cfg consensus.Config) {
	var addrs []string
	for slot, a := range cfg.Seeds {
		if slot != cfg.Slot {
			addrs = append(addrs, a)
		}
	}
	sort.Strings(addrs)
	if len(cfg.Pins) == 0 && cfg.Discover == nil {
		// The JOIN would trust whoever answers: the SDK refuses it, and so
		// does Forgejo, loudly, instead of retrying forever.
		log.Error("cluster: cannot join: [cluster] PEER_PINS is empty; set slot=spki for the members to trust, from `forgejo cluster status` on a member")
		return
	}
	for {
		pins := cfg.Pins
		if cfg.Discover != nil {
			addrs, pins = cfg.Discover(ctx)
		}
		var pos sdk.PositionValue
		err := errors.New("no member found yet")
		if len(addrs) > 0 && len(pins) > 0 {
			jctx, cancel := context.WithTimeout(ctx, 10*time.Second)
			pos, err = tr.Join(jctx, sdk.JoinConfig{Addresses: addrs, Stamp: stamp, Address: cfg.ListenAddress, Pins: pins})
			cancel()
		}
		if err == nil {
			log.Info("cluster: joined as a learner at %d", pos.Index)
			return
		}
		if why, ok := sdk.JoinRefusalReason(err); ok {
			log.Warn("cluster: join refused: %s; retrying", why)
		} else {
			log.Warn("cluster: join refused or unanswered: %v; retrying", err)
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(5 * time.Second):
		}
	}
}

type joinAuthorizer func(ctx context.Context, r consensus.JoinRequest) (consensus.Operator, error)

// JoinAuthorizer adapts Forgejo's fence decision to the SDK's port.
func JoinAuthorizer(f func(ctx context.Context, r consensus.JoinRequest) (consensus.Operator, error)) sdk.JoinAuthorizer {
	return joinAuthorizer(f)
}

func (f joinAuthorizer) Authorize(ctx context.Context, r sdk.JoinRequestValue) (sdk.Operator, error) {
	op, err := f(ctx, consensus.JoinRequest{Slot: r.Slot, SPKI: r.SPKI, Address: r.Address})
	return sdk.Operator(op), err
}

// withDir runs f on the data directory, under its lock, offline.
func withDir[T any](dir string, f func(d sdk.DataDir) (T, error)) (T, error) {
	var zero T
	d, err := openDataDir(dir)
	if err != nil {
		return zero, mapErr(err)
	}
	v, err := f(d)
	if cerr := d.Close(); err == nil && cerr != nil {
		err = cerr
	}
	return v, mapErr(err)
}

func (driver) Init(dir string, cluster consensus.ClusterID, slot string) (consensus.NodeID, consensus.Incarnation, error) {
	type pair struct {
		id  sdk.NodeID
		inc sdk.Incarnation
	}
	p, err := withDir(dir, func(d sdk.DataDir) (pair, error) {
		id, inc, err := sdk.Init(d, sdk.ClusterID(cluster), slot)
		return pair{id, inc}, err
	})
	return consensus.NodeID(p.id), consensus.Incarnation(p.inc), err
}

// Bootstrap writes the founding configuration into this founder's data
// directory. It is offline: no peer is contacted, and every founder runs it
// with the identical member list.
func (driver) Bootstrap(ctx context.Context, cfg consensus.Config, members []consensus.Member) error {
	_, err := withDir(cfg.DataDir, func(d sdk.DataDir) (struct{}, error) {
		rep, err := sdk.Inspect(d)
		if err != nil {
			return struct{}{}, err
		}
		ms := make([]sdk.MemberValue, 0, len(members))
		for _, m := range members {
			ms = append(ms, sdkMember(m))
		}
		sc := sdk.Config{
			Cluster: rep.Stamp.Log.Cluster, ID: rep.Stamp.ID, Machine: Machine(cfg.Machine),
			Durability: sdk.DurabilityVolatile, Log: d.Log(), Snapshots: d.Snapshots(),
			MaxSnapshotBytes: cfg.MaxSnapshotBytes, MaxLogBytes: cfg.MaxLogBytes, SnapshotPausesApply: true,
			MinVoters: cfg.MinVoters,
		}
		return struct{}{}, sdk.Bootstrap(ctx, &sc, ms)
	})
	return err
}

func (driver) Inspect(dir string) (consensus.Report, error) {
	return withDir(dir, func(d sdk.DataDir) (consensus.Report, error) {
		r, err := sdk.Inspect(d)
		if err != nil {
			return consensus.Report{}, err
		}
		out := consensus.Report{
			Cluster: consensus.ClusterID(r.Stamp.Log.Cluster), Node: consensus.NodeID(r.Stamp.ID),
			Incarnation: consensus.Incarnation(r.Stamp.Incarnation), Slot: r.Stamp.Slot,
			Joining: r.Stamp.State == sdk.StampJoining, BootstrapPending: r.Stamp.State == sdk.StampBootstrapPending,
			HardTerm: r.HardState.Term, LastIndex: r.Last, LastTerm: r.LastTerm,
			Quarantined: r.Quarantine != "", Markers: r.Markers,
		}
		out.Detail = append(out.Detail, fmt.Sprintf("stamp state %d, epoch %d", r.Stamp.State, r.Stamp.Log.Epoch),
			fmt.Sprintf("log %d..%d, last term %d", r.First, r.Last, r.LastTerm))
		if r.Quarantine != "" {
			out.Detail = append(out.Detail, "quarantine: "+r.Quarantine)
		}
		out.Detail = append(out.Detail, r.Markers...)
		if n := len(r.Snapshots); n > 0 {
			m := snapshotMeta(r.Snapshots[n-1])
			out.Snapshot = &m
		}
		return out, nil
	})
}

func (driver) Recover(dir string, newCluster consensus.ClusterID, members []consensus.Member, opt consensus.RecoverOptions) error {
	ms := make([]sdk.MemberValue, 0, len(members)+len(opt.Joining))
	for _, m := range members {
		ms = append(ms, sdkMember(m))
	}
	for _, m := range opt.Joining {
		m.Voter = false
		ms = append(ms, sdkMember(m))
	}
	so := sdk.RecoverOptions{Confirm: opt.Confirm, Operator: sdk.Operator(opt.Operator)}
	for _, f := range opt.Fenced {
		so.Fenced = append(so.Fenced, sdk.FencedAttestationValue{Voter: sdk.NodeID(f.Voter), Method: f.Method, Receipts: f.Receipts})
	}
	for _, k := range opt.WitnessKeys {
		if len(k) != ed25519.PublicKeySize {
			return fmt.Errorf("%w: a witness key is %d bytes, not %d", consensus.ErrMisconfigured, len(k), ed25519.PublicKeySize)
		}
		so.WitnessKeys = append(so.WitnessKeys, ed25519.PublicKey(k))
	}
	_, err := withDir(dir, func(d sdk.DataDir) (struct{}, error) {
		return struct{}{}, sdk.Recover(d, sdk.ClusterID(newCluster), ms, &so)
	})
	return err
}

func (driver) Restore(dir string, r io.Reader, newCluster consensus.ClusterID, members []consensus.Member, opt consensus.RestoreOptions) (consensus.SnapshotMeta, error) {
	var expected sdk.SnapshotMetaValue
	if err := sdk.ParseSnapshotMeta(opt.Meta, &expected); err != nil {
		return consensus.SnapshotMeta{}, fmt.Errorf("the snapshot's metadata file: %w", err)
	}
	ms := make([]sdk.MemberValue, 0, len(members))
	for _, m := range members {
		ms = append(ms, sdkMember(m))
	}
	so := sdk.RestoreOptions{Expected: expected, Confirm: opt.Confirm, Operator: sdk.Operator(opt.Operator), MaxEncoded: opt.MaxBytes, MaxDecoded: opt.MaxBytes}
	return withDir(dir, func(d sdk.DataDir) (consensus.SnapshotMeta, error) {
		m, err := sdk.RestoreSnapshot(d, r, sdk.ClusterID(newCluster), ms, &so)
		return snapshotMeta(m), err
	})
}

// SnapshotOrigin reads the cluster a snapshot was cut from and its voters
// at the cut.
func (driver) SnapshotOrigin(meta []byte) (consensus.ClusterID, []consensus.NodeID, error) {
	var m sdk.SnapshotMetaValue
	if err := sdk.ParseSnapshotMeta(meta, &m); err != nil {
		return consensus.ClusterID{}, nil, fmt.Errorf("the snapshot's metadata file: %w", err)
	}
	var voters []consensus.NodeID
	for _, id := range m.Membership.Voters() {
		voters = append(voters, consensus.NodeID(id))
	}
	return consensus.ClusterID(m.Log.Cluster), voters, nil
}

func (driver) ClearQuarantine(dir string, exit consensus.QuarantineExit, op consensus.Operator) error {
	e := sdk.WipeAndRejoin
	if exit == consensus.TruncateAndStaySilent {
		e = sdk.TruncateAndStaySilent
	}
	_, err := withDir(dir, func(d sdk.DataDir) (struct{}, error) {
		return struct{}{}, sdk.ClearQuarantine(d, e, sdk.Operator(op))
	})
	return err
}

// ---- the state machine, seen from the engine ----

// machine adapts Forgejo's state machine to the SDK's port. The table cuts
// its snapshots with Apply paused (SnapshotPausesApply).
type machine struct{ m consensus.StateMachine }

func (a machine) Apply(e sdk.EntryValue) any {
	return a.m.Apply(consensus.Entry{
		Term: e.Position.Term, Index: e.Position.Index,
		Kind:    consensus.EntryKind(e.Kind),
		Version: uint16(e.Version),
		Data:    e.Data,
		Token:   token(e.Token),
	})
}

func (a machine) Snapshot() (sdk.Snapshot, error) { return a.m.Snapshot() }

// Restore carries a snapshot of a format the table does not read to the
// engine under the SDK's VERSION_UNSUPPORTED code, which stops the node
// (#513): any other error only refuses that one install.
func (a machine) Restore(r io.Reader, meta sdk.SnapshotMetaValue) error {
	err := a.m.Restore(r, snapshotMeta(meta))
	if errors.Is(err, consensus.ErrVersionUnsupported) && !errors.Is(err, sdk.VersionUnsupported) {
		return fmt.Errorf("%w: %w", sdk.VersionUnsupported, err)
	}
	return err
}

// Machine wraps a Forgejo state machine for the SDK.
func Machine(m consensus.StateMachine) sdk.StateMachine { return machine{m} }

// ---- conversions ----

func token(t sdk.TokenValue) consensus.Token {
	return consensus.Token{Cluster: consensus.ClusterID(t.Cluster), Epoch: t.Epoch, Term: t.Term, Index: t.Index}
}

func identity(l sdk.LogIdentityValue) consensus.LogIdentity {
	return consensus.LogIdentity{Cluster: consensus.ClusterID(l.Cluster), Epoch: l.Epoch}
}

func sdkIdentity(l consensus.LogIdentity) sdk.LogIdentityValue {
	return sdk.LogIdentityValue{Cluster: sdk.ClusterID(l.Cluster), Epoch: l.Epoch}
}

func snapshotMeta(m sdk.SnapshotMetaValue) consensus.SnapshotMeta {
	return consensus.SnapshotMeta{
		ID: m.ID, Position: consensus.Position{Term: m.Position.Term, Index: m.Position.Index},
		Identity: identity(m.Log), Size: m.DecodedSize, SHA256: m.SHA256,
		Raw: sdk.AppendSnapshotMeta(nil, &m),
	}
}

func role(r sdk.Role) consensus.Role {
	switch r {
	case sdk.RoleLeader:
		return consensus.RoleLeader
	case sdk.RoleCandidate, sdk.RolePreCandidate:
		return consensus.RoleCandidate
	case sdk.RoleLearner:
		return consensus.RoleLearner
	default:
		return consensus.RoleFollower
	}
}

func member(m sdk.MemberValue) consensus.Member {
	return consensus.Member{
		ID: consensus.NodeID(m.ID), Incarnation: consensus.Incarnation(m.Incarnation), Slot: m.Slot,
		SPKI: m.SPKI, Address: m.Address, Voter: m.Voter, Joining: m.Joining,
	}
}

func sdkMember(m consensus.Member) sdk.MemberValue {
	return sdk.MemberValue{
		ID: sdk.NodeID(m.ID), Incarnation: sdk.Incarnation(m.Incarnation), Slot: m.Slot,
		SPKI: m.SPKI, Address: m.Address, Voter: m.Voter, Joining: m.Joining,
	}
}

// mapErr keeps the SDK's error as the cause and makes the boundary's
// sentinel match, so the replicator decides on errors.Is alone.
func mapErr(err error) error {
	if err == nil {
		return nil
	}
	pairs := []struct{ sdk, ours error }{
		{sdk.NotLeader, consensus.ErrNotLeader},
		{sdk.OutcomeUnknown, consensus.ErrOutcomeUnknown},
		{sdk.LeadershipUnconfirmed, consensus.ErrLeadershipUnconfirmed},
		{sdk.MembershipChangePending, consensus.ErrMembershipPending},
		{sdk.MembershipInvalid, consensus.ErrMembershipInvalid},
		{sdk.TransferFailed, consensus.ErrTransferFailed},
		{sdk.EntryTooLarge, consensus.ErrEntryTooLarge},
		{sdk.StateMachineFailed, consensus.ErrStateMachineFailed},
		{sdk.NodeStopped, consensus.ErrNodeStopped},
		{sdk.EntrySuperseded, consensus.ErrEntrySuperseded},
		{sdk.Overloaded, consensus.ErrOverloaded},
		{sdk.NoSpace, consensus.ErrNoSpace},
		{sdk.LineageMismatch, consensus.ErrLineageMismatch},
		{sdk.AdminPreconditionFailed, consensus.ErrAdminPrecondition},
		{sdk.NodeIDRetired, consensus.ErrNodeIDRetired},
		{sdk.ConsensusMisconfigured, consensus.ErrMisconfigured},
		{sdk.VersionUnsupported, consensus.ErrVersionUnsupported},
	}
	for _, p := range pairs {
		if errors.Is(err, p.sdk) {
			return fmt.Errorf("%w: %w", p.ours, err)
		}
	}
	return err
}

// ---- the engine ----

// Engine adapts an SDK node to Forgejo's boundary.
type Engine struct {
	n    *sdk.Node
	slot string
	// closers release what Open opened for the node, after it closed.
	closers []func() error
	refused func() int64
}

// RefusedBeforeHandshake counts the connections the transport refused
// before TLS, over its per-address budget; 0 without a TCP transport.
func (e *Engine) RefusedBeforeHandshake() int64 {
	if e.refused == nil {
		return 0
	}
	return e.refused()
}

// Wrap adapts a running SDK node.
func Wrap(n *sdk.Node, slot string) *Engine { return &Engine{n: n, slot: slot} }

// Node returns the SDK node.
func (e *Engine) Node() *sdk.Node { return e.n }

func (e *Engine) Propose(ctx context.Context, data []byte) (consensus.Token, any, error) {
	t, res, err := e.n.Propose(ctx, data)
	return token(t), res, mapErr(err)
}

func (e *Engine) Barrier(ctx context.Context) (consensus.Token, error) {
	t, err := e.n.Barrier(ctx)
	return token(t), mapErr(err)
}

func (e *Engine) WaitApplied(ctx context.Context, t consensus.Token) error {
	return mapErr(e.n.WaitApplied(ctx, sdk.TokenValue{Cluster: sdk.ClusterID(t.Cluster), Epoch: t.Epoch, Term: t.Term, Index: t.Index}))
}

func (e *Engine) status(st sdk.StatusValue) consensus.Status {
	out := consensus.Status{
		ID: consensus.NodeID(st.ID), Slot: e.slot, Role: role(st.Role), Term: st.Term, Leader: consensus.NodeID(st.Leader),
		Commit: st.Commit, Applied: st.Applied, Identity: identity(st.Log),
		ClusterVersion: st.Versions.Cluster, ClusterGeneration: st.Versions.ClusterGeneration,
	}
	if m, ok := e.n.Membership().Member(st.ID); ok {
		out.SPKI = m.SPKI
	}
	return out
}

func (e *Engine) Status() consensus.Status { return e.status(e.n.Status()) }

// The SDK's health flags, in its bit order (internal/service/app/consensus,
// health.go): the facade exposes the Flags fields but not their constants,
// and an untyped constant converts to the field's type at Has.
const (
	healthReady = 1 << iota
	healthHasLeader
	healthQuorumReachable
	healthStopped
	healthSnapshotNearLimit
)

const (
	peerVoter = 1 << iota
	peerUnreachable
	peerSnapshot
)

// Health maps the SDK's verdict. The failure tolerance and the per-peer
// progress are known by the leader alone: elsewhere the tolerance is -1,
// "unknown", and Peers is empty.
func (e *Engine) Health() consensus.Health {
	h := e.n.Health()
	st := h.Status
	out := consensus.Health{
		Ready: h.Flags.Has(healthReady), Role: role(st.Role), LeaderKnown: h.Flags.Has(healthHasLeader),
		QuorumReachable: h.Flags.Has(healthQuorumReachable), Stopped: h.Flags.Has(healthStopped), Identity: identity(st.Log), ContractGen: st.Timing.Generation,
		ClusterVersion: st.Versions.Cluster, FailureTolerance: h.FailureTolerance, ApplyLagEntries: h.ApplyLagEntries,
		SnapshotBytes: h.SnapshotBytes, SnapshotLimit: h.SnapshotLimit, SnapshotNearLimit: h.Flags.Has(healthSnapshotNearLimit),
	}
	if h.StopReason.Err != nil {
		out.StopReason = h.StopReason.Err.Error()
		out.StopExitCode = h.StopReason.ExitCode()
	}
	if st.NoSpace != 0 {
		out.Alarms = append(out.Alarms, consensus.Alarm{ID: consensus.AlarmID(st.NoSpace), Kind: "NO_SPACE"})
	}
	for _, p := range h.Peers {
		out.Peers = append(out.Peers, consensus.PeerHealth{
			ID: consensus.NodeID(p.ID), Match: p.Match, Lag: p.Lag, LastContact: p.LastContact,
			Unreachable: p.Flags.Has(peerUnreachable), Snapshot: p.Flags.Has(peerSnapshot), Inflight: p.Inflight,
		})
	}
	return out
}

func (e *Engine) Membership() consensus.Membership {
	m := e.n.Membership()
	out := consensus.Membership{At: consensus.Position{Index: m.Index}}
	for _, x := range m.Members {
		out.Members = append(out.Members, member(x))
	}
	for _, id := range m.Retired {
		out.Retired = append(out.Retired, consensus.NodeID(id))
	}
	return out
}

func (e *Engine) AddLearner(ctx context.Context, op consensus.Operator, m consensus.Member) error {
	return mapErr(e.n.AddLearner(ctx, sdk.Operator(op), sdkMember(m)))
}

func (e *Engine) Promote(ctx context.Context, op consensus.Operator, id consensus.NodeID) error {
	return mapErr(e.n.Promote(ctx, sdk.Operator(op), sdk.NodeID(id)))
}

func (e *Engine) Remove(ctx context.Context, op consensus.Operator, id consensus.NodeID) error {
	return mapErr(e.n.Remove(ctx, sdk.Operator(op), sdk.NodeID(id)))
}

func (e *Engine) UpdateAddress(ctx context.Context, op consensus.Operator, id consensus.NodeID, addr string) error {
	return mapErr(e.n.UpdateAddress(ctx, sdk.Operator(op), sdk.NodeID(id), addr))
}

func (e *Engine) DenyPin(ctx context.Context, op consensus.Operator, id consensus.NodeID) error {
	return mapErr(e.n.DenyPin(ctx, sdk.Operator(op), sdk.NodeID(id)))
}

func (e *Engine) TransferLeadership(ctx context.Context, op consensus.Operator, to consensus.NodeID) error {
	return mapErr(e.n.TransferLeadership(ctx, sdk.Operator(op), sdk.NodeID(to)))
}

func (e *Engine) ClearNoSpace(ctx context.Context, op consensus.Operator, pre consensus.Precondition) error {
	return mapErr(e.n.ClearNoSpace(ctx, sdk.Operator(op), sdk.PreconditionSpec{Log: sdkIdentity(pre.Identity), Alarm: sdk.AlarmID(pre.AlarmID), Generation: pre.Generation}))
}

// RaiseClusterVersion raises the engine's cluster version. The leader
// refuses VERSION_UNSUPPORTED while a member of the configuration was not
// heard announcing v in its term, and ADMIN_PRECONDITION_FAILED against
// another lineage or generation.
func (e *Engine) RaiseClusterVersion(ctx context.Context, op consensus.Operator, pre consensus.Precondition, v uint32) error {
	return mapErr(e.n.RaiseClusterVersion(ctx, sdk.Operator(op), sdk.PreconditionSpec{Log: sdkIdentity(pre.Identity), Generation: pre.Generation}, v))
}

func (e *Engine) SaveSnapshot(ctx context.Context, op consensus.Operator, w io.Writer) (consensus.SnapshotMeta, error) {
	m, err := e.n.SaveSnapshot(ctx, sdk.Operator(op), w)
	return snapshotMeta(m), mapErr(err)
}

func (e *Engine) Watch(ctx context.Context) <-chan consensus.Status {
	in := e.n.Watch(ctx)
	out := make(chan consensus.Status, 1)
	go func() {
		defer close(out)
		for st := range in {
			select {
			case out <- e.status(st):
			default:
				// Capacity one, latest value: drop the unread one.
				select {
				case <-out:
				default:
				}
				out <- e.status(st)
			}
		}
	}()
	return out
}

func (e *Engine) Close(ctx context.Context) error {
	err := mapErr(e.n.Close(ctx))
	for _, c := range e.closers {
		if cerr := c(); err == nil {
			err = cerr
		}
	}
	return err
}

var _ consensus.Engine = (*Engine)(nil)
