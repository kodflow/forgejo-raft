// Copyright 2026 The Forgejo Authors. All rights reserved.
// SPDX-License-Identifier: GPL-3.0-or-later

// Package consensus is the boundary between Forgejo's replicator and the
// consensus engine (kitsunium ADR 0152, D14). It mirrors the part of the
// engine's public surface the replicator uses, so the replicator, its state
// machine and its tests do not depend on one engine implementation. A driver
// binds it to a concrete engine: the SDK's `pkg/v1/app/consensus` in
// production, an in-process log in tests.
package consensus

import (
	"context"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"strconv"
	"time"
)

// NodeID identifies a member. It is never 0 and never reused (D8).
type NodeID uint64

func (id NodeID) String() string { return fmt.Sprintf("%016x", uint64(id)) }

// Incarnation is drawn when a data directory is first stamped. It is an
// identifier, not a secret.
type Incarnation [16]byte

func (i Incarnation) String() string { return hex.EncodeToString(i[:]) }

// ClusterID names a cluster. It is 16 bytes everywhere it is encoded.
type ClusterID [16]byte

func (c ClusterID) String() string { return hex.EncodeToString(c[:]) }

// IsZero reports whether the id was never set.
func (c ClusterID) IsZero() bool { return c == ClusterID{} }

// ParseClusterID reads the lowercase hex form.
func ParseClusterID(s string) (ClusterID, error) {
	var c ClusterID
	b, err := hex.DecodeString(s)
	if err != nil || len(b) != len(c) {
		return c, fmt.Errorf("cluster id must be 32 hex characters, got %q", s)
	}
	copy(c[:], b)
	return c, nil
}

// Position is an entry's place in the log. Index is a fencing token: unique
// and strictly increasing over the group's life (D10).
type Position struct {
	Term  uint64
	Index uint64
}

// LogIdentity scopes positions: a Recover or a restore starts a new lineage
// (D10, R3-22).
type LogIdentity struct {
	Cluster ClusterID
	Epoch   uint64
}

func (l LogIdentity) String() string { return fmt.Sprintf("%s/%d", l.Cluster, l.Epoch) }

// Token is the lineage-scoped identity of a committed command (D14, R7-09).
type Token struct {
	Cluster ClusterID
	Epoch   uint64
	Term    uint64
	Index   uint64
}

// TokenLen is the encoded size of a Token: version, cluster, epoch, term, index.
const TokenLen = 1 + 16 + 8 + 8 + 8

// Identity returns the lineage the token belongs to.
func (t Token) Identity() LogIdentity { return LogIdentity{Cluster: t.Cluster, Epoch: t.Epoch} }

// IsZero reports whether the token was never set.
func (t Token) IsZero() bool { return t == Token{} }

// AppendToken encodes t in the fixed 41-byte layout of D14.
func AppendToken(dst []byte, t Token) []byte {
	dst = append(dst, 1)
	dst = append(dst, t.Cluster[:]...)
	dst = binary.BigEndian.AppendUint64(dst, t.Epoch)
	dst = binary.BigEndian.AppendUint64(dst, t.Term)
	return binary.BigEndian.AppendUint64(dst, t.Index)
}

// ParseToken decodes the layout AppendToken writes; any other length or
// version is refused.
func ParseToken(b []byte) (Token, error) {
	var t Token
	if len(b) != TokenLen || b[0] != 1 {
		return t, ErrLineageMismatch
	}
	copy(t.Cluster[:], b[1:17])
	t.Epoch = binary.BigEndian.Uint64(b[17:25])
	t.Term = binary.BigEndian.Uint64(b[25:33])
	t.Index = binary.BigEndian.Uint64(b[33:41])
	return t, nil
}

// String is the hex of the encoding, the form used in cookies and URLs.
func (t Token) String() string { return hex.EncodeToString(AppendToken(nil, t)) }

// ParseTokenString reads the form String writes.
func ParseTokenString(s string) (Token, error) {
	b, err := hex.DecodeString(s)
	if err != nil {
		return Token{}, ErrLineageMismatch
	}
	return ParseToken(b)
}

// EntryKind distinguishes the caller's commands from the engine's entries.
// Apply only ever receives Command entries.
type EntryKind uint8

const EntryCommand EntryKind = 1

// Entry is a committed command handed to Apply. Data is immutable.
type Entry struct {
	Position
	Kind    EntryKind
	Version uint16
	Data    []byte
	// Token is set in memory by the engine, never encoded in the log (R8-36).
	Token Token
}

// Operator is the opaque identity the product vouches for (D17): printable
// ASCII 0x21-0x7E, at most 128 bytes, no personal data.
type Operator string

// Validate applies the D14 rules to an operator identity.
func (o Operator) Validate() error {
	if len(o) == 0 || len(o) > 128 {
		return errors.New("operator must be 1 to 128 bytes")
	}
	for i := 0; i < len(o); i++ {
		if o[i] < 0x21 || o[i] > 0x7e {
			return errors.New("operator must be printable ASCII without spaces")
		}
	}
	return nil
}

// Member is one node of the membership.
type Member struct {
	ID          NodeID
	Incarnation Incarnation
	Slot        string
	SPKI        [32]byte
	Address     string
	Voter       bool
	Joining     bool
}

// Membership is the configuration at a position.
type Membership struct {
	At      Position
	Members []Member
	Retired []NodeID
}

// Voters counts the voting members.
func (m Membership) Voters() int {
	n := 0
	for _, mb := range m.Members {
		if mb.Voter {
			n++
		}
	}
	return n
}

// Role is a node's role in its current term.
type Role uint8

const (
	RoleFollower Role = iota
	RoleCandidate
	RoleLeader
	RoleLearner
)

func (r Role) String() string {
	switch r {
	case RoleLeader:
		return "leader"
	case RoleCandidate:
		return "candidate"
	case RoleLearner:
		return "learner"
	default:
		return "follower"
	}
}

// Status is a node's view of itself.
type Status struct {
	ID       NodeID
	Slot     string
	Role     Role
	Term     uint64
	Leader   NodeID
	Commit   uint64
	Applied  uint64
	Identity LogIdentity
	// ClusterVersion is the committed cluster version, the engine's
	// protocol every member must speak, and ClusterGeneration counts its
	// raises: the generation a RaiseClusterVersion names (D18).
	ClusterVersion    uint32
	ClusterGeneration uint64
	SPKI              [32]byte
}

// AlarmID is the index of the entry that raised an alarm (R4-06).
type AlarmID uint64

// Alarm is a replicated alarm, such as the no-space alarm of D17.
type Alarm struct {
	ID    AlarmID
	Kind  string
	Nodes []NodeID
}

// PeerHealth is the leader's view of one peer.
type PeerHealth struct {
	ID          NodeID
	Match       uint64
	Lag         uint64
	LastContact time.Duration
	Unreachable bool
	// Snapshot is set while the peer receives a snapshot.
	Snapshot bool
	Inflight int
}

// Health is the structured verdict of D13.
type Health struct {
	Ready            bool
	Role             Role
	LeaderKnown      bool
	QuorumReachable  bool
	ApplyLagOver     bool
	ApplyLagEntries  uint64
	FailureTolerance int
	Alarms           []Alarm
	CertExpiry       time.Time
	Stopped          bool
	StopReason       string
	// StopExitCode is the code the process exits with once the node
	// stopped itself, so its supervisor does not start it again in the
	// same boot: 73 for a STORAGE_FAILED whose marker could be written
	// nowhere (ADR 0152 D4, R9-05). 0 for a node closed, or running.
	StopExitCode   int
	Peers          []PeerHealth
	Identity       LogIdentity
	ContractGen    uint64
	ClusterVersion uint32
	// SnapshotBytes is the size of the newest snapshot the node holds, and
	// SnapshotLimit the size a cut is held to now: MAX_SNAPSHOT_BYTES, or
	// the smallest a member announces. SnapshotNearLimit is set past four
	// fifths of it, before the cuts are refused (R2-30).
	SnapshotBytes     int64
	SnapshotLimit     int64
	SnapshotNearLimit bool
}

// Precondition names what a clearing or raising operation acts on, scoped by
// lineage, so a stale click cannot clear an alarm raised since (R4-06, R5-09).
// Generation is the cluster-version generation a raise replaces.
type Precondition struct {
	Identity   LogIdentity
	AlarmID    AlarmID
	Generation uint64
}

// SnapshotMeta describes a snapshot.
type SnapshotMeta struct {
	ID       string
	Position Position
	Identity LogIdentity
	Size     int64
	SHA256   [32]byte
	// Raw is the engine's own encoding of the metadata, which a restore
	// needs whole; `forgejo cluster snapshot save` stores it beside the
	// snapshot.
	Raw []byte
}

// Snapshot is a point-in-time view of the state machine (D3).
type Snapshot interface {
	WriteTo(w io.Writer) (int64, error)
	Release()
}

// StateMachine is the caller's deterministic, pure transition (D3).
type StateMachine interface {
	Apply(e Entry) any
	Snapshot() (Snapshot, error)
	Restore(r io.Reader, meta SnapshotMeta) error
}

// Replicated is the consumer port of D14.
type Replicated interface {
	Propose(ctx context.Context, data []byte) (Token, any, error)
	Barrier(ctx context.Context) (Token, error)
	WaitApplied(ctx context.Context, t Token) error
	Status() Status
}

// Engine is one running node: the consumer port plus the online operations
// of D17. Every operation may be submitted on any node; the engine forwards
// it to the leader.
type Engine interface {
	Replicated
	Health() Health
	Membership() Membership
	AddLearner(ctx context.Context, op Operator, m Member) error
	Promote(ctx context.Context, op Operator, id NodeID) error
	Remove(ctx context.Context, op Operator, id NodeID) error
	UpdateAddress(ctx context.Context, op Operator, id NodeID, addr string) error
	DenyPin(ctx context.Context, op Operator, id NodeID) error
	TransferLeadership(ctx context.Context, op Operator, to NodeID) error
	ClearNoSpace(ctx context.Context, op Operator, pre Precondition) error
	// RaiseClusterVersion raises the committed cluster version to v, against
	// the lineage and the generation pre names (Status's Identity and
	// ClusterGeneration): a stale one is ErrAdminPrecondition. It is refused
	// ErrVersionUnsupported while a member of the latest configuration,
	// voter or learner, was not heard in the leader's term announcing v: a
	// member on an older binary would stop on the raise (D18). It is refused
	// so as well while such a member was last heard a vote window ago or
	// more, whatever it announced: one that went down may come back on an
	// older binary (kitsunium/sdk#665). A raise is never undone.
	RaiseClusterVersion(ctx context.Context, op Operator, pre Precondition, v uint32) error
	SaveSnapshot(ctx context.Context, op Operator, w io.Writer) (SnapshotMeta, error)
	// Watch returns its own channel of status changes, closed when ctx ends
	// or the engine closes (D13, R7-11).
	Watch(ctx context.Context) <-chan Status
	Close(ctx context.Context) error
}

// CommittedReader reads committed entries back, for an observer's export:
// [from, to], at most maxBytes of data, never past the commit index. A
// driver whose engine cannot read its log back does not implement it, and
// the export then serves checkpoints alone. ErrCompacted says from is
// behind the oldest entry kept: the reader starts again from a snapshot.
type CommittedReader interface {
	ReadCommitted(ctx context.Context, from, to uint64, maxBytes int) ([]Entry, error)
}

// TLSFiles names the PEM files of the node's identity. The certificate and
// key may be the same file, which a renewer replaces atomically (D17, R7-25).
type TLSFiles struct {
	CertFile string
	KeyFile  string
	CAFile   string
}

// MaxEntryBytes bounds one replicated command: the engine's own bound,
// which the kitsunium driver passes to the SDK as MaxEntryBytes, and past
// which the replicator refuses a lifecycle command before it proposes it.
// Both read it here, so the early refusal and the engine never disagree.
//
// It is the SDK's default, 1 MiB. Below reftable.OwnerRenameVersion, an
// owner's rename moves every repository of the owner in one entry listing
// them, some 80 bytes per repository for short names and as much again for
// its wiki: past about 6 600 repositories with wikis, it is refused, early
// and with its reason. From that version the entry names the owner alone,
// whatever it holds (#182). Raising the bound is an operating trade-off,
// not taken here: the SDK refuses a MaxEntryBytes above MaxBatchBytes, so
// every catch-up append would grow with it, and the transport writes one
// within its write deadline (WriteTimeout, 2 s), which at 4 MiB would ask
// the links between members for 2 MiB/s where 1 MiB asks 0.5 MiB/s.
const MaxEntryBytes = 1 << 20

// Config is what the replicator hands a driver to open a node.
type Config struct {
	DataDir       string
	Slot          string
	ListenAddress string
	Seeds         map[string]string // slot -> address, a seed only (D8)
	// Pins are the SPKIs of the members a joining node trusts: its JOIN
	// counts an answer only from a peer presenting one of them.
	Pins             [][32]byte
	TLS              TLSFiles
	Machine          StateMachine
	MaxLogBytes      int64
	MaxSnapshotBytes int64
	JoinAuthorizer   func(ctx context.Context, r JoinRequest) (Operator, error)
	// MinVoters is the floor a Remove may not cross, and the fewest voters a
	// founding list may have; 0 is the engine's default, three.
	MinVoters int
	// Discover, when set, is asked before each JOIN attempt for the
	// addresses to send it to and the keys an answer may come from: the
	// autonomous mode finds them at run time instead of in Seeds and Pins.
	Discover func(ctx context.Context) (addrs []string, pins [][32]byte)
}

// JoinRequest is what a member knows of a node asking to join: the slot
// and key its certificate proved, and the address it listens on.
type JoinRequest struct {
	Slot    string
	SPKI    [32]byte
	Address string
}

// Report is what Inspect says about a data directory, read-only.
type Report struct {
	Cluster     ClusterID
	Node        NodeID
	Incarnation Incarnation
	Slot        string
	Joining     bool
	// BootstrapPending is a founding Bootstrap started and did not finish
	// (a crash, a write that failed): it resumes with the same list.
	BootstrapPending bool
	HardTerm         uint64
	// LastIndex is the last index the directory holds as the node's start
	// and a Recover keep it: the log's last index, or the index of the
	// snapshot an install a crash interrupted published, past the log's end
	// or inside the log under another term, which they move the log onto,
	// the log's entries past it then dropped (kitsunium/sdk#746). It is
	// not the log's own extent, which Detail gives, nor the log's last
	// index after a Recover, which may still drop a suffix that does not
	// verify.
	LastIndex uint64
	// LastTerm is the term at LastIndex, of the log's entry there or of
	// that snapshot: with it, the key a Recover's survivor is chosen by,
	// (LastTerm, LastIndex) in that order, never HardTerm, the term of the
	// last vote.
	LastTerm    uint64
	Snapshot    *SnapshotMeta
	Quarantined bool
	// Markers names the stop markers of the directory, which only an
	// operator clears: STOP-RECOVER for a Recover interrupted before it
	// completed.
	Markers []string
	Detail  []string
}

// MarkerRecover is the stop marker of a Recover that did not complete.
const MarkerRecover = "STOP-RECOVER"

// QuarantineExit is the way out of a LOG_CORRUPT quarantine (R3-09).
type QuarantineExit uint8

const (
	WipeAndRejoin QuarantineExit = iota + 1
	TruncateAndStaySilent
)

// FencedVoter is the operator's attestation for one old voter (D17).
type FencedVoter struct {
	Voter    NodeID
	Method   string
	Receipts [][]byte
}

// RecoverOptions mirrors the SDK's options for a forced recovery.
type RecoverOptions struct {
	Confirm     string
	Operator    Operator
	Joining     []Member
	Fenced      []FencedVoter
	WitnessKeys [][]byte
}

// RestoreOptions mirrors the SDK's options for a restore from a backup.
type RestoreOptions struct {
	// Meta is the engine's encoding of the snapshot's metadata. The file
	// itself is authenticated by its caller, against the digest `snapshot
	// save` wrote; the engine checks the decoded stream against Meta.
	Meta     []byte
	Confirm  string
	Operator Operator
	MaxBytes int64
}

// Driver binds the boundary to an engine. Offline functions run on a stopped
// node under the data directory's lock.
type Driver interface {
	Name() string
	Open(ctx context.Context, cfg Config) (Engine, error)
	Init(dir string, cluster ClusterID, slot string) (NodeID, Incarnation, error)
	Bootstrap(ctx context.Context, cfg Config, members []Member) error
	Inspect(dir string) (Report, error)
	Recover(dir string, newCluster ClusterID, members []Member, opt RecoverOptions) error
	Restore(dir string, r io.Reader, newCluster ClusterID, members []Member, opt RestoreOptions) (SnapshotMeta, error)
	ClearQuarantine(dir string, exit QuarantineExit, op Operator) error
}

// SnapshotOrigin is implemented by a driver that reads, in a snapshot's
// metadata, the cluster it was cut from and that cluster's voters at the
// cut: a restore fences them before it founds a new cluster.
type SnapshotOrigin interface {
	SnapshotOrigin(meta []byte) (ClusterID, []NodeID, error)
}

var drivers = map[string]Driver{}

// Register makes a driver available by name; drivers register in init.
func Register(d Driver) { drivers[d.Name()] = d }

// Lookup returns a registered driver.
func Lookup(name string) (Driver, error) {
	d, ok := drivers[name]
	if !ok {
		return nil, fmt.Errorf("%w: no consensus driver %q is compiled into this binary", ErrMisconfigured, name)
	}
	return d, nil
}

// The errors the replicator acts on, mirroring D12's codes. A driver maps the
// engine's codes onto these with errors.Is.
var (
	ErrMisconfigured         = errors.New("CONSENSUS_MISCONFIGURED")
	ErrNotLeader             = errors.New("NOT_LEADER")
	ErrOutcomeUnknown        = errors.New("OUTCOME_UNKNOWN")
	ErrLeadershipUnconfirmed = errors.New("LEADERSHIP_UNCONFIRMED")
	ErrMembershipPending     = errors.New("MEMBERSHIP_CHANGE_PENDING")
	ErrMembershipInvalid     = errors.New("MEMBERSHIP_INVALID")
	ErrTransferFailed        = errors.New("TRANSFER_FAILED")
	ErrEntryTooLarge         = errors.New("ENTRY_TOO_LARGE")
	ErrStateMachineFailed    = errors.New("STATE_MACHINE_FAILED")
	ErrNodeStopped           = errors.New("NODE_STOPPED")
	ErrEntrySuperseded       = errors.New("ENTRY_SUPERSEDED")
	ErrOverloaded            = errors.New("OVERLOADED")
	ErrNoSpace               = errors.New("NO_SPACE")
	ErrLineageMismatch       = errors.New("LINEAGE_MISMATCH")
	ErrAdminPrecondition     = errors.New("ADMIN_PRECONDITION_FAILED")
	ErrNodeIDRetired         = errors.New("NODE_ID_RETIRED")
	ErrNotImplemented        = errors.New("not implemented by this driver")
	ErrCompacted             = errors.New("LOG_COMPACTED")
	// ErrVersionUnsupported, from a StateMachine's Restore, says the snapshot
	// is of a format this build does not read: the engine stops the node and
	// refuses a restart over that snapshot (D18), instead of having the
	// leader resend it forever. From RaiseClusterVersion, it says a member
	// does not announce the version yet, or did not answer within the vote
	// window (kitsunium/sdk#665): nothing was raised, and the raise may be
	// asked again. From opening a data directory, offline or at the node's
	// start, it says the directory holds what a newer build wrote, such as
	// a snapshot metadata of a version this one does not know: nothing in
	// it was removed (kitsunium/sdk#665). Inspect says it too of the
	// snapshot an install a crash interrupted published, when it is of a
	// version this build does not read (kitsunium/sdk#746).
	ErrVersionUnsupported = errors.New("VERSION_UNSUPPORTED")
)

// Certain reports whether err says, with certainty, that nothing was
// appended, so the caller may retry.
func Certain(err error) bool {
	return errors.Is(err, ErrNotLeader) || errors.Is(err, ErrOverloaded) ||
		errors.Is(err, ErrNoSpace) || errors.Is(err, ErrEntryTooLarge) || errors.Is(err, ErrEntrySuperseded)
}

// The identifiers print as hex in JSON, as `forgejo cluster init` prints them.

func (id NodeID) MarshalText() ([]byte, error)     { return []byte(id.String()), nil }
func (i Incarnation) MarshalText() ([]byte, error) { return []byte(i.String()), nil }
func (c ClusterID) MarshalText() ([]byte, error)   { return []byte(c.String()), nil }

func (id *NodeID) UnmarshalText(b []byte) error {
	v, err := strconv.ParseUint(string(b), 16, 64)
	if err != nil {
		return fmt.Errorf("node id %q: %w", b, err)
	}
	*id = NodeID(v)
	return nil
}

func (i *Incarnation) UnmarshalText(b []byte) error { return unhex(b, i[:]) }
func (c *ClusterID) UnmarshalText(b []byte) error   { return unhex(b, c[:]) }

func unhex(b, dst []byte) error {
	if hex.DecodedLen(len(b)) != len(dst) {
		return fmt.Errorf("want %d hex digits, got %d", 2*len(dst), len(b))
	}
	_, err := hex.Decode(dst, b)
	return err
}
