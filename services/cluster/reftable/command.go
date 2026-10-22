// Copyright 2026 The Forgejo Authors. All rights reserved.
// SPDX-License-Identifier: GPL-3.0-or-later

package reftable

import (
	"encoding/json" //nolint:depguard // the state machine decodes with the standard library alone: Apply's verdict must not depend on a third-party codec's version
	"errors"
	"fmt"

	"forgejo.org/services/cluster/consensus"
)

// PayloadVersion is the highest command version this code applies; an
// entry above it stops the node, never skipped (ADR 0152 D18).
//
// Version 2 adds one rule to Apply: a RefTxn marked Push is refused on a
// key the table deleted, or renamed away with no live ref since. Version 1
// entries keep applying exactly as before, whatever binary replays them.
// This code writes version 1 until the cluster agreed to version 2
// (Table.WriteVersion): the agreement is a version 1 Audit entry, which a
// member running an older binary applies as a plain audit record, so a
// rolling upgrade never hands an older member an entry it cannot read
// before every member runs this code.
//
// Version 3 adds one kind, KindRevokeKey, written at version 3 only: an
// older binary stops on any entry above the version it applies, so a key
// revocation is never written before every member applies it. Until then
// an operator's revocation waits as a version 1 audit record (AuditRevokeKey),
// which an older member keeps as a plain record, and is written once the
// cluster raised its version.
//
// Version 4 carries the pusher's name in a ref transaction (PusherName),
// copied into its events: the log consumer tells a pusher ID handed out
// again after a database loss by the name, not by comparing two nodes'
// clocks. Below version 4 Apply ignores the field, as an older binary
// does, so a log written before the upgrade replays to the same table.
//
// Version 5 tells a repository from the key it lives under (GenerationVersion):
// a ref transaction into a deleted or renamed-away key is refused unless it
// creates the repository anew, and one carrying the id of a repository that
// left the key is refused even once the key was taken again (#77, #91); a
// transaction of another object format than the key's live refs is
// refused; a lifecycle command carries a nonce its proposer finds again
// (#74); an owner's rename moves every live key under the owner or none
// (#56); a repository name may start with a dot (#54); a join grant bound
// to a key is not bound again to another one (#70). Every kind may be
// written at version 5, and the table's snapshot is then written in format
// 2, which an older binary refuses (#50).
const PayloadVersion = 5

// RevokeVersion is the payload version that carries KindRevokeKey.
const RevokeVersion = 3

// PusherVersion is the payload version from which a ref transaction's
// PusherName is applied.
const PusherVersion = 4

// GenerationVersion is the payload version of the rules listed above
// PayloadVersion's version 5.
const GenerationVersion = 5

// MaxPusherName bounds a pusher's name, from PusherVersion: a longer one
// is refused, on every node alike.
const MaxPusherName = 255

// BaseVersion is the version written until the cluster raised it.
const BaseVersion = 1

// The audit record that raises the written version: Action, with Outcome
// AuditRaised and the version in Target.
const (
	AuditPayloadVersion = "payload-version"
	AuditRaised         = "raised"
	// AuditRevokeKey with Outcome AuditPending and Target "slot:spki" is a
	// revocation an operator asked for while the cluster wrote a version
	// below RevokeVersion: it is written as KindRevokeKey once it may be.
	AuditRevokeKey = "revoke-key"
	AuditPending   = "pending"
)

// Kind is the command's discriminant, the second byte of the envelope.
type Kind uint8

const (
	KindRefTxn     Kind = 1
	KindAck        Kind = 2
	KindAudit      Kind = 3
	KindRepoDelete Kind = 4
	KindRepoRename Kind = 5
	KindJoinGrant  Kind = 6
	// The checkpoint kinds are proposed only once [cluster]
	// CHECKPOINT_INTERVAL is set, which every member's binary must know
	// first: an older one stops on them (D18).
	KindCheckpointBarrier  Kind = 7
	KindCheckpointPosition Kind = 8
	// KindRevokeKey is written at payload version 3 only (RevokeVersion),
	// once every member applies it: an older binary stops on it (D18).
	KindRevokeKey Kind = 9
)

// Limits pinned to the payload version: they are part of Apply's verdict, so
// they must be equal on every node and never come from a node's settings.
const (
	MaxUpdatesPerTxn = 4096
	// MaxEventBytes caps the event queue held in memory (R8-23). At the cap
	// a push is refused, by the hook before proposing and by Apply.
	MaxEventBytes = 64 << 20
	// AuditKeep is how many audit records the table retains.
	AuditKeep = 1000
)

// RefUpdate is one ref of a git transaction.
type RefUpdate struct {
	Ref string `json:"r"`
	// Old is the value git expected, empty when git expected nothing. A
	// non-empty Old must equal the table's live value.
	Old string `json:"o,omitempty"`
	// ExpectRev is the revision the proposer read; 0 means "absent". It is
	// always checked, which closes ABA (R1-04).
	ExpectRev uint64 `json:"e"`
	New       string `json:"n,omitempty"`
	Symbolic  string `json:"s,omitempty"`
	Delete    bool   `json:"d,omitempty"`
}

// RefTxn is one whole git transaction: one entry, all or nothing (R1-03).
// What the log consumer needs travels with it (R9-15).
type RefTxn struct {
	Repo        string            `json:"repo"`
	RepoID      int64             `json:"repo_id"`
	Format      string            `json:"fmt"`
	Updates     []RefUpdate       `json:"u"`
	PusherID    int64             `json:"pusher"`
	Time        int64             `json:"t"`
	Nonce       string            `json:"nonce,omitempty"`
	Trigger     string            `json:"trigger,omitempty"`
	PRID        int64             `json:"pr,omitempty"`
	IsWiki      bool              `json:"wiki,omitempty"`
	PushOptions map[string]string `json:"opts,omitempty"`
	// Push marks a transaction a pusher started (receive-pack). From
	// version 2, Apply refuses it on a deleted or renamed-away key; a
	// version 1 entry ignores it.
	Push bool `json:"push,omitempty"`
	// PusherName is the pusher's account name when it pushed. Written and
	// applied from PusherVersion only: an older binary drops it, and so
	// does Apply below that version.
	PusherName string `json:"pn,omitempty"`
	// ReadAt is the index the proposer's table had applied when it read
	// the revisions, from GenerationVersion: a transaction read before its
	// repository left the key, deleted or renamed away, is refused, even
	// once another repository took the key (#91). 0 skips the check.
	ReadAt uint64 `json:"read,omitempty"`
}

// Ack trims the events of a lineage up to an index, once the dispatcher read
// its cursor back from SQL (R8-23).
type Ack struct {
	Identity string `json:"id"`
	Index    uint64 `json:"i"`
}

// Audit records an administrative operation with its operator, so the
// product's audit is replicated like the log (D17, R2-36).
type Audit struct {
	// ID identifies the record, so a retried proposal is found, not doubled.
	ID       string `json:"id,omitempty"`
	Operator string `json:"op"`
	Action   string `json:"a"`
	Target   string `json:"tg,omitempty"`
	Outcome  string `json:"out"`
	Detail   string `json:"d,omitempty"`
	Time     int64  `json:"t"`
}

// RepoDelete tombstones every ref of a repository; the projector removes it.
type RepoDelete struct {
	Repo     string `json:"repo"`
	RepoID   int64  `json:"repo_id"`
	Operator string `json:"op,omitempty"`
	Time     int64  `json:"t"`
	// Nonce names this command, from GenerationVersion: its proposer tells
	// by it whether THIS command committed, not a state an older one left.
	Nonce string `json:"nonce,omitempty"`
}

// RepoRename moves a repository's refs to a new key; the projector moves it.
// Also lists further moves applied with it, all or none: a repository and
// its wiki, or every repository of a renamed owner.
type RepoRename struct {
	From string      `json:"from"`
	To   string      `json:"to"`
	Time int64       `json:"t"`
	Also [][2]string `json:"also,omitempty"`
	// Nonce names this command, from GenerationVersion (RepoDelete.Nonce).
	Nonce string `json:"nonce,omitempty"`
	// Owner is set on an owner's rename, old and new names, from
	// GenerationVersion: Apply refuses it when a live key under the old
	// owner is not moved, which a proposer behind the log omitted (#56).
	Owner [2]string `json:"owner,omitzero"`
}

// Moves returns every move of the command, the first one first.
func (rn *RepoRename) Moves() [][2]string {
	return append([][2]string{{rn.From, rn.To}}, rn.Also...)
}

// JoinGrant is an operator's permission for a new node to join under a slot
// by itself, until Expires (unix seconds), optionally only with one key.
// The consensus engine refuses a JOIN for a slot that still has a member;
// the grant is Forgejo's half of the fence decision (ADR 0152 D19, R8-09).
type JoinGrant struct {
	Slot     string `json:"slot"`
	SPKI     string `json:"spki,omitempty"`
	Expires  int64  `json:"exp"`
	Operator string `json:"op"`
}

// RevokeKey refuses a node key for good: a JOIN presenting it is refused
// whatever the grant or the CA says, and the object channel admits no
// member holding it. An operator's Remove and DenyPin write it; the
// autopilot's removal of a dead member does not, so that member returns.
type RevokeKey struct {
	SPKI     string `json:"spki"`
	Slot     string `json:"slot"`
	Operator string `json:"op"`
	Time     int64  `json:"t"`
}

// Encode writes the envelope at BaseVersion: payload version, kind, JSON
// body.
func Encode(kind Kind, body any) ([]byte, error) {
	return EncodeVersion(BaseVersion, kind, body)
}

// EncodeVersion writes the envelope at version v, which the caller read from
// Table.WriteVersion: a version the cluster has not agreed to would stop
// the members that cannot read it. Below GenerationVersion only a ref
// transaction and a key revocation are written above BaseVersion: v is
// lowered to BaseVersion for the other kinds, as older binaries read them.
func EncodeVersion(v uint8, kind Kind, body any) ([]byte, error) {
	if v > BaseVersion && v < GenerationVersion && kind != KindRefTxn && (kind != KindRevokeKey || v < RevokeVersion) {
		v = BaseVersion
	}
	if v < BaseVersion || v > PayloadVersion {
		return nil, fmt.Errorf("%w: cannot write payload version %d", ErrVersion, v)
	}
	b, err := json.Marshal(body)
	if err != nil {
		return nil, err
	}
	return append([]byte{v, byte(kind)}, b...), nil
}

// ErrVersion is a command of a version this node does not know: the node
// must stop, never skip it (D18).
var ErrVersion = consensus.ErrVersionUnsupported

// errMalformed is a body of a known kind that does not parse: a refusal.
var errMalformed = errors.New("malformed command body")

// decode reads an envelope a binary applying versions up to limit reads:
// PayloadVersion, but for the tests that play an older binary.
func decode(data []byte, limit uint8) (Kind, any, error) {
	if len(data) < 2 {
		return 0, nil, errors.New("short command")
	}
	if data[0] == 0 || data[0] > limit {
		return 0, nil, fmt.Errorf("%w: payload version %d", ErrVersion, data[0])
	}
	if data[0] > BaseVersion && data[0] < GenerationVersion && Kind(data[1]) != KindRefTxn && (Kind(data[1]) != KindRevokeKey || data[0] < RevokeVersion) {
		// Versions 2 and 4 change the ref transaction only, version 3 adds
		// the key revocation: any other kind between versions 2 and 4 was
		// written by no version of this code. From version 5 every kind
		// may be.
		return 0, nil, fmt.Errorf("%w: command kind %d at payload version %d", ErrVersion, data[1], data[0])
	}
	kind := Kind(data[1])
	var v any
	switch kind {
	case KindRefTxn:
		v = &RefTxn{}
	case KindAck:
		v = &Ack{}
	case KindAudit:
		v = &Audit{}
	case KindRepoDelete:
		v = &RepoDelete{}
	case KindRepoRename:
		v = &RepoRename{}
	case KindJoinGrant:
		v = &JoinGrant{}
	case KindCheckpointBarrier:
		v = &CheckpointBarrier{}
	case KindCheckpointPosition:
		v = &CheckpointPosition{}
	case KindRevokeKey:
		v = &RevokeKey{}
	default:
		return 0, nil, fmt.Errorf("%w: command kind %d", ErrVersion, kind)
	}
	if err := json.Unmarshal(data[2:], v); err != nil {
		// A body that does not parse is answered as a refusal, the same on
		// every node, never a skipped entry.
		return kind, nil, errMalformed
	}
	return kind, v, nil
}

// Result is Apply's answer to a command. A refusal is an answer, identical on
// every node (D3); it is not an error.
type Result struct {
	OK     bool     `json:"ok"`
	Reason string   `json:"reason,omitempty"`
	Revs   []uint64 `json:"revs,omitempty"`
}

func refuse(format string, args ...any) Result {
	return Result{Reason: fmt.Sprintf(format, args...)}
}
