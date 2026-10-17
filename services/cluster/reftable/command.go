// Copyright 2026 The Forgejo Authors. All rights reserved.
// SPDX-License-Identifier: GPL-3.0-or-later

package reftable

import (
	"encoding/json" //nolint:depguard // the state machine decodes with the standard library alone: Apply's verdict must not depend on a third-party codec's version
	"errors"
	"fmt"
)

// PayloadVersion is the version of every command this code writes and the
// highest it applies. A new version starts only at a gate entry raised by
// RaiseClusterVersion (ADR 0152 D18); an entry above it stops the node.
const PayloadVersion = 1

// Kind is the command's discriminant, the second byte of the envelope.
type Kind uint8

const (
	KindRefTxn     Kind = 1
	KindAck        Kind = 2
	KindAudit      Kind = 3
	KindRepoDelete Kind = 4
	KindRepoRename Kind = 5
	KindJoinGrant  Kind = 6
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
}

// RepoRename moves a repository's refs to a new key; the projector moves it.
// Also lists further moves applied with it, all or none: a repository and
// its wiki, or every repository of a renamed owner.
type RepoRename struct {
	From string      `json:"from"`
	To   string      `json:"to"`
	Time int64       `json:"t"`
	Also [][2]string `json:"also,omitempty"`
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

// Encode writes the envelope: payload version, kind, JSON body.
func Encode(kind Kind, body any) ([]byte, error) {
	b, err := json.Marshal(body)
	if err != nil {
		return nil, err
	}
	return append([]byte{PayloadVersion, byte(kind)}, b...), nil
}

// ErrVersion is a command of a version this node does not know: the node
// must stop, never skip it (D18).
var ErrVersion = errors.New("VERSION_UNSUPPORTED")

// errMalformed is a body of a known kind that does not parse: a refusal.
var errMalformed = errors.New("malformed command body")

func decode(data []byte) (Kind, any, error) {
	if len(data) < 2 {
		return 0, nil, errors.New("short command")
	}
	if data[0] == 0 || data[0] > PayloadVersion {
		return 0, nil, fmt.Errorf("%w: payload version %d", ErrVersion, data[0])
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
