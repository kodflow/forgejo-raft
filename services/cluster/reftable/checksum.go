// Copyright 2026 The Forgejo Authors. All rights reserved.
// SPDX-License-Identifier: GPL-3.0-or-later

package reftable

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"math/bits"
)

// Sum is a value modulo 2^256 held on four 64-bit limbs, least significant
// first (ADR 0152 D19, R5-07, R6-11). A repository's hash is the sum of its
// leaves, and the root the sum of its repositories' terms, so an update
// costs one subtraction and one addition whatever the table's size.
type Sum [4]uint64

func (s Sum) add(o Sum) Sum {
	var r Sum
	var c uint64
	for i := range 4 {
		r[i], c = bits.Add64(s[i], o[i], c)
	}
	return r
}

func (s Sum) sub(o Sum) Sum {
	var r Sum
	var b uint64
	for i := range 4 {
		r[i], b = bits.Sub64(s[i], o[i], b)
	}
	return r
}

// Bytes encodes the limbs least significant first, each big-endian.
func (s Sum) Bytes() []byte {
	b := make([]byte, 0, 32)
	for i := range 4 {
		b = binary.BigEndian.AppendUint64(b, s[i])
	}
	return b
}

func (s Sum) String() string { return hex.EncodeToString(s.Bytes()) }

// IsZero reports the additive identity.
func (s Sum) IsZero() bool { return s == Sum{} }

// sumOf reads a 32-byte digest with the same limb layout Bytes writes.
func sumOf(d [32]byte) Sum {
	var s Sum
	for i := range 4 {
		s[i] = binary.BigEndian.Uint64(d[i*8:])
	}
	return s
}

const hashVersion = 1

const (
	leafDirect    = 0
	leafSymbolic  = 1
	leafTombstone = 2
)

func appendLP(b []byte, s string) []byte {
	b = binary.BigEndian.AppendUint32(b, uint32(len(s)))
	return append(b, s...)
}

// leaf is the SHA-256 of the fixed, length-prefixed, big-endian encoding of
// one table entry. Nothing local to the node enters it.
func leaf(repo, ref string, v *RefValue) Sum {
	b := make([]byte, 0, 128)
	b = append(b, hashVersion)
	switch {
	case v.Tombstone:
		b = append(b, leafTombstone)
	case v.Symbolic != "":
		b = append(b, leafSymbolic)
	default:
		b = append(b, leafDirect)
	}
	b = appendLP(b, repo)
	b = appendLP(b, ref)
	b = binary.BigEndian.AppendUint64(b, v.Rev)
	switch {
	case v.Tombstone:
	case v.Symbolic != "":
		b = appendLP(b, v.Symbolic)
	default:
		raw, _ := hex.DecodeString(v.SHA)
		b = appendLP(b, v.Format)
		b = appendLP(b, string(raw))
	}
	return sumOf(sha256.Sum256(b))
}

// rootTerm is one repository's contribution to the root. The id is
// length-prefixed so no two ids collide by concatenation.
func rootTerm(repo string, h Sum) Sum {
	b := make([]byte, 0, 4+len(repo)+32)
	b = appendLP(b, repo)
	b = append(b, h.Bytes()...)
	return sumOf(sha256.Sum256(b))
}

// ProjectionDigest hashes what git can observe of a repository: live refs by
// value, symbolic refs by target name, no tombstones and no revisions. The
// projector compares a repository's git state with the table under these
// same rules (R4-25), never with the consensus digest.
func ProjectionDigest(refs map[string]ProjectedRef) Sum {
	var s Sum
	for name, r := range refs {
		b := make([]byte, 0, 96)
		b = append(b, hashVersion)
		b = appendLP(b, name)
		if r.Symbolic != "" {
			b = append(b, leafSymbolic)
			b = appendLP(b, r.Symbolic)
		} else {
			b = append(b, leafDirect)
			b = appendLP(b, r.SHA)
		}
		s = s.add(sumOf(sha256.Sum256(b)))
	}
	return s
}

// ProjectedRef is a ref as git shows it.
type ProjectedRef struct {
	SHA      string
	Symbolic string
}
