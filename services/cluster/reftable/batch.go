// Copyright 2026 The Forgejo Authors. All rights reserved.
// SPDX-License-Identifier: GPL-3.0-or-later

package reftable

import (
	"encoding/json" //nolint:depguard // the weight of an update is its encoding by the state machine's own codec
	"math"
	"strings"

	"forgejo.org/services/cluster/consensus"
)

// BatchBytes bounds the weight (UpdateBytes) of the updates of one batch,
// where Forgejo writes refs by batches in a cluster: a mirror's sync, a
// fork, a migration, an import. A batch is one transaction and one entry,
// which the engine refuses past consensus.MaxEntryBytes as
// ENTRY_TOO_LARGE: cut on its count alone, a batch of long names, or of
// names encoding/json escapes, passed the bound well under
// MaxUpdatesPerTxn and was refused at every attempt (#385). The rest of
// the entry, its repository's key, nonce and pusher, takes a few hundred
// bytes, and is left far more. It decides no verdict of Apply.
const BatchBytes = consensus.MaxEntryBytes - 64<<10

// longestObjectID is an object id of the longest format, sha256.
var longestObjectID = strings.Repeat("f", 64)

// UpdateBytes bounds what the update of ref, symbolic to target when
// target is set, adds to an encoded RefTxn, whatever the hook builds of it
// (buildTxn): the names as encoding/json writes them, '&', '<' and '>' on
// six bytes each, an old and a new object id of the longest format, the
// largest revision, the delete flag, their keys, and the comma before the
// next update. A name ValidRefName takes weighs some 6 KiB at most, far
// within BatchBytes.
func UpdateBytes(ref, target string) int {
	b, err := json.Marshal(RefUpdate{Ref: ref, Old: longestObjectID, ExpectRev: math.MaxUint64, New: longestObjectID, Symbolic: target, Delete: true})
	if err != nil {
		// A struct of strings and numbers always encodes.
		return BatchBytes
	}
	return len(b) + 1
}

// BatchLen returns how many of the next n updates go in one batch: at most
// limit, their weights, size(i) for the i-th, within BatchBytes, and at
// least one while n is not zero, so that a caller cutting batches always
// moves on.
func BatchLen(n, limit int, size func(i int) int) int {
	k, total := 0, 0
	for k < min(n, limit) {
		total += size(k)
		if total > BatchBytes && k > 0 {
			break
		}
		k++
	}
	return max(k, min(n, 1))
}
