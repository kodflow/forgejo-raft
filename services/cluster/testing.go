// Copyright 2026 The Forgejo Authors. All rights reserved.
// SPDX-License-Identifier: GPL-3.0-or-later

package cluster

import (
	"context"
	"crypto/sha256"
	"errors"
	"time"

	"forgejo.org/services/cluster/consensus/memraft"
	"forgejo.org/services/cluster/reftable"
)

// NewForTesting builds replicators on an in-process log, one per slot, with
// no listener, no projector loop and no consumer: enough for the
// administration's tests. It is never called by Forgejo itself.
func NewForTesting(root string, slots ...string) (*memraft.Cluster, []*Replicator) {
	mem := memraft.New()
	var out []*Replicator
	for _, slot := range slots {
		r := newReplicator(root, newRegistry(time.Second, 8, 64))
		r.setEngine(mem.Add(slot, sha256.Sum256([]byte(slot)), r.table))
		// Every member of the in-memory cluster runs this binary, and
		// answers a member's hello as such.
		r.memberProbe = func(_ context.Context, _, slot string) (hello, error) {
			for _, m := range r.Engine().Membership().Members {
				if m.Slot == slot {
					return hello{Slot: slot, State: helloMember, Node: m.ID.String(), Payload: reftable.PayloadVersion, SPKI: m.SPKI}, nil
				}
			}
			return hello{}, errors.New("no member under slot " + slot)
		}
		out = append(out, r)
	}
	return mem, out
}

// SetCurrentForTesting makes r the running replicator and returns the undo.
func SetCurrentForTesting(r *Replicator) func() {
	currentMu.Lock()
	prev := current
	current = r
	currentMu.Unlock()
	return func() {
		currentMu.Lock()
		current = prev
		currentMu.Unlock()
	}
}

// SetSQLDurabilityForTesting sets the consumer's last durability verdict.
func SetSQLDurabilityForTesting(r *Replicator, ok bool, reason string) {
	if r.consumer == nil {
		r.consumer = &consumer{r: r}
	}
	r.consumer.durability.set(ok, reason)
}

// SetBackupLagForTesting sets the consumer's held backup lag.
func SetBackupLagForTesting(r *Replicator, lag uint64) {
	SetBackupHoldForTesting(r, lag, 0)
}

// SetBackupHoldForTesting sets the consumer's held backup lag and the bytes
// the backup holds behind it.
func SetBackupHoldForTesting(r *Replicator, lag uint64, held int64) {
	if r.consumer == nil {
		r.consumer = &consumer{r: r}
	}
	r.consumer.mu.Lock()
	r.consumer.backupLag, r.consumer.backupHeld = lag, held
	r.consumer.mu.Unlock()
}

// SetBackupCursorForTesting makes the consumer's last pass have read the
// backup cursor of the engine's lineage at position.
func SetBackupCursorForTesting(r *Replicator, position uint64) {
	if r.consumer == nil {
		r.consumer = &consumer{r: r}
	}
	lin := r.Engine().Status().Identity.String()
	r.consumer.mu.Lock()
	r.consumer.backupSeen = backupNote{Lineage: lin, Position: position, At: time.Now()}
	r.consumer.mu.Unlock()
}

// SetCheckpointViewForTesting makes the leader have just found which voters
// take the checkpoints (on) and which did not say (unknown).
func SetCheckpointViewForTesting(r *Replicator, on, unknown []string) {
	r.ckState.set(checkpointView{At: r.clock(), On: on, Unknown: unknown})
}
