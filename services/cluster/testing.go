// Copyright 2026 The Forgejo Authors. All rights reserved.
// SPDX-License-Identifier: GPL-3.0-or-later

package cluster

import (
	"crypto/sha256"
	"time"

	"forgejo.org/services/cluster/consensus/memraft"
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
