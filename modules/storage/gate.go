// Copyright 2026 The Forgejo Authors. All rights reserved.
// SPDX-License-Identifier: GPL-3.0-or-later

package storage

import "sync/atomic"

// In a cluster, a checkpoint lists the stored files once it has read
// PostgreSQL's position L: a file deleted between L and that listing would
// be named by the SQL at L and missing from the checkpoint (#90). The
// deletions of the storages a checkpoint covers go through a gate the
// replicator registers, which holds them while a listing runs.
var deleteGate atomic.Pointer[func(del func() error) error]

// SetDeleteGate registers the gate the covered storages delete through;
// nil removes it.
func SetDeleteGate(gate func(del func() error) error) {
	if gate == nil {
		deleteGate.Store(nil)
		return
	}
	deleteGate.Store(&gate)
}

// gatedStorage deletes through the gate, when one is registered.
type gatedStorage struct {
	ObjectStorage
}

// WithDeleteGate returns s, its deletions going through the registered
// gate.
func WithDeleteGate(s ObjectStorage) ObjectStorage {
	return gatedStorage{ObjectStorage: s}
}

// Delete removes path once the gate lets it.
func (g gatedStorage) Delete(path string) error {
	if gate := deleteGate.Load(); gate != nil {
		return (*gate)(func() error { return g.ObjectStorage.Delete(path) })
	}
	return g.ObjectStorage.Delete(path)
}
