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

// RegisterDeleteGate registers gate in place of the registered one, and
// returns the function that puts that one back. release does so only while
// gate is still the registered one: a gate registered after it, as a
// replicator registers its own once it started, stays. Releases go in the
// reverse order of the registrations, as deferred calls do.
func RegisterDeleteGate(gate func(del func() error) error) (release func()) {
	registered := &gate
	previous := deleteGate.Swap(registered)
	return func() { deleteGate.CompareAndSwap(registered, previous) }
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
