// Copyright 2026 The Forgejo Authors. All rights reserved.
// SPDX-License-Identifier: GPL-3.0-or-later

package cluster

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// A pass in flight when refence runs may still certify a watermark or
// record a failure of the table about to be emptied: refence clears the
// projector's state once that pass ended, so none of it survives into the
// table that replays from its start (CodeRabbit on #120).
func TestRefenceClearsWhatAPassInFlightWrote(t *testing.T) {
	r := newReplicator(t.TempDir(), newRegistry(0, 1, 1))
	p := r.projector
	p.unfence()
	p.passMu.Lock() // a pass in flight
	done := make(chan struct{})
	go func() {
		p.refence()
		close(done)
	}()
	require.Eventually(t, p.Fenced, 5*time.Second, time.Millisecond)
	// The pass, still running, writes the old table's state.
	p.mu.Lock()
	p.watermark["o/r.git"] = 7
	p.failures["o/r.git"] = "boom"
	p.dirty["o/r.git"] = 7
	p.pendingFirst["o/r.git"] = true
	p.mu.Unlock()
	p.passMu.Unlock()
	<-done
	p.mu.Lock()
	defer p.mu.Unlock()
	assert.Empty(t, p.watermark)
	assert.Empty(t, p.failures)
	assert.Empty(t, p.dirty)
	assert.Empty(t, p.pendingFirst)
}
