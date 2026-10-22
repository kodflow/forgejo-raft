// Copyright 2026 The Forgejo Authors. All rights reserved.
// SPDX-License-Identifier: GPL-3.0-or-later

package cluster

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"forgejo.org/modules/test"
	"forgejo.org/services/cluster/consensus"

	"github.com/stretchr/testify/assert"
)

// healthEngine answers Health and counts its closes.
type healthEngine struct {
	consensus.Engine
	health consensus.Health
	closed atomic.Int32
}

func (e *healthEngine) Health() consensus.Health { return e.health }

func (e *healthEngine) Close(context.Context) error {
	e.closed.Add(1)
	return nil
}

// A node that stopped itself ends Forgejo with the stop's exit code, 73
// for a STORAGE_FAILED whose marker could be written nowhere, so the
// supervisor does not start it again in the same boot; a node closed
// without a stop does not (#116, R9-05).
func TestForgejoExitsWithTheStopsCode(t *testing.T) {
	defer test.MockVariableValue(&stopPoll, 10*time.Millisecond)()
	exited := make(chan int, 1)
	defer test.MockVariableValue(&exitProcess, func(code int) { exited <- code })()

	for _, tc := range []struct {
		health consensus.Health
		code   int
	}{
		{consensus.Health{Stopped: true, StopReason: "STORAGE_FAILED", StopExitCode: 73}, 73},
		{consensus.Health{Stopped: true}, 0},
		{consensus.Health{Ready: true}, 0},
	} {
		r := newReplicator(t.TempDir(), newRegistry(0, 1, 1))
		eng := &healthEngine{health: tc.health}
		r.setEngine(eng)
		ctx, cancel := context.WithCancel(context.Background())
		done := make(chan struct{})
		go func() { defer close(done); r.exitOnStop(ctx) }()
		select {
		case code := <-exited:
			assert.Equal(t, tc.code, code)
			assert.EqualValues(t, 1, eng.closed.Load(), "closed before the exit")
		case <-time.After(200 * time.Millisecond):
			assert.Zero(t, tc.code, "no exit for %+v", tc.health)
		}
		cancel()
		<-done
	}
}
