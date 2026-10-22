// Copyright 2026 The Forgejo Authors. All rights reserved.
// SPDX-License-Identifier: GPL-3.0-or-later

package cluster

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"forgejo.org/modules/clusterhook"
	"forgejo.org/modules/test"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// The SSH path reserves its nonce before git runs. Until "prepared", git
// can publish nothing, so the reservation fences no certification; it
// still keeps a directory move and git gc away from the upload, and
// "prepared" turns it into a fence.
func TestAReservationFencesNothingUntilPrepared(t *testing.T) {
	reg := newRegistry(10*time.Millisecond, 8, 64)
	require.NoError(t, reg.reserve("n1", "alice/a.git", os.Getpid()))
	time.Sleep(20 * time.Millisecond) // past the TTL, serv still alive

	assert.False(t, reg.inFlight("alice/a.git"))
	assert.True(t, reg.busy("alice/a.git"), "the upload still writes under the directory")
	ran := false
	assert.True(t, reg.certify("alice/a.git", func() { ran = true }))
	assert.True(t, ran)

	require.NoError(t, reg.register("n1", "alice/a.git", []string{"refs/heads/main"}, os.Getpid(), false))
	assert.Equal(t, 1, reg.count(), "prepared completes the reservation")
	assert.True(t, reg.inFlight("alice/a.git"))
	assert.False(t, reg.certify("alice/a.git", func() { t.Error("certified behind a prepared transaction") }))

	require.ErrorIs(t, reg.reserve("n1", "bob/b.git", 1), errNonceReused)
	reg.release("n1")
	assert.False(t, reg.busy("alice/a.git"))
}

// A long SSH push into one repository, still uploading on the leader, does
// not hold the consumer's queue: another push of that repository is
// projected, certified and its event dispatched, nothing parked.
func TestALongSSHUploadDoesNotHoldTheConsumer(t *testing.T) {
	defer test.MockVariableValue(&projectionStallBound, time.Minute)()
	tc := newTestCluster(t, "node-a", "node-b", "node-c")
	leader := tc.nodes[0]
	resp := leader.r.Handle(context.Background(), &clusterhook.Request{
		Op: clusterhook.OpRegister, Nonce: "slow-upload", PID: os.Getpid(),
		GitDir: filepath.Join(leader.r.root, "o", "slow.git"),
	})
	require.True(t, resp.OK, resp.Message)
	defer leader.r.registry.release("slow-upload")

	work := t.TempDir()
	run(t, work, "init", "-q")
	c1 := commit(t, work, "one")
	require.True(t, tc.pushVia(tc.nodes[1], work, "o/slow.git", "refs/heads/main", strings.Repeat("0", 40), c1).OK)

	require.Eventually(t, func() bool {
		leader.work.mu.Lock()
		defer leader.work.mu.Unlock()
		for _, ev := range leader.work.events {
			if ev.Repo == "o/slow.git" {
				return true
			}
		}
		return false
	}, 10*time.Second, 10*time.Millisecond, "the event of the repository being uploaded to is dispatched")
	rows, err := tc.parked.list(context.Background())
	require.NoError(t, err)
	assert.Empty(t, rows)
}

// SSH sessions left hanging on a repository hold reservations only: they
// count against a total cap of their own, never against the repository's,
// so a later push of that repository still registers.
func TestHangingSSHSessionsDoNotTurnARepositoryAway(t *testing.T) {
	reg := newRegistry(time.Minute, 2, 4)
	for _, n := range []string{"r1", "r2", "r3"} {
		require.NoError(t, reg.reserve(n, "alice/a.git", os.Getpid()))
	}
	require.NoError(t, reg.register("p1", "alice/a.git", []string{"refs/heads/main"}, os.Getpid(), false))
	require.NoError(t, reg.register("r1", "alice/a.git", []string{"refs/heads/b"}, os.Getpid(), false), "a reservation completes")
	require.ErrorIs(t, reg.register("r2", "alice/a.git", []string{"refs/heads/c"}, os.Getpid(), false), errTooManyTxn,
		"the per-repository cap holds for prepared transactions")
	require.ErrorIs(t, reg.register("p2", "alice/a.git", []string{"refs/heads/d"}, os.Getpid(), false), errTooManyTxn)

	// Reservations have their own total cap.
	require.NoError(t, reg.reserve("r4", "bob/b.git", os.Getpid()))
	require.NoError(t, reg.reserve("r5", "bob/b.git", os.Getpid()))
	require.ErrorIs(t, reg.reserve("r6", "bob/b.git", os.Getpid()), errTooManyTxn)

	// A reservation whose process is gone past the TTL is swept at the cap.
	now := time.Now()
	reg.now = func() time.Time { return now.Add(time.Hour) }
	reg.alive = func(int) bool { return false }
	require.NoError(t, reg.reserve("r6", "bob/b.git", 1))
}
