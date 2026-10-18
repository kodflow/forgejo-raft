// Copyright 2026 The Forgejo Authors. All rights reserved.
// SPDX-License-Identifier: GPL-3.0-or-later

package cluster

import (
	"context"
	"testing"

	"forgejo.org/modules/setting"
	"forgejo.org/modules/test"

	"github.com/stretchr/testify/assert"
)

// Finding 4: an acknowledged commit survives a failover only when a
// synchronous standby flushed it before the acknowledgement, which
// Patroni guarantees with synchronous_mode_strict alone (non-strict, it
// empties synchronous_standby_names when the standby goes away).
func TestPGVerdict(t *testing.T) {
	durable := pgSettings{SynchronousCommit: "on", StandbyNames: `"pg-2"`, Fsync: "on", Sync: 1, Caught: 1}
	for _, tc := range []struct {
		name       string
		change     func(*pgSettings)
		standalone bool
		ok         bool
	}{
		{"strict synchronous standby holds every commit", func(*pgSettings) {}, false, true},
		{"remote_apply", func(s *pgSettings) { s.SynchronousCommit = "remote_apply" }, false, true},
		{"non-strict Patroni without its standby", func(s *pgSettings) { s.StandbyNames, s.Sync, s.Caught = "", 0, 0 }, false, false},
		{"standby configured, none connected", func(s *pgSettings) { s.Sync, s.Caught = 0, 0 }, false, false},
		{"standby behind the position read", func(s *pgSettings) { s.Caught = 0 }, false, false},
		{"one of two quorum standbys behind", func(s *pgSettings) { s.Sync, s.Caught = 2, 1 }, false, false},
		{"synchronous_commit off", func(s *pgSettings) { s.SynchronousCommit = "off" }, false, false},
		{"synchronous_commit local", func(s *pgSettings) { s.SynchronousCommit = "local" }, false, false},
		{"synchronous_commit remote_write", func(s *pgSettings) { s.SynchronousCommit = "remote_write" }, false, false},
		{"fsync off", func(s *pgSettings) { s.Fsync = "off" }, false, false},
		{"answering from a standby", func(s *pgSettings) { s.InRecovery = true }, false, false},
		{"standalone, local flush", func(s *pgSettings) { s.SynchronousCommit, s.StandbyNames, s.Sync, s.Caught = "local", "", 0, 0 }, true, true},
		{"standalone, synchronous_commit off", func(s *pgSettings) { s.SynchronousCommit = "off" }, true, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := durable
			tc.change(&s)
			ok, why := pgVerdict(s, tc.standalone)
			assert.Equal(t, tc.ok, ok, why)
			assert.NotEmpty(t, why)
		})
	}
}

// SQLite is one local file: no failover, the single-node mode trims.
func TestSQLiteIsDurable(t *testing.T) {
	defer test.MockVariableValue(&setting.Database.Type, setting.DatabaseType("sqlite3"))()
	ok, _ := sqlDurable(context.Background())
	assert.True(t, ok)
	defer test.MockVariableValue(&setting.Database.Type, setting.DatabaseType("mysql"))()
	ok, why := sqlDurable(context.Background())
	assert.False(t, ok, "a database whose replication the cluster cannot see is not assumed durable")
	assert.Contains(t, why, "SQL_STANDALONE")
	defer test.MockVariableValue(&setting.Cluster.SQLStandalone, true)()
	ok, _ = sqlDurable(context.Background())
	assert.True(t, ok)
}
