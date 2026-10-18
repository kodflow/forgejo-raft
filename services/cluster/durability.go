// Copyright 2026 The Forgejo Authors. All rights reserved.
// SPDX-License-Identifier: GPL-3.0-or-later

package cluster

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	"forgejo.org/models/db"
	"forgejo.org/modules/setting"
)

// The consumer trims the log's events only behind SQL rows that survive
// any PostgreSQL failover: a cursor read from the primary is not enough,
// since a failover to a standby that had not received its commit would
// bring it back, and the events past it would be gone from the log.
//
// sqlDurable is called AFTER the values a trim depends on were read: it
// reads the primary's WAL position then, so every commit those values
// came from lies at or before it, and confirms that every synchronous
// standby, the only nodes Patroni promotes in synchronous_mode_strict,
// has flushed past it.

// Durability is the last verdict on SQL's durability.
type Durability struct {
	Checked time.Time
	OK      bool
	Reason  string
}

// durabilityState keeps the last verdict for the health check and the
// administration.
type durabilityState struct {
	mu   sync.Mutex
	last Durability
}

func (s *durabilityState) set(ok bool, reason string) {
	s.mu.Lock()
	s.last = Durability{Checked: time.Now(), OK: ok, Reason: reason}
	s.mu.Unlock()
}

func (s *durabilityState) get() Durability {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.last
}

// sqlDurable reports whether what SQL committed so far survives a failover
// of the database, and why not.
func sqlDurable(ctx context.Context) (bool, string) {
	switch {
	case setting.Database.Type.IsSQLite3():
		// One file on this node: no failover can bring an older copy back.
		return true, "SQLite: a local database, no failover"
	case setting.Database.Type.IsPostgreSQL():
		return pgDurable(ctx)
	default:
		if setting.Cluster.SQLStandalone {
			return true, fmt.Sprintf("%s declared standalone ([cluster] SQL_STANDALONE)", setting.Database.Type)
		}
		return false, fmt.Sprintf("%s: the cluster cannot verify its replication; set [cluster] SQL_STANDALONE only if no replica can ever be promoted", setting.Database.Type)
	}
}

// pgSettings is what pgDurable reads from the primary.
type pgSettings struct {
	SynchronousCommit string
	StandbyNames      string
	Fsync             string
	InRecovery        bool
	// Sync is the number of synchronous or quorum standbys connected, and
	// Caught how many of them flushed past the position read.
	Sync, Caught int
}

// pgVerdict is pgDurable's rule, apart from the queries.
func pgVerdict(s pgSettings, standalone bool) (bool, string) {
	if s.InRecovery {
		return false, "the database answers from a standby"
	}
	if s.Fsync != "on" {
		return false, "fsync is " + s.Fsync + ": a crash loses committed transactions"
	}
	switch s.SynchronousCommit {
	case "on", "remote_apply":
	case "off", "local", "remote_write":
		if standalone && s.SynchronousCommit != "off" {
			break
		}
		return false, "synchronous_commit is " + s.SynchronousCommit + ": a commit is acknowledged before a synchronous standby flushed it"
	default:
		return false, "synchronous_commit is " + s.SynchronousCommit
	}
	if standalone {
		return true, "standalone PostgreSQL ([cluster] SQL_STANDALONE): commits are flushed locally"
	}
	if strings.TrimSpace(s.StandbyNames) == "" {
		return false, "synchronous_standby_names is empty: a failover can promote a standby that lacks acknowledged commits (Patroni needs synchronous_mode_strict: true)"
	}
	if s.Sync == 0 {
		return false, "no synchronous standby is connected"
	}
	if s.Caught < s.Sync {
		return false, fmt.Sprintf("%d of %d synchronous standbys have flushed the position read (or their positions are hidden: grant pg_monitor to Forgejo's role)", s.Caught, s.Sync)
	}
	return true, fmt.Sprintf("%d synchronous standbys hold every commit read", s.Sync)
}

// standbyCatchUp bounds how long a durability check waits for the
// synchronous standbys to flush the position it read.
var standbyCatchUp = 5 * time.Second

func pgDurable(ctx context.Context) (bool, string) {
	var s pgSettings
	e := db.GetEngine(ctx)
	row, err := queryOne(e, `SELECT current_setting('synchronous_commit') AS sc,
		current_setting('synchronous_standby_names') AS ssn,
		current_setting('fsync') AS fsync,
		pg_is_in_recovery()::text AS rec`)
	if err != nil {
		return false, "reading PostgreSQL's durability: " + err.Error()
	}
	s.SynchronousCommit, s.StandbyNames, s.Fsync = row["sc"], row["ssn"], row["fsync"]
	s.InRecovery = row["rec"] == "true"
	if s.InRecovery || setting.Cluster.SQLStandalone {
		return pgVerdict(s, setting.Cluster.SQLStandalone)
	}
	// The position is read now, after the caller's reads: every commit
	// they saw is at or before it.
	row, err = queryOne(e, "SELECT pg_current_wal_lsn()::text AS lsn")
	if err != nil {
		return false, "reading PostgreSQL's position: " + err.Error()
	}
	lsn := row["lsn"]
	deadline := time.Now().Add(standbyCatchUp)
	for {
		row, err = queryOne(e, `SELECT count(*)::text AS sync,
			count(*) FILTER (WHERE flush_lsn IS NOT NULL AND flush_lsn >= CAST(? AS pg_lsn))::text AS caught
			FROM pg_stat_replication WHERE sync_state IN ('sync', 'quorum')`, lsn)
		if err != nil {
			return false, "reading the standbys' positions: " + err.Error()
		}
		s.Sync, _ = strconv.Atoi(row["sync"])
		s.Caught, _ = strconv.Atoi(row["caught"])
		ok, why := pgVerdict(s, false)
		if ok || s.Sync == 0 || strings.TrimSpace(s.StandbyNames) == "" || !time.Now().Before(deadline) {
			return ok, why
		}
		select {
		case <-ctx.Done():
			return false, ctx.Err().Error()
		case <-time.After(50 * time.Millisecond):
		}
	}
}
