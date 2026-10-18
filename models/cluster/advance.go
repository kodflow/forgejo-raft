// Copyright 2026 The Forgejo Authors. All rights reserved.
// SPDX-License-Identifier: GPL-3.0-or-later

package cluster

import (
	"context"
	"fmt"
	"math"
	"strconv"

	"forgejo.org/models/db"
	"forgejo.org/modules/setting"
)

// A recovery to a checkpoint's position L brings every sequence back to
// its state at L: the IDs handed out after L would be handed out again,
// while mails, webhooks, CI runners and other forges still hold them, and
// so would the issue and pull request numbers of each repository. The
// recovery advances them all, once, past anything issued after L.

// SequenceAdvance is how far each sequence moves: far beyond what a forge
// hands out between a checkpoint and a disaster.
const SequenceAdvance int64 = 1_000_000_000

// ResourceIndexAdvance is how far each repository's numbers (issues and
// pull requests, action runs) move.
const ResourceIndexAdvance int64 = 100_000

// resourceIndexes are the per-repository numbering tables, and the rows
// they number.
var resourceIndexes = []struct{ Index, Rows, Group string }{
	{"issue_index", "issue", "repo_id"},
	{"action_run_index", "action_run", "repo_id"},
}

// CursorRecoveryAdvance records the recovery whose identifiers were
// advanced, so a resumed recovery does not advance them again.
const CursorRecoveryAdvance = "recovery-advance"

// Sequence is one sequence of the database, as the plan reads it.
type Sequence struct {
	Name     string
	DataType string
	// Last is the last value handed out (start - 1 when none was).
	Last int64
	Max  int64
}

// SequenceStep is one sequence's move.
type SequenceStep struct {
	Name     string
	From, To int64
	// Narrow is set when the sequence's type leaves less room than
	// SequenceAdvance: it moves half of what is left.
	Narrow bool
}

// PlanSequenceAdvance plans the move of every sequence. A sequence too
// close to its maximum to move is an error: the recovery stops rather than
// reissue its IDs.
func PlanSequenceAdvance(seqs []Sequence) ([]SequenceStep, error) {
	out := make([]SequenceStep, 0, len(seqs))
	for _, s := range seqs {
		if s.Max <= s.Last+1 {
			return nil, fmt.Errorf("sequence %s (%s) is at %d of %d: it cannot be advanced", s.Name, s.DataType, s.Last, s.Max)
		}
		delta, narrow := SequenceAdvance, false
		if room := s.Max - s.Last; delta > room/2 {
			delta, narrow = room/2, true
		}
		out = append(out, SequenceStep{Name: s.Name, From: s.Last, To: s.Last + delta, Narrow: narrow})
	}
	return out, nil
}

// SequenceSQL is the statement that applies a step on the database type.
func SequenceSQL(dbType setting.DatabaseType, st SequenceStep) (string, []any) {
	if dbType.IsSQLite3() {
		return "UPDATE sqlite_sequence SET seq = ? WHERE name = ? AND seq < ?", []any{st.To, st.Name, st.To}
	}
	// setval is not transactional: once run, the sequence stays moved
	// even if what follows fails, which only widens the gap.
	return "SELECT setval(CAST(? AS regclass), ?, true)", []any{st.Name, st.To}
}

// ResourceIndexSQL are the statements that move each repository's numbers:
// existing counters move by ResourceIndexAdvance, and a repository without
// one gets one past its highest row, so its first number after the
// recovery is not one handed out after L.
func ResourceIndexSQL(index, rows, group string) []string {
	return []string{
		fmt.Sprintf("UPDATE %s SET max_index = max_index + %d", index, ResourceIndexAdvance),
		fmt.Sprintf(`INSERT INTO %[1]s (group_id, max_index) SELECT r.id, COALESCE((SELECT MAX(t."index") FROM %[2]s t WHERE t.%[3]s = r.id), 0) + %[4]d FROM repository r WHERE r.id NOT IN (SELECT group_id FROM %[1]s)`,
			index, rows, group, ResourceIndexAdvance),
	}
}

func listSequences(ctx context.Context) ([]Sequence, error) {
	e := db.GetEngine(ctx)
	var rows []map[string][]byte
	var err error
	switch {
	case setting.Database.Type.IsPostgreSQL():
		rows, err = e.Query(`SELECT quote_ident(schemaname) || '.' || quote_ident(sequencename) AS name, data_type::text AS type,
			COALESCE(last_value, start_value - 1)::text AS last, max_value::text AS max
			FROM pg_sequences WHERE schemaname = current_schema() ORDER BY sequencename`)
	case setting.Database.Type.IsSQLite3():
		rows, err = e.Query("SELECT name, 'integer' AS type, CAST(seq AS TEXT) AS last, '" + strconv.FormatInt(math.MaxInt64, 10) + "' AS max FROM sqlite_sequence ORDER BY name")
	default:
		return nil, fmt.Errorf("advancing the sequences of %s is not supported", setting.Database.Type)
	}
	if err != nil {
		return nil, err
	}
	out := make([]Sequence, 0, len(rows))
	for _, r := range rows {
		s := Sequence{Name: string(r["name"]), DataType: string(r["type"])}
		if s.Last, err = strconv.ParseInt(string(r["last"]), 10, 64); err != nil {
			return nil, fmt.Errorf("sequence %s: %w", s.Name, err)
		}
		if s.Max, err = strconv.ParseInt(string(r["max"]), 10, 64); err != nil {
			return nil, fmt.Errorf("sequence %s: %w", s.Name, err)
		}
		out = append(out, s)
	}
	return out, nil
}

// AdvanceIdentifiers advances every sequence of the database and every
// repository's numbers, once for the recovery marker names (a resumed
// recovery finds the marker and does nothing). It reports whether it
// advanced them now.
func AdvanceIdentifiers(ctx context.Context, marker string, logf func(format string, args ...any)) (bool, error) {
	if len(marker) > 64 {
		return false, fmt.Errorf("recovery marker %q is longer than 64 characters", marker)
	}
	advanced := false
	err := db.WithTx(ctx, func(ctx context.Context) error {
		cur, err := GetCursor(ctx, CursorRecoveryAdvance)
		if err != nil {
			return err
		}
		if cur.Lineage == marker {
			logf("identifiers: already advanced for this recovery")
			return nil
		}
		seqs, err := listSequences(ctx)
		if err != nil {
			return err
		}
		plan, err := PlanSequenceAdvance(seqs)
		if err != nil {
			return err
		}
		e := db.GetEngine(ctx)
		for _, st := range plan {
			q, args := SequenceSQL(setting.Database.Type, st)
			if _, err := e.Exec(append([]any{q}, args...)...); err != nil {
				return fmt.Errorf("advancing %s: %w", st.Name, err)
			}
			if st.Narrow {
				logf("identifiers: %s advanced from %d to %d only: its type leaves less room than %d", st.Name, st.From, st.To, SequenceAdvance)
			} else {
				logf("identifiers: %s advanced from %d to %d", st.Name, st.From, st.To)
			}
		}
		for _, ri := range resourceIndexes {
			ok, err := e.IsTableExist(ri.Index)
			if err != nil {
				return err
			}
			if !ok {
				continue
			}
			for _, q := range ResourceIndexSQL(ri.Index, ri.Rows, ri.Group) {
				if _, err := e.Exec(q); err != nil {
					return fmt.Errorf("advancing %s: %w", ri.Index, err)
				}
			}
			logf("identifiers: every repository's %s advanced by %d", ri.Index, ResourceIndexAdvance)
		}
		advanced = true
		return SetCursor(ctx, CursorRecoveryAdvance, Cursor{Lineage: marker})
	})
	return advanced, err
}
