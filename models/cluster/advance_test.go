// Copyright 2026 The Forgejo Authors. All rights reserved.
// SPDX-License-Identifier: GPL-3.0-or-later

package cluster

import (
	"math"
	"testing"

	"forgejo.org/models/db"
	"forgejo.org/models/unittest"
	"forgejo.org/modules/setting"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Finding 5: every sequence moves by SequenceAdvance; a narrow one moves
// half of what it has left; one at its end stops the recovery.
func TestPlanSequenceAdvance(t *testing.T) {
	plan, err := PlanSequenceAdvance([]Sequence{
		{Name: "public.repository_id_seq", DataType: "bigint", Last: 41, Max: math.MaxInt64},
		{Name: "public.small_id_seq", DataType: "integer", Last: 2_000_000_000, Max: math.MaxInt32},
		{Name: "public.unused_id_seq", DataType: "bigint", Last: 0, Max: math.MaxInt64},
	})
	require.NoError(t, err)
	require.Len(t, plan, 3)
	assert.Equal(t, SequenceStep{Name: "public.repository_id_seq", From: 41, To: 41 + SequenceAdvance}, plan[0])
	assert.True(t, plan[1].Narrow)
	assert.Greater(t, plan[1].To, plan[1].From)
	assert.LessOrEqual(t, plan[1].To, int64(math.MaxInt32))
	assert.Equal(t, SequenceAdvance, plan[2].To, "a sequence that handed nothing out at L moves too")

	_, err = PlanSequenceAdvance([]Sequence{{Name: "public.full_seq", DataType: "integer", Last: math.MaxInt32 - 1, Max: math.MaxInt32}})
	require.Error(t, err, "a sequence that cannot move stops the recovery")
}

func TestSequenceSQL(t *testing.T) {
	st := SequenceStep{Name: `public."issue_id_seq"`, From: 7, To: 7 + SequenceAdvance}
	q, args := SequenceSQL(setting.DatabaseType("postgres"), st)
	assert.Equal(t, "SELECT setval(CAST(? AS regclass), ?, true)", q)
	assert.Equal(t, []any{`public."issue_id_seq"`, 7 + SequenceAdvance}, args, "the name is a bound parameter, never spliced")
	q, args = SequenceSQL(setting.DatabaseType("sqlite3"), st)
	assert.Equal(t, "UPDATE sqlite_sequence SET seq = ? WHERE name = ? AND seq < ?", q)
	assert.Equal(t, []any{7 + SequenceAdvance, `public."issue_id_seq"`, 7 + SequenceAdvance}, args)

	stmts := ResourceIndexSQL("issue_index", "issue", "repo_id")
	require.Len(t, stmts, 2)
	assert.Equal(t, "UPDATE issue_index SET max_index = max_index + 100000", stmts[0])
	assert.Contains(t, stmts[1], `INSERT INTO issue_index (group_id, max_index) SELECT r.id, COALESCE((SELECT MAX(t."index") FROM issue t WHERE t.repo_id = r.id), 0) + 100000 FROM repository r WHERE r.id NOT IN (SELECT group_id FROM issue_index)`)
}

func maxIndex(t *testing.T, table string, group int64) int64 {
	var v int64
	has, err := db.GetEngine(db.DefaultContext).SQL("SELECT max_index FROM "+table+" WHERE group_id = ?", group).Get(&v)
	require.NoError(t, err)
	require.True(t, has, "%s has a row for %d", table, group)
	return v
}

func sqliteSeq(t *testing.T, name string) int64 {
	var v int64
	has, err := db.GetEngine(db.DefaultContext).SQL("SELECT seq FROM sqlite_sequence WHERE name = ?", name).Get(&v)
	require.NoError(t, err)
	require.True(t, has, "sqlite_sequence has %s", name)
	return v
}

// The identifiers move once per recovery: the issue numbers of every
// repository, those without a counter included, and the IDs.
func TestAdvanceIdentifiersOncePerRecovery(t *testing.T) {
	require.NoError(t, unittest.PrepareTestDatabase())
	if !setting.Database.Type.IsSQLite3() {
		t.Skip("the sequences part is checked on SQLite here; PostgreSQL by TestSequenceSQL")
	}
	ctx := db.DefaultContext
	before := maxIndex(t, "issue_index", 1)
	seqBefore := sqliteSeq(t, "repository")
	var noCounter int64
	has, err := db.GetEngine(ctx).SQL("SELECT id FROM repository WHERE id NOT IN (SELECT group_id FROM issue_index) ORDER BY id LIMIT 1").Get(&noCounter)
	require.NoError(t, err)
	require.True(t, has, "a fixture repository has no issue counter")

	var lines []string
	logf := func(f string, a ...any) { lines = append(lines, f) }
	done, err := AdvanceIdentifiers(ctx, "recovery-1", logf)
	require.NoError(t, err)
	assert.True(t, done)
	assert.Equal(t, before+ResourceIndexAdvance, maxIndex(t, "issue_index", 1))
	assert.Equal(t, ResourceIndexAdvance, maxIndex(t, "issue_index", noCounter), "a repository without a counter gets one past L")
	assert.Equal(t, seqBefore+SequenceAdvance, sqliteSeq(t, "repository"))
	assert.NotEmpty(t, lines, "each move is logged")

	// The next issue number is past anything handed out after L.
	next, err := db.GetNextResourceIndex(ctx, "issue_index", 1)
	require.NoError(t, err)
	assert.Equal(t, before+ResourceIndexAdvance+1, next)

	// Resumed: nothing moves twice.
	done, err = AdvanceIdentifiers(ctx, "recovery-1", logf)
	require.NoError(t, err)
	assert.False(t, done)
	assert.Equal(t, before+ResourceIndexAdvance+1, maxIndex(t, "issue_index", 1))
	assert.Equal(t, seqBefore+SequenceAdvance, sqliteSeq(t, "repository"))
}
