// Copyright 2026 The Forgejo Authors. All rights reserved.
// SPDX-License-Identifier: GPL-3.0-or-later

package cluster

import (
	"context"
	"testing"

	"forgejo.org/models/db"
	"forgejo.org/models/unittest"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCursorCompareAndSet(t *testing.T) {
	require.NoError(t, unittest.PrepareTestDatabase())
	ctx := db.DefaultContext

	c, err := GetCursor(ctx, "dispatch@x/1")
	require.NoError(t, err)
	assert.Empty(t, c.Lineage)

	next := Cursor{Lineage: "x/1", Position: 10}
	require.NoError(t, AdvanceCursor(ctx, "dispatch@x/1", *c, next))
	c, err = GetCursor(ctx, "dispatch@x/1")
	require.NoError(t, err)
	assert.EqualValues(t, 10, c.Position)

	// A dispatcher that read the old cursor loses.
	require.ErrorIs(t, AdvanceCursor(ctx, "dispatch@x/1", Cursor{}, Cursor{Lineage: "x/1", Position: 5}), ErrCursorMoved)
	require.ErrorIs(t, AdvanceCursor(ctx, "dispatch@x/1", Cursor{Lineage: "x/1", Position: 9}, Cursor{Lineage: "x/1", Position: 12}), ErrCursorMoved)

	// The cursor commits with the work of its transaction, or not at all.
	err = db.WithTx(ctx, func(ctx2 context.Context) error {
		if err := AdvanceCursor(ctx2, "dispatch@x/1", next, Cursor{Lineage: "x/1", Position: 20}); err != nil {
			return err
		}
		return assert.AnError
	})
	require.ErrorIs(t, err, assert.AnError)
	c, _ = GetCursor(ctx, "dispatch@x/1")
	assert.EqualValues(t, 10, c.Position)

	require.NoError(t, AdvanceCursor(ctx, "dispatch@x/1", next, Cursor{Lineage: "x/1", Position: 20}))
	c, _ = GetCursor(ctx, "dispatch@x/1")
	assert.EqualValues(t, 20, c.Position)
}

func TestOperatorGrants(t *testing.T) {
	require.NoError(t, unittest.PrepareTestDatabase())
	ctx := db.DefaultContext
	ok, err := IsOperator(ctx, 1)
	require.NoError(t, err)
	assert.False(t, ok)
	require.NoError(t, AddOperator(ctx, 1, 0, ""))
	require.NoError(t, AddOperator(ctx, 1, 0, ""))
	require.NoError(t, AddOperator(ctx, 2, 1, "sub-2"))
	n, err := CountOperators(ctx)
	require.NoError(t, err)
	assert.EqualValues(t, 2, n)
	ops, err := ListOperators(ctx)
	require.NoError(t, err)
	require.Len(t, ops, 2)
	assert.EqualValues(t, 1, ops[1].GrantedBy+ops[0].GrantedBy)
	o, has, err := GetOperator(ctx, 2)
	require.NoError(t, err)
	require.True(t, has)
	assert.Equal(t, "sub-2", o.OIDCSubject)
	require.NoError(t, PinOperatorSubject(ctx, 1, "sub-1"))
	o, _, _ = GetOperator(ctx, 1)
	assert.Equal(t, "sub-1", o.OIDCSubject)
	require.Error(t, PinOperatorSubject(ctx, 9, "x"), "not an operator")

	require.NoError(t, RemoveOperatorUnlessLast(ctx, 2))
	ok, _ = IsOperator(ctx, 2)
	assert.False(t, ok)
	require.ErrorIs(t, RemoveOperatorUnlessLast(ctx, 1), ErrLastOperator)
	require.NoError(t, RemoveOperatorUnlessLast(ctx, 2), "not held: nothing to do")
	require.NoError(t, RemoveOperator(ctx, 1), "a node may withdraw the last one")
}

// Two operators withdrawing each other at once never leave nobody.
func TestRemoveOperatorUnlessLastIsAtomic(t *testing.T) {
	require.NoError(t, unittest.PrepareTestDatabase())
	ctx := db.DefaultContext
	require.NoError(t, AddOperator(ctx, 1, 0, ""))
	require.NoError(t, AddOperator(ctx, 2, 0, ""))
	errs := make(chan error, 2)
	for _, id := range []int64{1, 2} {
		go func() { errs <- RemoveOperatorUnlessLast(ctx, id) }()
	}
	e1, e2 := <-errs, <-errs
	n, err := CountOperators(ctx)
	require.NoError(t, err)
	assert.EqualValues(t, 1, n, "one withdrawal is refused: %v / %v", e1, e2)
}
