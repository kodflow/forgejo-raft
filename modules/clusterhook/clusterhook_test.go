// Copyright 2026 The Forgejo Authors. All rights reserved.
// SPDX-License-Identifier: GPL-3.0-or-later

package clusterhook

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// A repository and its wiki are one move request; the undo is the reverse
// request, made even when the failed request's context is gone.
func TestRepoRenamedAndItsUndo(t *testing.T) {
	var calls [][][2]string
	SetRepoLifecycle(nil, func(ctx context.Context, moves [][2]string) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		calls = append(calls, moves)
		return nil
	})
	defer SetRepoLifecycle(nil, nil)

	require.NoError(t, RepoRenamed(context.Background(), "Alice", "Demo", "bob", "demo"))
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	require.NoError(t, UndoRepoRenamed(ctx, "Alice", "Demo", "bob", "demo"))

	require.Len(t, calls, 2)
	assert.Equal(t, [][2]string{{"alice/demo.git", "bob/demo.git"}, {"alice/demo.wiki.git", "bob/demo.wiki.git"}}, calls[0])
	assert.Equal(t, [][2]string{{"bob/demo.git", "alice/demo.git"}, {"bob/demo.wiki.git", "alice/demo.wiki.git"}}, calls[1])
}

// The push path keeps the hook's budget; an operator's command gets its own.
func TestBudgetPerOperation(t *testing.T) {
	assert.Equal(t, ServerBudget, Budget(OpPrepared))
	assert.Less(t, Budget(OpPrepared), DefaultTimeout)
	for _, op := range []string{OpImport, OpReconcile, OpObjectsFill, OpSnapshot, OpReproject} {
		assert.Greater(t, Budget(op), 10*time.Minute, op)
		assert.Less(t, Budget(op), AdminTimeout, op)
	}
}
