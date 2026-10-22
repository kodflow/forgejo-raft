// Copyright 2026 The Forgejo Authors. All rights reserved.
// SPDX-License-Identifier: GPL-3.0-or-later

package clusterhook

import (
	"context"
	"errors"
	"strings"
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
	assert.Equal(t, ServerBudget(), Budget(OpPrepared))
	assert.Less(t, Budget(OpPrepared), DefaultTimeout)
	// [cluster] HOOK_TIMEOUT moves the push path's budget, always below
	// the hook's own.
	SetHookTimeout(5 * time.Minute)
	defer SetHookTimeout(0)
	assert.Equal(t, 5*time.Minute, HookTimeout())
	assert.Equal(t, 5*time.Minute-10*time.Second, Budget(OpPrepared))
	for _, op := range []string{OpImport, OpReconcile, OpObjectsFill, OpSnapshot, OpReproject} {
		assert.Greater(t, Budget(op), 10*time.Minute, op)
		assert.Less(t, Budget(op), AdminTimeout, op)
	}
}

// git accepts Unicode spaces in a ref name and writes the line byte for
// byte: the name is the rest of the line after the second ASCII space.
func TestParseLinesKeepsTheRefNameByteForByte(t *testing.T) {
	old, nw := strings.Repeat("0", 40), strings.Repeat("a", 40)
	for _, name := range []string{
		"refs/heads/main x", "refs/heads/feature　x", "refs/heads/a\u0085b",
		"refs/heads/a b", "refs/heads/a b", "refs/heads/trailing ",
	} {
		lines, err := ParseLines(strings.NewReader(old + " " + nw + " " + name + "\n"))
		require.NoError(t, err, "%q", name)
		require.Len(t, lines, 1, "%q", name)
		assert.Equal(t, Line{Old: old, New: nw, Ref: name}, lines[0])
	}
}

// A line that cannot be read fails the hook: skipping it would let git
// write a ref the cluster never saw.
func TestParseLinesFailsClosed(t *testing.T) {
	old, nw := strings.Repeat("0", 40), strings.Repeat("a", 40)
	for _, in := range []string{
		old + " " + nw + "\n",
		old + "\n",
		old + " " + nw + " refs/heads/a\xffb\n",
		old + "  refs/heads/a\n",
		old + " " + nw + " refs/heads/" + strings.Repeat("x", 2<<20) + "\n",
	} {
		_, err := ParseLines(strings.NewReader(in))
		assert.Error(t, err, "%.80q", in)
	}
}

// Two ref transactions of one git process (git before 2.54 runs one per ref
// of a non-atomic push) carry different nonces in the table; the same lines
// give the same nonce whatever their order.
func TestTxnNoncePerTransaction(t *testing.T) {
	old, nw := strings.Repeat("0", 40), strings.Repeat("a", 40)
	a := []Line{{Old: old, New: nw, Ref: "refs/heads/a"}}
	b := []Line{{Old: old, New: nw, Ref: "refs/heads/b"}}
	assert.NotEqual(t, TxnNonce("n", a), TxnNonce("n", b))
	assert.Equal(t, TxnNonce("n", append(append([]Line{}, a...), b...)), TxnNonce("n", append(append([]Line{}, b...), a...)))
	assert.True(t, strings.HasPrefix(TxnNonce("n", a), "n."))
	assert.Empty(t, TxnNonce("", a))
}

// The creation asks the cluster whether it takes the repository, and its
// wiki, before git init writes anything (#54).
func TestCheckRepoKey(t *testing.T) {
	require.NoError(t, CheckRepoKey("u", ".profile"), "no cluster, no check")
	var asked []string
	SetKeyCheck(func(key string) error {
		asked = append(asked, key)
		if strings.Contains(key, "/.") {
			return errors.New("refused")
		}
		return nil
	})
	defer SetKeyCheck(nil)
	require.Error(t, CheckRepoKey("User4", ".profile"))
	require.NoError(t, CheckRepoKey("User4", "Demo"))
	assert.Equal(t, []string{"user4/.profile.git", "user4/demo.git", "user4/demo.wiki.git"}, asked)
}
