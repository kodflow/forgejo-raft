// Copyright 2026 The Forgejo Authors. All rights reserved.
// SPDX-License-Identifier: GPL-3.0-or-later

package cluster

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"forgejo.org/modules/clusterhook"
	"forgejo.org/modules/test"
	"forgejo.org/services/cluster/consensus"
	"forgejo.org/services/cluster/reftable"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// unknownEngine applies a proposal and then answers OUTCOME_UNKNOWN, as a
// leader whose context ended after the append; its barriers fail until
// barrierFrom, never when it is zero. With checked, a barrier before the
// first proposal passes: the proposer's check of its table, which a rename
// makes before it proposes (#220), found a leader.
type unknownEngine struct {
	consensus.Engine
	table       *reftable.Table
	barrierFrom time.Time
	checked     bool
	// lost drops the proposal instead of applying it: the outcome stays
	// unknown, and a commit boundary shows it did not land.
	lost  bool
	mu    sync.Mutex
	index uint64
}

func (e *unknownEngine) token() consensus.Token {
	return consensus.Token{Cluster: consensus.ClusterID{0x46}, Epoch: 1, Term: 1, Index: e.index}
}

func (e *unknownEngine) Propose(_ context.Context, data []byte) (consensus.Token, any, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.index++
	if e.lost {
		return e.token(), nil, consensus.ErrOutcomeUnknown
	}
	e.table.Apply(consensus.Entry{Term: 1, Index: e.index, Kind: consensus.EntryCommand, Data: data, Token: e.token()})
	return e.token(), nil, consensus.ErrOutcomeUnknown
}

func (e *unknownEngine) Barrier(context.Context) (consensus.Token, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.checked && e.index == 0 {
		return e.token(), nil
	}
	if e.barrierFrom.IsZero() || time.Now().Before(e.barrierFrom) {
		return consensus.Token{}, consensus.ErrLeadershipUnconfirmed
	}
	return e.token(), nil
}

func (e *unknownEngine) WaitApplied(context.Context, consensus.Token) error { return nil }

func (e *unknownEngine) Membership() consensus.Membership { return consensus.Membership{} }

// A rename whose proposal's outcome is unknown is never reported as a
// refusal while it may have committed: past the request's budget it is
// resolved at a commit boundary, and when none comes in time the caller is
// told ErrOutcomeUnresolved, which forbids moving the directories back
// (#46, TestHuntR4ProveRenameUnknownOutcome).
func TestARenameWithAnUnknownOutcomeIsNotReportedRefused(t *testing.T) {
	moves := [][2]string{{"o/a.git", "o/b.git"}, {"o/a.wiki.git", "o/b.wiki.git"}}

	// The client went away and the request's budget ended before a leader
	// confirmed: the rename committed, and is reported so.
	r := newReplicator(t.TempDir(), newRegistry(0, 1, 1))
	r.setEngine(&unknownEngine{table: r.table, checked: true, barrierFrom: time.Now().Add(600 * time.Millisecond)})
	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()
	require.NoError(t, r.proposeRepoMoves(ctx, moves))
	from, _, ok := r.table.RenamedFrom("o/b.git")
	assert.True(t, ok)
	assert.Equal(t, "o/a.git", from)

	// No commit boundary within the budget: unresolved, never refused.
	defer test.MockVariableValue(&lifecycleResolveBudget, 300*time.Millisecond)()
	r = newReplicator(t.TempDir(), newRegistry(0, 1, 1))
	r.setEngine(&unknownEngine{table: r.table, checked: true})
	ctx, cancel = context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	err := r.proposeRepoMoves(ctx, moves)
	require.Error(t, err)
	assert.True(t, clusterhook.IsOutcomeUnresolved(err), "%v", err)
	assert.True(t, errors.Is(err, consensus.ErrOutcomeUnknown))
}

// A rename whose outcome was unknown, then shown at a commit boundary not
// to have landed, is a refusal: the caller moves the directories back, and
// the user's transaction rolls back. Reported unresolved, both kept the
// directories at their new paths (CodeRabbit on #120).
func TestARenameShownLostAtACommitBoundaryIsRefused(t *testing.T) {
	moves := [][2]string{{"o/a.git", "o/b.git"}}
	r := newReplicator(t.TempDir(), newRegistry(0, 1, 1))
	r.setEngine(&unknownEngine{table: r.table, lost: true, checked: true, barrierFrom: time.Now().Add(400 * time.Millisecond)})
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	err := r.proposeRepoMoves(ctx, moves)
	require.Error(t, err)
	assert.False(t, clusterhook.IsOutcomeUnresolved(err), "%v", err)
	_, _, ok := r.table.RenamedFrom("o/b.git")
	assert.False(t, ok)
}

// A rename on a node that cannot confirm it applied the log is refused for
// certain, before it is proposed: its source is read on the confirmed
// table only, as an owner's keys are, and the caller moves the directories
// back (#220).
func TestARenameOnANodeThatCannotConfirmIsRefusedBeforeItIsProposed(t *testing.T) {
	defer test.MockVariableValue(&lifecycleResolveBudget, 300*time.Millisecond)()
	r := newReplicator(t.TempDir(), newRegistry(0, 1, 1))
	eng := &unknownEngine{table: r.table}
	r.setEngine(eng)
	err := r.proposeRepoMoves(context.Background(), [][2]string{{"o/a.git", "o/b.git"}})
	require.ErrorContains(t, err, "could not confirm it applied the log")
	assert.False(t, clusterhook.IsOutcomeUnresolved(err), "%v", err)
	assert.Zero(t, eng.index, "nothing was proposed")
}
