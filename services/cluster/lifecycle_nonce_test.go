// Copyright 2026 The Forgejo Authors. All rights reserved.
// SPDX-License-Identifier: GPL-3.0-or-later

package cluster

import (
	"context"
	"strconv"
	"sync"
	"testing"
	"time"

	"forgejo.org/services/cluster/consensus"
	"forgejo.org/services/cluster/reftable"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// dropEngine applies every proposal, but answers OUTCOME_UNKNOWN to the
// next one without appending it: a leader that died before replicating it.
type dropEngine struct {
	consensus.Engine
	table *reftable.Table
	mu    sync.Mutex
	index uint64
	drop  bool
}

func (e *dropEngine) token() consensus.Token {
	return consensus.Token{Cluster: consensus.ClusterID{0x74}, Epoch: 1, Term: 1, Index: e.index}
}

func (e *dropEngine) Propose(_ context.Context, data []byte) (consensus.Token, any, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.drop {
		e.drop = false
		return e.token(), nil, consensus.ErrOutcomeUnknown
	}
	e.index++
	res := e.table.Apply(consensus.Entry{Term: 1, Index: e.index, Kind: consensus.EntryCommand, Data: data, Token: e.token()})
	return e.token(), res, nil
}

func (e *dropEngine) Barrier(context.Context) (consensus.Token, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.token(), nil
}

func (e *dropEngine) WaitApplied(context.Context, consensus.Token) error { return nil }

func (e *dropEngine) Membership() consensus.Membership { return consensus.Membership{} }

func (e *dropEngine) propose(t *testing.T, v uint8, kind reftable.Kind, body any) {
	t.Helper()
	data, err := reftable.EncodeVersion(v, kind, body)
	require.NoError(t, err)
	_, res, err := e.Propose(context.Background(), data)
	require.NoError(t, err)
	require.True(t, res.(reftable.Result).OK, "%+v", res)
}

// After a round trip alice to org and back, a third transfer whose
// proposal was dropped with an unknown outcome read org's rename mark,
// left by the first one, and was reported recorded with nothing applied
// (#74). Written at version 5, it carries a nonce that only its own entry
// leaves: it is proposed again and applied.
func TestHuntR4ProveRenameLandedReadsStaleMark(t *testing.T) {
	for _, v := range []uint8{reftable.PusherVersion, reftable.GenerationVersion} {
		t.Run("version "+strconv.Itoa(int(v)), func(t *testing.T) {
			r := newReplicator(t.TempDir(), newRegistry(0, 1, 1))
			eng := &dropEngine{table: r.table}
			r.setEngine(eng)
			eng.propose(t, reftable.BaseVersion, reftable.KindAudit, &reftable.Audit{Operator: "op", Action: reftable.AuditPayloadVersion, Target: strconv.Itoa(int(v)), Outcome: reftable.AuditRaised})
			eng.propose(t, v, reftable.KindRefTxn, &reftable.RefTxn{Repo: "alice/x.git", RepoID: 7, Format: "sha1", Updates: []reftable.RefUpdate{{Ref: "refs/heads/main", New: "1111111111111111111111111111111111111111"}}})
			there := [][2]string{{"alice/x.git", "org/x.git"}, {"alice/x.wiki.git", "org/x.wiki.git"}}
			back := [][2]string{{"org/x.git", "alice/x.git"}, {"org/x.wiki.git", "alice/x.wiki.git"}}
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			require.NoError(t, r.proposeRepoMoves(ctx, there))
			require.NoError(t, r.proposeRepoMoves(ctx, back))
			before, _ := r.table.Applied()

			eng.mu.Lock()
			eng.drop = true
			eng.mu.Unlock()
			err := r.proposeRepoMoves(ctx, there)
			after, _ := r.table.Applied()
			main, _ := r.table.Get("org/x.git", "refs/heads/main")
			if v < reftable.GenerationVersion {
				// The defect, kept as a witness: reported recorded, nothing
				// applied.
				require.NoError(t, err)
				assert.Equal(t, before.Index, after.Index)
				assert.False(t, main.Live())
				return
			}
			require.NoError(t, err)
			assert.Greater(t, after.Index, before.Index, "the transfer was proposed again")
			assert.True(t, main.Live(), "the table holds the repository under org/x.git, as the database does")
		})
	}
}

// The same with a deletion: a key deleted, written again and deleted once
// more reads deleted before the third deletion committed.
func TestChaosProjRepeatedRenameUnknownOutcomeReportedCommitted(t *testing.T) {
	r := newReplicator(t.TempDir(), newRegistry(0, 1, 1))
	eng := &dropEngine{table: r.table}
	r.setEngine(eng)
	eng.propose(t, reftable.BaseVersion, reftable.KindAudit, &reftable.Audit{Operator: "op", Action: reftable.AuditPayloadVersion, Target: strconv.Itoa(reftable.GenerationVersion), Outcome: reftable.AuditRaised})
	main := "1111111111111111111111111111111111111111"
	eng.propose(t, reftable.GenerationVersion, reftable.KindRefTxn, &reftable.RefTxn{Repo: "t/a.git", RepoID: 3, Format: "sha1", Updates: []reftable.RefUpdate{{Ref: "refs/heads/main", New: main}}})
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	for i := range 2 {
		require.NoError(t, r.proposeRepoRename(ctx, "t/a.git", "t/b.git"), i)
		require.NoError(t, r.proposeRepoRename(ctx, "t/b.git", "t/a.git"), i)
	}
	eng.mu.Lock()
	eng.drop = true
	eng.mu.Unlock()
	require.NoError(t, r.proposeRepoRename(ctx, "t/a.git", "t/b.git"))
	v, _ := r.table.Get("t/b.git", "refs/heads/main")
	assert.True(t, v.Live(), "the third rename was applied, not taken for the first")
	v, _ = r.table.Get("t/a.git", "refs/heads/main")
	assert.False(t, v.Live())
}
