// Copyright 2026 The Forgejo Authors. All rights reserved.
// SPDX-License-Identifier: GPL-3.0-or-later

package cluster

import (
	"context"
	"fmt"
	"strconv"
	"sync"
	"testing"
	"time"

	"forgejo.org/modules/clusterhook"
	"forgejo.org/services/cluster/consensus"
	"forgejo.org/services/cluster/reftable"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// cutOffEngine is the engine of a node cut off from the majority right
// after its proposal was appended and replicated: the majority commits it,
// unless it was lost, then commits others ref transactions of other
// nodes' pushes, and whatever meanwhile applies, while this node cannot
// reach a leader. This node learns them all when the partition heals,
// heal after the proposal returned, before its first commit boundary
// since. A later proposal is answered by the leader, or by later when set.
type cutOffEngine struct {
	consensus.Engine
	table     *reftable.Table
	others    int
	heal      time.Duration
	healAt    time.Time
	lost      bool
	meanwhile func(e *cutOffEngine)
	later     error
	mu        sync.Mutex
	epoch     uint64
	index     uint64
	proposals int
	pushed    int
}

func (e *cutOffEngine) tok() consensus.Token {
	return consensus.Token{Cluster: consensus.ClusterID{0x46}, Epoch: 1 + e.epoch, Term: 2, Index: e.index}
}

func (e *cutOffEngine) apply(data []byte) reftable.Result {
	e.index++
	return e.table.Apply(consensus.Entry{Term: 2, Index: e.index, Kind: consensus.EntryCommand, Data: data, Token: e.tok()}).(reftable.Result)
}

// applyTxn applies a ref transaction other nodes committed, which must be
// accepted.
func (e *cutOffEngine) applyTxn(tx *reftable.RefTxn) {
	data, err := reftable.EncodeVersion(e.table.WriteVersion(), reftable.KindRefTxn, tx)
	if err != nil {
		panic(err)
	}
	if res := e.apply(data); !res.OK {
		panic(res.Reason)
	}
}

// push applies n pushes of other nodes into other repositories, one nonce
// each, spread over a few keys: Apply reads every live ref of the key it
// writes.
func (e *cutOffEngine) push(n int) {
	for range n {
		e.pushed++
		i := strconv.Itoa(e.pushed)
		e.applyTxn(&reftable.RefTxn{
			Repo: "p/busy" + strconv.Itoa(e.pushed%64) + ".git", RepoID: 9, Format: "sha1", PusherID: 3, Time: 1, Nonce: "push-" + i,
			Updates: []reftable.RefUpdate{{Ref: "refs/heads/b" + i, New: "1111111111111111111111111111111111111111"}},
		})
	}
}

func (e *cutOffEngine) Propose(_ context.Context, data []byte) (consensus.Token, any, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.proposals++
	if e.proposals > 1 {
		if e.later != nil {
			return consensus.Token{}, nil, e.later
		}
		res := e.apply(data)
		return e.tok(), res, nil
	}
	if !e.lost {
		e.apply(data) // committed by the majority
	}
	e.push(e.others)
	if e.meanwhile != nil {
		e.meanwhile(e)
	}
	e.healAt = time.Now().Add(e.heal)
	return e.tok(), nil, consensus.ErrOutcomeUnknown
}

func (e *cutOffEngine) Barrier(context.Context) (consensus.Token, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.proposals > 0 && time.Now().Before(e.healAt) {
		return consensus.Token{}, consensus.ErrLeadershipUnconfirmed
	}
	return e.tok(), nil
}

func (e *cutOffEngine) WaitApplied(context.Context, consensus.Token) error { return nil }

func (e *cutOffEngine) Membership() consensus.Membership { return consensus.Membership{} }

// count is how many proposals the engine received.
func (e *cutOffEngine) count() int {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.proposals
}

// cutOff returns a replicator whose engine is a cutOffEngine, the cluster
// raised to payload version v, o/a.git and its wiki created.
func cutOff(t *testing.T, v uint8, others int, heal time.Duration) (*Replicator, *cutOffEngine) {
	t.Helper()
	r := newReplicator(t.TempDir(), newRegistry(0, 1, 1))
	e := &cutOffEngine{table: r.table, others: others, heal: heal}
	data, err := reftable.Encode(reftable.KindAudit, &reftable.Audit{ID: "raise", Operator: "op", Action: reftable.AuditPayloadVersion, Target: strconv.Itoa(int(v)), Outcome: reftable.AuditRaised, Time: 1})
	require.NoError(t, err)
	require.True(t, e.apply(data).OK)
	require.Equal(t, v, r.table.WriteVersion())
	for _, key := range []string{"o/a.git", "o/a.wiki.git"} {
		e.applyTxn(&reftable.RefTxn{Repo: key, RepoID: 7, Format: "sha1", Nonce: "init-" + key, Init: true, Updates: []reftable.RefUpdate{{Ref: "HEAD", Symbolic: "refs/heads/main"}}})
		e.applyTxn(&reftable.RefTxn{Repo: key, RepoID: 7, Format: "sha1", Nonce: "main-" + key, Updates: []reftable.RefUpdate{{Ref: "refs/heads/main", New: "2222222222222222222222222222222222222222"}}})
	}
	r.setEngine(e)
	return r, e
}

// A repository's rename whose outcome is unknown was settled by its nonce
// alone, and the table keeps the newest NoncesKept nonces of every kind,
// one per push: when more pushes than that committed between the rename
// and the commit boundary that settled it, its nonce was gone, the rename
// that committed was proposed again, refused, and answered as a certain
// refusal. Forgejo moved the directories back and never wrote the new name
// in SQL; the table held the repository under the new key, SQL under the
// old one, and every later rename or transfer of the old name was refused
// for good (#220's check, payload version 9). Past the ring, the rename is
// told by its effect on the table, and is not proposed again (#234).
func TestACommittedRenameWhoseNonceWasDroppedIsNotRefused(t *testing.T) {
	moves := [][2]string{{"o/a.git", "o/b.git"}, {"o/a.wiki.git", "o/b.wiki.git"}}
	for _, others := range []int{reftable.NoncesKept - 1, reftable.NoncesKept} {
		t.Run(fmt.Sprintf("%d pushes meanwhile", others), func(t *testing.T) {
			// The partition heals as the proposal returns: the first commit
			// boundary this node asks for passes, after it applied the
			// pushes the majority committed meanwhile.
			r, eng := cutOff(t, reftable.PayloadVersion, others, 0)
			ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
			defer cancel()

			err := r.proposeRepoMoves(ctx, moves)

			from, _, renamed := r.table.RenamedFrom("o/b.git")
			require.True(t, renamed, "the rename committed")
			assert.Equal(t, "o/a.git", from)
			require.NoError(t, err, "the rename committed: it must not be answered as refused")
			assert.Equal(t, 1, eng.count(), "settled from the table, not proposed again")
		})
	}
}

// The same for a transfer, a rename to another owner, for an owner's
// rename, at payload version 7, where it lists its moves, and from
// OwnerRenameVersion, where it names the two owners, and for a deletion
// (#234).
func TestACommittedTransferOwnerRenameOrDeletionWhoseNonceWasDroppedIsNotRefused(t *testing.T) {
	t.Run("deletion", func(t *testing.T) {
		r, eng := cutOff(t, reftable.PayloadVersion, reftable.NoncesKept, 0)
		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
		defer cancel()
		require.NoError(t, r.proposeRepoDelete(ctx, "o/a.git", 7))
		_, deleted := r.table.Deleted("o/a.git")
		assert.True(t, deleted)
		assert.Equal(t, 1, eng.count())
	})
	t.Run("transfer", func(t *testing.T) {
		r, eng := cutOff(t, reftable.PayloadVersion, reftable.NoncesKept, 0)
		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
		defer cancel()
		require.NoError(t, r.proposeRepoMoves(ctx, [][2]string{{"o/a.git", "q/a.git"}, {"o/a.wiki.git", "q/a.wiki.git"}}))
		from, _, renamed := r.table.RenamedFrom("q/a.git")
		require.True(t, renamed)
		assert.Equal(t, "o/a.git", from)
		assert.Equal(t, 1, eng.count())
	})
	for _, v := range []uint8{reftable.OwnerRenameVersion - 1, reftable.PayloadVersion} {
		t.Run("owner rename at payload version "+strconv.Itoa(int(v)), func(t *testing.T) {
			r, eng := cutOff(t, v, reftable.NoncesKept, 0)
			ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
			defer cancel()
			require.NoError(t, r.proposeOwnerRename(ctx, "o", "q"))
			for _, key := range []string{"a.git", "a.wiki.git"} {
				from, _, renamed := r.table.RenamedFrom("q/" + key)
				require.True(t, renamed, key)
				assert.Equal(t, "o/"+key, from)
			}
			assert.Empty(t, r.table.OwnerKeys("o"))
			assert.Equal(t, 1, eng.count())
		})
	}
}

// Past the request's budget, a lifecycle command whose outcome is unknown
// is settled at a commit boundary, and one without its nonce was a
// certain refusal, "not applied at a commit boundary", or "no leader in
// time" when the boundary came as the budget ended. A rename, a transfer,
// an owner's rename or a deletion that committed is told by its effect
// instead; a rename that did not, past the ring, stays unresolved: the
// table no longer tells, and the callers keep the directories (#234).
func TestALifecycleCommandSettledPastTheRingIsNeverAFalseRefusal(t *testing.T) {
	moves := [][2]string{{"o/a.git", "o/b.git"}, {"o/a.wiki.git", "o/b.wiki.git"}}
	// The request's budget ends before the partition heals.
	settle := func(t *testing.T, lost bool, others int, op func(context.Context, *Replicator) error) (*Replicator, error) {
		t.Helper()
		r, eng := cutOff(t, reftable.PayloadVersion, others, 600*time.Millisecond)
		eng.lost = lost
		ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
		defer cancel()
		err := op(ctx, r)
		assert.Equal(t, 1, eng.count(), "settled at a commit boundary, not proposed again")
		return r, err
	}

	t.Run("rename", func(t *testing.T) {
		r, err := settle(t, false, reftable.NoncesKept, func(ctx context.Context, r *Replicator) error { return r.proposeRepoMoves(ctx, moves) })
		require.NoError(t, err)
		_, _, renamed := r.table.RenamedFrom("o/b.git")
		assert.True(t, renamed)
	})
	t.Run("transfer", func(t *testing.T) {
		r, err := settle(t, false, reftable.NoncesKept, func(ctx context.Context, r *Replicator) error {
			return r.proposeRepoMoves(ctx, [][2]string{{"o/a.git", "q/a.git"}, {"o/a.wiki.git", "q/a.wiki.git"}})
		})
		require.NoError(t, err)
		_, _, renamed := r.table.RenamedFrom("q/a.git")
		assert.True(t, renamed)
	})
	t.Run("owner rename", func(t *testing.T) {
		r, err := settle(t, false, reftable.NoncesKept, func(ctx context.Context, r *Replicator) error { return r.proposeOwnerRename(ctx, "o", "q") })
		require.NoError(t, err)
		assert.Empty(t, r.table.OwnerKeys("o"))
	})
	t.Run("deletion", func(t *testing.T) {
		r, err := settle(t, false, reftable.NoncesKept, func(ctx context.Context, r *Replicator) error { return r.proposeRepoDelete(ctx, "o/a.git", 7) })
		require.NoError(t, err)
		_, deleted := r.table.Deleted("o/a.git")
		assert.True(t, deleted)
	})
	t.Run("a rename lost stays unresolved", func(t *testing.T) {
		// One more push than the ring holds: a nonce committed after the
		// proposal's start was dropped, which might have been its own.
		r, err := settle(t, true, reftable.NoncesKept+1, func(ctx context.Context, r *Replicator) error { return r.proposeRepoMoves(ctx, moves) })
		require.Error(t, err)
		assert.True(t, clusterhook.IsOutcomeUnresolved(err), "never a certain refusal: %v", err)
		_, _, renamed := r.table.RenamedFrom("o/b.git")
		assert.False(t, renamed)
	})
	t.Run("a rename lost across a change of lineage stays unresolved", func(t *testing.T) {
		// The table moved to another lineage meanwhile: its indexes do not
		// compare with the proposal's start, and the ring tells nothing.
		r, err := settle(t, true, 0, func(ctx context.Context, r *Replicator) error {
			r.Engine().(*cutOffEngine).meanwhile = func(e *cutOffEngine) {
				e.epoch++
				e.push(1)
			}
			return r.proposeRepoMoves(ctx, moves)
		})
		require.Error(t, err)
		assert.True(t, clusterhook.IsOutcomeUnresolved(err), "never a certain refusal: %v", err)
		_, _, renamed := r.table.RenamedFrom("o/b.git")
		assert.False(t, renamed)
	})
	t.Run("a rename lost while the ring covers it is refused", func(t *testing.T) {
		// As many pushes as the ring holds: it dropped only nonces of
		// entries before the proposal's start, and the absence is certain.
		r, err := settle(t, true, reftable.NoncesKept, func(ctx context.Context, r *Replicator) error { return r.proposeRepoMoves(ctx, moves) })
		require.Error(t, err)
		assert.False(t, clusterhook.IsOutcomeUnresolved(err), "a certain refusal: %v", err)
		_, _, renamed := r.table.RenamedFrom("o/b.git")
		assert.False(t, renamed)
	})
}

// A rename that committed, and whose effect the table no longer shows once
// its nonce is gone, is proposed again, refused, and stays unresolved: a
// refusal would have the callers move the directories back under a key
// the table may have moved away (#234).
func TestARenameTheTableNoLongerTellsIsUnresolved(t *testing.T) {
	r, eng := cutOff(t, reftable.PayloadVersion, reftable.NoncesKept, 0)
	eng.meanwhile = func(e *cutOffEngine) {
		data, err := reftable.EncodeVersion(e.table.WriteVersion(), reftable.KindRepoDelete, &reftable.RepoDelete{Repo: "o/b.git", RepoID: 7, Time: 1, Nonce: "gone"})
		if err != nil {
			panic(err)
		}
		if res := e.apply(data); !res.OK {
			panic(res.Reason)
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	err := r.proposeRepoMoves(ctx, [][2]string{{"o/a.git", "o/b.git"}, {"o/a.wiki.git", "o/b.wiki.git"}})
	require.Error(t, err)
	assert.True(t, clusterhook.IsOutcomeUnresolved(err), "never a certain refusal: %v", err)
	assert.Equal(t, 2, eng.count(), "proposed again, and refused")
}

// A rename lost with its proposal, past the ring, is proposed again and
// committed: the second attempt settles it (#234).
func TestALostRenamePastTheRingIsProposedAgain(t *testing.T) {
	r, eng := cutOff(t, reftable.PayloadVersion, reftable.NoncesKept+1, 0)
	eng.lost = true
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	require.NoError(t, r.proposeRepoMoves(ctx, [][2]string{{"o/a.git", "o/b.git"}, {"o/a.wiki.git", "o/b.wiki.git"}}))
	from, _, renamed := r.table.RenamedFrom("o/b.git")
	require.True(t, renamed)
	assert.Equal(t, "o/a.git", from)
	assert.Equal(t, 2, eng.count())
}

// A deletion past the ring whose effect the table no longer shows is not
// proposed again: Apply takes a deletion's copy whether or not the first
// one committed, and once it did, the key may hold a repository created
// again since, the database having let the name go before the deletion
// was proposed. The outcome stays unresolved, and the new repository
// keeps its refs (#234).
func TestADeletionTheTableNoLongerTellsIsNotProposedAgain(t *testing.T) {
	r, eng := cutOff(t, reftable.PayloadVersion, reftable.NoncesKept, 0)
	eng.meanwhile = func(e *cutOffEngine) {
		// Another node creates o/a again, git init's HEAD.
		head, _ := e.table.Get("o/a.git", "HEAD")
		e.applyTxn(&reftable.RefTxn{Repo: "o/a.git", RepoID: 8, Format: "sha1", Nonce: "again", Init: true, Updates: []reftable.RefUpdate{{Ref: "HEAD", Symbolic: "refs/heads/main", ExpectRev: head.Rev}}})
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	err := r.proposeRepoDelete(ctx, "o/a.git", 7)
	require.Error(t, err)
	assert.True(t, clusterhook.IsOutcomeUnresolved(err), "never a certain refusal: %v", err)
	assert.Equal(t, 1, eng.count(), "not proposed again")
	head, _ := r.table.Get("o/a.git", "HEAD")
	assert.True(t, head.Live(), "the repository created again keeps its refs")
}

// A rename past the ring whose effect the table no longer shows, proposed
// again, stays unresolved when the engine then fails for certain or finds
// no leader in time: the first attempt may have committed (#234).
func TestARenameStillOpenIsUnresolvedWhenTheCopyFails(t *testing.T) {
	for _, later := range []error{consensus.ErrNoSpace, consensus.ErrNotLeader} {
		t.Run(later.Error(), func(t *testing.T) {
			r, eng := cutOff(t, reftable.PayloadVersion, reftable.NoncesKept, 0)
			eng.later = later
			eng.meanwhile = func(e *cutOffEngine) {
				data, err := reftable.EncodeVersion(e.table.WriteVersion(), reftable.KindRepoDelete, &reftable.RepoDelete{Repo: "o/b.git", RepoID: 7, Time: 1, Nonce: "gone"})
				if err != nil {
					panic(err)
				}
				if res := e.apply(data); !res.OK {
					panic(res.Reason)
				}
			}
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()
			err := r.proposeRepoMoves(ctx, [][2]string{{"o/a.git", "o/b.git"}, {"o/a.wiki.git", "o/b.wiki.git"}})
			require.ErrorIs(t, err, later)
			assert.True(t, clusterhook.IsOutcomeUnresolved(err), "never a certain refusal: %v", err)
			assert.GreaterOrEqual(t, eng.count(), 2, "proposed again")
		})
	}
}

// A push whose outcome is unknown, committed, whose ref another push moved
// since, and whose nonce the ring dropped before the commit boundary that
// settles it, was answered "the cluster refused the write": its values no
// longer showed it, and its copy was refused. It is answered as unknown
// (#234).
func TestACommittedPushWhoseNonceWasDroppedIsNotRefused(t *testing.T) {
	for _, others := range []int{reftable.NoncesKept - 2, reftable.NoncesKept} {
		t.Run(fmt.Sprintf("%d pushes meanwhile", others), func(t *testing.T) {
			r, eng := cutOff(t, reftable.PayloadVersion, others, 0)
			cur, ok := r.table.Get("o/a.git", "refs/heads/main")
			require.True(t, ok)
			mine := &reftable.RefTxn{
				Repo: "o/a.git", RepoID: 7, Format: "sha1", PusherID: 3, Time: 1, Push: true, Nonce: "mine",
				Updates: []reftable.RefUpdate{{Ref: "refs/heads/main", Old: cur.SHA, New: "3333333333333333333333333333333333333333", ExpectRev: cur.Rev}},
			}
			eng.meanwhile = func(e *cutOffEngine) {
				// Another node's push moves the branch on.
				e.applyTxn(&reftable.RefTxn{
					Repo: "o/a.git", RepoID: 7, Format: "sha1", PusherID: 4, Time: 2, Push: true, Nonce: "theirs",
					Updates: []reftable.RefUpdate{{Ref: "refs/heads/main", Old: mine.Updates[0].New, New: "4444444444444444444444444444444444444444", ExpectRev: cur.Rev + 1}},
				})
			}
			data, err := reftable.EncodeVersion(r.table.WriteVersion(), reftable.KindRefTxn, mine)
			require.NoError(t, err)
			ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
			defer cancel()

			resp := r.proposeWithRetry(ctx, "o/a.git", "proc", mine, data)

			_, kept := r.table.CommittedNonce("mine")
			if others < reftable.NoncesKept {
				require.True(t, kept)
				assert.True(t, resp.OK, resp.Message)
				return
			}
			require.False(t, kept, "the push's nonce was dropped")
			require.False(t, resp.OK)
			assert.NotContains(t, resp.Message, "the cluster refused the write")
			assert.Contains(t, resp.Message, "the outcome of the write is unknown")
		})
	}
}

// A push lost with its proposal, past the ring, is proposed again and
// committed; one that committed, whose copy the engine then fails for
// certain, is answered as unknown, not as refused (#234).
func TestAPushPastTheRingIsProposedAgain(t *testing.T) {
	for _, lost := range []bool{true, false} {
		t.Run("lost "+strconv.FormatBool(lost), func(t *testing.T) {
			r, eng := cutOff(t, reftable.PayloadVersion, reftable.NoncesKept+1, 0)
			eng.lost = lost
			cur, ok := r.table.Get("o/a.git", "refs/heads/main")
			require.True(t, ok)
			mine := &reftable.RefTxn{
				Repo: "o/a.git", RepoID: 7, Format: "sha1", PusherID: 3, Time: 1, Push: true, Nonce: "mine",
				Updates: []reftable.RefUpdate{{Ref: "refs/heads/main", Old: cur.SHA, New: "3333333333333333333333333333333333333333", ExpectRev: cur.Rev}},
			}
			if !lost {
				eng.later = consensus.ErrNoSpace
				eng.meanwhile = func(e *cutOffEngine) {
					e.applyTxn(&reftable.RefTxn{
						Repo: "o/a.git", RepoID: 7, Format: "sha1", PusherID: 4, Time: 2, Push: true, Nonce: "theirs",
						Updates: []reftable.RefUpdate{{Ref: "refs/heads/main", Old: mine.Updates[0].New, New: "4444444444444444444444444444444444444444", ExpectRev: cur.Rev + 1}},
					})
				}
			}
			data, err := reftable.EncodeVersion(r.table.WriteVersion(), reftable.KindRefTxn, mine)
			require.NoError(t, err)
			ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
			defer cancel()

			resp := r.proposeWithRetry(ctx, "o/a.git", "proc", mine, data)

			assert.Equal(t, 2, eng.count(), "proposed again")
			if lost {
				require.True(t, resp.OK, resp.Message)
				v, _ := r.table.Get("o/a.git", "refs/heads/main")
				assert.Equal(t, mine.Updates[0].New, v.SHA)
				return
			}
			require.False(t, resp.OK)
			assert.Contains(t, resp.Message, "the outcome of the write is unknown")
		})
	}
}
