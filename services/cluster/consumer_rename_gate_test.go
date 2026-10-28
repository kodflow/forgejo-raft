// Copyright 2026 The Forgejo Authors. All rights reserved.
// SPDX-License-Identifier: GPL-3.0-or-later

package cluster

import (
	"context"
	"errors"
	"maps"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	cluster_model "forgejo.org/models/cluster"
	"forgejo.org/models/db"
	git_model "forgejo.org/models/git"
	repo_model "forgejo.org/models/repo"
	"forgejo.org/models/unittest"
	"forgejo.org/modules/clusterhook"
	"forgejo.org/modules/json"
	"forgejo.org/modules/setting"
	"forgejo.org/modules/test"
	"forgejo.org/services/cluster/consensus"
	"forgejo.org/services/cluster/reftable"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// gateRenames is a table's renames for the gate, by target: the key
// renamed into it and the rename's index.
type gateRenames map[string][]gateMark

func (r gateRenames) after(from string, after uint64) (string, uint64, bool) {
	to, at, found := "", uint64(0), false
	for k, ms := range r {
		for _, m := range ms {
			if m.From == from && m.Index > after && (!found || m.Index < at) {
				to, at, found = k, m.Index, true
			}
		}
	}
	return to, at, found
}

type gateMark struct {
	From  string
	Index uint64
}

// gateConsumer is a consumer whose table renamed o/a.git to o/b.git at 6,
// in testToken's lineage, and whose projector says projected.
func gateConsumer(projected map[string]uint64) (*consumer, *sync.Mutex) {
	var mu sync.Mutex
	r := gateRenames{"o/b.git": {{From: "o/a.git", Index: 6}}}
	c := &consumer{
		projectedAt: func(repo string) uint64 {
			mu.Lock()
			defer mu.Unlock()
			return projected[repo]
		},
		renamedAfter: r.after,
		lineageNow:   func() string { return testToken(1).Identity().String() },
		failures:     map[uint64]int{}, reasons: map[uint64]string{}, seen: map[string]int64{}, lastAck: map[string]uint64{},
		stalledOut: map[string]bool{},
	}
	return c, &mu
}

// An event's repository renamed since is projected, for its work, at the
// key the renames of the table's lineage led it to, from the last of them;
// an event of another lineage keeps its key, the indexes of two lineages
// comparing nothing (#326).
func TestConsumerProjectionGateFollowsTheRenamesOfItsLineage(t *testing.T) {
	r := gateRenames{
		"o/b.git": {{From: "o/a.git", Index: 6}},
		"o/c.git": {{From: "o/b.git", Index: 9}},
		// A rename before the event does not move it.
		"o/z.git": {{From: "o/a.git", Index: 2}},
	}
	c := &consumer{renamedAfter: r.after}
	now := testToken(1).Identity().String()
	key, at := c.projectionGate("o/a.git", 4, now, now)
	assert.Equal(t, "o/c.git", key)
	assert.EqualValues(t, 9, at)
	key, at = c.projectionGate("o/b.git", 7, now, now)
	assert.Equal(t, "o/c.git", key)
	assert.EqualValues(t, 9, at)
	key, at = c.projectionGate("o/c.git", 10, now, now)
	assert.Equal(t, "o/c.git", key, "nothing renamed it since")
	assert.EqualValues(t, 10, at)
	key, at = c.projectionGate("o/a.git", 4, "another", now)
	assert.Equal(t, "o/a.git", key, "another lineage's event")
	assert.EqualValues(t, 4, at)
	key, _ = c.projectionGate("o/a.git", 4, now, "")
	assert.Equal(t, "o/a.git", key, "a lineage not known")
	c.renamedAfter = nil
	key, _ = c.projectionGate("o/a.git", 4, now, now)
	assert.Equal(t, "o/a.git", key, "no table")
}

// The gate asks the table's RewrittenSince before its RenamedAfter, which
// gathers every rename target the table ever kept at each call, for each
// pending event of each pass: an event of a key no rename moved since
// never reaches it, and the keys the gate finds are those RenamedAfter
// alone finds, a name freed by a rename and taken again by another one
// included (#326).
func TestConsumerProjectionGateAsksTheTableCheaplyFirst(t *testing.T) {
	const sha = "65f1bf27bc3bf70f64657658635e66094edbcb4d"
	tbl := reftable.New()
	push := func(repo string) *reftable.RefTxn {
		return &reftable.RefTxn{Repo: repo, RepoID: 5, Format: "sha1", PusherID: 2, Updates: []reftable.RefUpdate{{Ref: "refs/heads/main", New: sha}}}
	}
	applyAt(t, tbl, 1, reftable.KindRefTxn, push("o/a.git"))
	applyAt(t, tbl, 2, reftable.KindRepoRename, &reftable.RepoRename{From: "o/a.git", To: "o/b.git"})
	applyAt(t, tbl, 3, reftable.KindRefTxn, push("o/x.git"))
	applyAt(t, tbl, 4, reftable.KindRepoRename, &reftable.RepoRename{From: "o/x.git", To: "o/a.git"})
	applyAt(t, tbl, 5, reftable.KindRepoRename, &reftable.RepoRename{From: "o/b.git", To: "o/c.git"})
	applyAt(t, tbl, 6, reftable.KindRefTxn, push("o/q.git"))

	asked := 0
	renamedAfter := func(from string, after uint64) (string, uint64, bool) {
		asked++
		return tbl.RenamedAfter(from, after)
	}
	cheap := &consumer{renamedAfter: renamedAfter, rewrittenSince: tbl.RewrittenSince}
	alone := &consumer{renamedAfter: tbl.RenamedAfter}
	now := testToken(1).Identity().String()
	for _, tc := range []struct {
		key   string
		index uint64
		want  string
		at    uint64
	}{
		{"o/a.git", 1, "o/c.git", 5},
		{"o/a.git", 4, "o/a.git", 4},
		{"o/x.git", 3, "o/a.git", 4},
		{"o/b.git", 2, "o/c.git", 5},
		{"o/q.git", 6, "o/q.git", 6},
	} {
		key, at := cheap.projectionGate(tc.key, tc.index, now, now)
		assert.Equal(t, tc.want, key, "%s at %d", tc.key, tc.index)
		assert.Equal(t, tc.at, at, "%s at %d", tc.key, tc.index)
		key, at = alone.projectionGate(tc.key, tc.index, now, now)
		assert.Equal(t, tc.want, key, "%s at %d, RenamedAfter alone", tc.key, tc.index)
		assert.Equal(t, tc.at, at, "%s at %d, RenamedAfter alone", tc.key, tc.index)
	}

	asked = 0
	key, at := cheap.projectionGate("o/q.git", 6, now, now)
	assert.Equal(t, "o/q.git", key)
	assert.EqualValues(t, 6, at)
	assert.Zero(t, asked, "no rename moved o/q.git since: RenamedAfter is not asked")
	key, _ = cheap.projectionGate("o/c.git", 5, now, now)
	assert.Equal(t, "o/c.git", key)
	assert.Zero(t, asked, "nor for the end of a chain")
}

// The dispatcher waits for the projection of the key a rename moved an
// event's repository to, as for any projection: no failure is counted,
// the stall names the key it waits for, and the event goes through once
// that key is projected at the rename. Past projectionStallBound it is
// parked, as any event whose repository does not project, and the later
// events of that key are parked behind it (#45, #326).
func TestConsumerWaitsForTheProjectionOfARenamedRepository(t *testing.T) {
	projected := map[string]uint64{"o/a.git": 6, "o/b.git": 5}
	c, mu := gateConsumer(projected)
	ev := reftable.Event{Token: testToken(4), Kind: reftable.EventRef, Repo: "o/a.git", Ref: "refs/heads/main", PusherID: 1}
	for range 3 {
		assert.False(t, c.projected([]reftable.Event{ev}), "o/a.git is projected at its event, o/b.git not at the rename")
	}
	stalled, _ := c.Stalled()
	assert.Equal(t, "o/b.git", stalled)
	assert.Zero(t, c.failures[4], "a wait is no failure")
	assert.Empty(t, c.reasons)

	mu.Lock()
	projected["o/b.git"] = 6
	mu.Unlock()
	assert.True(t, c.projected([]reftable.Event{ev}))
	assert.Zero(t, c.failures[4])

	// Past the bound, parked, and the key's later events behind it.
	defer test.MockVariableValue(&projectionStallBound, time.Millisecond)()
	mu.Lock()
	projected["o/b.git"] = 5
	mu.Unlock()
	c.clearStall()
	require.False(t, c.projected([]reftable.Event{ev}))
	time.Sleep(5 * time.Millisecond)
	require.True(t, c.projected([]reftable.Event{ev}), "let through to be parked")
	assert.Equal(t, maxAttempts, c.failures[4])
	assert.Contains(t, c.reasons[4], "not projected after")
	later := reftable.Event{Token: testToken(7), Kind: reftable.EventRef, Repo: "o/b.git", Ref: "refs/heads/main", PusherID: 1}
	require.True(t, c.projected([]reftable.Event{later}))
	assert.Equal(t, maxAttempts, c.failures[7], "parked behind the stalled key")
}

// A parked event whose repository was renamed since waits, in the
// leader's retries, for the projection of the key the renames led it to:
// retried earlier, its work read the directory under the new name, which
// may still hold the repository renamed away from it (#326). The wait is
// noted for the key it waits for, from the rename's index, and the entry
// is applied once that key is projected there.
func TestConsumerRetryOfAParkedEventWaitsForItsRenamedRepository(t *testing.T) {
	projected := map[string]uint64{"o/a.git": 6, "o/b.git": 5}
	c, mu := gateConsumer(projected)
	ran := 0
	c.work = func(context.Context, []reftable.Event) error {
		ran++
		return nil
	}
	c.tx = func(ctx context.Context, f func(context.Context) error) error { return f(ctx) }
	parked := &memParked{}
	parked.wire(c)
	ev := reftable.Event{Token: testToken(4), Kind: reftable.EventRef, Repo: "o/a.git", RepoID: 3, Ref: "refs/heads/main", PusherID: 1}
	b, err := json.Marshal(ev)
	require.NoError(t, err)
	lineage := testToken(1).Identity().String()
	require.NoError(t, parked.park(context.Background(), []*cluster_model.ParkedEvent{{
		Lineage: lineage, LogIndex: 4, Repo: "o/a.git", RepoID: 3, Event: string(b), Reason: "not projected after 10m0s", Attempts: maxAttempts,
	}}))

	var waits []parkedWait
	n, err := c.retryParkedNoting(context.Background(), &waits)
	require.NoError(t, err)
	assert.Zero(t, n)
	assert.Zero(t, ran, "not retried before the rename is projected")
	require.Len(t, waits, 1)
	assert.Equal(t, "o/b.git", waits[0].Repo, "waits for the key the rename led it to")
	assert.EqualValues(t, 4, waits[0].Index, "the parked entry's index, which its row and the views name")
	assert.EqualValues(t, 6, waits[0].waitsFrom(), "from the rename's index")

	defer test.MockVariableValue(&unprojectedYieldAfter, time.Duration(0))()
	c.noteUnprojected(waits)
	w, stuck := c.stuckProjection()
	assert.True(t, stuck, "o/b.git is not projected at the rename")
	assert.Equal(t, "o/b.git", w.Repo)

	mu.Lock()
	projected["o/b.git"] = 6
	mu.Unlock()
	_, stuck = c.stuckProjection()
	assert.False(t, stuck)
	waits = nil
	n, err = c.retryParkedNoting(context.Background(), &waits)
	require.NoError(t, err)
	assert.Equal(t, 1, n)
	assert.Equal(t, 1, ran)
	assert.Empty(t, waits)
}

// The post-receive work refuses to read a directory that holds another
// repository than the one it derives: before it reads it, and once it did,
// the directory may have moved meanwhile. Its transaction rolls back, and
// it is retried (#326).
func TestPushWorkRefusesADirectoryThatHoldsAnotherRepository(t *testing.T) {
	require.NoError(t, unittest.PrepareTestDatabase())
	ev := reftable.Event{
		Token: consensus.Token{Cluster: consensus.ClusterID{1}, Epoch: 1, Term: 1, Index: 3},
		Kind:  reftable.EventRef, Repo: "user2/repo1.git", RepoID: 1, PusherID: 2,
		Ref: "refs/tags/no-such-tag", Old: strings.Repeat("1", 40),
	}
	var asked []string
	check := func(before, after error) context.Context {
		return context.WithValue(db.DefaultContext, nodeProbeKey{}, nodeProbe{readCheck: func(key string, id int64) (func() error, error) {
			asked = append(asked, key)
			assert.EqualValues(t, 1, id)
			if before != nil {
				return nil, before
			}
			return func() error { return after }, nil
		}})
	}
	refused := errors.New("this node of the cluster still holds another repository under user2/repo1.git than repository 1")

	err := pushWorkWith(check(refused, nil), []reftable.Event{ev}, nil)
	require.ErrorIs(t, err, refused)
	assert.Contains(t, err.Error(), "does not read user2/repo1.git on this node now")

	err = pushWorkWith(check(nil, refused), []reftable.Event{ev}, nil)
	require.ErrorIs(t, err, refused, "the directory moved while the work read it")
	assert.Contains(t, err.Error(), "may not have held it meanwhile")

	require.NoError(t, pushWorkWith(check(nil, nil), []reftable.Event{ev}, nil))
	assert.Equal(t, []string{"user2/repo1.git", "user2/repo1.git", "user2/repo1.git"}, asked, "the repository's name in SQL")

	// Without the consumer's check, as before.
	require.NoError(t, pushWorkWith(db.DefaultContext, []reftable.Event{ev}, nil))
}

// readCheckRefusesTheFreedName checks, on the leader whose disk still
// holds A under B's new name, that neither the consumer's obtain nor its
// work reads that directory for B: obtain fetches none of B's commits into
// it, and the work, given the consumer's read check, refuses it and writes
// nothing (#326). The projection gate keeps the dispatch from it as well;
// these are what any other way to the work meets.
func readCheckRefusesTheFreedName(r *freedNameRun) {
	t := r.tc.t
	c := r.leader.r.consumer
	require.NotNil(t, c.readCheck)
	lost := c.obtain(context.Background(), []reftable.Event{r.ev})
	assert.Empty(t, lost)
	assert.False(t, hasObject(r.leader, freedA, r.cB2), "obtain fetched B's commit into A's directory")

	wctx, _ := clusterhook.WithAfterCommit(c.withProbe(db.DefaultContext))
	err := pushWorkWith(wctx, []reftable.Event{r.ev}, r.leader.r.table)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "does not read "+freedA+" on this node now")
	b, err := git_model.GetBranch(db.DefaultContext, 1, "main")
	if err == nil {
		assert.NotEqual(t, r.cA, b.CommitID, "B's branch row took A's head")
	}
}

// pushedUnderTheFreedName pushes to B under the name it took from A, once
// the leader projected the renames: the push is derived from B's directory
// there at once, the gate waiting for nothing more and the read check
// passing.
func pushedUnderTheFreedName(t *testing.T, r *freedNameRun) {
	tc, leader, f := r.tc, r.leader, r.f
	// The bench's pushes carry the id of the repository under the key.
	tc.repoIDsMu.Lock()
	tc.repoIDs[freedA], tc.repoIDs[freedAOld] = 1, 2
	tc.repoIDsMu.Unlock()
	defer test.MockVariableValue(&setting.RepoRootPath, leader.r.root)()
	cB3 := commit(t, r.workB, "B three")
	var mu sync.Mutex
	derived := map[string]error{}
	leader.work.setThrough(func(ctx context.Context, evs []reftable.Event) (bool, error) {
		ev := evs[0]
		if ev.Repo != freedA || ev.Kind != reftable.EventRef || !(ev.Ref == "refs/tags/freed-v2" || ev.Ref == "refs/heads/main" && ev.New == cB3) {
			return false, nil
		}
		wctx, _ := clusterhook.WithAfterCommit(ctx)
		err := pushWorkWith(wctx, evs, leader.r.table)
		mu.Lock()
		derived[ev.Ref] = err
		mu.Unlock()
		return true, err
	})
	defer leader.work.setThrough(nil)

	require.True(t, tc.pushVia(f, r.workB, freedA, "refs/tags/freed-v2", strings.Repeat("0", 40), r.cB2).OK)
	require.True(t, tc.pushVia(f, r.workB, freedA, "refs/heads/main", r.cB2, cB3).OK)
	require.Eventually(t, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(derived) == 2
	}, 20*time.Second, 20*time.Millisecond, "the leader's consumer derives both pushes")
	mu.Lock()
	got := maps.Clone(derived)
	mu.Unlock()
	for ref, err := range got {
		require.NoError(t, err, ref)
	}
	rel, err := repo_model.GetRelease(db.DefaultContext, 1, "freed-v2")
	require.NoError(t, err, "B's tag pushed under its new name has its release row")
	assert.Equal(t, r.cB2, rel.Sha1)
	b, err := git_model.GetBranch(db.DefaultContext, 1, "main")
	require.NoError(t, err)
	assert.Equal(t, cB3, b.CommitID)
	assert.Equal(t, "B three", strings.TrimSpace(b.CommitMessage))
	rows, err := tc.parked.list(context.Background())
	require.NoError(t, err)
	assert.Empty(t, rows)
}

// A tag the table holds live is taken for gone on git's word only where
// this node projected its key up to the table's last change of it: below,
// git may not show it yet, or still hold the repository a rename moved
// away from the key (#326). Without the consumer's probe, or without a
// table, git decides, as before.
func TestGitBehindReadsTheProjectionAgainstTheTable(t *testing.T) {
	const sha = "65f1bf27bc3bf70f64657658635e66094edbcb4d"
	tbl := reftable.New()
	push := func(ref string) *reftable.RefTxn {
		return &reftable.RefTxn{Repo: "o/r.git", RepoID: 5, Format: "sha1", PusherID: 2, Updates: []reftable.RefUpdate{{Ref: ref, New: sha}}}
	}
	applyAt(t, tbl, 1, reftable.KindRefTxn, push("refs/tags/v1"))
	applyAt(t, tbl, 2, reftable.KindRefTxn, push("refs/heads/main"))
	require.True(t, tagLive(tbl, "o/r.git", "refs/tags/v1"))
	assert.False(t, tagLive(tbl, "o/r.git", "refs/tags/v2"))
	assert.False(t, tagLive(nil, "o/r.git", "refs/tags/v1"))

	at := uint64(1)
	ctx := context.WithValue(context.Background(), nodeProbeKey{}, nodeProbe{projectedAt: func(string) uint64 { return at }})
	assert.True(t, gitBehind(ctx, tbl, "o/r.git"), "projected below the key's last change")
	at = 2
	assert.False(t, gitBehind(ctx, tbl, "o/r.git"), "projected at it")
	assert.False(t, gitBehind(context.Background(), tbl, "o/r.git"), "no probe")
	assert.False(t, gitBehind(ctx, nil, "o/r.git"), "no table")
}

// The consumer's fetch of the commits a work reads asks again, before each
// commit it fetches, whether the directory still holds the repository it
// fetches for: the projector may have moved another repository's directory
// under the name since. A refusal ends the fetch, nothing more fetched
// there (#326).
func TestConsumerFetchStopsOnceTheDirectoryHoldsAnotherRepository(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "o", "r.git")
	require.NoError(t, initBare(t.Context(), dir, "sha1"))
	var fetched []string
	c := &consumer{fetch: func(_ context.Context, _, _ string, shas []string) error {
		fetched = append(fetched, shas...)
		return nil
	}}
	refused := errors.New("this node of the cluster still holds another repository under o/r.git than repository 3")
	ids := []string{strings.Repeat("1", 40), strings.Repeat("2", 40)}
	asked := 0
	done := c.startFetchChecked(t.Context(), "o/r.git", dir, ids, func() error {
		asked++
		if asked > 1 {
			return refused
		}
		return nil
	})
	require.NotNil(t, done)
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("the fetch did not end")
	}
	assert.Equal(t, ids[:1], fetched, "the second commit is not fetched once the directory holds another repository")
	assert.Equal(t, 2, asked)

	// Without the check, as before: every missing commit is asked for.
	fetched = nil
	done = c.startFetch(t.Context(), "o/r.git", dir, ids)
	require.NotNil(t, done)
	<-done
	assert.Equal(t, ids, fetched)
}
