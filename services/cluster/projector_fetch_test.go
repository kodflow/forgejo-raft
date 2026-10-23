// Copyright 2026 The Forgejo Authors. All rights reserved.
// SPDX-License-Identifier: GPL-3.0-or-later

package cluster

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"forgejo.org/services/cluster/reftable"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// fetchBench is a projector over a table holding one branch per key, all
// at commits of one scratch repository, with an object channel the test
// drives key by key: a key in block waits for its release or for its
// context to end before it receives its objects.
type fetchBench struct {
	p    *projector
	work string

	mu       sync.Mutex
	block    map[string]chan struct{}
	inFlight map[string]int
	most     map[string]int
	total    int
	peak     int
}

func newFetchBench(t *testing.T, keys ...string) *fetchBench {
	t.Helper()
	b := &fetchBench{work: t.TempDir(), block: map[string]chan struct{}{}, inFlight: map[string]int{}, most: map[string]int{}}
	run(t, b.work, "init", "-q")
	sha := commit(t, b.work, "one")
	table := reftable.New()
	for i, key := range keys {
		applyAt(t, table, uint64(i+1), reftable.KindRefTxn, &reftable.RefTxn{
			Repo: key, Format: "sha1",
			Updates: []reftable.RefUpdate{{Ref: "HEAD", Symbolic: "refs/heads/main"}, {Ref: "refs/heads/main", New: sha}},
		})
	}
	b.p = newProjector(t.TempDir(), table, newRegistry(time.Minute, 1, 1))
	b.p.fetch = b.fetch
	return b
}

// fetch brings every object of the scratch repository into dir.
func (b *fetchBench) fetch(ctx context.Context, repo, dir string, _ []string) error {
	b.mu.Lock()
	b.inFlight[repo]++
	b.total++
	b.most[repo] = max(b.most[repo], b.inFlight[repo])
	b.peak = max(b.peak, b.total)
	gate := b.block[repo]
	b.mu.Unlock()
	defer func() {
		b.mu.Lock()
		b.inFlight[repo]--
		b.total--
		b.mu.Unlock()
	}()
	if gate != nil {
		select {
		case <-gate:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return gitFetchInto(ctx, b.work, dir)
}

// gitFetchInto copies the scratch repository's objects into dir, leaving
// no ref behind.
func gitFetchInto(ctx context.Context, work, dir string) error {
	if _, err := gitRun(ctx, dir, nil, "fetch", "-q", "--no-tags", work, "+refs/heads/main:refs/fetch-bench/main"); err != nil {
		return err
	}
	_, err := gitRun(ctx, dir, nil, "update-ref", "-d", "refs/fetch-bench/main")
	return err
}

func (b *fetchBench) hold(repo string) chan struct{} {
	b.mu.Lock()
	defer b.mu.Unlock()
	ch := make(chan struct{})
	b.block[repo] = ch
	return ch
}

// A repository whose missing objects take long to come (gigabytes behind a
// slow member) froze the serial pass, and every other repository waited
// behind it, uncertified, for up to projectBudget (#137). Its fetch now
// runs beside the pass: another repository, after it in the pass's order,
// is projected and certified meanwhile, and the slow one once its objects
// came.
func TestASlowFetchDoesNotHoldTheOtherRepositories(t *testing.T) {
	t.Parallel()
	b := newFetchBench(t, "o/a-slow.git", "o/b-fast.git")
	release := b.hold("o/a-slow.git")
	b.p.unfence()
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	done := make(chan struct{})
	go func() {
		defer close(done)
		b.p.run(ctx)
	}()
	defer func() { cancel(); <-done; b.p.refence() }()
	b.p.signal()

	require.Eventually(t, func() bool { return b.p.Projected("o/b-fast.git") > 0 }, 10*time.Second, 10*time.Millisecond,
		"the fast repository is projected while the slow one's fetch runs")
	assert.Zero(t, b.p.Projected("o/a-slow.git"), "the slow repository is not certified before its objects came")
	assert.True(t, b.p.Writable("o/b-fast.git"))
	assert.False(t, b.p.Writable("o/a-slow.git"))
	assert.False(t, b.p.WaitsBehind("o/a-slow.git"), "the slow repository waits on its own fetch")

	close(release)
	require.Eventually(t, func() bool { return b.p.Projected("o/a-slow.git") > 0 }, 10*time.Second, 10*time.Millisecond,
		"the slow repository is projected once its objects came")
	b.mu.Lock()
	defer b.mu.Unlock()
	assert.Equal(t, 1, b.most["o/a-slow.git"], "never two fetches of one repository at once")
}

// The fetches beside the pass are bounded, one per repository: the keys
// past maxFetches wait for a slot, and every one is certified in the end.
func TestFetchesBesideThePassAreBounded(t *testing.T) {
	t.Parallel()
	keys := []string{"o/r1.git", "o/r2.git", "o/r3.git", "o/r4.git", "o/r5.git", "o/r6.git"}
	b := newFetchBench(t, keys...)
	gates := make([]chan struct{}, 0, len(keys))
	for _, k := range keys {
		gates = append(gates, b.hold(k))
	}
	b.p.unfence()
	for range 5 {
		b.p.pass(t.Context())
	}
	b.mu.Lock()
	peak := b.peak
	b.mu.Unlock()
	assert.Equal(t, maxFetches, peak, "fetches in flight")
	for _, g := range gates {
		close(g)
	}
	require.Eventually(t, func() bool {
		b.p.pass(t.Context())
		for _, k := range keys {
			if b.p.Projected(k) == 0 {
				return false
			}
		}
		return true
	}, 15*time.Second, 10*time.Millisecond)
	b.mu.Lock()
	defer b.mu.Unlock()
	for _, k := range keys {
		assert.Equal(t, 1, b.most[k], "%s fetched once at a time", k)
	}
	b.p.refence()
}

// refence cancels the fetches in flight and waits for them: none records a
// failure of the table about to be emptied, and none outlives it.
func TestRefenceCancelsTheFetchesInFlight(t *testing.T) {
	t.Parallel()
	b := newFetchBench(t, "o/r.git")
	b.hold("o/r.git") // never released: only the cancellation ends it
	b.p.unfence()
	b.p.pass(t.Context())
	require.True(t, b.p.isFetching("o/r.git"))
	refenced := make(chan struct{})
	go func() {
		b.p.refence()
		close(refenced)
	}()
	select {
	case <-refenced:
	case <-time.After(10 * time.Second):
		t.Fatal("refence waits on a fetch it did not cancel")
	}
	assert.False(t, b.p.isFetching("o/r.git"))
	b.p.mu.Lock()
	defer b.p.mu.Unlock()
	assert.Empty(t, b.p.failures, "the cancelled fetch left no failure of the old table")
	b.mu.Lock()
	defer b.mu.Unlock()
	assert.Zero(t, b.total)
}

// A rename's source whose objects are being fetched is not moved under the
// target: the fetch would land in a directory gone from its path, or fail.
// The move waits until the fetch ended.
func TestARenameWaitsForTheFetchOfItsSource(t *testing.T) {
	t.Parallel()
	b := newFetchBench(t, "o/a.git")
	b.p.unfence()
	require.Eventually(t, func() bool {
		b.p.pass(t.Context())
		return b.p.Projected("o/a.git") > 0
	}, 10*time.Second, 10*time.Millisecond)
	applyAt(t, b.p.table, 2, reftable.KindRepoRename, &reftable.RepoRename{From: "o/a.git", To: "o/b.git"})

	// A fetch of o/a.git still in flight.
	b.p.mu.Lock()
	b.p.fetching["o/a.git"] = func() {}
	b.p.mu.Unlock()
	b.p.pass(t.Context())
	src, dst := filepath.Join(b.p.root, "o", "a.git"), filepath.Join(b.p.root, "o", "b.git")
	_, err := os.Stat(src)
	require.NoError(t, err, "the source stays while its fetch runs")
	_, err = os.Stat(dst)
	assert.True(t, os.IsNotExist(err), "nothing is created under the target meanwhile")
	assert.Zero(t, b.p.Projected("o/b.git"))

	b.p.mu.Lock()
	delete(b.p.fetching, "o/a.git")
	b.p.mu.Unlock()
	require.Eventually(t, func() bool {
		b.p.pass(t.Context())
		return b.p.Projected("o/b.git") > 0
	}, 10*time.Second, 10*time.Millisecond)
	_, err = os.Stat(dst)
	require.NoError(t, err, "the directory moved once the fetch ended")
}

// A repository deleted while its objects are fetched needs none of them:
// the fetch is cancelled, and its directory removed once it ended.
func TestADeletionCancelsTheFetch(t *testing.T) {
	t.Parallel()
	b := newFetchBench(t, "o/r.git")
	b.hold("o/r.git")
	b.p.unfence()
	b.p.pass(t.Context())
	require.True(t, b.p.isFetching("o/r.git"))
	applyAt(t, b.p.table, 2, reftable.KindRepoDelete, &reftable.RepoDelete{Repo: "o/r.git"})
	var gone atomic.Bool
	require.Eventually(t, func() bool {
		b.p.pass(t.Context())
		_, err := os.Stat(filepath.Join(b.p.root, "o", "r.git"))
		gone.Store(os.IsNotExist(err))
		return gone.Load() && !b.p.isFetching("o/r.git")
	}, 10*time.Second, 10*time.Millisecond)
	deleted, _ := b.p.table.Deleted("o/r.git")
	assert.GreaterOrEqual(t, b.p.Projected("o/r.git"), deleted)
}

// A fetch that fails at once (no member answers, the object channel
// refused) must not wake the pass: woken, it projected the repository
// again at once, which failed again, a hot loop where a failed projection
// waits for the next tick.
func TestAFailedFetchWaitsForTheNextTick(t *testing.T) {
	t.Parallel()
	b := newFetchBench(t, "o/r.git")
	var calls atomic.Int64
	b.p.fetch = func(context.Context, string, string, []string) error {
		calls.Add(1)
		return errors.New("no member holds them")
	}
	b.p.unfence()
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan struct{})
	go func() {
		defer close(done)
		b.p.run(ctx)
	}()
	b.p.signal()
	require.Eventually(t, func() bool { return calls.Load() >= 1 }, 5*time.Second, time.Millisecond)
	time.Sleep(time.Second) // under the 2 s tick
	cancel()
	<-done
	b.p.refence()
	assert.EqualValues(t, 1, calls.Load(), "a failed fetch is not retried before the next tick")
}

// A repository whose missing objects wait for a fetch slot, every one
// taken by other repositories' long transfers, waits behind them: the
// consumer counted that wait against its own stall bound, parked it, then
// the next one (#151). Once it has its slot, it waits on its own fetch.
func TestAWaitForAFetchSlotIsAWaitBehindTheOthers(t *testing.T) {
	t.Parallel()
	keys := []string{"o/r1.git", "o/r2.git", "o/r3.git", "o/r4.git", "o/r5.git"}
	b := newFetchBench(t, keys...)
	gates := make([]chan struct{}, 0, len(keys))
	for _, k := range keys {
		gates = append(gates, b.hold(k))
	}
	b.p.unfence()
	defer b.p.refence()
	b.p.pass(t.Context())
	require.False(t, b.p.isFetching("o/r5.git"), "no slot left for r5")
	require.Zero(t, b.p.Projected("o/r5.git"))
	assert.True(t, b.p.WaitsBehind("o/r5.git"), "r5 waits behind the other repositories' fetches")
	assert.False(t, b.p.WaitsBehind("o/r1.git"), "r1 waits on its own fetch")

	close(gates[0])
	require.Eventually(t, func() bool {
		b.p.pass(t.Context())
		return b.p.isFetching("o/r5.git")
	}, 10*time.Second, 10*time.Millisecond, "r5 has a slot once r1's fetch ended")
	assert.False(t, b.p.WaitsBehind("o/r5.git"), "r5 waits on its own fetch")
	for _, g := range gates[1:] {
		close(g)
	}
	require.Eventually(t, func() bool {
		b.p.pass(t.Context())
		return b.p.Projected("o/r5.git") > 0
	}, 10*time.Second, 10*time.Millisecond)
	assert.False(t, b.p.WaitsBehind("o/r5.git"))
}

// Four repositories whose fetches fail, then hang, at every retry came back
// at each tick ahead of a healthy one in the pass's order and took every
// freed slot again: the healthy one stayed queued, its wait never counted
// (#151), and the consumer waited behind it for good (#160). A retry now
// leaves a slot to a queued repository that did not fail.
func TestRetriedFetchesLeaveASlotToAHealthyRepository(t *testing.T) {
	t.Parallel()
	failing := []string{"o/a1.git", "o/a2.git", "o/a3.git", "o/a4.git"}
	b := newFetchBench(t, append(append([]string{}, failing...), "o/z.git")...)
	first := make(chan struct{})
	var calls atomic.Int64
	healthy := b.fetch
	b.p.fetch = func(ctx context.Context, repo, dir string, missing []string) error {
		if repo == "o/z.git" {
			return healthy(ctx, repo, dir, missing)
		}
		if calls.Add(1) <= int64(len(failing)) {
			select {
			case <-first:
			case <-ctx.Done():
			}
			return errors.New("the member holding them does not answer")
		}
		<-ctx.Done() // the retry hangs until projectBudget
		return ctx.Err()
	}
	b.p.unfence()
	defer b.p.refence()
	// First attempts: every slot taken, z waits behind them (#151).
	b.p.pass(t.Context())
	require.False(t, b.p.isFetching("o/z.git"))
	require.True(t, b.p.WaitsBehind("o/z.git"), "z waits behind first attempts")
	close(first)
	require.Eventually(t, func() bool {
		b.p.mu.Lock()
		defer b.p.mu.Unlock()
		return len(b.p.fetching) == 0 && len(b.p.failures) == len(failing)
	}, 10*time.Second, 10*time.Millisecond)

	// The retries come back first in the pass's order, and hang.
	require.Eventually(t, func() bool {
		b.p.pass(t.Context())
		return b.p.Projected("o/z.git") > 0
	}, 10*time.Second, 10*time.Millisecond, "z has a slot although the retries come first")
	assert.Greater(t, calls.Load(), int64(len(failing)), "the failed repositories were retried")
}

// A repository queued while every slot is held by retries of failed
// fetches, each of which may fail or hang again for projectBudget, waits
// for nothing certain: its wait counts against its own bound (#160). A
// first attempt among the holders is a wait behind the others (#151).
func TestAWaitBehindRetriesAloneCounts(t *testing.T) {
	t.Parallel()
	p := newProjector(t.TempDir(), reftable.New(), newRegistry(time.Minute, 1, 1))
	p.mu.Lock()
	for _, r := range []string{"o/a1.git", "o/a2.git", "o/a3.git", "o/a4.git"} {
		p.fetching[r] = func() {}
		p.retrying[r] = true
	}
	p.queued["o/z.git"] = true
	p.mu.Unlock()
	assert.False(t, p.WaitsBehind("o/z.git"), "slots held by retries alone")
	p.mu.Lock()
	delete(p.retrying, "o/a1.git")
	p.mu.Unlock()
	assert.True(t, p.WaitsBehind("o/z.git"), "a first attempt holds a slot")
}
