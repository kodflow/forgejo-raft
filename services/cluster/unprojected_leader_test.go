// Copyright 2026 The Forgejo Authors. All rights reserved.
// SPDX-License-Identifier: GPL-3.0-or-later

package cluster

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"forgejo.org/modules/json"
	"forgejo.org/modules/setting"
	"forgejo.org/modules/test"
	"forgejo.org/services/cluster/consensus"
	"forgejo.org/services/cluster/reftable"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// mockUnprojectedBounds makes a parked event's wait for its projection on
// the leader short enough for a test.
func mockUnprojectedBounds(t *testing.T) {
	t.Cleanup(test.MockVariableValue(&projectionStallBound, 2*time.Second))
	t.Cleanup(test.MockVariableValue(&parkedRetryEvery, 50*time.Millisecond))
	t.Cleanup(test.MockVariableValue(&ackEvery, uint64(1)))
	t.Cleanup(test.MockVariableValue(&setting.Cluster.AckMargin, uint64(0)))
	t.Cleanup(test.MockVariableValue(&unprojectedYieldAfter, time.Second))
	t.Cleanup(test.MockVariableValue(&projectionProbeEvery, time.Duration(0)))
}

// damageCopy makes the copy of repo on node n unreadable to git, as #39's
// damaged packed-refs: that node alone no longer projects it.
func damageCopy(t *testing.T, n *testNode, repo string) {
	dir := filepath.Join(n.r.root, filepath.FromSlash(repo))
	require.NoError(t, os.WriteFile(filepath.Join(dir, "packed-refs"), []byte("garbage line\n"), 0o644))
}

// parkedRows is what the parked events' table holds.
func parkedRows(t *testing.T, tc *testCluster) int {
	rows, err := tc.parked.list(context.Background())
	require.NoError(t, err)
	return len(rows)
}

// A leader that cannot project one repository (its own copy damaged, the
// case #39 accepted) holds that repository's parked event, and the trim of
// the whole event queue, for as long as it leads, while its consumer's
// passes all go through: nothing made it yield, and every write of the
// cluster ended refused. The autopilot hands the leadership to a voter
// that projects it; that voter's retry applies the event, the queue is
// trimmed, and the leader that yielded says so in its hello, so the
// leadership is not handed back to it for its checkpoints (#249).
func TestALeaderThatCannotProjectARepositoryHandsTheLeadershipToAVoterThatDoes(t *testing.T) {
	_ = handsOverAnUnprojectedRepository(t, nil)
}

// A leader elected by the bench itself before the damaged one parked the
// event, as CI's load gave one (run 41135477872), projects the repository
// and applies the event at once, and nothing is ever parked. Here the
// leader is cut from both followers for two seconds, past the election
// timeout, right after the first attempt's second push: that attempt does
// not count, and the hand-off is played again on another repository
// (#384).
func TestALeaderChangeBeforeTheParkPlaysTheHandOffAgain(t *testing.T) {
	missed := handsOverAnUnprojectedRepository(t, func(b *autoBench, attempt int, l *autoNode) {
		if attempt > 1 {
			return
		}
		cut := func(on bool) {
			for _, an := range b.alive() {
				if an != l {
					b.network.Cut(l.id, an.id, on)
					b.network.Cut(an.id, l.id, on)
				}
			}
		}
		cut(true)
		time.Sleep(2 * time.Second)
		cut(false)
	})
	require.NotEmpty(t, missed, "the cut spoiled the first attempt")
	assert.True(t, strings.HasPrefix(missed[0], "o/broken-1.git: "), "the first attempt did not count: %v", missed)
}

// handOffAttempts bounds handsOverAnUnprojectedRepository's attempts.
const handOffAttempts = 3

// handsOverAnUnprojectedRepository plays the hand-off of
// TestALeaderThatCannotProjectARepositoryHandsTheLeadershipToAVoterThatDoes.
// disturb, when set, runs right after each attempt's second push, given
// the leader whose copy that attempt damaged. It returns why the attempts
// that did not count did not.
//
// The scenario needs that leader to lead from the second push until it
// hands the leadership over, for seconds, while the autonomous bench, with
// its election timeout of 100 to 200 ms, elected another one by itself
// once a few heartbeats were lost under load. A leader elected before the
// park projects the repository and applies the event, and one elected
// after the park, before the hand-off, applies the parked event without
// the first leader ever yielding (#384). The bench runs here under
// wideBenchTiming, and an attempt that a leader change still spoiled does
// not count: once the new leader applied the event, the scenario is played
// again on another repository, the copy of the leader of the moment
// damaged, at most handOffAttempts times.
func handsOverAnUnprojectedRepository(t *testing.T, disturb func(b *autoBench, attempt int, l *autoNode)) []string {
	mockUnprojectedBounds(t)
	tc, b := newAutoClusterTimed(t, 3, nil, wideBenchTiming, "node-a", "node-b", "node-c")
	b.waitMembers(3, 3)
	work := t.TempDir()
	run(t, work, "init", "-q")
	zero := strings.Repeat("0", 40)

	var first, next *autoNode
	var repo string
	var parkedAt int64
	var missed []string
	for attempt := 1; ; attempt++ {
		require.LessOrEqual(t, attempt, handOffAttempts, "the leadership moved by itself before the hand-off at every attempt: %s", strings.Join(missed, "; "))
		repo = fmt.Sprintf("o/broken-%d.git", attempt)
		via := b.follower()

		// Whichever node leads applies c1's event, and the queue is
		// trimmed everywhere.
		c1, ok, msg := pushOne(tc, via.n, work, repo, zero, "one")
		require.True(t, ok, msg)
		require.Eventually(t, func() bool { return workRun(b, repo) >= 1 }, 10*time.Second, 10*time.Millisecond, "the event of %s is applied", repo)
		for _, an := range b.alive() {
			require.Eventually(t, func() bool { return an.r().table.EventCount() == 0 }, 10*time.Second, 10*time.Millisecond, "trimmed while healthy on %s", an.n.slot)
		}

		first = b.leader()
		require.NotNil(t, first)
		if via == first {
			// The leadership went to via meanwhile: the second push goes
			// through a node whose copy stays readable.
			for _, an := range b.alive() {
				if an != first {
					via = an
					break
				}
			}
		}
		require.Eventually(t, func() bool { return gitRef(t, first.n, repo, "refs/heads/main") == c1 }, 10*time.Second, 10*time.Millisecond)
		damageCopy(t, first.n, repo)
		c2, ok, msg := pushOne(tc, via.n, work, repo, c1, "two")
		require.True(t, ok, msg)
		if disturb != nil {
			disturb(b, attempt, first)
		}
		for _, an := range b.alive() {
			if an == first {
				continue
			}
			require.Eventually(t, func() bool { return gitRef(t, an.n, repo, "refs/heads/main") == c2 },
				10*time.Second, 20*time.Millisecond, "%s projects it", an.n.slot)
		}

		// first, the only node that does not project it, parks the event
		// as long as it leads; a leader elected meanwhile applies it.
		parkedAt = 0
		require.Eventually(t, func() bool {
			rows, err := tc.parked.list(context.Background())
			if err == nil && len(rows) == 1 {
				parkedAt = rows[0].LogIndex
				return true
			}
			return workRun(b, repo) >= 2
		}, 4*projectionStallBound, 20*time.Millisecond, "the leader parks the event")
		if parkedAt == 0 {
			missed = append(missed, fmt.Sprintf("%s: %s applied the event before %s parked it", repo, leaderSlot(b), first.n.slot))
			for _, an := range b.alive() {
				require.Eventually(t, func() bool { return an.r().table.EventCount() == 0 }, 10*time.Second, 20*time.Millisecond, "the leader elected meanwhile applied it: trimmed on %s", an.n.slot)
			}
			assert.Zero(t, parkedRows(t, tc), "nothing parked")
			continue
		}
		assert.Zero(t, first.n.r.consumerFails.Load(), "no failed pass: the passes alone never made it yield")

		require.Eventually(t, func() bool {
			l := b.leader()
			return l != nil && l != first
		}, 20*time.Second, 20*time.Millisecond, "the leadership goes to a voter that projects it")
		require.Eventually(t, func() bool { return parkedRows(t, tc) == 0 }, 10*time.Second, 20*time.Millisecond, "the new leader applies the parked event")
		next = b.leader()
		require.NotNil(t, next)
		// sql_yield is stored once the transfer returned.
		if !holdsWithin(2*time.Second, func() bool { return first.r().sqlYield.Load() != 0 }) {
			missed = append(missed, fmt.Sprintf("%s: %s parked the event at %d, then the leadership went to %s without its hand-off", repo, first.n.slot, parkedAt, next.n.slot))
			continue
		}
		break
	}

	require.Eventually(t, func() bool { return next.r().table.EventCount() == 0 }, 10*time.Second, 20*time.Millisecond, "and the queue is trimmed again")
	assert.NotEqual(t, consensus.RoleLeader, first.r().Engine().Status().Role)
	assert.NotZero(t, first.r().sqlYield.Load(), "its hello says it gave the leadership away")
	audited := slices.ContainsFunc(next.r().table.Audit(), func(a reftable.AuditRecord) bool {
		return a.Action == string(ActionTransfer) && strings.Contains(a.Detail, "does not project "+repo)
	})
	assert.True(t, audited, "the transfer is audited with why")
	t.Logf("parked at %d, applied by %s after the leadership left %s; attempts that did not count: %v", parkedAt, next.n.slot, first.n.slot, missed)
	return missed
}

// workRun is how many events of repo the consumers of the bench's live
// nodes ran, whichever led.
func workRun(b *autoBench, repo string) int {
	n := 0
	for _, an := range b.alive() {
		w := an.n.work
		w.mu.Lock()
		for _, ev := range w.events {
			if ev.Repo == repo {
				n++
			}
		}
		w.mu.Unlock()
	}
	return n
}

// leaderSlot is the slot of the bench's leader, "none" without one.
func leaderSlot(b *autoBench) string {
	if l := b.leader(); l != nil {
		return l.n.slot
	}
	return "none"
}

// holdsWithin reports whether cond holds within d, polled every 20 ms.
func holdsWithin(d time.Duration, cond func() bool) bool {
	for deadline := time.Now().Add(d); ; time.Sleep(20 * time.Millisecond) {
		if cond() {
			return true
		}
		if time.Now().After(deadline) {
			return false
		}
	}
}

// In the manual mode nothing moves the leadership by itself (#47): the
// leader that cannot project a repository says, in its health, that the
// parked event waits for its projection on it and that the way out is the
// leadership of a member that projects it, not reconcile; and its hello
// says that it could not derive the SQL, so the operator's transfer away
// from it stands. Handed to a member that projects it, the event is
// applied and the queue trimmed (#249). The pilot does not keep that word
// fresh: past checkpointHandBack, the member may lead again.
func TestInTheManualModeALeaderThatCannotProjectARepositorySaysSo(t *testing.T) {
	mockUnprojectedBounds(t)
	tc := newTestCluster(t, "node-a", "node-b", "node-c")
	leader, follower, third := tc.nodes[0], tc.nodes[1], tc.nodes[2]
	require.Same(t, leader.node, tc.mem.Leader())
	ctx := context.Background()

	work := t.TempDir()
	run(t, work, "init", "-q")
	zero := strings.Repeat("0", 40)
	c1 := commit(t, work, "one")
	require.True(t, tc.pushVia(follower, work, "o/broken.git", "refs/heads/main", zero, c1).OK)
	require.Eventually(t, func() bool { return leader.work.count() == 1 }, 10*time.Second, 10*time.Millisecond)
	require.Eventually(t, func() bool { return leader.r.table.EventCount() == 0 }, 10*time.Second, 10*time.Millisecond, "trimmed while healthy")
	assert.Zero(t, leader.r.memberHello().SQLYield)

	damageCopy(t, leader, "o/broken.git")
	c2 := commit(t, work, "two")
	require.True(t, tc.pushVia(follower, work, "o/broken.git", "refs/heads/main", c1, c2).OK)
	for _, n := range []*testNode{follower, third} {
		require.Eventually(t, func() bool { return gitRef(t, n, "o/broken.git", "refs/heads/main") == c2 },
			10*time.Second, 20*time.Millisecond, "%s projects it", n.slot)
	}
	require.Eventually(t, func() bool { return parkedRows(t, tc) == 1 }, 4*projectionStallBound, 20*time.Millisecond, "the leader parks the event")
	rows, err := tc.parked.list(ctx)
	require.NoError(t, err)
	parkedAt := rows[0].LogIndex

	require.Eventually(t, func() bool { return leader.r.ConsumerHealth(ctx).ProjectionWait != nil }, 10*time.Second, 20*time.Millisecond,
		"the leader's health says the parked event waits for its projection on it")
	v := leader.r.ConsumerHealth(ctx)
	assert.Equal(t, "o/broken.git", v.ProjectionWait.Repo)
	assert.Equal(t, parkedAt, v.ProjectionWait.Index)
	assert.False(t, v.ProjectionYield, "no autopilot in the manual mode")
	assert.Nil(t, follower.r.ConsumerHealth(ctx).ProjectionWait, "only the leader's retries know")

	require.Eventually(t, func() bool { return leader.r.memberHello().SQLYield != 0 }, 10*time.Second, 20*time.Millisecond,
		"its hello says it could not derive the SQL")
	assert.Contains(t, takerHold(leader.r.memberHello(), consensus.PeerHealth{}, true, time.Now()), "could not derive the SQL",
		"the leadership is not handed back to it for its checkpoints")
	assert.Same(t, leader.node, tc.mem.Leader(), "nothing moves the leadership by itself in the manual mode")
	assert.Equal(t, uint64(parkedAt), leader.r.table.Events()[0].Token.Index, "the queue is held at the parked event")

	// The operator's transfer to a member that projects it.
	tc.mem.SetLeader(follower.node)
	require.Eventually(t, func() bool { return parkedRows(t, tc) == 0 }, 10*time.Second, 20*time.Millisecond, "the follower applies it")
	require.Eventually(t, func() bool { return follower.r.table.EventCount() == 0 }, 10*time.Second, 20*time.Millisecond, "and the queue is trimmed")
	require.Eventually(t, func() bool { return leader.r.ConsumerHealth(ctx).ProjectionWait == nil }, 10*time.Second, 20*time.Millisecond,
		"no longer said once it does not lead")
	h := leader.r.memberHello()
	assert.NotEmpty(t, takerHold(h, consensus.PeerHealth{}, true, time.Now()), "the transfer stands")
	assert.Empty(t, takerHold(h, consensus.PeerHealth{}, true, time.Now().Add(checkpointHandBack+time.Minute)),
		"not held for good: its SQL of every other repository works")
}

// The leader hands the leadership over for a parked event it cannot
// project only to a voter that answers that it projects the repository at
// the event's index: one whose own copy fails is passed over, and with
// none the leadership stays; the voters are asked again only after
// projectionProbeEvery (#249).
func TestOnlyAVoterThatProjectsTheRepositorySucceedsTheLeader(t *testing.T) {
	tc := newTestCluster(t, "node-a", "node-b", "node-c")
	leader, nb, nc := tc.nodes[0], tc.nodes[1], tc.nodes[2]
	ctx := context.Background()
	work := t.TempDir()
	run(t, work, "init", "-q")
	zero := strings.Repeat("0", 40)
	c1 := commit(t, work, "one")
	require.True(t, tc.pushVia(leader, work, "o/r.git", "refs/heads/main", zero, c1).OK)
	// The index of the repository's last entry, which every member
	// certifies once it projected it.
	_, at := leader.r.table.RepoHash("o/r.git")
	require.Positive(t, at)
	for _, n := range tc.nodes {
		require.Eventually(t, func() bool { return n.r.projector.Projected("o/r.git") >= at }, 10*time.Second, 20*time.Millisecond, "%s projects it", n.slot)
	}
	eng := leader.node

	to, ok := leader.r.projectingSuccessor(ctx, eng, eng.Membership(), parkedWait{Repo: "o/r.git", Index: int64(at)}, "test")
	require.True(t, ok)
	assert.Contains(t, []consensus.NodeID{nb.node.Status().ID, nc.node.Status().ID}, to)

	// node-b's own copy fails: it does not project the next push.
	damageCopy(t, nb, "o/r.git")
	c2 := commit(t, work, "two")
	require.True(t, tc.pushVia(leader, work, "o/r.git", "refs/heads/main", c1, c2).OK)
	_, last := leader.r.table.RepoHash("o/r.git")
	require.Greater(t, last, at)
	require.Eventually(t, func() bool { return nc.r.projector.Projected("o/r.git") >= last }, 10*time.Second, 20*time.Millisecond, "node-c projects it")
	require.Eventually(t, func() bool {
		return slices.ContainsFunc(nb.r.projector.Lag(), func(l RepoLag) bool { return l.Repo == "o/r.git" && l.Error != "" })
	}, 10*time.Second, 20*time.Millisecond, "node-b fails to project it")
	require.Less(t, nb.r.projector.Projected("o/r.git"), last)
	idx := int64(last)
	for range 3 {
		leader.r.projectionProbe = time.Time{}
		to, ok = leader.r.projectingSuccessor(ctx, eng, eng.Membership(), parkedWait{Repo: "o/r.git", Index: idx}, "test")
		require.True(t, ok)
		assert.Equal(t, nc.node.Status().ID, to, "only node-c projects it")
	}

	// No voter projects it at that index: the leadership stays, and the
	// voters are not asked again before projectionProbeEvery.
	leader.r.projectionProbe = time.Time{}
	_, ok = leader.r.projectingSuccessor(ctx, eng, eng.Membership(), parkedWait{Repo: "o/r.git", Index: idx + 1_000_000}, "test")
	assert.False(t, ok)
	assert.Contains(t, leader.r.projectionNoted, "no voter in reach says it projects o/r.git")
	_, ok = leader.r.projectingSuccessor(ctx, eng, eng.Membership(), parkedWait{Repo: "o/r.git", Index: idx}, "test")
	assert.False(t, ok, "asked again only after projectionProbeEvery")
}

// The object channel says, to a member, up to which index this node
// projects a repository; a binary without the question answers 404, which
// the leader takes for no answer, and a key is checked first (#249).
func TestTheObjectChannelSaysUpToWhereItProjects(t *testing.T) {
	ask := func(oc *objectChannel, repo string) *httptest.ResponseRecorder {
		w := httptest.NewRecorder()
		oc.handleProjected(w, httptest.NewRequest(http.MethodGet, "/v1/projected?repo="+repo, nil))
		return w
	}
	oc := &objectChannel{projected: func(repo string) uint64 {
		if repo == "o/r.git" {
			return 42
		}
		return 7
	}}
	w := ask(oc, "o/r.git")
	require.Equal(t, http.StatusOK, w.Code, w.Body.String())
	var ans projectedAnswer
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &ans))
	assert.Equal(t, projectedAnswer{Repo: "o/r.git", Projected: 42}, ans)
	assert.Equal(t, http.StatusBadRequest, ask(oc, "..%2Fx").Code)
	assert.Equal(t, http.StatusNotFound, ask(&objectChannel{}, "o/r.git").Code, "as a binary without the question")
}
