// Copyright 2026 The Forgejo Authors. All rights reserved.
// SPDX-License-Identifier: GPL-3.0-or-later

package cluster

import (
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	cluster_model "forgejo.org/models/cluster"
	"forgejo.org/models/db"
	git_model "forgejo.org/models/git"
	"forgejo.org/models/unittest"
	"forgejo.org/modules/clusterhook"
	"forgejo.org/modules/setting"
	"forgejo.org/modules/test"
	"forgejo.org/services/cluster/reftable"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// forcedPast is a cluster whose node-c never received a commit: node-c is
// out while main moves base -> n1, then is forced to n2 (n1 unreachable);
// back, node-c projects n2 and holds no n1. e1 (base -> n1) and e2
// (n1 -> n2) are the two pushes' events, as the table holds them.
type forcedPast struct {
	tc         *testCluster
	a, b, c    *testNode
	base       string
	n1, n2     string
	e1, e2     reftable.Event
	dirA, dirB string
	dirC       string
}

func newForcedPast(t *testing.T) *forcedPast {
	t.Helper()
	require.NoError(t, unittest.PrepareTestDatabase())
	f := &forcedPast{tc: newTestCluster(t, "node-a", "node-b", "node-c")}
	f.a, f.b, f.c = f.tc.nodes[0], f.tc.nodes[1], f.tc.nodes[2]
	work := t.TempDir()
	run(t, work, "init", "-q")
	f.base = commit(t, work, "base")
	require.Equal(t, int64(1), f.tc.repoID(laggingRepo))
	require.True(t, f.tc.pushVia(f.a, work, laggingRepo, "refs/heads/main", strings.Repeat("0", 40), f.base).OK)
	for _, m := range f.tc.nodes {
		require.Eventually(t, func() bool { return gitRef(t, m, laggingRepo, "refs/heads/main") == f.base }, 15*time.Second, 20*time.Millisecond)
	}

	// node-c is out: no shipment reaches it, its projection of the
	// repository waits.
	hold := clusterhook.NewNonce()
	require.NoError(t, f.c.r.registry.register(hold, laggingRepo, nil, os.Getpid(), false))
	require.NoError(t, f.c.srv.Close())
	f.n1 = commit(t, work, "n1")
	resp := f.tc.pushVia(f.a, work, laggingRepo, "refs/heads/main", f.base, f.n1)
	require.True(t, resp.OK, resp.Message)
	run(t, work, "reset", "-q", "--hard", f.base)
	f.n2 = commit(t, work, "n2")
	resp = f.tc.pushVia(f.a, work, laggingRepo, "refs/heads/main", f.n1, f.n2)
	require.True(t, resp.OK, resp.Message)

	f.c.r.registry.release(hold)
	require.Eventually(t, func() bool { return gitRef(t, f.c, laggingRepo, "refs/heads/main") == f.n2 }, 15*time.Second, 20*time.Millisecond, "node-c projects the table")
	f.dirA = filepath.Join(f.a.r.root, filepath.FromSlash(laggingRepo))
	f.dirB = filepath.Join(f.b.r.root, filepath.FromSlash(laggingRepo))
	f.dirC = filepath.Join(f.c.r.root, filepath.FromSlash(laggingRepo))
	require.False(t, holds(f.dirC, f.n1), "node-c never received n1")

	for _, e := range f.a.r.table.Events() {
		if e.Ref != "refs/heads/main" {
			continue
		}
		switch e.New {
		case f.n1:
			f.e1 = e
		case f.n2:
			f.e2 = e
		}
	}
	require.NotEmpty(t, f.e1.Repo, "the table holds the event of base -> n1")
	require.NotEmpty(t, f.e2.Repo, "the table holds the event of n1 -> n2")
	return f
}

// holds reports whether the repository in dir holds the commit sha.
func holds(dir, sha string) bool {
	_, err := gitIn(dir, "cat-file", "-e", sha+"^{commit}")
	return err == nil
}

// leading is a consumer of node n as its leader runs it: the real
// post-receive work, in a real SQL transaction with its cursor and its
// parked rows, on n's git.
func leading(t *testing.T, n *testNode) *consumer {
	t.Helper()
	t.Cleanup(test.MockVariableValue(&setting.RepoRootPath, n.r.root))
	c := newConsumer(n.r)
	// Its fetches write into n's directories: they end before those go.
	t.Cleanup(c.fetches.Wait)
	return c
}

// dispatchOne dispatches evs, one entry, from the lineage's cursor in SQL.
func dispatchOne(t *testing.T, c *consumer, evs ...reftable.Event) error {
	t.Helper()
	ctx := db.DefaultContext
	lineage := evs[0].Token.Identity().String()
	cur, err := cluster_model.GetCursor(ctx, cursorName(lineage))
	require.NoError(t, err)
	return c.dispatch(ctx, lineage, cur, evs)
}

// The post-receive work of a branch push reads the push's commits in the
// leader's git (services/repository/push.go: GetCommit(NewCommitID), then
// CommitsBeforeUntil(OldCommitID)). node-c, leader before the consumer
// derived the push of n1, never received n1: its projector fetches only
// what the table's refs name now, and n1 is named by none. The work failed
// on node-c at every attempt, was parked after maxAttempts, and every
// retry failed the same while node-c led: the repository's later events
// parked behind it and the trim of the event queue stopped there (#224,
// TestHuntFjpALeaderMissingAForcePushedCommitFailsItsEventForGood). The
// consumer now fetches from the members, before the batch's transaction,
// the commits the work reads that the leader lacks: node-a and node-b,
// which received the push, still hold n1 (PinnedObjects keeps git gc away
// from it while its event is in the table).
func TestALeaderMissingAForcePushedCommitFetchesItForItsEvent(t *testing.T) {
	f := newForcedPast(t)
	c := leading(t, f.c)
	require.NoError(t, dispatchOne(t, c, f.e1), "node-c, leader, derives the event of the push it never received")
	assert.True(t, holds(f.dirC, f.n1), "node-c fetched n1 from the members")
	require.NoError(t, dispatchOne(t, c, f.e2), "and the event of the push forced over it")

	row, err := git_model.GetBranch(db.DefaultContext, 1, "main")
	require.NoError(t, err)
	assert.Equal(t, f.n2, row.CommitID, "the branch row follows the table")
	n, err := cluster_model.CountParked(db.DefaultContext)
	require.NoError(t, err)
	assert.Zero(t, n, "nothing is parked")
}

// collected removes the unreachable n1 from the repository in dir, as git gc
// does once nothing pins it.
func collected(t *testing.T, dir, sha string) {
	t.Helper()
	run(t, dir, "reflog", "expire", "--expire=now", "--all")
	run(t, dir, "gc", "-q", "--prune=now")
	require.False(t, holds(dir, sha), "git gc removed %.8s from %s", sha, dir)
}

// lackOf is what the consumer knows of the commit id of dir it lacks.
func lackOf(c *consumer, dir, id string) (lackState, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	s, ok := c.lacking[lackKey{dir: dir, id: id}]
	if !ok {
		return lackState{}, false
	}
	return *s, true
}

// The commit of a push forced over it is held by no member any more: git
// gc removed it everywhere, the loss of the members that held it would as
// well. Its event can never be derived as it was pushed: parked, it would
// stay so for good. Once every member has gone on answering for
// lostConfirm that it does not hold it, the consumer declares it lost and
// the work derives the push without it: the branch row follows the table,
// the push of n1 sends no notification, and the push forced over it is
// described by n2 alone. The event is parked meanwhile, as any failing
// one, and so is the next event of the repository, behind it.
func TestAnEventWhoseCommitNoMemberHoldsIsDerivedWithoutIt(t *testing.T) {
	f := newForcedPast(t)
	collected(t, f.dirA, f.n1)
	collected(t, f.dirB, f.n1)
	c := leading(t, f.c)
	ctx := db.DefaultContext

	for i := 1; i <= maxAttempts; i++ {
		err := dispatchOne(t, c, f.e1)
		require.ErrorContains(t, err, "object does not exist", "attempt %d: not lost before lostConfirm", i)
	}
	s, ok := lackOf(c, f.dirC, f.n1)
	require.True(t, ok)
	assert.False(t, s.nowhere.IsZero(), "every member answered that it does not hold n1")
	require.NoError(t, dispatchOne(t, c, f.e1), "parked after maxAttempts")
	require.NoError(t, dispatchOne(t, c, f.e2), "parked behind it")
	n, err := cluster_model.CountParked(ctx)
	require.NoError(t, err)
	require.EqualValues(t, 2, n)

	// lostConfirm later, the members answer the same: n1 is lost.
	c.mu.Lock()
	for _, s := range c.lacking {
		s.nowhere = s.nowhere.Add(-lostConfirm)
	}
	c.mu.Unlock()
	applied, err := c.retryParked(ctx)
	require.NoError(t, err)
	assert.Equal(t, 2, applied, "both events derived without n1")
	n, err = cluster_model.CountParked(ctx)
	require.NoError(t, err)
	assert.Zero(t, n, "nothing stays parked")
	row, err := git_model.GetBranch(ctx, 1, "main")
	require.NoError(t, err)
	assert.Equal(t, f.n2, row.CommitID, "the branch row follows the table")
	assert.False(t, holds(f.dirC, f.n1), "no member could provide n1")
}

// A member that does not answer may hold the commit: it is not lost, even
// when every member that answers does not hold it, and the event waits,
// tried again, until that member answers and the commit is fetched.
func TestACommitAMemberThatDoesNotAnswerMayHoldIsNotLost(t *testing.T) {
	f := newForcedPast(t)
	defer test.MockVariableValue(&lostConfirm, 0)()
	collected(t, f.dirA, f.n1)
	require.NoError(t, f.b.srv.Close())
	c := leading(t, f.c)

	err := dispatchOne(t, c, f.e1)
	require.ErrorContains(t, err, "object does not exist", "node-b, which holds n1, does not answer")
	s, ok := lackOf(c, f.dirC, f.n1)
	require.True(t, ok)
	assert.True(t, s.nowhere.IsZero(), "not every member answered that it does not hold n1")

	// node-b's object channel is back on its address.
	ln, err := net.Listen("tcp", f.b.addr)
	require.NoError(t, err)
	srv := &http.Server{Handler: f.b.r.objects.handler(), TLSConfig: f.b.srv.TLSConfig, ReadHeaderTimeout: 5 * time.Second}
	go func() { _ = srv.ServeTLS(ln, "", "") }()
	t.Cleanup(func() { _ = srv.Close() })

	require.NoError(t, dispatchOne(t, c, f.e1), "n1 fetched from node-b, the event derived as pushed")
	assert.True(t, holds(f.dirC, f.n1))
	require.NoError(t, dispatchOne(t, c, f.e2))
	n, err := cluster_model.CountParked(db.DefaultContext)
	require.NoError(t, err)
	assert.Zero(t, n)
}
