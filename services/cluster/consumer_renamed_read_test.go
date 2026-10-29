// Copyright 2026 The Forgejo Authors. All rights reserved.
// SPDX-License-Identifier: GPL-3.0-or-later

package cluster

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"forgejo.org/models/db"
	git_model "forgejo.org/models/git"
	repo_model "forgejo.org/models/repo"
	"forgejo.org/models/unittest"
	"forgejo.org/modules/clusterhook"
	"forgejo.org/modules/setting"
	"forgejo.org/modules/test"
	"forgejo.org/services/cluster/reftable"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// The fixtures: user2/repo1 (id 1, public) is B, user2/repo2 (id 2,
// private) is A.
const (
	freedB    = "user2/repo1.git"
	freedA    = "user2/repo2.git"
	freedAOld = "user2/freed-old.git"
)

// renameVia plays, on n, what Forgejo does for a rename it serves: the
// directory moved on n's disk, the row renamed in SQL, then the entry
// proposed (services/repository/transfer.go).
func renameVia(t *testing.T, n *testNode, id int64, from, to string) {
	t.Helper()
	settle(t, n)
	root := n.r.root
	require.NoError(t, os.Rename(filepath.Join(root, filepath.FromSlash(from)), filepath.Join(root, filepath.FromSlash(to))))
	name := strings.TrimSuffix(strings.TrimPrefix(to, "user2/"), ".git")
	_, err := db.GetEngine(db.DefaultContext).Exec("UPDATE repository SET name = ?, lower_name = ? WHERE id = ?", name, strings.ToLower(name), id)
	require.NoError(t, err)
	require.NoError(t, n.r.proposeRepoRename(context.Background(), from, to))
}

type freedNameRun struct {
	tc           *testCluster
	leader, f    *testNode
	cA, cB1, cB2 string
	// workB is B's working copy.
	workB string
	// ev is B's event, idx its index; derivedErr is what its work returned.
	ev         reftable.Event
	idx        uint64
	derivedErr error
	// frozen is called, when the scenario blocks the leader's projector,
	// while the leader's disk still holds A under B's new name and the
	// consumer was let through.
	frozen func(r *freedNameRun)
}

// freedNameScenario: B and A hold main (B at cB1, A at cA, "secret-of-A").
// The leader's consumer is held, then B receives push (ref, old -> sha)
// through a follower, acknowledged and projected on the leader. Then,
// through the follower, A is renamed to user2/freed-old and B renamed into
// A's freed name user2/repo2 (a fork replacing the repository it came
// from, an archive moved aside). The consumer being held stands for a
// consumer that waits behind another repository's projection (a large
// fetch in flight). block says what keeps the leader's disk holding A
// under user2/repo2 meanwhile: "fetch", a fetch of A's missing objects in
// flight on the leader (the projector then moves neither A's directory nor
// B's: isFetching, awaitsFetch, movesOn), "pass", a pass of the projector
// that does not end (a long pass on other repositories), "" nothing (the
// control: the leader projects the renames before the consumer goes on).
// The consumer is then let through, and derives B's push with Forgejo's
// real post-receive work, the one it runs, in the context it gives it.
//
// While the leader's disk holds A under B's new name, the consumer must
// wait for the projection of the renames, without counting it a failure
// nor parking the event (#326): its work would read A's directory for B.
// Once the projector moves the directories, it derives B's push.
func freedNameScenario(t *testing.T, block, ref string, bPush func(r *freedNameRun) (old, sha string), frozen func(r *freedNameRun)) *freedNameRun {
	require.NoError(t, unittest.PrepareTestDatabase())
	tc := newTestCluster(t, "node-a", "node-b", "node-c")
	raisePayload(t, tc)
	leader := clusterLeader(t, tc)
	f := followerOf(tc, leader)
	r := &freedNameRun{tc: tc, leader: leader, f: f, frozen: frozen}
	zero := strings.Repeat("0", 40)

	workB := t.TempDir()
	r.workB = workB
	run(t, workB, "init", "-q")
	r.cB1 = commit(t, workB, "B one")
	require.True(t, tc.pushVia(f, workB, freedB, "refs/heads/main", zero, r.cB1).OK)
	workA := t.TempDir()
	run(t, workA, "init", "-q")
	r.cA = commit(t, workA, "secret-of-A")
	require.True(t, tc.pushVia(f, workA, freedA, "refs/heads/main", zero, r.cA).OK)
	require.Equal(t, int64(1), tc.repoID(freedB))
	require.Equal(t, int64(2), tc.repoID(freedA))
	waitAll(t, tc, func(n *testNode) bool {
		return gitRef(t, n, freedA, "refs/heads/main") == r.cA && gitRef(t, n, freedB, "refs/heads/main") == r.cB1
	})
	r.cB2 = commit(t, workB, "B two")

	defer test.MockVariableValue(&setting.RepoRootPath, leader.r.root)()

	var stopOnce sync.Once
	var freezeOnce sync.Once
	frozenBy := ""
	p := leader.r.projector
	unfreeze := func() {
		freezeOnce.Do(func() {
			switch frozenBy {
			case "pass":
				p.passMu.Unlock()
			case "fetch":
				p.mu.Lock()
				delete(p.fetching, freedA)
				p.mu.Unlock()
			}
			p.signal()
		})
	}
	// Runs before tc.close (cleanups are LIFO): a frozen projector would
	// keep the bench from stopping.
	t.Cleanup(func() {
		stopOnce.Do(func() { leader.work.setThrough(nil) })
		leader.r.consumer.release()
		unfreeze()
	})

	old, sha := bPush(r)
	done := make(chan struct{})
	var once sync.Once
	leader.work.setThrough(func(ctx context.Context, evs []reftable.Event) (bool, error) {
		if evs[0].Repo != freedB || evs[0].Ref != ref || evs[0].New != sha {
			return false, nil
		}
		wctx, _ := clusterhook.WithAfterCommit(ctx)
		err := pushWorkWith(wctx, evs, leader.r.table)
		once.Do(func() {
			r.derivedErr = err
			close(done)
		})
		return true, err
	})

	pos, _ := leader.r.table.Applied()
	leader.r.consumer.holdAt(pos.Index)
	require.True(t, tc.pushVia(f, workB, freedB, ref, old, sha).OK)
	for _, ev := range leader.r.table.Events() {
		if ev.Repo == freedB && ev.Ref == ref && ev.New == sha {
			r.ev, r.idx = ev, ev.Token.Index
		}
	}
	require.NotZero(t, r.idx, "B's event in the leader's queue")
	require.Eventually(t, func() bool {
		return gitRef(t, leader, freedB, ref) == sha && leader.r.projector.Projected(freedB) >= r.idx
	}, 15*time.Second, 20*time.Millisecond, "the leader projects B's push")

	switch block {
	case "pass":
		p.passMu.Lock()
	case "fetch":
		p.mu.Lock()
		p.fetching[freedA] = func() {}
		p.mu.Unlock()
	}
	frozenBy = block
	renameVia(t, f, 2, freedA, freedAOld)
	renameVia(t, f, 1, freedB, freedA)
	waitApplied(t, f, leader)
	if block == "" {
		settle(t, leader)
	} else {
		// The leader's projector runs, or would, and keeps A's directory
		// under user2/repo2 all the same.
		time.Sleep(time.Second)
		require.Equal(t, r.cA, gitRef(t, leader, freedA, "refs/heads/main"), "the leader's disk holds A under B's new name")
	}
	t.Logf("leader %s: Projected(%s)=%d >= %d; %s on its disk: main=%s (A's cA=%s, B's cB1=%s)", leader.slot, freedB, leader.r.projector.Projected(freedB), r.idx,
		freedA, gitRef(t, leader, freedA, "refs/heads/main"), r.cA, r.cB1)

	leader.r.consumer.release()
	leader.r.signalConsumer()
	if block != "" {
		// The consumer waits for the projection of the renames: no work
		// runs from A's directory, and the wait is neither a failure nor a
		// parking.
		select {
		case <-done:
			assert.Fail(t, "the leader's consumer derived B's push while its disk held A under B's new name", "its work returned %v", r.derivedErr)
		case <-time.After(3 * time.Second):
			stalled, _ := leader.r.consumer.Stalled()
			assert.Equal(t, freedA, stalled, "the consumer waits for the projection of B's new name")
			leader.r.consumer.mu.Lock()
			failures := leader.r.consumer.failures[r.idx]
			leader.r.consumer.mu.Unlock()
			assert.Zero(t, failures, "waiting for the projection is no failure")
			rows, err := tc.parked.list(context.Background())
			require.NoError(t, err)
			assert.Empty(t, rows, "nothing is parked")
		}
		if r.frozen != nil {
			r.frozen(r)
		}
		unfreeze()
	}
	select {
	case <-done:
	case <-time.After(20 * time.Second):
		require.FailNow(t, "the leader's consumer did not derive B's push")
	}
	stopOnce.Do(func() { leader.work.setThrough(nil) })
	unfreeze()
	settle(t, leader)
	for _, n := range tc.nodes {
		require.Equal(t, sha, gitRef(t, n, freedA, ref), "%s serves B's %s under its new name", n.slot, ref)
	}
	return r
}

// A tag pushed to B, then B renamed into the name A left: the leader's
// consumer derived the tag from the directory its disk held under B's new
// name, A's, found no such tag there (tagGone), and dropped the tag's whole
// post-receive work. The tag lived in git on every node; its tag-only
// release row was never written, nor its push notifications, webhooks and
// workflows run (#326).
func testTagPushedBeforeARenameIntoAFreedName(t *testing.T, block string) {
	r := freedNameScenario(t, block, "refs/tags/freed-v1", func(r *freedNameRun) (string, string) {
		return strings.Repeat("0", 40), r.cB1
	}, func(r *freedNameRun) {
		// Run from A's directory, past the read check too, the work
		// refuses to take the tag for gone while the table holds it live
		// under B's name, which the leader did not project up to the
		// table: git there is not B's yet.
		wctx, _ := clusterhook.WithAfterCommit(context.WithValue(db.DefaultContext, nodeProbeKey{}, nodeProbe{projectedAt: r.leader.r.projector.Projected}))
		err := pushWorkWith(wctx, []reftable.Event{r.ev}, r.leader.r.table)
		if assert.Error(t, err, "the tag's work, read from A's directory, is retried, not dropped") {
			assert.Contains(t, err.Error(), "is live in the cluster's table under "+freedA)
		}
		_, err = repo_model.GetRelease(db.DefaultContext, 1, "freed-v1")
		assert.True(t, repo_model.IsErrReleaseNotExist(err), "nothing written from A's directory: %v", err)
	})
	require.NoError(t, r.derivedErr, "B's post-receive work")
	rel, err := repo_model.GetRelease(db.DefaultContext, 1, "freed-v1")
	if repo_model.IsErrReleaseNotExist(err) {
		assert.Fail(t, "B's tag freed-v1 has no release row: its post-receive work was dropped, read from A's directory under B's new name on the leader")
		return
	}
	require.NoError(t, err)
	assert.Equal(t, r.cB1, rel.Sha1)
}

func TestLeaderDerivesATagPushedBeforeARenameIntoAFreedNameControl(t *testing.T) {
	testTagPushedBeforeARenameIntoAFreedName(t, "")
}

func TestLeaderDerivesATagPushedBeforeARenameIntoAFreedName(t *testing.T) {
	testTagPushedBeforeARenameIntoAFreedName(t, "fetch")
}

func TestLeaderDerivesATagPushedBeforeARenameIntoAFreedNameLongPass(t *testing.T) {
	testTagPushedBeforeARenameIntoAFreedName(t, "pass")
}

// The same with a branch: B's main moves from cB1 to cB2. The consumer's
// obtain fetched B's commits from the members under B's new key into A's
// directory on the leader, and SyncBranchesToDB read main there: B's
// branch row took A's head, the private repository's commit and message,
// while every node's refs held cB2 (#326).
func testBranchPushedBeforeARenameIntoAFreedName(t *testing.T, block string, frozen func(r *freedNameRun)) *freedNameRun {
	r := freedNameScenario(t, block, "refs/heads/main", func(r *freedNameRun) (string, string) {
		return r.cB1, r.cB2
	}, frozen)
	require.NoError(t, r.derivedErr, "B's post-receive work")
	b, err := git_model.GetBranch(db.DefaultContext, 1, "main")
	if git_model.IsErrBranchNotExist(err) {
		assert.Fail(t, "B's main has no branch row")
		return r
	}
	require.NoError(t, err)
	t.Logf("B's branch row main: commit %s %q (B's cB2=%s, A's cA=%s)", b.CommitID, b.CommitMessage, r.cB2, r.cA)
	assert.Equal(t, r.cB2, b.CommitID, "B's branch row main")
	assert.NotContains(t, b.CommitMessage, "secret-of-A", "B's (public) branch row carries the private A's head commit message")
	assert.False(t, hasObject(r.leader, freedAOld, r.cB2), "the leader fetched B's commit into A's directory, now %s", freedAOld)
	return r
}

func TestLeaderDerivesABranchPushedBeforeARenameIntoAFreedNameControl(t *testing.T) {
	r := testBranchPushedBeforeARenameIntoAFreedName(t, "", nil)
	pushedUnderTheFreedName(t, r)
}

func TestLeaderDerivesABranchPushedBeforeARenameIntoAFreedName(t *testing.T) {
	testBranchPushedBeforeARenameIntoAFreedName(t, "fetch", readCheckRefusesTheFreedName)
}
