// Copyright 2026 The Forgejo Authors. All rights reserved.
// SPDX-License-Identifier: GPL-3.0-or-later

package cluster

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"forgejo.org/modules/clusterhook"
	"forgejo.org/services/cluster/consensus"
	"forgejo.org/services/cluster/consensus/memraft"
	"forgejo.org/services/cluster/reftable"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// raiseClusterTo raises the version the cluster writes to v through via, as
// RaisePayloadVersion raises it to this binary's, and waits for every
// replicator of all to apply it: a cluster one of whose members runs a
// binary applying v at most stays there.
func raiseClusterTo(t *testing.T, via *Replicator, v int, all ...*Replicator) {
	t.Helper()
	data, err := reftable.Encode(reftable.KindAudit, &reftable.Audit{
		ID: clusterhook.NewNonce(), Operator: "op", Action: reftable.AuditPayloadVersion, Target: strconv.Itoa(v), Outcome: reftable.AuditRaised,
	})
	require.NoError(t, err)
	_, res, err := via.proposeCommand(context.Background(), data, nil)
	require.NoError(t, err)
	require.True(t, res.OK, res.Reason)
	for _, r := range all {
		require.Eventually(t, func() bool { return r.table.WriteVersion() == uint8(v) }, 10*time.Second, 10*time.Millisecond)
	}
}

// replicatorsOf lists the replicators of a test cluster.
func replicatorsOf(tc *testCluster) []*Replicator {
	rs := make([]*Replicator, len(tc.nodes))
	for i, n := range tc.nodes {
		rs[i] = n.r
	}
	return rs
}

// createKeys creates every key in the table through r, as git init's HEAD,
// a few at once.
func createKeys(t *testing.T, r *Replicator, keys []string) {
	t.Helper()
	work := make(chan string)
	errs := make(chan error, len(keys))
	var wg sync.WaitGroup
	for range 128 {
		wg.Go(func() {
			for key := range work {
				tx := &reftable.RefTxn{
					Repo: key, Format: "sha1", Nonce: clusterhook.NewNonce(), Init: true,
					Updates: []reftable.RefUpdate{{Ref: "HEAD", Symbolic: "refs/heads/main"}},
				}
				data, err := reftable.EncodeVersion(r.table.WriteVersion(), reftable.KindRefTxn, tx)
				if err == nil {
					var res reftable.Result
					// An outcome left unknown under load is settled by the
					// nonce, as a push settles it.
					landed := func() bool { _, ok := r.table.CommittedNonce(tx.Nonce); return ok }
					_, res, err = r.proposeCommand(context.Background(), data, landed)
					if err == nil && !res.OK {
						err = fmt.Errorf("%s: %s", key, res.Reason)
					}
				}
				errs <- err
			}
		})
	}
	for _, key := range keys {
		work <- key
	}
	close(work)
	wg.Wait()
	close(errs)
	for err := range errs {
		require.NoError(t, err)
	}
}

// lastEntry is the newest committed entry of a memraft node.
func lastEntry(t *testing.T, n *memraft.Node) consensus.Entry {
	t.Helper()
	pos := n.Status().Applied
	entries, err := n.ReadCommitted(t.Context(), pos, pos, 1<<20)
	require.NoError(t, err)
	require.Len(t, entries, 1)
	return entries[0]
}

// On the SDK's real engine: an owner past the engine's entry bound is
// renamed once every member applies reftable.OwnerRenameVersion. Below it,
// the entry listing its keys is past the bound, and the rename is refused
// before anything is proposed, from any member, as since #194. From it,
// the entry names the owner alone, the engine takes it, and every member's
// Apply moves every key to the same table (#182). Names as long as Forgejo
// takes them (40 characters for an owner, 100 for a repository) cross the
// bound with the fewest keys; reftable's test renames the owner of 8000
// repositories of the issue.
func TestAnOwnerPastTheEntryBoundIsRenamedOnceThePayloadIsRaised(t *testing.T) {
	var nodes []*testNode
	for _, slot := range []string{"node-a", "node-b", "node-c"} {
		nodes = append(nodes, &testNode{slot: slot, r: newReplicator(t.TempDir(), newRegistry(5*time.Second, 64, 4096)), addr: "mem-" + slot})
	}
	startSDKEngines(t, consensus.ClusterID{0x0f, 0x18, 0x2b, 0x01}, nodes)
	var rs []*Replicator
	for _, n := range nodes {
		rs = append(rs, n.r)
	}
	ctx := context.Background()
	raiseClusterTo(t, rs[1], reftable.OwnerRenameVersion-1, rs...)

	from, to := strings.Repeat("o", 39)+"1", strings.Repeat("o", 39)+"2"
	owner := [2]string{from, to}
	moves := func(n int) [][2]string {
		out := make([][2]string, 0, 2*n)
		for i := range n {
			name := fmt.Sprintf("repository-%05d-", i) + strings.Repeat("r", 83)
			out = append(out, [2]string{from + "/" + name + ".git", to + "/" + name + ".git"}, [2]string{from + "/" + name + ".wiki.git", to + "/" + name + ".wiki.git"})
		}
		return out
	}
	base := renameEntrySize(t, rs[0], moves(1000), owner)
	n := 1000 + (consensus.MaxEntryBytes-base)/(renameEntrySize(t, rs[0], moves(1001), owner)-base) + 1
	listed := moves(n)
	size := renameEntrySize(t, rs[0], listed, owner)
	require.Greater(t, size, consensus.MaxEntryBytes)
	keys := make([]string, len(listed))
	for i, m := range listed {
		keys[i] = m[0]
	}
	start := time.Now()
	createKeys(t, rs[0], keys)
	t.Logf("an owner of %d repositories with wikis: the entry listing them is %d bytes (bound %d); created in %v", n, size, consensus.MaxEntryBytes, time.Since(start))
	for _, r := range rs {
		require.Eventually(t, func() bool { return len(r.table.OwnerKeys(from)) == 2*n }, 30*time.Second, 50*time.Millisecond)
	}

	for _, r := range rs {
		before, _ := r.table.Applied()
		err := r.ownerRenameRefusal(ctx, from, to)
		require.ErrorIs(t, err, clusterhook.ErrTooLarge, "below version %d", reftable.OwnerRenameVersion)
		err = r.proposeOwnerRename(ctx, from, to)
		require.ErrorIs(t, err, clusterhook.ErrTooLarge)
		assert.Contains(t, err.Error(), fmt.Sprintf("renaming the owner %s moves its %d repositories", from, 2*n))
		after, _ := r.table.Applied()
		assert.Equal(t, before.Index, after.Index, "nothing was proposed")
	}

	require.NoError(t, rs[1].RaisePayloadVersion(ctx, "op"))
	for _, r := range rs {
		require.Eventually(t, func() bool { return r.table.WriteVersion() == reftable.PayloadVersion }, 10*time.Second, 10*time.Millisecond)
	}
	for _, r := range rs {
		require.NoError(t, r.ownerRenameRefusal(ctx, from, to))
	}
	require.NoError(t, rs[2].proposeOwnerRename(ctx, from, to))
	applied, _ := rs[2].table.Applied()
	for _, r := range rs {
		require.Eventually(t, func() bool { pos, _ := r.table.Applied(); return pos.Index >= applied.Index }, 10*time.Second, 10*time.Millisecond)
		assert.Empty(t, r.table.OwnerKeys(from))
		assert.Len(t, r.table.OwnerKeys(to), 2*n)
		assert.Equal(t, rs[2].table.Root(), r.table.Root())
		last := listed[len(listed)-1]
		renamedFrom, _, ok := r.table.RenamedFrom(last[1])
		assert.True(t, ok && renamedFrom == last[0])
	}
}

// A mixed cluster: while one member's binary applies version 7 at most, the
// raise to OwnerRenameVersion waits for it, an owner's rename is the entry
// listing its keys, refused before anything is proposed past the bound, as
// before, and nothing that member cannot apply is written. Once it runs this
// binary and the version is raised, the same owner is renamed by its names
// (#182).
func TestAnOwnerRenameByItsNamesWaitsForEveryMemberToApplyIt(t *testing.T) {
	m := consensus.Membership{Members: []consensus.Member{{ID: 1}, {ID: 2}, {ID: 3}}}
	mixed := []hello{{Node: consensus.NodeID(2).String(), Payload: reftable.PayloadVersion}, {Node: consensus.NodeID(3).String(), Payload: reftable.OwnerRenameVersion - 1}}
	assert.False(t, everyMemberApplies(m, 1, mixed, anyAnswer), "a member applying version 7 holds the raise")

	mem, rs := NewForTesting(t.TempDir(), "node-a", "node-b", "node-c", "node-d", "node-e")
	ctx := context.Background()
	leader, follower := rs[0], rs[2]
	mem.SetLeader(leader.Engine().(*memraft.Node))
	raiseClusterTo(t, leader, reftable.OwnerRenameVersion-1, rs...)
	probe := leader.memberProbe
	older := true
	leader.memberProbe = func(ctx context.Context, addr, slot string) (hello, error) {
		h, err := probe(ctx, addr, slot)
		if slot == "node-e" && older {
			h.Payload = reftable.OwnerRenameVersion - 1
		}
		return h, err
	}
	err := leader.Operate(ctx, OperatorFor(1), Operation{Action: ActionRaisePayload, Target: strconv.Itoa(reftable.PayloadVersion)})
	require.ErrorContains(t, err, "not raised")
	require.Equal(t, uint8(reftable.OwnerRenameVersion-1), leader.table.WriteVersion())

	createKeys(t, leader, []string{"big/a.git", "big/a.wiki.git", "big/b.git"})
	v, rn := leader.renameCommand(leader.ownerMoves("big", "huge"), [2]string{"big", "huge"})
	listing, err := reftable.EncodeVersion(v, reftable.KindRepoRename, rn)
	require.NoError(t, err)
	for _, r := range rs {
		r.maxEntry = len(listing) - 1
	}
	before, _ := leader.table.Applied()
	require.ErrorIs(t, follower.ownerRenameRefusal(ctx, "big", "huge"), clusterhook.ErrTooLarge)
	err = follower.proposeOwnerRename(ctx, "big", "huge")
	require.ErrorIs(t, err, clusterhook.ErrTooLarge)
	assert.Contains(t, err.Error(), "renaming the owner big moves its 3 repositories")
	after, _ := leader.table.Applied()
	require.Equal(t, before.Index, after.Index, "nothing was proposed")
	entries, err := leader.Engine().(*memraft.Node).ReadCommitted(ctx, 1, after.Index, 1<<20)
	require.NoError(t, err)
	for _, e := range entries {
		require.Less(t, e.Data[0], uint8(reftable.OwnerRenameVersion), "entry %d is written at a version node-e cannot apply", e.Index)
	}
	for _, r := range rs {
		assert.Len(t, r.table.OwnerKeys("big"), 3, "%s: nothing moved", r.Engine().Status().Slot)
	}

	// node-e runs this binary now: the version is raised, and the owner is
	// renamed by its names, past the same bound.
	older = false
	require.NoError(t, leader.Operate(ctx, OperatorFor(1), Operation{Action: ActionRaisePayload, Target: strconv.Itoa(reftable.PayloadVersion)}))
	for _, r := range rs {
		require.Equal(t, uint8(reftable.PayloadVersion), r.table.WriteVersion())
	}
	require.NoError(t, follower.ownerRenameRefusal(ctx, "big", "huge"))
	require.NoError(t, follower.proposeOwnerRename(ctx, "big", "huge"))
	e := lastEntry(t, leader.Engine().(*memraft.Node))
	assert.Equal(t, []byte{reftable.PayloadVersion, byte(reftable.KindOwnerRename)}, e.Data[:2])
	assert.Less(t, len(e.Data), len(listing), "the entry names the owner alone")
	for _, r := range rs {
		assert.Empty(t, r.table.OwnerKeys("big"))
		assert.Equal(t, []string{"huge/a.git", "huge/a.wiki.git", "huge/b.git"}, r.table.OwnerKeys("huge"))
		assert.Equal(t, leader.table.Root(), r.table.Root())
	}
}

// A node behind the log replays an owner's rename, and one that restores a
// snapshot cut after it, as a member installs one or an observer restores a
// checkpoint's refs.snapshot, holds its outcome: at both versions, each one
// moves the owner's directories under the new owner, the very directories,
// with nothing set aside and nothing left under the old owner. The rename
// by its names is projected as the rename listing its keys (#182).
func TestAnOwnerRenameByItsNamesIsProjectedAsTheRenameOfItsKeys(t *testing.T) {
	for _, version := range []int{reftable.OwnerRenameVersion - 1, reftable.OwnerRenameVersion} {
		t.Run(fmt.Sprintf("payload version %d", version), func(t *testing.T) {
			tc := newTestCluster(t, "node-a", "node-b", "node-c", "node-d", "node-e")
			raiseClusterTo(t, tc.nodes[1].r, version, replicatorsOf(tc)...)
			work := t.TempDir()
			run(t, work, "init", "-q")
			c1 := commit(t, work, "one")
			c2 := commit(t, work, "two")
			zero := strings.Repeat("0", 40)
			origin, peer, behind, restored := tc.nodes[0], tc.nodes[1], tc.nodes[3], tc.nodes[4]
			heads := map[string]string{"alice/a.git": c1, "alice/a.wiki.git": c2, "alice/x.git": c2}
			for repo, sha := range heads {
				require.True(t, tc.pushVia(origin, work, repo, "refs/heads/main", zero, sha).OK, repo)
			}
			waitAll(t, tc, func(n *testNode) bool { return gitRef(t, n, "alice/x.git", "refs/heads/main") == c2 })
			settle(t, origin)
			renameOn(t, origin, "alice/x.git", "alice/y.git")
			moved := map[string]string{"alice/a.git": "bob/a.git", "alice/a.wiki.git": "bob/a.wiki.git", "alice/y.git": "bob/y.git"}
			want := map[string]string{"bob/a.git": c1, "bob/a.wiki.git": c2, "bob/y.git": c2}
			waitAll(t, tc, func(n *testNode) bool {
				return gitRef(t, n, "alice/y.git", "refs/heads/main") == c2 && len(n.r.projector.Lag()) == 0
			})
			inodes := map[*testNode]map[string]os.FileInfo{}
			for _, n := range []*testNode{behind, restored} {
				inodes[n] = map[string]os.FileInfo{}
				for from := range moved {
					fi, err := os.Stat(filepath.Join(n.r.root, filepath.FromSlash(from)))
					require.NoError(t, err)
					inodes[n][from] = fi
				}
				tc.mem.SetDown(n.node, true)
			}

			require.NoError(t, os.Rename(filepath.Join(origin.r.root, "alice"), filepath.Join(origin.r.root, "bob")))
			require.NoError(t, origin.r.ownerRenameRefusal(t.Context(), "alice", "bob"))
			require.NoError(t, origin.r.proposeOwnerRename(t.Context(), "alice", "bob"))
			kind := reftable.KindRepoRename
			if version >= reftable.OwnerRenameVersion {
				kind = reftable.KindOwnerRename
			}
			e := lastEntry(t, origin.node)
			assert.Equal(t, []byte{byte(version), byte(kind)}, e.Data[:2])
			require.Eventually(t, func() bool { return gitRef(t, peer, "bob/y.git", "refs/heads/main") == c2 }, 10*time.Second, 20*time.Millisecond)

			// restored installs a snapshot of origin's table, cut after the
			// rename; behind replays the log.
			snap, err := origin.r.table.Snapshot()
			require.NoError(t, err)
			var buf bytes.Buffer
			_, err = snap.WriteTo(&buf)
			require.NoError(t, err)
			require.NoError(t, restored.r.table.Restore(&buf, consensus.SnapshotMeta{}))
			tc.mem.SetDown(restored.node, false)
			tc.mem.SetDown(behind.node, false)

			for _, n := range []*testNode{behind, restored} {
				assert.Equal(t, origin.r.table.Root(), n.r.table.Root(), n.slot)
				for _, to := range moved {
					require.Eventually(t, func() bool { return gitRef(t, n, to, "refs/heads/main") == want[to] }, 15*time.Second, 20*time.Millisecond, "%s: %s", n.slot, to)
				}
				settle(t, n)
				for from, to := range moved {
					fi, err := os.Stat(filepath.Join(n.r.root, filepath.FromSlash(to)))
					require.NoError(t, err)
					assert.True(t, os.SameFile(inodes[n][from], fi), "%s: the directory of %s reached %s", n.slot, from, to)
					assert.False(t, dirExists(n, from), "%s: %s is left", n.slot, from)
				}
				assert.False(t, dirExists(n, "bob/x.git"), "%s: a directory under bob/x.git, a name the database does not have", n.slot)
				assert.Zero(t, setAsideCount(t, n), "%s: nothing set aside", n.slot)
			}
		})
	}
}
