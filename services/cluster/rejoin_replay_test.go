// Copyright 2026 The Forgejo Authors. All rights reserved.
// SPDX-License-Identifier: GPL-3.0-or-later

package cluster

import (
	"context"
	"testing"
	"time"

	"forgejo.org/services/cluster/reftable"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// A node retired and back under a new id replays the whole log on its
// table: a deletion followed by a recreation, and a rename and its undo,
// leave it with the leader's table, and it follows the writes accepted
// after (#41, TestHuntR1ProveRejoinReplayCluster).
func TestARejoinUnderANewIDKeepsTheTable(t *testing.T) {
	_, b := newAutoCluster(t, 4, "node-a", "node-b", "node-c", "node-d")
	b.waitMembers(4, 4)
	const (
		sha1 = "1111111111111111111111111111111111111111"
		sha2 = "2222222222222222222222222222222222222222"
		sha3 = "3333333333333333333333333333333333333333"
	)
	propose := func(kind reftable.Kind, body any) {
		t.Helper()
		data, err := reftable.Encode(kind, body)
		require.NoError(t, err)
		var res reftable.Result
		require.Eventually(t, func() bool {
			l := b.leader()
			if l == nil {
				return false
			}
			ctx, cancel := context.WithTimeout(b.ctx, 5*time.Second)
			defer cancel()
			_, res, err = l.r().proposeCommand(ctx, data, nil)
			return err == nil
		}, 30*time.Second, 50*time.Millisecond, "proposed: %v", err)
		require.True(t, res.OK, res.Reason)
	}
	txn := func(repo, sha string, expect uint64) *reftable.RefTxn {
		return &reftable.RefTxn{Repo: repo, RepoID: 7, Format: "sha1", PusherID: 3, Updates: []reftable.RefUpdate{{Ref: "refs/heads/main", New: sha, ExpectRev: expect}}}
	}
	propose(reftable.KindRefTxn, txn("o/x.git", sha1, 0))
	propose(reftable.KindRepoDelete, &reftable.RepoDelete{Repo: "o/x.git"})
	propose(reftable.KindRefTxn, txn("o/x.git", sha2, 2))
	propose(reftable.KindRefTxn, txn("p/a.git", sha1, 0))
	propose(reftable.KindRepoRename, &reftable.RepoRename{From: "p/a.git", To: "p/b.git"})
	propose(reftable.KindRepoRename, &reftable.RepoRename{From: "p/b.git", To: "p/a.git"})

	sameTable := func(msg string) {
		t.Helper()
		require.Eventually(t, func() bool {
			l := b.leader()
			if l == nil {
				return false
			}
			want, _ := l.r().table.Applied()
			for _, an := range b.alive() {
				got, _ := an.r().table.Applied()
				if got.Index != want.Index || an.r().table.Root() != l.r().table.Root() {
					return false
				}
			}
			return true
		}, 30*time.Second, 20*time.Millisecond, msg)
	}
	sameTable("every node holds the leader's table before the cut")

	victim := b.follower()
	old := b.idOf(victim)
	m := b.waitRemoved(b.expire(victim), 3, 3)
	require.Contains(t, m.Retired, old)
	b.restart(victim)
	b.waitMembers(4, 4)
	require.NotEqual(t, old, b.idOf(victim), "the victim rejoined under a new id")
	sameTable("the victim holds the leader's table after its rejoin")

	v, _ := victim.r().table.Get("o/x.git", "refs/heads/main")
	assert.False(t, v.Tombstone, "the recreated repository is alive on the victim")
	_, deleted := victim.r().table.Deleted("o/x.git")
	assert.False(t, deleted)

	propose(reftable.KindRefTxn, txn("o/x.git", sha3, v.Rev))
	sameTable("the victim follows a write accepted after its rejoin")
	v, _ = victim.r().table.Get("o/x.git", "refs/heads/main")
	assert.Equal(t, sha3, v.SHA)
}
