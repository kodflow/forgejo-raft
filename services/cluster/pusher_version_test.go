// Copyright 2026 The Forgejo Authors. All rights reserved.
// SPDX-License-Identifier: GPL-3.0-or-later

package cluster

import (
	"context"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"forgejo.org/modules/clusterhook"
	"forgejo.org/services/cluster/consensus"
	"forgejo.org/services/cluster/reftable"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// A mixed cluster: while one member's binary applies version 3 only, the
// cluster keeps writing version 3 and no pusher name travels; once every
// member applies version 4 and the version is raised, the name travels in
// the entry and every member holds the same events.
func TestPusherNameWaitsForEveryMember(t *testing.T) {
	m := consensus.Membership{Members: []consensus.Member{{ID: 1}, {ID: 2}, {ID: 3}}}
	mixed := []hello{{Node: consensus.NodeID(2).String(), Payload: reftable.PayloadVersion}, {Node: consensus.NodeID(3).String(), Payload: reftable.RevokeVersion}}
	assert.False(t, everyMemberApplies(m, 1, mixed, anyAnswer), "a member applying version 3 holds the raise")

	tc := newTestCluster(t, "node-a", "node-b", "node-c")
	work := t.TempDir()
	run(t, work, "init", "-q")
	c1 := commit(t, work, "one")
	c2 := commit(t, work, "two")
	zero := strings.Repeat("0", 40)
	n := tc.nodes[0]
	repo := "alice/pn.git"
	require.True(t, tc.pushVia(n, work, repo, "refs/heads/main", zero, c1).OK)
	dir := filepath.Join(n.r.root, filepath.FromSlash(repo))
	run(t, work, "push", "-q", dir, c2+":refs/tmp/incoming")
	run(t, dir, "update-ref", "-d", "refs/tmp/incoming")

	named := func(ref, old, sha string) {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		req := &clusterhook.Request{
			Op: clusterhook.OpPrepared, Nonce: clusterhook.NewNonce(), GitDir: dir, RepoID: 1, PusherID: 2, PusherName: "alice",
			Lines: []clusterhook.Line{{Old: old, New: sha, Ref: ref}},
		}
		lock := holdRefLock(t, dir, ref, sha)
		resp := n.r.Handle(ctx, req)
		lock()
		require.True(t, resp.OK, resp.Message)
		req.Op = clusterhook.OpCommitted
		n.r.Handle(ctx, req)
	}
	eventsOf := func(nd *testNode, ref string) []reftable.Event {
		var out []reftable.Event
		for _, ev := range nd.r.table.Events() {
			if ev.Repo == repo && ev.Ref == ref {
				out = append(out, ev)
			}
		}
		return out
	}
	settle := func(ref string) {
		for _, nd := range tc.nodes {
			require.Eventually(t, func() bool { return len(eventsOf(nd, ref)) == 1 }, 10*time.Second, 10*time.Millisecond, nd.slot)
		}
	}

	require.Less(t, n.r.table.WriteVersion(), uint8(reftable.PusherVersion))
	named("refs/heads/before", zero, c2)
	settle("refs/heads/before")
	for _, nd := range tc.nodes {
		assert.Empty(t, eventsOf(nd, "refs/heads/before")[0].PusherName, "%s: not written before every member applies it", nd.slot)
	}

	require.NoError(t, tc.nodes[1].r.RaisePayloadVersion(context.Background(), "op"))
	for _, nd := range tc.nodes {
		require.Eventually(t, func() bool { return nd.r.table.WriteVersion() >= reftable.PusherVersion }, 10*time.Second, 10*time.Millisecond, nd.slot)
	}
	named("refs/heads/after", zero, c2)
	settle("refs/heads/after")
	want := eventsOf(n, "refs/heads/after")[0]
	assert.Equal(t, "alice", want.PusherName)
	for _, nd := range tc.nodes {
		assert.Equal(t, want, eventsOf(nd, "refs/heads/after")[0], nd.slot)
	}
}
