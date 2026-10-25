// Copyright 2026 The Forgejo Authors. All rights reserved.
// SPDX-License-Identifier: GPL-3.0-or-later

package cluster

import (
	"context"
	"strconv"
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

// interposingEngine runs before, once, ahead of the first rename it is
// asked to propose: what another node commits between the proposer's
// check of its table and its own entry.
type interposingEngine struct {
	consensus.Engine
	once   sync.Once
	before func()
}

func (e *interposingEngine) Propose(ctx context.Context, data []byte) (consensus.Token, any, error) {
	if len(data) > 1 && reftable.Kind(data[1]) == reftable.KindRepoRename {
		e.once.Do(e.before)
	}
	return e.Engine.Propose(ctx, data)
}

// A repository's rename whose source is live in its proposer's confirmed
// table, and that commits after an owner's rename moved the source away
// through another node: no check of the proposer sees what commits
// between its read and its entry, so the proposer's refusal alone leaves
// the window of #220 open, and only Apply, which reads the table at the
// entry's index, closes it. Below reftable.RenameSourceVersion Apply takes
// the rename as it always did, moving nothing, alike on every member; a
// member that applies version 8 only holds the raise, and nothing is
// written at version 9 meanwhile. Once every member applies it and the
// version is raised, Apply refuses the same rename, for certain, on every
// member, and nothing moves.
func TestARenameWhoseSourceLeftAfterItsCheckIsRefusedFromItsVersion(t *testing.T) {
	m := consensus.Membership{Members: []consensus.Member{{ID: 1}, {ID: 2}}}
	mixed := []hello{{Node: consensus.NodeID(2).String(), Payload: reftable.RenameSourceVersion - 1}}
	assert.False(t, everyMemberApplies(m, 1, mixed, anyAnswer), "a member applying version 8 holds the raise")

	mem, rs := NewForTesting(t.TempDir(), "node-a", "node-b", "node-c", "node-d", "node-e")
	ctx := context.Background()
	leader, follower := rs[0], rs[2]
	mem.SetLeader(leader.Engine().(*memraft.Node))
	raiseClusterTo(t, leader, reftable.RenameSourceVersion-1, rs...)
	probe := leader.memberProbe
	older := true
	leader.memberProbe = func(ctx context.Context, addr, slot string) (hello, error) {
		h, err := probe(ctx, addr, slot)
		if slot == "node-e" && older {
			h.Payload = reftable.RenameSourceVersion - 1
		}
		return h, err
	}
	err := leader.Operate(ctx, OperatorFor(1), Operation{Action: ActionRaisePayload, Target: strconv.Itoa(reftable.PayloadVersion)})
	require.ErrorContains(t, err, "not raised")
	require.Equal(t, uint8(reftable.RenameSourceVersion-1), leader.table.WriteVersion())

	// race renames owner/repo to org/repo through the follower, its check
	// passed, while the owner's rename to newOwner commits through the
	// leader just before the follower's entry.
	race := func(owner, newOwner, repo string) error {
		createKeys(t, leader, []string{owner + "/" + repo + ".git"})
		real := follower.Engine()
		follower.setEngine(&interposingEngine{Engine: real, before: func() {
			require.NoError(t, leader.proposeOwnerRename(ctx, owner, newOwner))
			applied, _ := leader.table.Applied()
			require.Eventually(t, func() bool { pos, _ := follower.table.Applied(); return pos.Index >= applied.Index }, 10*time.Second, 10*time.Millisecond)
		}})
		defer follower.setEngine(real)
		return follower.proposeRepoMoves(ctx, [][2]string{
			{owner + "/" + repo + ".git", "org/" + repo + ".git"},
			{owner + "/" + repo + ".wiki.git", "org/" + repo + ".wiki.git"},
		})
	}
	live := func(r *Replicator, key string) bool {
		refs, _ := r.table.Refs(key)
		return anyLive(refs)
	}

	// Version 8: recorded, and nothing moved, on every member alike.
	require.NoError(t, race("alice", "bob", "x"), "below version 9 the rename is taken as before")
	for _, r := range rs {
		slot := r.Engine().Status().Slot
		from, _, marked := r.table.RenamedFrom("org/x.git")
		assert.True(t, marked && from == "alice/x.git", "%s: the rename is recorded", slot)
		assert.False(t, live(r, "org/x.git"), "%s: and moves nothing", slot)
		assert.True(t, live(r, "bob/x.git"), slot)
		assert.Equal(t, leader.table.Root(), r.table.Root(), slot)
	}
	applied, _ := leader.table.Applied()
	entries, err := leader.Engine().(*memraft.Node).ReadCommitted(ctx, 1, applied.Index, 1<<20)
	require.NoError(t, err)
	for _, e := range entries {
		require.Less(t, e.Data[0], uint8(reftable.RenameSourceVersion), "entry %d is written at a version node-e cannot apply", e.Index)
	}

	// node-e runs this binary now: the version is raised, and Apply refuses
	// the same race.
	older = false
	require.NoError(t, leader.Operate(ctx, OperatorFor(1), Operation{Action: ActionRaisePayload, Target: strconv.Itoa(reftable.PayloadVersion)}))
	for _, r := range rs {
		require.Equal(t, uint8(reftable.PayloadVersion), r.table.WriteVersion())
	}
	err = race("carol", "dave", "y")
	t.Logf("the rename through the follower, from version 9: %v", err)
	require.Error(t, err)
	assert.False(t, clusterhook.IsOutcomeUnresolved(err), "the refusal is certain: Forgejo moves the directories back")
	require.ErrorContains(t, err, "the repository carol/y.git was renamed to dave/y.git")
	e := lastEntry(t, leader.Engine().(*memraft.Node))
	assert.Equal(t, []byte{reftable.PayloadVersion, byte(reftable.KindRepoRename)}, e.Data[:2], "Apply refused it")
	for _, r := range rs {
		slot := r.Engine().Status().Slot
		_, _, marked := r.table.RenamedFrom("org/y.git")
		assert.False(t, marked, "%s: no rename into org/y", slot)
		assert.True(t, live(r, "dave/y.git"), slot)
		assert.Equal(t, leader.table.Root(), r.table.Root(), slot)
	}
}
