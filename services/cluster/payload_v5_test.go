// Copyright 2026 The Forgejo Authors. All rights reserved.
// SPDX-License-Identifier: GPL-3.0-or-later

package cluster

import (
	"context"
	"os"
	"path/filepath"
	"strconv"
	"testing"

	"forgejo.org/services/cluster/consensus"
	"forgejo.org/services/cluster/reftable"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Outside the autonomous mode an operator's raise of the payload version
// checked no member: two members of three on an older binary stopped on
// the first entry of the new version, and the quorum was lost (#50). Every
// member is asked now, and the raise waits for all of them.
func TestManualRaiseAsksEveryMember(t *testing.T) {
	r, e, _ := newLeaderBench(t, 3)
	r.auto = nil
	e.join(consensus.Member{ID: 0xb, Slot: "node-b", SPKI: [32]byte{0xb}, Voter: true})
	e.join(consensus.Member{ID: 0xc, Slot: "node-c", SPKI: [32]byte{0xc}, Voter: true})
	answer := func(payload int, silent string) func(context.Context, string, string) (hello, error) {
		return func(_ context.Context, _, slot string) (hello, error) {
			if slot == silent {
				return hello{}, consensus.ErrNotLeader
			}
			id := map[string]consensus.NodeID{"node-b": 0xb, "node-c": 0xc}[slot]
			return hello{Slot: slot, State: helloMember, Node: id.String(), Payload: payload, SPKI: [32]byte{byte(id)}}, nil
		}
	}
	raise := func() error {
		return r.operate(t.Context(), e, "op", Operation{Action: ActionRaisePayload, Target: strconv.Itoa(reftable.PayloadVersion)})
	}

	r.memberProbe = answer(reftable.PayloadVersion-1, "")
	require.ErrorContains(t, raise(), "not raised", "a member on an older binary")
	r.memberProbe = answer(reftable.PayloadVersion, "node-c")
	require.ErrorContains(t, raise(), "not raised", "a member that does not answer, as an older binary")
	assert.Equal(t, uint8(reftable.BaseVersion), r.table.WriteVersion())

	// Every member applies it: the raise is proposed (this bench's engine
	// refuses every proposal, with certainty).
	r.memberProbe = answer(reftable.PayloadVersion, "")
	err := raise()
	require.ErrorIs(t, err, consensus.ErrEntryTooLarge)
}

// A member's hello outside the autonomous mode names its slot, its id and
// the version its binary applies.
func TestMemberHelloOutsideTheAutonomousMode(t *testing.T) {
	r, _, _ := newLeaderBench(t, 3)
	r.auto = nil
	h := r.memberHello()
	assert.Equal(t, reftable.PayloadVersion, h.Payload)
	assert.Equal(t, consensus.NodeID(0xa).String(), h.Node)
	assert.Equal(t, helloMember, h.State)
}

// A repository whose name starts with a dot was skipped by the import in
// silence, the report reading complete (#54). Until the cluster writes
// version 5, it is counted and named; the hook refuses it before git
// writes anything, and so does the creation, before git init.
func TestHuntR2ProveDotnameImport(t *testing.T) {
	root := t.TempDir()
	r := newReplicator(root, newRegistry(0, 1, 1))
	require.NoError(t, os.MkdirAll(filepath.Join(root, "user4", ".profile.git"), 0o755))
	rep, err := r.Import(t.Context(), "")
	require.NoError(t, err)
	assert.Equal(t, 1, rep.Skipped)
	require.Len(t, rep.Failures, 1)
	assert.Contains(t, rep.Failures[0], "user4/.profile.git")
	assert.Equal(t, 0, rep.Repos)

	_, err = r.repoKey(filepath.Join(root, "user4", ".profile.git"))
	require.Error(t, err)
	err = r.writableKey("user4/.profile.git")
	require.ErrorContains(t, err, "raised to 5")
	require.Error(t, r.writableKey("user4/..git"))
	require.NoError(t, r.writableKey("user4/profile.git"))
}

// From version 5 the cluster takes such a name, from the creation on.
func TestHuntR2ProveDotnameCreate(t *testing.T) {
	r := newReplicator(t.TempDir(), newRegistry(0, 1, 1))
	eng := &dropEngine{table: r.table}
	r.setEngine(eng)
	eng.propose(t, reftable.BaseVersion, reftable.KindAudit, &reftable.Audit{Operator: "op", Action: reftable.AuditPayloadVersion, Target: strconv.Itoa(reftable.GenerationVersion), Outcome: reftable.AuditRaised})
	require.NoError(t, r.writableKey("user4/.profile.git"))
	require.NoError(t, r.writableKey("user4/.profile.wiki.git"))
	require.Error(t, r.writableKey("user4/..git"))
	key, err := r.repoKey(filepath.Join(r.root, "user4", ".profile.git"))
	require.NoError(t, err)
	assert.Equal(t, "user4/.profile.git", key)
}
