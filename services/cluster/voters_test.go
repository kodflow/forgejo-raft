// Copyright 2026 The Forgejo Authors. All rights reserved.
// SPDX-License-Identifier: GPL-3.0-or-later

package cluster

import (
	"testing"

	"forgejo.org/services/cluster/consensus"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// membership builds a membership in seniority order; v marks the voters.
func membership(v ...bool) consensus.Membership {
	var m consensus.Membership
	for i, voter := range v {
		m.Members = append(m.Members, consensus.Member{ID: consensus.NodeID(i + 1), Voter: voter})
	}
	return m
}

func TestVoterTarget(t *testing.T) {
	for members, want := range map[int]int{0: 0, 1: 1, 2: 1, 3: 3, 4: 4, 5: 5} {
		assert.Equal(t, want, VoterTarget(members), "%d members", members)
	}
}

func TestPromotionFromThreeMembers(t *testing.T) {
	// The eldest learner first.
	id, ok := NextPromotion(membership(true, true, false, false))
	assert.True(t, ok)
	assert.Equal(t, consensus.NodeID(3), id)
	_, ok = NextPromotion(membership(true, true, true))
	assert.False(t, ok, "everyone votes")
	// Never below three members.
	_, ok = NextPromotion(membership(true, false))
	assert.False(t, ok)
	_, ok = NextPromotion(membership(false))
	assert.False(t, ok)
	assert.Equal(t, 3, Rank(membership(true, true, false), 3))
	assert.Zero(t, Rank(membership(true), 9))
}

func TestRemovalStopsAtThreeVoters(t *testing.T) {
	assert.True(t, Removable(membership(true, true, true, true, true), 5), "5 to 4")
	assert.True(t, Removable(membership(true, true, true, true), 4), "4 to 3")
	assert.False(t, Removable(membership(true, true, true), 3), "never below 3 voters")
	assert.True(t, Removable(membership(true, true, true, false), 4), "a learner, three voters left")
	assert.False(t, Removable(membership(true, true, false), 3))
	assert.False(t, Removable(membership(true, true, true), 9), "not a member")
}

// While the autonomous cluster forms, the founder is its only voter until
// three members are there: a write it committed alone would sit on one
// disk. Writes wait for three voters; a cluster that already had them and
// lost one keeps writing on the two left.
func TestWritesWaitForThreeVoters(t *testing.T) {
	m := func(voters, learners int) consensus.Membership {
		var ms consensus.Membership
		for i := range voters + learners {
			ms.Members = append(ms.Members, consensus.Member{ID: consensus.NodeID(i + 1), Voter: i < voters})
		}
		return ms
	}
	auto := &autoMode{expected: 3}
	require.Error(t, formingRefusal(auto, m(1, 0)))
	require.Error(t, formingRefusal(auto, m(1, 1)))
	require.Error(t, formingRefusal(auto, m(2, 1)))
	require.NoError(t, formingRefusal(auto, m(3, 0)))
	require.NoError(t, formingRefusal(auto, m(4, 1)))
	// Outside the autonomous mode the operator chose the members.
	require.NoError(t, formingRefusal(nil, m(1, 0)))
}
