// Copyright 2026 The Forgejo Authors. All rights reserved.
// SPDX-License-Identifier: GPL-3.0-or-later

package cluster

import (
	"slices"
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
	all := func(consensus.Member) bool { return true }
	// The eldest learner first.
	assert.Equal(t, []consensus.NodeID{4}, NextPromotion(membership(true, true, true, false, false), all))
	assert.Empty(t, NextPromotion(membership(true, true, true), all), "everyone votes")
	// Never below three members.
	assert.Empty(t, NextPromotion(membership(true, false), all))
	assert.Empty(t, NextPromotion(membership(false), all))
	assert.Equal(t, 3, Rank(membership(true, true, false), 3))
	assert.Zero(t, Rank(membership(true), 9))
}

// From one voter, two learners are promoted together or none: the cluster
// never rests on two voters, whose second one's loss would leave it with no
// leader for good (#92). A learner that is not ready, dead or behind, never
// holds back the learners after it (#94).
func TestPromotionSkipsALearnerNotReadyAndNeverStopsAtTwoVoters(t *testing.T) {
	ready := func(ids ...consensus.NodeID) func(consensus.Member) bool {
		return func(x consensus.Member) bool { return slices.Contains(ids, x.ID) }
	}
	forming := membership(true, false, false)
	assert.Equal(t, []consensus.NodeID{2, 3}, NextPromotion(forming, ready(2, 3)), "both learners at once")
	assert.Empty(t, NextPromotion(forming, ready(3)), "one ready learner would make two voters")
	assert.Empty(t, NextPromotion(forming, ready(2)))
	assert.Equal(t, []consensus.NodeID{3, 4}, NextPromotion(membership(true, false, false, false), ready(3, 4)), "the dead eldest is skipped")
	assert.Equal(t, []consensus.NodeID{5}, NextPromotion(membership(true, true, true, false, false), ready(5)), "a lagging eldest is skipped")
	assert.Equal(t, []consensus.NodeID{3}, NextPromotion(membership(true, true, false), ready(3)), "a cluster already at two voters reaches three")
}

func TestRemovalStopsAtThreeVoters(t *testing.T) {
	assert.True(t, Removable(membership(true, true, true, true, true), 5), "5 to 4")
	assert.True(t, Removable(membership(true, true, true, true), 4), "4 to 3")
	assert.False(t, Removable(membership(true, true, true), 3), "never below 3 voters")
	assert.True(t, Removable(membership(true, true, true, false), 4), "a learner, three voters left")
	assert.True(t, Removable(membership(true, false, false), 2), "a learner of a forming cluster: no voter goes (#94)")
	assert.True(t, Removable(membership(true, true, false), 3))
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
