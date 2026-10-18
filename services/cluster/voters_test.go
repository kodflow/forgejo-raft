// Copyright 2026 The Forgejo Authors. All rights reserved.
// SPDX-License-Identifier: GPL-3.0-or-later

package cluster

import (
	"testing"

	"forgejo.org/services/cluster/consensus"

	"github.com/stretchr/testify/assert"
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
