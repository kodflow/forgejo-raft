// Copyright 2026 The Forgejo Authors. All rights reserved.
// SPDX-License-Identifier: GPL-3.0-or-later

package cluster

import (
	"slices"

	"forgejo.org/services/cluster/consensus"
)

// The voter rule of the autonomous mode ([cluster] EXPECTED_NODES): every
// live member votes. EXPECTED_NODES only sizes the founding and PEERS;
// afterwards the voters follow the members, which the autopilot keeps to
// the live ones by removing a member unreachable for DEAD_MEMBER_TIMEOUT.
//
// The removal stops at three voters: with three voters and one dead, the
// two others are a majority and every write still lands on two disks; with
// one left, the cluster is read-only until a member returns. Below three
// members (while the cluster forms) nobody is promoted: a learner of a pair
// promoted on a cut link would make two writers.
//
// Seniority is the order of the committed membership: the founders first,
// by id, then each node in the order it was added as a learner. Every
// member reads the same membership, so every member picks the same learner
// to promote first; nothing else is recorded.

// minVotingMembers is the fewest members among which every member votes.
const minVotingMembers = 3

// VoterTarget is how many voters a membership of members keeps: all of
// them from three, one below.
func VoterTarget(members int) int {
	if members >= minVotingMembers {
		return members
	}
	return min(members, 1)
}

// Rank is a member's seniority, 1 for the eldest, 0 when it is not a member.
func Rank(m consensus.Membership, id consensus.NodeID) int {
	return slices.IndexFunc(m.Members, func(x consensus.Member) bool { return x.ID == id }) + 1
}

// NextPromotion names the learner to promote: the eldest one, from three
// members only.
func NextPromotion(m consensus.Membership) (consensus.NodeID, bool) {
	if len(m.Members) < minVotingMembers {
		return 0, false
	}
	for _, x := range m.Members {
		if !x.Voter {
			return x.ID, true
		}
	}
	return 0, false
}

// Removable reports whether a dead member may be removed: only while at
// least three voters remain after it. With three voters and one dead, the
// two others are a majority already: removing it would buy no availability
// and halve the copies of each write.
func Removable(m consensus.Membership, id consensus.NodeID) bool {
	i := Rank(m, id) - 1
	if i < 0 {
		return false
	}
	voters := m.Voters()
	if m.Members[i].Voter {
		voters--
	}
	return voters >= minVotingMembers
}
