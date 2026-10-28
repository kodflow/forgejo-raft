// Copyright 2026 The Forgejo Authors. All rights reserved.
// SPDX-License-Identifier: GPL-3.0-or-later

package cluster

import (
	"strings"
	"testing"

	repo_model "forgejo.org/models/repo"
	"forgejo.org/services/cluster/reftable"

	"github.com/stretchr/testify/assert"
)

// refHistoryOf is a table that answers tagDeletedSince with one value.
type refHistoryOf struct {
	lifecycleTable
	value     reftable.RefValue
	held      bool
	rewritten bool
}

func (h refHistoryOf) Get(string, string) (reftable.RefValue, bool) { return h.value, h.held }

func (h refHistoryOf) RewrittenSince(string, uint64) bool { return h.rewritten }

// The table decides that a created or moved tag is gone only on positive
// evidence: a tombstone written after the event, under the event's key,
// which neither a rename nor a deletion of the repository rewrote since,
// and which still is the repository's name. Anything else leaves it to
// git, as before.
func TestTagDeletedSinceReadsOnlyALaterDeletionOfTheRef(t *testing.T) {
	repo := &repo_model.Repository{ID: 1, OwnerName: "User2", Name: "Repo1"}
	ev := reftable.Event{Kind: reftable.EventRef, Repo: laggingRepo, Ref: "refs/tags/v1", New: strings.Repeat("1", 40), Rev: 4}
	ev.Token.Index = 10
	deleted := reftable.RefValue{Rev: 5, Tombstone: true}

	assert.True(t, tagDeletedSince(refHistoryOf{value: deleted, held: true}, repo, ev), "deleted after the event")
	assert.False(t, tagDeletedSince(nil, repo, ev), "no table")
	assert.False(t, tagDeletedSince(refHistoryOf{held: false}, repo, ev), "a ref the table does not hold")
	assert.False(t, tagDeletedSince(refHistoryOf{value: reftable.RefValue{Rev: 6, SHA: ev.New}, held: true}, repo, ev), "live")
	assert.False(t, tagDeletedSince(refHistoryOf{value: reftable.RefValue{Rev: 4, Tombstone: true}, held: true}, repo, ev), "not written after the event")
	assert.False(t, tagDeletedSince(refHistoryOf{value: deleted, held: true, rewritten: true}, repo, ev), "a rename or a deletion of the repository since")
	renamed := &repo_model.Repository{ID: 1, OwnerName: "user2", Name: "repo1-renamed"}
	assert.False(t, tagDeletedSince(refHistoryOf{value: deleted, held: true}, renamed, ev), "the repository has another name now")
}
