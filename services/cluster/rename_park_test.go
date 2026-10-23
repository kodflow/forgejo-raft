// Copyright 2026 The Forgejo Authors. All rights reserved.
// SPDX-License-Identifier: GPL-3.0-or-later

package cluster

import (
	"context"
	"errors"
	"testing"
	"time"

	"forgejo.org/modules/test"
	"forgejo.org/services/cluster/reftable"

	"github.com/stretchr/testify/assert"
)

// A parked push of o/x, then o/x renamed to o/y, then a push to o/y: the
// later push waits behind the parked one, the order is the repository's,
// not its name's (#133).
func TestParkedOrderSurvivesRename(t *testing.T) {
	defer test.MockVariableValue(&parkedRetryEvery, time.Duration(0))()
	store, lg, parked := newSQLStore(), &memLog{}, &memParked{}
	lg.events = []reftable.Event{
		{Token: testToken(1), Kind: reftable.EventRef, Repo: "o/x.git", RepoID: 7, Ref: "refs/heads/feat", New: "aa", PusherID: 1},
		{Token: testToken(2), Kind: reftable.EventRepoRename, Repo: "o/y.git", RenamedFrom: "o/x.git", RepoID: 7},
		{Token: testToken(3), Kind: reftable.EventRef, Repo: "o/y.git", RepoID: 7, Ref: "refs/heads/feat", Old: "aa", PusherID: 1},
	}
	failFirst := true
	var order []uint64
	work := func(_ context.Context, evs []reftable.Event) error {
		if evs[0].Token.Index == 1 && failFirst {
			return errors.New("transient")
		}
		order = append(order, evs[0].Token.Index)
		return nil
	}
	c := unitConsumer(store, lg, parked, work)
	ctx := context.Background()
	for range maxAttempts + 1 {
		_ = c.pass(ctx)
	}
	failFirst = false
	_ = c.pass(ctx)
	t.Logf("work order: %v, parked left: %v", order, parked.indexes())
	assert.Equal(t, []uint64{1, 2, 3}, order, "the push of index 3 ran before the parked push of index 1")
}

// A repository created under the name of another one whose event is
// parked is not held behind it (#133).
func TestParkedOrderNotInheritedByName(t *testing.T) {
	defer test.MockVariableValue(&parkedRetryEvery, time.Hour)()
	store, lg, parked := newSQLStore(), &memLog{}, &memParked{}
	lg.events = []reftable.Event{
		{Token: testToken(1), Kind: reftable.EventRef, Repo: "o/x.git", RepoID: 7, Ref: "refs/heads/feat", New: "aa", PusherID: 1},
		{Token: testToken(2), Kind: reftable.EventRepoRename, Repo: "o/y.git", RenamedFrom: "o/x.git", RepoID: 7},
		{Token: testToken(3), Kind: reftable.EventRef, Repo: "o/x.git", RepoID: 8, Ref: "refs/heads/main", New: "bb", PusherID: 1},
	}
	var order []uint64
	work := func(_ context.Context, evs []reftable.Event) error {
		if evs[0].Token.Index == 1 {
			return errors.New("still failing")
		}
		order = append(order, evs[0].Token.Index)
		return nil
	}
	c := unitConsumer(store, lg, parked, work)
	ctx := context.Background()
	for range maxAttempts + 2 {
		_ = c.pass(ctx)
	}
	assert.Equal(t, []int64{1, 2}, parked.indexes(), "repository 7's rename waits behind its failing event")
	assert.Equal(t, []uint64{3}, order, "repository 8 under the old name is not held")
}
