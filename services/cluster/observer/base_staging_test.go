// Copyright 2026 The Forgejo Authors. All rights reserved.
// SPDX-License-Identifier: GPL-3.0-or-later

package observer

import (
	"context"
	"os"
	"testing"

	"forgejo.org/modules/clusterhook"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// A base backup the scan refuses, a truncated one here, leaves nothing in
// pg/staging: passes that refuse one never fill the quota, and a base that
// verifies is taken as soon as one arrives (#112).
func TestRefusedBaseLeavesNothingInStaging(t *testing.T) {
	ctx := context.Background()
	c := newFakeCluster(t)
	o := newTestObserver(t, c, &hookLog{})
	truncate := true
	c.tamper = func(_ string, req *clusterhook.ExportRequest, data []byte) []byte {
		if req.Op == clusterhook.OpExportPGBase && truncate {
			return data[:600]
		}
		return data
	}
	c.push("alice/demo.git", "refs/heads/main", c.commit("one"))
	c.checkpoint()
	for range 3 {
		err := o.Step(ctx)
		require.ErrorContains(t, err, "the base backup failed")
		assert.Zero(t, o.store.stagingSize())
		entries, err := os.ReadDir(o.store.path("pg", "staging"))
		require.NoError(t, err)
		assert.Empty(t, entries)
		assert.Empty(t, o.State().Bases)
	}
	truncate = false
	require.ErrorIs(t, o.Step(ctx), ErrNoCoverage)
	require.Len(t, o.State().Bases, 1)
	c.push("alice/demo.git", "refs/heads/main", c.commit("two"))
	c.checkpoint()
	require.NoError(t, o.Step(ctx))
}

// A label is free only if neither a base nor a base in staging has it.
func TestBaseLabelSkipsStaging(t *testing.T) {
	c := newFakeCluster(t)
	o := newTestObserver(t, c, &hookLog{})
	require.NoError(t, os.MkdirAll(o.store.path("pg", "staging", "20260101T000000Z"), 0o700))
	free, err := o.freeLabel("20260101T000000Z")
	require.NoError(t, err)
	assert.False(t, free)
	free, err = o.freeLabel("20260101T000001Z")
	require.NoError(t, err)
	assert.True(t, free)
}
