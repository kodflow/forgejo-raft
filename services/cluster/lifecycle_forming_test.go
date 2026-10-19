// Copyright 2026 The Forgejo Authors. All rights reserved.
// SPDX-License-Identifier: GPL-3.0-or-later

package cluster

import (
	"context"
	"testing"
	"time"

	"forgejo.org/modules/test"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// While the autonomous cluster forms, a repository deletion or rename is
// refused like a push: committed on the founder's disk alone, after SQL
// already changed, it would be lost with that disk.
func TestLifecycleWaitsForTheClusterToForm(t *testing.T) {
	defer test.MockVariableValue(&writeLeaderWait, 300*time.Millisecond)()
	_, rs := NewForTesting(t.TempDir(), "node-a")
	r := rs[0]
	r.auto = &autoMode{expected: 3, self: "node-a", peers: map[string]string{"node-a": "a:7480", "node-b": "b:7480", "node-c": "c:7480"}}
	ctx := context.Background()

	err := r.proposeRepoDelete(ctx, "alice/a.git", 1)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "still forming")
	_, deleted := r.table.Deleted("alice/a.git")
	assert.False(t, deleted)

	err = r.proposeRepoRename(ctx, "alice/a.git", "alice/b.git")
	require.ErrorContains(t, err, "still forming")
	err = r.proposeOwnerRename(ctx, "alice", "bob")
	assert.NoError(t, err, "nothing to move")

	// Formed (the manual mode has no forming phase): recorded.
	r.auto = nil
	require.NoError(t, r.proposeRepoDelete(ctx, "alice/a.git", 1))
	_, deleted = r.table.Deleted("alice/a.git")
	assert.True(t, deleted)
}
