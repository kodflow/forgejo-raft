// Copyright 2026 The Forgejo Authors. All rights reserved.
// SPDX-License-Identifier: GPL-3.0-or-later

package cluster

import (
	"errors"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// An init whose sync of the new directory's name failed leaves the
// directory in place, and the next user of it skips the init: the sync is
// owed until something vouches for that repository, which syncs it then.
// Certified without it, a power loss could take the repository away
// (CodeRabbit on #24).
func TestAFailedInitSyncIsDoneBeforeTheRepositoryIsVouchedFor(t *testing.T) {
	tc := newTestCluster(t, "node-a", "node-b", "node-c")
	origin, lag := tc.nodes[0], tc.nodes[2]
	parent := filepath.Join(lag.r.root, "s")
	var mu sync.Mutex
	failing, synced := true, false
	prev := syncDirFn
	t.Cleanup(func() { syncDirFn = prev })
	syncDirFn = func(d string) error {
		if d == parent {
			mu.Lock()
			defer mu.Unlock()
			if failing {
				return errors.New("EIO")
			}
			synced = true
		}
		return prev(d)
	}
	lag.r.projector.refence()
	work := t.TempDir()
	run(t, work, "init", "-q")
	c1 := commit(t, work, "one")
	require.True(t, tc.pushVia(origin, work, "s/a.git", "refs/heads/main", strings.Repeat("0", 40), c1).OK)
	// The shipment to the lagging node made the directory, and failed to
	// sync its name.
	require.Eventually(t, func() bool { return dirExists(lag, "s/a.git") }, 10*time.Second, 20*time.Millisecond)

	mu.Lock()
	failing = false
	mu.Unlock()
	lag.r.projector.unfence()
	require.Eventually(t, func() bool { return lag.r.projector.Projected("s/a.git") > 0 }, 15*time.Second, 20*time.Millisecond)
	mu.Lock()
	defer mu.Unlock()
	assert.True(t, synced, "the name of the directory is synced before it is certified")
}
