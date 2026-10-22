// Copyright 2026 The Forgejo Authors. All rights reserved.
// SPDX-License-Identifier: GPL-3.0-or-later

package cluster

import (
	"path/filepath"
	"strings"
	"testing"
	"time"

	"forgejo.org/modules/clusterhook"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// A node behind while a branch is deleted and a branch under its name
// created (a, then a/b), or the reverse, projects both at once: git
// refuses that swap of a file and a directory in one transaction, and the
// node must still converge and be ready after its start (#73).
func TestALaggingNodeConvergesOnADirectoryAndFileSwap(t *testing.T) {
	for _, tt := range []struct {
		name          string
		before, after string
		packed        bool
	}{
		{"file then dir", "refs/heads/a", "refs/heads/a/b", false},
		{"dir then file", "refs/heads/a/b", "refs/heads/a", false},
		{"file then dir, packed", "refs/heads/a", "refs/heads/a/b", true},
		{"dir then file, packed", "refs/heads/a/b", "refs/heads/a", true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			tc := newTestCluster(t, "node-a", "node-b", "node-c")
			work := t.TempDir()
			run(t, work, "init", "-q")
			c1 := commit(t, work, "one")
			c2 := commit(t, work, "two")
			zero := strings.Repeat("0", 40)
			origin, peer, lag := tc.nodes[0], tc.nodes[1], tc.nodes[2]
			require.True(t, tc.pushVia(origin, work, "dfr/r.git", "refs/heads/main", zero, c1).OK)
			require.True(t, tc.pushVia(origin, work, "dfr/r.git", tt.before, zero, c1).OK)
			require.Eventually(t, func() bool { return gitRef(t, lag, "dfr/r.git", tt.before) == c1 }, 10*time.Second, 20*time.Millisecond)
			if tt.packed {
				run(t, filepath.Join(lag.r.root, "dfr", "r.git"), "pack-refs", "--all")
			}

			// node-c stops projecting, as a node down or restarting.
			lag.r.projector.refence()
			odir := filepath.Join(origin.r.root, "dfr", "r.git")
			req := &clusterhook.Request{
				Op: clusterhook.OpPrepared, Nonce: clusterhook.NewNonce(), GitDir: odir, RepoID: 1,
				Lines: []clusterhook.Line{{Old: c1, New: zero, Ref: tt.before}},
			}
			resp := origin.r.Handle(t.Context(), req)
			require.True(t, resp.OK, resp.Message)
			run(t, odir, "update-ref", "-d", tt.before)
			req.Op = clusterhook.OpCommitted
			origin.r.Handle(t.Context(), req)
			require.True(t, tc.pushVia(origin, work, "dfr/r.git", tt.after, zero, c2).OK)
			require.Eventually(t, func() bool { return gitRef(t, peer, "dfr/r.git", tt.after) == c2 }, 10*time.Second, 20*time.Millisecond)

			lag.r.projector.unfence()
			require.Eventually(t, lag.r.projector.Ready, 15*time.Second, 20*time.Millisecond, "node-c ready: %+v", lag.r.projector.Lag())
			assert.Equal(t, c2, gitRef(t, lag, "dfr/r.git", tt.after))
			assert.Empty(t, gitRef(t, lag, "dfr/r.git", tt.before))
			assert.Empty(t, lag.r.projector.Lag())
		})
	}
}
