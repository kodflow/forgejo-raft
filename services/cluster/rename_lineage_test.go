// Copyright 2026 The Forgejo Authors. All rights reserved.
// SPDX-License-Identifier: GPL-3.0-or-later

package cluster

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"forgejo.org/modules/clusterhook"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// A ref deleted, then the repository renamed (once, or twice in a chain),
// while a node is down: the node moves the old directory in at its start,
// and must not serve the deleted ref under the new name, nor report it
// matched, nor import it back (#96).
func TestADeletedRefDoesNotSurviveARenameOnALaggingNode(t *testing.T) {
	for _, tt := range []struct {
		name  string
		ref   string
		chain bool
	}{
		{"branch", "refs/heads/feature", false},
		{"tag", "refs/tags/v1", false},
		{"branch, chain of renames", "refs/heads/feature", true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			tc := newTestCluster(t, "node-a", "node-b", "node-c")
			work := t.TempDir()
			run(t, work, "init", "-q")
			c1 := commit(t, work, "one")
			c2 := commit(t, work, "two")
			zero := strings.Repeat("0", 40)
			origin, lag := tc.nodes[0], tc.nodes[2]
			require.True(t, tc.pushVia(origin, work, "s/a.git", "refs/heads/main", zero, c1).OK)
			require.True(t, tc.pushVia(origin, work, "s/a.git", tt.ref, zero, c2).OK)
			require.Eventually(t, func() bool { return gitRef(t, lag, "s/a.git", tt.ref) == c2 }, 10*time.Second, 20*time.Millisecond)

			lag.r.projector.refence()
			root := origin.r.root
			odir := filepath.Join(root, "s", "a.git")
			req := &clusterhook.Request{
				Op: clusterhook.OpPrepared, Nonce: clusterhook.NewNonce(), GitDir: odir, RepoID: 1, PusherID: 2,
				Lines: []clusterhook.Line{{Old: c2, New: zero, Ref: tt.ref}},
			}
			unlock := holdRefLock(t, odir, tt.ref, "")
			resp := origin.r.Handle(t.Context(), req)
			unlock()
			require.True(t, resp.OK, resp.Message)
			run(t, odir, "update-ref", "-d", tt.ref)
			req.Op = clusterhook.OpCommitted
			origin.r.Handle(t.Context(), req)

			final := "s/b.git"
			require.NoError(t, os.Rename(odir, filepath.Join(root, "s", "b.git")))
			require.NoError(t, origin.r.proposeRepoRename(t.Context(), "s/a.git", "s/b.git"))
			if tt.chain {
				final = "s/c.git"
				require.NoError(t, os.Rename(filepath.Join(root, "s", "b.git"), filepath.Join(root, "s", "c.git")))
				require.NoError(t, origin.r.proposeRepoRename(t.Context(), "s/b.git", "s/c.git"))
			}
			applied, _ := origin.r.table.Applied()
			require.Eventually(t, func() bool {
				pos, _ := lag.r.table.Applied()
				return pos.Index >= applied.Index
			}, 10*time.Second, 20*time.Millisecond)

			lag.r.projector.unfence()
			require.Eventually(t, lag.r.projector.Ready, 15*time.Second, 20*time.Millisecond, "node-c ready")
			assert.Equal(t, c1, gitRef(t, lag, final, "refs/heads/main"))
			assert.Empty(t, gitRef(t, lag, final, tt.ref), "the deleted ref is served under %s", final)
			same, err := lag.r.projector.Verify(t.Context(), final)
			require.NoError(t, err)
			assert.True(t, same)
			_, err = lag.r.Import(t.Context(), final)
			require.NoError(t, err)
			v, ok := lag.r.table.Get(final, tt.ref)
			assert.False(t, ok && v.Live(), "the deleted ref is imported back")
		})
	}
}
