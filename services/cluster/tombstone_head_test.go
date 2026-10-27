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

// A repository with a symbolic HEAD in the table, renamed twice, renamed
// and back, or renamed and its target deleted, while a node is down: every
// key the renames left (all tombstones, HEAD included) must certify on that
// node, the repository reach its final name with its directory, the freed
// name hold nothing, and the node be ready after a start (#43).
func TestKeysAllTombstonesCertifyAfterRenamesOnALaggingNode(t *testing.T) {
	t.Parallel()
	for _, tt := range []struct {
		name  string
		steps func(t *testing.T, tc *testCluster) (final, freed string)
	}{
		{"renamed twice", func(t *testing.T, tc *testCluster) (string, string) {
			renameOn(t, tc.nodes[0], "s/a.git", "s/b.git")
			renameOn(t, tc.nodes[0], "s/b.git", "s/c.git")
			return "s/c.git", "s/b.git"
		}},
		{"renamed and back", func(t *testing.T, tc *testCluster) (string, string) {
			renameOn(t, tc.nodes[0], "s/a.git", "s/b.git")
			renameOn(t, tc.nodes[0], "s/b.git", "s/a.git")
			return "s/a.git", "s/b.git"
		}},
		{"renamed then deleted", func(t *testing.T, tc *testCluster) (string, string) {
			renameOn(t, tc.nodes[0], "s/a.git", "s/b.git")
			require.NoError(t, os.RemoveAll(filepath.Join(tc.nodes[0].r.root, "s", "b.git")))
			require.NoError(t, tc.nodes[0].r.proposeRepoDelete(t.Context(), "s/b.git", 1))
			return "", "s/a.git"
		}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			tc := newTestCluster(t, "node-a", "node-b", "node-c")
			work := t.TempDir()
			run(t, work, "init", "-q")
			c1 := commit(t, work, "one")
			zero := strings.Repeat("0", 40)
			origin, lag := tc.nodes[0], tc.nodes[2]
			require.True(t, tc.pushVia(origin, work, "s/a.git", "refs/heads/main", zero, c1).OK)
			odir := filepath.Join(origin.r.root, "s", "a.git")
			req := &clusterhook.Request{
				Op: clusterhook.OpPrepared, Nonce: clusterhook.NewNonce(), GitDir: odir, RepoID: 1,
				Lines: []clusterhook.Line{{Old: zero, New: "ref:refs/heads/main", Ref: "HEAD"}},
			}
			resp := origin.r.Handle(t.Context(), req)
			require.True(t, resp.OK, resp.Message)
			run(t, odir, "symbolic-ref", "HEAD", "refs/heads/main")
			req.Op = clusterhook.OpCommitted
			origin.r.Handle(t.Context(), req)
			v, ok := origin.r.table.Get("s/a.git", "HEAD")
			require.True(t, ok && v.Live(), "HEAD is in the table")
			require.Eventually(t, func() bool {
				return gitRef(t, lag, "s/a.git", "refs/heads/main") == c1 && lag.r.projector.Projected("s/a.git") > 0
			}, 10*time.Second, 20*time.Millisecond)
			before, err := os.Stat(filepath.Join(lag.r.root, "s", "a.git"))
			require.NoError(t, err)

			lag.r.projector.refence()
			final, freed := tt.steps(t, tc)
			applied, _ := origin.r.table.Applied()
			require.Eventually(t, func() bool {
				pos, _ := lag.r.table.Applied()
				return pos.Index >= applied.Index
			}, 10*time.Second, 20*time.Millisecond)

			lag.r.projector.unfence()
			require.Eventually(t, func() bool { return lag.r.projector.Ready() && len(lag.r.projector.Lag()) == 0 },
				15*time.Second, 20*time.Millisecond, "node-c ready")
			if final != "" {
				assert.Equal(t, c1, gitRef(t, lag, final, "refs/heads/main"))
				refs, err := readGitRefs(t.Context(), filepath.Join(lag.r.root, filepath.FromSlash(final)))
				require.NoError(t, err)
				assert.Equal(t, "refs/heads/main", refs["HEAD"].Symbolic, "HEAD of the final name")
				after, err := os.Stat(filepath.Join(lag.r.root, filepath.FromSlash(final)))
				require.NoError(t, err)
				assert.True(t, os.SameFile(before, after), "the repository's own directory reached %s", final)
			}
			_, err = os.Stat(filepath.Join(lag.r.root, filepath.FromSlash(freed)))
			assert.True(t, os.IsNotExist(err), "%s holds a directory on node-c", freed)
			if final != "" {
				_, err = os.Stat(filepath.Join(lag.r.root, staleDirName))
				assert.True(t, os.IsNotExist(err), "nothing set aside")
			}

			// And again after another start.
			lag.r.projector.unfence()
			require.Eventually(t, func() bool { return lag.r.projector.Ready() && len(lag.r.projector.Lag()) == 0 },
				15*time.Second, 20*time.Millisecond, "node-c ready again")
		})
	}
}

// renameOn renames a repository on node n as Forgejo does: the keys of
// the repository and of its wiki, under both names, held on n
// (clusterhook.HoldRepoDirectories), then the directory, then the entry,
// and the hold released once the entry returned. Without the hold, a fetch
// of n's projector into the old key that fell between the move and the
// entry created its directory again, and the next rename to that name met
// it, "file exists" (#274).
func renameOn(t *testing.T, n *testNode, from, to string) {
	t.Helper()
	root := n.r.root
	wiki := func(key string) string { return strings.TrimSuffix(key, ".git") + ".wiki.git" }
	release, err := n.r.holdOwnerDirectories(t.Context(), []string{from, wiki(from), to, wiki(to)})
	require.NoError(t, err)
	defer release()
	require.NoError(t, os.Rename(filepath.Join(root, filepath.FromSlash(from)), filepath.Join(root, filepath.FromSlash(to))))
	require.NoError(t, n.r.proposeRepoRename(t.Context(), from, to))
}
