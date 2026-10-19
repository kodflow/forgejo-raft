// Copyright 2026 The Forgejo Authors. All rights reserved.
// SPDX-License-Identifier: GPL-3.0-or-later

package cluster

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"forgejo.org/modules/clusterhook"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// The limits R-D8 left: a deletion's lock that holds a value or was left
// behind, and a directory and file conflict with this node's refs, which
// the reftable backend rejects per ref with no lock to read. Each is a
// ref git rejects in part; the cluster refuses the whole push.
func TestPushGitRejectsInPartDeletionsAndNames(t *testing.T) {
	tc := newTestCluster(t, "node-a", "node-b", "node-c")
	work := t.TempDir()
	run(t, work, "init", "-q")
	c1 := commit(t, work, "one")
	c2 := commit(t, work, "two")
	zero := strings.Repeat("0", 40)
	n := tc.nodes[0]
	repo := "alice/del.git"
	require.True(t, tc.pushVia(n, work, repo, "refs/heads/main", zero, c1).OK)
	require.True(t, tc.pushVia(n, work, repo, "refs/heads/b1", zero, c1).OK)
	dir := filepath.Join(n.r.root, filepath.FromSlash(repo))
	run(t, work, "push", "-q", dir, c2+":refs/tmp/incoming")
	run(t, dir, "update-ref", "-d", "refs/tmp/incoming")

	lock := func(ref, content string, age time.Duration) func() {
		p := filepath.Join(dir, filepath.FromSlash(ref)) + ".lock"
		require.NoError(t, os.WriteFile(p, []byte(content), 0o644))
		at := time.Now().Add(-age)
		require.NoError(t, os.Chtimes(p, at, at))
		return func() { _ = os.Remove(p) }
	}
	push := func(d string, lines []clusterhook.Line) *clusterhook.Response {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		req := &clusterhook.Request{Op: clusterhook.OpPrepared, Nonce: clusterhook.NewNonce(), GitDir: d, RepoID: 1, PusherID: 2, Lines: lines}
		resp := n.r.Handle(ctx, req)
		req.Op = clusterhook.OpAborted
		n.r.Handle(ctx, req)
		return resp
	}
	lines := []clusterhook.Line{{Old: c1, New: c2, Ref: "refs/heads/main"}, {Old: c1, New: zero, Ref: "refs/heads/b1"}}

	// Another writer's update holds b1's lock: git rejects the deletion.
	u1, u2 := lock("refs/heads/main", c2+"\n", 0), lock("refs/heads/b1", c2+"\n", 0)
	resp := push(dir, lines)
	u2()
	assert.False(t, resp.OK)
	assert.Contains(t, resp.Message, "refs/heads/b1")
	// An empty lock left behind on b1.
	u2 = lock("refs/heads/b1", "", time.Hour)
	resp = push(dir, lines)
	u2()
	assert.False(t, resp.OK)
	assert.Contains(t, resp.Message, "left behind")
	// So is an old lock holding the new value of an update.
	u1()
	u1 = lock("refs/heads/main", c2+"\n", time.Hour)
	u2 = lock("refs/heads/b1", "", 0)
	resp = push(dir, lines)
	u1()
	u2()
	assert.False(t, resp.OK)
	for _, ref := range []string{"refs/heads/main", "refs/heads/b1"} {
		v, _ := n.r.table.Get(repo, ref)
		assert.Equal(t, c1, v.SHA, "%s: nothing was committed", ref)
	}

	// The reftable backend: refs/heads/x is on this node's git, not in the
	// table (a deletion this node has not projected yet, say). Creating
	// refs/heads/x/y: git rejects it, the table would take it.
	rrepo := "alice/rt.git"
	rdir := filepath.Join(n.r.root, filepath.FromSlash(rrepo))
	run(t, n.r.root, "init", "-q", "--bare", "--ref-format=reftable", rdir)
	run(t, work, "push", "-q", rdir, c1+":refs/heads/x")
	resp = push(rdir, []clusterhook.Line{{Old: zero, New: c1, Ref: "refs/heads/main"}, {Old: zero, New: c1, Ref: "refs/heads/x/y"}})
	assert.False(t, resp.OK)
	assert.Contains(t, resp.Message, "conflicts with refs/heads/x")
	_, ok := n.r.table.Get(rrepo, "refs/heads/main")
	assert.False(t, ok, "nothing was committed")
	// Deleting refs/heads/x in the same push frees the name.
	resp = push(rdir, []clusterhook.Line{{Old: c1, New: zero, Ref: "refs/heads/x"}, {Old: zero, New: c1, Ref: "refs/heads/x/y"}})
	assert.NotContains(t, resp.Message, "conflicts with")
	// And the reverse: refs/heads/x/y on git, creating refs/heads/x.
	run(t, rdir, "update-ref", "-d", "refs/heads/x")
	run(t, rdir, "update-ref", "refs/heads/x/y", c1)
	resp = push(rdir, []clusterhook.Line{{Old: zero, New: c1, Ref: "refs/heads/x"}})
	assert.False(t, resp.OK)
	assert.Contains(t, resp.Message, "conflicts with refs/heads/x/y")
}
