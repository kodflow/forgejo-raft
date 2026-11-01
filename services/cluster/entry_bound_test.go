// Copyright 2026 The Forgejo Authors. All rights reserved.
// SPDX-License-Identifier: GPL-3.0-or-later

package cluster

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"forgejo.org/models/db"
	"forgejo.org/modules/clusterhook"
	"forgejo.org/modules/setting"
	"forgejo.org/services/cluster/consensus"
	"forgejo.org/services/cluster/reftable"
	mirror_service "forgejo.org/services/mirror"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// boundEngine refuses an entry past consensus.MaxEntryBytes as the
// kitsunium SDK's Propose does on the proposing node, before anything is
// appended (cfg.MaxEntryBytes, set by consensus/kitsunium): memraft, the
// bench's engine, takes an entry of any size. It counts what it is given.
type boundEngine struct {
	consensus.Engine
	mu                          sync.Mutex
	proposals, refused, largest int
}

func (e *boundEngine) Propose(ctx context.Context, data []byte) (consensus.Token, any, error) {
	e.mu.Lock()
	e.proposals++
	e.largest = max(e.largest, len(data))
	too := len(data) > consensus.MaxEntryBytes
	if too {
		e.refused++
	}
	e.mu.Unlock()
	if too {
		return consensus.Token{}, nil, fmt.Errorf("%w: %d bytes", consensus.ErrEntryTooLarge, len(data))
	}
	return e.Engine.Propose(ctx, data)
}

// counts returns what the engine was given.
func (e *boundEngine) counts() (proposals, refused, largest int) {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.proposals, e.refused, e.largest
}

// boundEngines puts a boundEngine before each node's engine, by slot.
func boundEngines(nodes ...*testNode) map[string]*boundEngine {
	bounds := map[string]*boundEngine{}
	for _, n := range nodes {
		b := &boundEngine{Engine: n.r.Engine()}
		bounds[n.slot] = b
		n.r.setEngine(b)
	}
	return bounds
}

// longRef is the i-th of the names of about 930 bytes the tests below
// write: some 1 100 of them weigh more than one entry may.
func longRef(i int) string {
	return fmt.Sprintf("refs/heads/%sb%05d", strings.Repeat(strings.Repeat("x", 100)+"/", 9), i)
}

// escapedRef is the i-th of the names of 418 bytes the tests below write,
// each '&' written on six bytes by encoding/json: some 430 of them weigh
// more than one entry may. A component of a loose ref is a file name, 255
// bytes at most.
func escapedRef(i int) string {
	amp := strings.Repeat("&", 200)
	return fmt.Sprintf("refs/heads/%s/%s/b%05d", amp, amp, i)
}

// createRefs creates names at sha in the repository at dir, in one
// transaction and without any hook.
func createRefs(t *testing.T, dir, sha string, names []string) {
	t.Helper()
	var in strings.Builder
	for _, name := range names {
		require.NoError(t, reftable.ValidRefName(name))
		fmt.Fprintf(&in, "create %s %s\n", name, sha)
	}
	cmd := exec.Command("git", "-c", "core.hooksPath=/dev/null", "update-ref", "--stdin")
	cmd.Dir = dir
	cmd.Env = append(os.Environ(), "HOME="+setting.Git.HomePath)
	cmd.Stdin = strings.NewReader(in.String())
	out, err := cmd.CombinedOutput()
	require.NoError(t, err, "%s", out)
}

// A transaction of fewer refs than MaxUpdatesPerTxn whose entry passes
// consensus.MaxEntryBytes is refused before its objects go anywhere, as
// one past the count is, with a reason that names the bound and says to
// split it: proposed, the engine refused it as ENTRY_TOO_LARGE once its
// objects had gone to a majority for nothing (#385).
func TestOversizedEntryIsRefusedEarly(t *testing.T) {
	tc := newTestCluster(t, "node-a", "node-b", "node-c")
	bounds := boundEngines(tc.nodes...)
	n := tc.nodes[0]
	work := t.TempDir()
	run(t, work, "init", "-q")
	c1 := commit(t, work, "one")
	dir := filepath.Join(n.r.root, "big", "entry.git")
	run(t, n.r.root, "init", "-q", "--bare", dir)
	run(t, work, "push", "-q", dir, c1+":refs/tmp/incoming")
	run(t, dir, "update-ref", "-d", "refs/tmp/incoming")
	lines := make([]clusterhook.Line, 1200)
	for i := range lines {
		lines[i] = clusterhook.Line{Old: strings.Repeat("0", 40), New: c1, Ref: longRef(i)}
	}
	require.Less(t, len(lines), reftable.MaxUpdatesPerTxn)
	resp := n.r.Handle(context.Background(), &clusterhook.Request{Op: clusterhook.OpPrepared, Nonce: clusterhook.NewNonce(), GitDir: dir, Lines: lines})
	t.Logf("refused: %s", resp.Message)
	require.False(t, resp.OK)
	assert.Contains(t, resp.Message, clusterhook.ErrTooLarge.Error())
	assert.Contains(t, resp.Message, fmt.Sprintf("the bound is %d", consensus.MaxEntryBytes))
	assert.Contains(t, resp.Message, "several smaller pushes")
	proposals, _, _ := bounds[n.slot].counts()
	assert.Zero(t, proposals, "refused before it is proposed")
	for _, peer := range tc.nodes[1:] {
		_, err := gitIn(filepath.Join(peer.r.root, "big", "entry.git"), "cat-file", "-e", c1)
		assert.Error(t, err, "%s: refused before its objects travel", peer.slot)
	}
}

// An import of a repository whose ref names need JSON escapes: a batch of
// 512 of them made an entry the engine refused past
// consensus.MaxEntryBytes, and the import of that repository failed every
// time (#385). Its batches are cut on their weight as on their count.
func TestImportCutsItsBatchesOnTheEntryBound(t *testing.T) {
	tc := newTestCluster(t, "node-a", "node-b", "node-c")
	bounds := boundEngines(tc.nodes...)
	origin := tc.nodes[1]
	work := t.TempDir()
	run(t, work, "init", "-q")
	c1 := commit(t, work, "one")
	dir := filepath.Join(origin.r.root, "old", "forge.git")
	run(t, origin.r.root, "init", "-q", "--bare", dir)
	run(t, work, "push", "-q", dir, c1+":refs/heads/main")
	names := make([]string, 600)
	for i := range names {
		names[i] = escapedRef(i)
	}
	createRefs(t, dir, c1, names)

	rep, err := origin.r.Import(context.Background(), "old/forge.git")
	require.NoError(t, err)
	proposals, refused, largest := bounds[origin.slot].counts()
	t.Logf("import: %+v; %d entries proposed, %d refused past the bound, the largest %d bytes", rep, proposals, refused, largest)
	assert.Empty(t, rep.Failures)
	assert.Equal(t, len(names)+2, rep.Refs, "main, HEAD and the escaped names")
	assert.Zero(t, refused)
	for _, nd := range tc.nodes {
		require.Eventually(t, func() bool { return gitRef(t, nd, "old/forge.git", names[len(names)-1]) == c1 }, 15*time.Second, 20*time.Millisecond, nd.slot)
	}
}

// A pull mirror whose sync writes fewer refs than MaxUpdatesPerTxn, with
// long names: from git 2.51 the sync's own fetch is one transaction, whose
// entry the engine refused past consensus.MaxEntryBytes. git aborted the
// fetch, and every later sync fetched the same refs and was refused the
// same way: the mirror never moved again, its master included, on every
// node (#385). The sync now goes by batches, each within the bound.
func TestPullMirrorSyncPastTheEntryBound(t *testing.T) {
	s := newPullMirrorSwap(t)
	ctx := db.DefaultContext
	bounds := boundEngines(s.a, s.b, s.c)

	// The upstream: master moves on, and gains branches of about 930
	// bytes, fewer than MaxUpdatesPerTxn, whose entry in one transaction
	// passes the bound.
	s.master = commit(t, s.up, "upstream master beside long branch names")
	run(t, s.up, "push", "-q", s.upstream, s.master+":refs/heads/master")
	names := make([]string, 1200)
	for i := range names {
		names[i] = longRef(i)
	}
	createRefs(t, s.upstream, s.master, names)

	s.due(t)
	ok := mirror_service.SyncPullMirror(ctx, pmPullMirrorID)
	waitApplied(t, s.a, s.b)
	waitApplied(t, s.a, s.c)
	proposals, refused, largest := bounds[s.a.slot].counts()
	t.Logf("node-a proposed %d entries, %d refused past the bound, the largest %d bytes", proposals, refused, largest)
	assert.True(t, ok, "the sync of fewer refs than MaxUpdatesPerTxn, past the entry bound")
	assert.Zero(t, refused, "no entry past the bound")
	for _, nd := range []*testNode{s.a, s.b, s.c} {
		master, _ := nd.r.table.Get(pmPullMirror, "refs/heads/master")
		assert.Equal(t, s.master, master.SHA, "%s: the mirror's master follows the upstream's", nd.slot)
		refs, _ := nd.r.table.Refs(pmPullMirror)
		live := 0
		for name, v := range refs {
			if v.Live() && strings.HasPrefix(name, "refs/heads/xxx") {
				live++
			}
		}
		assert.Equal(t, len(names), live, "%s: the new branches", nd.slot)
	}
}
