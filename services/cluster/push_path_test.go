// Copyright 2026 The Forgejo Authors. All rights reserved.
// SPDX-License-Identifier: GPL-3.0-or-later

package cluster

import (
	"context"
	"encoding/hex"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"forgejo.org/modules/clusterhook"
	"forgejo.org/modules/setting"
	"forgejo.org/modules/test"
	"forgejo.org/services/cluster/consensus"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// git accepts Unicode spaces in a ref name; readGitRefs reads the name byte
// for byte, a trailing one included.
func TestReadGitRefsKeepsTheNameByteForByte(t *testing.T) {
	work := t.TempDir()
	run(t, work, "init", "-q")
	c1 := commit(t, work, "one")
	c2 := commit(t, work, "two")
	names := map[string]string{
		"refs/heads/main":       c1,
		"refs/heads/main x":     c2,
		"refs/heads/feature　x":  c1,
		"refs/heads/zz ":        c2,
		"refs/heads/a\u0085b c": c1,
	}
	for name, sha := range names {
		run(t, work, "update-ref", name, sha)
	}
	refs, err := readGitRefs(context.Background(), work)
	require.NoError(t, err)
	for name, sha := range names {
		assert.Equal(t, sha, refs[name].SHA, "%q", name)
		assert.Empty(t, refs[name].Symbolic, "%q", name)
	}
	assert.Equal(t, "refs/heads/main", refs["HEAD"].Symbolic)
}

// A ref git holds beside a replicated one, under a name that only a Unicode
// space tells apart, neither hides the replicated ref nor stops the node
// from certifying the repository; and a push of such a name replicates
// under its exact name.
func TestUnicodeSpaceRefNamesReplicateAndCertify(t *testing.T) {
	tc := newTestCluster(t, "node-a", "node-b", "node-c")
	work := t.TempDir()
	run(t, work, "init", "-q")
	c1 := commit(t, work, "one")
	c2 := commit(t, work, "two")
	zero := strings.Repeat("0", 40)
	repo := "alice/nbsp.git"
	require.True(t, tc.pushVia(tc.nodes[0], work, repo, "refs/heads/main", zero, c1).OK)
	nodeB := tc.nodes[1]
	require.Eventually(t, func() bool { return gitRef(t, nodeB, repo, "refs/heads/main") == c1 }, 10*time.Second, 20*time.Millisecond)

	// A ref the table never held, left by git on node-b.
	dir := filepath.Join(nodeB.r.root, filepath.FromSlash(repo))
	run(t, work, "push", "-q", dir, c2+":refs/heads/main x")
	same, err := nodeB.r.projector.Verify(context.Background(), repo)
	require.NoError(t, err)
	assert.True(t, same, "the replicated refs/heads/main is read as it is")

	// A push of such a name reaches every member under that name.
	name := "refs/heads/feature　x"
	resp := tc.pushVia(tc.nodes[2], work, repo, name, zero, c2)
	require.True(t, resp.OK, resp.Message)
	for _, n := range tc.nodes {
		v, ok := n.r.table.Get(repo, name)
		require.True(t, ok, n.slot)
		assert.Equal(t, c2, v.SHA)
		require.Eventually(t, func() bool { return gitRef(t, n, repo, name) == c2 }, 10*time.Second, 20*time.Millisecond, n.slot)
	}
}

// Before git 2.54, a non-atomic push of two refs is two transactions under
// one process nonce. The second's outcome must not be read from the first's
// commit: an unknown outcome settled that way acknowledged a push the
// cluster never held.
func TestSisterTransactionOfOneProcessIsNotCommitted(t *testing.T) {
	tc := newTestCluster(t, "node-a", "node-b", "node-c")
	work := t.TempDir()
	run(t, work, "init", "-q")
	c1 := commit(t, work, "one")
	zero := strings.Repeat("0", 40)
	n := tc.nodes[0]
	repo := "alice/sister.git"
	dir := filepath.Join(n.r.root, filepath.FromSlash(repo))
	run(t, n.r.root, "init", "-q", "--bare", dir)
	run(t, work, "push", "-q", dir, c1+":refs/tmp/incoming")
	run(t, dir, "update-ref", "-d", "refs/tmp/incoming")

	nonce := clusterhook.NewNonce()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	first := &clusterhook.Request{
		Op: clusterhook.OpPrepared, Nonce: nonce, GitDir: dir, RepoID: 1, PusherID: 2,
		Lines: []clusterhook.Line{{Old: zero, New: c1, Ref: "refs/heads/a"}},
	}
	unlock := holdRefLock(t, dir, "refs/heads/a", c1)
	resp := n.r.Handle(ctx, first)
	unlock()
	require.True(t, resp.OK, resp.Message)
	run(t, dir, "update-ref", "refs/heads/a", c1)
	first.Op = clusterhook.OpCommitted
	n.r.Handle(ctx, first)

	second := &clusterhook.Request{
		Op: clusterhook.OpPrepared, Nonce: nonce, GitDir: dir, RepoID: 1, PusherID: 2,
		Lines: []clusterhook.Line{{Old: zero, New: c1, Ref: "refs/heads/b"}},
	}
	tx, _, err := n.r.buildTxn(ctx, repo, second)
	require.NoError(t, err)
	assert.False(t, n.r.committed(repo, tx), "the second transaction was never proposed")
}

// From git 2.54 a non-atomic push is one transaction that tolerates a
// failure per ref, and the hook reads the rejected refs too: the cluster
// refuses the whole push rather than commit a ref the client reads as
// rejected.
func TestPushGitRejectsInPartIsRefused(t *testing.T) {
	tc := newTestCluster(t, "node-a", "node-b", "node-c")
	work := t.TempDir()
	run(t, work, "init", "-q")
	c1 := commit(t, work, "one")
	c2 := commit(t, work, "two")
	zero := strings.Repeat("0", 40)
	n := tc.nodes[0]
	repo := "alice/partial.git"
	require.True(t, tc.pushVia(n, work, repo, "refs/heads/main", zero, c1).OK)
	require.True(t, tc.pushVia(n, work, repo, "refs/heads/b1", zero, c1).OK)
	dir := filepath.Join(n.r.root, filepath.FromSlash(repo))
	run(t, work, "push", "-q", dir, c2+":refs/tmp/incoming")
	run(t, dir, "update-ref", "-d", "refs/tmp/incoming")

	push := func(lines []clusterhook.Line, locks map[string]string) *clusterhook.Response {
		var unlocks []func()
		for ref, v := range locks {
			unlocks = append(unlocks, holdRefLock(t, dir, ref, v))
		}
		defer func() {
			for _, u := range unlocks {
				u()
			}
		}()
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		req := &clusterhook.Request{Op: clusterhook.OpPrepared, Nonce: clusterhook.NewNonce(), GitDir: dir, RepoID: 1, PusherID: 2, Lines: lines}
		resp := n.r.Handle(ctx, req)
		req.Op = clusterhook.OpAborted
		n.r.Handle(ctx, req)
		return resp
	}
	both := []clusterhook.Line{{Old: c1, New: c2, Ref: "refs/heads/main"}, {Old: c1, New: c2, Ref: "refs/heads/b1"}}

	// A stranger's lock on b1: git rejects b1 and updates main.
	resp := push(both, map[string]string{"refs/heads/main": c2, "refs/heads/b1": ""})
	assert.False(t, resp.OK)
	assert.Contains(t, resp.Message, "refs/heads/b1")
	// No lock at all on b1 (git could not take it).
	resp = push(both, map[string]string{"refs/heads/main": c2})
	assert.False(t, resp.OK)
	// b1 does not hold the old value the push names on this node.
	resp = push([]clusterhook.Line{{Old: c1, New: c2, Ref: "refs/heads/main"}, {Old: c2, New: c1, Ref: "refs/heads/b1"}},
		map[string]string{"refs/heads/main": c2, "refs/heads/b1": c1})
	assert.False(t, resp.OK)
	// A creation of a ref that exists on this node.
	resp = push([]clusterhook.Line{{Old: zero, New: c2, Ref: "refs/heads/b1"}}, map[string]string{"refs/heads/b1": c2})
	assert.False(t, resp.OK)
	for _, nd := range tc.nodes {
		for _, ref := range []string{"refs/heads/main", "refs/heads/b1"} {
			v, _ := nd.r.table.Get(repo, ref)
			assert.Equal(t, c1, v.SHA, "%s %s: nothing was committed", nd.slot, ref)
		}
	}

	// Both locks held by git with the new values: the push commits whole.
	resp = push(both, map[string]string{"refs/heads/main": c2, "refs/heads/b1": c2})
	require.True(t, resp.OK, resp.Message)
	v, _ := n.r.table.Get(repo, "refs/heads/b1")
	assert.Equal(t, c2, v.SHA)
}

// A ref name that is not valid UTF-8 is refused before it is proposed: JSON
// would carry another name than git's.
func TestInvalidUTF8RefNameIsRefused(t *testing.T) {
	tc := newTestCluster(t, "node-a", "node-b", "node-c")
	work := t.TempDir()
	run(t, work, "init", "-q")
	c1 := commit(t, work, "one")
	zero := strings.Repeat("0", 40)
	n := tc.nodes[0]
	dir := filepath.Join(n.r.root, "alice", "utf8.git")
	run(t, n.r.root, "init", "-q", "--bare", dir)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	resp := n.r.Handle(ctx, &clusterhook.Request{
		Op: clusterhook.OpPrepared, Nonce: clusterhook.NewNonce(), GitDir: dir, RepoID: 1, PusherID: 2,
		Lines: []clusterhook.Line{{Old: zero, New: c1, Ref: "refs/heads/a\xffb"}},
	})
	assert.False(t, resp.OK)
	assert.Contains(t, resp.Message, "UTF-8")
	assert.Empty(t, n.r.table.Repos())
}

// A certification runs git outside the registry's lock: a push to another
// repository registers at once, one to the same repository waits for it.
func TestCertificationHoldsOnlyItsRepository(t *testing.T) {
	reg := newRegistry(time.Minute, 8, 64)
	started, release := make(chan struct{}), make(chan struct{})
	certified := make(chan bool, 1)
	go func() {
		certified <- reg.certify("alice/a.git", func() { close(started); <-release })
	}()
	<-started

	other := make(chan error, 1)
	go func() { other <- reg.register("n1", "bob/b.git", nil, 1, false) }()
	select {
	case err := <-other:
		require.NoError(t, err)
	case <-time.After(time.Second):
		close(release)
		t.Fatal("another repository's push waited for the certification")
	}
	assert.False(t, reg.certify("alice/a.git", func() { t.Error("a second certification of the same repository ran") }))

	registered := make(chan error, 1)
	go func() { registered <- reg.register("n2", "alice/a.git", nil, 1, false) }()
	select {
	case err := <-registered:
		t.Fatalf("the same repository registered during its certification: %v", err)
	case <-time.After(200 * time.Millisecond):
	}
	close(release)
	require.True(t, <-certified)
	require.NoError(t, <-registered)
	assert.Equal(t, 2, reg.count())
}

// A certification that outlasts the registration TTL turns the push away,
// retryable, rather than hold it.
func TestRegistrationGivesUpPastTheTTL(t *testing.T) {
	reg := newRegistry(100*time.Millisecond, 8, 64)
	started, release := make(chan struct{}), make(chan struct{})
	go reg.certify("alice/a.git", func() { close(started); <-release })
	<-started
	defer close(release)
	got := make(chan error, 1)
	go func() { got <- reg.register("n1", "alice/a.git", nil, 1, false) }()
	select {
	case err := <-got:
		require.ErrorIs(t, err, errTooManyTxn)
	case <-time.After(5 * time.Second):
		t.Fatal("the registration still waits past its TTL")
	}
}

// A retried push whose objects the peers already hold ships nothing: the
// shipment succeeds even from a directory that holds none of them.
func TestShipmentAsksThePeersFirst(t *testing.T) {
	tc := newTestCluster(t, "node-a", "node-b", "node-c")
	work := t.TempDir()
	run(t, work, "init", "-q")
	c1 := commit(t, work, "one")
	c2 := commit(t, work, "two")
	repo := "alice/have.git"
	origin := tc.nodes[0]
	require.True(t, tc.pushVia(origin, work, repo, "refs/heads/main", strings.Repeat("0", 40), c1).OK)
	for _, n := range tc.nodes {
		require.Eventually(t, func() bool { return gitRef(t, n, repo, "refs/heads/main") == c1 }, 10*time.Second, 20*time.Millisecond)
	}

	empty := filepath.Join(t.TempDir(), "empty.git")
	run(t, filepath.Dir(empty), "init", "-q", "--bare", empty)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	require.NoError(t, origin.r.objects.shipToMajority(ctx, repo, empty, "sha1", []string{c1}, nil),
		"every peer holds c1: nothing to pack")
	// c2 is held by no peer: it must be packed, and the empty directory
	// cannot.
	require.ErrorIs(t, origin.r.objects.shipToMajority(ctx, repo, empty, "sha1", []string{c2}, nil), errNoMajority)
}

// Forgejo accepts on push the fsck warnings it ignores (zero-padded tree
// modes in old histories, among others); the object channel accepts the same
// objects, or such a repository could never be written in the cluster.
func TestObjectChannelAcceptsWhatForgejoAccepts(t *testing.T) {
	for _, prefix := range []string{"fsck.", "fetch.fsck.", "receive.fsck."} {
		for _, id := range []string{"badTimezone", "missingSpaceBeforeDate", "zeroPaddedFilemode"} {
			key := strings.ToLower(prefix + id)
			if _, ok := setting.GitConfig.Options[key]; !ok {
				setting.GitConfig.SetOption(key, "ignore")
				t.Cleanup(func() { delete(setting.GitConfig.Options, key) })
			}
		}
	}
	work := t.TempDir()
	run(t, work, "init", "-q")
	blob := gitStdin(t, work, "hello\n", "hash-object", "-w", "--stdin")
	raw, err := hex.DecodeString(blob)
	require.NoError(t, err)
	tree := gitStdin(t, work, "0100644 f\x00"+string(raw), "hash-object", "-t", "tree", "--literally", "-w", "--stdin")
	c := run(t, work, "commit-tree", tree, "-m", "zero-padded")
	pack := gitStdin(t, work, c+"\n", "pack-objects", "--revs", "--stdout", "-q")

	dst := filepath.Join(t.TempDir(), "dst.git")
	run(t, filepath.Dir(dst), "init", "-q", "--bare", dst)
	require.NoError(t, receivePack(context.Background(), dst, strings.NewReader(pack)))
	missing, err := missingObjects(context.Background(), dst, []string{c, tree, blob})
	require.NoError(t, err)
	assert.Empty(t, missing)
	// Corruption is still refused.
	require.Error(t, receivePack(context.Background(), dst, strings.NewReader(pack[:len(pack)/2])))
}

// gitStdin runs git with stdin and returns its raw output.
func gitStdin(t *testing.T, dir, stdin string, args ...string) string {
	t.Helper()
	cmd := exec.Command("git", append([]string{"-c", "core.hooksPath=/dev/null", "-c", "user.name=t", "-c", "user.email=t@t"}, args...)...)
	cmd.Dir = dir
	cmd.Stdin = strings.NewReader(stdin)
	cmd.Env = append(os.Environ(), "HOME="+setting.Git.HomePath)
	out, err := cmd.Output()
	require.NoError(t, err, "git %v", args)
	return strings.TrimSuffix(string(out), "\n")
}

// A member that stalls in the middle of a pack is cut once no byte arrived
// for objectIdle, on either side: the projector's serial pass, and the
// handler of a shipment, never wait on it forever.
func TestStalledObjectTransfersAreCut(t *testing.T) {
	// Restored after the cluster stops (cleanups run last in, first out).
	t.Cleanup(test.MockVariableValue(&objectIdle, 300*time.Millisecond))
	t.Cleanup(test.MockVariableValue(&packObjectsFn, func(ctx context.Context, _ string, _, _ []string, w io.Writer) error {
		_, _ = w.Write([]byte("PACK\x00\x00\x00\x02"))
		<-ctx.Done()
		return ctx.Err()
	}))
	tc := newTestCluster(t, "node-a", "node-b", "node-c")
	work := t.TempDir()
	run(t, work, "init", "-q")
	c1 := commit(t, work, "one")
	repo := "alice/stall.git"
	require.True(t, tc.pushVia(tc.nodes[0], work, repo, "refs/heads/main", strings.Repeat("0", 40), c1).OK)

	// Fetch: every member answers with a pack that never ends.
	n := tc.nodes[0]
	dir := filepath.Join(t.TempDir(), "fetch.git")
	run(t, filepath.Dir(dir), "init", "-q", "--bare", dir)
	fetched := make(chan error, 1)
	go func() { fetched <- n.r.objects.fetchMissing(context.Background(), repo, dir, []string{c1}) }()
	select {
	case err := <-fetched:
		require.Error(t, err)
	case <-time.After(20 * time.Second):
		t.Fatal("fetching from stalled members still waits")
	}

	// Push: a sender that stops in the middle of its pack.
	peer := tc.nodes[1]
	m := memberOf(t, n, peer.slot)
	u, err := n.r.objects.objectURL(m, "/v1/push?repo="+repo+"&format=sha1")
	require.NoError(t, err)
	pr, pw := io.Pipe()
	defer pw.Close()
	go func() { _, _ = pw.Write([]byte("PACK\x00\x00\x00\x02\x00\x00\x00\x05")) }()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, u, pr)
	require.NoError(t, err)
	resp, err := n.r.objects.client(m).Do(req)
	if err == nil {
		resp.Body.Close()
		assert.NotEqual(t, http.StatusNoContent, resp.StatusCode)
	}
	require.NoError(t, ctx.Err(), "the peer still waits for the rest of the pack")
}

func memberOf(t *testing.T, n *testNode, slot string) consensus.Member {
	t.Helper()
	for _, m := range n.r.Engine().Membership().Members {
		if m.Slot == slot {
			return m
		}
	}
	t.Fatalf("no member %s", slot)
	return consensus.Member{}
}

// leaderlessEngine knows no leader: its barrier never completes.
type leaderlessEngine struct{ consensus.Engine }

func (leaderlessEngine) Health() consensus.Health { return consensus.Health{} }

func (leaderlessEngine) Barrier(ctx context.Context) (consensus.Token, error) {
	<-ctx.Done()
	return consensus.Token{}, ctx.Err()
}

// Without a known leader, a git request is served at once, reported
// possibly behind, instead of waiting READ_AFTER_WRITE_TIMEOUT for a
// barrier that cannot complete.
func TestWaitFreshDoesNotWaitWithoutALeader(t *testing.T) {
	defer test.MockVariableValue(&setting.Cluster.ReadAfterWriteTimeout, 2*time.Second)()
	r := newReplicator(t.TempDir(), newRegistry(time.Minute, 8, 64))
	r.setEngine(leaderlessEngine{})
	start := time.Now()
	assert.False(t, r.WaitFresh(context.Background(), "alice/demo.git"))
	assert.Less(t, time.Since(start), time.Second)
}
