// Copyright 2026 The Forgejo Authors. All rights reserved.
// SPDX-License-Identifier: GPL-3.0-or-later

package cluster

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/sha256"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"fmt"
	"math/big"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	cluster_model "forgejo.org/models/cluster"
	"forgejo.org/models/unittest"
	"forgejo.org/modules/clusterhook"
	"forgejo.org/modules/setting"
	"forgejo.org/services/cluster/consensus"
	"forgejo.org/services/cluster/consensus/kitsunium"
	"forgejo.org/services/cluster/consensus/memraft"
	"forgejo.org/services/cluster/reftable"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMain(m *testing.M) {
	// The test binary is also the reference-transaction hook of the tests
	// that run real git commands through the replicator (fork_test.go).
	if sock := os.Getenv(testHookSocketEnv); sock != "" {
		os.Exit(runTestHook(sock))
	}
	// The full test environment: git with its home, and a fixture database
	// for the consumer's transactions.
	unittest.MainTest(m, runtimeDirOptions())
}

// runtimeDirOptions gives the engine a boot-scoped marker directory of this
// binary's own, which a runner cannot create at its Linux default,
// /run/kitsunium-consensus. SetUp runs only in the suite, never in the git
// hooks that run this binary, and TearDown removes the directory.
func runtimeDirOptions() *unittest.TestOptions {
	return &unittest.TestOptions{
		SetUp: func() error {
			dir, err := os.MkdirTemp("", "kitsunium-consensus-")
			kitsunium.RuntimeDir = dir
			return err
		},
		TearDown: func() error { return os.RemoveAll(kitsunium.RuntimeDir) },
	}
}

// ---- test PKI ----

type testPKI struct {
	dir    string
	caCert *x509.Certificate
	caKey  *ecdsa.PrivateKey
	caFile string
}

func newPKI(t *testing.T) *testPKI {
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)
	tpl := &x509.Certificate{
		SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "test consensus CA"},
		NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(24 * time.Hour),
		IsCA: true, BasicConstraintsValid: true, KeyUsage: x509.KeyUsageCertSign,
	}
	der, err := x509.CreateCertificate(rand.Reader, tpl, tpl, &key.PublicKey, key)
	require.NoError(t, err)
	cert, err := x509.ParseCertificate(der)
	require.NoError(t, err)
	dir := t.TempDir()
	caFile := filepath.Join(dir, "ca.pem")
	require.NoError(t, os.WriteFile(caFile, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}), 0o600))
	return &testPKI{dir: dir, caCert: cert, caKey: key, caFile: caFile}
}

// issue returns the PEM file of a node certificate for slot, and its SPKI.
func (p *testPKI) issue(t *testing.T, cluster, slot string) (string, [32]byte) {
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)
	u, _ := url.Parse("kitsunium-consensus://" + cluster + "/slot/" + slot)
	tpl := &x509.Certificate{
		SerialNumber: big.NewInt(time.Now().UnixNano()), NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(24 * time.Hour),
		URIs: []*url.URL{u}, KeyUsage: x509.KeyUsageDigitalSignature,
		ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth},
	}
	der, err := x509.CreateCertificate(rand.Reader, tpl, p.caCert, &key.PublicKey, p.caKey)
	require.NoError(t, err)
	kb, err := x509.MarshalECPrivateKey(key)
	require.NoError(t, err)
	file := filepath.Join(p.dir, slot+".pem")
	pemData := append(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}), pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: kb})...)
	require.NoError(t, os.WriteFile(file, pemData, 0o600))
	cert, _ := x509.ParseCertificate(der)
	return file, sha256.Sum256(cert.RawSubjectPublicKeyInfo)
}

// ---- an in-process cluster of three replicators ----

type testNode struct {
	slot string
	r    *Replicator
	node *memraft.Node
	addr string
	srv  *http.Server
	work *recordingWork
}

type recordingWork struct {
	mu     sync.Mutex
	events []reftable.Event
	// through, when set, does the work of the groups it claims, which are
	// not recorded: a test runs Forgejo's real post-receive work for them.
	through func(ctx context.Context, evs []reftable.Event) (bool, error)
}

func (w *recordingWork) do(ctx context.Context, evs []reftable.Event) error {
	w.mu.Lock()
	through := w.through
	w.mu.Unlock()
	if through != nil {
		if claimed, err := through(ctx, evs); claimed {
			return err
		}
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	w.events = append(w.events, evs...)
	return nil
}

// setThrough sets the work a test does itself, nil for none.
func (w *recordingWork) setThrough(f func(ctx context.Context, evs []reftable.Event) (bool, error)) {
	w.mu.Lock()
	w.through = f
	w.mu.Unlock()
}

func (w *recordingWork) count() int {
	w.mu.Lock()
	defer w.mu.Unlock()
	return len(w.events)
}

// memCursors is the SQL cursor table, shared by the nodes like PostgreSQL.
type memCursors struct {
	mu sync.Mutex
	m  map[string]cluster_model.Cursor
}

func (c *memCursors) get(_ context.Context, name string) (*cluster_model.Cursor, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	v := c.m[name]
	v.Name = name
	return &v, nil
}

func (c *memCursors) set(_ context.Context, name string, next cluster_model.Cursor) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.m[name] = next
	return nil
}

func (c *memCursors) advance(_ context.Context, name string, prev, next cluster_model.Cursor) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	cur := c.m[name]
	if cur.Lineage != prev.Lineage || cur.Position != prev.Position {
		return cluster_model.ErrCursorMoved
	}
	c.m[name] = next
	return nil
}

type testCluster struct {
	t       *testing.T
	mem     *memraft.Cluster
	nodes   []*testNode
	cursors *memCursors
	parked  *memParked
	ctx     context.Context
	cancel  context.CancelFunc
	// loops counts the background goroutines, which close waits for so
	// none writes into a directory being removed.
	loops sync.WaitGroup
	// repoIDs gives each key its own repository id, in the order of its
	// first push, from 1: the consumer tells repositories by id (#133).
	repoIDsMu sync.Mutex
	repoIDs   map[string]int64
}

// repoID is the repository id of a key.
func (tc *testCluster) repoID(repo string) int64 {
	tc.repoIDsMu.Lock()
	defer tc.repoIDsMu.Unlock()
	if tc.repoIDs == nil {
		tc.repoIDs = map[string]int64{}
	}
	id, ok := tc.repoIDs[repo]
	if !ok {
		id = int64(len(tc.repoIDs) + 1)
		tc.repoIDs[repo] = id
	}
	return id
}

func newTestCluster(t *testing.T, slots ...string) *testCluster {
	return newTestClusterOn(t, false, slots...)
}

// newTestClusterOn builds three replicators, on the in-process stand-in or,
// with sdkEngine, on the kitsunium SDK's real engine over its in-memory
// network.
func newTestClusterOn(t *testing.T, sdkEngine bool, slots ...string) *testCluster {
	if sdkEngine {
		clusterID := consensus.ClusterID{0x0f, 0x07, 0x9e, 0x10}
		return newTestClusterWith(t, clusterID, func(tc *testCluster, _ map[string][32]byte) {
			startSDKEngines(t, clusterID, tc.nodes)
		}, slots...)
	}
	return newTestClusterWith(t, memraft.New().ID(), func(tc *testCluster, pins map[string][32]byte) {
		for _, n := range tc.nodes {
			n.node = tc.mem.Add(n.slot, pins[n.slot], n.r.table)
			n.r.setEngine(n.node)
		}
	}, slots...)
}

// newTestClusterWith builds replicators whose engines start opens: it is
// given the nodes, with their object channels listening, and their
// certificates' pins by slot.
func newTestClusterWith(t *testing.T, clusterID consensus.ClusterID, start func(tc *testCluster, pins map[string][32]byte), slots ...string) *testCluster {
	pki := newPKI(t)
	mem := memraft.New()
	clusterHex := clusterID.String()
	tc := &testCluster{t: t, mem: mem, cursors: &memCursors{m: map[string]cluster_model.Cursor{}}, parked: &memParked{}}
	ctx, cancel := context.WithCancel(context.Background())
	tc.ctx, tc.cancel = ctx, cancel
	t.Cleanup(tc.close)

	addrs := map[string]string{}
	pins := map[string][32]byte{}
	for _, slot := range slots {
		certFile, spki := pki.issue(t, clusterHex, slot)
		pins[slot] = spki
		id, err := newIdentity(certFile, certFile, pki.caFile)
		require.NoError(t, err)
		r := newReplicator(t.TempDir(), newRegistry(5*time.Second, 64, 4096))
		r.ctx = ctx
		ln, err := net.Listen("tcp", "127.0.0.1:0")
		require.NoError(t, err)
		addrs[slot] = ln.Addr().String()
		r.objects = &objectChannel{
			root: r.root, cluster: clusterHex, id: id, engine: r.Engine, self: r.selfID,
			resolve: func(m consensus.Member) string { return addrs[m.Slot] },
			// The engines may authenticate with other keys than the
			// certificates (the SDK's memory network does).
			pinOf:        func(m consensus.Member) [32]byte { return pins[m.Slot] },
			leavesKey:    r.projector.leavesKey,
			awaitsRename: r.projector.awaitsRename,
			projected:    r.projector.Projected,
			holds:        r.projector.holds,
		}
		r.projector.fetch = r.objects.fetchMissing
		srv := &http.Server{Handler: r.objects.handler(), TLSConfig: id.serverConfig(), ReadHeaderTimeout: 5 * time.Second}
		go func() { _ = srv.ServeTLS(ln, "", "") }()
		n := &testNode{slot: slot, r: r, addr: addrs[slot], srv: srv, work: &recordingWork{}}
		tc.nodes = append(tc.nodes, n)
		// The consumer is wired before start, as Init wires it before the
		// engine opens: the autopilots start launches read it from their
		// first pass (#260).
		r.consumer = newConsumer(r)
		r.consumer.work = n.work.do
		r.consumer.dbReady = func() bool { return true }
		r.consumer.cursor = tc.cursors.get
		r.consumer.advance = tc.cursors.advance
		r.consumer.tx = func(ctx context.Context, f func(context.Context) error) error { return f(ctx) }
		r.consumer.inTx = nil
		r.consumer.durable = func(context.Context) (bool, string) { return true, "test" }
		tc.parked.wire(r.consumer)
	}

	start(tc, pins)

	for _, n := range tc.nodes {
		r := n.r
		tc.loops.Add(3)
		go func() { defer tc.loops.Done(); r.projector.run(ctx) }()
		go func() { defer tc.loops.Done(); r.startupBarrier(ctx) }()
		go func() { defer tc.loops.Done(); r.consumer.run(ctx) }()
	}
	for _, n := range tc.nodes {
		require.Eventually(t, n.r.projector.Ready, 15*time.Second, 10*time.Millisecond, "projector of %s ready", n.slot)
	}
	return tc
}

func (tc *testCluster) close() {
	tc.cancel()
	for _, n := range tc.nodes {
		_ = n.srv.Close()
	}
	tc.loops.Wait()
}

func run(t *testing.T, dir string, args ...string) string {
	t.Helper()
	out, err := gitIn(dir, args...)
	require.NoError(t, err)
	return out
}

// gitIn is run without the test: it returns git's failure instead of
// failing the test, for a goroutine of a test, which must not call
// FailNow (#200).
func gitIn(dir string, args ...string) (string, error) {
	cmd := exec.Command("git", append([]string{"-c", "core.hooksPath=/dev/null", "-c", "user.name=t", "-c", "user.email=t@t", "-c", "init.defaultBranch=main"}, args...)...)
	cmd.Dir = dir
	cmd.Env = append(os.Environ(), "HOME="+setting.Git.HomePath)
	out, err := cmd.CombinedOutput()
	if err != nil {
		return "", fmt.Errorf("git %v: %w: %s", args, err, out)
	}
	return strings.TrimSpace(string(out)), nil
}

// commit makes a commit in a scratch repository and returns its id.
func commit(t *testing.T, work, msg string) string {
	t.Helper()
	sha, err := commitIn(work, msg)
	require.NoError(t, err)
	return sha
}

// commitIn is commit without the test, as gitIn.
func commitIn(work, msg string) (string, error) {
	if err := os.WriteFile(filepath.Join(work, "f.txt"), []byte(msg), 0o644); err != nil {
		return "", err
	}
	if _, err := gitIn(work, "add", "."); err != nil {
		return "", err
	}
	if _, err := gitIn(work, "commit", "-q", "-m", msg); err != nil {
		return "", err
	}
	return gitIn(work, "rev-parse", "HEAD")
}

// pushVia plays what git receive-pack does on node n: the objects land in
// the repository, the hook's "prepared" phase is asked, and on success git
// publishes the ref and reports "committed".
func (tc *testCluster) pushVia(n *testNode, work, repo, ref, old, sha string) *clusterhook.Response {
	return tc.pushViaWithin(30*time.Second, n, work, repo, ref, old, sha)
}

// pushViaWithin is pushVia with the hook's budget given.
func (tc *testCluster) pushViaWithin(budget time.Duration, n *testNode, work, repo, ref, old, sha string) *clusterhook.Response {
	tc.t.Helper()
	resp, err := tc.tryPushViaWithin(budget, n, work, repo, ref, old, sha)
	require.NoError(tc.t, err)
	return resp
}

// tryPushVia is pushVia without the test: git's failures are returned, for
// a goroutine of a test, which must not call FailNow (#200).
func (tc *testCluster) tryPushVia(n *testNode, work, repo, ref, old, sha string) (*clusterhook.Response, error) {
	return tc.tryPushViaWithin(30*time.Second, n, work, repo, ref, old, sha)
}

// tryPushViaWithin is tryPushVia with the hook's budget given.
func (tc *testCluster) tryPushViaWithin(budget time.Duration, n *testNode, work, repo, ref, old, sha string) (*clusterhook.Response, error) {
	dir := filepath.Join(n.r.root, filepath.FromSlash(repo))
	if _, err := os.Stat(dir); os.IsNotExist(err) {
		if _, err := gitIn(n.r.root, "init", "-q", "--bare", dir); err != nil {
			return nil, err
		}
	}
	// Objects first, through a temporary ref, as receive-pack's quarantine.
	if _, err := gitIn(work, "push", "-q", dir, sha+":refs/tmp/incoming"); err != nil {
		return nil, err
	}
	if _, err := gitIn(dir, "update-ref", "-d", "refs/tmp/incoming"); err != nil {
		return nil, err
	}
	nonce := clusterhook.NewNonce()
	req := &clusterhook.Request{
		Op: clusterhook.OpPrepared, Nonce: nonce, GitDir: dir, RepoID: tc.repoID(repo), PusherID: 2,
		Lines: []clusterhook.Line{{Old: old, New: sha, Ref: ref}},
	}
	ctx, cancel := context.WithTimeout(context.Background(), budget)
	defer cancel()
	// In "prepared" git holds the ref's lock, written with the new value.
	unlock, err := holdRefLockIn(dir, ref, sha)
	if err != nil {
		return nil, err
	}
	resp := n.r.Handle(ctx, req)
	unlock()
	// The transaction ends either way, as git ends it: a ref git could not
	// update after "prepared" is "aborted".
	req.Op = clusterhook.OpAborted
	if resp.OK {
		if _, err = gitIn(dir, "update-ref", ref, sha); err == nil {
			req.Op = clusterhook.OpCommitted
		}
	}
	n.r.Handle(ctx, req)
	return resp, err
}

// holdRefLock writes the lock git holds on ref during "prepared", and
// returns its removal.
func holdRefLock(t *testing.T, dir, ref, value string) func() {
	t.Helper()
	unlock, err := holdRefLockIn(dir, ref, value)
	require.NoError(t, err)
	return unlock
}

// holdRefLockIn is holdRefLock without the test.
func holdRefLockIn(dir, ref, value string) (func(), error) {
	lock := filepath.Join(dir, filepath.FromSlash(ref)) + ".lock"
	if err := os.MkdirAll(filepath.Dir(lock), 0o755); err != nil {
		return nil, err
	}
	if err := os.WriteFile(lock, []byte(value+"\n"), 0o644); err != nil {
		return nil, err
	}
	return func() { _ = os.Remove(lock) }, nil
}

func gitRef(t *testing.T, n *testNode, repo, ref string) string {
	dir := filepath.Join(n.r.root, filepath.FromSlash(repo))
	refs, err := readGitRefs(context.Background(), dir)
	if err != nil {
		return ""
	}
	return refs[ref].SHA
}

func TestThreeNodesReplicateAPush(t *testing.T) {
	tc := newTestCluster(t, "node-a", "node-b", "node-c")
	work := t.TempDir()
	run(t, work, "init", "-q")
	c1 := commit(t, work, "one")
	zero := strings.Repeat("0", 40)

	resp := tc.pushVia(tc.nodes[1], work, "alice/demo.git", "refs/heads/main", zero, c1)
	require.True(t, resp.OK, resp.Message)

	for _, n := range tc.nodes {
		v, ok := n.r.table.Get("alice/demo.git", "refs/heads/main")
		require.True(t, ok, n.slot)
		assert.Equal(t, c1, v.SHA)
		require.Eventually(t, func() bool { return gitRef(t, n, "alice/demo.git", "refs/heads/main") == c1 },
			10*time.Second, 20*time.Millisecond, "projection on %s", n.slot)
	}
	// Every node holds the same table.
	assert.Equal(t, tc.nodes[0].r.table.Root(), tc.nodes[2].r.table.Root())

	// The leader's consumer derived the event, once.
	leader := tc.nodes[0]
	require.Eventually(t, func() bool { return leader.work.count() == 1 }, 5*time.Second, 10*time.Millisecond)
	assert.Zero(t, tc.nodes[1].work.count())
	assert.Zero(t, tc.nodes[2].work.count())

	// A read-after-write on any node waits for its projection.
	tok, repo, ok := tc.nodes[1].r.LastWrite([]string{"unknown"})
	assert.False(t, ok)
	_ = tok
	_ = repo
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	require.NoError(t, tc.nodes[2].r.WaitProjected(ctx, "alice/demo.git", 1))
}

func TestStalePushIsRefused(t *testing.T) {
	tc := newTestCluster(t, "node-a", "node-b", "node-c")
	work := t.TempDir()
	run(t, work, "init", "-q")
	c1 := commit(t, work, "one")
	c2 := commit(t, work, "two")
	zero := strings.Repeat("0", 40)
	require.True(t, tc.pushVia(tc.nodes[0], work, "a/r.git", "refs/heads/main", zero, c1).OK)
	// git on node 2 believes the ref is absent: the table says otherwise.
	resp := tc.pushVia(tc.nodes[2], work, "a/r.git", "refs/heads/main", c2, c2)
	assert.False(t, resp.OK)
	assert.Contains(t, resp.Message, "refused")
	for _, n := range tc.nodes {
		v, _ := n.r.table.Get("a/r.git", "refs/heads/main")
		assert.Equal(t, c1, v.SHA)
	}
}

func TestMinorityNodeCatchesUpAndFetchesObjects(t *testing.T) {
	tc := newTestCluster(t, "node-a", "node-b", "node-c")
	work := t.TempDir()
	run(t, work, "init", "-q")
	c1 := commit(t, work, "one")
	zero := strings.Repeat("0", 40)
	nodeC := tc.nodes[2]
	tc.mem.SetDown(nodeC.node, true)
	// node-c's object channel is down too: two of three still form a majority.
	_ = nodeC.srv.Close()
	require.True(t, tc.pushVia(tc.nodes[0], work, "b/r.git", "refs/heads/main", zero, c1).OK)
	_, ok := nodeC.r.table.Get("b/r.git", "refs/heads/main")
	assert.False(t, ok)

	tc.mem.SetDown(nodeC.node, false)
	// node-c applies the entry, lacks the objects, and fetches them.
	require.Eventually(t, func() bool { return gitRef(t, nodeC, "b/r.git", "refs/heads/main") == c1 },
		10*time.Second, 20*time.Millisecond)
}

func TestProjectorSetsAForeignWriteBack(t *testing.T) {
	tc := newTestCluster(t, "node-a", "node-b", "node-c")
	work := t.TempDir()
	run(t, work, "init", "-q")
	c1 := commit(t, work, "one")
	c2 := commit(t, work, "two")
	zero := strings.Repeat("0", 40)
	require.True(t, tc.pushVia(tc.nodes[0], work, "c/r.git", "refs/heads/main", zero, c1).OK)
	nodeB := tc.nodes[1]
	require.Eventually(t, func() bool { return gitRef(t, nodeB, "c/r.git", "refs/heads/main") == c1 }, 10*time.Second, 20*time.Millisecond)

	// A write the table never accepted: the projector sets it back.
	dir := filepath.Join(nodeB.r.root, "c", "r.git")
	run(t, work, "push", "-q", dir, c2+":refs/heads/main")
	assert.Equal(t, c2, gitRef(t, nodeB, "c/r.git", "refs/heads/main"))
	same, err := nodeB.r.projector.Verify(context.Background(), "c/r.git")
	require.NoError(t, err)
	assert.False(t, same)
	require.Eventually(t, func() bool { return gitRef(t, nodeB, "c/r.git", "refs/heads/main") == c1 }, 10*time.Second, 20*time.Millisecond)
}

func TestNoQuorumRefusesBeforeWriting(t *testing.T) {
	tc := newTestCluster(t, "node-a", "node-b", "node-c")
	work := t.TempDir()
	run(t, work, "init", "-q")
	c1 := commit(t, work, "one")
	zero := strings.Repeat("0", 40)
	tc.mem.SetDown(tc.nodes[1].node, true)
	tc.mem.SetDown(tc.nodes[2].node, true)
	_ = tc.nodes[1].srv.Close()
	_ = tc.nodes[2].srv.Close()
	resp := tc.pushVia(tc.nodes[0], work, "d/r.git", "refs/heads/main", zero, c1)
	assert.False(t, resp.OK)
	_, ok := tc.nodes[0].r.table.Get("d/r.git", "refs/heads/main")
	assert.False(t, ok)
}

func TestTransactionWithoutNonceIsRefused(t *testing.T) {
	tc := newTestCluster(t, "node-a", "node-b", "node-c")
	n := tc.nodes[0]
	dir := filepath.Join(n.r.root, "e", "r.git")
	run(t, n.r.root, "init", "-q", "--bare", dir)
	resp := n.r.Handle(context.Background(), &clusterhook.Request{
		Op: clusterhook.OpPrepared, GitDir: dir,
		Lines: []clusterhook.Line{{Old: strings.Repeat("0", 40), New: strings.Repeat("1", 40), Ref: "refs/heads/main"}},
	})
	assert.False(t, resp.OK)
	assert.Contains(t, resp.Message, "nonce")
}

func TestOperateWritesAudit(t *testing.T) {
	tc := newTestCluster(t, "node-a", "node-b", "node-c")
	n := tc.nodes[2]
	target := tc.nodes[1].node.Status().ID.String()
	require.NoError(t, n.r.Operate(context.Background(), OperatorFor(1), Operation{Action: ActionTransfer, Target: target}))
	assert.Equal(t, tc.nodes[1].node.Status().ID, tc.mem.Leader().Status().ID)
	audit := tc.nodes[0].r.table.Audit()
	require.Len(t, audit, 2)
	assert.Equal(t, "ok", audit[0].Outcome)
	assert.Equal(t, "requested", audit[1].Outcome)
	assert.Equal(t, "forgejo-user:1", audit[0].Operator)

	err := n.r.Operate(context.Background(), OperatorFor(1), Operation{Action: ActionRemove, Target: target})
	require.Error(t, err)
	assert.Equal(t, "failed", tc.nodes[0].r.table.Audit()[0].Outcome)

	_, err = ParseMember("0000000000000001:" + strings.Repeat("ab", 16) + ":node-a:" + strings.Repeat("00", 32) + ":10.0.0.1:7480")
	require.Error(t, err, "an empty SPKI pin is refused")
	m, err := ParseMember("0000000000000001:" + strings.Repeat("ab", 16) + ":node-a:" + strings.Repeat("cd", 32) + ":10.0.0.1:7480")
	require.NoError(t, err)
	assert.Equal(t, "10.0.0.1:7480", m.Address)
}

func TestObjectChannelRefusesNonMembers(t *testing.T) {
	tc := newTestCluster(t, "node-a", "node-b", "node-c")
	pki := newPKI(t)
	// A certificate from another CA, naming a real slot.
	certFile, _ := pki.issue(t, tc.mem.ID().String(), "node-b")
	id, err := newIdentity(certFile, certFile, pki.caFile)
	require.NoError(t, err)
	oc := &objectChannel{id: id, cluster: tc.mem.ID().String()}
	c := oc.client(consensus.Member{Slot: "node-a", SPKI: [32]byte{1}})
	_, err = c.Post(fmt.Sprintf("https://%s/v1/push?repo=a/b.git", tc.nodes[0].addr), "application/octet-stream", strings.NewReader("x"))
	require.Error(t, err)
}

func TestRenameAndDeleteFollowOnEveryMember(t *testing.T) {
	tc := newTestCluster(t, "node-a", "node-b", "node-c")
	work := t.TempDir()
	run(t, work, "init", "-q")
	c1 := commit(t, work, "one")
	zero := strings.Repeat("0", 40)
	require.True(t, tc.pushVia(tc.nodes[0], work, "o/old.git", "refs/heads/main", zero, c1).OK)
	for _, n := range tc.nodes {
		require.Eventually(t, func() bool { return gitRef(t, n, "o/old.git", "refs/heads/main") == c1 }, 10*time.Second, 20*time.Millisecond)
	}

	// Forgejo moves the directory on the node that served the request, then
	// the rename is replicated.
	origin := tc.nodes[0]
	require.NoError(t, os.Rename(filepath.Join(origin.r.root, "o", "old.git"), filepath.Join(origin.r.root, "o", "new.git")))
	ctx := context.Background()
	require.NoError(t, origin.r.proposeRepoRename(ctx, "o/old.git", "o/new.git"))
	for _, n := range tc.nodes {
		require.Eventually(t, func() bool { return gitRef(t, n, "o/new.git", "refs/heads/main") == c1 }, 10*time.Second, 20*time.Millisecond, n.slot)
		_, err := os.Stat(filepath.Join(n.r.root, "o", "old.git"))
		assert.True(t, os.IsNotExist(err), n.slot)
	}

	require.NoError(t, os.RemoveAll(filepath.Join(origin.r.root, "o", "new.git")))
	require.NoError(t, origin.r.proposeRepoDelete(ctx, "o/new.git", 1))
	for _, n := range tc.nodes {
		require.Eventually(t, func() bool {
			_, err := os.Stat(filepath.Join(n.r.root, "o", "new.git"))
			return os.IsNotExist(err)
		}, 10*time.Second, 20*time.Millisecond, n.slot)
	}
}

func TestImportOfARepositoryThatPredatesTheCluster(t *testing.T) {
	tc := newTestCluster(t, "node-a", "node-b", "node-c")
	work := t.TempDir()
	run(t, work, "init", "-q")
	c1 := commit(t, work, "one")
	c2 := commit(t, work, "two")
	origin := tc.nodes[1]
	dir := filepath.Join(origin.r.root, "old", "forge.git")
	run(t, origin.r.root, "init", "-q", "--bare", dir)
	run(t, work, "push", "-q", dir, c1+":refs/heads/main")

	// Until it is imported, a push to an existing ref is refused.
	assert.False(t, tc.pushVia(origin, work, "old/forge.git", "refs/heads/main", c1, c2).OK)
	run(t, dir, "update-ref", "refs/heads/main", c1)

	rep, err := origin.r.Import(context.Background(), "")
	require.NoError(t, err)
	assert.Equal(t, 1, rep.Repos)
	assert.Equal(t, 2, rep.Refs) // main and HEAD
	assert.Empty(t, rep.Failures)
	for _, n := range tc.nodes {
		require.Eventually(t, func() bool { return gitRef(t, n, "old/forge.git", "refs/heads/main") == c1 }, 10*time.Second, 20*time.Millisecond, n.slot)
	}
	require.True(t, tc.pushVia(origin, work, "old/forge.git", "refs/heads/main", c1, c2).OK)
	// The import itself derives no push: the leader's consumer sees the
	// push that followed, and only it.
	leader := tc.nodes[0]
	require.Eventually(t, func() bool { return leader.work.count() == 1 }, 5*time.Second, 10*time.Millisecond)
	leader.work.mu.Lock()
	assert.Equal(t, c2, leader.work.events[0].New)
	leader.work.mu.Unlock()
	// A second import finds nothing new.
	rep, err = origin.r.Import(context.Background(), "old/forge.git")
	require.NoError(t, err)
	assert.Zero(t, rep.Refs)
}

func TestObjectsVerifyAndBackfill(t *testing.T) {
	tc := newTestCluster(t, "node-a", "node-b", "node-c")
	work := t.TempDir()
	run(t, work, "init", "-q")
	c1 := commit(t, work, "one")
	require.True(t, tc.pushVia(tc.nodes[0], work, "v/r.git", "refs/heads/main", strings.Repeat("0", 40), c1).OK)
	nodeC := tc.nodes[2]
	require.Eventually(t, func() bool { return gitRef(t, nodeC, "v/r.git", "refs/heads/main") == c1 }, 10*time.Second, 20*time.Millisecond)
	rep, err := nodeC.r.VerifyObjects(context.Background(), false)
	require.NoError(t, err)
	assert.Equal(t, 1, rep.Repos)
	assert.Empty(t, rep.Missing)

	// A node that lost its objects says so, and backfills them.
	dir := filepath.Join(nodeC.r.root, "v", "r.git")
	require.NoError(t, os.RemoveAll(filepath.Join(dir, "objects")))
	require.NoError(t, os.MkdirAll(filepath.Join(dir, "objects", "pack"), 0o755))
	require.NoError(t, os.MkdirAll(filepath.Join(dir, "objects", "info"), 0o755))
	rep, err = nodeC.r.VerifyObjects(context.Background(), false)
	require.NoError(t, err)
	assert.Equal(t, 1, rep.Missing["v/r.git"])
	rep, err = nodeC.r.VerifyObjects(context.Background(), true)
	require.NoError(t, err)
	assert.Empty(t, rep.Missing, rep.Errors)
	assert.Equal(t, 1, rep.Fetched)
	missing, err := missingObjects(context.Background(), dir, []string{c1})
	require.NoError(t, err)
	assert.Empty(t, missing)
}

// A peer that accepts connections and never answers does not hold a push
// back: the objects return at majority, the straggler is left behind.
func TestObjectsReturnAtMajorityPastASilentPeer(t *testing.T) {
	tc := newTestCluster(t, "node-a", "node-b", "node-c")
	silent, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer silent.Close()
	go func() {
		for {
			c, err := silent.Accept()
			if err != nil {
				return
			}
			defer c.Close() // held open, never answered
		}
	}()
	origin := tc.nodes[0]
	real := origin.r.objects.resolve
	origin.r.objects.resolve = func(m consensus.Member) string {
		if m.Slot == "node-c" {
			return silent.Addr().String()
		}
		return real(m)
	}
	work := t.TempDir()
	run(t, work, "init", "-q")
	c1 := commit(t, work, "one")
	start := time.Now()
	resp := tc.pushVia(origin, work, "q/r.git", "refs/heads/main", strings.Repeat("0", 40), c1)
	require.True(t, resp.OK, resp.Message)
	// Waiting on the silent peer would last the whole 30 s request budget.
	assert.Less(t, time.Since(start), 15*time.Second)
}
