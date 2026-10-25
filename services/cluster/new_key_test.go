// Copyright 2026 The Forgejo Authors. All rights reserved.
// SPDX-License-Identifier: GPL-3.0-or-later

package cluster

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"crypto/x509"
	"encoding/hex"
	"encoding/pem"
	"errors"
	"math/big"
	"net/url"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"forgejo.org/services/cluster/certrenew"
	"forgejo.org/services/cluster/consensus"
	"forgejo.org/services/cluster/consensus/kitsunium"
	"forgejo.org/services/cluster/consensus/memraft"
	"forgejo.org/services/cluster/reftable"

	sdk "github.com/kitsunium/sdk/pkg/v1/app/consensus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ---- the cluster's side: the autopilot marks a dead member's key ----

// The autopilot removes a dead member and marks its key (#209): the
// revocation is asked before the change and written once it is made, on
// every member; the key is refused at the JOIN under any id, a new key for
// the slot is admitted.
func TestTheAutopilotMarksTheKeyOfADeadMember(t *testing.T) {
	mem, rs := NewForTesting(t.TempDir(), "node-a", "node-b", "node-c", "node-d", "node-e")
	ctx := context.Background()
	leader := rs[0]
	require.NoError(t, leader.RaisePayloadVersion(ctx, "op"))
	mem.SetLeader(leader.Engine().(*memraft.Node))
	idB := slotID(t, leader, "node-b")
	keyB := sha256.Sum256([]byte("node-b"))

	require.True(t, leader.removeDead(ctx, leader.Engine(), idB))
	assert.Zero(t, Rank(leader.Engine().Membership(), idB))
	for _, r := range rs {
		assert.True(t, r.table.KeyRevoked(hex.EncodeToString(keyB[:])), "%s: the dead member's key is revoked", r.Engine().Status().Slot)
	}

	leader.auto = &autoMode{expected: 5, self: "node-a", peers: map[string]string{"node-a": "a:1", "node-b": "b:1", "node-c": "c:1", "node-d": "d:1", "node-e": "e:1"}}
	_, err := leader.authorizeJoin(ctx, consensus.JoinRequest{Slot: "node-b", SPKI: keyB})
	require.ErrorContains(t, err, "revoked", "the old key, under a new id")
	_, err = leader.authorizeJoin(ctx, consensus.JoinRequest{Slot: "node-b", SPKI: sha256.Sum256([]byte("node-b, a new key"))})
	require.NoError(t, err, "a new key for the slot")
}

// The mark never cuts a live member (#64, #165, #171): a removal the engine
// refuses for certain leaves no request; one whose outcome is unknown keeps
// its request, but the key is written only once no member holds it, so the
// member stays admitted on the object channel while it is in place.
func TestTheMarkOfADeadMemberNeverCutsALiveOne(t *testing.T) {
	mem, rs := NewForTesting(t.TempDir(), "node-a", "node-b", "node-c", "node-d", "node-e")
	ctx := context.Background()
	leader := rs[0]
	require.NoError(t, leader.RaisePayloadVersion(ctx, "op"))
	mem.SetLeader(leader.Engine().(*memraft.Node))
	idB, idC := slotID(t, leader, "node-b"), slotID(t, leader, "node-c")
	keyC := sha256.Sum256([]byte("node-c"))
	real := leader.Engine()

	leader.setEngine(refusingEngine{real})
	require.False(t, leader.removeDead(ctx, leader.Engine(), idB))
	leader.setEngine(real)
	for _, r := range rs {
		assert.Empty(t, r.table.PendingRevocations(), "a refused removal leaves no request")
		revoked, admitted := keyState(t, r, "node-b")
		assert.False(t, revoked)
		require.NoError(t, admitted, "node-b keeps its object channel")
	}
	assert.Positive(t, Rank(leader.Engine().Membership(), idB))

	leader.setEngine(uncertainChangeEngine{real})
	require.False(t, leader.removeDead(ctx, leader.Engine(), idC))
	require.False(t, leader.removeDead(ctx, leader.Engine(), idC), "retried at the next pass")
	leader.setEngine(real)
	require.NoError(t, leader.flushRevocations(ctx))
	for _, r := range rs {
		assert.Len(t, r.table.RevocationRequests(hex.EncodeToString(keyC[:])), 1, "an unknown outcome keeps its request, which the retry reuses")
		revoked, admitted := keyState(t, r, "node-c")
		assert.False(t, revoked, "not written while node-c holds the key")
		require.NoError(t, admitted, "node-c is not cut off")
	}

	// It leaves for good: the key is written.
	require.True(t, leader.removeDead(ctx, real, idC))
	for _, r := range rs {
		revoked, _ := keyState(t, r, "node-c")
		assert.True(t, revoked)
	}
}

// A removal the engine keeps refusing for certain records a request and
// its withdrawal once, then waits markBackoff before it marks the key
// again: the replicated audit, which keeps the newest records only, is
// not flooded at each pass.
func TestARefusedMarkWaitsBeforeItIsTriedAgain(t *testing.T) {
	mem, rs := NewForTesting(t.TempDir(), "node-a", "node-b", "node-c", "node-d", "node-e")
	ctx := context.Background()
	leader := rs[0]
	require.NoError(t, leader.RaisePayloadVersion(ctx, "op"))
	mem.SetLeader(leader.Engine().(*memraft.Node))
	clock := &fakeClock{t: time.Date(2026, 10, 25, 12, 0, 0, 0, time.UTC)}
	leader.auto = &autoMode{now: clock.now}
	idB := slotID(t, leader, "node-b")
	real := leader.Engine()
	leader.setEngine(refusingEngine{real})
	require.False(t, leader.removeDead(ctx, leader.Engine(), idB))
	records := len(leader.table.Audit())
	for range 5 {
		clock.advance(2 * time.Second)
		require.False(t, leader.removeDead(ctx, leader.Engine(), idB))
	}
	assert.Len(t, leader.table.Audit(), records, "nothing written while it waits")
	clock.advance(markBackoff)
	require.False(t, leader.removeDead(ctx, leader.Engine(), idB))
	assert.Greater(t, len(leader.table.Audit()), records, "tried again after markBackoff")
	assert.Empty(t, leader.table.PendingRevocations(), "each refusal withdraws its request")
	leader.setEngine(real)
	clock.advance(markBackoff)
	require.True(t, leader.removeDead(ctx, real, idB))
	keyB := sha256.Sum256([]byte("node-b"))
	assert.True(t, leader.table.KeyRevoked(hex.EncodeToString(keyB[:])))
}

// A mixed cluster: while it writes a payload version below WithdrawVersion,
// some member's binary cannot withdraw a request, and the autopilot's
// removal of a dead member asks nothing, as before; from it, it marks the
// key. No new command kind, no new payload version.
func TestTheMarkWaitsForEveryMemberToWithdrawIt(t *testing.T) {
	mem, rs := NewForTesting(t.TempDir(), "node-a", "node-b", "node-c", "node-d", "node-e")
	ctx := context.Background()
	leader := rs[0]
	mem.SetLeader(leader.Engine().(*memraft.Node))
	raise, err := reftable.Encode(reftable.KindAudit, &reftable.Audit{
		ID: "raise-6", Operator: "op", Action: reftable.AuditPayloadVersion, Target: "6", Outcome: reftable.AuditRaised,
	})
	require.NoError(t, err)
	_, res, err := leader.proposeCommand(ctx, raise, nil)
	require.NoError(t, err)
	require.True(t, res.OK)
	require.Equal(t, uint8(reftable.WithdrawVersion-1), leader.table.WriteVersion())

	idB := slotID(t, leader, "node-b")
	require.True(t, leader.removeDead(ctx, leader.Engine(), idB))
	for _, r := range rs {
		assert.Empty(t, r.table.PendingRevocations(), "below WithdrawVersion nothing is asked")
	}

	require.NoError(t, leader.RaisePayloadVersion(ctx, "op"))
	idC := slotID(t, leader, "node-c")
	keyC := sha256.Sum256([]byte("node-c"))
	require.True(t, leader.removeDead(ctx, leader.Engine(), idC))
	for _, r := range rs {
		assert.True(t, r.table.KeyRevoked(hex.EncodeToString(keyC[:])), "from WithdrawVersion the key is marked")
	}
	assert.Equal(t, 8, reftable.PayloadVersion, "no payload version raised for it")
}

// ---- the node's side: a new id takes a new key ----

// pkiSigner signs a CSR with the test CA, as Vault's sign endpoint does:
// the CSR's key, the URI SAN asked for.
type pkiSigner struct {
	p     *testPKI
	calls atomic.Int32
	fail  atomic.Bool
}

func (s *pkiSigner) Sign(_ context.Context, csrPEM []byte, uriSAN string) ([]byte, error) {
	s.calls.Add(1)
	if s.fail.Load() {
		return nil, errors.New("vault sealed")
	}
	blk, _ := pem.Decode(csrPEM)
	csr, err := x509.ParseCertificateRequest(blk.Bytes)
	if err != nil {
		return nil, err
	}
	if err := csr.CheckSignature(); err != nil {
		return nil, err
	}
	u, err := url.Parse(uriSAN)
	if err != nil {
		return nil, err
	}
	tpl := &x509.Certificate{
		SerialNumber: big.NewInt(time.Now().UnixNano()), NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(24 * time.Hour),
		URIs: []*url.URL{u}, KeyUsage: x509.KeyUsageDigitalSignature,
		ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth},
	}
	der, err := x509.CreateCertificate(rand.Reader, tpl, s.p.caCert, csr.PublicKey, s.p.caKey)
	if err != nil {
		return nil, err
	}
	return pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}), nil
}

// installKey issues a new key and certificate for slot over the file the
// node reads, as an operator installs them. The file is written in place:
// its modification time is moved a second ahead, so the identity sees the
// change however coarse the file system's clock is, as it sees a file an
// operator copies seconds later.
func installKey(t *testing.T, pki *testPKI, cluster consensus.ClusterID, slot string) [32]byte {
	t.Helper()
	file, key := pki.issue(t, cluster.String(), slot)
	later := time.Now().Add(time.Second)
	require.NoError(t, os.Chtimes(file, later, later))
	return key
}

// keyNode is a replicator whose object channel reads a node certificate of
// the test CA for slot, and whose renewer signs with that CA.
type keyNode struct {
	r      *Replicator
	file   string
	key    [32]byte
	signer *pkiSigner
	rn     *certrenew.Renewer
}

func newKeyNode(t *testing.T, pki *testPKI, cluster consensus.ClusterID, slot string) *keyNode {
	file, key := pki.issue(t, cluster.String(), slot)
	id, err := newIdentity(file, file, pki.caFile)
	require.NoError(t, err)
	r := newReplicator(t.TempDir(), newRegistry(time.Second, 8, 64))
	r.objects = &objectChannel{id: id}
	s := &pkiSigner{p: pki}
	rn := &certrenew.Renewer{CertFile: file, KeyFile: file, CAFile: pki.caFile, Slot: slot, Identity: certIdentity, Signer: s}
	return &keyNode{r: r, file: file, key: key, signer: s, rn: rn}
}

// stampDriver is a data directory form stamps: empty until Init, which
// reads the key the node presents at that moment.
type stampDriver struct {
	consensus.Driver
	node      *keyNode
	rep       consensus.Report
	stamped   bool
	inits     int
	keyAtInit [32]byte
	bootstrap []consensus.Member
}

func (d *stampDriver) Inspect(string) (consensus.Report, error) {
	if !d.stamped {
		return consensus.Report{}, os.ErrNotExist
	}
	return d.rep, nil
}

func (d *stampDriver) Init(_ string, cluster consensus.ClusterID, slot string) (consensus.NodeID, consensus.Incarnation, error) {
	d.inits++
	d.keyAtInit = d.node.r.presentedKey()
	d.stamped = true
	d.rep = consensus.Report{Cluster: cluster, Node: 0xd2, Incarnation: consensus.Incarnation{0xd2}, Slot: slot, Joining: true}
	return d.rep.Node, d.rep.Incarnation, nil
}

func (d *stampDriver) Bootstrap(_ context.Context, _ consensus.Config, ms []consensus.Member) error {
	d.bootstrap = ms
	return nil
}

// A node that stamps a new id on an empty data directory gets a new key
// first, signed by the CA under its slot, and stamps only then; restarted
// on that stamp, it keeps the key. A founder bootstraps under the new key.
// Without a renewer the key stays; a signer that refuses holds the stamp
// back (#209).
func TestANewStampTakesANewKeyFirst(t *testing.T) {
	pki := newPKI(t)
	cluster := consensus.ClusterID{0x20, 0x9a}
	auto := func(self, state string) *autoMode {
		return &autoMode{
			expected: 3, self: self, tick: 10 * time.Millisecond,
			peers: map[string]string{"node-a": "a:1", "node-b": "b:1", "node-c": "c:1"},
			probe: func(_ context.Context, _, slot string) (hello, error) {
				return hello{Slot: slot, State: state, Expected: 3}, nil
			},
		}
	}
	cfg := consensus.Config{DataDir: t.TempDir(), Slot: "node-b", ListenAddress: "b:1"}

	t.Run("joiner", func(t *testing.T) {
		n := newKeyNode(t, pki, cluster, "node-b")
		n.r.renewer, n.r.auto = n.rn, auto("node-b", helloMember)
		d := &stampDriver{node: n}
		require.NoError(t, n.r.form(t.Context(), d, cfg, cluster, n.r.presentedKey))
		require.Equal(t, 1, d.inits)
		assert.NotEqual(t, n.key, d.keyAtInit, "stamped under a new key")
		assert.Equal(t, d.keyAtInit, n.r.presentedKey())
		assert.Equal(t, int32(1), n.signer.calls.Load())
		leaf, err := n.rn.Current()
		require.NoError(t, err)
		require.Len(t, leaf.URIs, 1)
		assert.Equal(t, certrenew.URISAN(cluster.String(), "node-b"), leaf.URIs[0].String(), "the same slot")

		// Restarted on its stamp: the key stays.
		require.NoError(t, n.r.form(t.Context(), d, cfg, cluster, n.r.presentedKey))
		assert.Equal(t, 1, d.inits)
		assert.Equal(t, int32(1), n.signer.calls.Load(), "no key made again for the same stamp")
		h := n.r.helloOf()
		assert.Equal(t, consensus.Incarnation{0xd2}.String(), h.Incarnation, "the hello carries the stamp")
	})

	t.Run("founder", func(t *testing.T) {
		n := newKeyNode(t, pki, cluster, "node-a")
		n.r.renewer, n.r.auto = n.rn, auto("node-a", helloJoining)
		d := &stampDriver{node: n}
		fcfg := cfg
		fcfg.Slot = "node-a"
		require.NoError(t, n.r.form(t.Context(), d, fcfg, cluster, n.r.presentedKey))
		require.Len(t, d.bootstrap, 1)
		assert.Equal(t, d.keyAtInit, d.bootstrap[0].SPKI, "the founding list pins the new key")
		assert.NotEqual(t, n.key, d.bootstrap[0].SPKI)
	})

	t.Run("no renewer", func(t *testing.T) {
		n := newKeyNode(t, pki, cluster, "node-b")
		n.r.auto = auto("node-b", helloMember)
		d := &stampDriver{node: n}
		require.NoError(t, n.r.form(t.Context(), d, cfg, cluster, n.r.presentedKey))
		assert.Equal(t, n.key, d.keyAtInit, "the key of TLS_KEY_FILE stays")
	})

	t.Run("refused", func(t *testing.T) {
		defer func(d time.Duration) { newKeyRetry = d }(newKeyRetry)
		newKeyRetry = 10 * time.Millisecond
		n := newKeyNode(t, pki, cluster, "node-b")
		n.r.renewer, n.r.auto = n.rn, auto("node-b", helloMember)
		n.signer.fail.Store(true)
		d := &stampDriver{node: n}
		ctx, cancel := context.WithTimeout(t.Context(), 300*time.Millisecond)
		defer cancel()
		require.Error(t, n.r.form(ctx, d, cfg, cluster, n.r.presentedKey))
		assert.Zero(t, d.inits, "nothing stamped without a new key")
		assert.Greater(t, n.signer.calls.Load(), int32(1), "asked again")
		assert.Equal(t, n.key, n.r.presentedKey())
	})
}

// rejoinEngine is an engine a rejoin closes or opens.
type rejoinEngine struct {
	consensus.Engine
	closed atomic.Bool
}

func (e *rejoinEngine) Close(context.Context) error      { e.closed.Store(true); return nil }
func (e *rejoinEngine) Status() consensus.Status         { return consensus.Status{ID: 0xe2} }
func (e *rejoinEngine) Membership() consensus.Membership { return consensus.Membership{} }
func (e *rejoinEngine) Barrier(context.Context) (consensus.Token, error) {
	return consensus.Token{}, nil
}
func (e *rejoinEngine) WaitApplied(context.Context, consensus.Token) error { return nil }

// listedEngine is a retired id's engine whose membership still lists it,
// under the key it held.
type listedEngine struct {
	rejoinEngine
	m consensus.Membership
}

func (e *listedEngine) Membership() consensus.Membership { return e.m }

// rejoinDriver records the key the node presents when its new engine
// opens.
type rejoinDriver struct {
	consensus.Driver
	node      *keyNode
	inits     int
	keyAtOpen [32]byte
}

func (d *rejoinDriver) Inspect(string) (consensus.Report, error) { return consensus.Report{}, nil }

func (d *rejoinDriver) Init(dir string, cluster consensus.ClusterID, slot string) (consensus.NodeID, consensus.Incarnation, error) {
	d.inits++
	return 0xe2, consensus.Incarnation{0xe2}, os.MkdirAll(dir, 0o700)
}

func (d *rejoinDriver) Open(context.Context, consensus.Config) (consensus.Engine, error) {
	d.keyAtOpen = d.node.r.presentedKey()
	return &rejoinEngine{}, nil
}

// A node whose id the cluster retired rejoins under a new id with a new
// key, made before anything moves; without a renewer it touches nothing
// and says why, until an operator installs a new key, with which it
// rejoins (#209).
func TestARetiredNodeRejoinsOnlyWithANewKey(t *testing.T) {
	pki := newPKI(t)
	cluster := consensus.ClusterID{0x20, 0x9b}
	setup := func(t *testing.T) (*keyNode, *rejoinEngine, *rejoinDriver, consensus.Config) {
		n := newKeyNode(t, pki, cluster, "node-e")
		n.r.ctx = t.Context()
		n.r.consumer = &consumer{r: n.r}
		n.r.auto = &autoMode{self: "node-e", expected: 3}
		old := &rejoinEngine{}
		n.r.setEngine(old)
		n.r.setOpenedKey(n.key)
		cfg := consensus.Config{DataDir: filepath.Join(t.TempDir(), "data"), Slot: "node-e"}
		require.NoError(t, os.MkdirAll(cfg.DataDir, 0o700))
		return n, old, &rejoinDriver{node: n}, cfg
	}

	t.Run("no renewer", func(t *testing.T) {
		n, old, d, cfg := setup(t)
		require.ErrorIs(t, n.r.rejoinUnderNewID(t.Context(), d, cfg, cluster), errNeedsNewKey)
		assert.False(t, old.closed.Load(), "the retired engine is left as it is")
		assert.Zero(t, d.inits, "no new id")
		_, err := os.Stat(cfg.DataDir)
		require.NoError(t, err, "the data directory is not moved")

		// An operator installs a new key: the node rejoins with it.
		key := installKey(t, pki, cluster, "node-e")
		require.NoError(t, n.r.rejoinUnderNewID(t.Context(), d, cfg, cluster))
		assert.Equal(t, key, d.keyAtOpen)
		assert.Equal(t, key, n.r.openedKey())
		assert.True(t, old.closed.Load())
	})

	// Restarted, the retired id's engine opens with the key on disk: the
	// key the retired id held is the one its membership lists.
	listed := func(n *keyNode) *listedEngine {
		return &listedEngine{m: consensus.Membership{Members: []consensus.Member{{ID: 0xe2, Slot: "node-e", SPKI: n.key, Voter: true}}}}
	}
	t.Run("restarted on its old key", func(t *testing.T) {
		n, _, d, cfg := setup(t)
		old := listed(n)
		n.r.setEngine(old)
		require.ErrorIs(t, n.r.rejoinUnderNewID(t.Context(), d, cfg, cluster), errNeedsNewKey)
		assert.False(t, old.closed.Load())
	})
	t.Run("restarted on a new key", func(t *testing.T) {
		n, _, d, cfg := setup(t)
		old := listed(n)
		n.r.setEngine(old)
		key := installKey(t, pki, cluster, "node-e")
		n.r.setOpenedKey(key)
		require.NoError(t, n.r.rejoinUnderNewID(t.Context(), d, cfg, cluster), "the operator's new key, then a restart")
		assert.Equal(t, key, d.keyAtOpen)
		assert.True(t, old.closed.Load())
	})
	t.Run("a key the table refuses", func(t *testing.T) {
		n, _, d, cfg := setup(t)
		n.r.setEngine(listed(n))
		key := installKey(t, pki, cluster, "node-e")
		revokeKey(t, n.r.table, key)
		require.ErrorIs(t, n.r.rejoinUnderNewID(t.Context(), d, cfg, cluster), errNeedsNewKey, "a revoked key is no new key")
	})

	t.Run("renewer", func(t *testing.T) {
		n, old, d, cfg := setup(t)
		n.r.renewer = n.rn
		require.NoError(t, n.r.rejoinUnderNewID(t.Context(), d, cfg, cluster))
		assert.True(t, old.closed.Load())
		assert.Equal(t, 1, d.inits)
		assert.NotEqual(t, n.key, d.keyAtOpen, "the new engine presents a new key")
		assert.Equal(t, d.keyAtOpen, n.r.openedKey())
		assert.Equal(t, int32(1), n.signer.calls.Load())
		h := n.r.helloOf()
		assert.Equal(t, helloJoining, h.State)
		assert.Equal(t, consensus.Incarnation{0xe2}.String(), h.Incarnation)
	})
}

// End to end, over TCP with the CA's certificates (#209): four voters, the
// autopilot removes a dead one and marks its key. The node comes back on an
// empty data directory: with its old key, its JOIN is refused; with a new
// key from the renewer, it is admitted, and it follows the leader.
func TestARemovedMemberReturnsWithANewKeyOverTCP(t *testing.T) {
	pki := newPKI(t)
	cluster := sdk.ClusterID{0x0f, 0x07, 0x9e, 0x20}
	timing := sdk.TimingContractSpec{HeartbeatInterval: 20 * time.Millisecond, ElectionTimeoutMin: 200 * time.Millisecond, ElectionTimeoutMax: 400 * time.Millisecond}
	slots := []string{"node-a", "node-b", "node-c", "node-d"}
	peers := map[string]string{}
	for _, s := range slots {
		peers[s] = s + ":1"
	}
	type part struct {
		slot string
		file string
		dd   sdk.DataDir
		tr   *sdk.TCPTransport
		node *sdk.Node
		r    *Replicator
	}
	open := func(slot, file string) (*part, sdk.NodeID, sdk.Incarnation) {
		p := &part{slot: slot, file: file, r: newReplicator(t.TempDir(), newRegistry(time.Second, 8, 64))}
		p.r.auto = &autoMode{expected: len(slots), self: slot, peers: peers}
		dd, err := sdk.NewDataDirWith(filepath.Join(t.TempDir(), "data"), sdk.DataDirOptions{RuntimeDir: kitsunium.RuntimeDir})
		require.NoError(t, err)
		p.dd = dd
		tr, err := sdk.NewTCPTransportFromPEM(file, file, pki.caFile, &sdk.TransportConfig{ListenAddress: "127.0.0.1:0"})
		require.NoError(t, err)
		p.tr = tr
		id, inc, err := sdk.Init(dd, cluster, slot)
		require.NoError(t, err)
		return p, id, inc
	}
	cfg := func(p *part) sdk.Config {
		return sdk.Config{
			Cluster: cluster, Machine: kitsunium.Machine(p.r.table), Durability: sdk.DurabilityVolatile,
			Log: p.dd.Log(), Snapshots: p.dd.Snapshots(), Transport: p.tr, InitialTiming: timing,
			MaxSnapshotBytes: 1 << 30, MaxLogBytes: 1 << 30, MinVoters: autoMinVoters, SnapshotPausesApply: true,
			JoinAuthorizer: joinAuthorizerFor(p.r),
		}
	}
	start := func(p *part) {
		n, err := sdk.NewNode(cfg(p))
		require.NoError(t, err)
		p.node = n
		t.Cleanup(func() {
			_ = n.Close(context.Background())
			_ = p.tr.Close()
			_ = p.dd.Close()
		})
		p.r.setEngine(kitsunium.Wrap(n, p.slot))
	}
	var founders []*part
	var members []sdk.MemberValue
	keys := map[string][32]byte{}
	for _, slot := range slots {
		file, spki := pki.issue(t, cluster.String(), slot)
		p, id, inc := open(slot, file)
		founders = append(founders, p)
		keys[slot] = spki
		members = append(members, sdk.MemberValue{ID: id, Incarnation: inc, Slot: slot, SPKI: spki, Address: p.tr.Addr(), Voter: true})
	}
	for _, p := range founders {
		c := cfg(p)
		require.NoError(t, sdk.Bootstrap(context.Background(), &c, members))
		start(p)
	}
	var leader *part
	require.Eventually(t, func() bool {
		for _, p := range founders {
			if p.r.Engine().Status().Role == consensus.RoleLeader {
				leader = p
				return true
			}
		}
		return false
	}, 10*time.Second, 20*time.Millisecond)
	ctx := t.Context()
	require.NoError(t, leader.r.RaisePayloadVersion(ctx, "op"))

	// A follower dies; past DEAD_MEMBER_TIMEOUT the autopilot removes it.
	var dead *part
	var idD consensus.NodeID
	var alive []*part
	for i, p := range founders {
		if p != leader && dead == nil {
			dead, idD = p, consensus.NodeID(members[i].ID)
			continue
		}
		alive = append(alive, p)
	}
	require.NoError(t, dead.node.Close(context.Background()))
	require.NoError(t, dead.tr.Close())
	require.Eventually(t, func() bool { return leader.r.removeDead(ctx, leader.r.Engine(), idD) }, 10*time.Second, 200*time.Millisecond)
	keyD := keys[dead.slot]
	require.Eventually(t, func() bool { return leader.r.table.KeyRevoked(hex.EncodeToString(keyD[:])) }, 5*time.Second, 20*time.Millisecond)

	var addrs []string
	var pins [][32]byte
	for _, p := range alive {
		addrs = append(addrs, p.tr.Addr())
		pins = append(pins, keys[p.slot])
	}
	listed := func(slot string) (consensus.Member, bool) {
		for _, m := range leader.r.Engine().Membership().Members {
			if m.Slot == slot {
				return m, true
			}
		}
		return consensus.Member{}, false
	}
	joinOnce := func(p *part) error {
		stamp, err := p.dd.Log().Stamp()
		require.NoError(t, err)
		jctx, cancel := context.WithTimeout(ctx, 3*time.Second)
		defer cancel()
		_, err = p.tr.Join(jctx, sdk.JoinConfig{Addresses: addrs, Stamp: stamp, Address: p.tr.Addr(), Pins: pins})
		return err
	}

	// It comes back on an empty data directory with its old key: refused.
	stale, _, _ := open(dead.slot, dead.file)
	t.Cleanup(func() { _ = stale.tr.Close(); _ = stale.dd.Close() })
	time.Sleep(timing.ElectionTimeoutMin)
	require.Error(t, joinOnce(stale), "the old key, under a new id")
	_, ok := listed(dead.slot)
	require.False(t, ok, "never admitted")

	// A new key from the renewer, then a new stamp: admitted, and it follows
	// the leader.
	rn := &certrenew.Renewer{CertFile: dead.file, KeyFile: dead.file, CAFile: pki.caFile, Slot: dead.slot, Identity: certIdentity, Signer: &pkiSigner{p: pki}}
	cert, err := rn.Rekey(ctx)
	require.NoError(t, err)
	newKey := sha256.Sum256(cert.RawSubjectPublicKeyInfo)
	require.NotEqual(t, keyD, newKey)
	back, _, _ := open(dead.slot, dead.file)
	start(back)
	require.Eventually(t, func() bool { return joinOnce(back) == nil }, 10*time.Second, 250*time.Millisecond)
	m, ok := listed(dead.slot)
	require.True(t, ok)
	assert.Equal(t, newKey, m.SPKI, "admitted under its new key")
	assert.NotEqual(t, idD, m.ID, "under a new id")

	mark, err := reftable.Encode(reftable.KindAudit, &reftable.Audit{ID: "after-the-return", Operator: "op", Action: "test", Target: dead.slot, Outcome: "ok"})
	require.NoError(t, err)
	_, res, err := leader.r.proposeCommand(ctx, mark, nil)
	require.NoError(t, err)
	require.True(t, res.OK)
	require.Eventually(t, func() bool { return back.r.table.HasAudit("after-the-return") }, 10*time.Second, 20*time.Millisecond, "it follows the leader")
}
