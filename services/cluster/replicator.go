// Copyright 2026 The Forgejo Authors. All rights reserved.
// SPDX-License-Identifier: GPL-3.0-or-later

// Package cluster is Forgejo's in-process ref replicator (kitsunium ADR 0152
// D19): a consensus node replicating a table of refs, a projector making the
// local git repositories match it, an object channel shipping objects to a
// majority before a ref is proposed, the reference-transaction hook path
// that turns every git ref write into a proposal, and the log consumer that
// derives SQL from the log on the leader.
package cluster

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"forgejo.org/services/cluster/nomadgate"

	"forgejo.org/modules/clusterhook"
	"forgejo.org/modules/graceful"
	"forgejo.org/modules/log"
	"forgejo.org/modules/setting"
	"forgejo.org/services/cluster/consensus"
	"forgejo.org/services/cluster/reftable"
)

// Replicator is one node's replicator.
type Replicator struct {
	root      string
	table     *reftable.Table
	registry  *registry
	projector *projector
	objects   *objectChannel
	consumer  *consumer

	mu     sync.RWMutex
	engine consensus.Engine

	// raw maps a transaction's nonce to the token of its entry, for the
	// read-after-write cookie of the request that ran it.
	rawMu sync.Mutex
	raw   map[string]rawWrite

	started time.Time
	// wake signals the consumer that the table changed.
	wake chan struct{}
	ctx  context.Context
	// gate reads Nomad for the JOIN drain check; nil off Nomad. Tests set
	// it directly.
	gate *nomadgate.Gate
	// auto is the autonomous mode ([cluster] EXPECTED_NODES from 3); nil
	// in the manual mode.
	auto *autoMode
	// writerHook follows the right to write (writer.go); nil without one.
	writerHook WriterHook
	writer     atomic.Uint32
	// dead is when the leader found each member out of reach; seen is
	// what this node saw of each member, for the status (autopilot.go,
	// retire.go).
	dead   deadClock
	seenMu sync.Mutex
	seen   map[consensus.NodeID]memberRecord
}

type rawWrite struct {
	token consensus.Token
	repo  string
	at    time.Time
}

var (
	currentMu sync.RWMutex
	current   *Replicator
)

// Current returns the running replicator, or nil when the cluster is off.
func Current() *Replicator {
	currentMu.RLock()
	defer currentMu.RUnlock()
	return current
}

// Enabled reports whether this node runs a replicator.
func Enabled() bool { return Current() != nil }

func newReplicator(root string, reg *registry) *Replicator {
	// Git names repositories by their real path; the root is resolved the
	// same way so keys compare (/tmp is /private/tmp on macOS).
	if real, err := filepath.EvalSymlinks(root); err == nil {
		root = real
	}
	r := &Replicator{
		root:     filepath.Clean(root),
		table:    reftable.New(),
		registry: reg,
		raw:      map[string]rawWrite{},
		started:  time.Now(),
		wake:     make(chan struct{}, 1),
	}
	r.projector = newProjector(r.root, r.table, reg)
	return r
}

// Engine returns the consensus node, nil until it is open.
func (r *Replicator) Engine() consensus.Engine {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.engine
}

func (r *Replicator) setEngine(e consensus.Engine) {
	r.mu.Lock()
	r.engine = e
	r.mu.Unlock()
}

// Table exposes the replicated table, read-only by convention.
func (r *Replicator) Table() *reftable.Table { return r.table }

// Init starts the replicator when [cluster] is enabled. It runs before the
// database is initialised and does not depend on it (ADR 0152 D19, R7-32):
// only the consumer waits for the database.
func Init(ctx context.Context) error {
	if !setting.Cluster.Enabled {
		return nil
	}
	if setting.Cluster.ConfigError != nil {
		return fmt.Errorf("[cluster]: %w", setting.Cluster.ConfigError)
	}
	driver, err := consensus.Lookup(setting.Cluster.Driver)
	if err != nil {
		return err
	}
	// The Recover fence, before anything opens the data directory (R9-09).
	if err := fenceOrExit(ctx, setting.Cluster.DataDir, driver); err != nil {
		return err
	}
	if err := os.MkdirAll(setting.RepoRootPath, 0o755); err != nil {
		return fmt.Errorf("[cluster] repository root: %w", err)
	}
	r := newReplicator(setting.RepoRootPath, newRegistry(setting.Cluster.TxnTTL, setting.Cluster.MaxTxnPerRepo, setting.Cluster.MaxTxnTotal))
	r.ctx = graceful.GetManager().ShutdownContext()

	id, err := newIdentity(setting.Cluster.TLSCertFile, setting.Cluster.TLSKeyFile, setting.Cluster.TLSCAFile)
	if err != nil {
		return fmt.Errorf("[cluster] TLS identity: %w", err)
	}
	clusterHex := setting.Cluster.ClusterID
	var autoCluster consensus.ClusterID
	if setting.Cluster.ExpectedNodes >= 3 {
		if autoCluster, err = autoClusterID(id); err != nil {
			return fmt.Errorf("[cluster] %w", err)
		}
		clusterHex = autoCluster.String()
	}
	r.objects = &objectChannel{
		root: r.root, port: setting.Cluster.ObjectPort, cluster: clusterHex, id: id,
		engine: r.Engine, self: r.selfID, repoKnown: r.repoKnown,
	}
	if setting.Cluster.ExpectedNodes >= 3 {
		r.auto = newAutoMode(r.objects)
		r.objects.hello = r.helloOf
		r.writerHook = registeredWriterHook()
	}
	if len(setting.Cluster.ObjectPeers) > 0 {
		peers := setting.Cluster.ObjectPeers
		r.objects.resolve = func(m consensus.Member) string {
			if a, ok := peers[m.Slot]; ok {
				return a
			}
			host, _, _ := net.SplitHostPort(m.Address)
			return net.JoinHostPort(host, setting.Cluster.ObjectPort)
		}
	}
	r.projector.fetch = r.objects.fetchMissing
	// Before the socket opens: a request on it may read the consumer.
	r.consumer = newConsumer(r)

	// The private socket first: a hook that cannot reach it fails closed.
	if err := r.serveHookSocket(setting.Cluster.HookSocket); err != nil {
		return err
	}
	ln, err := net.Listen("tcp", setting.Cluster.ObjectListenAddr)
	if err != nil {
		return fmt.Errorf("[cluster] object listener: %w", err)
	}
	rot := newConnRotator()
	srv := &http.Server{Handler: r.objects.handler(), TLSConfig: id.serverConfig(), ReadHeaderTimeout: 10 * time.Second, ConnState: rot.ConnState}
	go func() {
		if err := srv.ServeTLS(ln, "", ""); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Error("cluster: object channel stopped: %v", err)
		}
	}()

	cfg := consensus.Config{
		DataDir: setting.Cluster.DataDir, Slot: setting.Cluster.Slot, ListenAddress: setting.Cluster.ListenAddr,
		Seeds: setting.Cluster.Peers, Pins: peerPins(),
		TLS:     consensus.TLSFiles{CertFile: setting.Cluster.TLSCertFile, KeyFile: setting.Cluster.TLSKeyFile, CAFile: setting.Cluster.TLSCAFile},
		Machine: r.table, MaxLogBytes: setting.Cluster.MaxLogBytes, MaxSnapshotBytes: setting.Cluster.MaxSnapshotBytes,
		JoinAuthorizer: r.authorizeJoin,
	}
	if r.auto != nil {
		// The founder's list has one voter: the floor is the rule's, and
		// the founding is checked against one.
		cfg.MinVoters = 1
		if err := r.form(r.ctx, driver, cfg, autoCluster, id.SPKI()); err != nil {
			_ = srv.Close()
			return err
		}
		cfg.MinVoters = autoMinVoters
		cfg.Discover = r.auto.discoverForJoin
		r.auto.rejoin = func(ctx context.Context) error { return r.rejoinUnderNewID(ctx, driver, cfg, autoCluster) }
	}
	eng, err := driver.Open(r.ctx, cfg)
	if err != nil {
		_ = srv.Close()
		return fmt.Errorf("[cluster] opening the consensus node: %w", err)
	}
	r.setEngine(eng)

	currentMu.Lock()
	current = r
	currentMu.Unlock()
	clusterhook.SetEnabled(true)
	clusterhook.SetRepoLifecycle(r.proposeRepoDelete, r.proposeRepoMoves)
	clusterhook.SetOwnerLifecycle(r.proposeOwnerRename)
	clusterhook.SetGarbageCollectionGate(r.projector.MayCollectGarbage)
	clusterhook.SetWriteWaiter(r.waitWrites)

	go r.projector.run(r.ctx)
	go r.startupBarrier(r.ctx)
	go r.consumer.run(r.ctx)
	go r.sweepRaw(r.ctx)
	if r.auto != nil {
		go r.autopilot(r.ctx)
	}
	go watchIdentity(r.ctx, id, identityPoll, func() {
		r.objects.rotate()
		log.Info("cluster: node certificate changed on disk; object channel rotated, %d idle connections closed", rot.rotate())
	})
	if rn := NewRenewer(); rn != nil {
		go runRenewer(r.ctx, rn)
	}

	graceful.GetManager().RunAtShutdown(context.Background(), func() {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		// Close transfers leadership first (D14). The engine is the
		// current one: a rejoin under a new id replaces it.
		if err := r.Engine().Close(ctx); err != nil {
			log.Error("cluster: closing the consensus node: %v", err)
		}
		_ = srv.Close()
	})
	if r.auto != nil {
		log.Info("cluster: replicator started, slot %s, driver %s, autonomous for %d nodes", setting.Cluster.Slot, driver.Name(), r.auto.expected)
		return nil
	}
	log.Info("cluster: replicator started, slot %s, driver %s", setting.Cluster.Slot, driver.Name())
	return nil
}

// rejoinUnderNewID replaces the engine of a node whose id the cluster
// retired (retire.go): the old engine closed, its data directory moved
// aside, a new id stamped, and a new engine opened, joining.
func (r *Replicator) rejoinUnderNewID(ctx context.Context, driver consensus.Driver, cfg consensus.Config, cluster consensus.ClusterID) error {
	if old := r.Engine(); old != nil {
		cctx, cancel := context.WithTimeout(ctx, 30*time.Second)
		err := old.Close(cctx)
		cancel()
		if err != nil {
			log.Warn("cluster: closing the retired engine: %v", err)
		}
	}
	// A cluster a Recover superseded is never joined again under its id,
	// and nothing is moved before the witnesses, when configured, said so.
	if err := fenceClusterOrExit(ctx, cluster); err != nil {
		return err
	}
	moved, err := retireDataDir(cfg.DataDir, time.Now())
	if err != nil {
		return err
	}
	log.Warn("cluster: the retired data directory is kept at %s", moved)
	id, _, err := driver.Init(cfg.DataDir, cluster, cfg.Slot)
	if err != nil {
		return fmt.Errorf("stamping a new id: %w", err)
	}
	r.auto.noteDisk(false)
	eng, err := driver.Open(r.ctx, cfg)
	if err != nil {
		return fmt.Errorf("opening under the new id %s: %w", id, err)
	}
	r.setEngine(eng)
	log.Warn("cluster: rejoining under the new id %s", id)
	return nil
}

func (r *Replicator) selfID() consensus.NodeID {
	if e := r.Engine(); e != nil {
		return e.Status().ID
	}
	return 0
}

// startupBarrier lifts the projector's fence once a commit boundary
// confirmed by a quorum after the restart is applied (R4-19, R5-03).
func (r *Replicator) startupBarrier(ctx context.Context) {
	for ctx.Err() == nil {
		eng := r.Engine()
		bctx, cancel := context.WithTimeout(ctx, 5*time.Second)
		tok, err := eng.Barrier(bctx)
		if err == nil {
			err = eng.WaitApplied(bctx, tok)
		}
		cancel()
		if err == nil {
			r.projector.unfence()
			log.Info("cluster: start-up barrier at %d applied, projection unfenced", tok.Index)
			return
		}
		select {
		case <-ctx.Done():
		case <-time.After(time.Second):
		}
	}
}

// Ready is the readiness of ADR 0152 D19 (R4-18): the engine is ready, the
// table rebuilt, and the projector caught up once since the start.
func (r *Replicator) Ready() bool {
	eng := r.Engine()
	if eng == nil {
		return false
	}
	h := eng.Health()
	return h.Ready && !h.Stopped && r.projector.Ready()
}

// repoKey turns a git directory under the repository root into the table's
// key, lexically.
func (r *Replicator) repoKey(gitDir string) (string, error) {
	if real, err := filepath.EvalSymlinks(gitDir); err == nil {
		gitDir = real
	}
	rel, err := filepath.Rel(r.root, filepath.Clean(gitDir))
	if err != nil || strings.HasPrefix(rel, "..") {
		return "", fmt.Errorf("%s is outside the repository root", gitDir)
	}
	key := filepath.ToSlash(rel)
	if err := reftable.ValidRepoKey(key); err != nil {
		return "", err
	}
	return key, nil
}

// ---- read-after-write ----

func (r *Replicator) recordRaw(nonce, repo string, tok consensus.Token) {
	r.rawMu.Lock()
	r.raw[nonce] = rawWrite{token: tok, repo: repo, at: time.Now()}
	r.rawMu.Unlock()
}

// LastWrite returns the newest write among a request's git invocations.
func (r *Replicator) LastWrite(nonces []string) (consensus.Token, string, bool) {
	r.rawMu.Lock()
	defer r.rawMu.Unlock()
	var best rawWrite
	found := false
	for _, n := range nonces {
		if w, ok := r.raw[n]; ok && (!found || w.token.Index > best.token.Index) {
			best, found = w, true
		}
	}
	return best.token, best.repo, found
}

func (r *Replicator) sweepRaw(ctx context.Context) {
	t := time.NewTicker(time.Minute)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
		cut := time.Now().Add(-5 * time.Minute)
		r.rawMu.Lock()
		for n, w := range r.raw {
			if w.at.Before(cut) {
				delete(r.raw, n)
			}
		}
		r.rawMu.Unlock()
	}
}

// WaitReadAfterWrite waits until this node serves a repository at least at
// a token: projected, and its SQL derived by the consumer (R8-24). It
// reports false when the wait timed out and the page may be behind.
func (r *Replicator) WaitReadAfterWrite(ctx context.Context, tok consensus.Token, repo string) bool {
	_, ident := r.table.Applied()
	if ident != (consensus.LogIdentity{}) && tok.Identity() != ident {
		return true // another lineage: nothing to wait for, and nothing to trust
	}
	ctx, cancel := context.WithTimeout(ctx, setting.Cluster.ReadAfterWriteTimeout)
	defer cancel()
	if err := r.projector.Wait(ctx, repo, tok.Index); err != nil {
		return false
	}
	return r.consumer.waitCursor(ctx, tok) == nil
}

// WaitProjected waits until the local projection of repo reaches index.
func (r *Replicator) WaitProjected(ctx context.Context, repo string, index uint64) error {
	return r.projector.Wait(ctx, repo, index)
}

func (r *Replicator) signalConsumer() {
	select {
	case r.wake <- struct{}{}:
	default:
	}
}

// waitWrites waits for the newest write among nonces, read-after-write.
func (r *Replicator) waitWrites(ctx context.Context, nonces []string) bool {
	tok, repo, ok := r.LastWrite(nonces)
	if !ok {
		return true
	}
	return r.WaitReadAfterWrite(ctx, tok, repo)
}

// repoKnown answers the object channel's question, "does Forgejo know this
// repository?", before it touches the disk. The replicated table answers
// first: a repository Forgejo is creating has its HEAD in the table, written
// by `git init` through the hook, while its row is still uncommitted in
// the creating request's transaction, and the first push of an
// auto-initialised repository ships its objects then. Only Forgejo's own
// hook path proposes, so a key in the table was created by a member.
func (r *Replicator) repoKnown(ctx context.Context, key string) (bool, error) {
	if refs, _ := r.table.Refs(key); len(refs) > 0 {
		if _, deleted := r.table.Deleted(key); !deleted {
			return true, nil
		}
	}
	return repoKnownInDB(ctx, key)
}

// peerPins decodes [cluster] PEER_PINS, already validated.
func peerPins() [][32]byte {
	slots := make([]string, 0, len(setting.Cluster.PeerPins))
	for slot := range setting.Cluster.PeerPins {
		slots = append(slots, slot)
	}
	sort.Strings(slots)
	pins := make([][32]byte, 0, len(slots))
	for _, slot := range slots {
		var pin [32]byte
		if b, err := hex.DecodeString(setting.Cluster.PeerPins[slot]); err == nil && len(b) == 32 {
			copy(pin[:], b)
			pins = append(pins, pin)
		}
	}
	return pins
}
