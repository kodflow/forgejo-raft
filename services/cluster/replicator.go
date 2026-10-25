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

	"forgejo.org/modules/clusterhook"
	"forgejo.org/modules/graceful"
	"forgejo.org/modules/log"
	"forgejo.org/modules/setting"
	"forgejo.org/modules/storage"
	"forgejo.org/services/cluster/certrenew"
	"forgejo.org/services/cluster/consensus"
	"forgejo.org/services/cluster/nomadgate"
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
	// maxEntry is consensus.MaxEntryBytes, the engine's bound on one
	// entry, past which a lifecycle command is refused before it is
	// proposed (#182); a test lowers it.
	maxEntry int
	// memberProbe replaces the object channel's hello probe in tests.
	memberProbe func(ctx context.Context, consensusAddr, slot string) (hello, error)

	mu     sync.RWMutex
	engine consensus.Engine

	// raw maps a transaction's nonce to the token of its entry, for the
	// read-after-write cookie of the request that ran it.
	rawMu sync.Mutex
	raw   map[string]rawWrite

	started time.Time
	// hookAcceptFailing is set while the hook socket's Accept fails: no
	// git write reaches this node then.
	hookAcceptFailing atomic.Bool
	// wake signals the consumer that the table changed.
	wake chan struct{}
	ctx  context.Context
	// gate reads Nomad for the JOIN drain check; nil off Nomad. Tests set
	// it directly.
	gate *nomadgate.Gate
	// auto is the autonomous mode ([cluster] EXPECTED_NODES from 3); nil
	// in the manual mode.
	auto *autoMode
	// renewer renews the node certificate and makes the new key of a new
	// node id (newkey.go); nil without RENEW_VAULT_ADDR. engineKey is the
	// key the open engine's transport presents, read when it opened: the
	// transport never takes another one while it runs (openedKey).
	renewer   *certrenew.Renewer
	engineKey atomic.Pointer[[32]byte]
	// writerHook follows the right to write (writer.go); nil without one.
	writerHook WriterHook
	writer     atomic.Uint32
	writerAt   atomic.Pointer[writerMark]
	// dead is when the leader found each member out of reach, and
	// markRefused when the engine last refused for certain the removal of
	// a dead member whose key it marked; seen is what this node saw of each
	// member, for the status (autopilot.go, retire.go).
	dead        deadClock
	markRefused map[consensus.NodeID]time.Time
	// payloadProbe is when the autopilot next asks the members which
	// command version they apply, and clusterProbe when it next asks the
	// engine to raise the cluster version (clusterversion.go).
	payloadProbe time.Time
	clusterProbe time.Time
	// hookFails counts the writer hook's failed promotions in a row, and
	// yielded is when this leader last gave the leadership away because it
	// could not derive the SQL (autopilot.go). Only the autopilot's
	// goroutine touches them.
	hookFails int
	yielded   time.Time
	// sqlYield is when, in Unix seconds, this node last gave the leadership
	// away because it could not derive the SQL; its hello says it, so the
	// leadership is not handed back to it for its checkpoints
	// (checkpointlead.go).
	sqlYield atomic.Int64
	// checkpointProbe is when the leader next asks the members whether they
	// take the checkpoints, checkpointYielded when it last handed the
	// leadership to one that does, and ckState what it found, for the
	// health. Only the autopilot's goroutine, or the manual mode's pilot,
	// touches the first two.
	checkpointProbe   time.Time
	checkpointYielded time.Time
	ckState           checkpointState
	// consumerFails counts the consumer's failed passes in a row, which
	// the autopilot reads (consumer.go).
	consumerFails atomic.Int32
	seenMu        sync.Mutex
	seen          map[consensus.NodeID]memberRecord
	// fence queues this node's own writers while a checkpoint reads
	// PostgreSQL's position; checkpoints is the coordinator, nil while
	// [cluster] CHECKPOINT_INTERVAL is 0 (checkpoint.go). The consumer and
	// the health read it, a test may swap it.
	fence       sync.RWMutex
	checkpoints atomic.Pointer[coordinator]
	// checkpointing counts the checkpoints in flight on this node, from
	// before their barrier until their backup cursor is written: the
	// consumer does not trim an ended lineage to its end meanwhile (#205).
	checkpointing atomic.Int32
	// cut is the checkpoint's cut, one lock for the whole cluster in
	// PostgreSQL; nil without PostgreSQL, where no checkpoint runs (cut.go).
	cut cutLock
	// export is the PostgreSQL and archive side of the observers' export
	// (export.go).
	export exportSide
	// orphans are the keys reconcile found alive in the table and unknown
	// to SQL, with when it first saw each unchanged (reconcile.go).
	orphansMu sync.Mutex
	orphans   map[string]orphanMark
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
		maxEntry: consensus.MaxEntryBytes,
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
	if setting.Cluster.Role == setting.ClusterRoleObserver {
		// An observer never serves: no listener, no member, no web.
		return errors.New("[cluster] ROLE = observer: this node is an observer and never serves; run `forgejo cluster observe`")
	}
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
	// The socket's budget follows the hook's, read from the same setting.
	clusterhook.SetHookTimeout(setting.Cluster.HookTimeout)
	r.ctx = graceful.GetManager().ShutdownContext()

	// A new key a stop left half in place is completed, or undone, before
	// anything reads the pair (#209).
	if err := certrenew.ResumeRekey(setting.Cluster.TLSCertFile, setting.Cluster.TLSKeyFile); err != nil {
		return fmt.Errorf("[cluster] completing an interrupted key change: %w", err)
	}
	id, err := newIdentity(setting.Cluster.TLSCertFile, setting.Cluster.TLSKeyFile, setting.Cluster.TLSCAFile)
	if err != nil {
		return fmt.Errorf("[cluster] TLS identity: %w", err)
	}
	// One renewer renews and makes the new key of a new node id: the two
	// never interleave.
	r.renewer = NewRenewer()
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
		engine: r.Engine, self: r.selfID, repoKnown: r.repoKnown, revoked: r.table.KeyRevoked,
		leavesKey: r.projector.leavesKey, awaitsRename: r.projector.awaitsRename,
	}
	if setting.Cluster.ExpectedNodes >= 3 {
		r.auto = newAutoMode(r.objects)
		r.auto.revoked = r.keyRevoked
		r.objects.hello = r.helloOf
		r.writerHook = registeredWriterHook()
	} else {
		r.objects.hello, r.objects.helloMembersOnly = r.memberHello, true
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
	wal := pgWAL{role: setting.Cluster.CheckpointRole}
	if setting.Database.Type.IsPostgreSQL() {
		// On every member, whatever its own CHECKPOINT_INTERVAL: the leader
		// that takes the checkpoints may be another one.
		r.cut = pgCut{db: cutDB}
	}
	r.export = newExportSide(wal)
	if setting.Cluster.CheckpointInterval > 0 {
		if !setting.Database.Type.IsPostgreSQL() {
			return errors.New("[cluster] CHECKPOINT_INTERVAL needs PostgreSQL: a checkpoint pairs the refs with a WAL position")
		}
		if _, err := storageRoots(); err != nil {
			return fmt.Errorf("[cluster] CHECKPOINT_INTERVAL: %w", err)
		}
		r.checkpoints.Store(newCoordinator(r, setting.Cluster.CheckpointInterval, setting.Cluster.CheckpointDir, wal))
	}

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
		if err := r.form(r.ctx, driver, cfg, autoCluster, id.SPKI); err != nil {
			_ = srv.Close()
			return err
		}
		cfg.MinVoters = autoMinVoters
		cfg.Discover = r.auto.discoverForJoin
		r.auto.rejoin = func(ctx context.Context) error { return r.rejoinUnderNewID(ctx, driver, cfg, autoCluster) }
	}
	// The transport the engine opens reads the pair now, and keeps its key
	// for as long as it runs: recorded once it opened, before anything
	// reads the engine.
	opened := id.SPKI()
	eng, err := driver.Open(r.ctx, cfg)
	if err != nil {
		_ = srv.Close()
		return fmt.Errorf("[cluster] opening the consensus node: %w", err)
	}
	r.setOpenedKey(opened)
	r.setEngine(eng)
	// The refusal of a voter that would leave no checkpoint to close reads
	// the backup cursor from SQL: it waits for the database
	// (RefuseWithoutCheckpoints, routers/init.go).

	currentMu.Lock()
	current = r
	currentMu.Unlock()
	clusterhook.SetEnabled(true)
	clusterhook.SetRepoLifecycle(r.proposeRepoDelete, r.proposeRepoMoves)
	clusterhook.SetOwnerLifecycle(r.proposeOwnerRename)
	clusterhook.SetOwnerRenameCheck(r.ownerRenameRefusal)
	clusterhook.SetRepoRenameCheck(r.renameSourceRefusal)
	clusterhook.SetCutHold(r.holdCut)
	storage.SetDeleteGate(r.gateDelete)
	clusterhook.SetKeyCheck(r.writableKey)
	clusterhook.SetGarbageCollectionGate(r.projector.MayCollectGarbage)
	clusterhook.SetWriteWaiter(r.waitWrites)
	clusterhook.SetFreshWaiter(r.WaitFresh)
	clusterhook.SetDerivedWaiter(r.WaitDerived)
	clusterhook.SetReadGuard(r.ReadRefusal)
	clusterhook.SetRefLookup(r.refLive)

	go r.projector.run(r.ctx)
	go r.startupBarrier(r.ctx)
	go r.exitOnStop(r.ctx)
	go r.consumer.run(r.ctx)
	go r.sweepRaw(r.ctx)
	if c := r.checkpoints.Load(); c != nil {
		go c.run(r.ctx)
	}
	if r.auto != nil {
		go r.autopilot(r.ctx)
	} else {
		// No autopilot raises the cluster version (#170) nor hands the
		// leadership to a member that takes the checkpoints (#176) here.
		go r.leaderPilot(r.ctx, payloadProbeEvery)
	}
	watchIdentity(r.ctx, id, identityPoll, func() {
		r.objects.rotate()
		log.Info("cluster: node certificate changed on disk; object channel rotated, %d idle connections closed", rot.rotate())
	})
	if r.renewer != nil {
		go runRenewer(r.ctx, r.renewer)
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
// retired (retire.go): a new key first, then the old engine closed, its
// data directory moved aside, a new id stamped, and a new engine opened,
// joining. Without a new key nothing is touched: the retired id's key
// never joins again (#209).
func (r *Replicator) rejoinUnderNewID(ctx context.Context, driver consensus.Driver, cfg consensus.Config, cluster consensus.ClusterID) error {
	if err := r.keyForRejoin(ctx); err != nil {
		return err
	}
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
	if err := refuseStoppedDir(driver, cfg.DataDir); err != nil {
		return err
	}
	moved, err := retireDataDir(cfg.DataDir, time.Now())
	if err != nil {
		return err
	}
	log.Warn("cluster: the retired data directory is kept at %s", moved)
	id, inc, err := driver.Init(cfg.DataDir, cluster, cfg.Slot)
	if err != nil {
		return fmt.Errorf("stamping a new id: %w", err)
	}
	r.auto.noteJoining(id, inc)
	r.resetForRejoin()
	opened := r.presentedKey()
	eng, err := driver.Open(r.ctx, cfg)
	if err != nil {
		// The key recorded stays the one of the engine that last opened.
		return fmt.Errorf("opening under the new id %s: %w", id, err)
	}
	r.setOpenedKey(opened)
	r.setEngine(eng)
	go r.startupBarrier(r.ctx)
	log.Warn("cluster: rejoining under the new id %s", id)
	return nil
}

// resetForRejoin forgets what this node derived from the log before the
// engine of its new id opens: that engine applies the log from its start,
// and the table that already holds it would apply a deletion or a rename
// twice (#41). The projector stays fenced until the new engine applied a
// barrier; startupBarrier lifts it again.
func (r *Replicator) resetForRejoin() {
	r.projector.refence()
	r.table.Reset()
	r.consumer.forget()
}

// stopPoll paces exitOnStop.
var stopPoll = time.Second

// exitProcess ends the process; tests replace it.
var exitProcess = func(code int) {
	log.GetManager().Close()
	os.Exit(code)
}

// exitOnStop ends the process once the engine stopped itself, with the
// stop's exit code (ADR 0152 D4, R9-05): a supervisor that does not
// restart on it keeps the node down in the same boot, where a restart
// could read back a page cache a failed fsync left behind and acknowledge
// what the disk does not hold (#116). An engine closed, by a shutdown or
// a rejoin, has no exit code.
func (r *Replicator) exitOnStop(ctx context.Context) {
	t := time.NewTicker(stopPoll)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
		eng := r.Engine()
		if eng == nil {
			continue
		}
		h := eng.Health()
		if !h.Stopped || h.StopExitCode == 0 {
			continue
		}
		log.Critical("cluster: the consensus node stopped (%s); Forgejo exits %d, and an operator clears the stop: after a STORAGE_FAILED, reboot the machine, never only the service", h.StopReason, h.StopExitCode)
		cctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		if err := eng.Close(cctx); err != nil {
			log.Error("cluster: closing the stopped consensus node: %v", err)
		}
		cancel()
		exitProcess(h.StopExitCode)
		return
	}
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
	return h.Ready && !h.Stopped && r.projector.Ready() && !r.hookAcceptFailing.Load()
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
	// A key the cluster cannot write yet (a name that starts with a dot,
	// before reftable.GenerationVersion) is refused here, before git
	// writes anything.
	if err := reftable.ValidRepoKeyAt(key, r.table.WriteVersion()); err != nil {
		return "", err
	}
	return key, nil
}

// writableKey reports whether the cluster takes a repository under key at
// the payload version it writes now.
func (r *Replicator) writableKey(key string) error {
	if err := reftable.ValidRepoKeyAt(key, r.table.WriteVersion()); err != nil {
		if reftable.ValidRepoKey(key) == nil {
			return fmt.Errorf("%w; the cluster takes such a name once its payload version is raised to %d", err, reftable.GenerationVersion)
		}
		return err
	}
	return nil
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
		// Another lineage: nothing to wait for, and nothing to trust. A
		// Recover cut the history the write was in, which may have lost
		// it: the page is never presented as current.
		return false
	}
	ctx, cancel := context.WithTimeout(ctx, setting.Cluster.ReadAfterWriteTimeout)
	defer cancel()
	if err := r.projector.Wait(ctx, repo, tok.Index); err != nil {
		return false
	}
	return r.consumer.waitCursor(ctx, tok) == nil
}

// WaitFresh waits until this node serves repo at least at the commit index
// a quorum confirms now, within READ_AFTER_WRITE_TIMEOUT: every write
// acknowledged before the call, on whichever node, is then in git here. A
// git fetch carries no read-after-write cookie: without this, a fetch that
// follows a push, from the pusher or from a CI job its webhook started,
// may read the refs from before the push on a node the load balancer
// picked. It reports false when the node may be behind.
func (r *Replicator) WaitFresh(ctx context.Context, repo string) bool {
	_, ok := r.freshAt(ctx, repo)
	return ok
}

// WaitDerived is WaitFresh, then waits, within the same
// READ_AFTER_WRITE_TIMEOUT, until the consumer's cursor passed the newest
// event of repo up to the commit index WaitFresh reached: the SQL the
// consumer derives from every write acknowledged before the call (branch
// rows, the pull requests a push updates) is then committed, its work and
// its cursor moving in one transaction. A branch row read for its commit,
// or renamed with the head of its pull requests, right after a push would
// otherwise be the row from before the push, and the push's work, derived
// later under the branch's old name, would find neither (#225, #226). The
// cursor is in SQL: any node reads it. It reports false when the node may
// be behind, or the consumer has not derived the events yet.
func (r *Replicator) WaitDerived(ctx context.Context, repo string) bool {
	ctx, cancel := context.WithTimeout(ctx, setting.Cluster.ReadAfterWriteTimeout)
	defer cancel()
	at, ok := r.freshAt(ctx, repo)
	if !ok {
		return false
	}
	ev, ok := r.table.LastEventOf(repo, at.Identity(), at.Index)
	if !ok {
		return true
	}
	return r.consumer.waitCursor(ctx, ev) == nil
}

// freshAt waits until this node serves repo at least at the commit index a
// quorum confirms now, within READ_AFTER_WRITE_TIMEOUT, and returns the
// barrier's token at the index it waited for.
func (r *Replicator) freshAt(ctx context.Context, repo string) (consensus.Token, bool) {
	eng := r.Engine()
	if eng == nil || reftable.ValidRepoKey(repo) != nil {
		return consensus.Token{}, false
	}
	// Without a leader no barrier completes within the bound: every git
	// request would wait READ_AFTER_WRITE_TIMEOUT to be served as the node
	// has it anyway. It is served at once, reported possibly behind. A
	// leader the node names counts even before it answered the node's
	// probe: the engine reports it known only then (kitsunium/sdk#541),
	// and right after an election the barrier is the read that proves it.
	if !eng.Health().LeaderKnown && eng.Status().Leader == 0 {
		return consensus.Token{}, false
	}
	ctx, cancel := context.WithTimeout(ctx, setting.Cluster.ReadAfterWriteTimeout)
	defer cancel()
	tok, err := eng.Barrier(ctx)
	if err != nil {
		return consensus.Token{}, false
	}
	// The barrier's index counts the entries that are not commands (a new
	// term's no-op, a membership change), which never reach the table: on
	// a quiet forge after an election the projection would never reach
	// it (#69). The barrier returns once the node applied its index, and
	// a command is in the table before the engine counts it applied: every
	// command up to the barrier is at or below the table's position read
	// now, and what lies above it touches no repository.
	if pos, _ := r.table.Applied(); pos.Index < tok.Index {
		tok.Index = pos.Index
	}
	if r.projector.Wait(ctx, repo, tok.Index) != nil {
		return consensus.Token{}, false
	}
	return tok, true
}

// ReadRefusal refuses a read of repo while its directory here holds
// another repository than the one the table holds under the key (#82): a
// repository renamed away, or the previous one under a name a rename took,
// not projected here yet. Served, its history would be read under the
// database's permissions of the repository that now has the name. repoID
// is the database's ID of that repository, 0 when the caller has none: a
// directory stamped with another ID holds another repository, also on a
// node whose table never learnt of the rename or the deletion.
func (r *Replicator) ReadRefusal(repo string, repoID int64) error {
	if reftable.ValidRepoKey(repo) != nil {
		return nil
	}
	if r.projector.servesAnother(repo) {
		return fmt.Errorf("this node of the cluster has not projected the last rename of %s yet; retry shortly or through another node", repo)
	}
	if r.projector.holdsAnotherID(repo, repoID) {
		return fmt.Errorf("this node of the cluster still holds another repository under %s than repository %d; retry shortly or through another node", repo, repoID)
	}
	return nil
}

// refLive reports whether the table this node applied holds ref of repo
// live: a ref another node wrote, which this node's git may not show yet.
func (r *Replicator) refLive(repo, ref string) bool {
	v, ok := r.table.Get(repo, ref)
	return ok && v.Live()
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
// A key whose refs are all tombstones (renamed away) is not known by the
// table: a shipment there would create a stray repository (#100).
func (r *Replicator) repoKnown(ctx context.Context, key string) (bool, error) {
	if refs, _ := r.table.Refs(key); len(refs) > 0 {
		if _, deleted := r.table.Deleted(key); !deleted && anyLive(refs) {
			return true, nil
		}
	}
	return repoKnownInDB(ctx, key)
}

func anyLive(refs map[string]reftable.RefValue) bool {
	for _, v := range refs {
		if v.Live() {
			return true
		}
	}
	return false
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
