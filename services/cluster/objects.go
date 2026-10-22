// Copyright 2026 The Forgejo Authors. All rights reserved.
// SPDX-License-Identifier: GPL-3.0-or-later

package cluster

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"forgejo.org/modules/git"
	"forgejo.org/modules/json"
	"forgejo.org/modules/log"
	"forgejo.org/modules/setting"
	"forgejo.org/services/cluster/consensus"
	"forgejo.org/services/cluster/reftable"
)

// maxPackBytes bounds one pack on the object channel. It counts against the
// node's disk reservation like the log does (ADR 0152 D19, R3-13).
const maxPackBytes = 2 << 30

// objectChannel moves git objects between members: shipped to a majority
// before a ref is proposed, fetched by the projector when missing (ADR 0152
// D19, R1-05, R6-08). It runs on the node's management service address over
// mutual TLS, admitted like the log: a peer is a member of the current
// membership whose slot and pinned key its certificate carries. It never
// uses git over a public endpoint, and no credential of it reaches git.
type objectChannel struct {
	root      string
	port      string
	cluster   string
	id        *identity
	engine    func() consensus.Engine
	self      func() consensus.NodeID
	repoKnown func(ctx context.Context, repo string) (bool, error)
	// leavesKey reports a directory under the key the projector is about
	// to set aside or remove: objects indexed there would leave with it.
	leavesKey func(repo string) bool
	// awaitsRename reports a key whose renamed repository's directory the
	// projector has not moved in yet.
	awaitsRename func(repo string) bool
	// revoked reports a key an operator revoked (hex SPKI): a member
	// holding it is not admitted, even before its removal commits.
	revoked func(spki string) bool
	// resolve maps a member to its object channel address; nil uses the
	// host of its consensus address and the configured port.
	resolve func(m consensus.Member) string
	// pinOf maps a member to the key its object channel certificate must
	// carry; nil uses the member's pinned SPKI. Tests whose consensus
	// transport authenticates with other keys set it.
	pinOf func(m consensus.Member) [32]byte
	// hello answers GET /v1/hello to any node of the cluster; nil outside
	// the autonomous mode, where the path is a member's like the others.
	hello func() hello

	mu      sync.Mutex
	clients map[string]*http.Client
}

// ---- admission ----

func (oc *objectChannel) admit(p peer) (consensus.Member, error) {
	if oc.cluster != "" && p.Cluster != oc.cluster {
		return consensus.Member{}, fmt.Errorf("CLUSTER_MISMATCH: peer of cluster %s", p.Cluster)
	}
	eng := oc.engine()
	if eng == nil {
		return consensus.Member{}, errors.New("no membership yet")
	}
	for _, m := range eng.Membership().Members {
		if m.Slot != p.Slot || m.SPKI == ([32]byte{}) {
			continue
		}
		if oc.pin(m) != p.SPKI {
			continue
		}
		if oc.revoked != nil && (oc.revoked(hex.EncodeToString(p.SPKI[:])) || oc.revoked(hex.EncodeToString(m.SPKI[:]))) {
			return consensus.Member{}, fmt.Errorf("PEER_REJECTED: the key of slot %s was revoked", p.Slot)
		}
		return m, nil
	}
	return consensus.Member{}, fmt.Errorf("PEER_REJECTED: slot %s with this key is not a member", p.Slot)
}

func (oc *objectChannel) pin(m consensus.Member) [32]byte {
	if oc.pinOf != nil {
		return oc.pinOf(m)
	}
	return m.SPKI
}

// ---- server ----

func (oc *objectChannel) handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /v1/push", oc.handlePush)
	mux.HandleFunc("POST /v1/fetch", oc.handleFetch)
	mux.HandleFunc("POST /v1/have", oc.handleHave)
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.TLS == nil || len(r.TLS.PeerCertificates) == 0 {
			http.Error(w, "client certificate required", http.StatusUnauthorized)
			return
		}
		leaf := r.TLS.PeerCertificates[0]
		// A kept-alive connection outlives its handshake: each request
		// checks that the certificate it was opened with is still valid,
		// as the consensus transport bounds its connections by NotAfter.
		if time.Now().After(leaf.NotAfter) {
			w.Header().Set("Connection", "close")
			http.Error(w, "the client certificate expired; reconnect", http.StatusUnauthorized)
			return
		}
		p, err := peerFromChain(leaf)
		if err == nil && r.Method == http.MethodGet && r.URL.Path == helloPath && oc.hello != nil {
			// A node of the cluster, member or not yet, asks who this one
			// is: the autonomous mode's discovery. The chain was verified
			// against the cluster's CA; the cluster is checked here.
			if oc.cluster != "" && p.Cluster != oc.cluster {
				http.Error(w, "CLUSTER_MISMATCH", http.StatusForbidden)
				return
			}
			h := oc.hello()
			if id := r.URL.Query().Get("node"); id != "" {
				h.answerFor(id)
			}
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(h)
			return
		}
		if err == nil {
			_, err = oc.admit(p)
		}
		if err != nil {
			log.Warn("cluster objects: refused %s: %v", r.RemoteAddr, err)
			http.Error(w, err.Error(), http.StatusForbidden)
			return
		}
		mux.ServeHTTP(w, r)
	})
}

// repoDir checks the key lexically and refuses a repository unknown to
// Forgejo's database BEFORE touching the disk.
func (oc *objectChannel) repoDir(ctx context.Context, repo string) (string, int, error) {
	if err := reftable.ValidRepoKey(repo); err != nil {
		return "", http.StatusBadRequest, err
	}
	if oc.repoKnown != nil {
		ok, err := oc.repoKnown(ctx, repo)
		if err != nil {
			return "", http.StatusServiceUnavailable, err
		}
		if !ok {
			return "", http.StatusNotFound, fmt.Errorf("repository %s unknown", repo)
		}
	}
	return filepath.Join(oc.root, filepath.FromSlash(repo)), 0, nil
}

func (oc *objectChannel) handlePush(w http.ResponseWriter, r *http.Request) {
	repo := r.URL.Query().Get("repo")
	format := r.URL.Query().Get("format")
	if format != "" && format != "sha1" && format != "sha256" {
		http.Error(w, "format is sha1 or sha256", http.StatusBadRequest)
		return
	}
	dir, code, err := oc.repoDir(r.Context(), repo)
	if err != nil {
		http.Error(w, err.Error(), code)
		return
	}
	if _, err := os.Stat(dir); errors.Is(err, os.ErrNotExist) {
		if oc.awaitsRename != nil && oc.awaitsRename(repo) {
			// The renamed repository's directory is still under its old
			// key here: created empty now, the key would never take it,
			// and it would stay under the freed name (#100).
			http.Error(w, fmt.Sprintf("%s waits for its rename to be projected on this node", repo), http.StatusServiceUnavailable)
			return
		}
		if err := initBare(r.Context(), dir, format); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
	} else if err == nil && oc.leavesKey != nil && oc.leavesKey(repo) {
		// Acknowledged here, the objects would leave the key with that
		// directory, and no member would serve them once the sender is
		// lost (#99): the sender counts another member.
		http.Error(w, fmt.Sprintf("the directory of %s on this node is not the repository the cluster holds under that key yet", repo), http.StatusServiceUnavailable)
		return
	} else if err == nil && !validRepoDir(dir) {
		// An interrupted init: the projector sets it aside and creates the
		// repository anew; meanwhile this member does not acknowledge.
		http.Error(w, fmt.Sprintf("%s holds no git repository on this node yet", repo), http.StatusServiceUnavailable)
		return
	}
	// A sender that stalls mid-pack must not hold index-pack forever.
	ctx, cancel := context.WithTimeout(r.Context(), objectBudget)
	defer cancel()
	body := &idleBody{r: http.MaxBytesReader(w, r.Body, maxPackBytes), rc: http.NewResponseController(w)}
	if err := receivePack(ctx, dir, body); err != nil {
		// A pack whose bases this node lacks fails --strict; the sender
		// retries with a complete pack.
		http.Error(w, err.Error(), http.StatusConflict)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

type fetchRequest struct {
	Repo  string   `json:"repo"`
	Wants []string `json:"wants"`
	Haves []string `json:"haves"`
}

func (oc *objectChannel) handleFetch(w http.ResponseWriter, r *http.Request) {
	var req fetchRequest
	if err := json.NewDecoder(io.LimitReader(r.Body, 16<<20)).Decode(&req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	dir, code, err := oc.repoDir(r.Context(), req.Repo)
	if err != nil {
		http.Error(w, err.Error(), code)
		return
	}
	for _, s := range append(append([]string(nil), req.Wants...), req.Haves...) {
		if !isHex(s) {
			http.Error(w, "invalid object id", http.StatusBadRequest)
			return
		}
	}
	missing, err := missingObjects(r.Context(), dir, req.Wants)
	if err != nil || len(missing) > 0 {
		http.Error(w, "objects not held here", http.StatusNotFound)
		return
	}
	haves, _ := presentObjects(r.Context(), dir, req.Haves)
	w.Header().Set("Content-Type", "application/x-git-packfile")
	// The answer starts now: the fetcher times the pack from here.
	w.WriteHeader(http.StatusOK)
	_ = http.NewResponseController(w).Flush()
	// A fetcher that stops reading must not hold pack-objects forever.
	ctx, cancel := context.WithTimeout(r.Context(), objectBudget)
	defer cancel()
	out := &idleWriter{w: w, rc: http.NewResponseController(w)}
	if err := packObjectsFn(ctx, dir, req.Wants, haves, out); err != nil {
		log.Warn("cluster objects: pack for %s: %v", req.Repo, err)
	}
}

// haveAnswer lists the wants a member lacks.
type haveAnswer struct {
	Missing []string `json:"missing"`
}

// handleHave tells a sender which of its wants this member already holds:
// a retried push whose objects an earlier attempt's shipment delivered in
// the background ships nothing again. A member holds an object only once
// it is synced, as for an acknowledged shipment.
func (oc *objectChannel) handleHave(w http.ResponseWriter, r *http.Request) {
	var req fetchRequest
	if err := json.NewDecoder(io.LimitReader(r.Body, 16<<20)).Decode(&req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	dir, code, err := oc.repoDir(r.Context(), req.Repo)
	if err != nil {
		http.Error(w, err.Error(), code)
		return
	}
	for _, s := range req.Wants {
		if !isHex(s) {
			http.Error(w, "invalid object id", http.StatusBadRequest)
			return
		}
	}
	ans := haveAnswer{Missing: req.Wants}
	if _, err := os.Stat(dir); err == nil {
		if ans.Missing, err = missingObjects(r.Context(), dir, req.Wants); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
	}
	if ans.Missing == nil {
		// "Nothing missing" is an empty list: an absent one is no answer.
		ans.Missing = []string{}
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(ans)
}

// Bounds of the object channel beside the hook's own budget: a transfer
// that makes no progress for objectIdle is cut, and none runs longer than
// objectBudget, so a peer that stalls mid-stream never freezes the
// projector or a handler.
var (
	objectIdle   = 2 * time.Minute
	objectBudget = 30 * time.Minute
)

// idleBody pushes a request body's read deadline forward on each read.
type idleBody struct {
	r  io.Reader
	rc *http.ResponseController
}

func (b *idleBody) Read(p []byte) (int, error) {
	_ = b.rc.SetReadDeadline(time.Now().Add(objectIdle))
	return b.r.Read(p)
}

// idleWriter pushes a response's write deadline forward on each write.
type idleWriter struct {
	w  io.Writer
	rc *http.ResponseController
}

func (iw *idleWriter) Write(p []byte) (int, error) {
	_ = iw.rc.SetWriteDeadline(time.Now().Add(objectIdle))
	return iw.w.Write(p)
}

// watchdog cancels a request that makes no progress for idle: no answer,
// or no byte of its body.
type watchdog struct {
	timer *time.Timer
	idle  time.Duration
}

func newWatchdog(idle time.Duration, cancel context.CancelFunc) *watchdog {
	return &watchdog{timer: time.AfterFunc(idle, cancel), idle: idle}
}

func (w *watchdog) body(r io.Reader) io.Reader {
	return readerFunc(func(p []byte) (int, error) {
		n, err := r.Read(p)
		if n > 0 {
			w.timer.Reset(w.idle)
		}
		return n, err
	})
}

func (w *watchdog) stop() { w.timer.Stop() }

type readerFunc func(p []byte) (int, error)

func (f readerFunc) Read(p []byte) (int, error) { return f(p) }

// ---- git plumbing ----

func isHex(s string) bool {
	if len(s) != 40 && len(s) != 64 {
		return false
	}
	_, err := hex.DecodeString(s)
	return err == nil && strings.ToLower(s) == s
}

func presentObjects(ctx context.Context, dir string, shas []string) ([]string, error) {
	missing, err := missingObjects(ctx, dir, shas)
	if err != nil {
		return nil, err
	}
	gone := map[string]bool{}
	for _, m := range missing {
		gone[m] = true
	}
	var out []string
	for _, s := range shas {
		if !gone[s] {
			out = append(out, s)
		}
	}
	return out, nil
}

// packObjectsFn is what /v1/fetch packs with; tests replace it.
var packObjectsFn = packObjects

// packObjects writes a self-contained pack of wants minus what haves reach.
func packObjects(ctx context.Context, dir string, wants, haves []string, w io.Writer) error {
	var in strings.Builder
	for _, s := range wants {
		in.WriteString(s + "\n")
	}
	for _, s := range haves {
		in.WriteString("^" + s + "\n")
	}
	var stderr bytes.Buffer
	cmd := git.NewCommand(ctx, "-c", "core.hooksPath=/dev/null", "pack-objects", "--revs", "--stdout", "--quiet")
	err := cmd.Run(&git.RunOpts{Dir: dir, Stdin: strings.NewReader(in.String()), Stdout: w, Stderr: &stderr, UseContextTimeout: true})
	if err != nil {
		return fmt.Errorf("pack-objects: %w: %s", err, strings.TrimSpace(stderr.String()))
	}
	return nil
}

// receivePack indexes a pack into a quarantine directory with
// `git index-pack --strict`, then migrates it into the repository, index
// last, and syncs the directory.
func receivePack(ctx context.Context, dir string, body io.Reader) error {
	var rnd [8]byte
	_, _ = rand.Read(rnd[:])
	objects := filepath.Join(dir, "objects")
	quarantine := filepath.Join(objects, "cluster-incoming-"+hex.EncodeToString(rnd[:]))
	if err := os.MkdirAll(filepath.Join(quarantine, "pack"), 0o755); err != nil {
		return err
	}
	defer os.RemoveAll(quarantine)

	var stderr bytes.Buffer
	env := append(os.Environ(),
		"GIT_OBJECT_DIRECTORY="+quarantine,
		"GIT_ALTERNATE_OBJECT_DIRECTORIES="+objects,
	)
	cmd := git.NewCommand(ctx, "-c", "core.hooksPath=/dev/null", "index-pack").AddArguments(git.ToTrustedCmdArgs([]string{indexPackStrict(), "--stdin"})...)
	if err := cmd.Run(&git.RunOpts{Dir: dir, Env: env, Stdin: body, Stdout: io.Discard, Stderr: &stderr, UseContextTimeout: true}); err != nil {
		return fmt.Errorf("index-pack %s: %w: %s", indexPackStrict(), err, strings.TrimSpace(stderr.String()))
	}
	entries, err := os.ReadDir(filepath.Join(quarantine, "pack"))
	if err != nil {
		return err
	}
	// .pack and .rev first, .idx last: git only sees a pack once its index
	// exists.
	sort.Slice(entries, func(i, j int) bool {
		return strings.HasSuffix(entries[j].Name(), ".idx") && !strings.HasSuffix(entries[i].Name(), ".idx")
	})
	dst := filepath.Join(objects, "pack")
	if err := os.MkdirAll(dst, 0o755); err != nil {
		return err
	}
	for _, e := range entries {
		if e.Name() == "" || strings.HasPrefix(e.Name(), "tmp_") {
			continue
		}
		if err := os.Rename(filepath.Join(quarantine, "pack", e.Name()), filepath.Join(dst, e.Name())); err != nil {
			return err
		}
	}
	return syncDir(dst)
}

// indexPackStrict is index-pack's --strict with the fsck severities Forgejo
// sets for a push (modules/setting/git.go: badTimezone,
// missingSpaceBeforeDate and zeroPaddedFilemode ignored, and [git.config]).
// --strict reads none of the fsck.* settings and raises every warning to an
// error: a history Forgejo accepts on push or migration, such as an old
// tree with zero-padded modes, would be refused by every peer, and the
// repository could never be written again in the cluster.
func indexPackStrict() string {
	var ids []string
	for key, v := range setting.GitConfig.Options {
		id, ok := strings.CutPrefix(key, "receive.fsck.")
		if !ok || id == "" || strings.Trim(id, "abcdefghijklmnopqrstuvwxyz") != "" {
			continue
		}
		switch v = strings.ToLower(v); v {
		case "ignore", "warn", "error":
			ids = append(ids, id+"="+v)
		}
	}
	if len(ids) == 0 {
		return "--strict"
	}
	sort.Strings(ids)
	return "--strict=" + strings.Join(ids, ",")
}

func syncDir(dir string) error {
	f, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer f.Close()
	return f.Sync()
}

// ---- client ----

func (oc *objectChannel) client(m consensus.Member) *http.Client {
	oc.mu.Lock()
	defer oc.mu.Unlock()
	if oc.clients == nil {
		oc.clients = map[string]*http.Client{}
	}
	key := fmt.Sprintf("%s/%x", m.Slot, m.SPKI)
	if c := oc.clients[key]; c != nil {
		return c
	}
	cfg := oc.id.clientConfig(func(p peer) error {
		if oc.cluster != "" && p.Cluster != oc.cluster {
			return fmt.Errorf("CLUSTER_MISMATCH: %s", p.Cluster)
		}
		if p.Slot != m.Slot || m.SPKI == ([32]byte{}) || p.SPKI != oc.pin(m) {
			return fmt.Errorf("PEER_REJECTED: expected slot %s with its pinned key", m.Slot)
		}
		return nil
	})
	c := &http.Client{Transport: &http.Transport{
		DialContext:         (&net.Dialer{Timeout: peerDialTimeout}).DialContext,
		TLSHandshakeTimeout: peerHandshakeTimeout,
		TLSClientConfig:     cfg, MaxIdleConnsPerHost: 4, IdleConnTimeout: 90 * time.Second,
		// The answer to a shipment comes once the peer indexed the whole
		// pack, which may take long for a large one: the peer's own bound
		// (#75). Each request is bounded by its context besides.
		ResponseHeaderTimeout: objectBudget,
	}}
	oc.clients[key] = c
	return c
}

func (oc *objectChannel) objectURL(m consensus.Member, path string) (string, error) {
	if oc.resolve != nil {
		return "https://" + oc.resolve(m) + path, nil
	}
	host, _, err := net.SplitHostPort(m.Address)
	if err != nil {
		return "", err
	}
	return "https://" + net.JoinHostPort(host, oc.port) + path, nil
}

// peers lists the voters other than this node, and how many of them must
// hold the objects for a majority of voters to hold them.
func (oc *objectChannel) peers() ([]consensus.Member, int) {
	eng := oc.engine()
	if eng == nil {
		return nil, 1
	}
	ms := eng.Membership()
	self := oc.self()
	var out []consensus.Member
	voters, selfVoter := 0, false
	for _, m := range ms.Members {
		if !m.Voter {
			continue
		}
		voters++
		if m.ID == self {
			selfVoter = true
			continue
		}
		out = append(out, m)
	}
	need := voters/2 + 1
	if selfVoter {
		need--
	}
	return out, need
}

var errNoMajority = errors.New("the objects could not be written on a majority of the members")

// Bounds of one shipment to one peer. A peer that does not answer must not
// hold a push back once a majority holds the objects: the push returns at
// majority, and the stragglers keep receiving in the background within
// stragglerBudget, or the request's own deadline when it is later.
const (
	peerDialTimeout      = 3 * time.Second
	peerHandshakeTimeout = 5 * time.Second
)

var stragglerBudget = 5 * time.Minute

// shipToMajority writes the objects of wants on enough peers that a majority
// of voters, this node included, holds them, before the ref is proposed
// (R1-05). It sends to every peer at once and returns as soon as a majority
// acknowledged; the shipments still running continue in the background,
// detached from the request, and the pack files are removed when the last
// one ends.
func (oc *objectChannel) shipToMajority(ctx context.Context, repo, dir, format string, wants, haves []string) error {
	peers, need := oc.peers()
	if need <= 0 || len(wants) == 0 {
		return nil
	}
	if len(peers) < need {
		return errNoMajority
	}
	haves, _ = presentObjects(ctx, dir, haves)
	var thin, full string
	var thinOnce, fullOnce sync.Once
	var thinErr, fullErr error

	// The shipments the request waits for are bounded by the request's
	// deadline, HOOK_TIMEOUT less ten seconds for a push: a fixed budget
	// cut every push slower than it whatever HOOK_TIMEOUT said (#75).
	// Detached from the request, the stragglers keep at least
	// stragglerBudget once a majority returned.
	deadline := time.Now().Add(stragglerBudget)
	if d, ok := ctx.Deadline(); ok && d.After(deadline) {
		deadline = d
	}
	sendCtx, cancel := context.WithDeadline(context.WithoutCancel(ctx), deadline)
	acks := make(chan error, len(peers))
	var wg sync.WaitGroup
	for _, m := range peers {
		wg.Add(1)
		go func(m consensus.Member) {
			defer wg.Done()
			// A peer that already holds every want acknowledges at once: a
			// retry of a push whose objects an earlier attempt's background
			// shipment delivered ships nothing again, instead of starting
			// over within the same budget.
			if oc.peerHolds(sendCtx, m, repo, wants) {
				acks <- nil
				return
			}
			thinOnce.Do(func() { thin, thinErr = packToFile(sendCtx, dir, wants, haves) })
			err := thinErr
			if err == nil {
				err = oc.pushFile(sendCtx, m, repo, format, thin)
			}
			if errors.Is(err, errNeedsFull) {
				fullOnce.Do(func() { full, fullErr = packToFile(sendCtx, dir, wants, nil) })
				if fullErr != nil {
					err = fullErr
				} else {
					err = oc.pushFile(sendCtx, m, repo, format, full)
				}
			}
			if err != nil {
				log.Warn("cluster objects: shipping %s to %s: %v", repo, m.Slot, err)
			}
			acks <- err
		}(m)
	}
	go func() {
		wg.Wait()
		cancel()
		if thin != "" {
			os.Remove(thin)
		}
		if full != "" {
			os.Remove(full)
		}
	}()

	ok, failed := 0, 0
	var firstErr error
	for ok < need && ok+failed < len(peers) {
		select {
		case err := <-acks:
			if err == nil {
				ok++
			} else {
				failed++
				if firstErr == nil {
					firstErr = err
				}
			}
		case <-ctx.Done():
			return errNoMajority
		}
	}
	if ok < need {
		if firstErr != nil {
			return fmt.Errorf("%w: %v", errNoMajority, firstErr)
		}
		return errNoMajority
	}
	return nil
}

// probeTimeout bounds the question asked of a peer before a shipment.
const probeTimeout = 5 * time.Second

// peerHolds asks a peer whether it holds all of wants. A peer that does not
// answer, answers otherwise, or runs a binary without /v1/have, lacks them:
// it is shipped to as before.
func (oc *objectChannel) peerHolds(ctx context.Context, m consensus.Member, repo string, wants []string) bool {
	ctx, cancel := context.WithTimeout(ctx, probeTimeout)
	defer cancel()
	u, err := oc.objectURL(m, "/v1/have")
	if err != nil {
		return false
	}
	body, _ := json.Marshal(fetchRequest{Repo: repo, Wants: wants})
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, u, bytes.NewReader(body))
	if err != nil {
		return false
	}
	resp, err := oc.client(m).Do(req)
	if err != nil {
		return false
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return false
	}
	var ans haveAnswer
	if err := json.NewDecoder(io.LimitReader(resp.Body, 16<<20)).Decode(&ans); err != nil {
		return false
	}
	return ans.Missing != nil && len(ans.Missing) == 0
}

var errNeedsFull = errors.New("peer lacks the bases of a thin pack")

func packToFile(ctx context.Context, dir string, wants, haves []string) (string, error) {
	f, err := os.CreateTemp("", "forgejo-cluster-pack-*")
	if err != nil {
		return "", err
	}
	defer f.Close()
	if err := packObjects(ctx, dir, wants, haves, f); err != nil {
		os.Remove(f.Name())
		return "", err
	}
	return f.Name(), f.Sync()
}

func (oc *objectChannel) pushFile(ctx context.Context, m consensus.Member, repo, format, path string) error {
	u, err := oc.objectURL(m, "/v1/push?"+url.Values{"repo": {repo}, "format": {format}}.Encode())
	if err != nil {
		return err
	}
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, u, f)
	if err != nil {
		return err
	}
	resp, err := oc.client(m).Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	switch resp.StatusCode {
	case http.StatusNoContent, http.StatusOK:
		return nil
	case http.StatusConflict:
		return errNeedsFull
	default:
		b, _ := io.ReadAll(io.LimitReader(resp.Body, 1024))
		return fmt.Errorf("%s: %s", resp.Status, strings.TrimSpace(string(b)))
	}
}

// fetchMissing brings objects this node lacks from any member holding them.
func (oc *objectChannel) fetchMissing(ctx context.Context, repo, dir string, shas []string) error {
	eng := oc.engine()
	if eng == nil {
		return errors.New("no membership yet")
	}
	refs, err := readGitRefs(ctx, dir)
	if err != nil {
		return err
	}
	var haves []string
	for _, r := range refs {
		if r.SHA != "" {
			haves = append(haves, r.SHA)
		}
	}
	// A ref whose objects are gone is no base to build on: only what is
	// present is offered, or the sender would leave the wants out.
	if haves, err = presentObjects(ctx, dir, haves); err != nil {
		return err
	}
	self := oc.self()
	var lastErr error
	for _, m := range eng.Membership().Members {
		if m.ID == self {
			continue
		}
		if err := oc.fetchFrom(ctx, m, repo, dir, shas, haves); err != nil {
			lastErr = err
			continue
		}
		missing, err := missingObjects(ctx, dir, shas)
		if err == nil && len(missing) == 0 {
			return nil
		}
	}
	if lastErr == nil {
		lastErr = errors.New("no member holds them")
	}
	return lastErr
}

func (oc *objectChannel) fetchFrom(ctx context.Context, m consensus.Member, repo, dir string, wants, haves []string) error {
	u, err := oc.objectURL(m, "/v1/fetch")
	if err != nil {
		return err
	}
	body, _ := json.Marshal(fetchRequest{Repo: repo, Wants: wants, Haves: haves})
	// A member that stalls mid-pack is cut after objectIdle without a byte,
	// and the next member is asked: the projector's single pass must not
	// wait on it.
	ctx, cancel := context.WithTimeout(ctx, objectBudget)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, u, bytes.NewReader(body))
	if err != nil {
		return err
	}
	watch := newWatchdog(objectIdle, cancel)
	defer watch.stop()
	resp, err := oc.client(m).Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("%s answered %s", m.Slot, resp.Status)
	}
	if err := receivePack(ctx, dir, watch.body(io.LimitReader(resp.Body, maxPackBytes))); err != nil {
		if ctx.Err() != nil {
			return fmt.Errorf("%s stalled or overran the object budget: %w", m.Slot, err)
		}
		return err
	}
	return nil
}
