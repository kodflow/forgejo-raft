// Copyright 2026 The Forgejo Authors. All rights reserved.
// SPDX-License-Identifier: GPL-3.0-or-later

package cluster

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"time"

	"forgejo.org/modules/clusterhook"
	"forgejo.org/modules/json"
	"forgejo.org/modules/log"
)

// serveHookSocket listens on the private socket the reference-transaction
// hook, `forgejo serv` and the offline commands use (ADR 0148, ADR 0152
// D19). It is gated by its directory, 0700, and the socket file, 0600, both
// owned by the git user; the kernel names the peer, and a peer of another
// uid is refused. It is never routed under /api/internal.
func (r *Replicator) serveHookSocket(path string) error {
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return err
	}
	if err := os.Chmod(dir, 0o700); err != nil {
		return err
	}
	if err := os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	ln, err := net.Listen("unix", path)
	if err != nil {
		return fmt.Errorf("[cluster] hook socket: %w", err)
	}
	if err := os.Chmod(path, 0o600); err != nil {
		ln.Close()
		return err
	}
	go func() {
		<-r.ctx.Done()
		ln.Close()
	}()
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				if r.ctx.Err() == nil {
					log.Error("cluster: hook socket: %v", err)
				}
				return
			}
			go r.serveHookConn(conn)
		}
	}()
	return nil
}

func (r *Replicator) serveHookConn(conn net.Conn) {
	defer conn.Close()
	if uc, ok := conn.(*net.UnixConn); ok {
		if uid, err := peerUID(uc); err != nil || uid != os.Getuid() {
			log.Warn("cluster: hook socket peer refused (uid %d): %v", uid, err)
			return
		}
	}
	// The request is read within the hook's deadline; the work then gets
	// the budget of its operation, and the answer a little more.
	_ = conn.SetDeadline(time.Now().Add(clusterhook.HookTimeout()))
	var req clusterhook.Request
	if err := json.NewDecoder(bufio.NewReader(io.LimitReader(conn, 32<<20))).Decode(&req); err != nil {
		return
	}
	budget := clusterhook.Budget(req.Op)
	_ = conn.SetDeadline(time.Now().Add(budget + 10*time.Second))
	ctx, cancel := context.WithTimeout(r.ctx, budget)
	defer cancel()
	if clusterhook.IsExportOp(req.Op) {
		if req.Export != nil {
			req.Export.Op = req.Op
		}
		r.serveExport(ctx, conn, req.Export)
		return
	}
	resp := r.Handle(ctx, &req)
	_ = json.NewEncoder(conn).Encode(resp)
}

// Handle answers one request of the socket protocol.
func (r *Replicator) Handle(ctx context.Context, req *clusterhook.Request) *clusterhook.Response {
	switch req.Op {
	case clusterhook.OpRegister:
		repo, err := r.repoKey(req.GitDir)
		if err != nil {
			return fail(err)
		}
		if err := r.registry.register(req.Nonce, repo, nil, req.PID); err != nil {
			return fail(err)
		}
		return &clusterhook.Response{OK: true}
	case clusterhook.OpPrepared:
		return r.prepared(ctx, req)
	case clusterhook.OpFresh:
		if !r.WaitFresh(ctx, req.Repo) {
			return fail(errors.New("this node of the cluster may be behind the cluster's last writes"))
		}
		return &clusterhook.Response{OK: true}
	case clusterhook.OpCommitted, clusterhook.OpAborted:
		r.finished(req)
		return &clusterhook.Response{OK: true}
	case clusterhook.OpStatus:
		return withData(r.View(ctx))
	case clusterhook.OpSnapshot:
		meta, err := r.SaveSnapshot(ctx, operatorOrLocal(req.Operator), req.Path)
		if err != nil {
			return fail(err)
		}
		return withData(meta)
	case clusterhook.OpObjectsVerify, clusterhook.OpObjectsFill:
		rep, err := r.VerifyObjects(ctx, req.Op == clusterhook.OpObjectsFill)
		if err != nil {
			return fail(err)
		}
		return withData(rep)
	case clusterhook.OpReconcile:
		ms, err := r.Reconcile(ctx, !req.DryRun)
		if err != nil {
			return fail(err)
		}
		return withData(ms)
	case clusterhook.OpParkedList:
		ps, err := r.ListParked(ctx)
		if err != nil {
			return fail(err)
		}
		return withData(ps)
	case clusterhook.OpParkedResolve:
		res, err := r.ResolveParked(ctx, operatorOrLocal(req.Operator), req.Lineage, req.Index, req.Reason)
		if err != nil {
			return fail(err)
		}
		return withData(res)
	case clusterhook.OpImport:
		rep, err := r.Import(ctx, req.Repo)
		if err != nil {
			return fail(err)
		}
		return withData(rep)
	case clusterhook.OpReproject:
		ok, err := r.projector.Verify(ctx, req.Repo)
		if err != nil {
			return fail(err)
		}
		return withData(map[string]bool{"matched": ok})
	default:
		return fail(fmt.Errorf("unknown operation %q", req.Op))
	}
}

func fail(err error) *clusterhook.Response {
	return &clusterhook.Response{Message: err.Error()}
}

func withData(v any) *clusterhook.Response {
	b, err := json.Marshal(v)
	if err != nil {
		return fail(err)
	}
	return &clusterhook.Response{OK: true, Data: b}
}
