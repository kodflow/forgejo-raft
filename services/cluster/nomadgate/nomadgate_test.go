// Copyright 2026 The Forgejo Authors. All rights reserved.
// SPDX-License-Identifier: GPL-3.0-or-later

package nomadgate

import (
	"context"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"forgejo.org/modules/json"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// FakeNomad answers GET /v1/nodes and /v1/node/<id> from its nodes, and
// refuses any other method or path: the gate is read-only.
type FakeNomad struct {
	mu     sync.Mutex
	Nodes  map[string]map[string]any
	Tokens []string
	Writes int
}

func (f *FakeNomad) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.Tokens = append(f.Tokens, r.Header.Get("X-Nomad-Token"))
	if r.Method != http.MethodGet {
		f.Writes++
		http.Error(w, "read-only", http.StatusForbidden)
		return
	}
	switch {
	case r.URL.Path == "/v1/nodes":
		var stubs []map[string]string
		for id := range f.Nodes {
			stubs = append(stubs, map[string]string{"ID": id})
		}
		_ = json.NewEncoder(w).Encode(stubs)
	case strings.HasPrefix(r.URL.Path, "/v1/node/"):
		n, ok := f.Nodes[strings.TrimPrefix(r.URL.Path, "/v1/node/")]
		if !ok {
			http.NotFound(w, r)
			return
		}
		_ = json.NewEncoder(w).Encode(n)
	default:
		http.NotFound(w, r)
	}
}

func node(id, slot, addr string) map[string]any {
	return map[string]any{
		"ID": id, "Name": "vps-" + slot, "Status": "ready", "SchedulingEligibility": "eligible",
		"Drain": false, "DrainStrategy": nil, "HTTPAddr": addr, "Meta": map[string]string{"forgejo_slot": slot},
	}
}

func (f *FakeNomad) set(id, key string, v any) {
	f.mu.Lock()
	f.Nodes[id][key] = v
	f.mu.Unlock()
}

func TestCheckJoin(t *testing.T) {
	f := &FakeNomad{Nodes: map[string]map[string]any{
		"11111111-aaaa": node("11111111-aaaa", "node-a", "192.0.2.1:4646"),
		"22222222-bbbb": node("22222222-bbbb", "node-c", "192.0.2.3:4646"),
	}}
	srv := httptest.NewServer(f)
	defer srv.Close()
	tok := filepath.Join(t.TempDir(), "token")
	require.NoError(t, os.WriteFile(tok, []byte("node-read-token\n"), 0o600))
	g := &Gate{Addr: srv.URL, TokenFile: tok, SlotMeta: "forgejo_slot"}
	ctx := context.Background()

	require.NoError(t, g.CheckJoin(ctx, "node-c", "192.0.2.3:7480"), "eligible and ready")
	assert.Equal(t, "node-read-token", f.Tokens[0])

	f.set("22222222-bbbb", "DrainStrategy", map[string]any{"Deadline": 3600000000000})
	err := g.CheckJoin(ctx, "node-c", "192.0.2.3:7480")
	require.ErrorIs(t, err, ErrRefused)
	assert.Contains(t, err.Error(), "draining")
	f.set("22222222-bbbb", "DrainStrategy", nil)

	f.set("22222222-bbbb", "SchedulingEligibility", "ineligible")
	err = g.CheckJoin(ctx, "node-c", "192.0.2.3:7480")
	require.ErrorIs(t, err, ErrRefused)
	assert.Contains(t, err.Error(), "ineligible")
	f.set("22222222-bbbb", "SchedulingEligibility", "eligible")

	f.set("22222222-bbbb", "Status", "down")
	require.ErrorIs(t, g.CheckJoin(ctx, "node-c", "192.0.2.3:7480"), ErrRefused)
	f.set("22222222-bbbb", "Status", "ready")

	err = g.CheckJoin(ctx, "node-b", "192.0.2.2:7480")
	require.ErrorIs(t, err, ErrRefused, "no node carries the slot")

	// The old machine of a replacement still carries the slot, drained:
	// the joiner's address picks its own node.
	f.mu.Lock()
	f.Nodes["33333333-cccc"] = node("33333333-cccc", "node-c", "192.0.2.9:4646")
	f.Nodes["33333333-cccc"]["SchedulingEligibility"] = "ineligible"
	f.mu.Unlock()
	require.NoError(t, g.CheckJoin(ctx, "node-c", "192.0.2.3:7480"))
	require.ErrorIs(t, g.CheckJoin(ctx, "node-c", "192.0.2.9:7480"), ErrRefused)
	require.ErrorIs(t, g.CheckJoin(ctx, "node-c", "192.0.2.7:7480"), ErrRefused, "ambiguous")

	assert.Zero(t, f.Writes, "the gate only reads")

	// Nomad not answering refuses.
	dead := &Gate{Addr: "http://127.0.0.1:1", SlotMeta: "forgejo_slot"}
	err = dead.CheckJoin(ctx, "node-c", "192.0.2.3:7480")
	require.ErrorIs(t, err, ErrRefused)
	assert.Contains(t, err.Error(), "did not answer")
}

// The task API socket: unix://<secrets>/api.sock.
func TestCheckJoinOverTheTaskAPISocket(t *testing.T) {
	// t.TempDir() is too long for a Unix socket path on macOS.
	dir, err := os.MkdirTemp("", "ng") //nolint:usetesting
	require.NoError(t, err)
	defer os.RemoveAll(dir)
	sock := filepath.Join(dir, "api.sock")
	ln, err := net.Listen("unix", sock)
	require.NoError(t, err)
	f := &FakeNomad{Nodes: map[string]map[string]any{"44444444": node("44444444", "node-a", "192.0.2.1:4646")}}
	srv := &http.Server{Handler: f}
	go func() { _ = srv.Serve(ln) }()
	defer srv.Close()
	g := &Gate{Addr: "unix://" + sock, SlotMeta: "forgejo_slot"}
	require.NoError(t, g.CheckJoin(context.Background(), "node-a", "192.0.2.1:7480"))
	f.set("44444444", "Drain", true)
	require.ErrorIs(t, g.CheckJoin(context.Background(), "node-a", "192.0.2.1:7480"), ErrRefused)
}
