// Copyright 2026 The Forgejo Authors. All rights reserved.
// SPDX-License-Identifier: GPL-3.0-or-later

package cluster

import (
	"context"
	"crypto/rand"
	"crypto/x509"
	"encoding/pem"
	"io"
	"math/big"
	"net"
	"net/http"
	"net/http/httptrace"
	"os"
	"sync"
	"testing"
	"time"

	"forgejo.org/services/cluster/consensus"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// reissue signs a new certificate for the key already in file, as a
// renewal does: same key, new serial, replaced atomically.
func (p *testPKI) reissue(t *testing.T, file string) *big.Int {
	b, err := os.ReadFile(file)
	require.NoError(t, err)
	var leaf *x509.Certificate
	var keyBlock *pem.Block
	for rest := b; ; {
		var blk *pem.Block
		blk, rest = pem.Decode(rest)
		if blk == nil {
			break
		}
		switch blk.Type {
		case "CERTIFICATE":
			leaf, err = x509.ParseCertificate(blk.Bytes)
			require.NoError(t, err)
		case "EC PRIVATE KEY":
			keyBlock = blk
		}
	}
	key, err := x509.ParseECPrivateKey(keyBlock.Bytes)
	require.NoError(t, err)
	tpl := *leaf
	tpl.SerialNumber = big.NewInt(time.Now().UnixNano())
	der, err := x509.CreateCertificate(rand.Reader, &tpl, p.caCert, &key.PublicKey, p.caKey)
	require.NoError(t, err)
	out := append(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}), pem.EncodeToMemory(keyBlock)...)
	require.NoError(t, os.WriteFile(file+".renew", out, 0o600))
	require.NoError(t, os.Rename(file+".renew", file))
	// A renewal within the file system's mtime granularity still reloads.
	later := time.Now().Add(2 * time.Second)
	require.NoError(t, os.Chtimes(file, later, later))
	return tpl.SerialNumber
}

// A renewed certificate is used by the next handshake on both sides, and
// the object channel's connections opened before it are rotated: idle ones
// at once, a busy one as soon as its request ends.
func TestRenewedCertificateRotatesTheObjectChannel(t *testing.T) {
	pki := newPKI(t)
	clusterHex := "0f079e10a1b2c3d4e5f60718293a4b5c"
	file, spki := pki.issue(t, clusterHex, "node-a")
	id, err := newIdentity(file, file, pki.caFile)
	require.NoError(t, err)

	release, entered := make(chan struct{}), make(chan struct{}, 1)
	var releaseOnce sync.Once
	unblock := func() { releaseOnce.Do(func() { close(release) }) }
	defer unblock()
	mux := http.NewServeMux()
	mux.HandleFunc("GET /serial", func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Query().Get("block") != "" {
			entered <- struct{}{}
			<-release
		}
		_, _ = io.WriteString(w, r.TLS.PeerCertificates[0].SerialNumber.String())
	})
	rot := newConnRotator()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	srv := &http.Server{Handler: mux, TLSConfig: id.serverConfig(), ReadHeaderTimeout: 5 * time.Second, ConnState: rot.ConnState}
	go func() { _ = srv.ServeTLS(ln, "", "") }()
	defer srv.Close()

	oc := &objectChannel{id: id, cluster: clusterHex}
	m := consensus.Member{Slot: "node-a", SPKI: spki}
	type answer struct {
		server, client string // the serial the server presented, the one it saw
		reused         bool
	}
	try := func(c *http.Client, query string) (answer, error) {
		var reused bool
		ctx := httptrace.WithClientTrace(context.Background(), &httptrace.ClientTrace{GotConn: func(i httptrace.GotConnInfo) { reused = i.Reused }})
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, "https://"+ln.Addr().String()+"/serial"+query, nil)
		if err != nil {
			return answer{}, err
		}
		resp, err := c.Do(req)
		if err != nil {
			return answer{}, err
		}
		defer resp.Body.Close()
		b, _ := io.ReadAll(resp.Body)
		return answer{server: resp.TLS.PeerCertificates[0].SerialNumber.String(), client: string(b), reused: reused}, nil
	}
	get := func(c *http.Client, query string) answer {
		a, err := try(c, query)
		require.NoError(t, err)
		return a
	}

	first, _ := id.load()
	leaf, _ := x509.ParseCertificate(first.Certificate[0])
	s1 := leaf.SerialNumber.String()
	c1 := oc.client(m)
	a := get(c1, "")
	assert.Equal(t, answer{s1, s1, false}, a)
	assert.True(t, get(c1, "").reused, "kept alive before the renewal")

	// A second client holds a request in flight across the renewal.
	busy := &http.Client{Transport: &http.Transport{TLSClientConfig: c1.Transport.(*http.Transport).TLSClientConfig.Clone()}}
	// It reports from its goroutine, not through t, which a failure may
	// have ended meanwhile.
	type result struct {
		answer
		err error
	}
	inflight := make(chan result, 1)
	go func() {
		a, err := try(busy, "?block=1")
		inflight <- result{a, err}
	}()
	// The handler runs once the handshake is done, with the certificate of
	// before the renewal: a connection is counted open at its accept,
	// before its handshake, which a renewal could then precede (#153).
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("the busy request did not reach the server")
	}
	require.Equal(t, 2, rot.open())

	rotated := make(chan struct{}, 4)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	watchIdentity(ctx, id, 20*time.Millisecond, func() {
		oc.rotate()
		rot.rotate()
		rotated <- struct{}{}
	})
	s2 := pki.reissue(t, file).String()
	select {
	case <-rotated:
	case <-time.After(5 * time.Second):
		t.Fatal("the change on disk was not noticed")
	}
	// The idle connection is closed at once; the busy one stays open.
	require.Eventually(t, func() bool { return rot.open() == 1 }, 5*time.Second, 10*time.Millisecond)

	// New handshakes, on both sides, carry the renewed certificate.
	c2 := oc.client(m)
	assert.NotSame(t, c1, c2, "the client was dropped")
	assert.Equal(t, answer{s2, s2, false}, get(c2, ""))

	// The request in flight ends on its old connection, which then closes.
	unblock()
	old := <-inflight
	require.NoError(t, old.err)
	assert.Equal(t, s1, old.server)
	assert.Equal(t, s1, old.client)
	require.Eventually(t, func() bool { return rot.open() == 1 }, 5*time.Second, 10*time.Millisecond, "only c2's connection remains")
	assert.True(t, get(c2, "").reused)
}

// A new CA alone is picked up; a renewal whose CA cannot be read keeps the
// pair and pool in use instead of failing every handshake.
func TestIdentityReloadsTheCAAndKeepsThePairOnFailure(t *testing.T) {
	pki := newPKI(t)
	file, _ := pki.issue(t, "0f079e10a1b2c3d4e5f60718293a4b5c", "node-a")
	id, err := newIdentity(file, file, pki.caFile)
	require.NoError(t, err)
	_, err = id.load()
	require.NoError(t, err)

	// Another CA appended: the pool changes without touching the pair.
	other := newPKI(t)
	ca, _ := os.ReadFile(pki.caFile)
	more, _ := os.ReadFile(other.caFile)
	require.NoError(t, os.WriteFile(pki.caFile, append(ca, more...), 0o600))
	later := time.Now().Add(2 * time.Second)
	require.NoError(t, os.Chtimes(pki.caFile, later, later))
	inUse, err := id.load()
	require.NoError(t, err)
	assert.True(t, id.roots().Equal(func() *x509.CertPool {
		p := x509.NewCertPool()
		p.AppendCertsFromPEM(append(ca, more...))
		return p
	}()), "the new CA is trusted")

	// The CA becomes unreadable while a renewal lands: the old pair stays.
	require.NoError(t, os.WriteFile(pki.caFile, []byte("not a certificate"), 0o600))
	pki.reissue(t, file)
	cur, err := id.load()
	require.NoError(t, err)
	assert.Same(t, inUse, cur, "the pair in use is kept")
	assert.True(t, id.roots().Equal(func() *x509.CertPool {
		p := x509.NewCertPool()
		p.AppendCertsFromPEM(append(ca, more...))
		return p
	}()), "and the pool in use")
}
