// Copyright 2026 The Forgejo Authors. All rights reserved.
// SPDX-License-Identifier: GPL-3.0-or-later

package observer

import (
	"bufio"
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/pem"
	"io"
	"net"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"forgejo.org/modules/clusterhook"
	"forgejo.org/modules/json"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/crypto/ssh"
	"golang.org/x/crypto/ssh/knownhosts"
)

// sshMember is a member's sshd as the observer meets it: it records the
// requests a session makes, and answers the export with a head and a body.
type sshMember struct {
	addr     string
	hostKey  ssh.PublicKey
	mu       sync.Mutex
	requests []string
	got      clusterhook.ExportRequest
	// mute makes the session read the request and never answer.
	mute bool
}

func newSSHMember(t *testing.T, clientKey ssh.PublicKey) *sshMember {
	_, hostPriv, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)
	signer, err := ssh.NewSignerFromKey(hostPriv)
	require.NoError(t, err)
	cfg := &ssh.ServerConfig{PublicKeyCallback: func(_ ssh.ConnMetadata, k ssh.PublicKey) (*ssh.Permissions, error) {
		if string(k.Marshal()) != string(clientKey.Marshal()) {
			return nil, assert.AnError
		}
		return nil, nil
	}}
	cfg.AddHostKey(signer)
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	t.Cleanup(func() { ln.Close() })
	m := &sshMember{addr: ln.Addr().String(), hostKey: signer.PublicKey()}
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			go m.serve(conn, cfg)
		}
	}()
	return m
}

func (m *sshMember) serve(conn net.Conn, cfg *ssh.ServerConfig) {
	_, chans, reqs, err := ssh.NewServerConn(conn, cfg)
	if err != nil {
		return
	}
	go func() {
		for r := range reqs {
			m.record("global:" + r.Type)
			_ = r.Reply(false, nil)
		}
	}()
	for nc := range chans {
		m.record("channel:" + nc.ChannelType())
		if nc.ChannelType() != "session" {
			_ = nc.Reject(ssh.Prohibited, "no")
			continue
		}
		ch, creqs, err := nc.Accept()
		if err != nil {
			return
		}
		go func() {
			defer ch.Close()
			for r := range creqs {
				m.record(r.Type)
				_ = r.Reply(r.Type == "exec", nil)
				if r.Type != "exec" {
					continue
				}
				line, _ := bufio.NewReader(ch).ReadBytes('\n')
				m.mu.Lock()
				_ = json.Unmarshal(line, &m.got)
				mute := m.mute
				m.mu.Unlock()
				if mute {
					select {} // the member stops talking
				}
				b, _ := json.Marshal(clusterhook.ExportAnswer{OK: true, Stream: true})
				_, _ = ch.Write(append(b, '\n'))
				_, _ = ch.Write([]byte("the stream"))
				_, _ = ch.SendRequest("exit-status", false, ssh.Marshal(struct{ Status uint32 }{0}))
				return
			}
		}()
	}
}

func (m *sshMember) record(r string) {
	m.mu.Lock()
	m.requests = append(m.requests, r)
	m.mu.Unlock()
}

// The observer pins the member's host key, asks for one session and one
// exec, never a terminal, an agent or a forward, and reads the stream.
func TestSSHSourcePinsTheHostAndAsksForNothingElse(t *testing.T) {
	_, priv, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)
	block, err := ssh.MarshalPrivateKey(priv, "")
	require.NoError(t, err)
	dir := t.TempDir()
	keyFile := filepath.Join(dir, "id_ed25519")
	require.NoError(t, os.WriteFile(keyFile, pem.EncodeToMemory(block), 0o600))
	signer, err := ssh.NewSignerFromKey(priv)
	require.NoError(t, err)
	member := newSSHMember(t, signer.PublicKey())

	known := filepath.Join(dir, "known_hosts")
	require.NoError(t, os.WriteFile(known, []byte(knownhosts.Line([]string{knownhosts.Normalize(member.addr)}, member.hostKey)+"\n"), 0o600))
	src, err := NewSSHSource("forgejo-export@"+member.addr, keyFile, known)
	require.NoError(t, err)
	ans, body, err := src.Export(context.Background(), &clusterhook.ExportRequest{Op: clusterhook.OpExportWAL, Name: "x"})
	require.NoError(t, err)
	require.True(t, ans.OK && ans.Stream)
	got, err := io.ReadAll(body)
	require.NoError(t, err)
	body.Close()
	assert.Equal(t, "the stream", string(got))
	member.mu.Lock()
	assert.Equal(t, []string{"channel:session", "exec"}, member.requests)
	assert.Equal(t, clusterhook.OpExportWAL, member.got.Op)
	member.mu.Unlock()

	// Another host key at that address is refused, never learnt.
	_, other, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)
	otherSigner, err := ssh.NewSignerFromKey(other)
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(known, []byte(knownhosts.Line([]string{knownhosts.Normalize(member.addr)}, otherSigner.PublicKey())+"\n"), 0o600))
	src, err = NewSSHSource("forgejo-export@"+member.addr, keyFile, known)
	require.NoError(t, err)
	_, _, err = src.Export(context.Background(), &clusterhook.ExportRequest{Op: clusterhook.OpExportWAL})
	require.Error(t, err)
}

func observerKey(t *testing.T) (string, ssh.Signer) {
	t.Helper()
	_, priv, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)
	block, err := ssh.MarshalPrivateKey(priv, "")
	require.NoError(t, err)
	keyFile := filepath.Join(t.TempDir(), "id_ed25519")
	require.NoError(t, os.WriteFile(keyFile, pem.EncodeToMemory(block), 0o600))
	signer, err := ssh.NewSignerFromKey(priv)
	require.NoError(t, err)
	return keyFile, signer
}

func exportWithin(t *testing.T, src *SSHSource, bound time.Duration) error {
	t.Helper()
	done := make(chan error, 1)
	go func() {
		_, body, err := src.Export(context.Background(), &clusterhook.ExportRequest{Op: clusterhook.OpExportCheckpoints})
		if body != nil {
			body.Close()
		}
		done <- err
	}()
	select {
	case err := <-done:
		return err
	case <-time.After(bound):
		t.Fatalf("Export still blocked after %s", bound)
		return nil
	}
}

// A member whose sshd accepts the TCP connection and never speaks does not
// hold the observer: the handshake has a deadline of its own, the context
// none.
func TestSSHSourceDropsASilentMember(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	t.Cleanup(func() { ln.Close() })
	var held []net.Conn
	var mu sync.Mutex
	t.Cleanup(func() {
		mu.Lock()
		defer mu.Unlock()
		for _, c := range held {
			c.Close()
		}
	})
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			mu.Lock()
			held = append(held, c) // accepted, never read, never written
			mu.Unlock()
		}
	}()
	keyFile, _ := observerKey(t)
	known := filepath.Join(t.TempDir(), "known_hosts")
	require.NoError(t, os.WriteFile(known, nil, 0o600))
	src, err := NewSSHSource("obs@"+ln.Addr().String(), keyFile, known)
	require.NoError(t, err)
	src.cfg.Timeout = 200 * time.Millisecond
	require.Error(t, exportWithin(t, src, 10*time.Second))
}

// A member that completes the handshake, takes the request and stops
// talking is dropped after the idle deadline.
func TestSSHSourceDropsAMemberThatStopsTalking(t *testing.T) {
	keyFile, signer := observerKey(t)
	member := newSSHMember(t, signer.PublicKey())
	member.mu.Lock()
	member.mute = true
	member.mu.Unlock()
	known := filepath.Join(t.TempDir(), "known_hosts")
	require.NoError(t, os.WriteFile(known, []byte(knownhosts.Line([]string{knownhosts.Normalize(member.addr)}, member.hostKey)+"\n"), 0o600))
	src, err := NewSSHSource("obs@"+member.addr, keyFile, known)
	require.NoError(t, err)
	src.idle = 300 * time.Millisecond
	require.Error(t, exportWithin(t, src, 10*time.Second))
}
