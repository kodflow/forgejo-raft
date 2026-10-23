// Copyright 2026 The Forgejo Authors. All rights reserved.
// SPDX-License-Identifier: GPL-3.0-or-later

package observer

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"strings"
	"sync/atomic"
	"time"

	"forgejo.org/modules/clusterhook"
	"forgejo.org/modules/json"

	"golang.org/x/crypto/ssh"
	"golang.org/x/crypto/ssh/knownhosts"
)

// SSHSource reaches one member's export over SSH. The observer opens every
// connection; it asks for one session running the command the member's
// authorized_keys forces (`restrict,command="forgejo cluster export
// --observer <name>",from=...`), sends the request on its stdin and reads
// the answer on its stdout. It never asks for a terminal, an agent or a
// forward, and the member's host key must be in the pinned known_hosts:
// an unknown key is refused, never learnt.
type SSHSource struct {
	addr string
	cfg  *ssh.ClientConfig
	// idle is how long the connection may stay silent once the handshake
	// is done: a member that accepts and then stops talking is dropped,
	// never waited for.
	idle time.Duration
}

// sshIdleTimeout is how long an export may send nothing.
const sshIdleTimeout = 2 * time.Minute

// NewSSHSource builds the source of target, user@host:port.
func NewSSHSource(target, keyFile, knownHostsFile string) (*SSHSource, error) {
	user, addr, ok := strings.Cut(target, "@")
	if !ok {
		return nil, fmt.Errorf("%q is not user@host:port", target)
	}
	pem, err := os.ReadFile(keyFile)
	if err != nil {
		return nil, err
	}
	signer, err := ssh.ParsePrivateKey(pem)
	if err != nil {
		return nil, fmt.Errorf("the observer's SSH key: %w", err)
	}
	hostKeys, err := knownhosts.New(knownHostsFile)
	if err != nil {
		return nil, fmt.Errorf("the pinned host keys: %w", err)
	}
	return &SSHSource{addr: addr, cfg: &ssh.ClientConfig{
		User: user, Auth: []ssh.AuthMethod{ssh.PublicKeys(signer)},
		HostKeyCallback: hostKeys, Timeout: 15 * time.Second,
	}, idle: sshIdleTimeout}, nil
}

// Export runs one export call.
func (s *SSHSource) Export(ctx context.Context, req *clusterhook.ExportRequest) (*clusterhook.ExportAnswer, io.ReadCloser, error) {
	d := net.Dialer{Timeout: s.cfg.Timeout}
	conn, err := d.DialContext(ctx, "tcp", s.addr)
	if err != nil {
		return nil, nil, err
	}
	// ClientConfig.Timeout covers the dial only: the handshake gets a
	// deadline of its own, then every read or write pushes a rolling idle
	// deadline, so a member that stops talking is dropped.
	ic := newIdleConn(conn, 2*s.cfg.Timeout)
	if dl, ok := ctx.Deadline(); ok {
		ic.hard = dl
	}
	stop := context.AfterFunc(ctx, func() { conn.Close() })
	sc, chans, reqs, err := ssh.NewClientConn(ic, s.addr, s.cfg)
	if err != nil {
		stop()
		conn.Close()
		return nil, nil, err
	}
	ic.setIdle(s.idle)
	client := ssh.NewClient(sc, chans, reqs)
	closeAll := func() {
		stop()
		client.Close()
	}
	sess, err := client.NewSession()
	if err != nil {
		closeAll()
		return nil, nil, err
	}
	stdin, err := sess.StdinPipe()
	if err != nil {
		closeAll()
		return nil, nil, err
	}
	stdout, err := sess.StdoutPipe()
	if err != nil {
		closeAll()
		return nil, nil, err
	}
	if err := sess.Start("export"); err != nil {
		closeAll()
		return nil, nil, err
	}
	b, err := json.Marshal(req)
	if err == nil {
		_, err = stdin.Write(append(b, '\n'))
	}
	stdin.Close()
	if err != nil {
		closeAll()
		return nil, nil, err
	}
	br := bufio.NewReader(stdout)
	line, err := clusterhook.ReadExportHead(br, clusterhook.MaxExportHead)
	if err != nil {
		closeAll()
		return nil, nil, fmt.Errorf("the export's answer: %w", err)
	}
	var ans clusterhook.ExportAnswer
	if err := json.Unmarshal(line, &ans); err != nil {
		closeAll()
		return nil, nil, fmt.Errorf("the export's answer: %w", err)
	}
	return &ans, &sessionBody{Reader: br, sess: sess, done: closeAll}, nil
}

// sessionBody is a stream's body: closing it ends the session, and a
// stream cut short reads as an error, not as an end.
type sessionBody struct {
	io.Reader
	sess *ssh.Session
	done func()
	// ended is the session's exit, once the stream reached its end.
	ended  error
	waited bool
}

func (b *sessionBody) Read(p []byte) (int, error) {
	n, err := b.Reader.Read(p)
	if errors.Is(err, io.EOF) {
		if !b.waited {
			b.waited = true
			if werr := b.sess.Wait(); werr != nil {
				b.ended = fmt.Errorf("the export ended badly: %w", werr)
			}
		}
		if b.ended != nil {
			return n, b.ended
		}
	}
	return n, err
}

func (b *sessionBody) Close() error {
	b.done()
	return nil
}

// idleConn bounds every read and write: each one moves the deadline idle
// ahead, never past hard when the caller's context has a deadline.
type idleConn struct {
	net.Conn
	idle atomic.Int64
	hard time.Time
}

func newIdleConn(c net.Conn, idle time.Duration) *idleConn {
	ic := &idleConn{Conn: c}
	ic.idle.Store(int64(idle))
	return ic
}

func (c *idleConn) setIdle(d time.Duration) { c.idle.Store(int64(d)) }

func (c *idleConn) push() {
	dl := time.Now().Add(time.Duration(c.idle.Load()))
	if !c.hard.IsZero() && c.hard.Before(dl) {
		dl = c.hard
	}
	_ = c.SetDeadline(dl)
}

func (c *idleConn) Read(p []byte) (int, error) {
	c.push()
	return c.Conn.Read(p)
}

func (c *idleConn) Write(p []byte) (int, error) {
	c.push()
	return c.Conn.Write(p)
}
