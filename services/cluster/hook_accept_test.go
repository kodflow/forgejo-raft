// Copyright 2026 The Forgejo Authors. All rights reserved.
// SPDX-License-Identifier: GPL-3.0-or-later

package cluster

import (
	"context"
	"errors"
	"net"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// flakyListener fails its first Accepts as an exhausted process does.
type flakyListener struct {
	net.Listener
	fails atomic.Int32
}

func (l *flakyListener) Accept() (net.Conn, error) {
	if l.fails.Add(-1) >= 0 {
		return nil, &net.OpError{Op: "accept", Net: "unix", Err: syscall.EMFILE}
	}
	return l.Listener.Accept()
}

// An Accept error other than the listener's close must not end the hook
// socket's loop for good (#66): the node is not ready while it lasts, and
// serves hooks again once it passes.
func TestTheHookSocketOutlivesAnAcceptError(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	fl := &flakyListener{Listener: ln}
	fl.fails.Store(5)
	ctx, cancel := context.WithCancel(t.Context())
	var failing atomic.Bool
	served := make(chan struct{}, 1)
	done := make(chan struct{})
	go func() {
		acceptHooks(ctx, fl, &failing, func(c net.Conn) {
			c.Close()
			served <- struct{}{}
		})
		close(done)
	}()
	require.Eventually(t, failing.Load, 5*time.Second, time.Millisecond, "the failure is reported")

	conn, err := net.Dial("tcp", ln.Addr().String())
	require.NoError(t, err)
	defer conn.Close()
	select {
	case <-served:
	case <-time.After(10 * time.Second):
		t.Fatal("no connection served after the Accept errors")
	}
	assert.False(t, failing.Load(), "the failure cleared")

	cancel()
	ln.Close()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("the loop did not end with its listener")
	}
	assert.True(t, errors.Is(ctx.Err(), context.Canceled))
}
