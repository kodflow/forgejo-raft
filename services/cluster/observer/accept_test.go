// Copyright 2026 The Forgejo Authors. All rights reserved.
// SPDX-License-Identifier: GPL-3.0-or-later

//go:build unix

package observer

import (
	"context"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// A second process on a running observer's directory is refused before it
// touches anything: staging stays as the running one left it (#86).
func TestNewRefusesADirectoryHeldByAnother(t *testing.T) {
	c := newFakeCluster(t)
	o := newTestObserver(t, c, &hookLog{})
	inflight := o.store.path("staging", "in-flight")
	require.NoError(t, os.WriteFile(inflight, []byte("a base backup being copied"), 0o600))
	_, err := New(o.cfg)
	require.ErrorIs(t, err, ErrBusy)
	_, err = os.Stat(inflight)
	require.NoError(t, err, "the running observer's staging is kept")
	o.Close()
	n, err := New(o.cfg)
	require.NoError(t, err)
	n.Close()
}

// `observe accept` beside the running observer reaches it: the request is
// taken at the next pass, which ingests again (#86).
func TestRunningObserverTakesAnAcceptRequest(t *testing.T) {
	ctx := context.Background()
	c := newFakeCluster(t)
	o := newTestObserver(t, c, &hookLog{})
	seed(t, c, o)
	require.NoError(t, o.Step(ctx))
	recoverFake(c, 0x0d)
	c.push("alice/demo.git", "refs/heads/main", c.commit("after recover"))
	cp := c.checkpoint()
	require.ErrorIs(t, o.Step(ctx), ErrQuarantined)
	q := o.State().Quarantine
	require.NotNil(t, q)

	// A wrong lineage is refused, and the request goes with its answer.
	require.NoError(t, RequestAccept(o.cfg.Dir, "not-the-lineage"))
	require.ErrorContains(t, o.Step(ctx), "the quarantine is about lineage")
	require.NotNil(t, o.State().Quarantine)
	_, err := os.Stat(o.store.path(acceptFile))
	require.True(t, os.IsNotExist(err))

	require.NoError(t, RequestAccept(o.cfg.Dir, q.Lineage))
	require.NoError(t, o.Step(ctx))
	st := o.State()
	require.Nil(t, st.Quarantine)
	a, ok := st.newestOf(q.Lineage)
	require.True(t, ok)
	assert.Equal(t, cp.Name(), a.Name)
}
