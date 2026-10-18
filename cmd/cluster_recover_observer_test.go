// Copyright 2026 The Forgejo Authors. All rights reserved.
// SPDX-License-Identifier: GPL-3.0-or-later

package cmd

import (
	"context"
	"errors"
	"testing"
	"time"

	"forgejo.org/modules/clusterhook"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/urfave/cli/v3"
)

// The recovery's PostgreSQL may still replay when pg_ctl -w returns, and
// refuses connections with SQLSTATE 57P03 until it promoted: the recovery
// waits instead of failing its first query, as it did in the lab.
func TestRecoveryWaitsForThePromotion(t *testing.T) {
	notYet := errors.New("server error: FATAL: the database system is not accepting connections (SQLSTATE 57P03)")
	calls := 0
	err := waitAccepting(context.Background(), func() error {
		calls++
		if calls < 3 {
			return notYet
		}
		return nil
	}, time.Millisecond, time.Minute)
	require.NoError(t, err)
	assert.Equal(t, 3, calls)

	other := errors.New("password authentication failed (SQLSTATE 28P01)")
	calls = 0
	err = waitAccepting(context.Background(), func() error { calls++; return other }, time.Millisecond, time.Minute)
	require.ErrorIs(t, err, other, "any other error ends the wait")
	assert.Equal(t, 1, calls)

	err = waitAccepting(context.Background(), func() error { return notYet }, time.Millisecond, 20*time.Millisecond)
	require.ErrorContains(t, err, "still accepts no connection")
}

// The marker that records a recovery's advance of the identifiers fits a
// cursor and names that recovery alone: resuming it advances nothing
// twice, a later recovery advances again.
func TestRecoveryMarker(t *testing.T) {
	m := recoveryMarker("0123-5-00000000000000000042", "c1")
	assert.Len(t, m, 64)
	assert.Equal(t, m, recoveryMarker("0123-5-00000000000000000042", "c1"))
	assert.NotEqual(t, m, recoveryMarker("0123-5-00000000000000000042", "c2"))
	assert.NotEqual(t, m, recoveryMarker("0123-5-00000000000000000043", "c1"))
}

// `forgejo cluster parked resolve` gives nothing up without a reason.
func TestParkedResolveNeedsAReason(t *testing.T) {
	run := func(args ...string) (*clusterhook.Request, error) {
		var req *clusterhook.Request
		var rerr error
		cmd := &cli.Command{
			Name: "resolve",
			Flags: []cli.Flag{
				&cli.StringFlag{Name: "reason"},
				&cli.StringFlag{Name: "lineage"},
			},
			Action: func(_ context.Context, c *cli.Command) error {
				req, rerr = parkedResolveRequest(c)
				return nil
			},
		}
		require.NoError(t, cmd.Run(context.Background(), append([]string{"resolve"}, args...)))
		return req, rerr
	}
	_, err := run("5")
	require.ErrorContains(t, err, "--reason is required")
	_, err = run("--reason", "   ", "5")
	require.ErrorContains(t, err, "--reason is required")
	_, err = run("--reason", "x", "abc")
	require.Error(t, err)
	req, err := run("--reason", "account recreated", "--lineage", "l", "5")
	require.NoError(t, err)
	assert.Equal(t, clusterhook.OpParkedResolve, req.Op)
	assert.EqualValues(t, 5, req.Index)
	assert.Equal(t, "l", req.Lineage)
	assert.Equal(t, "account recreated", req.Reason)
	assert.NotEmpty(t, req.Operator)
}
