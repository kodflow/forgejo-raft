// Copyright 2026 The Forgejo Authors. All rights reserved.
// SPDX-License-Identifier: GPL-3.0-or-later

package cmd

import (
	"context"
	"errors"
	"io"
	"net"
	"testing"
	"time"

	"forgejo.org/modules/clusterhook"
	"forgejo.org/modules/setting"
	"forgejo.org/modules/test"
	"forgejo.org/services/cluster/consensus"

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

// After a Recover this node is the only voter: another survivor listed as a
// voter could run its own Recover into the same cluster id.
func TestRecoverNamesThisNodeAloneAsVoter(t *testing.T) {
	rep := consensus.Report{Node: 0xa, Slot: "node-a"}
	require.NoError(t, checkRecoverVoters([]consensus.Member{{ID: 0xa, Slot: "node-a", Voter: true}}, rep))
	err := checkRecoverVoters([]consensus.Member{{ID: 0xa, Slot: "node-a", Voter: true}, {ID: 0xb, Slot: "node-b", Voter: true}}, rep)
	require.ErrorContains(t, err, "RECOVER_REFUSED")
	require.ErrorContains(t, checkRecoverVoters([]consensus.Member{{ID: 0xb, Slot: "node-a", Voter: true}}, rep), "only this node")
}

// A recovery from an observer needs a fence for every voter the
// checkpoint names, and at least one when it names none.
func TestObserverRecoveryFencesEveryVoter(t *testing.T) {
	fenced := []consensus.FencedVoter{{Voter: 0xa}, {Voter: 0xb}}
	require.NoError(t, fenceCovers([]string{"000000000000000a", "000000000000000b"}, fenced))
	require.ErrorContains(t, fenceCovers([]string{"000000000000000a", "000000000000000c"}, fenced), "000000000000000c")
	require.ErrorContains(t, fenceCovers(nil, nil), "--fenced")
	require.NoError(t, fenceCovers(nil, fenced))
}

// The schema is migrated before the identifiers move: a table a migration
// creates has its sequence moved too.
func TestRecoveryMigratesBeforeAdvancing(t *testing.T) {
	var order []string
	step := func(name string, err error) func(context.Context) error {
		return func(context.Context) error {
			order = append(order, name)
			return err
		}
	}
	require.NoError(t, migrateThenAdvance(context.Background(), step("migrate", nil), step("advance", nil)))
	assert.Equal(t, []string{"migrate", "advance"}, order)
	order = nil
	require.Error(t, migrateThenAdvance(context.Background(), step("migrate", errors.New("no")), step("advance", nil)))
	assert.Equal(t, []string{"migrate"}, order, "nothing advances on a schema that did not migrate")
}

// The probe asks PEERS and every --old-peer, never this node's slot; with
// nothing to ask, or an old member answering, the Recover is refused
// unless forced.
func TestRecoverProbesTheOldMembers(t *testing.T) {
	defer test.MockVariableValue(&setting.Cluster.Slot, "node-a")()
	defer test.MockVariableValue(&setting.Cluster.Peers, map[string]string{"node-a": "127.0.0.1:1", "node-b": "127.0.0.1:2"})()
	defer test.MockVariableValue(&setting.Cluster.TLSCAFile, "")()
	run := func(args ...string) error {
		var got error
		cmd := &cli.Command{
			Name: "recover",
			Flags: []cli.Flag{
				&cli.StringSliceFlag{Name: "old-peer"},
				&cli.BoolFlag{Name: "force-old-members-answer"},
			},
			Action: func(ctx context.Context, c *cli.Command) error {
				addrs, err := oldPeerAddrs(c)
				require.NoError(t, err)
				assert.NotContains(t, addrs, "node-a")
				got = checkOldMembersSilent(ctx, c, consensus.ClusterID{1}, consensus.ClusterID{2})
				return nil
			},
		}
		cmd.Writer = io.Discard
		require.NoError(t, cmd.Run(context.Background(), append([]string{"recover"}, args...)))
		return got
	}
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer ln.Close()
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			c.Close()
		}
	}()
	require.ErrorContains(t, run("--old-peer", "node-c="+ln.Addr().String()), "old member answers")
	require.NoError(t, run("--old-peer", "node-c="+ln.Addr().String(), "--force-old-members-answer"))
	ln.Close()
	require.NoError(t, run("--old-peer", "node-c="+ln.Addr().String()), "every old member silent")

	defer test.MockVariableValue(&setting.Cluster.Peers, map[string]string{})()
	require.ErrorContains(t, run(), "no old member to probe")
	require.NoError(t, run("--force-old-members-answer"))
}
