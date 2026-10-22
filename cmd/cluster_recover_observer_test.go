// Copyright 2026 The Forgejo Authors. All rights reserved.
// SPDX-License-Identifier: GPL-3.0-or-later

package cmd

import (
	"context"
	"crypto/ed25519"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"forgejo.org/modules/clusterhook"
	"forgejo.org/modules/setting"
	"forgejo.org/modules/test"
	"forgejo.org/services/cluster/consensus"
	"forgejo.org/services/cluster/reftable"
	"forgejo.org/services/cluster/witness"

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

// The promotion's timeline history reaches the fresh archive: PostgreSQL
// wrote it with archiving off and never hands it to archive_command, and
// without it the observer covers no checkpoint of the new timeline (#115).
func TestRecoverArchivesThePromotedHistory(t *testing.T) {
	pgdata, dir := t.TempDir(), t.TempDir()
	wal := filepath.Join(pgdata, "pg_wal")
	require.NoError(t, os.MkdirAll(filepath.Join(wal, "archive_status"), 0o700))
	require.NoError(t, os.WriteFile(filepath.Join(wal, "00000002.history"), []byte("1\t0/3000000\tno recovery target specified\n"), 0o600))
	require.NoError(t, os.WriteFile(filepath.Join(wal, "00000003.history"), []byte("1\t0/3000000\n2\t0/5011940\tbefore 0/5011940\n"), 0o600))
	require.NoError(t, os.WriteFile(filepath.Join(wal, "000000030000000000000005"), []byte("segment"), 0o600))

	require.Error(t, archiveHistories(pgdata, dir, 3), "no history past the checkpoint's timeline: not promoted")
	require.NoError(t, archiveHistories(pgdata, dir, 2))
	for _, name := range []string{"00000002.history", "00000003.history"} {
		_, err := os.Stat(filepath.Join(dir, name))
		require.NoError(t, err, name)
	}
	_, err := os.Stat(filepath.Join(dir, "000000030000000000000005"))
	assert.True(t, os.IsNotExist(err), "segments are PostgreSQL's to archive")
	// A second run, after a crash, finds the same bytes and succeeds.
	require.NoError(t, archiveHistories(pgdata, dir, 2))
}

// originDriver is a driver that reads a snapshot's origin, and nothing else.
type originDriver struct {
	consensus.Driver
	old    consensus.ClusterID
	voters []consensus.NodeID
}

func (d originDriver) SnapshotOrigin([]byte) (consensus.ClusterID, []consensus.NodeID, error) {
	return d.old, d.voters, nil
}

// A restore founds a new cluster only once the cluster the snapshot was cut
// from is fenced at both witnesses, for every voter it had, and none of its
// members answers: otherwise the old site, back up, forms its cluster
// again and two forges write (#36).
func TestHuntR1ProveRestoreCommandHasNoFenceNorProbe(t *testing.T) {
	defer test.MockVariableValue(&setting.Cluster.Slot, "node-r")()
	defer test.MockVariableValue(&setting.Cluster.Peers, map[string]string{})()
	defer test.MockVariableValue(&setting.Cluster.TLSCAFile, "")()
	defer test.MockVariableValue(&setting.Cluster.WitnessKeys, []string(nil))()
	old, cid := consensus.ClusterID{0x0f, 0x07}, consensus.ClusterID{0x0e, 0x57}
	d := originDriver{old: old, voters: []consensus.NodeID{0xa, 0xb}}

	// Two witnesses, and their receipts for both old voters.
	dir := t.TempDir()
	var keys []string
	var privs []ed25519.PrivateKey
	for range 2 {
		pub, priv, err := ed25519.GenerateKey(nil)
		require.NoError(t, err)
		keys, privs = append(keys, hex.EncodeToString(pub)), append(privs, priv)
	}
	fence := func(voter consensus.NodeID, signers ...ed25519.PrivateKey) string {
		var files []string
		for i, k := range signers {
			f := filepath.Join(dir, fmt.Sprintf("%s-%d", voter, i))
			require.NoError(t, os.WriteFile(f, []byte(hex.EncodeToString(ed25519.Sign(k, witness.ReceiptMessage(old, cid, voter)))), 0o600))
			files = append(files, f)
		}
		return voter.String() + ":stopped:" + strings.Join(files, "+")
	}

	var restore *cli.Command
	for _, c := range cmdCluster().Commands {
		if c.Name == "restore" {
			restore = c
		}
	}
	require.NotNil(t, restore)
	run := func(args ...string) error {
		var got error
		cmd := &cli.Command{Name: "restore", Flags: restore.Flags, Action: func(ctx context.Context, c *cli.Command) error {
			nc, err := consensus.ParseClusterID(c.String("new-cluster"))
			require.NoError(t, err)
			got = checkRestoreFence(ctx, c, d, nil, nc)
			return nil
		}}
		cmd.Writer = io.Discard
		require.NoError(t, cmd.Run(context.Background(), append([]string{"restore", "--file", "x", "--sha256", "00", "--members", "x"}, args...)))
		return got
	}
	both := []string{"--new-cluster", cid.String(), "--witness-key", keys[0], "--witness-key", keys[1]}
	fenced := []string{"--fenced", fence(0xa, privs...), "--fenced", fence(0xb, privs...)}

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			c.Close()
		}
	}()
	oldPeer := []string{"--old-peer", "node-a=" + ln.Addr().String()}

	require.ErrorContains(t, run(append([]string{"--new-cluster", cid.String()}, oldPeer...)...), "--fenced", "no fence")
	require.ErrorContains(t, run(slices.Concat(both, []string{"--fenced", fence(0xa, privs...)}, oldPeer)...), "000000000000000b", "a voter left unfenced")
	require.ErrorContains(t, run(slices.Concat([]string{"--new-cluster", cid.String()}, fenced, oldPeer)...), "both witnesses", "no witness key")
	require.ErrorContains(t, run(slices.Concat(both, []string{"--fenced", fence(0xa, privs[0]), "--fenced", fence(0xb, privs...)}, oldPeer)...), "RECOVER_REFUSED", "one witness never recorded the fence")
	require.ErrorContains(t, run(slices.Concat(both, fenced, oldPeer)...), "old member answers", "an old member answers")
	require.ErrorContains(t, run(slices.Concat([]string{"--new-cluster", old.String(), "--witness-key", keys[0], "--witness-key", keys[1]}, fenced)...), "own id")
	ln.Close()
	require.NoError(t, run(slices.Concat(both, fenced, oldPeer)...), "fenced and silent")
}

// A Recover cut after its stamp and before it removed STOP-RECOVER is
// completed by running it again to the same cluster: the CLI no longer
// asks for receipts of (new, new), which no witness can sign, and the
// engine removes the marker (#35).
func TestHuntR1ProveRecoverInterruptedAfterStamp(t *testing.T) {
	d, err := consensus.Lookup("kitsunium")
	require.NoError(t, err)
	old, cid := consensus.ClusterID{0x0a}, consensus.ClusterID{0x0b}
	var members []consensus.Member
	dirs := map[string]string{}
	for i, slot := range []string{"node-a", "node-b", "node-c"} {
		dir := t.TempDir()
		id, inc, err := d.Init(dir, old, slot)
		require.NoError(t, err)
		dirs[slot] = dir
		members = append(members, consensus.Member{ID: id, Incarnation: inc, Slot: slot, SPKI: [32]byte{byte(i + 1)}, Address: fmt.Sprintf("127.0.0.1:%d", i+1), Voter: true})
	}
	dir := dirs["node-b"]
	require.NoError(t, d.Bootstrap(context.Background(), consensus.Config{DataDir: dir, Slot: "node-b", Machine: reftable.New(), MaxLogBytes: 1 << 30, MaxSnapshotBytes: 1 << 30}, members))

	// Two witnesses fenced node-a and node-c for the Recover of node-b.
	var keys [][]byte
	var privs []ed25519.PrivateKey
	for range 2 {
		pub, priv, err := ed25519.GenerateKey(nil)
		require.NoError(t, err)
		keys, privs = append(keys, pub), append(privs, priv)
	}
	opt := consensus.RecoverOptions{Operator: consensus.Operator("forgejo-user:1"), Confirm: cid.String(), WitnessKeys: keys}
	for _, m := range []consensus.Member{members[0], members[2]} {
		fv := consensus.FencedVoter{Voter: m.ID, Method: "stopped"}
		for _, k := range privs {
			fv.Receipts = append(fv.Receipts, ed25519.Sign(k, witness.ReceiptMessage(old, cid, m.ID)))
		}
		opt.Fenced = append(opt.Fenced, fv)
	}
	self := []consensus.Member{members[1]}
	require.NoError(t, d.Recover(dir, cid, self, opt))
	rep, err := d.Inspect(dir)
	require.NoError(t, err)
	require.Equal(t, cid, rep.Cluster)
	assert.False(t, recoverInterruptedAfterStamp(rep, cid), "a completed Recover")

	// The crash between the stamp and the marker's removal.
	require.NoError(t, os.WriteFile(filepath.Join(dir, consensus.MarkerRecover), fmt.Appendf(nil, "cluster=%s\nkept=%d\nRecover to cluster %s was interrupted: run Recover again to cluster %s to complete it\n", cid, rep.LastIndex-1, cid, cid), 0o600))
	rep, err = d.Inspect(dir)
	require.NoError(t, err)
	require.True(t, recoverInterruptedAfterStamp(rep, cid))
	assert.False(t, recoverInterruptedAfterStamp(rep, consensus.ClusterID{0x0c}), "another cluster is a new Recover")
	// The receipts the CLI used to check, against the stamp's cluster, can
	// never verify: they sign (old, new).
	require.Error(t, witness.VerifyReceipts([]ed25519.PublicKey{keys[0], keys[1]}, rep.Cluster, cid, opt.Fenced))

	// What the CLI runs now: the same Recover, without the fence.
	require.NoError(t, d.Recover(dir, cid, self, consensus.RecoverOptions{Operator: opt.Operator, Confirm: cid.String()}))
	rep, err = d.Inspect(dir)
	require.NoError(t, err)
	assert.NotContains(t, rep.Markers, consensus.MarkerRecover)
	_, err = os.Stat(filepath.Join(dir, consensus.MarkerRecover))
	assert.True(t, os.IsNotExist(err))
}
