// Copyright 2026 The Forgejo Authors. All rights reserved.
// SPDX-License-Identifier: GPL-3.0-or-later

package kitsunium

import (
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"forgejo.org/services/cluster/consensus"

	sdk "github.com/kitsunium/sdk/pkg/v1/app/consensus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// countMachine counts the entries it applies, and its snapshot holds the
// count: a body the engine encodes and checks as any other.
type countMachine struct{ n *atomic.Uint64 }

func (m countMachine) Apply(consensus.Entry) any { m.n.Add(1); return nil }

func (m countMachine) Snapshot() (consensus.Snapshot, error) { return countSnap(m.n.Load()), nil }

func (m countMachine) Restore(r io.Reader, _ consensus.SnapshotMeta) error {
	var b [8]byte
	if _, err := io.ReadFull(r, b[:]); err != nil {
		return err
	}
	m.n.Store(binary.LittleEndian.Uint64(b[:]))
	return nil
}

type countSnap uint64

func (s countSnap) WriteTo(w io.Writer) (int64, error) {
	n, err := w.Write(binary.LittleEndian.AppendUint64(nil, uint64(s)))
	return int64(n), err
}

func (countSnap) Release() {}

// cut is a one-voter data directory a node ran on until it cut a snapshot
// and wrote entries past it: the log's own extent, the term at its last
// index, and the newest snapshot with the log's term at its index.
type cut struct {
	dir                   string
	first, last, lastTerm uint64
	newest                sdk.SnapshotMetaValue
	termAtNewest          uint64
}

// cutDir founds a cut, and returns with the node stopped and the directory
// unlocked.
func cutDir(t *testing.T) cut {
	t.Helper()
	c := cut{dir: t.TempDir()}
	id, inc, err := driver{}.Init(c.dir, consensus.ClusterID{0x74, 0x06}, "a")
	require.NoError(t, err)
	d, err := openDataDir(c.dir)
	require.NoError(t, err)
	rep, err := sdk.Inspect(d)
	require.NoError(t, err)
	cfg := sdk.Config{
		Cluster: rep.Stamp.Log.Cluster, ID: rep.Stamp.ID, Machine: Machine(countMachine{n: new(atomic.Uint64)}),
		Durability: sdk.DurabilityVolatile, Log: d.Log(), Snapshots: d.Snapshots(),
		Transport:        sdk.NewMemoryTransport(sdk.NewMemoryNetwork(), rep.Stamp.ID),
		InitialTiming:    sdk.TimingContractSpec{HeartbeatInterval: 10 * time.Millisecond, ElectionTimeoutMin: 100 * time.Millisecond, ElectionTimeoutMax: 200 * time.Millisecond},
		MaxSnapshotBytes: 1 << 30, MaxLogBytes: 1 << 30, MinVoters: 1,
		SnapshotEvery: 8, SnapshotTrailing: 4,
	}
	self := sdk.MemberValue{ID: sdk.NodeID(id), Incarnation: sdk.Incarnation(inc), Slot: "a", SPKI: sdk.MemorySPKI(sdk.NodeID(id)), Voter: true}
	require.NoError(t, sdk.Bootstrap(context.Background(), &cfg, []sdk.MemberValue{self}))
	n, err := sdk.NewNode(cfg)
	require.NoError(t, err)
	stopped := false
	t.Cleanup(func() {
		if !stopped {
			_ = n.HardClose()
		}
		_ = d.Close()
	})
	require.Eventually(t, func() bool { return n.Status().Role == sdk.RoleLeader }, 10*time.Second, 10*time.Millisecond)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	propose := func(k int) {
		for range k {
			_, _, err := n.Propose(ctx, []byte("x"))
			require.NoError(t, err)
		}
	}
	propose(9)
	require.Eventually(t, func() bool {
		metas, err := d.Snapshots().List()
		return err == nil && len(metas) > 0
	}, 10*time.Second, 10*time.Millisecond, "no snapshot was cut")
	// Fewer than SnapshotEvery: the log ends past the snapshot.
	propose(3)
	require.NoError(t, n.Close(ctx))
	stopped = true

	c.first, c.last = d.Log().Bounds()
	c.lastTerm, err = d.Log().Term(c.last)
	require.NoError(t, err)
	metas, err := d.Snapshots().List()
	require.NoError(t, err)
	c.newest = metas[0]
	c.termAtNewest, err = d.Log().Term(c.newest.Position.Index)
	require.NoError(t, err)
	require.True(t, c.first <= c.newest.Position.Index && c.newest.Position.Index < c.last,
		"setup: the snapshot at %d lies inside the log %d..%d, below its end", c.newest.Position.Index, c.first, c.last)
	require.NoError(t, d.Close())
	return c
}

// republish writes m as the metadata of c's newest snapshot, under m's id:
// the snapshot's body is moved under that id, unchanged.
func (c cut) republish(t *testing.T, m *sdk.SnapshotMetaValue) {
	t.Helper()
	snap := filepath.Join(c.dir, "snap")
	old := c.newest.ID
	if m.ID != old {
		require.NoError(t, os.Rename(filepath.Join(snap, old+".snap"), filepath.Join(snap, m.ID+".snap")))
		require.NoError(t, os.Remove(filepath.Join(snap, old+".meta")))
	}
	require.NoError(t, os.WriteFile(filepath.Join(snap, m.ID+".meta"), sdk.AppendSnapshotMeta(nil, m), 0o600))
}

// interruptInstall leaves in c what an install that a crash interrupted
// once its snapshot was published leaves: the newest snapshot published at
// its index under a term the log does not hold there, inside the log, its
// body unchanged so that its checksums hold (kitsunium/sdk#746). mutate,
// when not nil, changes its metadata further.
func (c cut) interruptInstall(t *testing.T, mutate func(*sdk.SnapshotMetaValue)) sdk.SnapshotMetaValue {
	t.Helper()
	m := c.newest
	m.Position.Term = c.termAtNewest + 1
	m.ID = fmt.Sprintf("%020d-%020d", m.Position.Index, m.Position.Term)
	if mutate != nil {
		mutate(&m)
	}
	c.republish(t, &m)
	return m
}

// cluster inspect reports, for an install a crash interrupted, what the
// start and Recover keep, the snapshot's index and term, which an operator
// ranks survivors by (kitsunium/sdk#746), and its detail still says what
// the log itself holds: "log First..Last" read the log's extent, which the
// snapshot's index below the log's end no longer is. With no install
// interrupted, Inspect reads no snapshot, as the node's start inspects its
// directory several times before it opens it: a newest one of a version
// this build cannot read is left to the engine's start.
func TestInspectSaysWhatTheStartKeepsOfAnInterruptedInstall(t *testing.T) {
	c := cutDir(t)
	unread := c.newest
	unread.ClusterVersion = math.MaxUint32
	c.republish(t, &unread)
	rep, err := driver{}.Inspect(c.dir)
	require.NoError(t, err, "no install interrupted: no snapshot is read")
	assert.Equal(t, c.last, rep.LastIndex, "no install interrupted: the log's last index")
	assert.Equal(t, c.lastTerm, rep.LastTerm)
	require.NotEmpty(t, rep.Detail)
	assert.Equal(t, []string{fmt.Sprintf("log %d..%d, last term %d", c.first, c.last, c.lastTerm)}, rep.Detail[1:])

	m := c.interruptInstall(t, nil)
	rep, err = driver{}.Inspect(c.dir)
	require.NoError(t, err)
	assert.Equal(t, m.Position.Index, rep.LastIndex, "the snapshot's index, which the start and Recover move the log onto")
	assert.Equal(t, m.Position.Term, rep.LastTerm, "the snapshot's term")
	require.NotEmpty(t, rep.Detail)
	assert.Equal(t, []string{
		fmt.Sprintf("log %d..%d, last term %d", c.first, c.last, c.lastTerm),
		fmt.Sprintf("interrupted install: the start and Recover move the log onto the snapshot at %d, term %d", m.Position.Index, m.Position.Term),
	}, rep.Detail[1:], "the log's own extent, then what the start keeps")
}

// The snapshot of an interrupted install that a newer release wrote, of a
// cluster version this build does not speak, is refused VERSION_UNSUPPORTED
// by cluster inspect and by the node's start, which inspects the directory
// before it opens the engine, with the directory and the remedy, as a
// snapshot metadata of a newer version is (kitsunium/sdk#665): the start
// used to refuse it from the engine, with neither. Nothing is removed.
func TestAnInterruptedInstallANewerReleaseWroteIsRefusedWithTheRemedy(t *testing.T) {
	c := cutDir(t)
	m := c.interruptInstall(t, func(m *sdk.SnapshotMetaValue) { m.ClusterVersion = math.MaxUint32 })
	meta := filepath.Join(c.dir, "snap", m.ID+".meta")
	before, err := os.ReadFile(meta)
	require.NoError(t, err)

	refused := func(err error, what string) {
		t.Helper()
		require.ErrorIs(t, err, consensus.ErrVersionUnsupported, what)
		assert.ErrorContains(t, err, "the data directory "+c.dir+" holds what a newer release wrote", what)
	}
	_, err = driver{}.Inspect(c.dir)
	refused(err, "cluster inspect")
	_, err = driver{}.Open(context.Background(), consensus.Config{DataDir: c.dir, Slot: "a", ListenAddress: "127.0.0.1:0"})
	refused(err, "the node's start")

	after, err := os.ReadFile(meta)
	require.NoError(t, err, "the snapshot's metadata is kept")
	assert.Equal(t, before, after)
	_, err = os.Stat(filepath.Join(c.dir, "snap", m.ID+".snap"))
	require.NoError(t, err, "the snapshot is kept")
}
