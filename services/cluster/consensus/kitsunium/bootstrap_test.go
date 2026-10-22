// Copyright 2026 The Forgejo Authors. All rights reserved.
// SPDX-License-Identifier: GPL-3.0-or-later

package kitsunium

import (
	"context"
	"errors"
	"testing"

	"forgejo.org/services/cluster/consensus"

	sdk "github.com/kitsunium/sdk/pkg/v1/app/consensus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// failingAppend is a log whose disk fails at the founding entry.
type failingAppend struct{ sdk.LogStore }

func (failingAppend) Append([]sdk.EntryValue, *sdk.HardState) error {
	return errors.New("no space left on device")
}

// A Bootstrap stopped between its pending stamp and its member stamp is
// reported as such, and runs again to its end with the same list (#62).
func TestAnInterruptedBootstrapIsReportedAndResumes(t *testing.T) {
	dir := t.TempDir()
	cluster := consensus.ClusterID{0x62}
	id, inc, err := driver{}.Init(dir, cluster, "a")
	require.NoError(t, err)
	self := consensus.Member{ID: id, Incarnation: inc, Slot: "a", SPKI: [32]byte{1}, Address: "127.0.0.1:7001", Voter: true}

	_, err = withDir(dir, func(d sdk.DataDir) (struct{}, error) {
		rep, err := sdk.Inspect(d)
		require.NoError(t, err)
		sc := sdk.Config{
			Cluster: rep.Stamp.Log.Cluster, ID: rep.Stamp.ID, Machine: Machine(nopMachine{}),
			Durability: sdk.DurabilityVolatile, Log: failingAppend{d.Log()}, Snapshots: d.Snapshots(), MinVoters: 1,
		}
		return struct{}{}, sdk.Bootstrap(context.Background(), &sc, []sdk.MemberValue{sdkMember(self)})
	})
	require.Error(t, err, "the founding entry failed")
	rep, err := driver{}.Inspect(dir)
	require.NoError(t, err)
	assert.True(t, rep.BootstrapPending)
	assert.False(t, rep.Joining)

	require.NoError(t, driver{}.Bootstrap(context.Background(), consensus.Config{DataDir: dir, MinVoters: 1}, []consensus.Member{self}))
	rep, err = driver{}.Inspect(dir)
	require.NoError(t, err)
	assert.False(t, rep.BootstrapPending)
	assert.False(t, rep.Joining)
	assert.Equal(t, uint64(1), rep.LastIndex, "the founding entry")
}
