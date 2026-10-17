// Copyright 2026 The Forgejo Authors. All rights reserved.
// SPDX-License-Identifier: GPL-3.0-or-later

package consensus

import (
	"encoding/json" //nolint:depguard // the table encodes these with the standard library
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Identifiers print as hex and read back, so snapshots and events that
// carry them survive a round trip.
func TestIdentifiersRoundTripAsText(t *testing.T) {
	type all struct {
		ID  NodeID
		Inc Incarnation
		C   ClusterID
		Tok Token
	}
	in := all{ID: 0xabc, Inc: Incarnation{1, 2}, C: ClusterID{3}, Tok: Token{Cluster: ClusterID{4}, Epoch: 1, Term: 2, Index: 3}}
	b, err := json.Marshal(in)
	require.NoError(t, err)
	assert.Contains(t, string(b), `"ID":"0000000000000abc"`)
	var out all
	require.NoError(t, json.Unmarshal(b, &out))
	assert.Equal(t, in, out)
}
