// Copyright 2026 The Forgejo Authors. All rights reserved.
// SPDX-License-Identifier: GPL-3.0-or-later

package cluster

import (
	"context"
	"fmt"
	"path/filepath"
	"strings"
	"testing"

	"forgejo.org/modules/clusterhook"
	"forgejo.org/services/cluster/reftable"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// A transaction past the bound is refused before its objects go anywhere,
// with a reason that names the bound (#80). A fork and a migration stay
// within it: services/repository's TestHuntR4ProveBigTxnForkAndMirror.
func TestOversizedTransactionIsRefusedEarly(t *testing.T) {
	tc := newTestCluster(t, "node-a", "node-b", "node-c")
	n := tc.nodes[0]
	work := t.TempDir()
	run(t, work, "init", "-q")
	c1 := commit(t, work, "one")
	dir := filepath.Join(n.r.root, "big", "txn.git")
	run(t, n.r.root, "init", "-q", "--bare", dir)
	lines := make([]clusterhook.Line, reftable.MaxUpdatesPerTxn+1)
	for i := range lines {
		lines[i] = clusterhook.Line{Old: strings.Repeat("0", 40), New: c1, Ref: fmt.Sprintf("refs/tags/t%05d", i)}
	}
	resp := n.r.Handle(context.Background(), &clusterhook.Request{Op: clusterhook.OpPrepared, Nonce: clusterhook.NewNonce(), GitDir: dir, Lines: lines})
	require.False(t, resp.OK)
	assert.Contains(t, resp.Message, fmt.Sprintf("at most %d", reftable.MaxUpdatesPerTxn))
}
