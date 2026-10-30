// Copyright 2026 The Forgejo Authors. All rights reserved.
// SPDX-License-Identifier: GPL-3.0-or-later

package cmd

import (
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// git takes a ref name that is not valid UTF-8, which the cluster cannot
// carry: the production hook refuses the whole transaction that holds
// one, in "prepared", before it asks the replicator, and git writes none
// of its refs. A pull mirror's fetch that brought such a name from its
// upstream was refused whole, its other refs with it, at every sync
// (#365): the sync leaves the name out instead (services/mirror).
func TestClusterHookRefusesATransactionWithANameNotUTF8(t *testing.T) {
	e := newClusterHookEnv(t)
	up := filepath.Join(e.base, "upstream.git")
	e.git(t, e.base, "init", "-q", "--bare", up)
	bad := "refs/heads/caf\xe9"
	e.git(t, e.work, "push", "-q", up, "HEAD:refs/heads/main", "HEAD:"+bad)

	out, err := e.run(e.bare, "fetch", up, "+refs/heads/*:refs/heads/*")
	t.Logf("fetch with a name that is not UTF-8: err=%v\n%s", err, out)
	require.Error(t, err)
	assert.Contains(t, out, "not valid UTF-8")
	assert.False(t, e.refExists(e.bare, "refs/heads/main"), "git wrote the other ref of the transaction")
	assert.Empty(t, e.rep.prepared(), "the hook asked the replicator")

	// Control: without it, the fetch reaches the replicator, which refuses.
	e.git(t, up, "update-ref", "-d", bad)
	out, err = e.run(e.bare, "fetch", up, "+refs/heads/*:refs/heads/*")
	require.Error(t, err, out)
	assert.Contains(t, out, "refused by the test replicator")
	assert.Len(t, e.rep.prepared(), 1)
}
