// Copyright 2026 The Forgejo Authors. All rights reserved.
// SPDX-License-Identifier: GPL-3.0-or-later

package cluster

import (
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"forgejo.org/modules/setting"
	"forgejo.org/modules/test"
	"forgejo.org/services/cluster/consensus"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Only a cookie this forge signed is read, and a token the node does not
// know committed is never waited for: the wait comes before authentication.
func TestReadAfterWriteCookieIsSignedAndBounded(t *testing.T) {
	defer test.MockVariableValue(&setting.SecretKey, "secret-a")()
	tok := consensus.Token{Cluster: consensus.ClusterID{1}, Epoch: 1, Term: 1, Index: 7}
	signed := signRawCookie(tok, "o/r.git")
	got, repo, ok := parseRawCookie(signed)
	require.True(t, ok)
	assert.Equal(t, tok, got)
	assert.Equal(t, "o/r.git", repo)

	forged := strings.Replace(signed, tok.String(), consensus.Token{Cluster: consensus.ClusterID{1}, Epoch: 1, Term: 1, Index: 1 << 62}.String(), 1)
	_, _, ok = parseRawCookie(forged)
	assert.False(t, ok, "an edited cookie is ignored")
	_, _, ok = parseRawCookie(tok.String() + ".o/r.git")
	assert.False(t, ok, "an unsigned cookie is ignored")
	func() {
		defer test.MockVariableValue(&setting.SecretKey, "secret-b")()
		_, _, ok = parseRawCookie(signed)
		assert.False(t, ok, "another forge's key")
	}()

	tc := newTestCluster(t, "node-a", "node-b", "node-c")
	r := tc.nodes[0].r
	far := consensus.Token{Cluster: tok.Cluster, Epoch: 1, Term: 1, Index: r.Engine().Status().Commit + 1000}
	start := time.Now()
	assert.False(t, r.waitBounded(httptest.NewRequest("GET", "/", nil), far, "o/r.git"))
	assert.Less(t, time.Since(start), 100*time.Millisecond, "beyond the known commit: no wait")

	// Past the cap, a request does not wait either.
	for range cap(rawWaiters) {
		rawWaiters <- struct{}{}
	}
	defer func() {
		for range cap(rawWaiters) {
			<-rawWaiters
		}
	}()
	near := consensus.Token{Cluster: tok.Cluster, Epoch: 1, Term: 1, Index: r.Engine().Status().Commit}
	start = time.Now()
	assert.False(t, r.waitBounded(httptest.NewRequest("GET", "/", nil), near, "o/r.git"))
	assert.Less(t, time.Since(start), 100*time.Millisecond)
}
