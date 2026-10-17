// Copyright 2026 The Forgejo Authors. All rights reserved.
// SPDX-License-Identifier: GPL-3.0-or-later

package private

import (
	"net/http"
	"strings"
	"testing"

	"github.com/go-chi/chi/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// The ref replicator's proposals and operations never travel through
// /api/internal: the hook reaches it on its private socket, the operator
// through the site administration (kitsunium ADR 0152 D17, D19, R3-14).
func TestInternalRoutesHaveNoClusterRoute(t *testing.T) {
	var routes []string
	err := chi.Walk(Routes().R, func(method, route string, _ http.Handler, _ ...func(http.Handler) http.Handler) error {
		routes = append(routes, route)
		return nil
	})
	require.NoError(t, err)
	require.NotEmpty(t, routes)
	for _, r := range routes {
		assert.NotContains(t, strings.ToLower(r), "cluster", r)
		assert.NotContains(t, strings.ToLower(r), "consensus", r)
	}
}
