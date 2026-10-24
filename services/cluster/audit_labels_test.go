// Copyright 2026 The Forgejo Authors. All rights reserved.
// SPDX-License-Identifier: GPL-3.0-or-later

package cluster

import (
	"os"
	"path/filepath"
	"testing"

	"forgejo.org/modules/json"
	"forgejo.org/services/cluster/reftable"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// The audit records the cluster writes by itself show on the status page
// with a label of their own, and their outcomes are not shown as failures:
// the raise of the payload version read "failed" in red under a raw
// locale key (CodeRabbit on #24).
func TestAuditRecordsOfTheClusterShowOnTheStatusPage(t *testing.T) {
	root := filepath.Join("..", "..")
	for _, lang := range []string{"en-US", "fr-FR"} {
		b, err := os.ReadFile(filepath.Join(root, "options", "locale_next", "locale_"+lang+".json"))
		require.NoError(t, err)
		var keys map[string]any
		require.NoError(t, json.Unmarshal(b, &keys))
		for _, action := range []string{reftable.AuditPayloadVersion, reftable.AuditRevokeKey, string(ActionRaiseCluster)} {
			assert.Contains(t, keys, "admin.cluster.op."+action, lang)
		}
		for _, outcome := range []string{reftable.AuditRaised, reftable.AuditPending} {
			assert.Contains(t, keys, "admin.cluster.outcome."+outcome, lang)
		}
	}
	tmpl, err := os.ReadFile(filepath.Join(root, "templates", "admin", "cluster", "status.tmpl"))
	require.NoError(t, err)
	for _, outcome := range []string{reftable.AuditRaised, reftable.AuditPending} {
		assert.Contains(t, string(tmpl), `eq .Outcome "`+outcome+`"`, "the template labels the outcome %q", outcome)
	}
}
