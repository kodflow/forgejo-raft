// Copyright 2026 The Forgejo Authors. All rights reserved.
// SPDX-License-Identifier: GPL-3.0-or-later

package forgejo_migrations

import (
	"testing"

	migration_tests "forgejo.org/models/gitea_migrations/test"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// The migration creates action_deferred_notification, where a node of a
// cluster defers the Actions events it cannot resolve fresh, and running
// it again changes nothing.
func Test_addActionDeferredNotification(t *testing.T) {
	x, deferable := migration_tests.PrepareTestEnv(t, 0)
	defer deferable()
	if x == nil || t.Failed() {
		return
	}

	require.NoError(t, addActionDeferredNotification(x))
	require.NoError(t, addActionDeferredNotification(x))

	_, err := x.Exec("INSERT INTO action_deferred_notification (repo_id, doer_id, event, ref, payload) VALUES (1, 2, 'issue_comment', '', '{}')")
	require.NoError(t, err)
	rows, err := x.QueryString("SELECT repo_id, doer_id, event, pull_request_id, attempts FROM action_deferred_notification")
	require.NoError(t, err)
	require.Len(t, rows, 1)
	assert.Equal(t, "1", rows[0]["repo_id"])
	assert.Equal(t, "issue_comment", rows[0]["event"])
	assert.Equal(t, "0", rows[0]["pull_request_id"], "no pull request by default")
	assert.Equal(t, "0", rows[0]["attempts"])
}
