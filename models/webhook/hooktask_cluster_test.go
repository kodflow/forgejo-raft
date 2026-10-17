// Copyright 2026 The Forgejo Authors. All rights reserved.
// SPDX-License-Identifier: GPL-3.0-or-later

package webhook

import (
	"testing"

	"forgejo.org/models/db"
	"forgejo.org/models/unittest"
	"forgejo.org/modules/clusterhook"
	webhook_module "forgejo.org/modules/webhook"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// A replayed command creates no second webhook task (kitsunium ADR 0152
// D19, R8-25).
func TestCreateHookTaskUnderACommandIsIdempotent(t *testing.T) {
	require.NoError(t, unittest.PrepareTestDatabase())
	ctx := clusterhook.WithCommandPart(clusterhook.WithCommand(db.DefaultContext, "0123/1/42"), "refs/heads/main")
	task := func() *HookTask {
		return &HookTask{HookID: 1, PayloadVersion: 2, EventType: webhook_module.HookEventPush, PayloadContent: "{}"}
	}
	a, err := CreateHookTask(ctx, task())
	require.NoError(t, err)
	b, err := CreateHookTask(ctx, task())
	require.NoError(t, err)
	assert.Equal(t, a.ID, b.ID)
	assert.Equal(t, a.UUID, b.UUID)

	// Another event of the same kind in the same command part (a second
	// pull request closed by one push) is another task.
	second := task()
	second.PayloadContent = `{"number":2}`
	e, err := CreateHookTask(ctx, second)
	require.NoError(t, err)
	assert.NotEqual(t, a.ID, e.ID)
	again := task()
	again.PayloadContent = `{"number":2}`
	f, err := CreateHookTask(ctx, again)
	require.NoError(t, err)
	assert.Equal(t, e.ID, f.ID, "and its replay finds it")

	// Another ref of the same command is another task.
	other, err := CreateHookTask(clusterhook.WithCommandPart(clusterhook.WithCommand(db.DefaultContext, "0123/1/42"), "refs/heads/dev"), task())
	require.NoError(t, err)
	assert.NotEqual(t, a.ID, other.ID)

	// Outside a command, every call is a new task.
	c, err := CreateHookTask(db.DefaultContext, task())
	require.NoError(t, err)
	d, err := CreateHookTask(db.DefaultContext, task())
	require.NoError(t, err)
	assert.NotEqual(t, c.UUID, d.UUID)
}
