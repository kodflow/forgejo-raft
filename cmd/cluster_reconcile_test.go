// Copyright 2026 The Forgejo Authors. All rights reserved.
// SPDX-License-Identifier: GPL-3.0-or-later

package cmd

import (
	"context"
	"testing"

	"forgejo.org/modules/clusterhook"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/urfave/cli/v3"
)

// `forgejo cluster reconcile` reports and rebuilds rows; deleting an
// orphaned repository is its own explicit operation, for one repository,
// with the index the report gave and a reason.
func TestReconcileRequest(t *testing.T) {
	var flags []cli.Flag
	for _, c := range cmdCluster().Commands {
		if c.Name == "reconcile" {
			flags = c.Flags
		}
	}
	require.NotEmpty(t, flags)
	run := func(args ...string) (*clusterhook.Request, error) {
		var req *clusterhook.Request
		var rerr error
		cmd := &cli.Command{
			Name: "reconcile", Flags: flags,
			Action: func(_ context.Context, c *cli.Command) error {
				req, rerr = reconcileRequest(c)
				return nil
			},
		}
		require.NoError(t, cmd.Run(context.Background(), append([]string{"reconcile"}, args...)))
		return req, rerr
	}

	req, err := run()
	require.NoError(t, err)
	assert.True(t, req.DryRun)
	assert.Empty(t, req.Repo)
	req, err = run("--apply")
	require.NoError(t, err)
	assert.False(t, req.DryRun)
	assert.Empty(t, req.Repo, "--apply never deletes a repository")

	_, err = run("--delete-orphan", "o/r", "--reason", "x")
	require.ErrorContains(t, err, "--expect-index is required")
	_, err = run("--delete-orphan", "o/r", "--expect-index", "7")
	require.ErrorContains(t, err, "--reason is required")
	_, err = run("--delete-orphan", "o/r", "--expect-index", "7", "--reason", "x", "--apply")
	require.ErrorContains(t, err, "its own operation")
	req, err = run("--delete-orphan", "o/r", "--expect-index", "7", "--reason", "creation failed on 2026-10-19")
	require.NoError(t, err)
	assert.Equal(t, clusterhook.OpReconcile, req.Op)
	assert.Equal(t, "o/r", req.Repo)
	assert.EqualValues(t, 7, req.Index)
	assert.Equal(t, "creation failed on 2026-10-19", req.Reason)
	assert.NotEmpty(t, req.Operator)
}
