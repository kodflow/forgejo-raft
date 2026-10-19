// Copyright 2026 The Forgejo Authors. All rights reserved.
// SPDX-License-Identifier: GPL-3.0-or-later

package cmd

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"forgejo.org/modules/clusterhook"
	"forgejo.org/modules/git/pushoptions"
	repo_module "forgejo.org/modules/repository"
	"forgejo.org/modules/setting"

	"github.com/urfave/cli/v3"
)

func subcmdHookReferenceTransaction() *cli.Command {
	return &cli.Command{
		Name:        "reference-transaction",
		Usage:       "Delegate reference-transaction Git hook to the cluster replicator",
		Description: "This command should only be called by Git",
		Action:      runHookReferenceTransaction,
	}
}

// runHookReferenceTransaction hands one phase of a git ref transaction to the
// local replicator over its private socket (kitsunium ADR 0152 D19). In
// "prepared" it fails closed: no answer, no ref. The other phases only
// release the registration, and never fail git.
func runHookReferenceTransaction(ctx context.Context, c *cli.Command) error {
	state := c.Args().First()
	if state == "preparing" {
		return nil
	}
	setup(ctx, false, false)
	if !setting.Cluster.Enabled {
		return nil
	}

	gitDir := os.Getenv("GIT_DIR")
	if gitDir == "" {
		gitDir, _ = os.Getwd()
	}
	if !filepath.IsAbs(gitDir) {
		wd, _ := os.Getwd()
		gitDir = filepath.Join(wd, gitDir)
	}
	// Compare real paths: git reports its directory with symbolic links
	// resolved (/tmp is /private/tmp on macOS), the setting may not.
	gitDir = realPath(gitDir)
	root := realPath(setting.RepoRootPath)
	// Temporary clones (merges, web edits) are not replicated: only what
	// lands in a repository under the root is (spike finding).
	if rel, err := filepath.Rel(root, gitDir); err != nil || strings.HasPrefix(rel, "..") {
		return nil
	}

	req := &clusterhook.Request{
		Op:     state,
		Nonce:  os.Getenv(clusterhook.EnvTxn),
		GitDir: gitDir,
		PID:    os.Getppid(),
	}
	req.PusherID, _ = strconv.ParseInt(os.Getenv(repo_module.EnvPusherID), 10, 64)
	req.PusherName = os.Getenv(repo_module.EnvPusherName)
	req.RepoID, _ = strconv.ParseInt(os.Getenv(repo_module.EnvRepoID), 10, 64)
	req.PRID, _ = strconv.ParseInt(os.Getenv(repo_module.EnvPRID), 10, 64)
	req.IsWiki, _ = strconv.ParseBool(os.Getenv(repo_module.EnvRepoIsWiki))
	req.Trigger = os.Getenv(repo_module.EnvPushTrigger)
	if state == clusterhook.OpPrepared {
		req.PushOptions = pushoptions.New().ReadEnv().Map()
	}

	lines, err := clusterhook.ParseLines(os.Stdin)
	if err != nil {
		if state != clusterhook.OpPrepared {
			// Nothing to refuse any more: the registration expires.
			return nil
		}
		fmt.Fprintln(os.Stderr, "Forgejo: the cluster cannot replicate this ref transaction, nothing was written:", err)
		return cli.Exit("", 1)
	}
	req.Lines = lines
	if len(req.Lines) == 0 {
		return nil
	}

	ctx, cancel := context.WithTimeout(ctx, setting.Cluster.HookTimeout)
	defer cancel()
	resp, err := clusterhook.Call(ctx, setting.Cluster.HookSocket, req)
	if state != clusterhook.OpPrepared {
		return nil
	}
	if err != nil {
		// The proposal may still commit after the hook gave up, and the
		// projector would then write the ref: the outcome is unknown.
		fmt.Fprintln(os.Stderr, "Forgejo: the cluster did not answer in time; the push may or may not have been applied. Fetch before retrying:", err)
		return cli.Exit("", 1)
	}
	if !resp.OK {
		fmt.Fprintln(os.Stderr, "Forgejo:", resp.Message)
		return cli.Exit("", 1)
	}
	return nil
}

// realPath resolves symbolic links, falling back to the cleaned path.
func realPath(p string) string {
	if r, err := filepath.EvalSymlinks(p); err == nil {
		return r
	}
	return filepath.Clean(p)
}
