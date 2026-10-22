// Copyright 2026 The Forgejo Authors. All rights reserved.
// SPDX-License-Identifier: GPL-3.0-or-later

package repository

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"forgejo.org/modules/git"
	repo_module "forgejo.org/modules/repository"
)

// clusterRefBatch bounds the refs one git command writes in a cluster. git
// 2.54 and later make a single ref transaction of a fetch or a mirror
// clone, and the cluster takes at most 4096 refs in one transaction: a
// repository with more could never be forked nor migrated (#80). Batches
// of 512, like `forgejo cluster import`, stay well under the bound.
const clusterRefBatch = 512

// listRefs returns the refs of dir under patterns, every ref without one.
func listRefs(ctx context.Context, dir string, patterns ...string) ([]string, error) {
	cmd := git.NewCommand(ctx, "for-each-ref", "--format=%(refname)")
	if len(patterns) > 0 {
		cmd.AddDashesAndList(patterns...)
	}
	stdout, _, err := cmd.RunStdString(&git.RunOpts{Dir: dir})
	if err != nil {
		return nil, fmt.Errorf("git for-each-ref: %w", err)
	}
	return strings.Fields(stdout), nil
}

// fetchRefsInBatches fetches refs from the repository at from into the one
// at to, each under its own name, at most clusterRefBatch refs per git
// fetch. The refspecs go on stdin: thousands of them would overrun a
// command line.
func fetchRefsInBatches(ctx context.Context, from, to string, refs []string, timeout time.Duration) error {
	for len(refs) > 0 {
		n := min(len(refs), clusterRefBatch)
		var spec bytes.Buffer
		for _, ref := range refs[:n] {
			fmt.Fprintf(&spec, "+%s:%s\n", ref, ref)
		}
		refs = refs[n:]
		if stdout, _, err := git.NewCommand(ctx, "fetch", "--quiet", "--no-tags", "--stdin").AddDynamicArguments(from).
			SetDescription(fmt.Sprintf("fetchRefsInBatches(git fetch): %s to %s", from, to)).
			RunStdString(&git.RunOpts{Dir: to, Timeout: timeout, Stdin: &spec}); err != nil {
			return fmt.Errorf("git fetch: %w: %s", err, stdout)
		}
	}
	// A clone leaves no FETCH_HEAD; nothing reads this one.
	if err := os.Remove(filepath.Join(to, "FETCH_HEAD")); err != nil && !os.IsNotExist(err) {
		return err
	}
	return nil
}

// initBareInCluster runs `git init --bare` at to: in a cluster HEAD goes
// alone first, which ships no object and makes the key known to the
// replicated table before any ref brings objects.
func initBareInCluster(ctx context.Context, to, defaultBranch, objectFormat string) error {
	if err := os.MkdirAll(to, os.ModePerm); err != nil {
		return err
	}
	initCmd := git.NewCommand(ctx, "init", "--bare", "--template", "")
	if defaultBranch != "" {
		initCmd.AddOptionValues("--initial-branch", defaultBranch)
	}
	if git.SupportHashSha256 && objectFormat != "" {
		initCmd.AddOptionValues("--object-format", objectFormat)
	}
	if stdout, _, err := initCmd.SetDescription(fmt.Sprintf("initBareInCluster(git init): %s", to)).
		RunStdString(&git.RunOpts{Dir: to}); err != nil {
		return fmt.Errorf("git init: %w: %s", err, stdout)
	}
	return nil
}

// cloneMirrorInCluster does what `git clone --mirror` does, in a cluster:
// the clone lands in a temporary directory, outside the repository root
// and so not replicated, and its refs then come into to by batches, its
// remote configuration copied after them. A mirror clone writes every ref
// in one transaction, which the cluster refuses past its bound (#80).
func cloneMirrorInCluster(ctx context.Context, from, to string, opts git.CloneRepoOptions) error {
	staging, err := repo_module.CreateTemporaryPath("migrate")
	if err != nil {
		return err
	}
	defer func() { _ = repo_module.RemoveTemporaryPath(staging) }()
	if err := git.Clone(ctx, from, staging, opts); err != nil {
		return err
	}

	format, _, err := git.NewCommand(ctx, "rev-parse", "--show-object-format").RunStdString(&git.RunOpts{Dir: staging})
	if err != nil {
		return fmt.Errorf("git rev-parse --show-object-format: %w", err)
	}
	// A detached or unborn HEAD keeps git's default branch.
	head, _, _ := git.NewCommand(ctx, "symbolic-ref", "--quiet", "HEAD").RunStdString(&git.RunOpts{Dir: staging})
	if err := initBareInCluster(ctx, to, strings.TrimPrefix(strings.TrimSpace(head), git.BranchPrefix), strings.TrimSpace(format)); err != nil {
		return err
	}
	refs, err := listRefs(ctx, staging)
	if err != nil {
		return err
	}
	if err := fetchRefsInBatches(ctx, staging, to, refs, opts.Timeout); err != nil {
		return err
	}

	// The mirror's remote, its URL, refspec and mirror flag, as the clone
	// wrote it: the mirror's synchronisation reads it.
	remote, _, err := git.NewCommand(ctx, "config", "-z", "--get-regexp", `^remote\.`).RunStdString(&git.RunOpts{Dir: staging})
	if err != nil {
		return fmt.Errorf("git config --get-regexp remote: %w", err)
	}
	for entry := range strings.SplitSeq(remote, "\x00") {
		key, value, ok := strings.Cut(entry, "\n")
		if !ok {
			continue
		}
		// After "--": a value may start with a dash (tagopt is --no-tags).
		if _, _, err := git.NewCommand(ctx, "config", "--add").AddDashesAndList(key, value).RunStdString(&git.RunOpts{Dir: to}); err != nil {
			return fmt.Errorf("git config --add %s: %w", key, err)
		}
	}
	return nil
}
