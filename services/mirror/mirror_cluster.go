// Copyright 2026 The Forgejo Authors. All rights reserved.
// SPDX-License-Identifier: GPL-3.0-or-later

package mirror

import (
	"context"
	"fmt"
	"strings"

	repo_model "forgejo.org/models/repo"
	"forgejo.org/modules/clusterhook"
	"forgejo.org/modules/git"
	repo_module "forgejo.org/modules/repository"
)

// In a cluster each node holds its own copy of a repository, and the
// cluster replicates its refs and objects, never its git config. A
// mirror's remote lived only on the node that created it: the mirror
// failed on every other node, and on all of them once that node's disk was
// rebuilt (#79). Before each mirror operation, a node in a cluster
// recreates the remote from SQL when it lacks it.

// remoteURLOf returns the URL of the remote name in the repository at path,
// "" when there is no such remote.
func remoteURLOf(ctx context.Context, path, name string) (string, error) {
	stdout, _, err := git.NewCommand(ctx, "config", "--get").AddDynamicArguments("remote." + name + ".url").RunStdString(&git.RunOpts{Dir: path})
	if err != nil {
		if git.IsErrorExitCode(err, 1) {
			return "", nil
		}
		return "", err
	}
	return strings.TrimSpace(stdout), nil
}

// addPullRemote adds the remote of a pull mirror as `git clone --mirror`
// and the migration write it: every ref fetched under its own name, and
// the tags.
func addPullRemote(ctx context.Context, path, name, addr string) error {
	if _, _, err := git.NewCommand(ctx, "remote", "add", "--mirror=fetch").AddDynamicArguments(name, addr).RunStdString(&git.RunOpts{Dir: path}); err != nil {
		return fmt.Errorf("git remote add %s: %w", name, err)
	}
	for _, kv := range [][2]string{{"remote." + name + ".mirror", "true"}, {"remote." + name + ".fetch", "+refs/tags/*:refs/tags/*"}} {
		if _, _, err := git.NewCommand(ctx, "config", "--add").AddDynamicArguments(kv[0], kv[1]).RunStdString(&git.RunOpts{Dir: path}); err != nil {
			return fmt.Errorf("git config --add %s: %w", kv[0], err)
		}
	}
	return nil
}

// setPullRemoteURL points the pull mirror's remote at addr. In a cluster
// a node that lacks the remote adds it.
func setPullRemoteURL(ctx context.Context, path, name, addr string) error {
	if clusterhook.Enabled() {
		cur, err := remoteURLOf(ctx, path, name)
		if err != nil {
			return err
		}
		if cur == "" {
			if addr == "" {
				return nil
			}
			return addPullRemote(ctx, path, name, addr)
		}
	}
	_, _, err := git.NewCommand(ctx, "remote", "set-url").AddDynamicArguments(name, addr).RunStdString(&git.RunOpts{Dir: path})
	return err
}

// ensurePullMirrorRemote recreates, in a cluster, the remote of a pull
// mirror from the address SQL holds, without its password, which the sync
// passes through a credential helper. A remote another address replaced
// on another node follows SQL. A mirror from before the address was kept
// in SQL has no other source than the git config, and is left as is.
func ensurePullMirrorRemote(ctx context.Context, m *repo_model.Mirror) error {
	if !clusterhook.Enabled() {
		return nil
	}
	maybe, err := m.SanitizedRemoteAddress()
	if err != nil {
		return err
	}
	has, addr := maybe.Get()
	if !has {
		return nil
	}
	name := m.GetRemoteName()
	path := m.Repo.RepoPath()
	cur, err := remoteURLOf(ctx, path, name)
	if err != nil {
		return err
	}
	switch {
	case cur == "":
		if err := addPullRemote(ctx, path, name, addr); err != nil {
			return err
		}
	case cur != addr:
		if err := setPullRemoteURL(ctx, path, name, addr); err != nil {
			return err
		}
	}
	if !m.Repo.HasWiki() {
		return nil
	}
	wikiPath := m.Repo.WikiPath()
	if cur, err := remoteURLOf(ctx, wikiPath, name); err != nil || cur != "" {
		return err
	}
	// The wiki's address is found by asking the remote, as the migration
	// did; a wiki the remote does not serve keeps no remote.
	if wikiAddr := repo_module.WikiRemoteURL(ctx, addr); wikiAddr != "" {
		return addPullRemote(ctx, wikiPath, name, wikiAddr)
	}
	return nil
}

// ensurePushMirrorRemote recreates, in a cluster, the remote of a push
// mirror a node lacks, from the address SQL holds. SQL keeps no password:
// a push mirror authenticated by one pushes from the nodes that hold its
// remote, an SSH key or an address without credentials from every node.
func ensurePushMirrorRemote(ctx context.Context, m *repo_model.PushMirror, path string, isWiki bool) error {
	if !clusterhook.Enabled() || m.RemoteAddress == "" {
		return nil
	}
	cur, err := remoteURLOf(ctx, path, m.RemoteName)
	if err != nil || cur != "" {
		return err
	}
	addr := m.RemoteAddress
	if isWiki {
		if addr = repo_module.WikiRemoteURL(ctx, addr); addr == "" {
			return nil
		}
	}
	return addPushRemoteAndConfig(ctx, m, addr, path)
}
