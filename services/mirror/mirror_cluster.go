// Copyright 2026 The Forgejo Authors. All rights reserved.
// SPDX-License-Identifier: GPL-3.0-or-later

package mirror

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"net/url"
	"strings"
	"time"

	repo_model "forgejo.org/models/repo"
	"forgejo.org/modules/clusterhook"
	"forgejo.org/modules/git"
	giturl "forgejo.org/modules/git/url"
	repo_module "forgejo.org/modules/repository"
	"forgejo.org/services/cluster/reftable"
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
	return nil
}

// wikiRemoteURL asks a remote for its wiki's address; tests replace it.
var wikiRemoteURL = repo_module.WikiRemoteURL

// ensurePullWikiRemote recreates, in a cluster, the remote of a pull
// mirror's wiki. Its address is found by asking the remote, as the
// migration did, so it runs only once the mirror's address passed the
// allow and block lists again, and asks with authAddr, the address with
// its credentials: a private wiki answers no other. The git config keeps
// addr's form, without the password. A wiki the remote does not serve
// keeps no remote.
func ensurePullWikiRemote(ctx context.Context, m *repo_model.Mirror, authAddr, addr string) error {
	if !clusterhook.Enabled() || addr == "" || !m.Repo.HasWiki() {
		return nil
	}
	name := m.GetRemoteName()
	wikiPath := m.Repo.WikiPath()
	if cur, err := remoteURLOf(ctx, wikiPath, name); err != nil || cur != "" {
		return err
	}
	found := wikiRemoteURL(ctx, authAddr)
	suffix, ok := strings.CutPrefix(found, strings.TrimSuffix(authAddr, ".git"))
	if found == "" || !ok {
		return nil
	}
	return addPullRemote(ctx, wikiPath, name, strings.TrimSuffix(addr, ".git")+suffix)
}

// ensurePushMirrorRemote recreates, in a cluster, the remote of a push
// mirror a node lacks, from the address SQL holds, without its password:
// the push passes it through a credential helper (pushCredentialURL).
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
		// The wiki's address is found by asking the remote: only once the
		// address passed the allow and block lists again, and with the
		// mirror's password, which a private wiki requires. The git
		// config keeps the address without it.
		u, err := giturl.Parse(addr)
		if err != nil {
			return err
		}
		if err := recheckPushPermitted(ctx, m, u); err != nil {
			return err
		}
		ask, err := pushCredentialURL(ctx, m, u, true)
		if err != nil {
			return err
		}
		if ask == "" {
			ask = addr
		}
		found := wikiRemoteURL(ctx, ask)
		suffix, ok := strings.CutPrefix(found, strings.TrimSuffix(ask, ".git"))
		if found == "" || !ok {
			return nil
		}
		addr = strings.TrimSuffix(addr, ".git") + suffix
	}
	return addPushRemoteAndConfig(ctx, m, addr, path)
}

// hasPassword reports whether u carries a password.
func hasPassword(u *url.URL) bool {
	if u == nil || u.User == nil {
		return false
	}
	_, ok := u.User.Password()
	return ok
}

// pushCredentialURL returns, in a cluster, remoteURL with the credentials
// SQL keeps for the push mirror m, for a credential helper, or "" when
// there are none to add. A remote recreated from SQL has no password, so
// a push mirror authenticated by one pushed only from the node that
// created it (#79). That node's remote carries the password, as Forgejo
// writes it: a push mirror from before EncryptedRemoteAddress has it
// stored from there, for the other nodes. The credentials go to the host
// they were given for only.
func pushCredentialURL(ctx context.Context, m *repo_model.PushMirror, remoteURL *giturl.GitURL, isWiki bool) (string, error) {
	if !clusterhook.Enabled() || remoteURL == nil || remoteURL.URL == nil {
		return "", nil
	}
	if hasPassword(remoteURL.URL) {
		if m.EncryptedRemoteAddress == nil && !isWiki {
			if err := m.SetEncryptedRemoteAddress(ctx, remoteURL.URL.String()); err != nil {
				return "", err
			}
		}
		return "", nil
	}
	maybe, err := m.DecryptRemoteAddress()
	if err != nil {
		return "", err
	}
	has, addr := maybe.Get()
	if !has {
		return "", nil
	}
	stored, err := url.Parse(addr)
	if err != nil {
		return "", err
	}
	if !hasPassword(stored) || stored.Scheme != remoteURL.Scheme || !strings.EqualFold(stored.Host, remoteURL.Host) {
		return "", nil
	}
	authed := *remoteURL.URL
	authed.User = stored.User
	return authed.String(), nil
}

// syncRefBatch bounds the refs one git command of a mirror's sync writes in
// a cluster, as the clone's batches do (services/repository).
const syncRefBatch = 512

// syncInBatches runs the fetch of a mirror's sync by batches in a cluster
// when it must (fetchInBatches), and writes its output to out. credURL, if
// set, authenticates the fetch as the sync's own; tags adds --tags. It is
// false, and nothing was written, when the sync's own fetch is to run.
func syncInBatches(ctx context.Context, m *repo_model.Mirror, credURL, dir string, prune, tags bool, timeout time.Duration, env []string, out *strings.Builder) (bool, error) {
	if !clusterhook.Enabled() {
		return false, nil
	}
	dry := git.NewCommand(ctx)
	if credURL != "" {
		_, cleanup, err := dry.AddAuthCredentialHelperForRemote(credURL)
		if err != nil {
			return false, err
		}
		defer cleanup()
	}
	dry.AddArguments("fetch", "--dry-run", "--porcelain")
	if prune {
		dry.AddArguments("--prune")
	}
	if tags {
		dry.AddArguments("--tags")
	}
	dry.AddDynamicArguments(m.GetRemoteName())
	output, batched, err := fetchInBatches(ctx, dry, dir, timeout, env)
	out.WriteString(output)
	return batched, err
}

// fetchInBatches runs, in a cluster, the fetch of a mirror's sync when it
// would update more refs than the cluster takes in one transaction. git
// 2.54 and later write a whole fetch in one transaction, which the cluster
// refuses past reftable.MaxUpdatesPerTxn: the sync failed, and every later
// one fetched the same refs and failed the same way (#134). dry is the
// sync's fetch with --dry-run --porcelain, which brings the objects and
// lists the updates without writing a ref. Past the bound, the updates are
// written by batches of syncRefBatch, each checked against the old value
// the fetch read, and the output is the fetch's own, so the sync reports
// the same results; batched is false when the sync's fetch can run as is.
func fetchInBatches(ctx context.Context, dry *git.Command, dir string, timeout time.Duration, env []string) (output string, batched bool, err error) {
	if !clusterhook.Enabled() {
		return "", false, nil
	}
	var stdout, stderr bytes.Buffer
	if err := dry.Run(&git.RunOpts{Dir: dir, Timeout: timeout, Env: env, Stdout: &stdout, Stderr: &stderr}); err != nil {
		// The sync's own fetch fails the same way and reports it.
		return "", false, nil
	}
	updates, ok := parsePorcelainFetch(stdout.String())
	if !ok || len(updates) <= reftable.MaxUpdatesPerTxn {
		return "", false, nil
	}
	var out strings.Builder
	for len(updates) > 0 {
		n := min(len(updates), syncRefBatch)
		var in bytes.Buffer
		for _, u := range updates[:n] {
			switch u.flag {
			case '*':
				fmt.Fprintf(&in, "create %s %s\n", u.ref, u.newID)
			case '-':
				fmt.Fprintf(&in, "delete %s %s\n", u.ref, u.oldID)
			default:
				fmt.Fprintf(&in, "update %s %s %s\n", u.ref, u.newID, u.oldID)
			}
		}
		if _, _, err := git.NewCommand(ctx, "update-ref", "--stdin").
			SetDescription(fmt.Sprintf("fetchInBatches(git update-ref): %s", dir)).
			RunStdString(&git.RunOpts{Dir: dir, Timeout: timeout, Stdin: &in}); err != nil {
			return "", true, fmt.Errorf("git update-ref: %w", err)
		}
		for _, u := range updates[:n] {
			out.WriteString(u.line())
		}
		updates = updates[n:]
	}
	return out.String(), true, nil
}

// porcelainUpdate is one line of `git fetch --porcelain`.
type porcelainUpdate struct {
	flag              byte
	oldID, newID, ref string
}

// line writes the update as git fetch writes it without --porcelain, for
// parseRemoteUpdateOutput.
func (u porcelainUpdate) line() string {
	switch u.flag {
	case '*':
		return fmt.Sprintf(" * [new ref]         %s -> %s\n", u.ref, u.ref)
	case '-':
		return fmt.Sprintf(" - [deleted]         (none)     -> %s\n", u.ref)
	case ' ':
		return fmt.Sprintf("   %s..%s  %s -> %s\n", u.oldID, u.newID, u.ref, u.ref)
	default:
		return fmt.Sprintf(" + %s...%s %s -> %s  (forced update)\n", u.oldID, u.newID, u.ref, u.ref)
	}
}

// parsePorcelainFetch reads `git fetch --porcelain`: "<flag> <old> <new>
// <ref>" per line. ok is false on a rejected ref or a line it cannot read:
// the sync's own fetch then runs and reports it.
func parsePorcelainFetch(s string) (updates []porcelainUpdate, ok bool) {
	sc := bufio.NewScanner(strings.NewReader(s))
	sc.Buffer(make([]byte, 0, 64*1024), 1024*1024)
	for sc.Scan() {
		line := sc.Text()
		if line == "" {
			continue
		}
		f := strings.SplitN(line[min(len(line), 2):], " ", 3)
		if len(line) < 2 || line[1] != ' ' || len(f) != 3 {
			return nil, false
		}
		u := porcelainUpdate{flag: line[0], oldID: f[0], newID: f[1], ref: f[2]}
		switch u.flag {
		case '=':
			continue
		case ' ', '+', '-', 't', '*':
		default:
			return nil, false
		}
		if !strings.HasPrefix(u.ref, "refs/") {
			return nil, false
		}
		updates = append(updates, u)
	}
	return updates, sc.Err() == nil
}
