// Copyright 2026 The Forgejo Authors. All rights reserved.
// SPDX-License-Identifier: GPL-3.0-or-later

package mirror

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"net/url"
	"os"
	"slices"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	repo_model "forgejo.org/models/repo"
	"forgejo.org/modules/clusterhook"
	"forgejo.org/modules/git"
	giturl "forgejo.org/modules/git/url"
	repo_module "forgejo.org/modules/repository"
	"forgejo.org/modules/util"
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
	url, _, err := remoteURLIn(ctx, path, name)
	return url, err
}

// remoteURLIn returns the URL of the remote name in the repository at
// path, and whether the remote exists: UpdateAddress gives a wiki's remote
// an empty URL when the new address serves no wiki, and a remote added
// again over it fails.
func remoteURLIn(ctx context.Context, path, name string) (url string, exists bool, err error) {
	stdout, _, err := git.NewCommand(ctx, "config", "--get").AddDynamicArguments("remote." + name + ".url").RunStdString(&git.RunOpts{Dir: path})
	if err != nil {
		if git.IsErrorExitCode(err, 1) {
			return "", false, nil
		}
		return "", false, err
	}
	return strings.TrimSpace(stdout), true, nil
}

// addPullRemote adds the remote of a pull mirror as `git clone --mirror`
// and the migration write it: every ref fetched under its own name, and
// the tags. check, if set, runs right before each git command, which it
// refuses with its error (pullGuard).
func addPullRemote(ctx context.Context, path, name, addr string, check func() error) error {
	if err := checkDir(check); err != nil {
		return err
	}
	if _, _, err := git.NewCommand(ctx, "remote", "add", "--mirror=fetch").AddDynamicArguments(name, addr).RunStdString(&git.RunOpts{Dir: path}); err != nil {
		return fmt.Errorf("git remote add %s: %w", name, err)
	}
	for _, kv := range [][2]string{{"remote." + name + ".mirror", "true"}, {"remote." + name + ".fetch", "+refs/tags/*:refs/tags/*"}} {
		if err := checkDir(check); err != nil {
			return err
		}
		if _, _, err := git.NewCommand(ctx, "config", "--add").AddDynamicArguments(kv[0], kv[1]).RunStdString(&git.RunOpts{Dir: path}); err != nil {
			return fmt.Errorf("git config --add %s: %w", kv[0], err)
		}
	}
	return nil
}

// checkDir runs check, nil when there is none.
func checkDir(check func() error) error {
	if check == nil {
		return nil
	}
	return check()
}

// pullRefusal is a pull mirror's sync this node of the cluster refused to
// run, or to go on with, in a directory that may hold another repository
// than the mirror's. The sync fails without writing anything more: it is
// not recorded as done, the mirror stays due, and the next round of
// update_mirrors, on any node, retries it, with the repository loaded
// again, under its new name if it was renamed.
type pullRefusal struct{ error }

func (e pullRefusal) Unwrap() error { return e.error }

func pullRefused(err error) error { return pullRefusal{err} }

// isPullRefusal reports whether err is a pullRefusal.
func isPullRefusal(err error) bool { return errors.As(err, new(pullRefusal)) }

// pullGuard is, in a cluster, the checks that the directories a pull
// mirror's sync works in on this node, of the repository and of its wiki,
// under the name the sync loaded at its start, hold the mirror's
// repository and nothing else.
//
// The sync loaded its repository once, and every later step worked on the
// directory under that name, past no check at all: the mirror's remote,
// recreated from SQL into its git config (#79), and the fetch, `--prune`
// with the mirror's refspec +refs/*:refs/*. A rename, or a deletion, that
// took the repository away and put another one, a private one, under its
// name, projected on this node meanwhile, put that one's directory there:
// the fetch rewrote and pruned its branches, and the cluster took the
// transaction on every node, its old values the table's, which rightly
// holds that repository under the name (#322, the pull side of #294).
//
// The first checks run before anything touches the directories: the
// repository's row, read again by its ID, must still give the name, and
// the directory must hold the repository (clusterhook.GuardDirectory).
// Then check runs right before each git command that writes into a
// directory, the remote's recreation, the fetch, each of its batches and
// a prune, and once the reads that follow the fetch are done: a rename,
// a transfer or a deletion that took the key or freed it, applied on this
// node since the first check, refuses the sync, as a directory that holds
// another repository now does. A git command that started in the
// mirror's directory keeps working in it when the projector moves it, its
// working directory following the move, and the cluster's hook names it
// under its new key. The commands that write refs carry the repository's
// ID (env), which the cluster refuses in a directory stamped with another
// one (Replicator.prepared): what the checks leave, the time from a check
// to the start of the next command, is covered there for every repository
// written with its ID.
//
// A repository without an ID, moved under the name and projected here
// before the first checks, the watch of renames starting after its move,
// while SQL still gave the mirror the name, cannot be: Forgejo moves a
// repository to a name only once SQL freed it (changeRepositoryName, a
// transfer, an owner's rename check the name in SQL before they propose
// the move), so the mirror's row had left the name in SQL before that
// move was proposed, and the first checks read it under its new name. A
// move applied after the first checks is still()'s. Without a cluster
// nothing is checked, and nothing is added to git's environment.
type pullGuard struct {
	repo, wiki *clusterhook.DirectoryGuard
	repoID     int64
}

// guardPullSync makes the first checks of a pull mirror's sync, before
// anything touches its directories.
func guardPullSync(ctx context.Context, m *repo_model.Mirror) (*pullGuard, error) {
	g := &pullGuard{repoID: m.Repo.ID}
	for _, isWiki := range []bool{false, true} {
		key := clusterhook.RepoKey(m.Repo.OwnerName, m.Repo.Name, isWiki)
		d, err := clusterhook.GuardDirectory(key, m.Repo.ID, func() error {
			return mirrorRepoStillNamed(ctx, m.Repo, key, isWiki, "pull mirror", pullRefused)
		}, func(err error) error {
			return pullRefused(fmt.Errorf("%w; the pull mirror's sync is retried", err))
		})
		if err != nil {
			return nil, err
		}
		if isWiki {
			g.wiki = d
		} else {
			g.repo = d
		}
	}
	return g, nil
}

// check is the check of the directory of the repository, or of its wiki,
// right before a git command writes there.
func (g *pullGuard) check(isWiki bool) error {
	if isWiki {
		return g.wiki.Check()
	}
	return g.repo.Check()
}

// checker is check as a function, for the helpers that run git.
func (g *pullGuard) checker(isWiki bool) func() error {
	return func() error { return g.check(isWiki) }
}

// environ is base, the environment of a git command that writes refs into
// the directory of the repository or of its wiki, with, in a cluster, the
// repository's ID and whether it is the wiki, which the cluster's hook
// gives the transaction. base nil is this process's environment.
func (g *pullGuard) environ(base []string, isWiki bool) []string {
	if !clusterhook.Enabled() {
		return base
	}
	if base == nil {
		base = os.Environ()
	}
	return append(base,
		repo_module.EnvRepoID+"="+strconv.FormatInt(g.repoID, 10),
		repo_module.EnvRepoIsWiki+"="+strconv.FormatBool(isWiki),
	)
}

// setPullRemoteURL points the pull mirror's remote at addr. In a cluster
// a node that lacks the remote adds it.
func setPullRemoteURL(ctx context.Context, path, name, addr string) error {
	if clusterhook.Enabled() {
		_, exists, err := remoteURLIn(ctx, path, name)
		if err != nil {
			return err
		}
		if !exists {
			if addr == "" {
				return nil
			}
			return addPullRemote(ctx, path, name, addr, nil)
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
// check runs right before each git command that writes the git config.
func ensurePullMirrorRemote(ctx context.Context, m *repo_model.Mirror, check func() error) error {
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
	cur, exists, err := remoteURLIn(ctx, path, name)
	if err != nil {
		return err
	}
	switch {
	case !exists:
		if err := addPullRemote(ctx, path, name, addr, check); err != nil {
			return err
		}
	case cur != addr:
		if err := checkDir(check); err != nil {
			return err
		}
		if _, _, err := git.NewCommand(ctx, "remote", "set-url").AddDynamicArguments(name, addr).RunStdString(&git.RunOpts{Dir: path}); err != nil {
			return err
		}
	}
	return nil
}

// wikiRemoteURL asks a remote for its wiki's address; tests replace it.
var wikiRemoteURL = repo_module.WikiRemoteURL

// ensurePullWikiRemote recreates, in a cluster, the remote of a pull
// mirror's wiki, and points it at the address SQL holds as
// ensurePullMirrorRemote does the repository's: an address changed
// through another node reached that node's git config only, and the
// wiki went on syncing from the old address everywhere else (#183). A
// remote whose URL is the wiki of addr follows SQL, and nothing is asked.
// Otherwise the wiki's address is found by asking the remote, as the
// migration did, so it runs only once the mirror's address passed the
// allow and block lists again, and asks with authAddr, the address with
// its credentials: a private wiki answers no other. The git config keeps
// addr's form, without the password. A wiki the remote does not serve
// keeps no remote, or an empty URL, as UpdateAddress writes it: never the
// old address. check runs right before each git command that writes the
// wiki's git config.
func ensurePullWikiRemote(ctx context.Context, m *repo_model.Mirror, authAddr, addr string, check func() error) error {
	if !clusterhook.Enabled() || addr == "" || !m.Repo.HasWiki() {
		return nil
	}
	name := m.GetRemoteName()
	wikiPath := m.Repo.WikiPath()
	cur, exists, err := remoteURLIn(ctx, wikiPath, name)
	if err != nil {
		return util.SanitizeErrorCredentialURLs(err)
	}
	if cur != "" && repo_module.IsWikiRemoteURLOf(cur, addr) {
		return nil
	}
	want := ""
	found := wikiRemoteURL(ctx, authAddr)
	if suffix, ok := strings.CutPrefix(found, strings.TrimSuffix(authAddr, ".git")); found != "" && ok {
		want = strings.TrimSuffix(addr, ".git") + suffix
	}
	switch {
	case !exists && want == "", exists && cur == want:
		return nil
	case !exists:
		err = addPullRemote(ctx, wikiPath, name, want, check)
	default:
		if err = checkDir(check); err == nil {
			_, _, err = git.NewCommand(ctx, "remote", "set-url").AddDynamicArguments(name, want).RunStdString(&git.RunOpts{Dir: wikiPath})
		}
	}
	if err != nil {
		return util.SanitizeErrorCredentialURLs(err)
	}
	return nil
}

// ensurePushMirrorRemote recreates, in a cluster, the remote of a push
// mirror a node lacks, from the address SQL holds, without its password:
// the push passes it through a credential helper (pushCredentialURL). A
// remote the node holds gets the push refspecs of the branch filter SQL
// holds: a filter changed through another node reached that node's git
// config only, and the push from every other node went on mirroring the
// branches the filter took out (#183).
func ensurePushMirrorRemote(ctx context.Context, m *repo_model.PushMirror, path string, isWiki bool) error {
	if !clusterhook.Enabled() || m.RemoteAddress == "" {
		return nil
	}
	_, exists, err := remoteURLIn(ctx, path, m.RemoteName)
	if err != nil {
		return err
	}
	if exists {
		return alignPushRefSpecs(ctx, m, path)
	}
	addr, err := pushRemoteAddress(ctx, m, isWiki)
	if err != nil || addr == "" {
		return err
	}
	return addPushRemoteAndConfig(ctx, m, addr, path)
}

// pushRemoteAddress returns the address a node that lacks the remote of
// the push mirror m recreates it with: the address SQL holds, without its
// password, or for the wiki the address the remote gives for its wiki, ""
// when it serves none.
func pushRemoteAddress(ctx context.Context, m *repo_model.PushMirror, isWiki bool) (string, error) {
	addr := m.RemoteAddress
	if !isWiki || addr == "" {
		return addr, nil
	}
	// The wiki's address is found by asking the remote: only once the
	// address passed the allow and block lists again, and with the
	// mirror's password, which a private wiki requires. The git config
	// keeps the address without it.
	u, err := giturl.Parse(addr)
	if err != nil {
		return "", err
	}
	if err := recheckPushPermitted(ctx, m, u); err != nil {
		return "", err
	}
	ask, err := pushCredentialURL(ctx, m, u, true)
	if err != nil {
		return "", err
	}
	if ask == "" {
		ask = addr
	}
	found := wikiRemoteURL(ctx, ask)
	suffix, ok := strings.CutPrefix(found, strings.TrimSuffix(ask, ".git"))
	if found == "" || !ok {
		return "", nil
	}
	return strings.TrimSuffix(addr, ".git") + suffix, nil
}

// alignPushRefSpecs rewrites the push refspecs of the push mirror's remote
// in the repository at path when they are not the ones its branch filter
// gives, and reads them only otherwise.
func alignPushRefSpecs(ctx context.Context, m *repo_model.PushMirror, path string) error {
	stdout, _, err := git.NewCommand(ctx, "config", "--get-all").AddDynamicArguments("remote." + m.RemoteName + ".push").RunStdString(&git.RunOpts{Dir: path})
	if err != nil && !git.IsErrorExitCode(err, 1) {
		return err
	}
	if slices.Equal(strings.Fields(stdout), pushRefSpecs(m.BranchFilter)) {
		return nil
	}
	if _, _, err := git.NewCommand(ctx, "config", "--unset-all").AddDynamicArguments("remote." + m.RemoteName + ".push").RunStdString(&git.RunOpts{Dir: path}); err != nil && !git.IsErrorExitCode(err, 5) {
		return err
	}
	return addRemotePushRefSpecs(ctx, path, m)
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
// env and check are fetchInBatches's.
func syncInBatches(ctx context.Context, m *repo_model.Mirror, credURL, dir string, prune, tags bool, timeout time.Duration, env []string, check func() error, out *strings.Builder) (bool, error) {
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
	output, batched, err := fetchInBatches(ctx, dry, dir, timeout, env, check)
	out.WriteString(output)
	return batched, err
}

// fetchInBatches runs, in a cluster, the fetch of a mirror's sync when it
// would update more refs than the cluster takes in one transaction, or a
// ref git rejects or the cluster cannot hold. git 2.54 and later write a
// whole fetch in one transaction, which the cluster refuses past
// reftable.MaxUpdatesPerTxn: the sync failed, and every later one fetched
// the same refs and failed the same way (#134). dry is the sync's fetch
// with --dry-run --porcelain, which brings the objects and lists the
// updates without writing a ref. Past the bound, the updates are written
// by batches of syncRefBatch, each checked against the old value the fetch
// read, and the output is the fetch's own, so the sync reports the same
// results; batched is false when the sync's fetch can run as is.
//
// That transaction tolerates a failure per ref (measured with git 2.51):
// git rejects a ref that conflicts as a directory and a file with a ref
// the mirror keeps (a, without prune, against its upstream's a/b), and
// writes the others. The reference-transaction hook reads the rejected ref
// too, and the cluster, which commits a transaction whole or not at all,
// refused all of it: git aborted the fetch, and every later sync did the
// same, the mirror behind its upstream for good on every node (#365).
// Such a ref, and one whose name the cluster cannot hold, are not written
// (refusedUpdates); the rest is, by batches, which git and the cluster
// both take whole or not at all, and the sync fails as git's own does
// outside a cluster, its error naming the refs left (errRefsNotUpdated).
// env is the environment of the fetch and of the batches, and check, if
// set, runs right before each batch, which it refuses with its error
// (pullGuard): the caller checks before the fetch.
func fetchInBatches(ctx context.Context, dry *git.Command, dir string, timeout time.Duration, env []string, check func() error) (output string, batched bool, err error) {
	if !clusterhook.Enabled() {
		return "", false, nil
	}
	var stdout, stderr bytes.Buffer
	if err := dry.Run(&git.RunOpts{Dir: dir, Timeout: timeout, Env: env, Stdout: &stdout, Stderr: &stderr}); err != nil {
		// The sync's own fetch fails the same way and reports it.
		return "", false, nil
	}
	updates, ok := parsePorcelainFetch(stdout.String())
	if !ok || len(updates) == 0 {
		return "", false, nil
	}
	refused, err := refusedUpdates(ctx, dir, updates)
	if err != nil {
		// The sync's own fetch reads the same directory and reports it.
		return "", false, nil
	}
	if len(refused) == 0 && len(updates) <= reftable.MaxUpdatesPerTxn {
		return "", false, nil
	}
	if len(refused) > 0 {
		left := make(map[string]bool, len(refused))
		for _, r := range refused {
			left[r.ref] = true
		}
		updates = slices.DeleteFunc(updates, func(u porcelainUpdate) bool { return left[u.ref] })
	}
	// git fetch prunes in a transaction of its own before it writes the
	// rest. In one batch, git refuses to create a/b while a, pruned in the
	// same batch, still exists, and every later sync failed the same way
	// (#149): the pruned refs come first, in batches of their own.
	rest := slices.DeleteFunc(slices.Clone(updates), func(u porcelainUpdate) bool { return u.flag == '-' })
	updates = append(slices.DeleteFunc(updates, func(u porcelainUpdate) bool { return u.flag != '-' }), rest...)
	pruned := len(updates) - len(rest)
	var out strings.Builder
	for len(updates) > 0 {
		n := min(len(updates), syncRefBatch)
		if pruned > 0 {
			n = min(n, pruned)
			pruned -= n
		}
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
		if err := checkDir(check); err != nil {
			return "", true, err
		}
		if _, _, err := git.NewCommand(ctx, "update-ref", "--stdin").
			SetDescription(fmt.Sprintf("fetchInBatches(git update-ref): %s", dir)).
			RunStdString(&git.RunOpts{Dir: dir, Timeout: timeout, Env: env, Stdin: &in}); err != nil {
			return "", true, fmt.Errorf("git update-ref: %w", err)
		}
		for _, u := range updates[:n] {
			out.WriteString(u.line())
		}
		updates = updates[n:]
	}
	if len(refused) > 0 {
		return out.String(), true, errRefsNotUpdated(refused)
	}
	return out.String(), true, nil
}

// refusedRef is an update of a mirror's sync that is not written, and why.
type refusedRef struct{ ref, why string }

// refusedUpdates returns the updates of a mirror's sync into the
// repository at dir that are not to be written. One is a name the cluster
// cannot hold (reftable.ValidRefName: not valid UTF-8, too long), which
// git takes: the reference-transaction hook refuses the whole transaction
// that holds one (clusterhook.ParseLines), and every sync was refused
// whole (#365). The other is what git rejects: a ref created or updated
// that conflicts as a directory and a file with a ref the repository keeps
// once the sync's own deletions are written, or with another ref the sync
// writes. That check is git's own rule, on this node's refs: a conflict it
// misses fails its batch, which git and the cluster take whole or not at
// all, and the sync fails as before, nothing written apart.
func refusedUpdates(ctx context.Context, dir string, updates []porcelainUpdate) ([]refusedRef, error) {
	var refused []refusedRef
	invalid := map[string]bool{}
	for _, u := range updates {
		if err := reftable.ValidRefName(u.ref); err != nil {
			refused = append(refused, refusedRef{ref: u.ref, why: err.Error() + ", which the cluster cannot hold"})
			invalid[u.ref] = true
		}
	}
	stdout, _, err := git.NewCommand(ctx, "for-each-ref", "--format=%(refname)").RunStdString(&git.RunOpts{Dir: dir})
	if err != nil {
		return nil, err
	}
	deleted := map[string]bool{}
	for _, u := range updates {
		if u.flag == '-' && !invalid[u.ref] {
			deleted[u.ref] = true
		}
	}
	// The refs once the sync is written, and under each proper prefix of
	// one, at a slash, the smallest of them.
	after := map[string]bool{}
	for name := range strings.SplitSeq(stdout, "\n") {
		if name != "" && !deleted[name] {
			after[name] = true
		}
	}
	for _, u := range updates {
		if u.flag != '-' && !invalid[u.ref] {
			after[u.ref] = true
		}
	}
	under := map[string]string{}
	for name := range after {
		for i := strings.IndexByte(name, '/'); i > 0; {
			if cur, ok := under[name[:i]]; !ok || name < cur {
				under[name[:i]] = name
			}
			j := strings.IndexByte(name[i+1:], '/')
			if j < 0 {
				break
			}
			i += 1 + j
		}
	}
	for _, u := range updates {
		if u.flag == '-' || invalid[u.ref] {
			continue
		}
		conflict := under[u.ref]
		for i := strings.IndexByte(u.ref, '/'); i > 0 && conflict == ""; {
			if after[u.ref[:i]] {
				conflict = u.ref[:i]
				break
			}
			j := strings.IndexByte(u.ref[i+1:], '/')
			if j < 0 {
				break
			}
			i += 1 + j
		}
		if conflict != "" {
			refused = append(refused, refusedRef{ref: u.ref, why: "it conflicts with " + refLabel(conflict) + ", which the mirror keeps"})
		}
	}
	return refused, nil
}

// errRefsNotUpdated is a mirror's sync that wrote every update but these,
// as git fetch outside a cluster reports "some local refs could not be
// updated" once it wrote the others.
type errRefsNotUpdated []refusedRef

// refsNotUpdatedShown bounds the refs the error names.
const refsNotUpdatedShown = 20

func (e errRefsNotUpdated) Error() string {
	var b strings.Builder
	b.WriteString("these refs could not be updated, the others were written:")
	for i, r := range e {
		if i == refsNotUpdatedShown {
			fmt.Fprintf(&b, " and %d more;", len(e)-i)
			break
		}
		fmt.Fprintf(&b, " %s (%s);", refLabel(r.ref), r.why)
	}
	return strings.TrimSuffix(b.String(), ";")
}

// refLabel is a ref name as a log line or a notice may hold it: quoted
// when it is not valid UTF-8.
func refLabel(name string) string {
	if utf8.ValidString(name) {
		return name
	}
	return strconv.Quote(name)
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
