// Copyright 2026 The Forgejo Authors. All rights reserved.
// SPDX-License-Identifier: GPL-3.0-or-later

package observer

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"maps"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"syscall"
	"time"
)

// The observer's git store: one bare repository per repository key, under
// git/. Objects arrive as packs checked by `git index-pack --strict`; each
// checkpoint's refs are created under refs/observer/<checkpoint>/ in one
// transaction of create-only updates, so a ref of an accepted checkpoint is
// never moved, and nothing collects the objects they name (gc.auto = 0).

// ErrImmutable is a checkpoint whose refs exist with other values.
var ErrImmutable = errors.New("an accepted checkpoint's refs exist with other values")

// gitWaitDelay is how long a cancelled git is given to end.
const gitWaitDelay = 10 * time.Second

type gitStore struct {
	root string
	git  string
}

// RefNamespace is the namespace of a checkpoint's refs.
func RefNamespace(checkpoint string) string { return "refs/observer/" + checkpoint + "/" }

func (g gitStore) dir(repo string) string { return filepath.Join(g.root, filepath.FromSlash(repo)) }

// storeFormat is an object format as a store names it: sha1 when unset.
func storeFormat(format string) string {
	if format == "" {
		return "sha1"
	}
	return format
}

// formatOf reads the object format of an existing store, "" without one.
func formatOf(dir string) string {
	b, err := os.ReadFile(filepath.Join(dir, "config"))
	if err != nil {
		if _, serr := os.Stat(filepath.Join(dir, "HEAD")); serr != nil {
			return ""
		}
		return "sha1"
	}
	for line := range strings.SplitSeq(string(b), "\n") {
		k, v, ok := strings.Cut(line, "=")
		if ok && strings.EqualFold(strings.TrimSpace(k), "objectformat") {
			return strings.ToLower(strings.TrimSpace(v))
		}
	}
	return "sha1"
}

// dirFor is the store of a repository in one object format. The store
// under the key alone holds the format the key was first seen in; a
// repository recreated, or renamed to that key, in the other format has
// its own store under .<format>/, so neither is ever mixed into the other
// and older checkpoints keep reading theirs (#84).
func (g gitStore) dirFor(repo, format string) string {
	format = storeFormat(format)
	legacy := g.dir(repo)
	switch formatOf(legacy) {
	case "", format:
		return legacy
	}
	return filepath.Join(g.root, "."+format, filepath.FromSlash(repo))
}

func (g gitStore) run(ctx context.Context, dir string, stdin io.Reader, args ...string) (string, error) {
	var out, errb bytes.Buffer
	cmd := exec.CommandContext(ctx, g.git, append([]string{"-c", "core.hooksPath=/dev/null"}, args...)...)
	// A stop asks git to end and clean its locks first; only one that
	// does not end in time is killed (#85).
	cmd.Cancel = func() error {
		if err := cmd.Process.Signal(syscall.SIGTERM); err != nil {
			return cmd.Process.Kill()
		}
		return nil
	}
	cmd.WaitDelay = gitWaitDelay
	cmd.Dir, cmd.Stdin, cmd.Stdout, cmd.Stderr = dir, stdin, &out, &errb
	cmd.Env = append(os.Environ(), "GIT_CONFIG_NOSYSTEM=1", "GIT_TERMINAL_PROMPT=0")
	if err := cmd.Run(); err != nil {
		return out.String(), fmt.Errorf("git %s: %w: %s", args[0], err, strings.TrimSpace(errb.String()))
	}
	return out.String(), nil
}

// ensure creates a repository's store with the object format of its refs.
func (g gitStore) ensure(ctx context.Context, repo, format string) (string, error) {
	format = storeFormat(format)
	dir := g.dirFor(repo, format)
	if _, err := os.Stat(filepath.Join(dir, "HEAD")); err == nil {
		return dir, nil
	}
	if err := os.MkdirAll(filepath.Dir(dir), 0o700); err != nil {
		return "", err
	}
	if _, err := g.run(ctx, filepath.Dir(dir), nil, "init", "-q", "--bare", "--object-format="+format, dir); err != nil {
		return "", err
	}
	for _, kv := range [][2]string{{"gc.auto", "0"}, {"core.logAllRefUpdates", "false"}, {"receive.fsckObjects", "true"}, {"fetch.fsckObjects", "true"}} {
		if _, err := g.run(ctx, dir, nil, "config", kv[0], kv[1]); err != nil {
			return "", err
		}
	}
	return dir, nil
}

// ingestPack indexes a pack strictly into a repository's store.
func (g gitStore) ingestPack(ctx context.Context, dir string, pack io.Reader) error {
	_, err := g.run(ctx, dir, pack, "index-pack", "--stdin", "--strict")
	return err
}

// closed checks that every object reachable from wants is held, past what
// haves reach, which an earlier checkpoint already checked.
func (g gitStore) closed(ctx context.Context, dir string, wants, haves []string) error {
	args := []string{"rev-list", "--objects", "--missing=error", "--quiet"}
	args = append(args, wants...)
	if len(haves) > 0 {
		args = append(args, "--not")
		args = append(args, haves...)
	}
	_, err := g.run(ctx, dir, nil, args...)
	if err != nil {
		return fmt.Errorf("the object closure is incomplete: %w", err)
	}
	return nil
}

// present returns which of shas the store holds.
func (g gitStore) present(ctx context.Context, dir string, shas []string) []string {
	if len(shas) == 0 {
		return nil
	}
	out, err := g.run(ctx, dir, strings.NewReader(strings.Join(shas, "\n")+"\n"), "cat-file", "--batch-check=%(objectname) %(objecttype)")
	if err != nil {
		return nil
	}
	var have []string
	for line := range strings.SplitSeq(strings.TrimSpace(out), "\n") {
		f := strings.Fields(line)
		if len(f) == 2 && f[1] != "missing" {
			have = append(have, f[0])
		}
	}
	return have
}

// refs lists a checkpoint's refs in a repository's store, by their name
// without the namespace.
func (g gitStore) refs(ctx context.Context, dir, checkpoint string) (map[string]string, error) {
	ns := RefNamespace(checkpoint)
	out, err := g.run(ctx, dir, nil, "for-each-ref", "--format=%(objectname) %(refname)", ns)
	if err != nil {
		return nil, err
	}
	refs := map[string]string{}
	for line := range strings.SplitSeq(strings.TrimSpace(out), "\n") {
		sha, name, ok := strings.Cut(line, " ")
		if ok {
			refs[strings.TrimPrefix(name, ns)] = sha
		}
	}
	return refs, nil
}

// missingRefs compares the refs a checkpoint holds with refs and returns
// those still to create: ErrImmutable when one is there with another value,
// or is not one of refs. A transaction killed while it renamed its locks
// leaves some of them, which a retry completes.
func missingRefs(checkpoint string, have, refs map[string]string) (map[string]string, error) {
	for name, sha := range have {
		if refs[name] != sha {
			return nil, fmt.Errorf("%w: %s %s", ErrImmutable, checkpoint, name)
		}
	}
	missing := map[string]string{}
	for name, sha := range refs {
		if _, ok := have[name]; !ok {
			missing[name] = sha
		}
	}
	return missing, nil
}

// dropLocks removes the ref locks left under a checkpoint's namespace. The
// observer is its store's only writer and creates a namespace once, before
// the checkpoint is published: a lock there is a git killed in an earlier
// pass, never one at work (#85).
func (g gitStore) dropLocks(dir, checkpoint string) error {
	if !checkpointName.MatchString(checkpoint) {
		return fmt.Errorf("invalid checkpoint name %q", checkpoint)
	}
	root := filepath.Join(dir, "refs", "observer", checkpoint)
	err := filepath.WalkDir(root, func(p string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.Type().IsRegular() && strings.HasSuffix(d.Name(), ".lock") {
			return os.Remove(p)
		}
		return nil
	})
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	return err
}

// createRefs creates a checkpoint's refs in one transaction of create-only
// updates. Refs already there with the same values are a retry after a
// crash and succeed; refs there with any other value are ErrImmutable.
// A transaction that fails otherwise (a stop, a full disk) is an ordinary
// error, which the next pass retries.
func (g gitStore) createRefs(ctx context.Context, dir, checkpoint string, refs map[string]string) error {
	have, err := g.refs(ctx, dir, checkpoint)
	if err != nil {
		return err
	}
	missing, err := missingRefs(checkpoint, have, refs)
	if err != nil || len(missing) == 0 {
		return err
	}
	if err := g.dropLocks(dir, checkpoint); err != nil {
		return err
	}
	names := make([]string, 0, len(missing))
	for name := range missing {
		names = append(names, name)
	}
	sort.Strings(names)
	var in strings.Builder
	in.WriteString("start\n")
	for _, name := range names {
		fmt.Fprintf(&in, "create %s%s %s\n", RefNamespace(checkpoint), name, refs[name])
	}
	in.WriteString("prepare\ncommit\n")
	if _, err := g.run(ctx, dir, strings.NewReader(in.String()), "update-ref", "--stdin"); err != nil {
		if ctx.Err() != nil {
			return fmt.Errorf("%w: %w", ctx.Err(), err)
		}
		if have, rerr := g.refs(ctx, dir, checkpoint); rerr == nil {
			if left, merr := missingRefs(checkpoint, have, refs); merr != nil || len(left) == 0 {
				return merr
			}
		}
		return err
	}
	return nil
}

// dropRefs removes a pruned checkpoint's refs; the objects stay.
func (g gitStore) dropRefs(ctx context.Context, dir, checkpoint string) error {
	refs, err := g.refs(ctx, dir, checkpoint)
	if err != nil || len(refs) == 0 {
		return err
	}
	var in strings.Builder
	in.WriteString("start\n")
	for name, sha := range refs {
		fmt.Fprintf(&in, "delete %s%s %s\n", RefNamespace(checkpoint), name, sha)
	}
	in.WriteString("prepare\ncommit\n")
	_, err = g.run(ctx, dir, strings.NewReader(in.String()), "update-ref", "--stdin")
	return err
}

// IncompleteMark is the file a repository being restored holds until it is
// whole and durable.
const IncompleteMark = "forgejo-restore-incomplete"

// writeMark creates the IncompleteMark in target under its own name, then
// syncs it and target, before git writes anything there. Written under a
// temporary name and renamed, a crash before the rename left target with
// that name alone, neither empty nor marked (#291).
func writeMark(target string) error {
	mark := filepath.Join(target, IncompleteMark)
	f, err := os.OpenFile(mark, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	if err != nil {
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	if err := syncPath(mark); err != nil {
		return err
	}
	return syncDir(target)
}

// removeAll is os.RemoveAll; a test replaces it to stop a removal
// midway, as a kill would.
var removeAll = os.RemoveAll

// marked reports whether target holds the IncompleteMark: the regular file
// Materialize creates there first, and nothing else under that name.
func marked(target string) bool {
	info, err := os.Lstat(filepath.Join(target, IncompleteMark))
	return err == nil && info.Mode().IsRegular()
}

// unmark empties target, holding entries and marked by a restore that did
// not end, for Materialize to write it again: everything but the mark
// first, then, once target is synced holding the mark alone, the mark.
// Removed whole by os.RemoveAll, in the order the file system lists the
// entries, the mark could go first (XFS and Btrfs list a small directory
// in creation order, the mark created first) and a kill or a power loss
// before the last unlink left part of a repository and no mark: refused
// as not empty by every rerun, or, its refs left and its pack gone, kept
// as restored (#320). Cut anywhere here, the removal leaves target marked,
// or empty.
func unmark(target string, entries []os.DirEntry) error {
	for _, e := range entries {
		if e.Name() == IncompleteMark {
			continue
		}
		if err := removeAll(filepath.Join(target, e.Name())); err != nil {
			return err
		}
	}
	if err := syncDir(target); err != nil {
		return err
	}
	return removeAll(filepath.Join(target, IncompleteMark))
}

// markLeftovers reports whether entries, a target's, are only the
// temporary files under which an older restore wrote the IncompleteMark:
// one killed before the rename left target holding them alone, a target
// of this restore's (#291).
func markLeftovers(entries []os.DirEntry) bool {
	for _, e := range entries {
		if !e.Type().IsRegular() || !isTempName(e.Name(), IncompleteMark) {
			return false
		}
	}
	return len(entries) > 0
}

// restoredRefs reports whether target already holds exactly refs, each
// naming an object target holds: a repository an earlier run of the same
// restore wrote whole. for-each-ref lists a ref whose object is missing
// unless asked for something of the object itself: %(objecttype) makes it
// fail on such a ref (#320).
func (g gitStore) restoredRefs(ctx context.Context, target string, refs map[string]string) bool {
	out, err := g.run(ctx, target, nil, "for-each-ref", "--format=%(objectname) %(objecttype) %(refname)", "refs/")
	if err != nil {
		return false
	}
	have := map[string]string{}
	for line := range strings.SplitSeq(strings.TrimSpace(out), "\n") {
		sha, rest, ok := strings.Cut(line, " ")
		if !ok {
			continue
		}
		if _, name, ok := strings.Cut(rest, " "); ok {
			have[name] = sha
		}
	}
	return maps.Equal(have, refs)
}

// Materialize writes a checkpoint's repository into target, an absent or
// empty directory: its refs, their objects checked again by fetch (none
// for a repository without refs), and its symbolic refs.
func (g gitStore) Materialize(ctx context.Context, repo, checkpoint, target, format string, fetch bool, symbolic map[string]string) error {
	if entries, err := os.ReadDir(target); err == nil && len(entries) > 0 {
		return fmt.Errorf("%s is not empty", target)
	}
	if err := os.MkdirAll(filepath.Dir(target), 0o755); err != nil {
		return err
	}
	if format == "" {
		format = "sha1"
	}
	// The mark goes first: a target left by an interrupted restore is known
	// as this restore's, removed and written again; the restore removes it
	// once the repository is durable.
	if err := os.MkdirAll(target, 0o755); err != nil {
		return err
	}
	if err := writeMark(target); err != nil {
		return err
	}
	if _, err := g.run(ctx, filepath.Dir(target), nil, "init", "-q", "--bare", "--object-format="+format, target); err != nil {
		return err
	}
	// A repository without refs has nothing to fetch, and no store here.
	if fetch {
		ns := RefNamespace(checkpoint)
		if _, err := g.run(ctx, target, nil, "-c", "fetch.fsckObjects=true", "fetch", "-q", "--no-tags", "--no-write-fetch-head", g.dirFor(repo, format), "+"+ns+"refs/*:refs/*"); err != nil {
			return err
		}
	}
	for name, to := range symbolic {
		if _, err := g.run(ctx, target, nil, "symbolic-ref", name, to); err != nil {
			return err
		}
	}
	return nil
}
