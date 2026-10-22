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
)

// The observer's git store: one bare repository per repository key, under
// git/. Objects arrive as packs checked by `git index-pack --strict`; each
// checkpoint's refs are created under refs/observer/<checkpoint>/ in one
// transaction of create-only updates, so a ref of an accepted checkpoint is
// never moved, and nothing collects the objects they name (gc.auto = 0).

// ErrImmutable is a checkpoint whose refs exist with other values.
var ErrImmutable = errors.New("an accepted checkpoint's refs exist with other values")

type gitStore struct {
	root string
	git  string
}

// RefNamespace is the namespace of a checkpoint's refs.
func RefNamespace(checkpoint string) string { return "refs/observer/" + checkpoint + "/" }

func (g gitStore) dir(repo string) string { return filepath.Join(g.root, filepath.FromSlash(repo)) }

func (g gitStore) run(ctx context.Context, dir string, stdin io.Reader, args ...string) (string, error) {
	var out, errb bytes.Buffer
	cmd := exec.CommandContext(ctx, g.git, append([]string{"-c", "core.hooksPath=/dev/null"}, args...)...)
	cmd.Dir, cmd.Stdin, cmd.Stdout, cmd.Stderr = dir, stdin, &out, &errb
	cmd.Env = append(os.Environ(), "GIT_CONFIG_NOSYSTEM=1", "GIT_TERMINAL_PROMPT=0")
	if err := cmd.Run(); err != nil {
		return out.String(), fmt.Errorf("git %s: %w: %s", args[0], err, strings.TrimSpace(errb.String()))
	}
	return out.String(), nil
}

// ensure creates a repository's store with the object format of its refs.
func (g gitStore) ensure(ctx context.Context, repo, format string) (string, error) {
	dir := g.dir(repo)
	if _, err := os.Stat(filepath.Join(dir, "HEAD")); err == nil {
		return dir, nil
	}
	if err := os.MkdirAll(filepath.Dir(dir), 0o700); err != nil {
		return "", err
	}
	if format == "" {
		format = "sha1"
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

// createRefs creates a checkpoint's refs in one transaction of create-only
// updates. Refs already there with the same values are a retry after a
// crash and succeed; any other value is ErrImmutable.
func (g gitStore) createRefs(ctx context.Context, dir, checkpoint string, refs map[string]string) error {
	have, err := g.refs(ctx, dir, checkpoint)
	if err != nil {
		return err
	}
	if len(have) > 0 {
		if len(have) != len(refs) {
			return fmt.Errorf("%w: %s holds %d refs, not %d", ErrImmutable, checkpoint, len(have), len(refs))
		}
		for name, sha := range refs {
			if have[name] != sha {
				return fmt.Errorf("%w: %s %s", ErrImmutable, checkpoint, name)
			}
		}
		return nil
	}
	names := make([]string, 0, len(refs))
	for name := range refs {
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
		return fmt.Errorf("%w: %v", ErrImmutable, err)
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

// restoredRefs reports whether target already holds exactly refs: a
// repository an earlier run of the same restore wrote whole.
func (g gitStore) restoredRefs(ctx context.Context, target string, refs map[string]string) bool {
	out, err := g.run(ctx, target, nil, "for-each-ref", "--format=%(objectname) %(refname)", "refs/")
	if err != nil {
		return false
	}
	have := map[string]string{}
	for line := range strings.SplitSeq(strings.TrimSpace(out), "\n") {
		if sha, name, ok := strings.Cut(line, " "); ok {
			have[name] = sha
		}
	}
	return maps.Equal(have, refs)
}

// Materialize writes a checkpoint's repository into target, an absent or
// empty directory: its refs, their objects checked again by fetch, and its
// symbolic refs.
func (g gitStore) Materialize(ctx context.Context, repo, checkpoint, target, format string, symbolic map[string]string) error {
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
	if err := writeAtomic(filepath.Join(target, IncompleteMark), nil, true); err != nil {
		return err
	}
	if _, err := g.run(ctx, filepath.Dir(target), nil, "init", "-q", "--bare", "--object-format="+format, target); err != nil {
		return err
	}
	ns := RefNamespace(checkpoint)
	if _, err := g.run(ctx, target, nil, "-c", "fetch.fsckObjects=true", "fetch", "-q", "--no-tags", "--no-write-fetch-head", g.dir(repo), "+"+ns+"refs/*:refs/*"); err != nil {
		return err
	}
	for name, to := range symbolic {
		if _, err := g.run(ctx, target, nil, "symbolic-ref", name, to); err != nil {
			return err
		}
	}
	return nil
}
