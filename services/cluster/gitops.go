// Copyright 2026 The Forgejo Authors. All rights reserved.
// SPDX-License-Identifier: GPL-3.0-or-later

package cluster

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"forgejo.org/modules/git"
	"forgejo.org/services/cluster/reftable"
)

// The projector's git writes bypass the replication hook, so it never
// proposes what it is projecting (ADR 0152 D19): core.hooksPath on the
// command line overrides the global one Forgejo installs. Every argument
// comes from the table, whose ref names and ids Apply validated, from an
// absolute path, or from constants, so none can start an option.

// gitRun runs a projector command in dir.
func gitRun(ctx context.Context, dir string, stdin io.Reader, args ...string) (string, error) {
	var stdout, stderr bytes.Buffer
	cmd := git.NewCommand(ctx, "-c", "core.hooksPath=/dev/null").AddArguments(git.ToTrustedCmdArgs(args)...)
	err := cmd.Run(&git.RunOpts{Dir: dir, Stdin: stdin, Stdout: &stdout, Stderr: &stderr})
	if err != nil {
		return stdout.String(), fmt.Errorf("git %s: %w: %s", strings.Join(args, " "), err, strings.TrimSpace(stderr.String()))
	}
	return stdout.String(), nil
}

// readGitRefs lists what git shows of a repository: every ref under refs/
// with its value, symbolic refs by target, and HEAD. Fields are separated by
// NUL, which no ref name holds, and the name comes last, byte for byte: git
// accepts Unicode spaces in a name (U+00A0, U+3000), and a split on white
// space would read "refs/heads/main<U+00A0>x" as refs/heads/main.
func readGitRefs(ctx context.Context, dir string) (map[string]reftable.ProjectedRef, error) {
	out, err := gitRun(ctx, dir, nil, "for-each-ref", "--format=%(objectname)%00%(symref)%00%(refname)")
	if err != nil {
		return nil, err
	}
	refs := map[string]reftable.ProjectedRef{}
	for line := range strings.SplitSeq(out, "\n") {
		if line == "" {
			continue
		}
		f := strings.SplitN(line, "\x00", 3)
		if len(f) != 3 || f[2] == "" {
			return nil, fmt.Errorf("git for-each-ref: unexpected line %q", line)
		}
		if f[1] != "" {
			refs[f[2]] = reftable.ProjectedRef{Symbolic: f[1]}
			continue
		}
		refs[f[2]] = reftable.ProjectedRef{SHA: f[0]}
	}
	head, err := gitRun(ctx, dir, nil, "symbolic-ref", "-q", "HEAD")
	if head = strings.TrimSuffix(head, "\n"); err == nil && head != "" {
		refs["HEAD"] = reftable.ProjectedRef{Symbolic: head}
	}
	return refs, nil
}

// tableProjection is the table's side of the projection digest: live refs
// only, symbolic refs by name.
func tableProjection(refs map[string]reftable.RefValue) map[string]reftable.ProjectedRef {
	out := make(map[string]reftable.ProjectedRef, len(refs))
	for name, v := range refs {
		if !v.Live() {
			continue
		}
		if v.Symbolic != "" {
			out[name] = reftable.ProjectedRef{Symbolic: v.Symbolic}
		} else {
			out[name] = reftable.ProjectedRef{SHA: v.SHA}
		}
	}
	return out
}

// missingObjects returns the ids among shas the repository does not hold.
func missingObjects(ctx context.Context, dir string, shas []string) ([]string, error) {
	if len(shas) == 0 {
		return nil, nil
	}
	in := strings.Join(shas, "\n") + "\n"
	out, err := gitRun(ctx, dir, strings.NewReader(in), "cat-file", "--batch-check=%(objectname) %(objecttype)")
	if err != nil {
		return nil, err
	}
	var missing []string
	sc := bufio.NewScanner(strings.NewReader(out))
	for sc.Scan() {
		f := strings.Fields(sc.Text())
		if len(f) == 2 && f[1] == "missing" {
			missing = append(missing, f[0])
		}
	}
	return missing, nil
}

// writeRefs makes the given refs of dir hold their table values. The
// deletions go in a first git transaction and the updates in a second: git
// refuses to swap a file and a directory of refs in one transaction, in
// either order of its lines (t1404, "D/F conflict prevents add long +
// delete short"), and a node behind that projects a deleted refs/heads/a
// and a created refs/heads/a/b, or the reverse, would fail at every pass
// (#73). Between the two, the node serves neither ref, which the next
// write closes; the table is what holds the refs.
func writeRefs(ctx context.Context, dir string, set map[string]reftable.RefValue, del []string) error {
	var deletes, updates strings.Builder
	var symbolic [][2]string
	for name, v := range set {
		if v.Symbolic != "" {
			symbolic = append(symbolic, [2]string{name, v.Symbolic})
			continue
		}
		fmt.Fprintf(&updates, "update %s %s\n", name, v.SHA)
	}
	for _, name := range del {
		if name == "HEAD" {
			continue
		}
		fmt.Fprintf(&deletes, "delete %s\n", name)
	}
	for _, stdin := range []string{deletes.String(), updates.String()} {
		if stdin == "" {
			continue
		}
		if _, err := gitRun(ctx, dir, strings.NewReader(stdin), "update-ref", "--stdin"); err != nil {
			return err
		}
	}
	for _, s := range symbolic {
		if _, err := gitRun(ctx, dir, nil, "symbolic-ref", s[0], s[1]); err != nil {
			return err
		}
	}
	return nil
}

// initBare creates an empty bare repository for a key the table names and
// this node lacks.
func initBare(ctx context.Context, dir, format string) error {
	if err := os.MkdirAll(filepath.Dir(dir), 0o755); err != nil {
		return err
	}
	switch format {
	case "":
		format = "sha1"
	case "sha1", "sha256":
	default:
		return fmt.Errorf("object format %q is neither sha1 nor sha256", format)
	}
	if _, err := gitRun(ctx, filepath.Dir(dir), nil, "init", "--bare", "--object-format="+format, dir); err != nil {
		return err
	}
	// The objects about to land here are acknowledged once their pack is
	// synced: the directories leading to them are synced first, innermost
	// first, up to the repository root.
	for _, d := range []string{filepath.Join(dir, "objects"), filepath.Join(dir, "refs"), dir, filepath.Dir(dir), filepath.Dir(filepath.Dir(dir))} {
		if err := syncDirFn(d); err != nil {
			return err
		}
	}
	return nil
}

// syncDirFn syncs a directory; tests replace it.
var syncDirFn = syncDir
