// Copyright 2026 The Forgejo Authors. All rights reserved.
// SPDX-License-Identifier: GPL-3.0-or-later

package cluster

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"maps"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"syscall"

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

// readGitRefs lists what git holds of a repository: every ref under refs/
// with its value, HEAD, and each symbolic ref by the ref it names, one
// level deep, as the table holds it. Fields are separated by NUL, which no
// ref name holds, and the name comes last, byte for byte: git accepts
// Unicode spaces in a name (U+00A0, U+3000), and a split on white space
// would read "refs/heads/main<U+00A0>x" as refs/heads/main.
//
// for-each-ref names the end of a symbolic ref's chain, and lists a
// dangling symbolic ref, one whose target git does not hold, nowhere: the
// alias refs/heads/master -> refs/heads/main left once main was renamed or
// deleted, which the table keeps, never matched it, and the repository was
// never certified again; nor was HEAD -> refs/heads/master ->
// refs/heads/main (#348). Each symbolic ref it lists, HEAD, and each name
// in symbolic, the refs the table holds as symbolic, are read again one
// level deep: one more git process per symbolic ref, none for a repository
// whose only symbolic ref is HEAD.
func readGitRefs(ctx context.Context, dir string, symbolic ...string) (map[string]reftable.ProjectedRef, error) {
	return readRefs(ctx, dir, symbolic, nil)
}

// readTrackedRefs reads git as the projector compares it with refs, the
// table's entries of the repository: readGitRefs with every symbolic ref
// the table holds, and with each of its tombstones git may still hold as a
// dangling symbolic ref.
//
// A tombstone no longer says what the ref held. When the table deleted the
// dangling alias refs/heads/master -> refs/heads/main, every node but the
// one that ran `git symbolic-ref -d` kept it in git, unread, and certified
// the repository: receive-pack followed it there, and a push creating a
// new branch master landed on main, which came back on every node; a
// branch master/x never reached their git, which stopped certifying
// (#359). Read, the alias is deleted like any ref the table deleted, and a
// node whose git keeps it does not certify.
func readTrackedRefs(ctx context.Context, dir string, refs map[string]reftable.RefValue) (map[string]reftable.ProjectedRef, error) {
	var symbolic, gone []string
	for name, v := range refs {
		switch {
		case name == "HEAD":
		case !v.Live():
			gone = append(gone, name)
		case v.Symbolic != "":
			symbolic = append(symbolic, name)
		}
	}
	return readRefs(ctx, dir, symbolic, gone)
}

// readRefs is readGitRefs, which also reads one level deep each name in
// gone that for-each-ref does not list and git may hold as a symbolic ref.
// git's files backend holds a symbolic ref in a file of its own, never in
// packed-refs: a name with no file under it holds none, which a stat tells
// without a git process, and a repository rarely holds such a file.
// Measured, 2.5 µs per tombstone: 25 ms for 10,000 beside the 6 ms of
// for-each-ref over 2,000 packed refs. The reftable backend keeps no file
// per ref: git reads each name, one process, 1.5 ms, per tombstone.
// Neither Forgejo nor the projector creates such a repository unless git's
// own configuration asks for it (init.defaultRefFormat).
func readRefs(ctx context.Context, dir string, symbolic, gone []string) (map[string]reftable.ProjectedRef, error) {
	out, err := gitRun(ctx, dir, nil, "for-each-ref", "--format=%(objectname)%00%(symref)%00%(refname)")
	if err != nil {
		return nil, err
	}
	refs := map[string]reftable.ProjectedRef{}
	links := []string{"HEAD"}
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
			links = append(links, f[2])
			continue
		}
		refs[f[2]] = reftable.ProjectedRef{SHA: f[0]}
	}
	for _, name := range symbolic {
		// A name listed is read already, or holds a value of its own.
		if _, listed := refs[name]; !listed && name != "HEAD" {
			links = append(links, name)
		}
	}
	if len(gone) > 0 {
		// A directory the stat cannot tell about counts as the reftable
		// backend: git reads each name, and fails as the stat did.
		_, err := os.Stat(filepath.Join(dir, "reftable"))
		files := errors.Is(err, os.ErrNotExist)
		for _, name := range gone {
			if _, listed := refs[name]; listed || name == "HEAD" || reftable.ValidRefName(name) != nil {
				continue
			}
			if files && !looseRefMayExist(dir, name) {
				continue
			}
			links = append(links, name)
		}
	}
	for _, name := range links {
		// A name the table cannot hold, which git alone shows, stays as
		// for-each-ref lists it: filterTracked leaves it out. A valid one
		// starts no option.
		if reftable.ValidRefName(name) != nil {
			continue
		}
		to, err := readSymbolicRef(ctx, dir, name)
		switch {
		case err != nil:
			return nil, err
		case to != "" && reftable.ValidRefName(to) == nil && to != "HEAD":
			refs[name] = reftable.ProjectedRef{Symbolic: to}
		case to != "":
			// A first link no table holds, as Apply refuses it: HEAD or
			// a name outside refs/, which git alone writes
			// (refs/heads/x -> HEAD). It stays as for-each-ref lists it,
			// at the end of its chain, or unseen when dangling: import
			// takes what it took before, never a value Apply refuses.
		case refs[name].Symbolic != "":
			// Listed as symbolic a moment ago: what git holds now is
			// unknown, and the end of its chain is no first link.
			return nil, fmt.Errorf("git symbolic-ref %s: no longer a symbolic ref once listed", name)
		}
	}
	return refs, nil
}

// readSymbolicRef returns the ref that git's symbolic ref name names, one
// level deep, or "" when git holds no symbolic ref under name, which git
// tells by its exit code 1. Any other failure is returned: a symbolic ref
// git could not read would show as absent, or at the end of its chain.
func readSymbolicRef(ctx context.Context, dir, name string) (string, error) {
	out, err := gitRun(ctx, dir, nil, "symbolic-ref", "-q", "--no-recurse", "--end-of-options", name)
	if err != nil {
		if git.IsErrorExitCode(err, 1) {
			return "", nil
		}
		return "", err
	}
	return strings.TrimSuffix(out, "\n"), nil
}

// looseRefMayExist reports whether git's files backend may hold a loose
// ref, a symbolic one among them, under name in the repository dir: no
// entry, or a directory, under its path, or a file where a directory of it
// would be, holds none. Any other failure of the stat says yes: git reads
// the name, and reports what it cannot read. The name is valid
// (reftable.ValidRefName): no ".." component, no absolute path.
func looseRefMayExist(dir, name string) bool {
	st, err := os.Lstat(filepath.Join(dir, filepath.FromSlash(name)))
	switch {
	case err == nil:
		return !st.IsDir()
	case errors.Is(err, os.ErrNotExist), errors.Is(err, syscall.ENOTDIR):
		return false
	default:
		return true
	}
}

// projectionDiff names the refs git and the table hold otherwise, in
// order and at most three, with both values: a repository that is never
// certified tells an operator which ref keeps it so (#348).
func projectionDiff(have, table map[string]reftable.ProjectedRef) string {
	differ := map[string]bool{}
	for name, v := range have {
		if w, ok := table[name]; !ok || w != v {
			differ[name] = true
		}
	}
	for name := range table {
		if _, ok := have[name]; !ok {
			differ[name] = true
		}
	}
	const shown = 3
	names := slices.Sorted(maps.Keys(differ))
	var b strings.Builder
	for i, name := range names[:min(len(names), shown)] {
		if i > 0 {
			b.WriteString("; ")
		}
		fmt.Fprintf(&b, "%q: git %s, table %s", name, projectedValue(have, name), projectedValue(table, name))
	}
	if len(names) > shown {
		fmt.Fprintf(&b, "; and %d more", len(names)-shown)
	}
	return b.String()
}

// projectedValue shows what refs holds under name for projectionDiff.
func projectedValue(refs map[string]reftable.ProjectedRef, name string) string {
	v, ok := refs[name]
	switch {
	case !ok:
		return "nothing"
	case v.Symbolic != "":
		return strconv.Quote("ref:" + v.Symbolic)
	default:
		return strconv.Quote(v.SHA)
	}
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
//
// Each line writes the ref it names, never the one a symbolic ref there
// points to: update-ref follows a symbolic ref unless told not to, so the
// deletion of the alias refs/heads/master -> refs/heads/main deleted main,
// and a value of master's own moved main, on every node but the writer's
// (#348).
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
		if _, err := gitRun(ctx, dir, strings.NewReader(stdin), "update-ref", "--no-deref", "--stdin"); err != nil {
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

// validRepoDir reports whether dir holds what git needs to see a bare
// repository: a non-empty HEAD, objects/ and refs/. A git init killed
// halfway, or one whose files were never synced before a power cut,
// leaves a directory git refuses as "not a git repository" (#42).
func validRepoDir(dir string) bool {
	invalid, err := invalidRepoDir(dir)
	return err == nil && !invalid
}

// invalidRepoDir reports whether dir holds no bare repository for certain:
// an entry git needs is missing or of the wrong kind. Any other error
// (permission denied, an I/O error) is returned: the directory may hold a
// sound repository that cannot be read now.
func invalidRepoDir(dir string) (bool, error) {
	st, err := os.Lstat(filepath.Join(dir, "HEAD"))
	if errors.Is(err, os.ErrNotExist) {
		return true, nil
	}
	if err != nil {
		return false, err
	}
	if !st.Mode().IsRegular() || st.Size() == 0 {
		return true, nil
	}
	for _, d := range []string{"objects", "refs"} {
		st, err := os.Stat(filepath.Join(dir, d))
		if errors.Is(err, os.ErrNotExist) {
			return true, nil
		}
		if err != nil {
			return false, err
		}
		if !st.IsDir() {
			return true, nil
		}
	}
	return false, nil
}

// initBare creates an empty bare repository for a key the table names and
// this node lacks. It is made in a sibling directory, whose leading dot
// keeps it outside every repository key, synced, and renamed under the
// key: an interrupted init never leaves a partial repository there (#42).
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
	tmp, err := os.MkdirTemp(filepath.Dir(dir), "."+filepath.Base(dir)+".init-")
	if err != nil {
		return err
	}
	defer os.RemoveAll(tmp) // gone once renamed
	if _, err := gitRun(ctx, filepath.Dir(dir), nil, "init", "--bare", "--object-format="+format, tmp); err != nil {
		return err
	}
	for _, f := range []string{"HEAD", "config"} {
		if err := syncFileFn(filepath.Join(tmp, f)); err != nil {
			return err
		}
	}
	for _, d := range []string{filepath.Join(tmp, "objects"), filepath.Join(tmp, "refs"), tmp} {
		if err := syncDirFn(d); err != nil {
			return err
		}
	}
	if err := os.Rename(tmp, dir); err != nil {
		// Another init of the same key (the projector and the object
		// channel) won the race.
		if validRepoDir(dir) {
			return nil
		}
		return err
	}
	// The objects about to land here are acknowledged once their pack is
	// synced: the directories leading to them are synced first, up to the
	// repository root.
	return syncRepoParents(dir)
}

// syncRepoParents makes the name of a repository's directory durable: its
// parent and the parent's parent, the repository root, are synced. initBare
// does it after its rename; one whose sync failed leaves the directory in
// place, and every later user of it that vouches for it (an acknowledged
// pack, an answer that objects are held, a first certification) does it
// again, so the obligation survives the failure.
func syncRepoParents(dir string) error {
	for _, d := range []string{filepath.Dir(dir), filepath.Dir(filepath.Dir(dir))} {
		if err := syncDirFn(d); err != nil {
			return err
		}
	}
	return nil
}

// syncFileFn syncs a file; tests replace it.
var syncFileFn = func(path string) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()
	return f.Sync()
}

// syncDirFn syncs a directory; tests replace it.
var syncDirFn = syncDir
