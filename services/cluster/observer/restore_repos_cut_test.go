// Copyright 2026 The Forgejo Authors. All rights reserved.
// SPDX-License-Identifier: GPL-3.0-or-later

//go:build linux || darwin

package observer

import (
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"syscall"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// errKilled stands for the process killed in the middle of a removal.
var errKilled = errors.New("killed")

// cutRemoval removes a path as os.RemoveAll does on Linux
// (os/removeall_at.go), and stops, as a kill would, once budget unlinks are
// done: an unlink first, and for a directory its entries in the order
// getdents lists them, rearranged by order, depth first, then the directory
// itself. It never follows a symbolic link (unlink removes the link itself)
// and refuses any path outside root, a directory of the test.
type cutRemoval struct {
	root   string
	budget int
	order  func(names []string)
}

func (c *cutRemoval) removeAll(path string) error {
	if !strings.HasPrefix(path, c.root+string(filepath.Separator)) {
		return fmt.Errorf("%s is outside the test's directory", path)
	}
	if c.budget == 0 {
		return errKilled
	}
	err := syscall.Unlink(path)
	switch {
	case err == nil:
		c.budget--
		return nil
	case errors.Is(err, syscall.ENOENT):
		return nil
	case !errors.Is(err, syscall.EISDIR) && !errors.Is(err, syscall.EPERM):
		return err
	}
	d, err := os.Open(path)
	if err != nil {
		return err
	}
	names, err := d.Readdirnames(-1)
	d.Close()
	if err != nil {
		return err
	}
	c.order(names)
	for _, n := range names {
		if err := c.removeAll(filepath.Join(path, n)); err != nil {
			return err
		}
	}
	if c.budget == 0 {
		return errKilled
	}
	if err := syscall.Rmdir(path); err != nil {
		return err
	}
	c.budget--
	return nil
}

// Step 4 of recover --from-observer, resumed, empties a target an
// interrupted run left marked, then writes it again. Removed whole by
// os.RemoveAll, the target lost its mark wherever the file system listed
// it: first on XFS and Btrfs, which list a small directory in creation
// order, Materialize creating the mark first; after objects/ and config on
// ext4. A kill, a lost SSH session or a power loss after that unlink and
// before the last one left part of a repository and no mark: refused as
// "not empty" by every rerun or, its refs left and its pack gone, kept as
// restored (#320). Cut after any unlink, the removal leaves the target
// marked or empty, and the rerun writes it whole.
//
// Run 1 is killed during Materialize's fetch, the mark there; run 2 is
// killed k unlinks into the removal of the marked target; run 3 is the
// operator running the same command again.
func TestRestoreReposRerunAfterARemovalCutAnywhere(t *testing.T) {
	for _, tc := range []struct {
		name  string
		order func(names []string)
	}{
		{"as getdents lists the entries", func([]string) {}},
		{"the mark first, as XFS and Btrfs list a small directory", func(names []string) {
			if i := slices.Index(names, IncompleteMark); i > 0 {
				copy(names[1:i+1], names[:i])
				names[0] = IncompleteMark
			}
		}},
	} {
		t.Run(tc.name, func(t *testing.T) { restoreReposCut(t, tc.order) })
	}
}

func restoreReposCut(t *testing.T, order func([]string)) {
	ctx := t.Context()
	c := newFakeCluster(t)
	o := newTestObserver(t, c, &hookLog{})
	cp := seed(t, c, o)
	require.NoError(t, o.Step(ctx))
	a, err := OpenArchive(o.cfg.Dir, "")
	require.NoError(t, err)
	m, err := a.Manifest(cp.Name())
	require.NoError(t, err)
	const repo = "alice/demo.git"
	rm := m.Repos[repo]
	require.NotEmpty(t, rm.Refs)

	prev := removeAll
	t.Cleanup(func() { removeAll = prev })
	var refused, corrupt []string
	cuts := 0
	for k := 1; ; k++ {
		root := t.TempDir()
		target := filepath.Join(root, filepath.FromSlash(repo))
		require.NoError(t, a.store.git.Materialize(ctx, repo, cp.Name(), target, rm.Format, true, rm.Symbolic))
		require.FileExists(t, filepath.Join(target, IncompleteMark))

		cut := &cutRemoval{root: root, budget: k, order: order}
		removeAll = cut.removeAll
		_, err := a.RestoreRepos(ctx, cp.Name(), root)
		removeAll = prev
		if err == nil {
			break // k is past the last unlink: the removal ran whole
		}
		require.ErrorIs(t, err, errKilled, "k=%d", k)
		cuts++

		short := func(e error) string { return strings.ReplaceAll(fmt.Sprint(e), root, "<root>") }
		if _, err := a.RestoreRepos(ctx, cp.Name(), root); err != nil {
			_, again := a.RestoreRepos(ctx, cp.Name(), root)
			refused = append(refused, fmt.Sprintf("k=%d: %s / again: %s", k, short(err), short(again)))
			continue
		}
		assert.True(t, a.store.git.restoredRefs(ctx, target, rm), "k=%d: the checkpoint's refs", k)
		assert.NoFileExists(t, filepath.Join(target, IncompleteMark), "k=%d", k)
		if _, err := a.store.git.run(ctx, target, nil, "fsck", "--full", "--no-dangling"); err != nil {
			corrupt = append(corrupt, fmt.Sprintf("k=%d: %s", k, short(err)))
		}
	}
	require.Positive(t, cuts, "the removal was cut at least once")
	t.Logf("the removal cut at %d points: %d refused by the rerun, %d kept corrupt", cuts, len(refused), len(corrupt))
	for _, s := range corrupt {
		t.Logf("KEPT CORRUPT %s", s)
	}
	for _, s := range refused {
		t.Logf("REFUSED %s", s)
	}
	assert.Empty(t, corrupt, "a rerun of step 4 keeps as restored a repository missing its objects")
	assert.Empty(t, refused, "a rerun of step 4 refuses a target its own cut removal left")
}

// The rest of a marked target is gone, durably, before its mark goes:
// synced, the target holds the mark alone. A power loss after the mark's
// unlink finds the target marked or empty, never holding part of a
// repository without its mark (#320).
func TestRestoreReposSyncsTheTargetBeforeItsMarkGoes(t *testing.T) {
	ctx := t.Context()
	c := newFakeCluster(t)
	o := newTestObserver(t, c, &hookLog{})
	cp := seed(t, c, o)
	require.NoError(t, o.Step(ctx))
	a, err := OpenArchive(o.cfg.Dir, "")
	require.NoError(t, err)
	m, err := a.Manifest(cp.Name())
	require.NoError(t, err)
	const repo = "alice/demo.git"
	rm := m.Repos[repo]

	root := t.TempDir()
	target := filepath.Join(root, filepath.FromSlash(repo))
	mark := filepath.Join(target, IncompleteMark)
	require.NoError(t, a.store.git.Materialize(ctx, repo, cp.Name(), target, rm.Format, true, rm.Symbolic))
	require.FileExists(t, mark)

	var events []string
	prevSync, prevRemove := syncPath, removeAll
	t.Cleanup(func() { syncPath, removeAll = prevSync, prevRemove })
	syncPath = func(p string) error {
		if p == target {
			entries, err := os.ReadDir(target)
			require.NoError(t, err)
			names := make([]string, 0, len(entries))
			for _, e := range entries {
				names = append(names, e.Name())
			}
			events = append(events, "sync: "+strings.Join(names, " "))
		}
		return prevSync(p)
	}
	removeAll = func(p string) error {
		if p == mark {
			events = append(events, "remove the mark")
		}
		return prevRemove(p)
	}
	_, err = a.RestoreRepos(ctx, cp.Name(), root)
	require.NoError(t, err)
	i := slices.Index(events, "remove the mark")
	require.Positive(t, i, "the mark is removed on its own, after a sync of the target: %v", events)
	assert.Equal(t, "sync: "+IncompleteMark, events[i-1], "the target is synced holding the mark alone before the mark goes")
	assert.True(t, a.store.git.restoredRefs(ctx, target, rm))
	assert.NoFileExists(t, mark)
}

// Only the regular file Materialize writes marks a target as this
// restore's: a directory or a symbolic link under the mark's name is
// refused as any other non-empty target, and nothing in it is removed.
func TestRestoreReposRefusesAMarkThatIsNoFile(t *testing.T) {
	ctx := t.Context()
	c := newFakeCluster(t)
	o := newTestObserver(t, c, &hookLog{})
	cp := seed(t, c, o)
	require.NoError(t, o.Step(ctx))
	a, err := OpenArchive(o.cfg.Dir, "")
	require.NoError(t, err)

	for _, tc := range []struct {
		name string
		add  func(target string) error
	}{
		{"a directory", func(target string) error {
			return os.Mkdir(filepath.Join(target, IncompleteMark), 0o700)
		}},
		// The link names a file of the target, relative to it.
		{"a symbolic link to a file", func(target string) error {
			return os.Symlink("precious", filepath.Join(target, IncompleteMark))
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			root := t.TempDir()
			target := filepath.Join(root, "alice", "demo.git")
			require.NoError(t, os.MkdirAll(target, 0o755))
			require.NoError(t, os.WriteFile(filepath.Join(target, "precious"), []byte("x"), 0o600))
			require.NoError(t, tc.add(target))
			names := func() []string {
				entries, err := os.ReadDir(target)
				require.NoError(t, err)
				var out []string
				for _, e := range entries {
					out = append(out, e.Name())
				}
				return out
			}
			before := names()
			_, err := a.RestoreRepos(ctx, cp.Name(), root)
			require.ErrorContains(t, err, "is not empty")
			assert.Equal(t, before, names(), "nothing is removed")
		})
	}
}

// A repository holding the checkpoint's refs without their objects is not
// one an earlier run wrote whole. for-each-ref lists a ref whose object is
// missing, and step 4 kept as restored, its step recorded done, a target
// whose cut removal had taken the mark and the pack and left the refs
// (#320). Such a target is refused as any other this restore cannot tell
// its own, and nothing in it is removed.
func TestRestoreReposRefusesARepositoryMissingItsObjects(t *testing.T) {
	ctx := t.Context()
	c := newFakeCluster(t)
	o := newTestObserver(t, c, &hookLog{})
	cp := seed(t, c, o)
	require.NoError(t, o.Step(ctx))
	a, err := OpenArchive(o.cfg.Dir, "")
	require.NoError(t, err)
	root := t.TempDir()
	_, err = a.RestoreRepos(ctx, cp.Name(), root)
	require.NoError(t, err)
	target := filepath.Join(root, "alice", "demo.git")

	// Every file under objects/ removed, its directories and the refs left.
	var objects []string
	require.NoError(t, filepath.WalkDir(filepath.Join(target, "objects"), func(p string, d fs.DirEntry, err error) error {
		if err == nil && d.Type().IsRegular() {
			objects = append(objects, p)
		}
		return err
	}))
	require.NotEmpty(t, objects)
	for _, p := range objects {
		require.NoError(t, os.Remove(p))
	}
	under := func() []string {
		var out []string
		require.NoError(t, filepath.WalkDir(target, func(p string, _ fs.DirEntry, err error) error {
			if err == nil {
				out = append(out, p)
			}
			return err
		}))
		return out
	}
	before := under()
	_, err = a.RestoreRepos(ctx, cp.Name(), root)
	require.ErrorContains(t, err, "is not empty", "a repository missing its objects is not kept as restored")
	assert.Equal(t, before, under(), "nothing is removed")
}

// A target, or the owner's directory above it, that is a symbolic link is
// not this restore's: os.ReadDir follows the link, and taking the target up
// removed what the directory it names holds, wherever that is, mark or
// mark's temporary files there. The rerun refuses it, naming it, and
// neither follows nor removes anything. The directory the link names is a
// witness of the test, in t.TempDir() too.
func TestRestoreReposRefusesALinkedTarget(t *testing.T) {
	ctx := t.Context()
	c := newFakeCluster(t)
	o := newTestObserver(t, c, &hookLog{})
	cp := seed(t, c, o)
	require.NoError(t, o.Step(ctx))
	a, err := OpenArchive(o.cfg.Dir, "")
	require.NoError(t, err)

	// What the witness holds, by path relative to it: the bytes of a
	// regular file, "dir" for a directory.
	held := func(dir string) map[string]string {
		out := map[string]string{}
		require.NoError(t, filepath.WalkDir(dir, func(p string, d fs.DirEntry, err error) error {
			if err != nil {
				return err
			}
			rel, err := filepath.Rel(dir, p)
			if err != nil {
				return err
			}
			switch {
			case d.IsDir():
				out[rel] = "dir"
			case d.Type().IsRegular():
				b, err := os.ReadFile(p)
				if err != nil {
					return err
				}
				out[rel] = string(b)
			default:
				out[rel] = d.Type().String()
			}
			return nil
		}))
		return out
	}
	marked := func(dir string) error {
		if err := os.MkdirAll(filepath.Join(dir, "objects"), 0o755); err != nil {
			return err
		}
		for name, body := range map[string]string{IncompleteMark: "", "precious": "x", "objects/kept": "y"} {
			if err := os.WriteFile(filepath.Join(dir, filepath.FromSlash(name)), []byte(body), 0o600); err != nil {
				return err
			}
		}
		return nil
	}
	leftovers := func(dir string) error {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return err
		}
		return os.WriteFile(filepath.Join(dir, "."+IncompleteMark+".123"), nil, 0o600)
	}

	for _, tc := range []struct {
		name string
		// link is the path under root made a symbolic link to the
		// witness, which fill writes in at what the link stands for.
		link, under string
		fill        func(dir string) error
	}{
		{"the target, marked", "alice/demo.git", "", marked},
		{"the target, holding the mark's temporary files alone", "alice/demo.git", "", leftovers},
		{"the owner's directory, the target marked", "alice", "demo.git", marked},
	} {
		t.Run(tc.name, func(t *testing.T) {
			root := t.TempDir()
			witness := filepath.Join(t.TempDir(), "witness")
			require.NoError(t, tc.fill(filepath.Join(witness, tc.under)))
			link := filepath.Join(root, filepath.FromSlash(tc.link))
			require.NoError(t, os.MkdirAll(filepath.Dir(link), 0o755))
			require.NoError(t, os.Symlink(witness, link))
			before := held(witness)

			_, err := a.RestoreRepos(ctx, cp.Name(), root)
			assert.Equal(t, before, held(witness), "nothing the link names is removed or written")
			require.Error(t, err, "a linked target is refused")
			assert.Contains(t, err.Error(), link, "the refusal names the link")
			info, err := os.Lstat(link)
			require.NoError(t, err, "the link itself is left")
			assert.Equal(t, fs.ModeSymlink, info.Mode().Type())
		})
	}
}
