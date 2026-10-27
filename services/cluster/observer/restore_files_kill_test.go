// Copyright 2026 The Forgejo Authors. All rights reserved.
// SPDX-License-Identifier: GPL-3.0-or-later

//go:build linux || darwin

package observer

import (
	"errors"
	"io/fs"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"forgejo.org/modules/clusterhook"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// What the child half of TestRestoreFilesRerunAfterAKillLeavesNoTemporaryFile
// reads from its environment.
const (
	killedRestoreDir        = "FORGEJO_TEST_KILLED_RESTORE_DIR"
	killedRestoreCheckpoint = "FORGEJO_TEST_KILLED_RESTORE_CHECKPOINT"
	killedRestoreRoot       = "FORGEJO_TEST_KILLED_RESTORE_ROOT"
)

// The child half: it runs RestoreFiles, step 4 of recover --from-observer,
// and is killed by the parent while it copies the LFS object.
func TestRestoreFilesKilledChild(t *testing.T) {
	dir := os.Getenv(killedRestoreDir)
	if dir == "" {
		t.Skip("the child half of TestRestoreFilesRerunAfterAKillLeavesNoTemporaryFile")
	}
	a, err := OpenArchive(dir, "git")
	require.NoError(t, err)
	_, err = a.RestoreFiles(os.Getenv(killedRestoreCheckpoint), map[string]string{"lfs": os.Getenv(killedRestoreRoot)})
	t.Fatalf("the child was not killed: %v", err)
}

// A crash during step 4 (kill -9, an OOM kill, a power loss) leaves the
// temporary file of the copy it was making, ".<name>.<random>", beside the
// LFS object. The restored cluster's leader lists every regular file of its
// LFS root outside tmp/, and the observers refuse an LFS path that is not a
// digest: had the rerun kept it, no checkpoint of the restored cluster would
// ever be ingested (#290). The rerun removes it.
func TestRestoreFilesRerunAfterAKillLeavesNoTemporaryFile(t *testing.T) {
	ctx := t.Context()
	c := newFakeCluster(t)
	o := newTestObserver(t, c, &hookLog{})
	seed(t, c, o)
	require.NoError(t, o.Step(ctx))
	st := o.State()
	name := st.Checkpoints[len(st.Checkpoints)-1].Name
	m, err := o.store.ReadManifest(name)
	require.NoError(t, err)
	var lfs FileBlob
	for _, f := range m.Files {
		if f.Storage == "lfs" {
			lfs = f
		}
	}
	require.NotEmpty(t, lfs.SHA256, "the checkpoint holds an LFS object")

	// The blob becomes a FIFO the parent feeds: the child's copy blocks
	// after the first bytes, with its temporary file written in the target.
	blob := o.store.blobPath(lfs.SHA256)
	content, err := os.ReadFile(blob)
	require.NoError(t, err)
	require.NoError(t, os.Remove(blob))
	require.NoError(t, syscall.Mkfifo(blob, 0o600))

	lfsRoot := filepath.Join(t.TempDir(), "lfs")
	cmd := exec.Command(os.Args[0], "-test.run=^TestRestoreFilesKilledChild$", "-test.count=1")
	cmd.Env = append(os.Environ(), killedRestoreDir+"="+o.cfg.Dir, killedRestoreCheckpoint+"="+name, killedRestoreRoot+"="+lfsRoot)
	require.NoError(t, cmd.Start())
	// A failure below never leaves the child behind.
	t.Cleanup(func() {
		_ = cmd.Process.Kill()
		_ = cmd.Wait()
	})
	opened := make(chan *os.File, 1)
	go func() {
		w, err := os.OpenFile(blob, os.O_WRONLY, 0)
		if err != nil {
			opened <- nil
			return
		}
		opened <- w
	}()
	var w *os.File
	select {
	case w = <-opened:
	case <-time.After(30 * time.Second):
		t.Fatal("the child never opened the blob")
	}
	require.NotNil(t, w)
	defer w.Close()
	_, err = w.Write(content[:5])
	require.NoError(t, err)

	// Every regular file under the restored LFS root but the object.
	others := func() []string {
		var out []string
		require.NoError(t, filepath.WalkDir(lfsRoot, func(p string, d fs.DirEntry, err error) error {
			if errors.Is(err, fs.ErrNotExist) && p == lfsRoot {
				return nil
			}
			if err != nil {
				return err
			}
			if d.Type().IsRegular() {
				rel, err := filepath.Rel(lfsRoot, p)
				if err != nil {
					return err
				}
				if rel = filepath.ToSlash(rel); rel != lfs.Path {
					out = append(out, rel)
				}
			}
			return nil
		}))
		return out
	}
	copying := func() bool {
		for _, rel := range others() {
			if info, err := os.Stat(filepath.Join(lfsRoot, filepath.FromSlash(rel))); err == nil && info.Size() > 0 && strings.HasPrefix(filepath.Base(rel), ".") {
				return true
			}
		}
		return false
	}
	deadline := time.Now().Add(30 * time.Second)
	for !copying() && time.Now().Before(deadline) {
		time.Sleep(20 * time.Millisecond)
	}
	require.True(t, copying(), "the child is copying into its temporary file")
	// The crash.
	require.NoError(t, cmd.Process.Kill())
	_ = cmd.Wait()
	left := others()
	t.Logf("left in the restored LFS storage by the kill: %v", left)
	require.Len(t, left, 1)

	// The blob is back, and the operator runs the same command again.
	require.NoError(t, os.Remove(blob))
	require.NoError(t, os.WriteFile(blob, content, 0o600))
	a, err := OpenArchive(o.cfg.Dir, "git")
	require.NoError(t, err)
	n, err := a.RestoreFiles(name, map[string]string{"lfs": lfsRoot})
	require.NoError(t, err, "the rerun of step 4 succeeds")
	require.Equal(t, len(m.Files), n)
	got, err := os.ReadFile(filepath.Join(lfsRoot, filepath.FromSlash(lfs.Path)))
	require.NoError(t, err)
	require.Equal(t, content, got)
	assert.Empty(t, others(), "the rerun leaves the object alone in the LFS storage")

	// The restored cluster's leader lists every regular file of its LFS
	// root: the observer takes each checkpoint of what the rerun left.
	listed := map[string][]byte{}
	require.NoError(t, filepath.WalkDir(lfsRoot, func(p string, d fs.DirEntry, err error) error {
		if err != nil || !d.Type().IsRegular() {
			return err
		}
		rel, err := filepath.Rel(lfsRoot, p)
		if err != nil {
			return err
		}
		b, err := os.ReadFile(p)
		if err != nil {
			return err
		}
		_, err = expectedDigest(clusterhook.FileEntry{Storage: "lfs", Path: filepath.ToSlash(rel), Size: int64(len(b))})
		require.NoError(t, err, "lfs/%s", rel)
		listed["lfs/"+filepath.ToSlash(rel)] = b
		return nil
	}))
	c.mu.Lock()
	c.files = listed
	c.mu.Unlock()
	for i := range 3 {
		before := o.lastIndex()
		c.push("alice/demo.git", "refs/heads/main", c.commit("after "+string(rune('a'+i))))
		c.checkpoint()
		require.NoError(t, o.Step(ctx), "pass %d", i+1)
		assert.Greater(t, o.lastIndex(), before, "pass %d ingests the new checkpoint", i+1)
	}
}
