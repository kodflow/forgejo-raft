// Copyright 2026 The Forgejo Authors. All rights reserved.
// SPDX-License-Identifier: GPL-3.0-or-later

package cmd

import (
	"bytes"
	"context"
	"io"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"forgejo.org/models/db"
	git_model "forgejo.org/models/git"
	repo_model "forgejo.org/models/repo"
	"forgejo.org/models/unittest"
	"forgejo.org/modules/git"
	"forgejo.org/modules/lfs"
	repo_module "forgejo.org/modules/repository"
	"forgejo.org/modules/setting"
	"forgejo.org/modules/storage"
	"forgejo.org/modules/test"

	"github.com/stretchr/testify/require"
)

// upstreamLFS is a mirror's upstream LFS server. With started set, each
// object's body gives its first half, then ends with the context, as an
// HTTP body does when the node shuts down (the mirror queue runs under
// ShutdownContext); without it, the whole object.
type upstreamLFS struct {
	content []byte
	started chan struct{}
	calls   int
}

func (c *upstreamLFS) BatchSize() int { return 20 }

func (c *upstreamLFS) Upload(context.Context, []lfs.Pointer, lfs.UploadCallback) error {
	return nil
}

func (c *upstreamLFS) Download(ctx context.Context, objects []lfs.Pointer, cb lfs.DownloadCallback) error {
	c.calls++
	for _, p := range objects {
		if c.started == nil {
			if err := cb(p, io.NopCloser(bytes.NewReader(c.content)), nil); err != nil {
				return err
			}
			continue
		}
		pr, pw := io.Pipe()
		go func() {
			_, _ = pw.Write(c.content[:len(c.content)/2])
			close(c.started)
			<-ctx.Done()
			pw.CloseWithError(ctx.Err())
		}()
		if err := cb(p, pr, nil); err != nil {
			return err
		}
	}
	return nil
}

// A mirror's (or a migration's) LFS synchronisation writes the object
// before it commits its lfs_meta_object row (StoreMissingLfsObjectsInRepository):
// a checkpoint taken while the object downloads holds neither the row in
// its SQL at L nor the file in its list, and step 6 of recover
// --from-observer accepts it. When the row came first, it refused it, and a
// node that died in that window (kill -9, an OOM kill, a power loss) left
// the row for good: the next synchronisation skipped an object whose row
// existed, and step 6 refused every checkpoint taken afterwards, verified
// and protected by the observers, with no way past it (#289).
func TestRecoverAcceptsACheckpointTakenDuringAnLFSDownload(t *testing.T) {
	require.NoError(t, unittest.PrepareTestDatabase())
	unittest.PrepareTestEnv(t)
	t.Cleanup(func() { unittest.PrepareTestEnv(t) })
	ctx := db.DefaultContext
	e := db.GetEngine(ctx)

	m := checkpointOfRestoredSQL(t)
	// The LFS server on, its storage local, as the observers require, in
	// this test's own directory.
	lfsStorage := &setting.Storage{Type: setting.LocalStorageType, Path: filepath.Join(t.TempDir(), "lfs")}
	t.Cleanup(test.MockVariableValue(&setting.LFS.Storage, lfsStorage))
	t.Cleanup(test.MockVariableValue(&setting.LFS.StartServer, true))
	local, err := storage.NewStorage(setting.LocalStorageType, lfsStorage)
	require.NoError(t, err)
	t.Cleanup(test.MockVariableValue(&storage.LFS, local))
	// Only the object of this test is named by SQL.
	_, err = e.Exec("DELETE FROM lfs_meta_object")
	require.NoError(t, err)

	// A mirror whose history holds one LFS pointer.
	content := bytes.Repeat([]byte("large lfs object "), 4096)
	p, err := lfs.GeneratePointer(bytes.NewReader(content))
	require.NoError(t, err)
	bare := filepath.Join(t.TempDir(), "mirror.git")
	out, err := exec.Command("git", "init", "-q", "--bare", bare).CombinedOutput()
	require.NoError(t, err, "%s", out)
	hash := exec.Command("git", "-C", bare, "hash-object", "-w", "--stdin")
	hash.Stdin = strings.NewReader(p.StringContent())
	out, err = hash.CombinedOutput()
	require.NoError(t, err, "%s", out)
	gitRepo, err := git.OpenRepository(ctx, bare)
	require.NoError(t, err)
	defer gitRepo.Close()
	repo := unittest.AssertExistsAndLoadBean(t, &repo_model.Repository{ID: 1})
	rows := func() int64 {
		n, err := e.Where("oid = ?", p.Oid).Count(new(git_model.LFSMetaObject))
		require.NoError(t, err)
		return n
	}
	present := func() bool {
		ok, err := lfs.NewContentStore().Exists(p)
		require.NoError(t, err)
		return ok
	}

	// The synchronisation, in the mirror queue's context, halfway through
	// the object.
	syncCtx, shutdown := context.WithCancel(ctx)
	defer shutdown()
	upstream := &upstreamLFS{content: content, started: make(chan struct{})}
	done := make(chan error, 1)
	go func() { done <- repo_module.StoreMissingLfsObjectsInRepository(syncCtx, repo, gitRepo, upstream) }()
	select {
	case <-upstream.started:
	case <-time.After(30 * time.Second):
		t.Fatal("the download never started")
	}

	// A checkpoint now: SQL at L does not name the object yet, and step 6
	// accepts it.
	var stepOut bytes.Buffer
	require.NoError(t, verifyRecovered(ctx, m, &stepOut, false), "step 6 on the checkpoint taken during the download")
	require.Zero(t, rows(), "the row is committed before its file is written")
	require.False(t, present())

	// The node dies there (kill -9, an OOM kill, a power loss): the
	// synchronisation runs no more code, which the frozen goroutine stands
	// for. The next synchronisation, on the restarted node, downloads the
	// object, which no row names yet.
	next := &upstreamLFS{content: content}
	require.NoError(t, repo_module.StoreMissingLfsObjectsInRepository(ctx, repo, gitRepo, next))
	require.Equal(t, 1, next.calls, "the object is downloaded again")
	require.True(t, present())
	require.Equal(t, int64(1), rows())

	// Every later checkpoint holds the row and its file.
	stepOut.Reset()
	require.NoError(t, verifyRecovered(ctx, m, &stepOut, false), "step 6 on a later checkpoint")
	require.Contains(t, stepOut.String(), "verified: 1 LFS objects present")

	// The frozen synchronisation, stopped gracefully, fails its own write
	// and removes nothing: neither the row nor the file the next one
	// stored, which the content addresses.
	shutdown()
	select {
	case <-done:
	case <-time.After(30 * time.Second):
		t.Fatal("the synchronisation did not end")
	}
	require.Equal(t, int64(1), rows())
	require.True(t, present())
}
