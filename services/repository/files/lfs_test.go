// Copyright 2026 The Forgejo Authors. All rights reserved.
// SPDX-License-Identifier: GPL-3.0-or-later

package files

import (
	"bytes"
	"errors"
	"io"
	"os"
	"path"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"forgejo.org/models/db"
	git_model "forgejo.org/models/git"
	repo_model "forgejo.org/models/repo"
	"forgejo.org/models/unittest"
	user_model "forgejo.org/models/user"
	"forgejo.org/modules/lfs"
	"forgejo.org/modules/setting"
	"forgejo.org/modules/storage"
	"forgejo.org/modules/test"

	gouuid "github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// heldLFSStorage holds each write of an LFS object until the test says
// what it does: nil writes it, an error fails it.
type heldLFSStorage struct {
	storage.ObjectStorage
	saving  chan string
	release chan error
}

func (s *heldLFSStorage) Save(p string, r io.Reader, size int64) (int64, error) {
	s.saving <- p
	if err := <-s.release; err != nil {
		return 0, err
	}
	return s.ObjectStorage.Save(p, r, size)
}

// lfsWriters are the services that store a file of the web or of the API
// in LFS: an upload and a change, each of content onto branch lfs of
// repo1, whose .gitattributes sends *.bin to LFS, as the new branch name.
func lfsWriters(t *testing.T) (map[string]func(name string, content []byte) error, *heldLFSStorage) {
	t.Helper()
	ctx := db.DefaultContext
	repo := unittest.AssertExistsAndLoadBean(t, &repo_model.Repository{ID: 1})
	doer := unittest.AssertExistsAndLoadBean(t, &user_model.User{ID: 2})
	t.Cleanup(test.MockVariableValue(&setting.Repository.Upload.TempPath, t.TempDir()))
	lfsStorage := &setting.Storage{Type: setting.LocalStorageType, Path: filepath.Join(t.TempDir(), "lfs")}
	t.Cleanup(test.MockVariableValue(&setting.LFS.Storage, lfsStorage))
	t.Cleanup(test.MockVariableValue(&setting.LFS.StartServer, true))
	local, err := storage.NewStorage(setting.LocalStorageType, lfsStorage)
	require.NoError(t, err)
	t.Cleanup(test.MockVariableValue(&storage.LFS, local))

	_, err = ChangeRepoFiles(ctx, repo, doer, &ChangeRepoFilesOptions{
		OldBranch: "master", NewBranch: "lfs", Message: "track *.bin in LFS",
		Files: []*ChangeRepoFile{{
			Operation: "create", TreePath: ".gitattributes",
			ContentReader: strings.NewReader("*.bin filter=lfs diff=lfs merge=lfs -text\n"),
		}},
	})
	require.NoError(t, err)

	held := &heldLFSStorage{ObjectStorage: local, saving: make(chan string), release: make(chan error)}
	t.Cleanup(test.MockVariableValue(&storage.LFS, storage.ObjectStorage(held)))

	return map[string]func(name string, content []byte) error{
		"UploadRepoFiles": func(name string, content []byte) error {
			up := &repo_model.Upload{UUID: gouuid.New().String(), Name: name + ".bin"}
			require.NoError(t, os.MkdirAll(path.Dir(up.LocalPath()), 0o755))
			require.NoError(t, os.WriteFile(up.LocalPath(), content, 0o644))
			require.NoError(t, db.Insert(ctx, up))
			return UploadRepoFiles(ctx, repo, doer, &UploadRepoFileOptions{
				OldBranch: "lfs", NewBranch: name, Message: "upload " + name,
				Files: []string{up.UUID}, FullPaths: []string{up.Name},
			})
		},
		"ChangeRepoFiles": func(name string, content []byte) error {
			_, err := ChangeRepoFiles(ctx, repo, doer, &ChangeRepoFilesOptions{
				OldBranch: "lfs", NewBranch: name, Message: "change " + name,
				Files: []*ChangeRepoFile{{
					Operation: "create", TreePath: name + ".bin",
					ContentReader: bytes.NewReader(content),
				}},
			})
			return err
		},
	}, held
}

// lfsState returns how many lfs_meta_object rows name content's object,
// and whether its file is stored.
func lfsState(t *testing.T, content []byte) (int64, bool) {
	t.Helper()
	p, err := lfs.GeneratePointer(bytes.NewReader(content))
	require.NoError(t, err)
	rows, err := db.GetEngine(db.DefaultContext).Where("oid = ?", p.Oid).Count(new(git_model.LFSMetaObject))
	require.NoError(t, err)
	stored, err := lfs.NewContentStore().Exists(p)
	require.NoError(t, err)
	return rows, stored
}

// An upload or a change stored in LFS writes the object before it commits
// its lfs_meta_object row, as the LFS server does: a checkpoint taken while
// the object is written, or a node that dies then, holds no row without its
// file. When the row came first, step 6 of recover --from-observer refused
// that checkpoint, and every later one after such a death (#289).
func TestLFSWritersStoreTheObjectBeforeItsRow(t *testing.T) {
	unittest.PrepareTestEnv(t)
	writers, held := lfsWriters(t)

	for name, write := range writers {
		content := []byte("the object of " + name + "\n")
		done := make(chan error, 1)
		go func() { done <- write("held-"+name, content) }()
		select {
		case <-held.saving:
		case err := <-done:
			t.Fatalf("%s returned before writing its object: %v", name, err)
		case <-time.After(time.Minute):
			t.Fatalf("%s never wrote its object", name)
		}
		rows, stored := lfsState(t, content)
		assert.Zero(t, rows, "%s committed the row before its file", name)
		assert.False(t, stored, name)

		held.release <- nil
		require.NoError(t, <-done, name)
		rows, stored = lfsState(t, content)
		assert.Equal(t, int64(1), rows, name)
		assert.True(t, stored, name)
	}
}

// A write of the object that fails leaves no row naming it.
func TestLFSWritersLeaveNoRowWhenTheObjectFails(t *testing.T) {
	unittest.PrepareTestEnv(t)
	writers, held := lfsWriters(t)

	for name, write := range writers {
		content := []byte("the failed object of " + name + "\n")
		done := make(chan error, 1)
		go func() { done <- write("failed-"+name, content) }()
		select {
		case <-held.saving:
		case err := <-done:
			t.Fatalf("%s returned before writing its object: %v", name, err)
		case <-time.After(time.Minute):
			t.Fatalf("%s never wrote its object", name)
		}
		held.release <- errors.New("no space left on device")
		require.ErrorContains(t, <-done, "no space left on device", name)
		rows, stored := lfsState(t, content)
		assert.Zero(t, rows, name)
		assert.False(t, stored, name)
	}
}
