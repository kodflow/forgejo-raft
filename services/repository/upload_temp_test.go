// Copyright 2026 The Forgejo Authors. All rights reserved.
// SPDX-License-Identifier: GPL-3.0-or-later

package repository

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"
	"time"

	"forgejo.org/models/db"
	repo_model "forgejo.org/models/repo"
	system_model "forgejo.org/models/system"
	"forgejo.org/models/unittest"
	user_model "forgejo.org/models/user"
	"forgejo.org/modules/setting"
	"forgejo.org/modules/test"
	files_service "forgejo.org/services/repository/files"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// uploadedFile is the multipart.File the web form hands repo_model.NewUpload.
type uploadedFile struct{ *bytes.Reader }

func (uploadedFile) Close() error { return nil }

// newWebUpload is the dropzone's request of the web editor: the file under
// [repository.upload] TEMP_PATH, its row in SQL.
func newWebUpload(t *testing.T, name string) *repo_model.Upload {
	t.Helper()
	content := []byte("uploaded through the web: " + name + "\n")
	up, err := repo_model.NewUpload(db.DefaultContext, name, content[:4], uploadedFile{bytes.NewReader(content[4:])})
	require.NoError(t, err)
	return up
}

// startNode runs what a node's start does to TEMP_PATH.
func startNode(cluster bool) {
	defer test.MockVariableValue(&setting.Cluster.Enabled, cluster)()
	cleanUpUploadTempPath(db.DefaultContext)
}

// "Upload file" in the web editor is two requests: the dropzone's
// (UploadFileToServer) and the form's (UploadFilePost, which commits the
// file). In a cluster any node takes either request, so TEMP_PATH is on
// the storage every node mounts (#314): a node that starts between the
// two requests must leave the file another node took in place.
func TestStartKeepsAnUploadInFlightInCluster(t *testing.T) {
	require.NoError(t, unittest.PrepareTestDatabase())
	defer test.MockVariableValue(&setting.Repository.Upload.TempPath, t.TempDir())()
	repo := unittest.AssertExistsAndLoadBean(t, &repo_model.Repository{ID: 1})
	doer := unittest.AssertExistsAndLoadBean(t, &user_model.User{ID: 2})

	up := newWebUpload(t, "in-flight.txt") // the dropzone's request, on node A
	startNode(true)                        // node B starts

	// The form's request, on whichever node.
	err := files_service.UploadRepoFiles(db.DefaultContext, repo, doer, &files_service.UploadRepoFileOptions{
		OldBranch: "master", NewBranch: "upload-in-flight", Message: "upload " + up.Name,
		Files: []string{up.UUID}, FullPaths: []string{up.Name},
	})
	require.NoError(t, err, "a node that starts removes the file of an upload another node is serving")
	assert.NoFileExists(t, up.LocalPath(), "the commit removes the upload's file")
}

// What a starting node of a cluster still removes: the files older than a
// day, never what a link points to, and no directory, since another node
// may be creating a file in it.
func TestStartRemovesOldUploadsInCluster(t *testing.T) {
	require.NoError(t, unittest.PrepareTestDatabase())
	root := t.TempDir()
	defer test.MockVariableValue(&setting.Repository.Upload.TempPath, root)()

	young := newWebUpload(t, "young.txt")
	old := newWebUpload(t, "old.txt")
	past := time.Now().Add(-clusterUploadTempMaxAge - time.Hour)
	require.NoError(t, os.Chtimes(old.LocalPath(), past, past))

	// A link inside TEMP_PATH to a witness directory beside it, with an old
	// file in it.
	witness := t.TempDir()
	kept := filepath.Join(witness, "kept.txt")
	require.NoError(t, os.WriteFile(kept, []byte("outside TEMP_PATH\n"), 0o600))
	require.NoError(t, os.Chtimes(kept, past, past))
	require.NoError(t, os.Symlink(witness, filepath.Join(root, "link")))

	startNode(true)

	assert.FileExists(t, young.LocalPath(), "an upload of the last day stays")
	assert.NoFileExists(t, old.LocalPath(), "an upload older than a day goes")
	assert.DirExists(t, filepath.Dir(old.LocalPath()), "directories stay: another node may be writing into one")
	assert.FileExists(t, kept, "a link is never followed")
	_, err := os.Lstat(filepath.Join(root, "link"))
	require.NoError(t, err, "nor removed: only regular files are")
}

// Outside a cluster nothing but this process uses TEMP_PATH: its start
// empties it, as it always did.
func TestStartEmptiesUploadsOutsideCluster(t *testing.T) {
	require.NoError(t, unittest.PrepareTestDatabase())
	root := t.TempDir()
	defer test.MockVariableValue(&setting.Repository.Upload.TempPath, root)()

	up := newWebUpload(t, "standalone.txt")
	startNode(false)

	assert.NoFileExists(t, up.LocalPath())
	assert.NoDirExists(t, root)
}

// A cluster node whose TEMP_PATH was never created starts without a notice.
func TestStartWithoutUploadsInCluster(t *testing.T) {
	require.NoError(t, unittest.PrepareTestDatabase())
	defer test.MockVariableValue(&setting.Repository.Upload.TempPath, filepath.Join(t.TempDir(), "never-created"))()
	before := unittest.GetCount(t, &system_model.Notice{})

	startNode(true)

	assert.Equal(t, before, unittest.GetCount(t, &system_model.Notice{}))
}
