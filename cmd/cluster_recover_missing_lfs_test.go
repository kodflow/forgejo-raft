// Copyright 2026 The Forgejo Authors. All rights reserved.
// SPDX-License-Identifier: GPL-3.0-or-later

package cmd

import (
	"bytes"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"forgejo.org/models/db"
	git_model "forgejo.org/models/git"
	repo_model "forgejo.org/models/repo"
	"forgejo.org/models/unittest"
	"forgejo.org/modules/clusterhook"
	"forgejo.org/modules/git"
	"forgejo.org/modules/lfs"
	"forgejo.org/modules/log"
	repo_module "forgejo.org/modules/repository"
	"forgejo.org/modules/setting"
	"forgejo.org/modules/storage"
	"forgejo.org/modules/test"
	"forgejo.org/services/cluster/observer"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// lfsObject is an LFS object of the bench: its content and its pointer.
type lfsObject struct {
	content []byte
	pointer lfs.Pointer
}

// newLFSObject makes an LFS object out of a word repeated.
func newLFSObject(t *testing.T, word string) lfsObject {
	t.Helper()
	content := bytes.Repeat([]byte(word+" "), 4096)
	p, err := lfs.GeneratePointer(bytes.NewReader(content))
	require.NoError(t, err)
	return lfsObject{content: content, pointer: *p}
}

// prepareOrphanLFS sets the bench up and returns the checkpoint of its
// SQL: the LFS server on, its storage local, as the observers require, in
// the test's directory, and no lfs_meta_object row but those the test
// commits.
func prepareOrphanLFS(t *testing.T) *observer.Manifest {
	t.Helper()
	require.NoError(t, unittest.PrepareTestDatabase())
	unittest.PrepareTestEnv(t)
	t.Cleanup(func() { unittest.PrepareTestEnv(t) })
	m := checkpointOfRestoredSQL(t)
	lfsStorage := &setting.Storage{Type: setting.LocalStorageType, Path: filepath.Join(t.TempDir(), "lfs")}
	t.Cleanup(test.MockVariableValue(&setting.LFS.Storage, lfsStorage))
	t.Cleanup(test.MockVariableValue(&setting.LFS.StartServer, true))
	local, err := storage.NewStorage(setting.LocalStorageType, lfsStorage)
	require.NoError(t, err)
	t.Cleanup(test.MockVariableValue(&storage.LFS, local))
	_, err = db.GetEngine(db.DefaultContext).Exec("DELETE FROM lfs_meta_object")
	require.NoError(t, err)
	return m
}

// mirrorHolding is a bare repository, in the test's directory, whose
// history holds the pointer of each object.
func mirrorHolding(t *testing.T, objects ...lfsObject) *git.Repository {
	t.Helper()
	bare := filepath.Join(t.TempDir(), "mirror.git")
	out, err := exec.Command("git", "init", "-q", "--bare", bare).CombinedOutput()
	require.NoError(t, err, "%s", out)
	for _, o := range objects {
		hash := exec.Command("git", "-C", bare, "hash-object", "-w", "--stdin")
		hash.Stdin = strings.NewReader(o.pointer.StringContent())
		out, err = hash.CombinedOutput()
		require.NoError(t, err, "%s", out)
	}
	gitRepo, err := git.OpenRepository(db.DefaultContext, bare)
	require.NoError(t, err)
	t.Cleanup(func() { gitRepo.Close() })
	return gitRepo
}

// lfsRows counts the rows naming oid.
func lfsRows(t *testing.T, oid string) int64 {
	t.Helper()
	n, err := db.GetEngine(db.DefaultContext).Where("oid = ?", oid).Count(new(git_model.LFSMetaObject))
	require.NoError(t, err)
	return n
}

// lfsStored reports whether the LFS storage holds the object's file.
func lfsStored(t *testing.T, p lfs.Pointer) bool {
	t.Helper()
	ok, err := lfs.NewContentStore().Exists(p)
	require.NoError(t, err)
	return ok
}

// withCluster sets clusterhook's mode for the test.
func withCluster(t *testing.T, on bool) {
	t.Helper()
	was := clusterhook.Enabled()
	clusterhook.SetEnabled(on)
	t.Cleanup(func() { clusterhook.SetEnabled(was) })
}

// A row of lfs_meta_object without its file, left by a node that died
// between the two before #301 wrote the file first, made every later
// checkpoint unrestorable at step 6 of recover --from-observer, and the
// mirror's synchronisation skipped the object for good, since its row
// exists (#302). In a cluster, the synchronisation checks such a row
// against the storage and downloads the object again, through the content
// store every LFS write of the cluster takes: the checkpoints taken
// afterwards restore.
func TestAClusterSyncDownloadsAgainAnLFSObjectWhoseRowHasNoFile(t *testing.T) {
	m := prepareOrphanLFS(t)
	withCluster(t, true)
	ctx := db.DefaultContext
	orphan, kept := newLFSObject(t, "orphan"), newLFSObject(t, "kept")
	gitRepo := mirrorHolding(t, orphan, kept)
	repo := unittest.AssertExistsAndLoadBean(t, &repo_model.Repository{ID: 1})

	// The row of orphan without its file; kept whole.
	_, err := git_model.NewLFSMetaObject(ctx, repo.ID, orphan.pointer)
	require.NoError(t, err)
	require.NoError(t, lfs.NewContentStore().Put(kept.pointer, bytes.NewReader(kept.content)))
	_, err = git_model.NewLFSMetaObject(ctx, repo.ID, kept.pointer)
	require.NoError(t, err)
	var stepOut bytes.Buffer
	require.ErrorContains(t, verifyRecovered(ctx, m, &stepOut, verifyDecisions{}), "the LFS object "+orphan.pointer.Oid+" is in SQL and not in the restored storage")

	upstream := &upstreamLFS{content: orphan.content}
	require.NoError(t, repo_module.StoreMissingLfsObjectsInRepository(ctx, repo, gitRepo, upstream))
	assert.Equal(t, 1, upstream.calls, "the object whose row has no file is downloaded again, the whole one is not")
	assert.True(t, lfsStored(t, orphan.pointer))
	assert.Equal(t, int64(1), lfsRows(t, orphan.pointer.Oid), "its row, kept as it was")
	assert.Equal(t, int64(1), lfsRows(t, kept.pointer.Oid))

	// The checkpoints taken from then on restore.
	stepOut.Reset()
	require.NoError(t, verifyRecovered(ctx, m, &stepOut, verifyDecisions{}), stepOut.String())
	assert.Contains(t, stepOut.String(), "verified: 2 LFS objects present")
}

// Outside a cluster, the synchronisation is Forgejo's: an object whose row
// exists is skipped, its file not looked for (#302).
func TestASyncOutsideAClusterSkipsAnLFSObjectWhoseRowExists(t *testing.T) {
	prepareOrphanLFS(t)
	withCluster(t, false)
	ctx := db.DefaultContext
	orphan := newLFSObject(t, "orphan")
	gitRepo := mirrorHolding(t, orphan)
	repo := unittest.AssertExistsAndLoadBean(t, &repo_model.Repository{ID: 1})
	_, err := git_model.NewLFSMetaObject(ctx, repo.ID, orphan.pointer)
	require.NoError(t, err)

	upstream := &upstreamLFS{content: orphan.content}
	require.NoError(t, repo_module.StoreMissingLfsObjectsInRepository(ctx, repo, gitRepo, upstream))
	assert.Zero(t, upstream.calls, "nothing downloaded")
	assert.False(t, lfsStored(t, orphan.pointer))
	assert.Equal(t, int64(1), lfsRows(t, orphan.pointer.Oid))
}

// Without --accept-missing-lfs, step 6 refuses a checkpoint whose SQL
// names an LFS object its storage lacks, as before, deletes nothing, and
// names the option in its refusal (#302).
func TestRecoverStepSixRefusesAMissingLFSObjectAndNamesTheOption(t *testing.T) {
	m := prepareOrphanLFS(t)
	ctx := db.DefaultContext
	orphan := newLFSObject(t, "orphan")
	_, err := git_model.NewLFSMetaObject(ctx, 1, orphan.pointer)
	require.NoError(t, err)

	var stepOut bytes.Buffer
	err = verifyRecovered(ctx, m, &stepOut, verifyDecisions{})
	require.ErrorContains(t, err, "the LFS object "+orphan.pointer.Oid+" is in SQL and not in the restored storage")
	require.ErrorContains(t, err, "--accept-missing-lfs")
	assert.Equal(t, int64(1), lfsRows(t, orphan.pointer.Oid), "nothing deleted")
	assert.NotContains(t, stepOut.String(), "revoked:", "the step stopped before the revocations")
}

// With --accept-missing-lfs, the operator's decision, step 6 lists every
// LFS object SQL names and the restored storage lacks, with each
// repository naming it, on its output and in the log, deletes those rows
// and goes on: a mirror's next synchronisation downloads the object again
// from its upstream, and an object pushed to the forge is to be pushed
// again. An object whose file is there keeps its row, and a resumed step
// finds nothing missing any more (#302).
func TestRecoverStepSixWithAcceptMissingLFSListsThenDeletesTheRows(t *testing.T) {
	m := prepareOrphanLFS(t)
	ctx := db.DefaultContext
	orphan, kept := newLFSObject(t, "orphan"), newLFSObject(t, "kept")
	for _, id := range []int64{1, 2} {
		_, err := git_model.NewLFSMetaObject(ctx, id, orphan.pointer)
		require.NoError(t, err)
	}
	require.NoError(t, lfs.NewContentStore().Put(kept.pointer, bytes.NewReader(kept.content)))
	_, err := git_model.NewLFSMetaObject(ctx, 1, kept.pointer)
	require.NoError(t, err)

	lc, cleanup := test.NewLogChecker(log.DEFAULT, log.WARN)
	defer cleanup()
	listed := []string{
		"the LFS object " + orphan.pointer.Oid + " of user2/repo1 (ID 1) is in SQL and not in the restored storage",
		"the LFS object " + orphan.pointer.Oid + " of user2/repo2 (ID 2) is in SQL and not in the restored storage",
	}
	lc.Filter(listed...).StopMark("deleted: 2 rows of 1 LFS objects the restored storage lacks (--accept-missing-lfs)")

	var stepOut bytes.Buffer
	require.NoError(t, verifyRecovered(ctx, m, &stepOut, verifyDecisions{acceptMissingLFS: true}), stepOut.String())
	out := stepOut.String()
	for _, line := range listed {
		assert.Contains(t, out, line, "listed on the output")
	}
	assert.Contains(t, out, "deleted: 2 rows of 1 LFS objects the restored storage lacks (--accept-missing-lfs)")
	assert.Contains(t, out, "verified: 1 LFS objects present")
	assert.Contains(t, out, "revoked:", "the step went on")
	// Listed before they are deleted.
	assert.Less(t, strings.Index(out, listed[1]), strings.Index(out, "deleted: "))
	filtered, stopped := lc.Check(5 * time.Second)
	require.True(t, stopped, "the deletion is logged")
	for i, line := range listed {
		assert.True(t, filtered[i], "logged: %s", line)
	}
	assert.Zero(t, lfsRows(t, orphan.pointer.Oid), "the rows without a file are deleted")
	assert.Equal(t, int64(1), lfsRows(t, kept.pointer.Oid), "the row whose file is there is kept")

	// Resumed, the step finds nothing missing any more.
	stepOut.Reset()
	require.NoError(t, verifyRecovered(ctx, m, &stepOut, verifyDecisions{}), stepOut.String())
	assert.Contains(t, stepOut.String(), "verified: 1 LFS objects present")

	// The option of the command.
	var names []string
	for _, f := range recoverFromObserverFlags() {
		names = append(names, f.Names()...)
	}
	assert.Contains(t, names, "accept-missing-lfs")
}

// A file the restored storage cannot tell missing, its path unreadable,
// refuses step 6 with --accept-missing-lfs too: only a file the storage
// says it does not hold has its row deleted (#302).
func TestRecoverStepSixKeepsTheRowOfAnUnreadableLFSFile(t *testing.T) {
	m := prepareOrphanLFS(t)
	ctx := db.DefaultContext
	orphan := newLFSObject(t, "orphan")
	_, err := git_model.NewLFSMetaObject(ctx, 1, orphan.pointer)
	require.NoError(t, err)
	// A file where the storage keeps the object's first directory: the
	// object's path is not a directory's, and stat says so, not that the
	// file is missing.
	oid := orphan.pointer.Oid
	require.NoError(t, os.MkdirAll(setting.LFS.Storage.Path, 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(setting.LFS.Storage.Path, oid[:2]), []byte("not a directory"), 0o644))

	var stepOut bytes.Buffer
	err = verifyRecovered(ctx, m, &stepOut, verifyDecisions{acceptMissingLFS: true})
	require.ErrorContains(t, err, "the LFS object "+oid+" is in SQL and its file in the restored storage cannot be read")
	assert.Equal(t, int64(1), lfsRows(t, oid), "its row is kept")
	assert.NotContains(t, stepOut.String(), "deleted:")
}
