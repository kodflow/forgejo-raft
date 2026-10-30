// Copyright 2026 The Forgejo Authors. All rights reserved.
// SPDX-License-Identifier: GPL-3.0-or-later

package cluster

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"forgejo.org/models/db"
	repo_model "forgejo.org/models/repo"
	"forgejo.org/models/unittest"
	"forgejo.org/modules/clusterhook"
	"forgejo.org/modules/setting"
	"forgejo.org/modules/storage"
	"forgejo.org/modules/test"
	repo_service "forgejo.org/services/repository"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// `forgejo admin user delete --purge` runs in a process of its own, without
// a replicator, and deletes every repository of the user through
// repo_service.DeleteOwnerRepositoriesDirectly -> DeleteRepositoryDirectly:
// the rows go in SQL, then the attachments and the LFS objects through
// storage.Attachments and storage.LFS. #358 registered the deletion gate
// of such a process in `forgejo doctor` only, and the purge deleted those
// files at once while a checkpoint listed them: the checkpoint missed a
// file the SQL at its L names, and `recover --from-observer` refused it at
// step 6 (#363). Every command of the CLI but `forgejo web` registers it
// now (cmd's prepareWorkPathAndCustomConf, tested there).
//
// A checkpoint takes the cut and reads L; the purge of user2/repo1 commits
// its SQL after L and deletes the repository's attachments; the
// checkpoint's walk then lists the stored files. The purge is the real
// DeleteRepositoryDirectly, in a process set as the CLI leaves it:
// [cluster] enabled, no replicator (clusterhook disabled, no cut hold, no
// lifecycle), and the gate every command registers, on the bench's cut.
// Every file the deletion touches lives under t.TempDir().
func TestCLIPurgeDeletesStoredFilesUnderTheCut(t *testing.T) {
	require.NoError(t, unittest.PrepareTestDatabase())
	ctx := context.Background()
	root := t.TempDir()
	local := func(dir string) storage.ObjectStorage {
		st, err := storage.NewLocalStorage(ctx, &setting.Storage{Type: setting.LocalStorageType, Path: filepath.Join(root, dir)})
		require.NoError(t, err)
		return st
	}
	attCfg := &setting.Storage{Type: setting.LocalStorageType, Path: filepath.Join(root, "attachments")}
	attLocal, err := storage.NewLocalStorage(ctx, attCfg)
	require.NoError(t, err)
	t.Cleanup(test.MockVariableValue(&setting.RepoRootPath, filepath.Join(root, "repos")))
	t.Cleanup(test.MockVariableValue(&storage.Attachments, storage.WithDeleteGate(attLocal)))
	t.Cleanup(test.MockVariableValue(&storage.LFS, storage.WithDeleteGate(local("lfs"))))
	t.Cleanup(test.MockVariableValue(&storage.Packages, storage.WithDeleteGate(local("packages"))))
	t.Cleanup(test.MockVariableValue(&storage.Avatars, local("avatars")))
	t.Cleanup(test.MockVariableValue(&storage.RepoAvatars, local("repo-avatars")))
	t.Cleanup(test.MockVariableValue(&storage.RepoArchives, local("repo-archive")))
	t.Cleanup(test.MockVariableValue(&storage.Actions, local("actions_log")))
	t.Cleanup(test.MockVariableValue(&storage.ActionsArtifacts, local("actions_artifacts")))
	// The CLI's process on a node of a cluster.
	t.Cleanup(test.MockVariableValue(&setting.Cluster.Enabled, true))
	require.False(t, clusterhook.Enabled(), "the CLI runs no replicator")

	// The attachments of user2/repo1, as files.
	var atts []*repo_model.Attachment
	require.NoError(t, db.GetEngine(db.DefaultContext).Where("repo_id = ?", 1).Find(&atts))
	require.NotEmpty(t, atts)
	paths := make([]string, 0, len(atts))
	for _, a := range atts {
		_, err := storage.Attachments.Save(a.RelativePath(), strings.NewReader("1234567"), 7)
		require.NoError(t, err)
		paths = append(paths, a.RelativePath())
	}

	tc := newTestCluster(t, "node-a", "node-b", "node-c")
	pg := newFakePG()
	coord := withCheckpoints(t, tc, pg)
	hook := &markHook{fakePG: pg}
	coord.wal = hook
	for _, p := range paths {
		pg.exec("file+", StorageAttachments+"/"+p, "")
	}
	t.Cleanup(test.MockVariableValue(&cutWithoutReplicator, func() cutLock { return pg.cut }))
	// The gate the command's wrapper registers.
	t.Cleanup(GateDeletionsWithoutReplicator(ctx))

	roots := map[string]*setting.Storage{StorageAttachments: attCfg}
	cache := newDigestCache()
	_, err = listAndDigest(roots, cache)
	require.NoError(t, err, "an earlier checkpoint read the digests")
	walked := make(chan struct{})
	coord.files = func() (*storedListing, error) {
		// A walk that takes its time on a large storage.
		time.Sleep(time.Second)
		defer close(walked)
		return listStoredFiles(roots, cache)
	}
	purged := make(chan error, 1)
	goneBeforeWalk := make(chan bool, 1)
	hook.after = func() {
		go func() {
			// The purge's SQL commits after L (the fake PG mirrors it),
			// then its stored files go.
			for _, p := range paths {
				pg.exec("file-", StorageAttachments+"/"+p, "")
			}
			start := time.Now()
			err := repo_service.DeleteRepositoryDirectly(ctx, 1, repo_service.DeleteRepositoryOpts{})
			_, statErr := os.Stat(filepath.Join(attCfg.Path, paths[0]))
			select {
			case <-walked:
				goneBeforeWalk <- false
			default:
				goneBeforeWalk <- errors.Is(statErr, os.ErrNotExist)
			}
			t.Logf("DeleteRepositoryDirectly(user2/repo1) returned after %v: %v", time.Since(start), err)
			purged <- err
		}()
	}

	rec, err := coord.take(ctx)
	require.NoError(t, err)
	require.NoError(t, <-purged)
	unittest.AssertNotExistsBean(t, &repo_model.Repository{ID: 1})
	assert.False(t, <-goneBeforeWalk, "the purge deleted the attachment while the checkpoint held the cut, before its walk listed the files")
	named := pg.filesAt(parseFakeLSN(t, rec.LSN))
	raw, err := os.ReadFile(filepath.Join(coord.dir, rec.Name(), "files.json"))
	require.NoError(t, err)
	t.Logf("checkpoint %s at L=%s: SQL at L names %d attachments of user2/repo1; files.json=%s", rec.Name(), rec.LSN, len(named), raw)
	for _, p := range paths {
		require.True(t, named[StorageAttachments+"/"+p], "the SQL at L names %s", p)
		assert.Contains(t, string(raw), p,
			"files.json misses an attachment the SQL at L names: recover --from-observer refuses this checkpoint at step 6")
		_, err := os.Stat(filepath.Join(attCfg.Path, p))
		assert.ErrorIs(t, err, os.ErrNotExist, "the attachment goes once the walk ended")
	}
}
