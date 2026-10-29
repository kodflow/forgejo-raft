// Copyright 2026 The Forgejo Authors. All rights reserved.
// SPDX-License-Identifier: GPL-3.0-or-later

package repository_test

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"forgejo.org/models/db"
	git_model "forgejo.org/models/git"
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

// lfsGCSetup prepares user2/lfs with one LFS object no commit names, and
// returns the repository, the options that examine every row, and the
// lines the collection logged.
func lfsGCSetup(t *testing.T) (*repo_model.Repository, string, repo_service.GarbageCollectLFSMetaObjectsOptions, func() string) {
	t.Helper()
	unittest.PrepareTestEnv(t)
	t.Cleanup(test.MockVariableValue(&setting.LFS.StartServer, true))
	require.NoError(t, storage.Init())
	repo, err := repo_model.GetRepositoryByOwnerAndName(db.DefaultContext, "user2", "lfs")
	require.NoError(t, err)
	content := []byte("an LFS object no commit names")
	oid := storeObjectInRepo(t, repo.ID, &content)
	var mu sync.Mutex
	var logged []string
	opts := repo_service.GarbageCollectLFSMetaObjectsOptions{
		AutoFix:   true,
		OlderThan: time.Now().Add(7 * 24 * time.Hour),
		LogDetail: func(format string, v ...any) {
			mu.Lock()
			defer mu.Unlock()
			logged = append(logged, fmt.Sprintf(format, v...))
			t.Logf(format, v...)
		},
	}
	return repo, oid, opts, func() string {
		mu.Lock()
		defer mu.Unlock()
		return strings.Join(logged, "\n")
	}
}

func lfsRows(t *testing.T, repoID int64) int64 {
	t.Helper()
	n, err := db.GetEngine(db.DefaultContext).Count(git_model.LFSMetaObject{RepositoryID: repoID})
	require.NoError(t, err)
	return n
}

func lfsRowExists(t *testing.T, repoID int64, oid string) bool {
	t.Helper()
	_, err := git_model.GetLFSMetaObjectByOid(db.DefaultContext, repoID, oid)
	if err == nil {
		return true
	}
	require.ErrorIs(t, err, git_model.ErrLFSObjectNotExist)
	return false
}

// `forgejo doctor check --run gc-lfs --fix`, on a node of a cluster, runs
// in a process without the replicator, where the cluster's gates answer
// yes: it judged the objects from that node's git alone and deleted them
// for every member (#349). It refuses, naming the cron task; without
// --fix, it reports and deletes nothing.
func TestGarbageCollectLFSMetaObjectsWithoutReplicatorInACluster(t *testing.T) {
	repo, oid, opts, logged := lfsGCSetup(t)
	t.Cleanup(test.MockVariableValue(&setting.Cluster.Enabled, true))
	require.False(t, clusterhook.Enabled(), "no replicator runs in this process")
	rows := lfsRows(t, repo.ID)

	err := repo_service.GarbageCollectLFSMetaObjects(t.Context(), opts)
	require.ErrorIs(t, err, repo_service.ErrLFSCollectionWithoutReplicator)
	assert.Contains(t, err.Error(), "gc_lfs", "the refusal names the cron task")
	assert.True(t, lfsRowExists(t, repo.ID, oid), "nothing is deleted")
	require.ErrorIs(t, repo_service.GarbageCollectLFSMetaObjectsForRepo(t.Context(), repo, opts), repo_service.ErrLFSCollectionWithoutReplicator)
	assert.Equal(t, rows, lfsRows(t, repo.ID), "nothing is deleted")

	opts.AutoFix = false
	require.NoError(t, repo_service.GarbageCollectLFSMetaObjects(t.Context(), opts))
	assert.Contains(t, logged(), "orphaned LFSMetaObjects in user2/lfs", "the report counts the orphans")
	assert.Contains(t, logged(), "runs no replicator", "the report says whose view it is")
	assert.Equal(t, rows, lfsRows(t, repo.ID), "a report deletes nothing")
}

// In a node's server, the gc_lfs cron task asks the cluster's gates of
// each repository first: it is skipped while the projector has work
// pending on it (a directory projected again, its objects being fetched),
// or while the node may be behind the cluster's last writes to it (#349).
// Once both answer yes, an orphan is collected as without a cluster.
func TestGarbageCollectLFSMetaObjectsAsksTheClusterGates(t *testing.T) {
	repo, oid, opts, logged := lfsGCSetup(t)
	t.Cleanup(test.MockVariableValue(&setting.Cluster.Enabled, true))
	was := clusterhook.Enabled()
	clusterhook.SetEnabled(true)
	var mu sync.Mutex
	mayGC, fresh := false, true
	var asked []string
	clusterhook.SetGarbageCollectionGate(func(key string) bool {
		mu.Lock()
		defer mu.Unlock()
		asked = append(asked, "gc "+key)
		return mayGC
	})
	clusterhook.SetFreshWaiter(func(_ context.Context, key string) bool {
		mu.Lock()
		defer mu.Unlock()
		asked = append(asked, "fresh "+key)
		return fresh
	})
	t.Cleanup(func() {
		clusterhook.SetEnabled(was)
		clusterhook.SetGarbageCollectionGate(nil)
		clusterhook.SetFreshWaiter(nil)
	})
	set := func(gc, f bool) {
		mu.Lock()
		defer mu.Unlock()
		mayGC, fresh, asked = gc, f, nil
	}
	rows := lfsRows(t, repo.ID)

	set(false, true)
	require.NoError(t, repo_service.GarbageCollectLFSMetaObjects(t.Context(), opts))
	assert.True(t, lfsRowExists(t, repo.ID, oid), "kept while the projector has work pending on the repository")
	assert.Equal(t, rows, lfsRows(t, repo.ID))
	assert.Contains(t, asked, "gc user2/lfs.git")
	assert.Contains(t, logged(), "Skipping user2/lfs")

	set(true, false)
	require.NoError(t, repo_service.GarbageCollectLFSMetaObjects(t.Context(), opts))
	assert.True(t, lfsRowExists(t, repo.ID, oid), "kept while the node may be behind the cluster's last writes")
	assert.Equal(t, rows, lfsRows(t, repo.ID))
	assert.Contains(t, asked, "fresh user2/lfs.git")

	set(true, true)
	require.NoError(t, repo_service.GarbageCollectLFSMetaObjects(t.Context(), opts))
	assert.False(t, lfsRowExists(t, repo.ID, oid), "an orphan is collected once the node serves the repository")
	// With the fixtures' own orphan, as without a cluster
	// (TestGarbageCollectLFSMetaObjects).
	assert.Equal(t, rows-2, lfsRows(t, repo.ID), "only the orphans")
}

// git that gives no answer on a pointer (a cat-file that cannot start or
// dies, a cancelled context) said nothing about it: the collection counted
// it missing and deleted the object (#349). It keeps it, and the rest of
// the repository's objects, until a later run. The repository's directory
// here is not a git repository: git fails on it.
func TestGarbageCollectLFSMetaObjectsKeepsWhatGitCannotLookUp(t *testing.T) {
	repo, oid, opts, logged := lfsGCSetup(t)
	root := t.TempDir()
	t.Cleanup(test.MockVariableValue(&setting.RepoRootPath, root))
	// git looks for a repository no higher than root.
	t.Setenv("GIT_CEILING_DIRECTORIES", root)
	require.NoError(t, os.MkdirAll(filepath.Join(root, "user2", "lfs.git"), 0o755))
	rows := lfsRows(t, repo.ID)
	require.Greater(t, rows, int64(1))

	require.NoError(t, repo_service.GarbageCollectLFSMetaObjects(t.Context(), opts))
	assert.True(t, lfsRowExists(t, repo.ID, oid), "an object git could not look up is kept")
	assert.Equal(t, rows, lfsRows(t, repo.ID), "every object of the repository is kept")
	assert.Contains(t, logged(), "could not look up")
}
