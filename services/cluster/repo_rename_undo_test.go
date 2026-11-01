// Copyright 2026 The Forgejo Authors. All rights reserved.
// SPDX-License-Identifier: GPL-3.0-or-later

package cluster

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"forgejo.org/models/db"
	repo_model "forgejo.org/models/repo"
	"forgejo.org/models/unittest"
	user_model "forgejo.org/models/user"
	"forgejo.org/modules/clusterhook"
	"forgejo.org/modules/setting"
	"forgejo.org/modules/test"
	"forgejo.org/services/cluster/reftable"
	repo_service "forgejo.org/services/repository"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Forgejo renames alice/r to alice/s on this node: the keys are held, the
// directory moves, the rename applies (index 2), then the database fails to
// record it and the undo applies (index 3), still under the hold. Forgejo
// moves the directory back before it releases the hold, but a crash between
// the undo and that move, or a move that fails, leaves the directory under
// alice/s, its stamp naming alice/r before the rename. The projector took
// it for no rename's repository: it fetched the whole repository again into
// a new alice/r, and kept the directory, certified, under the freed name,
// where a repository created later through another member adopted it and
// this node served alice/r's objects under the new one's rights. The
// directory holds the repository the undo brings back: the projector moves
// it back under alice/r, and nothing stays under alice/s (#383).
func TestAnUndoneRenameWhoseDirectoryStayedUnderTheNewNameGoesBackToItsKey(t *testing.T) {
	t.Parallel()
	b := newFetchBench(t, "alice/r.git")
	p := b.p
	ctx := t.Context()
	p.unfence()
	require.Eventually(t, func() bool { p.pass(ctx); return p.Projected("alice/r.git") >= 1 }, 10*time.Second, 10*time.Millisecond)
	oldDir := filepath.Join(p.root, "alice", "r.git")
	newDir := filepath.Join(p.root, "alice", "s.git")
	// A file of the directory Forgejo moved: it comes back with it.
	require.NoError(t, os.WriteFile(filepath.Join(oldDir, "undo-marker"), []byte("x"), 0o600))
	b.mu.Lock()
	b.most = map[string]int{}
	b.mu.Unlock()

	release, err := p.holdDirs(ctx, renameKeys)
	require.NoError(t, err)
	require.NoError(t, os.Rename(oldDir, newDir))
	applyNow(t, p.table, 2, reftable.KindRepoRename, &reftable.RepoRename{From: "alice/r.git", To: "alice/s.git", Time: 2, Nonce: "undo-rename"})
	applyNow(t, p.table, 3, reftable.KindRepoRename, &reftable.RepoRename{From: "alice/s.git", To: "alice/r.git", Time: 3, Nonce: "undo-undo"})
	// Forgejo stops here: its hold goes with it, the directory stays.
	release()

	require.Eventually(t, func() bool {
		p.pass(ctx)
		return p.Projected("alice/r.git") >= 3 && p.Projected("alice/s.git") >= 3
	}, 20*time.Second, 10*time.Millisecond, "both keys are projected")
	for range 3 {
		p.pass(ctx)
	}
	b.mu.Lock()
	fetched := b.most["alice/r.git"]
	b.mu.Unlock()
	assert.Zero(t, fetched, "alice/r.git takes its directory back, nothing is fetched")
	_, err = os.Stat(filepath.Join(oldDir, "undo-marker"))
	require.NoError(t, err, "the directory Forgejo moved is back under alice/r.git")
	_, err = os.Stat(newDir)
	assert.True(t, os.IsNotExist(err), "nothing stays under the freed name: %v", err)
	main, ok := p.table.Get("alice/r.git", "refs/heads/main")
	require.True(t, ok)
	_, err = gitRun(ctx, oldDir, nil, "cat-file", "-e", main.SHA)
	require.NoError(t, err)
	s, known := readStamp(oldDir)
	require.True(t, known)
	assert.Equal(t, "alice/r.git", s.Repo, "certified under its key")

	// A repository created under the freed name through another member:
	// git init's HEAD (index 4), a new ID.
	head, _ := p.table.Get("alice/s.git", "HEAD")
	applyNow(t, p.table, 4, reftable.KindRefTxn, &reftable.RefTxn{
		Repo: "alice/s.git", RepoID: 99, Format: "sha1", Init: true,
		Updates: []reftable.RefUpdate{{Ref: "HEAD", Symbolic: "refs/heads/main", ExpectRev: head.Rev}},
	})
	require.Eventually(t, func() bool { p.pass(ctx); return p.Projected("alice/s.git") >= 4 }, 10*time.Second, 10*time.Millisecond)
	_, err = gitRun(ctx, newDir, nil, "cat-file", "-e", main.SHA)
	require.Error(t, err, "the new alice/s holds none of alice/r's objects")
	_, err = os.Stat(filepath.Join(newDir, "undo-marker"))
	assert.True(t, os.IsNotExist(err), "nor the directory of alice/r: %v", err)
}

// Forgejo's rename on a three-node cluster (services/repository,
// ChangeRepositoryName) that the database fails to record once the cluster
// did: a repository created under the new name meanwhile, through another
// node, fails the write of the name. The undo moves the repository back
// in the table, and Forgejo moves its directory back on the node that
// served the rename, under the hold: that node takes it back as it is,
// fetches nothing, keeps no copy under the freed name, and a repository
// created there later through another member holds none of the renamed
// repository's objects on that node (#383).
func TestARenameTheDatabaseFailsToRecordGoesBackUnderItsKeyOnTheNodeThatServedIt(t *testing.T) {
	require.NoError(t, unittest.PrepareTestDatabase())
	// The repository created under the new name is not found by the next
	// tests.
	t.Cleanup(func() { assert.NoError(t, unittest.PrepareTestDatabase()) })
	tc := newTestCluster(t, "node-a", "node-b", "node-c")
	origin, peer := tc.nodes[0], tc.nodes[1]
	work := t.TempDir()
	run(t, work, "init", "-q")
	c1 := commit(t, work, "one")
	zero := strings.Repeat("0", 40)
	const key, renamed, newName = "user2/repo1.git", "user2/undone-rename.git", "undone-rename"
	require.True(t, tc.pushVia(origin, work, key, "refs/heads/main", zero, c1).OK)
	waitAll(t, tc, func(n *testNode) bool { return gitRef(t, n, key, "refs/heads/main") == c1 })
	oldDir := filepath.Join(origin.r.root, filepath.FromSlash(key))
	newDir := filepath.Join(origin.r.root, filepath.FromSlash(renamed))
	require.NoError(t, os.WriteFile(filepath.Join(oldDir, "undo-marker"), []byte("x"), 0o600))

	t.Cleanup(test.MockVariableValue(&setting.RepoRootPath, origin.r.root))
	clusterhook.SetRepoRenameCheck(origin.r.renameSourceRefusal)
	clusterhook.SetOwnerDirectoryHold(origin.r.holdOwnerDirectories)
	t.Cleanup(func() {
		clusterhook.SetRepoRenameCheck(nil)
		clusterhook.SetOwnerDirectoryHold(nil)
		clusterhook.SetRepoLifecycle(nil, nil)
	})
	var proposed [][2]string
	clusterhook.SetRepoLifecycle(nil, func(ctx context.Context, moves [][2]string) error {
		proposed = append(proposed, moves[0])
		if err := origin.r.proposeRepoMoves(ctx, moves); err != nil {
			return err
		}
		if moves[0][0] == key {
			require.NoError(t, db.Insert(db.DefaultContext, &repo_model.Repository{OwnerID: 2, OwnerName: "user2", Name: newName, LowerName: newName}))
		}
		return nil
	})

	doer := unittest.AssertExistsAndLoadBean(t, &user_model.User{ID: 2})
	repo := unittest.AssertExistsAndLoadBean(t, &repo_model.Repository{ID: 1})
	require.ErrorContains(t, repo_service.ChangeRepositoryName(db.DefaultContext, doer, repo, newName), "UNIQUE")
	assert.Equal(t, [][2]string{{key, renamed}, {renamed, key}}, proposed, "the rename, then its undo")
	_, err := os.Stat(filepath.Join(oldDir, "undo-marker"))
	require.NoError(t, err, "Forgejo moved the directory back under the old name")
	_, err = os.Stat(newDir)
	require.True(t, os.IsNotExist(err), "nothing stays under the new name: %v", err)

	_, undone := origin.r.table.RepoHash(key)
	waitAll(t, tc, func(n *testNode) bool {
		return n.r.projector.Projected(key) >= undone && gitRef(t, n, key, "refs/heads/main") == c1
	})
	require.Eventually(t, func() bool { return origin.r.projector.Projected(renamed) >= undone }, 15*time.Second, 20*time.Millisecond)
	_, err = os.Stat(filepath.Join(oldDir, "undo-marker"))
	require.NoError(t, err, "the node that served the rename kept its directory: nothing was fetched into a new one")
	_, err = os.Stat(newDir)
	assert.True(t, os.IsNotExist(err), "no copy stays under the freed name: %v", err)

	// A repository created under the freed name through another member.
	tc.createVia(peer, renamed)
	_, created := peer.r.table.RepoHash(renamed)
	require.Eventually(t, func() bool { return origin.r.projector.Projected(renamed) >= created }, 15*time.Second, 20*time.Millisecond)
	assert.False(t, hasObject(origin, renamed, c1), "the new repository holds none of the renamed repository's objects")
}
