// Copyright 2026 The Forgejo Authors. All rights reserved.
// SPDX-License-Identifier: GPL-3.0-or-later

package cluster

import (
	"bytes"
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
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
	"forgejo.org/services/cluster/consensus"
	"forgejo.org/services/cluster/reftable"
	repo_service "forgejo.org/services/repository"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// applyNow applies body at index i, written at the current payload
// version, as a member of today's cluster writes it.
func applyNow(t *testing.T, tbl *reftable.Table, i uint64, kind reftable.Kind, body any) {
	t.Helper()
	data, err := reftable.EncodeVersion(reftable.PayloadVersion, kind, body)
	require.NoError(t, err)
	res := tbl.Apply(consensus.Entry{
		Term: 1, Index: i, Kind: consensus.EntryCommand, Version: reftable.PayloadVersion, Data: data,
		Token: consensus.Token{Cluster: consensus.ClusterID{1}, Epoch: 1, Term: 1, Index: i},
	}).(reftable.Result)
	require.True(t, res.OK, res.Reason)
}

// renameKeys are the keys a rename of alice/r to alice/s holds, as
// clusterhook.HoldRepoDirectories names them.
var renameKeys = []string{"alice/r.git", "alice/r.wiki.git", "alice/s.git", "alice/s.wiki.git"}

// renameWindow brings alice/r.git (ID 7) to the start of a rename's window
// on the node that serves it: projected at index 1, then a push committed
// through another member moves main to two (index 2), not projected here
// yet, so the key is dirty when Forgejo moves its directory. Fetches into
// alice/r.git wait on the gate returned.
func renameWindow(t *testing.T) (b *fetchBench, two string, gate chan struct{}) {
	t.Helper()
	b = newFetchBench(t, "alice/r.git")
	b.p.unfence()
	require.Eventually(t, func() bool { b.p.pass(t.Context()); return b.p.Projected("alice/r.git") >= 1 }, 10*time.Second, 10*time.Millisecond)
	two = commit(t, b.work, "two")
	gate = b.hold("alice/r.git")
	applyNow(t, b.p.table, 2, reftable.KindRefTxn, &reftable.RefTxn{
		Repo: "alice/r.git", RepoID: 7, Format: "sha1",
		Updates: []reftable.RefUpdate{{Ref: "refs/heads/main", New: two, ExpectRev: 1}},
	})
	return b, two, gate
}

// A repository's rename (or transfer) moves its directory on the node that
// serves it before RepoRenamed proposes the entry, and the table names the
// repository under its old key until the entry is applied. A pass of the
// projector that fell in between created the old key's directory again
// and fetched the whole repository into it; once the rename was applied,
// that copy was certified under the freed name, and a repository created
// there later, through any member, adopted it: this node served the
// renamed repository's objects under the new one's rights. Forgejo now
// holds the keys of both names from before the move until the rename
// ends: nothing of the cluster writes there in between (#274).
func TestNothingIsWrittenUnderARepositorysKeysWhileItsRenameMovesThem(t *testing.T) {
	t.Parallel()
	b, two, gate := renameWindow(t)
	p := b.p
	ctx := t.Context()
	oldDir := filepath.Join(p.root, "alice", "r.git")
	newDir := filepath.Join(p.root, "alice", "s.git")

	// Forgejo on this node renames alice/r to alice/s: the keys are held,
	// then the directory moves, and the entry is proposed after.
	release, err := p.holdDirs(ctx, renameKeys)
	require.NoError(t, err)
	require.NoError(t, os.Rename(oldDir, newDir))

	// The projector's pass falls in between.
	p.pass(ctx)
	_, err = os.Stat(oldDir)
	assert.True(t, os.IsNotExist(err), "nothing is created under the old key while it is held: %v", err)
	assert.False(t, p.isFetching("alice/r.git"), "no fetch into the old key starts while it is held")

	// The rename's entry applies (index 3): RepoRenamed returns once it is
	// applied here, and Forgejo releases the hold after its SQL.
	applyNow(t, p.table, 3, reftable.KindRepoRename, &reftable.RepoRename{From: "alice/r.git", To: "alice/s.git", Time: 3, Nonce: "rename-hold"})
	p.pass(ctx)
	_, err = os.Stat(oldDir)
	assert.True(t, os.IsNotExist(err), "nothing is created under the old key while it is held: %v", err)
	close(gate)
	release()

	require.Eventually(t, func() bool {
		p.pass(ctx)
		return p.Projected("alice/s.git") >= 3 && p.Projected("alice/r.git") >= 3
	}, 10*time.Second, 10*time.Millisecond, "both keys are projected once released")
	_, err = os.Stat(oldDir)
	assert.True(t, os.IsNotExist(err), "no copy of the renamed repository stays under the freed name: %v", err)
	_, err = gitRun(ctx, newDir, nil, "cat-file", "-e", two)
	require.NoError(t, err, "the renamed repository holds its commits under its new name")

	// A new repository is created under the freed name through another
	// member: git init's HEAD (index 4), a new ID.
	applyNow(t, p.table, 4, reftable.KindRefTxn, &reftable.RefTxn{
		Repo: "alice/r.git", RepoID: 99, Format: "sha1", Init: true,
		Updates: []reftable.RefUpdate{{Ref: "HEAD", Symbolic: "refs/heads/main", ExpectRev: 2}},
	})
	require.Eventually(t, func() bool { p.pass(ctx); return p.Projected("alice/r.git") >= 4 }, 10*time.Second, 10*time.Millisecond)
	_, err = gitRun(ctx, oldDir, nil, "cat-file", "-e", two)
	assert.Error(t, err, "the new alice/r holds none of the renamed repository's objects")
}

// The rename the cluster refuses is moved back by Forgejo under the same
// hold: nothing was created under the old key meanwhile, and the move back
// finds its place free (#274).
func TestARefusedRepositoryRenameMovesItsDirectoryBack(t *testing.T) {
	t.Parallel()
	b, two, gate := renameWindow(t)
	p := b.p
	ctx := t.Context()
	oldDir := filepath.Join(p.root, "alice", "r.git")
	newDir := filepath.Join(p.root, "alice", "s.git")

	release, err := p.holdDirs(ctx, renameKeys)
	require.NoError(t, err)
	require.NoError(t, os.Rename(oldDir, newDir))
	p.pass(ctx)

	// The cluster refused the rename: Forgejo moves the directory back,
	// then releases the hold.
	require.NoError(t, os.Rename(newDir, oldDir), "the move back of a refused rename")
	close(gate)
	release()
	require.Eventually(t, func() bool { p.pass(ctx); return p.Projected("alice/r.git") >= 2 }, 10*time.Second, 10*time.Millisecond, "projected again once released")
	_, err = gitRun(ctx, oldDir, nil, "cat-file", "-e", two)
	require.NoError(t, err)
	_, err = os.Stat(newDir)
	assert.True(t, os.IsNotExist(err), "nothing stays under the refused name: %v", err)
}

// The same window through Forgejo's own rename on a three-node cluster
// (services/repository, ChangeRepositoryName): between the move of the
// directory and the proposal of the entry, the projector of the renaming
// node projects the old key once more, and a member ships it the objects
// of a push. Neither writes under the old key, no copy of the repository
// stays under the freed name once the rename is applied, and a repository
// created there later through another member holds none of the renamed
// repository's objects on that node (#274).
func TestNothingIsWrittenUnderARepositorysDirectoryWhileForgejoRenamesIt(t *testing.T) {
	require.NoError(t, unittest.PrepareTestDatabase())
	// The fixtures' repository renamed here is found back by the next tests.
	t.Cleanup(func() { assert.NoError(t, unittest.PrepareTestDatabase()) })
	tc := newTestCluster(t, "node-a", "node-b", "node-c")
	origin, peer := tc.nodes[0], tc.nodes[1]
	work := t.TempDir()
	run(t, work, "init", "-q")
	c1 := commit(t, work, "one")
	zero := strings.Repeat("0", 40)
	const key, renamed = "user2/repo1.git", "user2/renamed-under-hold.git"
	require.True(t, tc.pushVia(origin, work, key, "refs/heads/main", zero, c1).OK)
	waitAll(t, tc, func(n *testNode) bool { return gitRef(t, n, key, "refs/heads/main") == c1 })

	t.Cleanup(test.MockVariableValue(&setting.RepoRootPath, origin.r.root))
	clusterhook.SetRepoRenameCheck(origin.r.renameSourceRefusal)
	clusterhook.SetOwnerDirectoryHold(origin.r.holdOwnerDirectories)
	t.Cleanup(func() {
		clusterhook.SetRepoRenameCheck(nil)
		clusterhook.SetOwnerDirectoryHold(nil)
		clusterhook.SetRepoLifecycle(nil, nil)
	})
	oldDir := filepath.Join(origin.r.root, filepath.FromSlash(key))
	clusterhook.SetRepoLifecycle(nil, func(ctx context.Context, moves [][2]string) error {
		if moves[0][0] == key {
			_, err := os.Stat(oldDir)
			require.True(t, errors.Is(err, os.ErrNotExist), "Forgejo moved the directory first")
			origin.r.projector.markDirty(key)
			origin.r.projector.pass(ctx)
			_, err = os.Stat(oldDir)
			assert.True(t, errors.Is(err, os.ErrNotExist), "the projection created nothing under the old key: %v", err)
			var pack bytes.Buffer
			require.NoError(t, packObjects(ctx, filepath.Join(peer.r.root, filepath.FromSlash(key)), []string{c1}, nil, &pack))
			w := httptest.NewRecorder()
			origin.r.objects.handlePush(w, httptest.NewRequest(http.MethodPost, "/v1/push?repo="+key, &pack))
			assert.Equal(t, http.StatusServiceUnavailable, w.Code, "the objects of a key whose directory moves are not taken: %s", w.Body)
			_, err = os.Stat(oldDir)
			assert.True(t, errors.Is(err, os.ErrNotExist), "nothing was created under the old key meanwhile: %v", err)
		}
		return origin.r.proposeRepoMoves(ctx, moves)
	})

	doer := unittest.AssertExistsAndLoadBean(t, &user_model.User{ID: 2})
	repo := unittest.AssertExistsAndLoadBean(t, &repo_model.Repository{ID: 1})
	require.NoError(t, repo_service.ChangeRepositoryName(db.DefaultContext, doer, repo, "renamed-under-hold"))
	waitAll(t, tc, func(n *testNode) bool { return gitRef(t, n, renamed, "refs/heads/main") == c1 })
	require.Eventually(t, func() bool { return origin.r.projector.Projected(key) > 0 }, 15*time.Second, 20*time.Millisecond)
	_, err := os.Stat(oldDir)
	assert.True(t, errors.Is(err, os.ErrNotExist), "no copy of the renamed repository stays under the freed name: %v", err)

	// A repository created under the freed name through another member.
	tc.createVia(peer, key)
	_, created := peer.r.table.RepoHash(key)
	require.Eventually(t, func() bool { return origin.r.projector.Projected(key) >= created }, 15*time.Second, 20*time.Millisecond)
	assert.False(t, hasObject(origin, key, c1), "the new repository holds none of the renamed repository's objects")
}
