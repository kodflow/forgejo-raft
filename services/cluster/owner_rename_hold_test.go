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

	"forgejo.org/models/db"
	"forgejo.org/models/unittest"
	user_model "forgejo.org/models/user"
	"forgejo.org/modules/clusterhook"
	"forgejo.org/modules/setting"
	"forgejo.org/modules/test"
	user_service "forgejo.org/services/user"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Forgejo moves an owner's directory on the node that renames the owner
// before the rename's entry is proposed, and moves it back when the
// cluster refuses the rename. Meanwhile the table still holds the owner's
// repositories under the old name: a projection of one of them that fell
// in between, its key left dirty by a push committed just before, created
// its directory again under the old owner and fetched its objects from the
// members, and so did a shipment of a push's objects from another member.
// That directory stayed once the rename was applied, and the next rename
// to that name, the owner's own rename back included, was refused by
// Forgejo ("rename user directory: file exists") until an operator removed
// it; a refused rename could not be moved back either (#257). Nothing of
// the cluster writes under the two owners' directories on that node while
// Forgejo moves them.
func TestNothingIsWrittenUnderAnOwnersDirectoryWhileItsRenameMovesIt(t *testing.T) {
	require.NoError(t, unittest.PrepareTestDatabase())
	tc := newTestCluster(t, "node-a", "node-b", "node-c")
	origin, peer := tc.nodes[0], tc.nodes[1]
	work := t.TempDir()
	run(t, work, "init", "-q")
	c1 := commit(t, work, "one")
	zero := strings.Repeat("0", 40)
	const key = "user4/c.git"
	require.True(t, tc.pushVia(origin, work, key, "refs/heads/main", zero, c1).OK)
	waitAll(t, tc, func(n *testNode) bool { return gitRef(t, n, key, "refs/heads/main") == c1 })

	t.Cleanup(test.MockVariableValue(&setting.RepoRootPath, origin.r.root))
	clusterhook.SetOwnerRenameCheck(origin.r.ownerRenameRefusal)
	clusterhook.SetOwnerDirectoryHold(origin.r.holdOwnerDirectories)
	t.Cleanup(func() {
		clusterhook.SetOwnerRenameCheck(nil)
		clusterhook.SetOwnerDirectoryHold(nil)
		clusterhook.SetOwnerLifecycle(nil)
	})
	oldDir := filepath.Join(origin.r.root, "user4")
	// Between Forgejo's move of the directory and the rename's entry: the
	// projector of this node projects the key once more, and a member
	// ships it the objects of a push.
	clusterhook.SetOwnerLifecycle(func(ctx context.Context, oldOwner, newOwner string) error {
		if oldOwner == "user4" {
			_, err := os.Stat(oldDir)
			require.True(t, errors.Is(err, os.ErrNotExist), "Forgejo moved the directory first")
			origin.r.projector.markDirty(key)
			origin.r.projector.pass(ctx)
			_, err = os.Stat(oldDir)
			assert.True(t, errors.Is(err, os.ErrNotExist), "the projection created nothing under the old owner: %v", err)
			var pack bytes.Buffer
			require.NoError(t, packObjects(ctx, filepath.Join(peer.r.root, filepath.FromSlash(key)), []string{c1}, nil, &pack))
			w := httptest.NewRecorder()
			origin.r.objects.handlePush(w, httptest.NewRequest(http.MethodPost, "/v1/push?repo="+key, &pack))
			assert.Equal(t, http.StatusServiceUnavailable, w.Code, "the objects of a key under a moving owner are not taken: %s", w.Body)
			_, err = os.Stat(oldDir)
			assert.True(t, errors.Is(err, os.ErrNotExist), "nothing was created under the old owner meanwhile: %v", err)
		}
		return origin.r.proposeOwnerRename(ctx, oldOwner, newOwner)
	})

	u := unittest.AssertExistsAndLoadBean(t, &user_model.User{ID: 4})
	require.NoError(t, user_service.RenameUser(db.DefaultContext, u, "heir"))
	waitAll(t, tc, func(n *testNode) bool { return gitRef(t, n, "heir/c.git", "refs/heads/main") == c1 })
	_, err := os.Stat(oldDir)
	assert.True(t, errors.Is(err, os.ErrNotExist), "no directory is left under the old owner: %v", err)

	// The owner's rename back is not refused by a directory left behind.
	require.NoError(t, user_service.RenameUser(db.DefaultContext, u, "user4"))
	waitAll(t, tc, func(n *testNode) bool { return gitRef(t, n, key, "refs/heads/main") == c1 })
	unittest.AssertExistsAndLoadBean(t, &user_model.User{ID: 4, LowerName: "user4"})
}
