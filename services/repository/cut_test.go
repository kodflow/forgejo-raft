// Copyright 2026 The Forgejo Authors. All rights reserved.
// SPDX-License-Identifier: GPL-3.0-or-later

package repository

import (
	"context"
	"fmt"
	"sync"
	"testing"

	"forgejo.org/models/db"
	repo_model "forgejo.org/models/repo"
	"forgejo.org/models/unittest"
	user_model "forgejo.org/models/user"
	"forgejo.org/modules/clusterhook"
	"forgejo.org/modules/storage"
	"forgejo.org/services/feed"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// cutLog records the holds of the checkpoint's cut, and what the
// lifecycle hooks and the stored files' deletions saw of them.
type cutLog struct {
	mu     sync.Mutex
	held   int
	events []string
}

func (l *cutLog) add(format string, args ...any) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.events = append(l.events, fmt.Sprintf(format, args...))
}

func (l *cutLog) hold(ctx context.Context) (func(), error) {
	l.mu.Lock()
	l.held++
	l.mu.Unlock()
	return func() {
		l.mu.Lock()
		l.held--
		l.mu.Unlock()
	}, nil
}

func (l *cutLog) heldNow() int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.held
}

func watchCut(t *testing.T) *cutLog {
	l := &cutLog{}
	clusterhook.SetCutHold(l.hold)
	clusterhook.SetRepoLifecycle(func(ctx context.Context, key string, _ int64) error {
		// The deletion's SQL committed before its entry, under the cut.
		exists, err := db.GetEngine(db.DefaultContext).Where("lower_name = ?", "repo1").Exist(&repo_model.Repository{})
		require.NoError(t, err)
		l.add("deleted %s held=%d row=%v", key, l.heldNow(), exists)
		return nil
	}, func(ctx context.Context, moves [][2]string) error {
		l.add("moved %s->%s held=%d", moves[0][0], moves[0][1], l.heldNow())
		return nil
	})
	storage.SetDeleteGate(func(del func() error) error {
		l.add("stored file held=%d", l.heldNow())
		return del()
	})
	t.Cleanup(func() {
		clusterhook.SetCutHold(nil)
		clusterhook.SetRepoLifecycle(nil, nil)
		storage.SetDeleteGate(nil)
	})
	return l
}

// The deletion, the rename and the transfer of a repository hold the
// checkpoint's cut from before their SQL until their entry and their SQL
// are both done, and the deletion's stored files go under it (#88, #90).
func TestLifecycleHoldsTheCheckpointCut(t *testing.T) {
	unittest.PrepareTestEnv(t)
	// The repositories deleted, renamed and transferred here are the
	// fixtures' own: the next tests find them back.
	t.Cleanup(func() { unittest.PrepareTestEnv(t) })
	doer := unittest.AssertExistsAndLoadBean(t, &user_model.User{ID: 2})

	t.Run("deletion", func(t *testing.T) {
		l := watchCut(t)
		require.NoError(t, DeleteRepositoryDirectly(db.DefaultContext, 1, DeleteRepositoryOpts{}))
		assert.Contains(t, l.events, "deleted user2/repo1.git held=1 row=false")
		assert.Contains(t, l.events, "stored file held=1", "the repository's stored files go under the cut")
		assert.Zero(t, l.heldNow(), "released once done")
	})

	t.Run("rename", func(t *testing.T) {
		l := watchCut(t)
		repo := unittest.AssertExistsAndLoadBean(t, &repo_model.Repository{ID: 2})
		require.NoError(t, ChangeRepositoryName(db.DefaultContext, doer, repo, "renamed-under-cut"))
		assert.Contains(t, l.events, "moved user2/repo2.git->user2/renamed-under-cut.git held=1")
		assert.Zero(t, l.heldNow())
	})

	t.Run("transfer", func(t *testing.T) {
		registerNotifier()
		require.NoError(t, feed.Init())
		l := watchCut(t)
		repo := unittest.AssertExistsAndLoadBean(t, &repo_model.Repository{ID: 3})
		repo.Owner = unittest.AssertExistsAndLoadBean(t, &user_model.User{ID: repo.OwnerID})
		require.NoError(t, TransferOwnership(db.DefaultContext, doer, doer, repo, nil))
		assert.Contains(t, l.events, "moved org3/repo3.git->user2/repo3.git held=1")
		assert.Zero(t, l.heldNow())
	})
}
