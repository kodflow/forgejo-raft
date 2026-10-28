// Copyright 2026 The Forgejo Authors. All rights reserved.
// SPDX-License-Identifier: GPL-3.0-or-later

package cluster

import (
	"os"
	"strings"
	"testing"
	"time"

	"forgejo.org/models/db"
	repo_model "forgejo.org/models/repo"
	"forgejo.org/models/unittest"
	user_model "forgejo.org/models/user"
	"forgejo.org/modules/clusterhook"
	"forgejo.org/modules/git"
	"forgejo.org/modules/gitrepo"
	repo_service "forgejo.org/services/repository"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// noteIn returns the note of sha in node n's copy of laggingRepo, and
// whether git found one.
func noteIn(n *testNode, sha string) (string, bool) {
	out, err := gitIn(n.r.root+"/"+laggingRepo, "notes", "show", sha)
	if err != nil {
		return "", false
	}
	return out, true
}

// laggingNotes builds a three-node cluster holding laggingRepo with main at
// c2 and a note on c1 (n0) on every node, makes node-a stop projecting it,
// then pushes a note on c2 through node-b, acknowledged (n1, parent n0):
// node-a's table holds n1, its git still n0, until release(hold).
func laggingNotes(t *testing.T) (tc *testCluster, a *testNode, c1, c2, n0, n1, hold string) {
	t.Helper()
	require.NoError(t, unittest.PrepareTestDatabase())
	tc = newTestCluster(t, "node-a", "node-b", "node-c")
	a, b := tc.nodes[0], tc.nodes[1]
	work := t.TempDir()
	run(t, work, "init", "-q")
	c1 = commit(t, work, "one")
	c2 = commit(t, work, "two")
	zero := strings.Repeat("0", 40)
	require.Equal(t, int64(1), tc.repoID(laggingRepo))
	require.True(t, tc.pushVia(a, work, laggingRepo, "refs/heads/main", zero, c2).OK)

	run(t, work, "notes", "add", "-m", "note zero on c1", c1)
	n0 = run(t, work, "rev-parse", git.NotesRef)
	require.True(t, tc.pushVia(a, work, laggingRepo, git.NotesRef, zero, n0).OK)
	for _, m := range tc.nodes {
		require.Eventually(t, func() bool {
			return gitRef(t, m, laggingRepo, "refs/heads/main") == c2 && gitRef(t, m, laggingRepo, git.NotesRef) == n0
		}, 15*time.Second, 20*time.Millisecond, "%s: precondition", m.slot)
	}

	hold = clusterhook.NewNonce()
	require.NoError(t, a.r.registry.register(hold, laggingRepo, nil, os.Getpid(), false))
	// A release is idempotent: a test that releases it first is unharmed.
	t.Cleanup(func() { a.r.registry.release(hold) })

	run(t, work, "notes", "add", "-m", "acknowledged note on c2", c2)
	n1 = run(t, work, "rev-parse", git.NotesRef)
	pushOnB(t, tc, a, b, work, git.NotesRef, n0, n1)
	tableAt(t, tc, git.NotesRef, n1, "the acknowledged note")
	return tc, a, c1, c2, n0, n1, hold
}

// notesConverge waits until every node's git shows refs/notes/commits at
// the value its table holds, and returns that value, node-a's.
func notesConverge(t *testing.T, tc *testCluster) string {
	t.Helper()
	for _, m := range tc.nodes {
		require.Eventually(t, func() bool {
			v, ok := m.r.table.Get(laggingRepo, git.NotesRef)
			return ok && gitRef(t, m, laggingRepo, git.NotesRef) == v.SHA
		}, 15*time.Second, 20*time.Millisecond, "%s converges", m.slot)
	}
	v, _ := tc.nodes[0].r.table.Get(laggingRepo, git.NotesRef)
	return v.SHA
}

// notesOnEveryNode checks the note of sha on every node: want, or none
// when want is empty.
func notesOnEveryNode(t *testing.T, tc *testCluster, sha, want, msg string) {
	t.Helper()
	for _, m := range tc.nodes {
		got, ok := noteIn(m, sha)
		if want == "" {
			assert.False(t, ok, "%s: %s: the note of %.10s is %q", m.slot, msg, sha, got)
			continue
		}
		assert.True(t, ok, "%s: %s: no note on %.10s", m.slot, msg, sha)
		assert.Equal(t, want, got, "%s: %s", m.slot, msg)
	}
}

// A note on c1 is on every node (n0). node-a stops projecting the
// repository; a note on c2 is pushed through node-b and acknowledged (n1,
// parent n0): node-a's table holds n1, its git still n0. A note is then set
// or removed through node-a (POST or DELETE /repos/{owner}/{repo}/git/
// notes/{sha}, the commit page's notes form): git notes built the new notes
// commit on node-a's n0 and updated the ref without an old value
// (notes-utils.c commit_notes), which the hook proposed at the table's
// revision: Apply overwrote n1 for the whole cluster, and the acknowledged
// note on c2 was gone (#324). The note now carries the value it was built
// on, which the cluster checks: written on a node behind, as a write that
// raced the freshness wait would be, it is refused and n1 is kept.
func TestALaggingNodesNoteKeepsAnAcknowledgedNote(t *testing.T) {
	tc, a, c1, c2, n0, n1, hold := laggingNotes(t)
	withGitThroughReplicator(t, a)
	ctx := db.DefaultContext
	sqlRepo := unittest.AssertExistsAndLoadBean(t, &repo_model.Repository{ID: 1})
	gitRepo, err := gitrepo.OpenRepository(ctx, sqlRepo)
	require.NoError(t, err)
	defer gitRepo.Close()

	err = git.SetNote(ctx, gitRepo, c1, "note on c1 edited through node-a", "user2", "user2@example.com")
	require.ErrorContains(t, err, "the table holds another value", "a note built on node-a's notes, older than the cluster's, is refused")
	tableAt(t, tc, git.NotesRef, n1, "a note set on node-a keeps the acknowledged note")
	err = git.RemoveNote(ctx, gitRepo, c1)
	require.ErrorContains(t, err, "the table holds another value", "a removal built on node-a's notes, older than the cluster's, is refused")
	tableAt(t, tc, git.NotesRef, n1, "a note removed on node-a keeps the acknowledged note")
	assert.Equal(t, n0, gitRef(t, a, laggingRepo, git.NotesRef), "node-a's git is left as it was")

	a.r.registry.release(hold)
	assert.Equal(t, n1, notesConverge(t, tc))
	notesOnEveryNode(t, tc, c2, "acknowledged note on c2", "the note acknowledged through node-b is kept")
	notesOnEveryNode(t, tc, c1, "note zero on c1", "the refused note changed nothing")
}

// Through Forgejo's notes writes (the API, the commit page's form), a note
// set or removed on node-a while it is behind waits for it to serve the
// repository, and is refused before anything is written. Once node-a caught
// up, the same writes are built on the cluster's notes, and keep every other
// note.
func TestAFreshNodeSetsNotesOnTheAcknowledgedOnes(t *testing.T) {
	tc, a, c1, c2, n0, n1, hold := laggingNotes(t)
	withGitThroughReplicator(t, a)
	ctx := db.DefaultContext
	sqlRepo := unittest.AssertExistsAndLoadBean(t, &repo_model.Repository{ID: 1})
	doer := unittest.AssertExistsAndLoadBean(t, &user_model.User{ID: 2})
	gitRepo, err := gitrepo.OpenRepository(ctx, sqlRepo)
	require.NoError(t, err)
	defer gitRepo.Close()
	parentOf := func(sha string) string { return run(t, a.r.root+"/"+laggingRepo, "rev-parse", sha+"^") }

	t.Run("node-a waits to serve the repository", func(t *testing.T) {
		freshFrom(t, a)
		require.ErrorIs(t, repo_service.SetNote(ctx, doer, sqlRepo, gitRepo, c1, "note on c1 edited through node-a"), clusterhook.ErrBehind)
		require.ErrorIs(t, repo_service.RemoveNote(ctx, sqlRepo, gitRepo, c1), clusterhook.ErrBehind)
		tableAt(t, tc, git.NotesRef, n1, "the acknowledged note is kept")
		assert.Equal(t, n0, gitRef(t, a, laggingRepo, git.NotesRef), "nothing was written on node-a")
	})

	caughtUp(t, a, hold, git.NotesRef, n1)
	freshFrom(t, a)
	require.NoError(t, repo_service.SetNote(ctx, doer, sqlRepo, gitRepo, c1, "note on c1 edited through node-a"))
	n2 := notesConverge(t, tc)
	require.NotEqual(t, n1, n2, "the cluster took the note")
	assert.Equal(t, n1, parentOf(n2), "the new notes commit is built on the acknowledged notes")
	notesOnEveryNode(t, tc, c1, "note on c1 edited through node-a", "the note set through node-a")
	notesOnEveryNode(t, tc, c2, "acknowledged note on c2", "the note acknowledged through node-b is kept")

	require.NoError(t, repo_service.RemoveNote(ctx, sqlRepo, gitRepo, c2))
	n3 := notesConverge(t, tc)
	require.NotEqual(t, n2, n3, "the cluster took the removal")
	assert.Equal(t, n2, parentOf(n3), "the removal is built on the cluster's notes")
	notesOnEveryNode(t, tc, c2, "", "the note removed through node-a")
	notesOnEveryNode(t, tc, c1, "note on c1 edited through node-a", "the other note is kept")
}
