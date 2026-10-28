// Copyright 2026 The Forgejo Authors. All rights reserved.
// SPDX-License-Identifier: GPL-3.0-or-later

package git_test

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"forgejo.org/models/unittest"
	"forgejo.org/modules/clusterhook"
	"forgejo.org/modules/git"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// clusterNotesRepo copies the fixture repository into the test's directory
// and opens it as a node of a cluster does: its reference-transaction hook,
// set in the copy's own config, records each call of the hook, its state,
// whether the git command said it only creates its refs, and its lines,
// then runs refuse, a shell command whose failure refuses the transaction
// as the cluster does. transactions returns what the hook recorded.
func clusterNotesRepo(t *testing.T, fixture, refuse string) (repo *git.Repository, dir string, transactions func() []string) {
	t.Helper()
	dir = filepath.Join(t.TempDir(), "repo")
	require.NoError(t, unittest.CopyDir(filepath.Join(testReposDir, fixture), dir))
	hooks := t.TempDir()
	record := filepath.Join(t.TempDir(), "transactions")
	script := fmt.Sprintf("#!/bin/sh\n{ echo \"$1 create-only=$%s\"; cat; } >> '%s'\n%s\n", clusterhook.EnvCreateOnly, record, refuse)
	require.NoError(t, os.WriteFile(filepath.Join(hooks, "reference-transaction"), []byte(script), 0o755))
	_, _, runErr := git.NewCommand(t.Context(), "config", "core.hooksPath").AddDynamicArguments(hooks).RunStdString(&git.RunOpts{Dir: dir})
	require.NoError(t, runErr)

	clusterhook.SetEnabled(true)
	t.Cleanup(func() { clusterhook.SetEnabled(false) })
	repo, err := openRepositoryWithDefaultContext(dir)
	require.NoError(t, err)
	t.Cleanup(func() { _ = repo.Close() })
	return repo, dir, func() []string {
		b, err := os.ReadFile(record)
		if os.IsNotExist(err) {
			return nil
		}
		require.NoError(t, err)
		return strings.Split(strings.TrimSpace(string(b)), "\n")
	}
}

// notesAt returns the value of refs/notes/commits in dir, "" when absent.
func notesAt(t *testing.T, dir string) string {
	t.Helper()
	out, _, err := git.NewCommand(t.Context(), "for-each-ref", "--format=%(objectname)", git.NotesRef).RunStdString(&git.RunOpts{Dir: dir})
	require.NoError(t, err)
	return strings.TrimSpace(out)
}

// noteOf returns the note of commitID in repo, failing the test when there
// is none.
func noteOf(t *testing.T, repo *git.Repository, commitID string) *git.Note {
	t.Helper()
	note, err := git.GetNote(t.Context(), repo, commitID)
	require.NoError(t, err)
	return note
}

// transaction is what the hook records of one transaction of refs/notes/
// commits from from to to.
func transaction(from, to, createOnly string) []string {
	line := from + " " + to + " " + git.NotesRef
	return []string{"prepared create-only=" + createOnly, line, "committed create-only=" + createOnly, line}
}

// In a cluster, `git notes` wrote refs/notes/commits without an old value,
// which a node behind proposed at the table's revision: the notes another
// node acknowledged were overwritten (#324). A note set or removed in a
// cluster is one update of refs/notes/commits that names the notes it was
// built on, which the hook passes to the cluster; the notes commit is built
// on them, and keeps the other notes.
func TestSetNoteInAClusterNamesTheNotesItIsBuiltOn(t *testing.T) {
	t.Setenv("TMPDIR", t.TempDir())
	repo, dir, transactions := clusterNotesRepo(t, "repo1_bare", "")
	noted, other := "95bb4d39648ee7e325106df01a621c530863a653", "37991dec2c8e592043f47155ce4808d4580f9123"
	n0 := notesAt(t, dir)
	require.NotEmpty(t, n0)

	require.NoError(t, git.SetNote(t.Context(), repo, other, "A note in a cluster", "Test", "test@test.com"))
	n1 := notesAt(t, dir)
	assert.Equal(t, transaction(n0, n1, ""), transactions(), "one transaction, from the notes it was built on")
	parent, _, runErr := git.NewCommand(t.Context(), "rev-parse").AddDynamicArguments(n1 + "^").RunStdString(&git.RunOpts{Dir: dir})
	require.NoError(t, runErr)
	assert.Equal(t, n0, strings.TrimSpace(parent))
	note := noteOf(t, repo, other)
	assert.Equal(t, "A note in a cluster\n", string(note.Message))
	assert.Equal(t, "Test", note.Commit.Author.Name)
	assert.Equal(t, "Note contents\n", string(noteOf(t, repo, noted).Message), "the other note is kept")

	require.NoError(t, git.RemoveNote(t.Context(), repo, noted))
	n2 := notesAt(t, dir)
	assert.Equal(t, append(transaction(n0, n1, ""), transaction(n1, n2, "")...), transactions())
	_, err := git.GetNote(t.Context(), repo, noted)
	assert.True(t, git.IsErrNotExist(err), "the note is removed: %v", err)
	assert.Equal(t, "A note in a cluster\n", string(noteOf(t, repo, other).Message), "the other note is kept")

	left, err := os.ReadDir(os.Getenv("TMPDIR"))
	require.NoError(t, err)
	assert.Empty(t, left, "the scratch git directories are removed")
}

// A repository without notes gets its refs/notes/commits created only if
// absent, which the hook is told: the cluster refuses the creation when it
// holds notes this node's git does not show yet (#189). The scratch git
// directory takes the repository's object format.
func TestSetNoteInAClusterCreatesTheNotesOnlyIfAbsent(t *testing.T) {
	repo, dir, transactions := clusterNotesRepo(t, "repo1_bare_sha256", "")
	commitID := "9433b2a62b964c17a4485ae180f45f595d3e69d31b786087775e28c6b6399df0"
	require.Empty(t, notesAt(t, dir))

	require.NoError(t, git.SetNote(t.Context(), repo, commitID, "The first note", "Test", "test@test.com"))
	n0 := notesAt(t, dir)
	require.Len(t, n0, 64)
	assert.Equal(t, transaction(strings.Repeat("0", 64), n0, "1"), transactions())
	assert.Equal(t, "The first note\n", string(noteOf(t, repo, commitID).Message))
}

// Refused by the cluster, a note leaves the notes as they were, and the
// caller gets the refusal.
func TestSetNoteInAClusterRefusedWritesNothing(t *testing.T) {
	repo, dir, _ := clusterNotesRepo(t, "repo1_bare", `[ "$1" = prepared ] && { echo "the table holds another value" >&2; exit 1; }; exit 0`)
	noted := "95bb4d39648ee7e325106df01a621c530863a653"
	n0 := notesAt(t, dir)

	err := git.SetNote(t.Context(), repo, noted, "A refused note", "Test", "test@test.com")
	require.ErrorContains(t, err, "the table holds another value")
	err = git.RemoveNote(t.Context(), repo, noted)
	require.ErrorContains(t, err, "the table holds another value")

	assert.Equal(t, n0, notesAt(t, dir))
	assert.Equal(t, "Note contents\n", string(noteOf(t, repo, noted).Message))
}
