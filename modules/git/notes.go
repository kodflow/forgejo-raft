// Copyright 2019 The Gitea Authors. All rights reserved.
// Copyright 2025 The Forgejo Authors. All rights reserved.
// SPDX-License-Identifier: MIT

package git

import (
	"context"
	"fmt"
	"io"
	"os"
	"slices"
	"strings"

	"forgejo.org/modules/clusterhook"
	"forgejo.org/modules/log"
	"forgejo.org/modules/setting"
	"forgejo.org/modules/util"
)

// NotesRef is the git ref where Gitea will look for git-notes data.
// The value ("refs/notes/commits") is the default ref used by git-notes.
const NotesRef = "refs/notes/commits"

// Note stores information about a note created using git-notes.
type Note struct {
	Message []byte
	Commit  *Commit
}

// GetNote retrieves the git-notes data for a given commit.
func GetNote(ctx context.Context, repo *Repository, commitID string) (*Note, error) {
	log.Trace("Searching for git note corresponding to the commit %q in the repository %q", commitID, repo.Path)
	notes, err := repo.GetCommit(NotesRef)
	if err != nil {
		if !IsErrNotExist(err) {
			log.Error("Unable to get commit from ref %q. Error: %v", NotesRef, err)
		}
		return nil, err
	}

	var path strings.Builder

	tree := &notes.Tree
	log.Trace("Found tree with ID %q while searching for git note corresponding to the commit %q", tree.ID, commitID)

	var entry *TreeEntry
	originalCommitID := commitID
	for len(commitID) > 2 {
		entry, err = tree.GetTreeEntryByPath(commitID)
		if err == nil {
			path.WriteString(commitID)
			break
		}
		if IsErrNotExist(err) {
			tree, err = tree.SubTree(commitID[0:2])
			path.WriteString(commitID[0:2] + "/")
			commitID = commitID[2:]
		}
		if err != nil {
			// Err may have been updated by the SubTree we need to recheck if it's again an ErrNotExist
			if !IsErrNotExist(err) {
				log.Error("Unable to find git note corresponding to the commit %q. Error: %v", originalCommitID, err)
			}
			return nil, err
		}
	}

	blob := entry.Blob()
	dataRc, err := blob.DataAsync()
	if err != nil {
		log.Error("Unable to read blob with ID %q. Error: %v", blob.ID, err)
		return nil, err
	}
	closed := false
	defer func() {
		if !closed {
			_ = dataRc.Close()
		}
	}()
	d, err := io.ReadAll(dataRc)
	if err != nil {
		log.Error("Unable to read blob with ID %q. Error: %v", blob.ID, err)
		return nil, err
	}
	_ = dataRc.Close()
	closed = true

	lastCommit, err := repo.getCommitByPathWithID(notes.ID, path.String())
	if err != nil {
		log.Error("Unable to get the commit for the path %q. Error: %v", path.String(), err)
		return nil, err
	}

	return &Note{Message: d, Commit: lastCommit}, nil
}

// SetNote sets the note of commitID to notes, written by doerName
// <doerEmail>, as `git notes add -f` does. In a cluster the note is written
// as an update of NotesRef checked against the notes it was built on
// (editNotesChecked).
func SetNote(ctx context.Context, repo *Repository, commitID, notes, doerName, doerEmail string) error {
	_, err := repo.GetCommit(commitID)
	if err != nil {
		return err
	}

	env := append(os.Environ(),
		"GIT_AUTHOR_NAME="+doerName,
		"GIT_AUTHOR_EMAIL="+doerEmail,
		"GIT_COMMITTER_NAME="+doerName,
		"GIT_COMMITTER_EMAIL="+doerEmail,
	)

	if setting.Cluster.Enabled || clusterhook.Enabled() {
		return editNotesChecked(ctx, repo, env, "notes: Notes added by 'git notes add'", commitID, func(cmd *Command, oid string) {
			cmd.AddArguments("add", "-f", "-m").AddDynamicArguments(notes, oid)
		})
	}

	cmd := NewCommand(ctx, "notes", "add", "-f", "-m")
	cmd.AddDynamicArguments(notes, commitID)

	_, stderr, err := cmd.RunStdString(&RunOpts{Dir: repo.Path, Env: env})
	if err != nil {
		log.Error("Error while running git notes add: %s", stderr)
		return err
	}

	return nil
}

// RemoveNote removes the note of commitID, as `git notes remove` does. In
// a cluster the removal is written as an update of NotesRef checked against
// the notes it was built on (editNotesChecked).
func RemoveNote(ctx context.Context, repo *Repository, commitID string) error {
	if setting.Cluster.Enabled || clusterhook.Enabled() {
		return editNotesChecked(ctx, repo, nil, "notes: Notes removed by 'git notes remove'", commitID, func(cmd *Command, oid string) {
			cmd.AddArguments("remove").AddDynamicArguments(oid)
		})
	}

	cmd := NewCommand(ctx, "notes", "remove")
	cmd.AddDynamicArguments(commitID)

	_, stderr, err := cmd.RunStdString(&RunOpts{Dir: repo.Path})
	if err != nil {
		log.Error("Error while running git notes remove: %s", stderr)
		return err
	}

	return nil
}

// editNotesChecked runs `git notes`, its subcommand added by edit on the
// object objectName names, and writes the notes commit it makes to NotesRef
// as one update checked against the notes it was built on. env is the
// command's environment, nil for this process's.
//
// git notes builds its commit on this node's NotesRef, and writes it without
// an old value (notes-utils.c commit_notes: update_ref(..., NULL, ...)). The
// reference-transaction hook then reads a null old value, which a node of
// the cluster proposes at the table's revision: a node behind on the
// repository built on notes older than those another node acknowledged, and
// Apply took its commit in their place, for every node (#324). Here git
// notes runs on a scratch git directory, outside the repository root, that
// holds NotesRef at the value read from this node's git and shares this
// repository's objects, with no hook: nothing of it is proposed. Its commit
// is then written with update-ref, checked against that value, which the
// hook passes on: the cluster refuses the update when its table holds
// another value, and this node's git when its ref moved, instead of either
// overwriting the notes written meanwhile. An absent NotesRef is only
// created if absent, which the hook is told (clusterhook.EnvCreateOnly). The
// caller waits first for this node to serve the repository at the cluster's
// commit index, so that a node behind builds on the acknowledged notes
// rather than being refused.
func editNotesChecked(ctx context.Context, repo *Repository, env []string, reflog, objectName string, edit func(cmd *Command, oid string)) error {
	// The object is named in this node's git: the scratch directory holds no
	// other ref.
	out, _, revErr := NewCommand(ctx, "rev-parse", "--path-format=absolute", "--git-path", "objects", "--show-object-format", "--verify", "--end-of-options").
		AddDynamicArguments(objectName).RunStdString(&RunOpts{Dir: repo.Path})
	if revErr != nil {
		return revErr
	}
	// One line each: the path may hold spaces.
	lines := strings.Split(strings.TrimSuffix(out, "\n"), "\n")
	if len(lines) != 3 {
		return fmt.Errorf("unexpected git rev-parse output %q", out)
	}
	objects, format, oid := lines[0], lines[1], lines[2]
	old, err := notesRefValue(ctx, repo.Path, nil)
	if err != nil {
		return err
	}

	scratch, err := os.MkdirTemp("", "forgejo-notes")
	if err != nil {
		return err
	}
	defer func() {
		if err := util.RemoveAll(scratch); err != nil {
			log.Error("Unable to remove the scratch notes directory %q: %v", scratch, err)
		}
	}()
	// No hook, no automatic maintenance: the scratch directory is no
	// repository of the cluster, and the objects are this repository's.
	scratchCmd := func() *Command {
		return NewCommand(ctx, "-c", "core.hooksPath=/dev/null", "-c", "gc.auto=0", "-c", "maintenance.auto=false")
	}
	if _, _, err := scratchCmd().AddArguments("init", "--template=", "--bare", "-q").AddOptionFormat("--object-format=%s", format).
		RunStdString(&RunOpts{Dir: scratch}); err != nil {
		return err
	}
	if env == nil {
		env = os.Environ()
	}
	scratchEnv := slices.Concat(env, []string{"GIT_DIR=" + scratch, "GIT_OBJECT_DIRECTORY=" + objects})
	if old != "" {
		if _, _, err := scratchCmd().AddArguments("update-ref", NotesRef).AddDynamicArguments(old).
			RunStdString(&RunOpts{Dir: scratch, Env: scratchEnv}); err != nil {
			return err
		}
	}
	cmd := scratchCmd().AddArguments("notes", "--ref="+NotesRef)
	edit(cmd, oid)
	if _, stderr, err := cmd.RunStdString(&RunOpts{Dir: scratch, Env: scratchEnv}); err != nil {
		log.Error("Error while running git notes: %s", stderr)
		return err
	}
	notes, err := notesRefValue(ctx, scratch, scratchEnv)
	if err != nil {
		return err
	}
	if notes == old {
		// git notes changed nothing: an empty note on an object without one.
		return nil
	}

	stdin := fmt.Sprintf("update %s %s %s\n", NotesRef, notes, old)
	var updateEnv []string
	if old == "" {
		stdin = fmt.Sprintf("create %s %s\n", NotesRef, notes)
		updateEnv = clusterhook.CreateOnlyEnv()
	}
	if _, stderr, err := NewCommand(ctx, "update-ref").AddOptionValues("-m", reflog).AddArguments("--stdin").
		RunStdString(&RunOpts{Dir: repo.Path, Env: updateEnv, Stdin: strings.NewReader(stdin)}); err != nil {
		log.Error("Error while writing %s: %s", NotesRef, stderr)
		return err
	}
	return nil
}

// notesRefValue returns the object NotesRef names in the git directory run
// in, or "" when it is absent.
func notesRefValue(ctx context.Context, dir string, env []string) (string, error) {
	out, _, err := NewCommand(ctx, "for-each-ref", "--format=%(refname) %(objectname)", NotesRef).RunStdString(&RunOpts{Dir: dir, Env: env})
	if err != nil {
		return "", err
	}
	for line := range strings.SplitSeq(out, "\n") {
		if ref, sha, ok := strings.Cut(line, " "); ok && ref == NotesRef {
			return sha, nil
		}
	}
	return "", nil
}
