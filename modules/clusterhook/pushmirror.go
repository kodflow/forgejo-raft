// Copyright 2026 The Forgejo Authors. All rights reserved.
// SPDX-License-Identifier: GPL-3.0-or-later

package clusterhook

import "fmt"

// ReadBetweenChecks runs read, which reads this node's directory under key
// and must read the repository repoID and nothing else, between two checks
// that the directory holds it. A check made before the read leaves the time
// the read takes, and nothing keeps the projector's moves out of the
// directory meanwhile: the repository renamed away and another one, a
// private one, renamed or created under its name, both projected right
// after the check, and the read took that one's directory (#261, #283). A
// directory whose stamp carries no repository ID, the stamp of a
// repository written only by a fork, a migration or a mirror's sync, is
// judged by the replicated table alone, which rightly holds, under the
// name, the repository a rename put there: only the caller's database can
// tell that the name is no longer the repository's own (#294).
//
// named is that check: the repository's row, read again by its ID, must
// still give key. It returns a refusal the caller already wrapped, or the
// database's error. Right before the read, named, then CheckRead; once
// read returned, before anything it read leaves the caller, still() and
// named again. A failed check refuses with refused, and so does a read
// that failed while the directory moved, the move being its reason. A
// template's generation, a fork and a push mirror's sync read their
// source's directory through it. Without a cluster it only runs read.
func ReadBetweenChecks(key string, repoID int64, named func() error, refused func(error) error, read func() error) error {
	if !Enabled() {
		return read()
	}
	g, err := GuardDirectory(key, repoID, named, refused)
	if err != nil {
		return err
	}
	if err := read(); err != nil {
		return g.Failed(err)
	}
	return g.Check()
}

// DirectoryGuard is ReadBetweenChecks's checks for a caller that runs
// several git commands in this node's directory under key, writes among
// them, and must touch the repository repoID and nothing else: a pull
// mirror's sync, which loaded its repository once, at its start, and
// recreated its remote, fetched into the directory and pruned it, under
// that name, past no check at all (#322). GuardDirectory makes the first
// check, before anything touches the directory; Check, right before each
// git command that writes there and once the last read is done, says
// whether the directory may have held another repository since. A git
// command that started in the directory the check passed keeps working in
// it when the projector moves it, as its working directory follows the
// move: what is left is the time from the check to the command's start.
type DirectoryGuard struct {
	named   func() error
	refused func(error) error
	still   func() error
}

// GuardDirectory runs named, then CheckRead, as ReadBetweenChecks does
// before its read: a failed check refuses with refused, before anything
// touched the directory. Without a cluster it checks nothing, and returns
// a guard whose checks pass.
func GuardDirectory(key string, repoID int64, named func() error, refused func(error) error) (*DirectoryGuard, error) {
	if !Enabled() {
		return &DirectoryGuard{}, nil
	}
	if err := named(); err != nil {
		return nil, err
	}
	still, err := CheckRead(key, repoID)
	if err != nil {
		return nil, refused(err)
	}
	return &DirectoryGuard{named: named, refused: refused, still: still}, nil
}

// Check is still() and named again: nil when the directory held the
// repository, and nothing else, since GuardDirectory, and its row still
// gives the name.
func (g *DirectoryGuard) Check() error {
	if g == nil || g.still == nil {
		return nil
	}
	if err := g.still(); err != nil {
		return g.refused(err)
	}
	return g.named()
}

// Failed is the reason a git command in the directory failed with err: a
// directory the projector moved meanwhile fails the command, and the move,
// a refusal the caller retries, is then its reason.
func (g *DirectoryGuard) Failed(err error) error {
	if g == nil || g.still == nil {
		return err
	}
	if serr := g.still(); serr != nil {
		return g.refused(fmt.Errorf("%w (and git failed there: %v)", serr, err))
	}
	return err
}
