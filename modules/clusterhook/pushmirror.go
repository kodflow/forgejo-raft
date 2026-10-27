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
	if err := named(); err != nil {
		return err
	}
	still, err := CheckRead(key, repoID)
	if err != nil {
		return refused(err)
	}
	if err := read(); err != nil {
		// A directory the projector moved while git read it fails the
		// read: the move is the reason, and the caller retries.
		if serr := still(); serr != nil {
			return refused(fmt.Errorf("%w (and the read failed: %v)", serr, err))
		}
		return err
	}
	if err := still(); err != nil {
		return refused(err)
	}
	return named()
}
