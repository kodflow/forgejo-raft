// Copyright 2026 The Forgejo Authors. All rights reserved.
// SPDX-License-Identifier: GPL-3.0-or-later

package cluster

import (
	"bytes"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
)

// ArchiveWAL is PostgreSQL's archive_command on a member: it copies one
// WAL file, %p named %f, into the local archive the export serves. It is
// idempotent, as PostgreSQL requires: a file already archived with the same
// bytes is success, which a retry after a crash and a promoted standby
// archiving a shared segment both meet; different bytes under the same name
// are refused, and the archive is never overwritten. The copy is synced
// before it takes its name. Timeline histories and backup history files are
// archived like segments, and so is the old timeline's last segment that a
// promoted standby archives with a .partial suffix: PostgreSQL retries a
// refused file forever and archives nothing after it, so refusing it would
// stop the archive at the first failover. The export never serves it; the
// new timeline's copy of that segment is what a recovery replays.
func ArchiveWAL(src, name, dir string) error {
	if !validArchiveName(name) {
		return fmt.Errorf("%q is not a WAL file name", name)
	}
	if dir == "" {
		return errors.New("[cluster] WAL_ARCHIVE_DIR is empty")
	}
	dst := filepath.Join(dir, name)
	in, err := os.ReadFile(src)
	if err != nil {
		return err
	}
	if have, err := os.ReadFile(dst); err == nil {
		if sha256.Sum256(have) == sha256.Sum256(in) {
			return nil
		}
		return fmt.Errorf("%s is already archived with other bytes; refusing to overwrite it", name)
	} else if !errors.Is(err, os.ErrNotExist) {
		return err
	}
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return err
	}
	tmp, err := os.CreateTemp(dir, "."+name+".*.partial")
	if err != nil {
		return err
	}
	defer os.Remove(tmp.Name())
	_, err = io.Copy(tmp, bytes.NewReader(in))
	if err == nil {
		err = tmp.Sync()
	}
	if cerr := tmp.Close(); err == nil {
		err = cerr
	}
	if err != nil {
		return err
	}
	// A link, not a rename: it fails if another archiver got there first,
	// and then the bytes are compared again.
	if err := os.Link(tmp.Name(), dst); err != nil {
		if errors.Is(err, os.ErrExist) {
			return ArchiveWAL(src, name, dir)
		}
		return err
	}
	return syncDir(dir)
}

// partialWALPattern is the old timeline's last segment after a promotion.
var partialWALPattern = regexp.MustCompile(`^[0-9A-F]{24}\.partial$`)

// validArchiveName accepts what archive_command is given: the export's WAL
// names and the .partial segment of a promotion.
func validArchiveName(name string) bool {
	return ValidWALName(name) || partialWALPattern.MatchString(name)
}
