// Copyright 2026 The Forgejo Authors. All rights reserved.
// SPDX-License-Identifier: GPL-3.0-or-later

package git

import (
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"time"

	"forgejo.org/modules/log"
)

// staleConfigLockAge is how old a lock of the global git config must be
// before syncGitConfig takes it for one a killed git left behind. git holds
// it for the milliseconds of one write; a writer this process cannot see (in
// another container sharing the volume, on another host sharing the
// directory) is never that slow.
const staleConfigLockAge = 30 * time.Second

// removeStaleConfigLock removes home's .gitconfig.lock when a git killed
// between taking it and renaming it into place left it behind: a container
// torn down during a first start, an OOM kill, a power loss. Every later
// `git config --global` fails on it, and so does every start (#308). It goes
// only once older than staleConfigLockAge, owned by this process's user, and
// held open by no process; otherwise it stays, and git's error stands.
func removeStaleConfigLock(home string, now time.Time) {
	lock, err := filepath.Abs(filepath.Join(home, ".gitconfig.lock"))
	if err != nil {
		return
	}
	info, err := os.Lstat(lock)
	if err != nil || !info.Mode().IsRegular() {
		return
	}
	age := now.Sub(info.ModTime())
	if age < staleConfigLockAge {
		return
	}
	if why := configLockInUse(lock, info); why != "" {
		log.Warn("git: %s is %s old and stays: %s", lock, age.Round(time.Second), why)
		return
	}
	if err := os.Remove(lock); err != nil && !errors.Is(err, fs.ErrNotExist) {
		log.Warn("git: %s is stale and could not be removed: %v", lock, err)
		return
	}
	log.Warn("git: removed %s, %s old and held open by no process: a git killed while it wrote the global config left it, and every later write of that config failed", lock, age.Round(time.Second))
}
