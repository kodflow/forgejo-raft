// Copyright 2026 The Forgejo Authors. All rights reserved.
// SPDX-License-Identifier: GPL-3.0-or-later

//go:build linux

package git

import (
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"syscall"
)

// configLockInUse says why lock may still be a live git's, "" when it cannot
// be. Its creator owns it: when that is this process's user, every process
// that could hold it open is one whose descriptors /proc shows this one.
func configLockInUse(lock string, info fs.FileInfo) string {
	st, ok := info.Sys().(*syscall.Stat_t)
	if !ok {
		return "its owner is unknown"
	}
	if int(st.Uid) != os.Geteuid() {
		return fmt.Sprintf("it belongs to uid %d, not to this process's user", st.Uid)
	}
	resolved, err := filepath.EvalSymlinks(lock)
	if err != nil {
		return err.Error()
	}
	// Glob passes over the directories it cannot read: other users'.
	fds, err := filepath.Glob("/proc/[0-9]*/fd/*")
	if err != nil || len(fds) == 0 {
		return "/proc shows no open file"
	}
	for _, fd := range fds {
		if target, err := os.Readlink(fd); err == nil && target == resolved {
			pid, _, _ := strings.Cut(strings.TrimPrefix(fd, "/proc/"), "/")
			return "process " + pid + " holds it open"
		}
	}
	return ""
}
