// Copyright 2026 The Forgejo Authors. All rights reserved.
// SPDX-License-Identifier: GPL-3.0-or-later

//go:build unix

package cluster

import (
	"os"
	"syscall"
)

func inode(st os.FileInfo) uint64 {
	if s, ok := st.Sys().(*syscall.Stat_t); ok {
		return uint64(s.Ino) //nolint:unconvert // uint32 on some platforms
	}
	return 0
}
