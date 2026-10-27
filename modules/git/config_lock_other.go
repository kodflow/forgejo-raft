// Copyright 2026 The Forgejo Authors. All rights reserved.
// SPDX-License-Identifier: GPL-3.0-or-later

//go:build !linux

package git

import (
	"io/fs"
	"runtime"
)

// configLockInUse cannot tell here which process holds lock open: it stays.
func configLockInUse(string, fs.FileInfo) string {
	return "which process holds it open is not known on " + runtime.GOOS
}
