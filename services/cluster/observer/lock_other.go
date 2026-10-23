// Copyright 2026 The Forgejo Authors. All rights reserved.
// SPDX-License-Identifier: GPL-3.0-or-later

//go:build !unix

package observer

import "os"

// lockDir opens the observer directory's lock file; this platform has no
// advisory lock, so a second process is not refused.
func lockDir(path string) (*os.File, error) {
	return os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0o600)
}
