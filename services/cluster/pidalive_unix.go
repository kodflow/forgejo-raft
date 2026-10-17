// Copyright 2026 The Forgejo Authors. All rights reserved.
// SPDX-License-Identifier: GPL-3.0-or-later

//go:build unix

package cluster

import (
	"errors"
	"math"
	"syscall"
)

// pidAlive reports whether a process exists, by signal 0. A pid outside
// kill(2)'s range is not a process: narrowed, it would name a process group.
func pidAlive(pid int) bool {
	if pid <= 0 || pid > math.MaxInt32 {
		return false
	}
	err := syscall.Kill(pid, 0)
	return err == nil || errors.Is(err, syscall.EPERM)
}
