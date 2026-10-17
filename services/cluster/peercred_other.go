// Copyright 2026 The Forgejo Authors. All rights reserved.
// SPDX-License-Identifier: GPL-3.0-or-later

//go:build !linux && !darwin

package cluster

import (
	"errors"
	"net"
)

// peerUID is not available here: the cluster is supported on Linux, and on
// macOS for development.
func peerUID(*net.UnixConn) (int, error) {
	return -1, errors.New("peer credentials are not supported on this system")
}
