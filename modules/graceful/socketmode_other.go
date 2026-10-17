// Copyright 2026 The Forgejo Authors. All rights reserved.
// SPDX-License-Identifier: GPL-3.0-or-later

//go:build !linux

package graceful

import (
	"fmt"
	"net"
	"os"
)

// chmodSocket sets a listening socket's mode by its path: macOS and the BSDs
// refuse fchmod on a socket descriptor (EINVAL). The path is checked to
// still be a socket, not a symbolic link put in its place, right before.
func chmodSocket(_ *net.UnixListener, path string, mode os.FileMode) error {
	st, err := os.Lstat(path)
	if err != nil {
		return err
	}
	if st.Mode()&os.ModeSocket == 0 {
		return fmt.Errorf("%s is no longer the socket just created", path)
	}
	return os.Chmod(path, mode)
}
