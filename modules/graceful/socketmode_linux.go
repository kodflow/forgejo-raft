// Copyright 2026 The Forgejo Authors. All rights reserved.
// SPDX-License-Identifier: GPL-3.0-or-later

package graceful

import (
	"net"
	"os"
)

// chmodSocket sets a listening socket's mode through its descriptor, which
// never follows a path an attacker could have replaced.
func chmodSocket(l *net.UnixListener, _ string, mode os.FileMode) error {
	f, err := l.File()
	if err != nil {
		return err
	}
	defer f.Close()
	return f.Chmod(mode)
}
