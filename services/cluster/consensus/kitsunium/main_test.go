// Copyright 2026 The Forgejo Authors. All rights reserved.
// SPDX-License-Identifier: GPL-3.0-or-later

package kitsunium

import (
	"os"
	"path/filepath"
	"testing"
)

func TestMain(m *testing.M) {
	// The engine's boot-scoped marker directory, which a runner cannot
	// create at its Linux default, /run/kitsunium-consensus.
	RuntimeDir = filepath.Join(os.TempDir(), "kitsunium-consensus")
	os.Exit(m.Run())
}
