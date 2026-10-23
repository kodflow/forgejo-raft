// Copyright 2026 The Forgejo Authors. All rights reserved.
// SPDX-License-Identifier: GPL-3.0-or-later

package kitsunium

import (
	"os"
	"testing"
)

func TestMain(m *testing.M) {
	// The engine's boot-scoped marker directory, which a runner cannot
	// create at its Linux default, /run/kitsunium-consensus: one of this
	// binary's own, removed after the suite.
	dir, err := os.MkdirTemp("", "kitsunium-consensus-")
	if err != nil {
		panic(err)
	}
	RuntimeDir = dir
	code := m.Run()
	_ = os.RemoveAll(dir)
	os.Exit(code)
}
