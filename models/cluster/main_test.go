// Copyright 2026 The Forgejo Authors. All rights reserved.
// SPDX-License-Identifier: GPL-3.0-or-later

package cluster

import (
	"testing"

	"forgejo.org/models/unittest"

	// The tables the recovery's identifiers advance.
	_ "forgejo.org/models/actions"
	_ "forgejo.org/models/issues"
	_ "forgejo.org/models/repo"
)

func TestMain(m *testing.M) {
	unittest.MainTest(m)
}
