// Copyright 2026 The Forgejo Authors. All rights reserved.
// SPDX-License-Identifier: GPL-3.0-or-later

//go:build unix

package cluster

import (
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestPidAlive(t *testing.T) {
	assert.True(t, pidAlive(os.Getpid()))
	// 1<<32 would be narrowed to 0 by kill(2): the caller's process group.
	assert.False(t, pidAlive(1<<32))
	assert.False(t, pidAlive(0))
	assert.False(t, pidAlive(-1))
}
