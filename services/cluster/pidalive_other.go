// Copyright 2026 The Forgejo Authors. All rights reserved.
// SPDX-License-Identifier: GPL-3.0-or-later

//go:build !unix

package cluster

// pidAlive cannot probe a process here. It is asked only once a
// transaction's TTL has passed, so answering "gone" lets the TTL release it.
func pidAlive(int) bool { return false }
