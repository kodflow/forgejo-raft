// Copyright 2026 The Forgejo Authors. All rights reserved.
// SPDX-License-Identifier: GPL-3.0-or-later

//go:build !linux && !darwin

package observer

func freeBytes(string) (uint64, bool) { return 0, false }
