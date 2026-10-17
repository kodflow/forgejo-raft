// Copyright 2026 The Forgejo Authors. All rights reserved.
// SPDX-License-Identifier: GPL-3.0-or-later

//go:build !unix

package cluster

import "os"

func inode(os.FileInfo) uint64 { return 0 }
