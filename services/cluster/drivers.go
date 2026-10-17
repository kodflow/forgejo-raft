// Copyright 2026 The Forgejo Authors. All rights reserved.
// SPDX-License-Identifier: GPL-3.0-or-later

package cluster

import (
	// The kitsunium driver registers itself with the consensus boundary; it
	// is the one [cluster] DRIVER selects by default.
	_ "forgejo.org/services/cluster/consensus/kitsunium"
)
