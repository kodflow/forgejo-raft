// Copyright 2026 The Forgejo Authors. All rights reserved.
// SPDX-License-Identifier: GPL-3.0-or-later

package webhook

import (
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"forgejo.org/modules/test"

	"github.com/stretchr/testify/require"
)

// A committed task whose enqueue fails is queued again in the background:
// its transaction cannot be replayed to queue it.
func TestCommittedTaskEnqueueIsRetried(t *testing.T) {
	var calls atomic.Int32
	defer test.MockVariableValue(&enqueueRetries, []time.Duration{time.Millisecond, time.Millisecond, time.Millisecond})()
	defer test.MockVariableValue(&pushHookTask, func(int64) error {
		if calls.Add(1) < 3 {
			return errors.New("queue full")
		}
		return nil
	})()
	enqueueCommitted(42)
	require.Eventually(t, func() bool { return calls.Load() == 3 }, 5*time.Second, time.Millisecond)
	time.Sleep(10 * time.Millisecond)
	require.EqualValues(t, 3, calls.Load(), "no further attempt once queued")
}
