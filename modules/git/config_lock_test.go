// Copyright 2026 The Forgejo Authors. All rights reserved.
// SPDX-License-Identifier: GPL-3.0-or-later

package git

import (
	"os"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	"forgejo.org/modules/setting"
	"forgejo.org/modules/test"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// A git config killed between taking the global config's lock and renaming
// it (a container torn down during a node's first start, an OOM kill, a
// power loss) leaves $HOME/.gitconfig.lock. Every later `git config
// --global` failed on it: syncGitConfig at every start, so the node never
// started again until an operator removed it (#308). A lock older than
// staleConfigLockAge that no process holds open is now removed; a younger
// one, or one a process holds open, stays: it may be a live git's.
func TestSyncGitConfigStaleLock(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skip("which process holds a file open is read from /proc")
	}
	// The global config is $HOME/.gitconfig, as on a node: not a file the
	// environment names instead.
	t.Setenv("GIT_CONFIG_GLOBAL", "")
	require.NoError(t, os.Unsetenv("GIT_CONFIG_GLOBAL"))
	// leave makes a new home holding the lock a git left there age ago:
	// nothing written yet, every value of syncGitConfig still to write.
	leave := func(t *testing.T, age time.Duration) string {
		home := t.TempDir()
		t.Cleanup(test.MockVariableValue(&setting.Git.HomePath, home))
		lock := filepath.Join(home, ".gitconfig.lock")
		require.NoError(t, os.WriteFile(lock, nil, 0o600))
		then := time.Now().Add(-age)
		require.NoError(t, os.Chtimes(lock, then, then))
		return lock
	}

	t.Run("left by a killed git", func(t *testing.T) {
		lock := leave(t, time.Hour)
		require.NoError(t, syncGitConfig(), "the node starts")
		assert.NoFileExists(t, lock)
		assert.True(t, gitConfigContains("hooksPath"))
	})
	t.Run("younger than the age", func(t *testing.T) {
		lock := leave(t, 0)
		require.ErrorContains(t, syncGitConfig(), "could not lock config file")
		assert.FileExists(t, lock, "a lock of the last moments may be a live git's")
	})
	t.Run("held open", func(t *testing.T) {
		// This process holds it open, as a live git does while it writes
		// the file, however long that takes.
		lock := leave(t, time.Hour)
		f, err := os.Open(lock)
		require.NoError(t, err)
		defer f.Close()
		require.ErrorContains(t, syncGitConfig(), "could not lock config file")
		assert.FileExists(t, lock, "a process holds it open")
	})
}
