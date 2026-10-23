// Copyright 2026 The Forgejo Authors. All rights reserved.
// SPDX-License-Identifier: GPL-3.0-or-later

//go:build unix

package cluster

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// archive-wal.sh, the POSIX twin of wal-archive, answers success only after
// it synced the archive directory, on every path: its own link, a retry of
// a file already archived, and a link another archiver won (#113).
func TestArchiveScriptSyncsTheDirectoryOnEverySuccess(t *testing.T) {
	script, err := filepath.Abs("../../contrib/cluster/observer/archive-wal.sh")
	require.NoError(t, err)
	root := t.TempDir()
	bin, archive := filepath.Join(root, "bin"), filepath.Join(root, "archive")
	require.NoError(t, os.MkdirAll(bin, 0o700))
	require.NoError(t, os.MkdirAll(archive, 0o700))
	spy := filepath.Join(root, "sync.log")
	// The spy records each sync; the real one is not needed for the test.
	require.NoError(t, os.WriteFile(filepath.Join(bin, "sync"), fmt.Appendf(nil, "#!/bin/sh\necho \"$*\" >> %q\n", spy), 0o755))
	src := filepath.Join(root, "000000010000000000000001")
	require.NoError(t, os.WriteFile(src, []byte("segment"), 0o600))
	run := func() {
		t.Helper()
		cmd := exec.Command("/bin/sh", script, archive, src, filepath.Base(src))
		cmd.Env = append(os.Environ(), "PATH="+bin+string(os.PathListSeparator)+os.Getenv("PATH"))
		out, err := cmd.CombinedOutput()
		require.NoError(t, err, "%s", out)
	}
	synced := func() []string {
		b, err := os.ReadFile(spy)
		require.NoError(t, err)
		require.NoError(t, os.Remove(spy))
		return strings.Split(strings.TrimSpace(string(b)), "\n")
	}

	run()
	assert.Contains(t, synced(), archive, "its own link")
	run()
	assert.Equal(t, []string{archive}, synced(), "a retry of a file already archived")

	// Another archiver wins the link: ln fails, the bytes compare equal.
	require.NoError(t, os.Remove(filepath.Join(archive, filepath.Base(src))))
	ln := filepath.Join(bin, "ln")
	require.NoError(t, os.WriteFile(ln, fmt.Appendf(nil, "#!/bin/sh\ncp %q %q\nexit 1\n", src, filepath.Join(archive, filepath.Base(src))), 0o755))
	run()
	assert.Contains(t, synced(), archive, "a link another archiver won")
}
