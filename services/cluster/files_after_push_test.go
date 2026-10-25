// Copyright 2026 The Forgejo Authors. All rights reserved.
// SPDX-License-Identifier: GPL-3.0-or-later

package cluster

import (
	"os"
	"path"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"forgejo.org/models"
	"forgejo.org/models/db"
	repo_model "forgejo.org/models/repo"
	"forgejo.org/models/unittest"
	user_model "forgejo.org/models/user"
	"forgejo.org/modules/clusterhook"
	"forgejo.org/modules/setting"
	"forgejo.org/modules/test"
	files_service "forgejo.org/services/repository/files"

	gouuid "github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// main moves from c2 to c3 through node-b, acknowledged. On node-a, which
// has not projected it, three services cloned main from node-a's git, at
// c2, and built a new branch on it, created for the whole cluster without
// the push (#239): ApplyDiffPatch (the API's /diffpatch, the web "apply
// patch", the web cherry-pick's fallback), UploadRepoFiles (the web
// upload) and CherryPick (the web cherry-pick). #233 made ChangeRepoFiles
// alone wait (#226). Now each waits for node-a to serve the repository,
// and is refused while it does not; an upload whose request read main's
// head before node-a caught up is refused too, its parent being the
// commit before the push.
func TestFilesWrittenToANewBranchRightAfterAPushStartFromIt(t *testing.T) {
	tc, a, b, work, _, c2, hold := laggingCluster(t)
	ctx := db.DefaultContext
	zero := strings.Repeat("0", 40)

	// The commit to cherry-pick is on a branch every node serves: it adds
	// pick.txt to c2.
	a.r.registry.release(hold)
	run(t, work, "checkout", "-q", "-b", "pick")
	require.NoError(t, os.WriteFile(filepath.Join(work, "pick.txt"), []byte("pick\n"), 0o644))
	run(t, work, "add", "pick.txt")
	run(t, work, "commit", "-q", "-m", "pick")
	pick := run(t, work, "rev-parse", "HEAD")
	run(t, work, "checkout", "-q", "main")
	resp := tc.pushVia(a, work, laggingRepo, "refs/heads/pick", zero, pick)
	require.True(t, resp.OK, resp.Message)
	for _, m := range tc.nodes {
		require.Eventually(t, func() bool { return gitRef(t, m, laggingRepo, "refs/heads/pick") == pick }, 15*time.Second, 20*time.Millisecond, "%s projects pick", m.slot)
	}
	// node-a stops projecting the repository again.
	hold = clusterhook.NewNonce()
	require.NoError(t, a.r.registry.register(hold, laggingRepo, nil, os.Getpid(), false))
	c3 := commit(t, work, "three")
	pushOnB(t, tc, a, b, work, "refs/heads/main", c2, c3)

	withGitThroughReplicator(t, a)
	freshFrom(t, a)
	t.Cleanup(test.MockVariableValue(&setting.Repository.Upload.TempPath, t.TempDir()))
	sqlRepo := unittest.AssertExistsAndLoadBean(t, &repo_model.Repository{ID: 1})
	doer := unittest.AssertExistsAndLoadBean(t, &user_model.User{ID: 2})

	patch := func(branch string) error {
		_, err := files_service.ApplyDiffPatch(ctx, sqlRepo, doer, &files_service.ApplyDiffPatchOptions{
			OldBranch: "main", NewBranch: branch, Message: "apply " + branch,
			Content: "diff --git a/lag-patch.txt b/lag-patch.txt\n" +
				"new file mode 100644\n" +
				"index 0000000000000000000000000000000000000000..9eb7b90ed5011100198485751247aa229d4fe0dc\n" +
				"--- /dev/null\n" +
				"+++ b/lag-patch.txt\n" +
				"@@ -0,0 +1 @@\n" +
				"+patch\n",
		})
		return err
	}
	// lastCommit is what the web form passes: main's head as node-a's git
	// shows it when the request starts.
	upload := func(branch, lastCommit string) error {
		up := &repo_model.Upload{UUID: gouuid.New().String(), Name: branch + ".txt"}
		require.NoError(t, os.MkdirAll(path.Dir(up.LocalPath()), 0o755))
		require.NoError(t, os.WriteFile(up.LocalPath(), []byte(branch+"\n"), 0o644))
		require.NoError(t, db.Insert(ctx, up))
		return files_service.UploadRepoFiles(ctx, sqlRepo, doer, &files_service.UploadRepoFileOptions{
			LastCommitID: lastCommit, OldBranch: "main", NewBranch: branch, Message: "upload " + branch,
			Files: []string{up.UUID}, FullPaths: []string{up.Name},
		})
	}
	cherryPick := func(branch string) error {
		_, err := files_service.CherryPick(ctx, sqlRepo, doer, false, &files_service.ApplyDiffPatchOptions{
			OldBranch: "main", NewBranch: branch, Message: "pick " + branch, Content: pick,
		})
		return err
	}
	branches := []string{"lag-patch", "lag-upload", "lag-pick"}
	notWritten := func(t *testing.T, branches ...string) {
		t.Helper()
		for _, m := range tc.nodes {
			for _, br := range branches {
				v, ok := m.r.table.Get(laggingRepo, "refs/heads/"+br)
				assert.False(t, ok && v.Live(), "%s: %s was written", m.slot, br)
			}
		}
	}

	t.Run("node-a has not projected the push", func(t *testing.T) {
		read := gitRef(t, a, laggingRepo, "refs/heads/main")
		require.Equal(t, c2, read)
		err := patch("lag-patch")
		t.Logf("ApplyDiffPatch to a new branch on node-a, behind: err=%v", err)
		require.ErrorIs(t, err, clusterhook.ErrBehind, "the patch was applied on node-a's main, at c2")
		err = upload("lag-upload", read)
		t.Logf("UploadRepoFiles to a new branch on node-a, behind: err=%v", err)
		require.ErrorIs(t, err, clusterhook.ErrBehind, "the files were uploaded on node-a's main, at c2")
		err = cherryPick("lag-pick")
		t.Logf("CherryPick to a new branch on node-a, behind: err=%v", err)
		require.ErrorIs(t, err, clusterhook.ErrBehind, "the commit was picked on node-a's main, at c2")
		t.Logf("main: c2=%.10s before the push, c3=%.10s acknowledged", c2, c3)
		logRefState(t, tc, laggingRepo, "refs/heads/lag-patch", "refs/heads/lag-upload", "refs/heads/lag-pick")
		notWritten(t, branches...)
	})

	// The web form read main's head in node-a's git, at c2, then node-a
	// caught up while the upload waited.
	read := gitRef(t, a, laggingRepo, "refs/heads/main")
	caughtUp(t, a, hold, "refs/heads/main", c3)
	t.Run("an upload whose request read main before node-a caught up", func(t *testing.T) {
		err := upload("lag-upload", read)
		t.Logf("UploadRepoFiles with main read at %.10s, node-a at %.10s: err=%v", read, c3, err)
		require.True(t, models.IsErrCommitIDDoesNotMatch(err), "the files were committed on main's tree at c3 with c2 for parent: %v", err)
		notWritten(t, "lag-upload")
	})

	// Once node-a serves the push, the new branches are built on it. The
	// cherry-pick waits as the patch does, before the same Validate: its
	// write, as slow as theirs through the test's hook, is not repeated
	// here.
	require.NoError(t, patch("lag-patch"))
	require.NoError(t, upload("lag-upload", c3))
	dirA := filepath.Join(a.r.root, filepath.FromSlash(laggingRepo))
	for _, br := range branches[:2] {
		for _, m := range tc.nodes {
			require.Eventually(t, func() bool {
				v, ok := m.r.table.Get(laggingRepo, "refs/heads/"+br)
				return ok && v.Live()
			}, 10*time.Second, 20*time.Millisecond, "%s: %s", m.slot, br)
		}
		parent := strings.TrimSpace(run(t, dirA, "rev-parse", "refs/heads/"+br+"^"))
		t.Logf("%s's parent=%.10s (c3=%.10s)", br, parent, c3)
		assert.Equal(t, c3, parent, "%s is built on main as the cluster acknowledged it", br)
	}
}
