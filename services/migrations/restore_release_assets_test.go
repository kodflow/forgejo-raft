// Copyright 2026 The Forgejo Authors. All rights reserved.
// SPDX-License-Identifier: GPL-3.0-or-later

package migrations

import (
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"forgejo.org/models/db"
	repo_model "forgejo.org/models/repo"
	"forgejo.org/models/unittest"
	user_model "forgejo.org/models/user"
	"forgejo.org/modules/gitrepo"
	base "forgejo.org/modules/migration"
	"forgejo.org/modules/setting"
	"forgejo.org/modules/storage"
	"forgejo.org/modules/test"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// mockLocalAttachments points the attachment storage at a local directory
// of the test's own, as a cluster's observers require it.
func mockLocalAttachments(t *testing.T) {
	t.Helper()
	attStorage := &setting.Storage{Type: setting.LocalStorageType, Path: filepath.Join(t.TempDir(), "attachments")}
	t.Cleanup(test.MockVariableValue(&setting.Attachment.Storage, attStorage))
	local, err := storage.NewStorage(setting.LocalStorageType, attStorage)
	require.NoError(t, err)
	t.Cleanup(test.MockVariableValue(&storage.Attachments, local))
}

// attachmentsWithoutFile runs, on the attachments of one release, the
// check step 6 of `forgejo cluster recover --from-observer` runs on every
// attachment (verifyRecovered and storedPresent in
// cmd/cluster_recover_observer.go): a row that is not a link to an
// external URL names a file the storage must hold.
func attachmentsWithoutFile(t *testing.T, releaseID int64) []string {
	t.Helper()
	rows, err := db.GetEngine(t.Context()).Query("SELECT uuid FROM attachment WHERE (external_url IS NULL OR external_url = '') AND release_id = ?", releaseID)
	require.NoError(t, err)
	var missing []string
	for _, r := range rows {
		k := string(r["uuid"])
		if len(k) < 5 {
			continue
		}
		if _, err := os.Stat(filepath.Join(setting.Attachment.Storage.Path, filepath.FromSlash(k[:1]+"/"+k[1:2]+"/"+k))); err != nil {
			missing = append(missing, k)
		}
	}
	return missing
}

func newRepo1Uploader(t *testing.T) *GiteaLocalUploader {
	t.Helper()
	doer := unittest.AssertExistsAndLoadBean(t, &user_model.User{ID: 1})
	repo := unittest.AssertExistsAndLoadBean(t, &repo_model.Repository{ID: 1})
	gitRepo, err := gitrepo.OpenRepository(t.Context(), repo)
	require.NoError(t, err)
	t.Cleanup(func() { gitRepo.Close() })
	uploader := NewGiteaLocalUploader(t.Context(), doer, repo.OwnerName, repo.Name)
	uploader.repo, uploader.gitRepo = repo, gitRepo
	return uploader
}

// `forgejo dump-repo --units releases` (without release_assets) writes
// release.yml with the assets the downloader listed, and no file for them
// (RepositoryDumper.CreateReleases). The GitHub and GitLab downloaders
// give an asset a DownloadFunc only, which YAML drops: the asset is
// written with download_url null. `forgejo restore-repo` reads it back
// with neither, and the uploader used to take that for success and insert
// an attachment row whose file was never written: its download fails, and
// step 6 of `forgejo cluster recover --from-observer` refuses every
// checkpoint taken afterwards (#350). The release is now restored without
// that asset.
func TestRestoreReleaseAssetWithoutFileIsSkipped(t *testing.T) {
	unittest.PrepareTestEnv(t)
	ctx := t.Context()
	mockLocalAttachments(t)

	// 1. dump-repo --units releases: the dumper of dump.go, release
	// assets off. One asset as the GitHub downloader lists it (a
	// DownloadFunc only).
	dumpDir := t.TempDir()
	size, count := 5, 0
	dumper := &RepositoryDumper{ctx: ctx, baseDir: dumpDir, opts: base.MigrateOptions{Releases: true}}
	require.NoError(t, dumper.CreateReleases(&base.Release{
		TagName: "restore-no-such-tag",
		Name:    "v1",
		Assets: []*base.ReleaseAsset{{
			ID: 1, Name: "asset.bin", Size: &size, DownloadCount: &count,
			DownloadFunc: func() (io.ReadCloser, error) { return io.NopCloser(strings.NewReader("bytes")), nil },
		}},
	}))
	require.NoError(t, dumper.releaseFile.Close())
	dumper.releaseFile = nil

	// 2. restore-repo: the restorer reads it back, the uploader writes it.
	restorer, err := NewRepositoryRestorer(ctx, dumpDir, "user2", "repo1", false)
	require.NoError(t, err)
	releases, err := restorer.GetReleases()
	require.NoError(t, err)
	require.Len(t, releases, 1)
	require.Len(t, releases[0].Assets, 1)
	require.Nil(t, releases[0].Assets[0].DownloadURL)
	require.Nil(t, releases[0].Assets[0].DownloadFunc)

	uploader := newRepo1Uploader(t)
	require.NoError(t, uploader.CreateReleases(releases...))

	// The release is restored, without the asset the dump has no file of.
	rel := unittest.AssertExistsAndLoadBean(t, &repo_model.Release{RepoID: uploader.repo.ID, TagName: "restore-no-such-tag"})
	unittest.AssertNotExistsBean(t, &repo_model.Attachment{ReleaseID: rel.ID, Name: "asset.bin"})
	assert.Empty(t, attachmentsWithoutFile(t, rel.ID), "an attachment row without its file is refused at step 6 of recover --from-observer")
}

// The assets that do carry their content are restored as before, beside
// one that does not, and an asset whose file cannot be read still fails
// the restoration: only an asset with no source at all is skipped.
func TestRestoreReleaseAssetsWithAndWithoutFile(t *testing.T) {
	unittest.PrepareTestEnv(t)
	mockLocalAttachments(t)

	dumpDir := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(dumpDir, "with-file.bin"), []byte("bytes"), 0o600))
	withFile := "file://" + filepath.Join(dumpDir, "with-file.bin")
	missing := "file://" + filepath.Join(dumpDir, "no-such-file.bin")
	size, count := 5, 3

	t.Run("mixed", func(t *testing.T) {
		uploader := newRepo1Uploader(t)
		require.NoError(t, uploader.CreateReleases(&base.Release{
			TagName: "restore-mixed",
			Name:    "mixed",
			Assets: []*base.ReleaseAsset{
				{ID: 1, Name: "with-url.bin", Size: &size, DownloadCount: &count, DownloadURL: &withFile},
				{ID: 2, Name: "no-source.bin", Size: &size, DownloadCount: &count},
				{
					ID: 3, Name: "with-func.bin", Size: &size, DownloadCount: &count,
					DownloadFunc: func() (io.ReadCloser, error) { return io.NopCloser(strings.NewReader("bytes")), nil },
				},
			},
		}))

		rel := unittest.AssertExistsAndLoadBean(t, &repo_model.Release{RepoID: uploader.repo.ID, TagName: "restore-mixed"})
		for _, name := range []string{"with-url.bin", "with-func.bin"} {
			att := unittest.AssertExistsAndLoadBean(t, &repo_model.Attachment{ReleaseID: rel.ID, Name: name})
			assert.EqualValues(t, count, att.DownloadCount, name)
			assert.Empty(t, att.ExternalURL, name)
			r, err := storage.Attachments.Open(att.RelativePath())
			require.NoError(t, err, name)
			content, err := io.ReadAll(r)
			r.Close()
			require.NoError(t, err, name)
			assert.Equal(t, "bytes", string(content), name)
		}
		unittest.AssertNotExistsBean(t, &repo_model.Attachment{ReleaseID: rel.ID, Name: "no-source.bin"})
		assert.Empty(t, attachmentsWithoutFile(t, rel.ID))
	})

	t.Run("unreadable file", func(t *testing.T) {
		uploader := newRepo1Uploader(t)
		err := uploader.CreateReleases(&base.Release{
			TagName: "restore-unreadable",
			Name:    "unreadable",
			Assets: []*base.ReleaseAsset{
				{ID: 4, Name: "missing.bin", Size: &size, DownloadCount: &count, DownloadURL: &missing},
			},
		})
		require.Error(t, err)
		unittest.AssertNotExistsBean(t, &repo_model.Release{RepoID: uploader.repo.ID, TagName: "restore-unreadable"})
	})
}
