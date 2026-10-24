// Copyright 2026 The Forgejo Authors. All rights reserved.
// SPDX-License-Identifier: GPL-3.0-or-later

package cluster

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"forgejo.org/modules/clusterhook"
	"forgejo.org/modules/json"
	"forgejo.org/modules/setting"
	"forgejo.org/modules/storage"
	"forgejo.org/services/cluster/consensus"
	"forgejo.org/services/cluster/reftable"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// refsAtBarrier reads a checkpoint's refs.snapshot as verifyRecovered
// compares it: the live branches, by repository and ref.
func refsAtBarrier(t *testing.T, coord *coordinator, rec reftable.CheckpointRecord) map[string]string {
	t.Helper()
	b, err := os.ReadFile(filepath.Join(coord.dir, rec.Name(), "refs.snapshot"))
	require.NoError(t, err)
	at := reftable.New()
	require.NoError(t, at.Restore(bytes.NewReader(b), consensus.SnapshotMeta{}))
	refs := map[string]string{}
	for _, repo := range at.Repos() {
		rs, _ := at.Refs(repo)
		for name, v := range rs {
			if v.Live() && v.Symbolic == "" {
				refs[repo+" "+name] = v.SHA
			}
		}
	}
	return refs
}

// markHook runs during before, and after after, PostgreSQL's position is read, inside the
// coordinator's window, once.
type markHook struct {
	*fakePG
	during func()
	after  func()
}

func (m *markHook) Mark(ctx context.Context, name string) (walMark, error) {
	if f := m.during; f != nil {
		m.during = nil
		f()
	}
	mark, err := m.fakePG.Mark(ctx, name)
	if f := m.after; f != nil {
		m.after = nil
		f()
	}
	return mark, err
}

// A deletion, a rename, a transfer and an owner's rename write their SQL in
// the request, beside their entry, on the leader or on a follower: a
// checkpoint taken while one runs, whether it started first or the
// operation did, has the refs at B and the SQL at L describe the same
// repositories, as verifyRecovered demands of a restore (#88).
func TestCheckpointCutHoldsTheLifecycleOperations(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	type op struct {
		name  string
		node  int
		repos []string
		// first and second are the two halves, in the order Forgejo runs
		// them: SQL first for a deletion, the entry first otherwise.
		first, second func(r *Replicator, pg *fakePG) error
	}
	sql := func(f func(pg *fakePG)) func(*Replicator, *fakePG) error {
		return func(_ *Replicator, pg *fakePG) error { f(pg); return nil }
	}
	deletion := func(node int) op {
		return op{
			name: "deletion", node: node, repos: []string{"alice/doomed.git"},
			first:  sql(func(pg *fakePG) { pg.exec("drop", "alice/doomed.git", "") }),
			second: func(r *Replicator, _ *fakePG) error { return r.proposeRepoDelete(ctx, "alice/doomed.git", 0) },
		}
	}
	ops := []op{
		deletion(0),
		deletion(1),
		{
			name: "rename", node: 1, repos: []string{"alice/old.git"},
			first: func(r *Replicator, _ *fakePG) error {
				return r.proposeRepoRename(ctx, "alice/old.git", "alice/new.git")
			},
			second: sql(func(pg *fakePG) { pg.exec("move", "alice/old.git", "alice/new.git") }),
		},
		{
			name: "transfer", node: 0, repos: []string{"alice/moved.git"},
			first: func(r *Replicator, _ *fakePG) error {
				return r.proposeRepoMoves(ctx, [][2]string{{"alice/moved.git", "bob/moved.git"}, {"alice/moved.wiki.git", "bob/moved.wiki.git"}})
			},
			second: sql(func(pg *fakePG) { pg.exec("move", "alice/moved.git", "bob/moved.git") }),
		},
		{
			name: "owner rename", node: 2, repos: []string{"carol/a.git", "carol/b.git"},
			first: func(r *Replicator, _ *fakePG) error { return r.proposeOwnerRename(ctx, "carol", "dave") },
			second: sql(func(pg *fakePG) {
				pg.exec("move", "carol/a.git", "dave/a.git")
				pg.exec("move", "carol/b.git", "dave/b.git")
			}),
		},
	}
	for _, o := range ops {
		for _, order := range []string{"operation first", "checkpoint first"} {
			t.Run(o.name+" on node "+string(rune('a'+o.node))+", "+order, func(t *testing.T) {
				t.Parallel()
				tc := newTestCluster(t, "node-a", "node-b", "node-c")
				pg := newFakePG()
				coord := withCheckpoints(t, tc, pg)
				read := make(chan struct{})
				hook := &markHook{fakePG: pg}
				coord.wal = hook
				work := t.TempDir()
				run(t, work, "init", "-q")
				for _, repo := range o.repos {
					sha := commit(t, work, repo)
					resp := tc.pushVia(tc.nodes[0], work, repo, "refs/heads/main", strings.Repeat("0", 40), sha)
					require.True(t, resp.OK, resp.Message)
				}
				// A checkpoint at rest: the SQL of the pushes is in.
				_, err := coord.take(ctx)
				require.NoError(t, err)

				hook.after = func() { close(read) }
				r := tc.nodes[o.node].r
				between := make(chan struct{})
				resume := make(chan struct{})
				done := make(chan error, 1)
				operation := func() {
					release, err := r.holdCut(ctx)
					if err != nil {
						done <- err
						return
					}
					defer release()
					if err := o.first(r, pg); err != nil {
						done <- err
						return
					}
					close(between)
					<-resume
					done <- o.second(r, pg)
				}

				var rec reftable.CheckpointRecord
				taken := make(chan error, 1)
				if order == "operation first" {
					go operation()
					<-between
					go func() {
						var err error
						rec, err = coord.take(ctx)
						taken <- err
					}()
					// The second half once the checkpoint read L, or found
					// the cut held.
					select {
					case <-read:
					case <-pg.cut.refused:
					}
					close(resume)
				} else {
					hook.during = func() {
						go operation()
						// Long enough for an operation that does not wait
						// to run its first half inside the window.
						select {
						case <-between:
						case <-time.After(300 * time.Millisecond):
						}
					}
					close(resume)
					var err error
					rec, err = coord.take(ctx)
					taken <- err
				}
				require.NoError(t, <-taken)
				require.NoError(t, <-done)

				sqlAtL := pg.stateAt(parseFakeLSN(t, rec.LSN))
				assert.Equal(t, refsAtBarrier(t, coord, rec), sqlAtL, "checkpoint %s: the refs at B=%d and the SQL at L=%s differ", rec.Name(), rec.Index, rec.LSN)

				// And the next one, the operation done.
				next, err := coord.take(ctx)
				require.NoError(t, err)
				assert.Equal(t, refsAtBarrier(t, coord, next), pg.stateAt(parseFakeLSN(t, next.LSN)))
			})
		}
	}
}

// A stored file deleted once PostgreSQL's position L is read, by the
// deletion of an attachment or of a repository's LFS objects, on the leader
// or on a follower, is still listed by the checkpoint: the SQL at L names
// it, and a restore needs it (#90). The attachments' digests were read by
// an earlier checkpoint: the cut holds the deletion until the walk ends,
// and the listing needs nothing more of the file.
func TestCheckpointListsEveryFileTheSQLAtLNames(t *testing.T) {
	for _, tt := range []struct {
		name, storage, path string
		node                int
		repoDeleted         bool
	}{
		{name: "attachment on the leader", storage: StorageAttachments, path: "7/f/7f3c2e10-hunt-r5ck-ptfi-les000000001", node: 0},
		{name: "attachment on a follower", storage: StorageAttachments, path: "7/f/7f3c2e10-hunt-r5ck-ptfi-les000000002", node: 1},
		{name: "LFS object of a deleted repository", storage: StorageLFS, path: "4f/26/8c12" + strings.Repeat("0", 58), node: 2, repoDeleted: true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			b := newDeletedAfterL(t, tt.storage, tt.path, tt.node, tt.repoDeleted)
			_, err := listAndDigest(b.roots, b.cache)
			require.NoError(t, err, "an earlier checkpoint read the digests")
			rec, err := b.coord.take(context.Background())
			require.NoError(t, err)
			require.NoError(t, <-b.deleted)
			b.requireGone(t)

			named := b.pg.filesAt(parseFakeLSN(t, rec.LSN))
			require.True(t, named[tt.storage+"/"+tt.path], "the SQL at L names the file")
			files := b.listed(t, rec)
			require.Contains(t, files, tt.path, "checkpoint %s at L=%s: files.json misses a file the SQL at L names", rec.Name(), rec.LSN)
			if tt.storage == StorageAttachments {
				assert.Equal(t, sha256Hex([]byte("1234567")), files[tt.path], "the attachment is listed with its digest")
			}
		})
	}
}

// An attachment no checkpoint has read yet, deleted once L is read: the
// cut holds the deletion until the walk ends, and lets it go before the
// digest is read (#161). The checkpoint then fails, never lists the file
// without its digest nor leaves it out, and the next period, past the
// deletion, closes without it (#173: the deletion the cut held back used
// to race the read of its digest).
func TestCheckpointFailsOnAnAttachmentDeletedBeforeItsDigest(t *testing.T) {
	const path = "7/f/7f3c2e10-hunt-r5ck-ptfi-les000000003"
	for _, node := range []int{0, 1} {
		t.Run(fmt.Sprintf("deleted on node %d", node), func(t *testing.T) {
			b := newDeletedAfterL(t, StorageAttachments, path, node, false)
			var reads []string
			files := b.coord.files
			b.coord.files = func() (*storedListing, error) {
				l, err := files()
				if l != nil {
					l.read = func(p string) (string, error) {
						// The deletion the cut held back goes first.
						require.NoError(t, <-b.deleted)
						reads = append(reads, filepath.Base(p))
						return fileDigest(p)
					}
				}
				return l, err
			}
			_, err := b.coord.take(context.Background())
			require.ErrorContains(t, err, path+": listed, and gone before its digest was read")
			assert.Equal(t, []string{filepath.Base(path)}, reads, "the walk listed the file: the cut held its deletion")
			b.requireGone(t)

			b.coord.files = files
			rec, err := b.coord.take(context.Background())
			require.NoError(t, err)
			assert.False(t, b.pg.filesAt(parseFakeLSN(t, rec.LSN))[StorageAttachments+"/"+path], "the SQL at the next L no longer names the file")
			assert.NotContains(t, b.listed(t, rec), path)
		})
	}
}

// deletedAfterL is a cluster whose checkpoint deletes one stored file once
// PostgreSQL's position L is read: its SQL first, then the file, through
// the deletion gate of one member, as Forgejo does. The walk takes its time.
type deletedAfterL struct {
	coord   *coordinator
	pg      *fakePG
	roots   map[string]*setting.Storage
	cache   *digestCache
	file    string
	deleted chan error
}

func newDeletedAfterL(t *testing.T, storageName, path string, node int, repoDeleted bool) *deletedAfterL {
	t.Helper()
	ctx := context.Background()
	tc := newTestCluster(t, "node-a", "node-b", "node-c")
	pg := newFakePG()
	coord := withCheckpoints(t, tc, pg)
	hook := &markHook{fakePG: pg}
	coord.wal = hook

	cfg := &setting.Storage{Type: setting.LocalStorageType, Path: t.TempDir()}
	local, err := storage.NewLocalStorage(ctx, cfg)
	require.NoError(t, err)
	st := storage.WithDeleteGate(local)
	_, err = st.Save(path, strings.NewReader("1234567"), 7)
	require.NoError(t, err)
	pg.exec("file+", storageName+"/"+path, "")
	r := tc.nodes[node].r
	storage.SetDeleteGate(r.gateDelete)
	t.Cleanup(func() { storage.SetDeleteGate(nil) })
	b := &deletedAfterL{
		coord: coord, pg: pg, roots: map[string]*setting.Storage{storageName: cfg}, cache: newDigestCache(),
		file: filepath.Join(cfg.Path, path), deleted: make(chan error, 1),
	}
	coord.files = func() (*storedListing, error) {
		// A walk that takes its time on a large storage.
		time.Sleep(300 * time.Millisecond)
		return listStoredFiles(b.roots, b.cache)
	}
	hook.after = func() {
		go func() {
			release := func() {}
			if repoDeleted {
				var err error
				if release, err = r.holdCut(ctx); err != nil {
					b.deleted <- err
					return
				}
			}
			defer release()
			pg.exec("file-", storageName+"/"+path, "")
			b.deleted <- st.Delete(path)
		}()
	}
	return b
}

func (b *deletedAfterL) requireGone(t *testing.T) {
	t.Helper()
	_, err := os.Stat(b.file)
	require.ErrorIs(t, err, os.ErrNotExist, "the file goes once the walk ended")
}

// listed is the checkpoint's files.json: each path, with its digest.
func (b *deletedAfterL) listed(t *testing.T, rec reftable.CheckpointRecord) map[string]string {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join(b.coord.dir, rec.Name(), "files.json"))
	require.NoError(t, err)
	var fm clusterhook.FilesManifest
	require.NoError(t, json.Unmarshal(raw, &fm))
	out := map[string]string{}
	for _, f := range fm.Files {
		out[f.Path] = f.SHA256
	}
	return out
}

// attachmentsAt is an attachments storage holding files, by path.
func attachmentsAt(t *testing.T, files map[string]string) *setting.Storage {
	t.Helper()
	cfg := &setting.Storage{Type: setting.LocalStorageType, Path: t.TempDir()}
	for p, content := range files {
		require.NoError(t, os.MkdirAll(filepath.Join(cfg.Path, filepath.Dir(p)), 0o755))
		require.NoError(t, os.WriteFile(filepath.Join(cfg.Path, p), []byte(content), 0o644))
	}
	return cfg
}

// The digests of the attachments are read once the cut is released: a
// first checkpoint on a new leader reads every attachment, and a deletion,
// a rename, a transfer or the deletion of a stored file on any member
// does not wait for it (#161).
func TestCheckpointReadsTheDigestsOutsideTheCut(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	tc := newTestCluster(t, "node-a", "node-b", "node-c")
	pg := newFakePG()
	coord := withCheckpoints(t, tc, pg)
	cfg := attachmentsAt(t, map[string]string{"a/b/ab000001": "release binary", "c/d/cd000002": "another one"})
	follower := tc.nodes[1].r
	var reads []string
	var held []error
	cache := newDigestCache()
	coord.files = func() (*storedListing, error) {
		l, err := listStoredFiles(map[string]*setting.Storage{StorageAttachments: cfg}, cache)
		if l != nil {
			l.read = func(path string) (string, error) {
				// A deletion on a follower takes the cut while the
				// digest is read, without waiting.
				hctx, cancel := context.WithTimeout(ctx, time.Second)
				defer cancel()
				release, err := follower.holdCut(hctx)
				if err == nil {
					release()
				}
				held = append(held, err)
				reads = append(reads, filepath.Base(path))
				return fileDigest(path)
			}
		}
		return l, err
	}
	rec, err := coord.take(ctx)
	require.NoError(t, err)
	assert.Equal(t, []string{"ab000001", "cd000002"}, reads, "every attachment the cache did not hold is read")
	for _, err := range held {
		require.NoError(t, err, "the cut was held while the digests were read")
	}
	b, err := os.ReadFile(filepath.Join(coord.dir, rec.Name(), "files.json"))
	require.NoError(t, err)
	var fm clusterhook.FilesManifest
	require.NoError(t, json.Unmarshal(b, &fm))
	require.Len(t, fm.Files, 2)
	assert.Equal(t, sha256Hex([]byte("release binary")), fm.Files[0].SHA256)
	assert.Equal(t, sha256Hex([]byte("another one")), fm.Files[1].SHA256)

	// The next checkpoint reads nothing again.
	reads = nil
	_, err = coord.take(ctx)
	require.NoError(t, err)
	assert.Empty(t, reads, "the digests are cached")
}

// An attachment listed and deleted before its digest is read fails the
// checkpoint: the SQL at L may name it, and a manifest without its digest
// or without it would not restore it. The digests read are kept, and the
// next period's checkpoint, which no longer lists it, closes (#161).
func TestCheckpointRetriesAnAttachmentGoneBeforeItsDigest(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	tc := newTestCluster(t, "node-a", "node-b", "node-c")
	pg := newFakePG()
	coord := withCheckpoints(t, tc, pg)
	cfg := attachmentsAt(t, map[string]string{"a/b/ab000003": "kept", "c/d/cd000004": "deleted meanwhile"})
	local, err := storage.NewLocalStorage(ctx, cfg)
	require.NoError(t, err)
	follower := tc.nodes[1].r
	reads := 0
	deleteOnRead := true
	cache := newDigestCache()
	coord.files = func() (*storedListing, error) {
		l, err := listStoredFiles(map[string]*setting.Storage{StorageAttachments: cfg}, cache)
		if l != nil {
			l.read = func(path string) (string, error) {
				reads++
				if deleteOnRead {
					deleteOnRead = false
					// The deletion of the other attachment, on a follower,
					// through the deletion gate: it does not wait.
					require.NoError(t, follower.gateDelete(func() error { return local.Delete("c/d/cd000004") }))
				}
				return fileDigest(path)
			}
		}
		return l, err
	}
	_, err = coord.take(ctx)
	require.ErrorContains(t, err, "cd000004: listed, and gone before its digest was read")
	assert.Equal(t, 1, reads)

	rec, err := coord.take(ctx)
	require.NoError(t, err)
	assert.Equal(t, 1, reads, "the digest read by the failed checkpoint is kept")
	b, err := os.ReadFile(filepath.Join(coord.dir, rec.Name(), "files.json"))
	require.NoError(t, err)
	assert.Contains(t, string(b), "ab000003")
	assert.NotContains(t, string(b), "cd000004")
}
