// Copyright 2026 The Forgejo Authors. All rights reserved.
// SPDX-License-Identifier: GPL-3.0-or-later

package cluster

import (
	"context"
	"math"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"forgejo.org/modules/clusterhook"
	"forgejo.org/modules/setting"
	"forgejo.org/modules/storage"
	"forgejo.org/modules/test"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// doctorProcess plays, in the test's process, a process of a cluster node
// that runs no replicator, `forgejo doctor` among them: it reaches the
// cluster's cut as such a process does, through the PostgreSQL every
// member shares, here pg's, and registers its deletion gate as `forgejo
// doctor` does, until the test ends. It runs before the test starts any
// goroutine: the setting it reads is not changed under one.
func doctorProcess(ctx context.Context, t *testing.T, pg *fakePG) {
	t.Helper()
	require.False(t, clusterhook.Enabled(), "the doctor runs no replicator")
	t.Cleanup(test.MockVariableValue(&cutWithoutReplicator, func() cutLock { return pg.cut }))
	defer test.MockVariableValue(&setting.Cluster.Enabled, true)()
	t.Cleanup(GateDeletionsWithoutReplicator(ctx))
}

// #90 made a running node delete a stored file under the cluster's cut: a
// file the SQL at a checkpoint's L names is still there when the
// checkpoint lists the files. `forgejo doctor check --run storages --fix`
// (or storage-attachments, storage-lfs, storage-packages, --all --fix)
// runs in a process of its own, without a replicator, and deleted a file
// it found without a row at once (#358).
//
// The checkpoint reads L, a user deletes an attachment on node b (its row
// goes; node b's deletion of the file waits on the cut), the doctor,
// started on any node meanwhile, finds the file without a row and deletes
// it, then the checkpoint's walk lists the files: the doctor's deletion
// waits for the walk, as node b's does, and files.json lists the file the
// SQL at L names. The doctor is what its process does: the orphan test
// against the SQL of now, then a Delete on its storage, a gated one like
// storage.Attachments, with the gate its process registered. Everything
// lives under t.TempDir().
func TestDoctorDeletesAStoredFileUnderTheCut(t *testing.T) {
	const path = "7/f/7f3c2e10-fj35-8doc-tor0-cut000000001"
	for _, doctor := range []bool{false, true} {
		name := "control: no doctor"
		if doctor {
			name = "doctor storage-attachments --fix during the walk"
		}
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			pg := newFakePG()
			if doctor {
				doctorProcess(ctx, t, pg)
			}
			tc := newTestCluster(t, "node-a", "node-b", "node-c")
			coord := withCheckpoints(t, tc, pg)
			hook := &markHook{fakePG: pg}
			coord.wal = hook

			cfg := &setting.Storage{Type: setting.LocalStorageType, Path: t.TempDir()}
			local, err := storage.NewLocalStorage(ctx, cfg)
			require.NoError(t, err)
			// The doctor's storage.Attachments: deletions through the
			// gate its process registered.
			st := storage.WithDeleteGate(local)
			_, err = st.Save(path, strings.NewReader("1234567"), 7)
			require.NoError(t, err)
			key := StorageAttachments + "/" + path
			pg.exec("file+", key, "")
			r := tc.nodes[1].r

			roots := map[string]*setting.Storage{StorageAttachments: cfg}
			cache := newDigestCache()
			_, err = listAndDigest(roots, cache)
			require.NoError(t, err, "an earlier checkpoint read the digests")
			walked := make(chan struct{})
			coord.files = func() (*storedListing, error) {
				// A walk that takes its time on a large storage.
				time.Sleep(300 * time.Millisecond)
				defer close(walked)
				return listStoredFiles(roots, cache)
			}
			serverDeleted := make(chan error, 1)
			doctorDone := make(chan error, 1)
			doctorBeforeWalk := make(chan bool, 1)
			hook.after = func() {
				go func() {
					// The user deletes the attachment on node b: its row,
					// then its file through node b's gate, which waits on
					// the cut.
					pg.exec("file-", key, "")
					if doctor {
						go func() {
							// forgejo doctor check --run storage-attachments
							// --fix: isOrphaned reads the SQL of now, then
							// commonCheckStorage deletes.
							if pg.filesAt(math.MaxUint64)[key] {
								doctorDone <- nil
								return
							}
							err := st.Delete(path)
							select {
							case <-walked:
								doctorBeforeWalk <- false
							default:
								doctorBeforeWalk <- true
							}
							doctorDone <- err
						}()
					} else {
						doctorBeforeWalk <- false
						doctorDone <- nil
					}
					serverDeleted <- r.gateDelete(func() error { return local.Delete(path) })
				}()
			}

			rec, err := coord.take(ctx)
			require.NoError(t, err)
			require.NoError(t, <-doctorDone)
			require.NoError(t, <-serverDeleted)
			assert.False(t, <-doctorBeforeWalk, "the doctor deleted the file before the checkpoint's walk ended")
			_, statErr := os.Stat(filepath.Join(cfg.Path, path))
			require.ErrorIs(t, statErr, os.ErrNotExist, "the file goes once the walk ended")

			named := pg.filesAt(parseFakeLSN(t, rec.LSN))
			require.True(t, named[key], "the SQL at L names the file")
			raw, err := os.ReadFile(filepath.Join(coord.dir, rec.Name(), "files.json"))
			require.NoError(t, err)
			t.Logf("checkpoint %s at L=%s: SQL at L names %s; files.json=%s", rec.Name(), rec.LSN, path, raw)
			assert.Contains(t, string(raw), path,
				"files.json misses an attachment the SQL at L names: recover --from-observer refuses this checkpoint at step 6 (verifyRecovered: \"is in SQL and not in the restored storage\")")
		})
	}
}

// The doctor's gate is bounded, as a running node's: a deletion that waits
// past cutDeleteBound for a checkpoint holding the cut, or whose doctor is
// interrupted, is refused, and the file stays, an orphan a later --fix
// deletes; it is never deleted without the cut.
func TestDoctorDeletionWithoutTheCutIsRefused(t *testing.T) {
	const path = "7/f/7f3c2e10-fj35-8doc-tor0-cut000000002"
	for _, tt := range []struct {
		name   string
		bound  time.Duration
		cancel bool
	}{
		{name: "the bound passes", bound: 100 * time.Millisecond},
		{name: "the doctor is interrupted", bound: time.Hour, cancel: true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			defer test.MockVariableValue(&cutDeleteBound, tt.bound)()
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			pg := newFakePG()
			cfg := &setting.Storage{Type: setting.LocalStorageType, Path: t.TempDir()}
			local, err := storage.NewLocalStorage(ctx, cfg)
			require.NoError(t, err)
			st := storage.WithDeleteGate(local)
			_, err = st.Save(path, strings.NewReader("1234567"), 7)
			require.NoError(t, err)
			doctorProcess(ctx, t, pg)

			// A checkpoint holds the cut, and does not let it go.
			unlock, ok, err := pg.cut.TryExclusive(ctx)
			require.NoError(t, err)
			require.True(t, ok)
			defer unlock()
			if tt.cancel {
				time.AfterFunc(100*time.Millisecond, cancel)
			}
			done := make(chan error, 1)
			go func() { done <- st.Delete(path) }()
			select {
			case err = <-done:
			case <-time.After(10 * time.Second):
				t.Fatal("the doctor's deletion waits past its bound")
			}
			require.ErrorContains(t, err, "waiting for the cluster's checkpoint to list the stored files")
			_, err = os.Stat(filepath.Join(cfg.Path, path))
			require.NoError(t, err, "the file stays: it was not deleted without the cut")
		})
	}
}

// The doctor's gate is registered only where it is needed: a process
// outside a cluster, or one that runs a replicator, whose own gate stays,
// registers nothing; a cluster without PostgreSQL takes no checkpoint, and
// the doctor's deletions go at once there.
func TestDoctorGateOnlyInAClusterWithoutReplicator(t *testing.T) {
	ctx := t.Context()
	pg := newFakePG()
	cfg := &setting.Storage{Type: setting.LocalStorageType, Path: t.TempDir()}
	local, err := storage.NewLocalStorage(ctx, cfg)
	require.NoError(t, err)
	st := storage.WithDeleteGate(local)
	save := func(name string) string {
		_, err := st.Save(name, strings.NewReader("1234567"), 7)
		require.NoError(t, err)
		return filepath.Join(cfg.Path, name)
	}
	defer test.MockVariableValue(&cutWithoutReplicator, func() cutLock { return pg.cut })()
	defer test.MockVariableValue(&cutDeleteBound, 100*time.Millisecond)()
	// A checkpoint holds the cut throughout: a deletion that took it would
	// be refused.
	unlock, ok, err := pg.cut.TryExclusive(ctx)
	require.NoError(t, err)
	require.True(t, ok)
	defer unlock()

	t.Run("outside a cluster", func(t *testing.T) {
		defer test.MockVariableValue(&setting.Cluster.Enabled, false)()
		file := save("a/b/ab000001")
		release := GateDeletionsWithoutReplicator(ctx)
		require.NoError(t, st.Delete("a/b/ab000001"))
		release()
		require.NoFileExists(t, file)
	})

	t.Run("in the process of a replicator", func(t *testing.T) {
		defer test.MockVariableValue(&setting.Cluster.Enabled, true)()
		defer clusterhook.SetEnabled(clusterhook.Enabled())
		clusterhook.SetEnabled(true)
		var replicatorGate int
		storage.SetDeleteGate(func(del func() error) error { replicatorGate++; return del() })
		defer storage.SetDeleteGate(nil)
		release := GateDeletionsWithoutReplicator(ctx)
		file := save("c/d/cd000002")
		require.NoError(t, st.Delete("c/d/cd000002"))
		release()
		require.NoFileExists(t, file)
		file = save("c/d/cd000003")
		require.NoError(t, st.Delete("c/d/cd000003"))
		require.NoFileExists(t, file)
		assert.Equal(t, 2, replicatorGate, "the replicator's gate stays, before and after")
	})

	t.Run("a cluster without PostgreSQL", func(t *testing.T) {
		defer test.MockVariableValue(&setting.Cluster.Enabled, true)()
		defer test.MockVariableValue(&cutWithoutReplicator, func() cutLock { return nil })()
		release := GateDeletionsWithoutReplicator(ctx)
		defer release()
		file := save("e/f/ef000004")
		require.NoError(t, st.Delete("e/f/ef000004"))
		require.NoFileExists(t, file)
	})
}
