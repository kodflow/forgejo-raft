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
	"sync"
	"testing"
	"time"

	cluster_model "forgejo.org/models/cluster"
	"forgejo.org/modules/clusterhook"
	"forgejo.org/modules/setting"
	"forgejo.org/services/cluster/consensus"
	"forgejo.org/services/cluster/reftable"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// fakePG stands for PostgreSQL: the consumer's work appends one row per
// event, each at the next WAL position, and Mark reads the position after
// the last committed row, as pg_current_wal_flush_lsn does.
type fakePG struct {
	mu   sync.Mutex
	rows []pgRow
	// marks are the restore points created, by name.
	marks map[string]uint64
	// foreign is what Foreign reports.
	dbs, spcs []string
	// cut is the advisory lock every member shares.
	cut *memCut
}

// pgRow is a branch row written; op names a web request's own SQL
// instead: "drop" removes repo's rows, "move" moves them to ref, "file+"
// and "file-" add and remove the row naming the stored file repo.
type pgRow struct {
	lsn                uint64
	op, repo, ref, sha string
}

// memCut is the cluster's cut as one in-memory lock the members share.
// refused is signalled when a checkpoint finds it held.
type memCut struct {
	mu      sync.RWMutex
	refused chan struct{}
}

func (c *memCut) Shared(ctx context.Context) (func(), error) {
	for !c.mu.TryRLock() {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(2 * time.Millisecond):
		}
	}
	return c.mu.RUnlock, nil
}

func (c *memCut) TryExclusive(context.Context) (func(), bool, error) {
	if !c.mu.TryLock() {
		select {
		case c.refused <- struct{}{}:
		default:
		}
		return func() {}, false, nil
	}
	return c.mu.Unlock, true, nil
}

// exec commits a web request's own SQL, at the next WAL position.
func (p *fakePG) exec(op, repo, ref string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.rows = append(p.rows, pgRow{lsn: uint64(len(p.rows)) + 1, op: op, repo: repo, ref: ref})
}

// filesAt is the stored files the SQL before lsn names.
func (p *fakePG) filesAt(lsn uint64) map[string]bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	out := map[string]bool{}
	for _, r := range p.rows {
		switch {
		case r.lsn >= lsn:
		case r.op == "file+":
			out[r.repo] = true
		case r.op == "file-":
			delete(out, r.repo)
		}
	}
	return out
}

func newFakePG() *fakePG {
	return &fakePG{marks: map[string]uint64{}, cut: &memCut{refused: make(chan struct{}, 1)}}
}

func (p *fakePG) work(_ context.Context, evs []reftable.Event) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	for _, ev := range evs {
		if ev.Ref == "" {
			continue // a move or a deletion: its SQL is the web request's
		}
		p.rows = append(p.rows, pgRow{lsn: uint64(len(p.rows)) + 1, repo: ev.Repo, ref: ev.Ref, sha: ev.New})
	}
	return nil
}

func (p *fakePG) Mark(_ context.Context, name string) (walMark, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	end := uint64(len(p.rows)) + 1
	p.marks[name] = end
	return walMark{LSN: fmt.Sprintf("0/%X", end), Timeline: 1, SystemID: "7400000000000000001", SegmentSize: 16 << 20}, nil
}

func (p *fakePG) SwitchWAL(context.Context) error { return nil }

func (p *fakePG) Foreign(context.Context) ([]string, []string, error) { return p.dbs, p.spcs, nil }

// stateAt replays the rows before lsn, as recovery_target_lsn with
// recovery_target_inclusive = off does: the branch rows SQL holds there.
func (p *fakePG) stateAt(lsn uint64) map[string]string {
	p.mu.Lock()
	defer p.mu.Unlock()
	out := map[string]string{}
	for _, r := range p.rows {
		if r.lsn >= lsn {
			continue
		}
		switch r.op {
		case "":
			out[r.repo+" "+r.ref] = r.sha
		case "drop", "move":
			for k, v := range out {
				if repo, ref, _ := strings.Cut(k, " "); repo == r.repo {
					delete(out, k)
					if r.op == "move" {
						out[r.ref+" "+ref] = v
					}
				}
			}
		}
	}
	return out
}

func parseFakeLSN(t *testing.T, s string) uint64 {
	var v uint64
	_, err := fmt.Sscanf(strings.TrimPrefix(s, "0/"), "%X", &v)
	require.NoError(t, err)
	return v
}

func withCheckpoints(t *testing.T, tc *testCluster, pg *fakePG) *coordinator {
	for _, n := range tc.nodes {
		n.r.consumer.work = pg.work
		n.r.cut = pg.cut
	}
	leader := tc.nodes[0].r
	c := newCoordinator(leader, time.Hour, t.TempDir(), pg)
	c.files = func() (*storedListing, error) { return nil, nil }
	c.backup = tc.cursors.set
	leader.checkpoints = c
	for _, n := range tc.nodes {
		n.r.export.ckptDir = c.dir
	}
	return c
}

// The refs at a checkpoint's barrier and the SQL at its WAL position
// describe the same pushes, while pushes keep landing on every member
// during the checkpoints: the SQL of an entry before the barrier is in,
// the SQL of an entry after it is out (observer plan v4, causal cut).
func TestBarrierCutRefsMatchSQL(t *testing.T) {
	tc := newTestCluster(t, "node-a", "node-b", "node-c")
	pg := newFakePG()
	coord := withCheckpoints(t, tc, pg)
	zero := strings.Repeat("0", 40)

	// Each writer owns a repository and pushes one commit after another
	// through its member, followers included.
	var wg sync.WaitGroup
	for i, n := range tc.nodes {
		wg.Go(func() {
			work := t.TempDir()
			run(t, work, "init", "-q")
			repo := fmt.Sprintf("user%d/cut.git", i)
			old := zero
			for k := range 8 {
				sha := commit(t, work, fmt.Sprintf("%d-%d", i, k))
				if resp := tc.pushVia(n, work, repo, "refs/heads/main", old, sha); resp.OK {
					old = sha
				}
			}
		})
	}
	writing := make(chan struct{})
	go func() { wg.Wait(); close(writing) }()

	// Checkpoints while the pushes land, and one once they are done.
	var closed []reftable.CheckpointRecord
	for running := true; running; {
		select {
		case <-writing:
			running = false
		case <-time.After(200 * time.Millisecond):
		}
		rec, err := coord.take(context.Background())
		require.NoError(t, err)
		closed = append(closed, rec)
	}
	require.GreaterOrEqual(t, len(closed), 4)
	// Each checkpoint records the backup cursor at its barrier: the
	// consumer never trims the events a restore to it would replay.
	last := closed[len(closed)-1]
	b, _ := tc.cursors.get(context.Background(), cluster_model.CursorBackup)
	assert.EqualValues(t, last.Index, b.Position, "the backup cursor is the last checkpoint's barrier")

	pushesSeen, checked := 0, 0
	for _, rec := range closed {
		// The refs at the barrier, from the artifact the export serves; the
		// leader keeps the newest ones only.
		b, err := os.ReadFile(filepath.Join(coord.dir, rec.Name(), "refs.snapshot"))
		if os.IsNotExist(err) {
			continue
		}
		require.NoError(t, err)
		checked++
		at := reftable.New()
		require.NoError(t, at.Restore(bytes.NewReader(b), consensus.SnapshotMeta{}))
		pos, _ := at.Applied()
		assert.Equal(t, rec.Index, pos.Index, "the capture is the table at the barrier")
		assert.Equal(t, rec.Root, at.Root().String())

		refs := map[string]string{}
		for _, repo := range at.Repos() {
			rs, _ := at.Refs(repo)
			for name, v := range rs {
				if v.Live() && v.Symbolic == "" {
					refs[repo+" "+name] = v.SHA
				}
			}
		}
		sql := pg.stateAt(parseFakeLSN(t, rec.LSN))
		assert.Equal(t, refs, sql, "checkpoint %s: the refs at the barrier and the SQL at %s differ", rec.Name(), rec.LSN)
		assert.Contains(t, pg.marks, rec.RestorePoint, "a restore point names the checkpoint")
		pushesSeen += len(refs)
	}
	assert.GreaterOrEqual(t, checked, 4)
	assert.Positive(t, pushesSeen, "the checkpoints saw pushes")
	// Checkpoints closed while pushes ran: the cut was not taken at rest.
	assert.Less(t, closed[0].Index, closed[len(closed)-1].Index)
}

// Pushes on the leader queue behind the fence while PostgreSQL's position
// is read, and none is lost: they commit once it is released.
func TestCheckpointFenceHoldsTheLeadersWriters(t *testing.T) {
	tc := newTestCluster(t, "node-a", "node-b", "node-c")
	pg := newFakePG()
	coord := withCheckpoints(t, tc, pg)
	leader := tc.nodes[0]
	work := t.TempDir()
	run(t, work, "init", "-q")
	sha := commit(t, work, "one")

	leader.r.fence.Lock()
	done := make(chan *clusterhook.Response, 1)
	go func() {
		done <- tc.pushVia(leader, work, "alice/fence.git", "refs/heads/main", strings.Repeat("0", 40), sha)
	}()
	select {
	case <-done:
		t.Fatal("a push on the leader went through the fence")
	case <-time.After(300 * time.Millisecond):
	}
	leader.r.fence.Unlock()
	resp := <-done
	require.True(t, resp.OK, resp.Message)

	_, err := coord.take(context.Background())
	require.NoError(t, err)
	assert.Len(t, leader.r.table.Checkpoints(), 1)
}

// A position read in one term is refused in another: the leader that held
// the fence lost it, and the cut is not provable.
func TestCheckpointOnlyOnTheLeaderTerm(t *testing.T) {
	tc := newTestCluster(t, "node-a", "node-b", "node-c")
	pg := newFakePG()
	coord := withCheckpoints(t, tc, pg)
	coord.wal = termBump{fakePG: pg, bump: func() { tc.mem.SetLeader(tc.nodes[0].node) }}
	_, err := coord.take(context.Background())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "term")
	assert.Empty(t, tc.nodes[0].r.table.Checkpoints())
}

type termBump struct {
	*fakePG
	bump func()
}

func (b termBump) Mark(ctx context.Context, name string) (walMark, error) {
	b.bump()
	return b.fakePG.Mark(ctx, name)
}

// archive_command is idempotent: the same bytes again succeed, other bytes
// under an archived name are refused, and history files are archived too.
func TestArchiveWALIsIdempotent(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "archive")
	src := filepath.Join(t.TempDir(), "seg")
	require.NoError(t, os.WriteFile(src, []byte("segment one"), 0o600))
	seg := "000000010000000000000003"
	require.NoError(t, ArchiveWAL(src, seg, dir))
	require.NoError(t, ArchiveWAL(src, seg, dir), "a retry of the same file succeeds")

	require.NoError(t, os.WriteFile(src, []byte("other bytes"), 0o600))
	require.ErrorContains(t, ArchiveWAL(src, seg, dir), "other bytes")
	b, err := os.ReadFile(filepath.Join(dir, seg))
	require.NoError(t, err)
	assert.Equal(t, "segment one", string(b), "never overwritten")

	require.NoError(t, os.WriteFile(src, []byte("1\t0/3000000\tno recovery target specified\n"), 0o600))
	require.NoError(t, ArchiveWAL(src, "00000002.history", dir))
	require.NoError(t, ArchiveWAL(src, "000000010000000000000002.00000028.backup", dir))
	require.Error(t, ArchiveWAL(src, "../escape", dir))
	require.Error(t, ArchiveWAL(src, "000000010000000000000002.nope", dir))
}

// archive_command killed between the link and the directory's sync leaves
// the name in the page cache only; PostgreSQL retries, and the retry that
// finds the same bytes must sync the directory before it answers success,
// or PostgreSQL recycles a segment a power loss then takes from the archive.
func TestArchiveWALRetrySyncsTheDirectory(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "archive")
	src := filepath.Join(t.TempDir(), "seg")
	require.NoError(t, os.WriteFile(src, []byte("segment one"), 0o600))
	seg := "000000010000000000000003"
	require.NoError(t, ArchiveWAL(src, seg, dir))

	var synced []string
	defer func(prev func(string) error) { syncDirFn = prev }(syncDirFn)
	syncDirFn = func(d string) error { synced = append(synced, d); return nil }
	require.NoError(t, ArchiveWAL(src, seg, dir), "a retry of the same file succeeds")
	assert.Contains(t, synced, dir, "the retry syncs the archive directory")
}

// After a promotion PostgreSQL archives the old timeline's last segment as
// <segment>.partial, and retries a refused file forever without archiving
// anything after it: a refusal stopped the archive at the first failover,
// and an observer never covered a checkpoint again. The file is archived;
// the export does not list it.
func TestArchiveWALTakesThePartialSegmentOfAPromotion(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "archive")
	src := filepath.Join(t.TempDir(), "seg")
	require.NoError(t, os.WriteFile(src, []byte("the old timeline's last segment"), 0o600))
	name := "00000001000000000000000C.partial"
	require.NoError(t, ArchiveWAL(src, name, dir))
	require.NoError(t, ArchiveWAL(src, name, dir), "a retry of the same file succeeds")
	_, err := os.Stat(filepath.Join(dir, name))
	require.NoError(t, err)

	files, err := exportSide{archiveDir: dir}.walList("")
	require.NoError(t, err)
	assert.Empty(t, files, "the export serves no .partial file")
}

// A barrier proposed again after an unknown outcome applies twice under one
// nonce: the table kept is the first one's, at another index than the
// position names. The checkpoint is refused rather than recorded with an
// artifact of another cut.
func TestCheckpointRefusesATableCapturedAtAnotherBarrier(t *testing.T) {
	c := &coordinator{}
	got := c.expect("n")
	c.capture("n", 5, func() ([]byte, error) { return []byte("at 5"), nil })
	c.capture("n", 7, func() ([]byte, error) { return []byte("at 7"), nil })
	_, err := tableAtBarrier(context.Background(), got, 7)
	require.ErrorContains(t, err, "proposed twice")

	got = c.expect("m")
	c.capture("m", 9, func() ([]byte, error) { return []byte("at 9"), nil })
	b, err := tableAtBarrier(context.Background(), got, 9)
	require.NoError(t, err)
	assert.Equal(t, "at 9", string(b))
}

// The stored files are listed outside the fence, and a walk longer than
// readToCommitBound does not discard the checkpoint: the bound counts from
// the end of the walk.
func TestCheckpointListsTheFilesOutsideTheFence(t *testing.T) {
	tc := newTestCluster(t, "node-a", "node-b", "node-c")
	pg := newFakePG()
	coord := withCheckpoints(t, tc, pg)
	leader := tc.nodes[0].r
	var mu sync.Mutex
	skew := time.Duration(0)
	coord.now = func() time.Time {
		mu.Lock()
		defer mu.Unlock()
		return time.Now().Add(skew)
	}
	fenceFree := false
	coord.files = func() (*storedListing, error) {
		if leader.fence.TryLock() {
			fenceFree = true
			leader.fence.Unlock()
		}
		mu.Lock()
		skew += 2 * readToCommitBound // a large storage on a slow disk
		mu.Unlock()
		return nil, nil
	}
	_, err := coord.take(context.Background())
	require.NoError(t, err)
	assert.True(t, fenceFree, "the walk ran while the leader's writers were fenced")
}

// listAndDigest lists the stored files and reads their digests, as a
// checkpoint does on each side of its cut.
func listAndDigest(roots map[string]*setting.Storage) ([]clusterhook.FileEntry, error) {
	l, err := listStoredFiles(roots)
	if err != nil {
		return nil, err
	}
	return l.digest()
}

// An upload in flight sits in the storage's temporary directory; it is not
// a stored file, and listing it fails the observer's ingest.
func TestStoredFilesSkipTheTemporaryDirectory(t *testing.T) {
	root := t.TempDir()
	other := t.TempDir()
	for _, f := range []string{"ab/cd/obj", "tmp/upload-123", "x/tmp/kept"} {
		require.NoError(t, os.MkdirAll(filepath.Join(root, filepath.Dir(f)), 0o755))
		require.NoError(t, os.WriteFile(filepath.Join(root, f), []byte("1234567"), 0o644))
	}
	require.NoError(t, os.MkdirAll(filepath.Join(other, "elsewhere"), 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(other, "kept"), []byte("1"), 0o644))
	require.NoError(t, os.WriteFile(filepath.Join(other, "elsewhere", "upload"), []byte("1"), 0o644))
	files, err := listAndDigest(map[string]*setting.Storage{
		StorageLFS:         {Type: setting.LocalStorageType, Path: root},
		StorageAttachments: {Type: setting.LocalStorageType, Path: other, TemporaryPath: filepath.Join(other, "elsewhere")},
	})
	require.NoError(t, err)
	var got []string
	for _, f := range files {
		got = append(got, f.Storage+":"+f.Path)
	}
	assert.Equal(t, []string{"attachments:kept", "lfs:ab/cd/obj", "lfs:x/tmp/kept"}, got)
	// An attachment's path does not name it: its digest is listed, and
	// read again once it changed (#106).
	assert.Equal(t, sha256Hex([]byte("1")), files[0].SHA256)
	assert.Empty(t, files[1].SHA256)
	require.NoError(t, os.WriteFile(filepath.Join(other, "kept"), []byte("2"), 0o644))
	require.NoError(t, os.Chtimes(filepath.Join(other, "kept"), time.Now(), time.Now().Add(time.Hour)))
	files, err = listAndDigest(map[string]*setting.Storage{
		StorageAttachments: {Type: setting.LocalStorageType, Path: other, TemporaryPath: filepath.Join(other, "elsewhere")},
	})
	require.NoError(t, err)
	assert.Equal(t, sha256Hex([]byte("2")), files[0].SHA256)
}

// A lineage whose identity sorts before the old one's keeps its newest
// artifacts: the old lineage's go first, then the current one's oldest by
// index (#59).
func TestHuntR2ProvePruneLineageKeepsCurrent(t *testing.T) {
	dir := t.TempDir()
	c := &coordinator{dir: dir}
	write := func(name string) {
		require.NoError(t, os.MkdirAll(filepath.Join(dir, name), 0o700))
		require.NoError(t, os.WriteFile(filepath.Join(dir, name, "refs.snapshot"), []byte(name), 0o600))
	}
	old := consensus.LogIdentity{Cluster: consensus.ClusterID{0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff}, Epoch: 1}.String()
	cur := consensus.LogIdentity{Cluster: consensus.ClusterID{15: 0x01}, Epoch: 1}.String()
	for i := uint64(1); i <= checkpointArtifactsKept; i++ {
		write(reftable.CheckpointName(old, i*100))
		c.prune(old)
	}
	x := exportSide{ckptDir: dir}
	for round := uint64(1); round <= checkpointArtifactsKept+3; round++ {
		name := reftable.CheckpointName(cur, round*10)
		write(name)
		c.prune(cur)
		_, err := x.artifact(name, "refs.snapshot")
		require.NoError(t, err, "round %d: the current lineage's newest checkpoint stays exportable", round)
	}
	entries, err := os.ReadDir(dir)
	require.NoError(t, err)
	var names []string
	for _, e := range entries {
		names = append(names, e.Name())
	}
	require.Len(t, names, checkpointArtifactsKept)
	for i, n := range names {
		assert.Equal(t, reftable.CheckpointName(cur, uint64(i+4)*10), n)
	}
}

// Indexes compare as numbers, not as names (#59).
func TestPruneKeepsTheHighestIndexes(t *testing.T) {
	dir := t.TempDir()
	c := &coordinator{dir: dir}
	cur := consensus.LogIdentity{Cluster: consensus.ClusterID{0: 0xab}, Epoch: 2}.String()
	for _, i := range []uint64{9, 10, 99, 100, 1000, 5, 7, 8, 11, 12} {
		require.NoError(t, os.MkdirAll(filepath.Join(dir, reftable.CheckpointName(cur, i)), 0o700))
	}
	c.prune(cur)
	for _, i := range []uint64{5, 7} {
		_, err := os.Stat(filepath.Join(dir, reftable.CheckpointName(cur, i)))
		assert.True(t, os.IsNotExist(err), "index %d goes", i)
	}
	_, err := os.Stat(filepath.Join(dir, reftable.CheckpointName(cur, 1000)))
	assert.NoError(t, err)
}
