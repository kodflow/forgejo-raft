// Copyright 2026 The Forgejo Authors. All rights reserved.
// SPDX-License-Identifier: GPL-3.0-or-later

package observer

import (
	"archive/tar"
	"bufio"
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"testing"

	"forgejo.org/modules/clusterhook"
	"forgejo.org/modules/json"
	"forgejo.org/services/cluster/consensus"
	"forgejo.org/services/cluster/reftable"

	"github.com/stretchr/testify/require"
	"golang.org/x/crypto/nacl/box"
)

// fakeCluster stands for three members and their PostgreSQL, behind the
// export's contract: one ref table applied from one log, real git
// repositories, and a PostgreSQL whose WAL is a list of records in
// segments, with timelines and their histories. It serves exactly the ops
// of services/cluster/export.go, with the same leader rules.
type fakeCluster struct {
	t  *testing.T
	mu sync.Mutex

	cluster consensus.ClusterID
	epoch   uint64
	term    uint64
	leader  string
	slots   []string
	members []string
	table   *reftable.Table
	log     []consensus.Entry
	// compacted is the last index the log no longer returns.
	compacted uint64
	// stale are deposed leaders still answering with what they knew.
	stale map[string]clusterhook.ExportView

	repos     string
	work      string
	artifacts map[string]map[string][]byte
	files     map[string][]byte

	pg *fakePG

	recipient *[32]byte
	// calls are the ops each slot was asked, in order.
	calls []string
	// down are the members that do not answer.
	down map[string]bool
	// tamper may rewrite an answer before it is sent.
	tamper func(slot string, req *clusterhook.ExportRequest, data []byte) []byte
}

const fakeSegSize = 16 << 20

func newFakeCluster(t *testing.T) *fakeCluster {
	c := &fakeCluster{
		t: t, cluster: consensus.ClusterID{0xc1, 0x05}, epoch: 1, term: 1, leader: "node-a",
		slots: []string{"node-a", "node-b", "node-c"}, members: []string{"node-a", "node-b", "node-c"},
		table: reftable.New(), stale: map[string]clusterhook.ExportView{}, down: map[string]bool{},
		repos: t.TempDir(), work: t.TempDir(), artifacts: map[string]map[string][]byte{}, files: map[string][]byte{},
		pg: newFakePG(),
	}
	c.git(c.work, "init", "-q")
	return c
}

func (c *fakeCluster) lineage() consensus.LogIdentity {
	return consensus.LogIdentity{Cluster: c.cluster, Epoch: c.epoch}
}

func (c *fakeCluster) git(dir string, args ...string) string {
	c.t.Helper()
	cmd := exec.Command("git", append([]string{"-c", "core.hooksPath=/dev/null", "-c", "user.name=t", "-c", "user.email=t@t", "-c", "init.defaultBranch=main"}, args...)...)
	cmd.Dir = dir
	cmd.Env = append(os.Environ(), "GIT_CONFIG_NOSYSTEM=1", "HOME="+c.work)
	out, err := cmd.CombinedOutput()
	require.NoError(c.t, err, "git %v: %s", args, out)
	return strings.TrimSpace(string(out))
}

// append commits an entry to the shared log and applies it.
func (c *fakeCluster) append(kind reftable.Kind, body any) reftable.Result {
	c.t.Helper()
	data, err := reftable.Encode(kind, body)
	require.NoError(c.t, err)
	idx := uint64(len(c.log)) + 1
	e := consensus.Entry{
		Term: c.term, Index: idx, Kind: consensus.EntryCommand, Version: 1, Data: data,
		Token: consensus.Token{Cluster: c.cluster, Epoch: c.epoch, Term: c.term, Index: idx},
	}
	c.log = append(c.log, e)
	return c.table.Apply(e).(reftable.Result)
}

// commit makes a commit in the scratch repository and returns its id.
func (c *fakeCluster) commit(msg string) string {
	require.NoError(c.t, os.WriteFile(filepath.Join(c.work, "f.txt"), []byte(msg), 0o644))
	c.git(c.work, "add", ".")
	c.git(c.work, "commit", "-q", "--allow-empty", "-m", msg)
	return c.git(c.work, "rev-parse", "HEAD")
}

// push writes sha to a ref of repo: objects into the member's repository,
// the ref through the log, and the branch row through the consumer.
func (c *fakeCluster) push(repo, ref, sha string) {
	c.t.Helper()
	c.mu.Lock()
	defer c.mu.Unlock()
	dir := filepath.Join(c.repos, filepath.FromSlash(repo))
	if _, err := os.Stat(dir); os.IsNotExist(err) {
		c.git(c.repos, "init", "-q", "--bare", dir)
	}
	u := reftable.RefUpdate{Ref: ref}
	if v, ok := c.table.Get(repo, ref); ok {
		u.ExpectRev = v.Rev
	}
	if sha == "" {
		u.Delete = true
	} else {
		c.git(c.work, "push", "-q", "-f", dir, sha+":"+ref)
		u.New = sha
	}
	res := c.append(reftable.KindRefTxn, &reftable.RefTxn{Repo: repo, Format: "sha1", Updates: []reftable.RefUpdate{u}})
	require.True(c.t, res.OK, res.Reason)
	if _, ok := c.table.Get(repo, "HEAD"); !ok {
		require.True(c.t, c.append(reftable.KindRefTxn, &reftable.RefTxn{Repo: repo, Format: "sha1", Updates: []reftable.RefUpdate{{Ref: "HEAD", Symbolic: "refs/heads/main"}}}).OK)
	}
	c.pg.write("branch "+repo+" "+ref, sha)
}

// storeFile adds an LFS object, named by its digest.
func (c *fakeCluster) storeFile(content string) string {
	sum := sha256.Sum256([]byte(content))
	oid := hex.EncodeToString(sum[:])
	path := oid[:2] + "/" + oid[2:4] + "/" + oid[4:]
	c.mu.Lock()
	c.files["lfs/"+path] = []byte(content)
	c.mu.Unlock()
	c.pg.write("lfs "+oid, "present")
	return path
}

// checkpoint takes one checkpoint, as the coordinator does: the barrier,
// the table at it, PostgreSQL's position and restore point, the WAL
// switch, the artifacts, the position entry.
func (c *fakeCluster) checkpoint() reftable.CheckpointRecord {
	c.t.Helper()
	c.mu.Lock()
	defer c.mu.Unlock()
	nonce := strings.Repeat(fmt.Sprintf("%x", len(c.log)%16), 32)
	var snap []byte
	c.table.OnCheckpoint(func(n string, _ uint64, encode func() ([]byte, error)) {
		if n == nonce {
			snap, _ = encode()
		}
	})
	require.True(c.t, c.append(reftable.KindCheckpointBarrier, &reftable.CheckpointBarrier{Nonce: nonce, Time: 1_700_000_000_000_000_000 + int64(len(c.log))}).OK)
	barrier := uint64(len(c.log))
	name := reftable.CheckpointName(c.lineage().String(), barrier)
	lsn, tli := c.pg.mark(name)
	c.pg.switchWAL()
	var files []clusterhook.FileEntry
	for k, v := range c.files {
		storage, path, _ := strings.Cut(k, "/")
		files = append(files, clusterhook.FileEntry{Storage: storage, Path: path, Size: int64(len(v))})
	}
	sort.Slice(files, func(i, j int) bool { return files[i].Path < files[j].Path })
	manifest, _ := json.Marshal(clusterhook.FilesManifest{Checkpoint: name, Files: files})
	c.artifacts[name] = map[string][]byte{"refs": snap, "files": manifest}
	pos := &reftable.CheckpointPosition{
		Nonce: nonce, Index: barrier, LSN: lsn.String(), Timeline: tli, SystemID: c.pg.sysid,
		RestorePoint: name, WALSegmentSize: fakeSegSize, RefsSHA256: digest(snap), FilesSHA256: digest(manifest),
	}
	res := c.append(reftable.KindCheckpointPosition, pos)
	require.True(c.t, res.OK, res.Reason)
	cps := c.table.Checkpoints()
	return cps[len(cps)-1]
}

func digest(b []byte) string {
	s := sha256.Sum256(b)
	return hex.EncodeToString(s[:])
}

// newLeader moves leadership to slot in a new term; the old leader keeps
// answering with its old view when deposed is set.
func (c *fakeCluster) newLeader(slot string, deposed bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if deposed {
		c.stale[c.leader] = c.viewLocked(c.leader, 0)
	}
	c.leader = slot
	c.term++
	delete(c.stale, slot)
}

func (c *fakeCluster) viewLocked(slot string, from uint64) clusterhook.ExportView {
	v := clusterhook.ExportView{
		Lineage: c.lineage().String(), Term: c.term, Commit: uint64(len(c.log)), Leader: slot,
		Members: append([]string(nil), c.members...), LogSupported: true, Checkpoints: []clusterhook.ExportCheckpoint{},
	}
	for _, cp := range c.table.Checkpoints() {
		if cp.Index > from && cp.Identity == v.Lineage {
			v.Checkpoints = append(v.Checkpoints, clusterhook.ExportCheckpoint{
				Name: cp.Name(), Lineage: cp.Identity, Index: cp.Index, Term: cp.Term, ClosedAt: cp.ClosedAt, Root: cp.Root,
				Time: cp.Time, LSN: cp.LSN, Timeline: cp.Timeline, SystemID: cp.SystemID, RestorePoint: cp.RestorePoint,
				WALSegmentSize: cp.WALSegmentSize, RefsSHA256: cp.RefsSHA256, FilesSHA256: cp.FilesSHA256,
			})
		}
	}
	return v
}

// endpoints returns the observer's sources, one per slot.
func (c *fakeCluster) endpoints() map[string]Source {
	out := map[string]Source{}
	for _, s := range c.slots {
		out[s] = fakeSource{c: c, slot: s}
	}
	return out
}

type fakeSource struct {
	c    *fakeCluster
	slot string
}

func (s fakeSource) Export(ctx context.Context, req *clusterhook.ExportRequest) (*clusterhook.ExportAnswer, io.ReadCloser, error) {
	c := s.c
	c.mu.Lock()
	defer c.mu.Unlock()
	c.calls = append(c.calls, req.Op)
	if c.down[s.slot] {
		return nil, nil, fmt.Errorf("%s does not answer", s.slot)
	}
	data, stream, err := c.serve(s.slot, req)
	if err != nil {
		a := &clusterhook.ExportAnswer{Message: err.Error()}
		if e, ok := err.(*clusterhook.ExportError); ok {
			a.Code, a.Message, a.Leader, a.Term = e.Code, e.Message, e.Leader, e.Term
		}
		return a, nil, nil
	}
	if c.tamper != nil {
		if stream != nil {
			stream = c.tamper(s.slot, req, stream)
		} else {
			data = c.tamper(s.slot, req, data)
		}
	}
	if stream == nil {
		return &clusterhook.ExportAnswer{OK: true, Data: data}, nil, nil
	}
	return &clusterhook.ExportAnswer{OK: true, Stream: true}, io.NopCloser(bytes.NewReader(stream)), nil
}

func notLeader(leader string, term uint64) error {
	return &clusterhook.ExportError{Code: clusterhook.CodeNotLeader, Message: "not the leader", Leader: leader, Term: term}
}

func (c *fakeCluster) serve(slot string, req *clusterhook.ExportRequest) ([]byte, []byte, error) {
	leaderOnly := func() error {
		if slot != c.leader {
			return notLeader(c.leader, c.term)
		}
		return nil
	}
	switch req.Op {
	case clusterhook.OpExportCheckpoints:
		if v, ok := c.stale[slot]; ok {
			b, _ := json.Marshal(v)
			return b, nil, nil
		}
		if err := leaderOnly(); err != nil {
			return nil, nil, err
		}
		v := c.viewLocked(slot, req.FromIndex)
		b, err := json.Marshal(v)
		return b, nil, err
	case clusterhook.OpExportLog:
		if err := leaderOnly(); err != nil {
			return nil, nil, err
		}
		if req.Lineage != c.lineage().String() {
			return nil, nil, &clusterhook.ExportError{Code: clusterhook.CodeLineage}
		}
		commit := uint64(len(c.log))
		if req.FromIndex > commit {
			return nil, nil, &clusterhook.ExportError{Code: clusterhook.CodeDiverged}
		}
		if req.FromIndex < c.compacted || req.FromIndex == 0 && c.compacted > 0 {
			return nil, nil, &clusterhook.ExportError{Code: clusterhook.CodeCompacted}
		}
		if req.FromIndex > 0 && c.log[req.FromIndex-1].Term != req.FromTerm {
			return nil, nil, &clusterhook.ExportError{Code: clusterhook.CodeDiverged}
		}
		var buf bytes.Buffer
		for _, e := range c.log[req.FromIndex:] {
			b, _ := json.Marshal(clusterhook.ExportEntry{Index: e.Index, Term: e.Term, Data: e.Data})
			buf.Write(append(b, '\n'))
		}
		return nil, buf.Bytes(), nil
	case clusterhook.OpExportRefs, clusterhook.OpExportFiles:
		if err := leaderOnly(); err != nil {
			return nil, nil, err
		}
		a, ok := c.artifacts[req.Checkpoint]
		if !ok {
			return nil, nil, &clusterhook.ExportError{Code: clusterhook.CodeUnavailable}
		}
		if req.Op == clusterhook.OpExportRefs {
			return nil, a["refs"], nil
		}
		return nil, a["files"], nil
	case clusterhook.OpExportPacks:
		dir := filepath.Join(c.repos, filepath.FromSlash(req.Repo))
		in := strings.Join(req.Wants, "\n") + "\n"
		for _, h := range req.Haves {
			in += "^" + h + "\n"
		}
		cmd := exec.Command("git", "pack-objects", "--revs", "--stdout", "--quiet")
		cmd.Dir, cmd.Stdin = dir, strings.NewReader(in)
		out, err := cmd.Output()
		return nil, out, err
	case clusterhook.OpExportPGBase:
		return nil, c.pg.baseBackup(), nil
	case clusterhook.OpExportWALList:
		var out []clusterhook.WALFile
		for name, b := range c.pg.archive {
			if name > req.Name || strings.HasSuffix(name, ".history") {
				out = append(out, clusterhook.WALFile{Name: name, Size: int64(len(b))})
			}
		}
		sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
		b, _ := json.Marshal(out)
		return b, nil, nil
	case clusterhook.OpExportWAL:
		b, ok := c.pg.archive[req.Name]
		if !ok {
			return nil, nil, &clusterhook.ExportError{Code: clusterhook.CodeUnavailable}
		}
		return nil, b, nil
	case clusterhook.OpExportFile:
		b, ok := c.files[req.Storage+"/"+req.Name]
		if !ok {
			return nil, nil, &clusterhook.ExportError{Code: clusterhook.CodeUnavailable}
		}
		return nil, b, nil
	case clusterhook.OpExportSecrets:
		sealed, err := box.SealAnonymous(nil, []byte(`{"secret_key":"fake-secret"}`), c.recipient, rand.Reader)
		return nil, sealed, err
	}
	return nil, nil, fmt.Errorf("unknown op %s", req.Op)
}

// ---- PostgreSQL ----

// fakePG writes records at increasing WAL positions in segments of
// fakeSegSize; a switch archives the segment. A failover starts a new
// timeline at the current position, with its history.
type fakePG struct {
	sysid   string
	tli     uint32
	insert  LSN
	records []walRecord
	archive map[string][]byte
	history []Switch
	// extra are files the base backup carries besides the data.
	extra map[string]string
}

type walRecord struct {
	LSN uint64 `json:"lsn"`
	Key string `json:"key"`
	Val string `json:"val"`
}

func newFakePG() *fakePG {
	return &fakePG{sysid: "7425000000000000042", tli: 1, insert: fakeSegSize, archive: map[string][]byte{}}
}

func (p *fakePG) write(key, val string) {
	p.records = append(p.records, walRecord{LSN: uint64(p.insert), Key: key, Val: val})
	p.insert += 64
}

// delete is a SQL DELETE: a record of the key with no value.
func (p *fakePG) delete(key string) { p.write(key, "") }

func (p *fakePG) mark(name string) (LSN, uint32) {
	at := p.insert
	p.write("restore-point", name)
	return at, p.tli
}

// switchWAL archives the current segment and moves to the next one.
func (p *fakePG) switchWAL() {
	seg := uint64(p.insert) / fakeSegSize
	start := LSN(seg * fakeSegSize)
	var buf bytes.Buffer
	for _, r := range p.records {
		if LSN(r.LSN) >= start && LSN(r.LSN) < start+fakeSegSize {
			b, _ := json.Marshal(r)
			buf.Write(append(b, '\n'))
		}
	}
	p.archive[SegmentName(p.tli, start, fakeSegSize)] = buf.Bytes()
	p.insert = LSN((seg + 1) * fakeSegSize)
}

// failover promotes a standby: a new timeline from the current position.
func (p *fakePG) failover() {
	p.history = append(p.history, Switch{Timeline: p.tli, At: p.insert})
	p.tli++
	var buf strings.Builder
	for _, s := range p.history {
		fmt.Fprintf(&buf, "%d\t%s\tno recovery target specified\n", s.Timeline, s.At)
	}
	p.archive[HistoryName(p.tli)] = []byte(buf.String())
}

// baseBackup is a tar as pg_basebackup -Ft -D - writes one: the label, the
// control file, and the rows as of its start.
func (p *fakePG) baseBackup() []byte {
	p.switchWAL()
	start := p.insert
	p.write("checkpoint", "base")
	rows, _ := json.Marshal(replayRecords(nil, p.records, 0, uint64(start)))
	label := fmt.Sprintf("START WAL LOCATION: %s (file %s)\nCHECKPOINT LOCATION: %s\nSTART TIMELINE: %d\n", start, SegmentName(p.tli, start, fakeSegSize), start, p.tli)
	var control [16]byte
	var id uint64
	fmt.Sscan(p.sysid, &id)
	binary.LittleEndian.PutUint64(control[:], id)
	var buf bytes.Buffer
	tw := tar.NewWriter(&buf)
	entries := []struct {
		name string
		body []byte
	}{{"backup_label", []byte(label)}, {"global/pg_control", control[:]}, {"base/rows.json", rows}}
	for name, body := range p.extra {
		entries = append(entries, struct {
			name string
			body []byte
		}{name, []byte(body)})
	}
	for _, f := range entries {
		_ = tw.WriteHeader(&tar.Header{Name: f.name, Mode: 0o600, Size: int64(len(f.body)), Typeflag: tar.TypeReg})
		_, _ = tw.Write(f.body)
	}
	_ = tw.Close()
	return buf.Bytes()
}

func replayRecords(rows map[string]string, recs []walRecord, from, to uint64) map[string]string {
	if rows == nil {
		rows = map[string]string{}
	}
	for _, r := range recs {
		if r.LSN < from || r.LSN >= to || r.Key == "restore-point" || r.Key == "checkpoint" {
			continue
		}
		if r.Val == "" {
			delete(rows, r.Key)
		} else {
			rows[r.Key] = r.Val
		}
	}
	return rows
}

// replay recovers PostgreSQL from what the observer holds for a
// checkpoint, as recovery_target_lsn with inclusive off does: the base's
// rows, then every record of the coverage's segments before the target.
func replay(t *testing.T, o *Observer, m *Manifest) map[string]string {
	t.Helper()
	f, err := os.Open(o.store.path("pg", "base", m.PG.Base.Label, "base.tar"))
	require.NoError(t, err)
	defer f.Close()
	rows := map[string]string{}
	tr := tar.NewReader(f)
	for {
		h, err := tr.Next()
		if err == io.EOF {
			break
		}
		require.NoError(t, err)
		if h.Name == "base/rows.json" {
			require.NoError(t, json.NewDecoder(tr).Decode(&rows))
		}
	}
	start, _ := ParseLSN(m.PG.Base.Start)
	target, _ := ParseLSN(m.Checkpoint.LSN)
	for _, seg := range m.PG.Segments {
		r, err := o.store.OpenWAL(seg)
		require.NoError(t, err, seg)
		var recs []walRecord
		sc := bufio.NewScanner(r)
		for sc.Scan() {
			var rec walRecord
			require.NoError(t, json.Unmarshal(sc.Bytes(), &rec))
			recs = append(recs, rec)
		}
		r.Close()
		rows = replayRecords(rows, recs, uint64(start), uint64(target))
	}
	return rows
}

// ---- the observer ----

type hookLog struct {
	mu    sync.Mutex
	calls []string
	fail  bool
}

func (h *hookLog) hook(_ context.Context, checkpoint string) (string, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.calls = append(h.calls, checkpoint)
	if h.fail {
		return "", fmt.Errorf("snapshot refused")
	}
	return fmt.Sprintf("snap-%d", len(h.calls)), nil
}

func newTestObserver(t *testing.T, c *fakeCluster, h *hookLog) *Observer {
	t.Helper()
	pub, priv, err := box.GenerateKey(rand.Reader)
	require.NoError(t, err)
	c.recipient = pub
	o, err := New(Config{
		Name: "nas-test", Dir: t.TempDir(), Endpoints: c.endpoints(), SecretsKey: priv, Hook: h.hook,
		StagingQuota: 1 << 30,
	})
	require.NoError(t, err)
	return o
}

// seed gives the cluster a repository, a stored file, a base backup the
// observer takes on its first step, and a first checkpoint after it.
func seed(t *testing.T, c *fakeCluster, o *Observer) reftable.CheckpointRecord {
	t.Helper()
	c.push("alice/demo.git", "refs/heads/main", c.commit("one"))
	c.storeFile("an lfs object")
	c.checkpoint()
	// The first checkpoint precedes any base the observer holds: it waits.
	require.ErrorIs(t, o.Step(context.Background()), ErrNoCoverage)
	require.Len(t, o.State().Bases, 1)
	c.push("alice/demo.git", "refs/heads/dev", c.commit("two"))
	return c.checkpoint()
}
