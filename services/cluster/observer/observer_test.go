// Copyright 2026 The Forgejo Authors. All rights reserved.
// SPDX-License-Identifier: GPL-3.0-or-later

package observer

import (
	"context"
	"crypto/rand"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"testing"
	"time"

	"forgejo.org/modules/clusterhook"
	"forgejo.org/modules/json"
	"forgejo.org/services/cluster/consensus"
	"forgejo.org/services/cluster/reftable"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/crypto/nacl/box"
)

// An observer is never a member: it does not link the consensus node nor
// the replicator, it opens no listener, the only thing it can ask a member
// is a read-only export call, and a membership that names it stops it.
func TestObserverNeverMember(t *testing.T) {
	// 1. What the package links.
	out, err := exec.Command("go", "list", "-deps", ".").Output()
	require.NoError(t, err)
	for dep := range strings.FieldsSeq(string(out)) {
		switch {
		case dep == "forgejo.org/services/cluster",
			dep == "forgejo.org/services/cluster/consensus/kitsunium",
			dep == "forgejo.org/services/cluster/consensus/memraft",
			strings.HasPrefix(dep, "github.com/kitsunium/sdk"):
			t.Errorf("the observer links %s", dep)
		}
	}

	// 2. What its code calls: no listener of any kind, from any of the
	// packages that make one.
	files, err := filepath.Glob("*.go")
	require.NoError(t, err)
	fset := token.NewFileSet()
	for _, name := range files {
		if strings.HasSuffix(name, "_test.go") {
			continue
		}
		f, err := parser.ParseFile(fset, name, nil, 0)
		require.NoError(t, err)
		ast.Inspect(f, func(n ast.Node) bool {
			if sel, ok := n.(*ast.SelectorExpr); ok {
				pkg, ok := sel.X.(*ast.Ident)
				if !ok || !slices.Contains([]string{"net", "http", "ssh", "tls", "unix", "syscall"}, pkg.Name) {
					return true
				}
				switch sel.Sel.Name {
				case "Listen", "ListenTCP", "ListenUnix", "ListenPacket", "ListenAndServe", "Serve", "NewServerConn":
					t.Errorf("%s: the observer calls %s", fset.Position(sel.Pos()), sel.Sel.Name)
				}
			}
			return true
		})
	}

	// 3. Its only port to a member is one read-only call.
	src := reflect.TypeFor[Source]()
	require.Equal(t, 1, src.NumMethod())
	assert.Equal(t, "Export", src.Method(0).Name)

	// 4. At run time: every call is an export op, and the membership the
	// cluster reports never changes nor names it.
	c := newFakeCluster(t)
	h := &hookLog{}
	o := newTestObserver(t, c, h)
	seed(t, c, o)
	require.NoError(t, o.Step(context.Background()))
	require.NotEmpty(t, c.calls)
	for _, op := range c.calls {
		assert.True(t, clusterhook.IsExportOp(op), op)
	}
	assert.Equal(t, []string{"node-a", "node-b", "node-c"}, c.members)

	c.members = append(c.members, "nas-test")
	c.checkpoint()
	err = o.Step(context.Background())
	require.ErrorIs(t, err, ErrQuarantined)
	assert.Contains(t, err.Error(), "never a member")
}

// The observer follows a leader change: a deposed leader that keeps
// answering with its older term is passed over, a hint is followed only
// to a pinned endpoint, and the log continues across the change.
func TestObserverFollowsLeaderChange(t *testing.T) {
	c := newFakeCluster(t)
	o := newTestObserver(t, c, &hookLog{})
	seed(t, c, o)
	require.NoError(t, o.Step(context.Background()))
	st := o.State()
	require.Len(t, st.Checkpoints, 1)
	assert.Equal(t, "node-a", st.Leader)
	assert.EqualValues(t, 1, st.Term)

	// node-b leads in term 2; node-a, deposed, still answers as it was.
	c.newLeader("node-b", true)
	c.push("alice/demo.git", "refs/heads/main", c.commit("three"))
	cp := c.checkpoint()
	require.NoError(t, o.Step(context.Background()))
	st = o.State()
	require.Len(t, st.Checkpoints, 2)
	assert.Equal(t, cp.Name(), st.Checkpoints[1].Name)
	assert.Equal(t, "node-b", st.Leader)
	assert.EqualValues(t, 2, st.Term)
	assert.EqualValues(t, len(c.log), st.LogIndex, "the log continued across the change")
	m, err := o.store.ReadManifest(cp.Name())
	require.NoError(t, err)
	assert.Equal(t, "node-b", m.Source, "the checkpoint came from the new leader")

	// The deposed leader alone answers: the observer accepts nothing older.
	c.down["node-b"], c.down["node-c"] = true, true
	err = o.Step(context.Background())
	require.ErrorIs(t, err, errStale)
	require.NotErrorIs(t, err, ErrQuarantined, "a deposed leader is passed over, not a fork")
	assert.EqualValues(t, 2, o.State().Term)
	assert.Equal(t, "node-b", o.State().Leader)

	// A hint never takes the observer off its pinned endpoints.
	c.down["node-b"], c.down["node-c"] = false, false
	c.newLeader("node-c", false)
	calls := len(c.calls)
	require.NoError(t, o.Step(context.Background()))
	assert.Equal(t, "node-c", o.State().Leader)
	assert.Greater(t, len(c.calls), calls)
}

// A fork or a rollback is never taken as history: a log that does not hold
// the accepted entry, a commit behind the accepted one, an accepted
// checkpoint described otherwise, a new lineage. Each one quarantines the
// observer, its accepted checkpoints untouched, until a human acts.
func TestObserverRejectsForkOrRollback(t *testing.T) {
	ctx := context.Background()
	accepted := func(t *testing.T) (*fakeCluster, *Observer, State) {
		c := newFakeCluster(t)
		o := newTestObserver(t, c, &hookLog{})
		seed(t, c, o)
		require.NoError(t, o.Step(ctx))
		st := o.State()
		require.Len(t, st.Checkpoints, 1)
		return c, o, st
	}
	untouched := func(t *testing.T, o *Observer, before State) {
		after := o.State()
		assert.Equal(t, before.Checkpoints, after.Checkpoints)
		m, err := o.store.ReadManifest(before.Checkpoints[0].Name)
		require.NoError(t, err)
		assert.Equal(t, before.Checkpoints[0].Root, m.Checkpoint.Root)
		require.ErrorIs(t, o.Step(ctx), ErrQuarantined, "ingestion stays stopped")
	}

	t.Run("fork in the log", func(t *testing.T) {
		c, o, before := accepted(t)
		// Another history holds the accepted index under another term.
		c.log[before.LogIndex-1].Term = 7
		c.push("alice/demo.git", "refs/heads/main", c.commit("other"))
		err := o.Step(ctx)
		require.ErrorIs(t, err, ErrQuarantined)
		assert.Contains(t, err.Error(), "fork")
		untouched(t, o, before)
	})

	t.Run("rollback of the commit", func(t *testing.T) {
		c, o, before := accepted(t)
		c.log = c.log[:len(c.log)-3]
		err := o.Step(ctx)
		require.ErrorIs(t, err, ErrQuarantined)
		assert.Contains(t, err.Error(), "rollback")
		untouched(t, o, before)
	})

	t.Run("an accepted checkpoint rewritten", func(t *testing.T) {
		c, o, before := accepted(t)
		c.tamper = func(_ string, req *clusterhook.ExportRequest, data []byte) []byte {
			if req.Op != clusterhook.OpExportCheckpoints {
				return data
			}
			// The leader's table describes the accepted checkpoint
			// otherwise; the view goes through the export's own filter.
			var v clusterhook.ExportView
			require.NoError(t, json.Unmarshal(data, &v))
			for i := range v.Checkpoints {
				if v.Checkpoints[i].Name == before.Checkpoints[0].Name {
					v.Checkpoints[i].Root = strings.Repeat("f", 64)
				}
			}
			b, _ := json.Marshal(v)
			return b
		}
		err := o.Step(ctx)
		require.ErrorIs(t, err, ErrQuarantined)
		assert.Contains(t, err.Error(), "described otherwise")
		untouched(t, o, before)
	})

	t.Run("a new lineage needs a human", func(t *testing.T) {
		c, o, before := accepted(t)
		c.cluster[15] = 0x99
		err := o.Step(ctx)
		require.ErrorIs(t, err, ErrQuarantined)
		untouched(t, o, before)
		q := o.State().Quarantine
		require.NotNil(t, q)
		require.Error(t, o.Accept("00000000000000000000000000000000/1"), "only the lineage it saw")
		require.NoError(t, o.Accept(q.Lineage))
		c.push("alice/demo.git", "refs/heads/main", c.commit("after recover"))
		c.checkpoint()
		require.NoError(t, o.Step(ctx))
		st := o.State()
		assert.Equal(t, q.Lineage, st.Lineage)
		require.Len(t, st.Checkpoints, 2, "the old lineage's checkpoint stays beside the new one")
	})
}

// A valid malicious append — a force-push and a SQL DELETE — becomes a
// checkpoint like any other, and the earlier PROTECTED one stays whole and
// recoverable: its manifest, its create-only refs, its objects, and a base
// with the WAL up to its position, whose replay still holds the deleted row.
func TestObserverPoisonKeepsOlderCheckpoint(t *testing.T) {
	ctx := context.Background()
	c := newFakeCluster(t)
	h := &hookLog{}
	o := newTestObserver(t, c, h)
	c.pg.write("issue 1", "the row the attacker deletes")
	clean := seed(t, c, o)
	require.NoError(t, o.Step(ctx))
	st := o.State()
	require.Len(t, st.Checkpoints, 1)
	require.Equal(t, StateProtected, st.Checkpoints[0].State)
	cleanMain, _ := c.table.Get("alice/demo.git", "refs/heads/main")
	manifestBefore, err := os.ReadFile(o.store.path("checkpoints", clean.Name(), "manifest.json"))
	require.NoError(t, err)

	// The attacker owns the cluster: a valid force-push, a deleted branch
	// and a SQL DELETE, through the normal paths.
	c.push("alice/demo.git", "refs/heads/main", c.commit("ransom note"))
	c.push("alice/demo.git", "refs/heads/dev", "")
	c.pg.delete("issue 1")
	poisoned := c.checkpoint()
	require.NoError(t, o.Step(ctx))
	st = o.State()
	require.Len(t, st.Checkpoints, 2)
	assert.Equal(t, StateProtected, st.Checkpoints[1].State, "the observer cannot tell; it is a valid history")

	// The clean checkpoint is unchanged and still restores.
	assert.Equal(t, StateProtected, st.Checkpoints[0].State)
	manifestAfter, err := os.ReadFile(o.store.path("checkpoints", clean.Name(), "manifest.json"))
	require.NoError(t, err)
	assert.Equal(t, manifestBefore, manifestAfter)
	m, err := o.store.ReadManifest(clean.Name())
	require.NoError(t, err)
	assert.Equal(t, cleanMain.SHA, m.Repos["alice/demo.git"].Refs["refs/heads/main"])
	refs, err := o.store.git.refs(ctx, o.store.git.dir("alice/demo.git"), clean.Name())
	require.NoError(t, err)
	assert.Equal(t, m.Repos["alice/demo.git"].Refs, refs, "its refs namespace is intact")
	require.NoError(t, o.store.git.closed(ctx, o.store.git.dir("alice/demo.git"), []string{cleanMain.SHA}, nil))

	rows := replay(t, o, m)
	assert.Equal(t, "the row the attacker deletes", rows["issue 1"], "SQL at the clean checkpoint holds the row")
	assert.Equal(t, cleanMain.SHA, rows["branch alice/demo.git refs/heads/main"], "and the refs and SQL agree")
	mp, err := o.store.ReadManifest(poisoned.Name())
	require.NoError(t, err)
	assert.NotContains(t, replay(t, o, mp), "issue 1", "the poisoned checkpoint lacks it")

	// The clean repository materializes from its namespace, checked again.
	target := filepath.Join(t.TempDir(), "demo.git")
	require.NoError(t, o.store.git.Materialize(ctx, "alice/demo.git", clean.Name(), target, "sha1", m.Repos["alice/demo.git"].Symbolic))
	c.git(target, "fsck", "--strict")
	assert.Equal(t, cleanMain.SHA, c.git(target, "rev-parse", "refs/heads/main"))
	assert.Equal(t, "refs/heads/main", c.git(target, "symbolic-ref", "HEAD"))

	// Rewriting the clean checkpoint's refs is refused, by git itself.
	err = o.store.git.createRefs(ctx, o.store.git.dir("alice/demo.git"), clean.Name(), map[string]string{"refs/heads/main": mp.Repos["alice/demo.git"].Refs["refs/heads/main"]})
	require.ErrorIs(t, err, ErrImmutable)
	// And so is publishing it again.
	require.ErrorIs(t, o.store.publish(clean.Name()), ErrImmutable)
	assert.Len(t, h.calls, 2, "one snapshot per checkpoint")
}

// A failing snapshot hook stops ingestion: the checkpoint stays VERIFIED,
// freshness does not advance, and the hook is retried for the same
// checkpoint before anything else is taken.
func TestObserverHookFailureStopsIngestion(t *testing.T) {
	ctx := context.Background()
	c := newFakeCluster(t)
	h := &hookLog{fail: true}
	o := newTestObserver(t, c, h)
	seed(t, c, o)
	require.ErrorIs(t, o.Step(ctx), ErrHookFailing)
	st := o.State()
	require.Len(t, st.Checkpoints, 1)
	assert.Equal(t, StateVerified, st.Checkpoints[0].State)
	assert.Contains(t, o.Status(nil).Alerts, "no protected checkpoint yet")

	c.push("alice/demo.git", "refs/heads/main", c.commit("later"))
	c.checkpoint()
	require.ErrorIs(t, o.Step(ctx), ErrHookFailing)
	assert.Len(t, o.State().Checkpoints, 1, "nothing ingested while the hook fails")

	h.fail = false
	require.NoError(t, o.Step(ctx))
	st = o.State()
	require.Len(t, st.Checkpoints, 2)
	assert.Equal(t, StateProtected, st.Checkpoints[0].State)
	assert.Equal(t, st.Checkpoints[0].Name, h.calls[len(h.calls)-2], "the same checkpoint first")
}

// A log compacted past the observer restarts it from its newest
// checkpoint, the earlier ones kept.
func TestObserverResnapshotAfterCompaction(t *testing.T) {
	ctx := context.Background()
	c := newFakeCluster(t)
	o := newTestObserver(t, c, &hookLog{})
	seed(t, c, o)
	require.NoError(t, o.Step(ctx))
	first := o.State().Checkpoints[0]

	c.push("alice/demo.git", "refs/heads/main", c.commit("x"))
	c.compacted = uint64(len(c.log))
	cp := c.checkpoint()
	require.NoError(t, o.Step(ctx))
	st := o.State()
	require.Len(t, st.Checkpoints, 2)
	assert.Equal(t, first, st.Checkpoints[0])
	assert.Equal(t, cp.Name(), st.Checkpoints[1].Name)
	// The next step restarts the log from the newest checkpoint.
	c.push("alice/demo.git", "refs/heads/main", c.commit("y"))
	require.NoError(t, o.Step(ctx))
	st = o.State()
	assert.EqualValues(t, len(c.log), st.LogIndex)
	assert.Empty(t, st.Rebootstrap)
}

// A PostgreSQL failover leaves no gap: the checkpoint after it is covered
// through the new timeline, its history included.
func TestObserverCoversAPostgreSQLFailover(t *testing.T) {
	ctx := context.Background()
	c := newFakeCluster(t)
	o := newTestObserver(t, c, &hookLog{})
	seed(t, c, o)
	require.NoError(t, o.Step(ctx))

	c.pg.write("before failover", "kept")
	c.pg.failover()
	c.pg.write("after failover", "kept too")
	c.push("alice/demo.git", "refs/heads/main", c.commit("on timeline 2"))
	cp := c.checkpoint()
	require.EqualValues(t, 2, cp.Timeline)
	require.NoError(t, o.Step(ctx))
	m, err := o.store.ReadManifest(cp.Name())
	require.NoError(t, err)
	assert.Equal(t, []string{"00000002.history"}, m.PG.History)
	rows := replay(t, o, m)
	assert.Equal(t, "kept", rows["before failover"])
	assert.Equal(t, "kept too", rows["after failover"])
}

// Old protected checkpoints leave the live directory once older than
// LiveKeep, never the newest one, and with them the data no other needs.
func TestObserverPrunesOnlyProtectedOldCheckpoints(t *testing.T) {
	ctx := context.Background()
	c := newFakeCluster(t)
	o := newTestObserver(t, c, &hookLog{})
	seed(t, c, o)
	require.NoError(t, o.Step(ctx))
	first := o.State().Checkpoints[0].Name
	o.cfg.LiveKeep = 1
	c.push("alice/demo.git", "refs/heads/main", c.commit("z"))
	c.checkpoint()
	require.NoError(t, o.Step(ctx))
	st := o.State()
	require.Len(t, st.Checkpoints, 2)
	assert.Equal(t, StatePruned, st.Checkpoints[0].State)
	assert.Equal(t, StateProtected, st.Checkpoints[1].State)
	_, err := os.Stat(o.store.path("checkpoints", first))
	assert.True(t, os.IsNotExist(err))
	refs, err := o.store.git.refs(ctx, o.store.git.dir("alice/demo.git"), first)
	require.NoError(t, err)
	assert.Empty(t, refs)
}

// Passes after a prune go on: the pruned checkpoint is never handed to the
// hook again, and new checkpoints are ingested and protected (#109).
func TestHuntR8ProvePassesAfterPrune(t *testing.T) {
	ctx := context.Background()
	c := newFakeCluster(t)
	h := &hookLog{}
	o := newTestObserver(t, c, h)
	seed(t, c, o)
	require.NoError(t, o.Step(ctx))
	first := o.State().Checkpoints[0].Name
	o.cfg.LiveKeep = 1
	c.push("alice/demo.git", "refs/heads/main", c.commit("z"))
	c.checkpoint()
	require.NoError(t, o.Step(ctx))
	require.Equal(t, StatePruned, o.State().Checkpoints[0].State)
	calls := len(h.calls)
	for i := range 3 {
		c.push("alice/demo.git", "refs/heads/main", c.commit(fmt.Sprintf("after the prune %d", i)))
		c.checkpoint()
		require.NoError(t, o.Step(ctx), "pass %d after the prune", i)
	}
	assert.NotContains(t, h.calls[calls:], first, "the hook ran again for the pruned checkpoint")
	st := o.State()
	require.Len(t, st.Checkpoints, 5)
	assert.Equal(t, StateProtected, st.Checkpoints[4].State)
	_, err := os.Stat(o.store.path("checkpoints", first))
	assert.True(t, os.IsNotExist(err))
}

// A gap in the archive is not waited out: after gapPasses passes the base
// held does not cover, a new base backup is taken, and the next
// checkpoint is covered from it.
func TestObserverTakesANewBaseAfterAGap(t *testing.T) {
	ctx := context.Background()
	c := newFakeCluster(t)
	o := newTestObserver(t, c, &hookLog{})
	// A frozen clock: the base after the gap is taken within the same
	// second as the first, which must not collide with it.
	frozen := time.Now()
	o.cfg.Now = func() time.Time { return frozen }
	seed(t, c, o)
	require.NoError(t, o.Step(ctx))

	// A segment is lost from the archive before the observer copied it.
	c.push("alice/demo.git", "refs/heads/main", c.commit("lost"))
	c.pg.switchWAL()
	for name := range c.pg.archive {
		if !o.store.haveWAL(name) && !strings.HasSuffix(name, ".history") {
			delete(c.pg.archive, name)
		}
	}
	c.checkpoint()
	for range gapPasses - 1 {
		require.ErrorIs(t, o.Step(ctx), ErrNoCoverage)
	}
	require.Len(t, o.State().Bases, 1)
	require.ErrorIs(t, o.Step(ctx), ErrNoCoverage)
	require.Len(t, o.State().Bases, 2, "a new base after the gap")
	c.push("alice/demo.git", "refs/heads/main", c.commit("after the gap"))
	c.checkpoint()
	require.NoError(t, o.Step(ctx))
	assert.Len(t, o.State().Checkpoints, 2)
}

// stalledSource accepts every call and never answers.
type stalledSource struct{}

func (stalledSource) Export(ctx context.Context, _ *clusterhook.ExportRequest) (*clusterhook.ExportAnswer, io.ReadCloser, error) {
	<-ctx.Done()
	return nil, nil, ctx.Err()
}

// A pass that stalls on a member is bounded, and STATUS.json carries a
// heartbeat written before each pass: monitoring sees a pass that hangs.
func TestRunBoundsAPassAndWritesAHeartbeat(t *testing.T) {
	_, priv, err := box.GenerateKey(rand.Reader)
	require.NoError(t, err)
	o, err := New(Config{
		Name: "nas-test", Dir: t.TempDir(), Endpoints: map[string]Source{"node-a": stalledSource{}}, SecretsKey: priv,
		StagingQuota: 1 << 30, StepTimeout: 300 * time.Millisecond,
	})
	require.NoError(t, err)
	ctx, cancel := context.WithCancel(context.Background())
	stopped := make(chan struct{})
	go func() {
		o.Run(ctx, time.Hour)
		close(stopped)
	}()
	defer func() {
		cancel()
		<-stopped
	}()

	read := func() Status {
		var s Status
		b, err := os.ReadFile(o.store.path("STATUS.json"))
		if err == nil {
			_ = json.Unmarshal(b, &s)
		}
		return s
	}
	require.Eventually(t, func() bool { return read().StepStarted != nil }, 5*time.Second, 10*time.Millisecond, "no heartbeat before the pass")
	require.Eventually(t, func() bool {
		s := read()
		return s.StepStarted == nil && s.LastError != ""
	}, 10*time.Second, 20*time.Millisecond, "the pass never ended")
	assert.Contains(t, read().LastError, "deadline exceeded")
}

// A timeline history the cluster lists with other bytes than the archive
// holds is a timeline number reused by another history: its segments are
// not the archive's, and the observer stops for a human.
func TestObserverQuarantinesAReusedTimeline(t *testing.T) {
	ctx := context.Background()
	c := newFakeCluster(t)
	o := newTestObserver(t, c, &hookLog{})
	seed(t, c, o)
	require.NoError(t, o.Step(ctx))
	c.pg.failover()
	c.push("alice/demo.git", "refs/heads/main", c.commit("on timeline 2"))
	c.checkpoint()
	require.NoError(t, o.Step(ctx))

	// A recover from an older checkpoint promoted onto timeline 2 again.
	c.pg.archive[HistoryName(2)] = []byte("1\t0/2000000\tanother switch point\n")
	c.push("alice/demo.git", "refs/heads/main", c.commit("on the reused timeline"))
	c.checkpoint()
	require.ErrorIs(t, o.Step(ctx), ErrQuarantined)
	assert.Contains(t, o.State().Quarantine.Reason, "00000002.history")
}

// A WAL name from a member becomes a path only once it is a WAL file's
// name: a compromised member cannot write outside the archive.
func TestObserverRefusesAnInvalidWALName(t *testing.T) {
	ctx := context.Background()
	c := newFakeCluster(t)
	o := newTestObserver(t, c, &hookLog{})
	seed(t, c, o)
	c.pg.archive["../../../escape.history"] = []byte("1\t0/0\tx\n")
	c.push("alice/demo.git", "refs/heads/main", c.commit("next"))
	c.checkpoint()
	err := o.Step(ctx)
	require.ErrorContains(t, err, "invalid WAL name")
	_, serr := os.Stat(filepath.Join(o.cfg.Dir, "..", "escape.history"))
	assert.True(t, os.IsNotExist(serr))
}

// A checkpoint name comes from a member, which may be compromised: a name
// that climbs out of staging ("..", "../..") or an absolute path never
// removes the store, its protected checkpoints, nor a file beside it, and
// the view that names it is passed over (#110).
func TestHuntR8ProveObserverDotDotWipesStore(t *testing.T) {
	for _, name := range []string{"..", "../..", "../../..", "/etc"} {
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			c := newFakeCluster(t)
			// The store sits four levels deep in the test's own directory:
			// should the defect come back, what it removes stays there.
			root := t.TempDir()
			cfg := newTestObserver(t, c, &hookLog{}).cfg
			cfg.Dir = filepath.Join(root, "a", "b", "c", "obs")
			require.NoError(t, os.MkdirAll(cfg.Dir, 0o700))
			o, err := New(cfg)
			require.NoError(t, err)
			seed(t, c, o)
			require.NoError(t, o.Step(ctx))
			protected := o.State().Checkpoints[0].Name
			require.Equal(t, StateProtected, o.State().Checkpoints[0].State)
			precious := filepath.Join(filepath.Dir(o.store.dir), "precious-outside-the-observer")
			require.NoError(t, os.WriteFile(precious, []byte("keep"), 0o600))

			// Every member, leader or not, names the forged checkpoint in a
			// higher term.
			c.tamper = func(_ string, req *clusterhook.ExportRequest, data []byte) []byte {
				if req.Op != clusterhook.OpExportCheckpoints {
					return data
				}
				var v clusterhook.ExportView
				require.NoError(t, json.Unmarshal(data, &v))
				v.Term++
				v.Checkpoints = append(v.Checkpoints, clusterhook.ExportCheckpoint{Name: name, Lineage: v.Lineage, Index: 1 << 40})
				b, err := json.Marshal(v)
				require.NoError(t, err)
				return b
			}
			require.Error(t, o.Step(ctx))
			// The same name handed to ingest directly.
			require.ErrorContains(t, o.ingest(ctx, "node-a", clusterhook.ExportCheckpoint{Name: name, Lineage: c.lineage().String(), Index: 1 << 40}, nil), "invalid checkpoint name")

			for _, p := range []string{o.store.path("state.json"), o.store.path("checkpoints", protected, "PROTECTED"), o.store.path("git"), precious} {
				_, err := os.Stat(p)
				require.NoError(t, err, "%s survives", p)
			}
			reopened, err := New(o.cfg)
			require.NoError(t, err)
			assert.Equal(t, o.State().Lineage, reopened.State().Lineage)
			assert.Len(t, reopened.State().Checkpoints, 1)
		})
	}
}

// store.path never yields a path outside the store.
func TestStorePathStaysInside(t *testing.T) {
	s := newStore(t.TempDir(), "git")
	for _, elem := range [][]string{{"staging", ".."}, {"staging", "../.."}, {"..", "x"}, {"staging", "a/../../b"}} {
		assert.Empty(t, s.path(elem...), "%v", elem)
	}
	assert.Equal(t, filepath.Join(s.dir, "staging", "x"), s.path("staging", "x"))
}

// recoverFake starts a new lineage on the fake cluster as recover
// --from-observer does: a new identity, a log that restarts from index 1,
// a table holding the restored refs.
func recoverFake(c *fakeCluster, id byte) {
	c.mu.Lock()
	c.cluster = consensus.ClusterID{id, 0x15}
	c.log, c.compacted = nil, 0
	c.table = reftable.New()
	c.term = 1
	c.mu.Unlock()
}

// The cursor is held per lineage: after a new lineage is accepted, its
// checkpoints are ingested although their indexes are below the newest
// one accepted in the old lineage (#55).
func TestHuntR2ProveLinCurRecoverRestartsAtOne(t *testing.T) {
	ctx := context.Background()
	c := newFakeCluster(t)
	o := newTestObserver(t, c, &hookLog{})
	seed(t, c, o)
	require.NoError(t, o.Step(ctx))
	for i := range 4 {
		c.push("alice/demo.git", "refs/heads/main", c.commit(fmt.Sprintf("old %d", i)))
		c.checkpoint()
		require.NoError(t, o.Step(ctx))
	}
	oldLast := o.lastIndex()
	require.Greater(t, oldLast, uint64(12))

	recoverFake(c, 0x0d)
	c.push("alice/demo.git", "refs/heads/main", c.commit("after recover"))
	cp := c.checkpoint()
	require.Less(t, cp.Index, oldLast)
	require.ErrorIs(t, o.Step(ctx), ErrQuarantined)
	q := o.State().Quarantine
	require.NotNil(t, q)
	require.NoError(t, o.Accept(q.Lineage))
	require.NoError(t, o.Step(ctx))
	st := o.State()
	a, ok := st.newestOf(q.Lineage)
	require.True(t, ok, "a checkpoint of the new lineage is accepted")
	assert.Equal(t, cp.Name(), a.Name)

	// The next one follows in the new lineage.
	c.push("alice/demo.git", "refs/heads/main", c.commit("after recover 2"))
	cp = c.checkpoint()
	require.NoError(t, o.Step(ctx))
	st = o.State()
	a, _ = st.newestOf(q.Lineage)
	assert.Equal(t, cp.Name(), a.Name)
}

// A restore of an older snapshot starts a lineage below the newest index
// accepted: it is followed from its first checkpoint (#55).
func TestHuntR2ProveLinCurRestoreOlderSnapshot(t *testing.T) {
	ctx := context.Background()
	c := newFakeCluster(t)
	o := newTestObserver(t, c, &hookLog{})
	seed(t, c, o)
	require.NoError(t, o.Step(ctx))
	snapshot := append([]consensus.Entry(nil), c.log...)
	for i := range 3 {
		c.push("alice/demo.git", "refs/heads/main", c.commit(fmt.Sprintf("old %d", i)))
		c.checkpoint()
		require.NoError(t, o.Step(ctx))
	}
	oldLast := o.lastIndex()

	// The restored cluster replays the snapshot's entries under its new
	// identity, then goes on.
	recoverFake(c, 0x0e)
	for _, e := range snapshot {
		e.Token.Cluster = c.cluster
		c.log = append(c.log, e)
		c.table.Apply(e)
	}
	c.push("alice/demo.git", "refs/heads/main", c.commit("after restore"))
	cp := c.checkpoint()
	require.Less(t, cp.Index, oldLast)
	require.ErrorIs(t, o.Step(ctx), ErrQuarantined)
	require.NoError(t, o.Accept(o.State().Quarantine.Lineage))
	require.NoError(t, o.Step(ctx))
	st := o.State()
	a, ok := st.newestOf(c.lineage().String())
	require.True(t, ok)
	assert.Equal(t, cp.Name(), a.Name)
}

// An accepted checkpoint described otherwise is seen through the
// export's real filter, and before a compacted log jumps past it; one
// gone from the leader's table while an older one is listed is a fork too
// (#87).
func TestHuntR5ProveCheckKnownDead(t *testing.T) {
	ctx := context.Background()
	setup := func(t *testing.T) (*fakeCluster, *Observer, Accepted) {
		c := newFakeCluster(t)
		o := newTestObserver(t, c, &hookLog{})
		seed(t, c, o)
		require.NoError(t, o.Step(ctx))
		c.push("alice/demo.git", "refs/heads/main", c.commit("two"))
		c.checkpoint()
		require.NoError(t, o.Step(ctx))
		require.Len(t, o.State().Checkpoints, 2)
		return c, o, o.State().Checkpoints[1]
	}
	rewrite := func(c *fakeCluster, name string) {
		c.tamper = func(_ string, req *clusterhook.ExportRequest, data []byte) []byte {
			if req.Op != clusterhook.OpExportCheckpoints {
				return data
			}
			var v clusterhook.ExportView
			_ = json.Unmarshal(data, &v)
			for i := range v.Checkpoints {
				if v.Checkpoints[i].Name == name {
					v.Checkpoints[i].Root = strings.Repeat("e", 64)
				}
			}
			b, _ := json.Marshal(v)
			return b
		}
	}

	t.Run("the real filter shows an accepted checkpoint", func(t *testing.T) {
		c, o, a := setup(t)
		rewrite(c, a.Name)
		err := o.Step(ctx)
		require.ErrorIs(t, err, ErrQuarantined)
		assert.Contains(t, err.Error(), "described otherwise")
	})

	t.Run("fork then compaction", func(t *testing.T) {
		c, o, a := setup(t)
		rewrite(c, a.Name)
		c.push("alice/demo.git", "refs/heads/main", c.commit("three"))
		c.checkpoint()
		c.mu.Lock()
		c.compacted = uint64(len(c.log))
		c.mu.Unlock()
		err := o.Step(ctx)
		require.ErrorIs(t, err, ErrQuarantined)
		assert.Len(t, o.State().Checkpoints, 2, "nothing of the divergent history is accepted")
	})

	t.Run("an accepted checkpoint gone from the table", func(t *testing.T) {
		c, o, a := setup(t)
		c.tamper = func(_ string, req *clusterhook.ExportRequest, data []byte) []byte {
			if req.Op != clusterhook.OpExportCheckpoints {
				return data
			}
			var v clusterhook.ExportView
			_ = json.Unmarshal(data, &v)
			v.Checkpoints = slices.DeleteFunc(v.Checkpoints, func(cp clusterhook.ExportCheckpoint) bool { return cp.Name == a.Name })
			b, _ := json.Marshal(v)
			return b
		}
		err := o.Step(ctx)
		require.ErrorIs(t, err, ErrQuarantined)
		assert.Contains(t, err.Error(), "gone from the leader's table")
	})

	t.Run("control: an honest leader", func(t *testing.T) {
		c, o, _ := setup(t)
		c.push("alice/demo.git", "refs/heads/main", c.commit("three"))
		c.checkpoint()
		require.NoError(t, o.Step(ctx))
		assert.Len(t, o.State().Checkpoints, 3)
	})
}
