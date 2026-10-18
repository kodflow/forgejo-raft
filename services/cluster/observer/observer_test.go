// Copyright 2026 The Forgejo Authors. All rights reserved.
// SPDX-License-Identifier: GPL-3.0-or-later

package observer

import (
	"context"
	"go/ast"
	"go/parser"
	"go/token"
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

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
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
			var v clusterhook.ExportView
			require.NoError(t, json.Unmarshal(data, &v))
			for i := range v.Checkpoints {
				v.Checkpoints[i].Root = strings.Repeat("0", 64)
			}
			// The rewritten one is served again, whatever was asked.
			v.Checkpoints = append(v.Checkpoints, clusterhook.ExportCheckpoint{Name: before.Checkpoints[0].Name, Root: strings.Repeat("f", 64)})
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
