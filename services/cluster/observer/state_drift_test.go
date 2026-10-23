// Copyright 2026 The Forgejo Authors. All rights reserved.
// SPDX-License-Identifier: GPL-3.0-or-later

package observer

import (
	"context"
	"os"
	"testing"

	"forgejo.org/modules/json"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// reopen stands for a restart: a new Observer on the same directory.
func reopen(t *testing.T, o *Observer) *Observer {
	t.Helper()
	n, err := New(o.cfg)
	require.NoError(t, err)
	return n
}

// A prune stopped after it removed a checkpoint's directory, or only its
// manifest, and before it saved the state, is finished by the next pass
// instead of failing every pass after it (#61).
func TestPruneFinishesAPruneStoppedBeforeTheState(t *testing.T) {
	ctx := context.Background()
	c := newFakeCluster(t)
	o := newTestObserver(t, c, &hookLog{})
	seed(t, c, o)
	require.NoError(t, o.Step(ctx))
	for _, msg := range []string{"two", "three"} {
		c.push("alice/demo.git", "refs/heads/main", c.commit(msg))
		c.checkpoint()
		require.NoError(t, o.Step(ctx))
	}
	st := o.State()
	require.Len(t, st.Checkpoints, 3)
	gone, half := st.Checkpoints[0].Name, st.Checkpoints[1].Name
	// The first prune got past its RemoveAll, the second past the
	// manifest's removal only; the state says both are protected still.
	for _, name := range []string{gone, half} {
		require.NoError(t, o.store.git.dropRefs(ctx, o.store.git.dir("alice/demo.git"), name))
	}
	require.NoError(t, os.RemoveAll(o.store.path("checkpoints", gone)))
	require.NoError(t, os.Remove(o.store.path("checkpoints", half, "manifest.json")))
	o = reopen(t, o)
	o.cfg.LiveKeep = 1
	c.push("alice/demo.git", "refs/heads/main", c.commit("four"))
	c.checkpoint()
	require.NoError(t, o.Step(ctx))
	st = o.State()
	require.Len(t, st.Checkpoints, 4)
	for i := range 3 {
		assert.Equal(t, StatePruned, st.Checkpoints[i].State, st.Checkpoints[i].Name)
	}
	assert.Equal(t, StateProtected, st.Checkpoints[3].State)
	for _, name := range []string{gone, half} {
		_, err := os.Stat(o.store.path("checkpoints", name))
		assert.True(t, os.IsNotExist(err), name)
	}
}

// A pass stopped after it published a checkpoint and before it saved the
// state leaves the checkpoint published: the next pass takes it into the
// state, and never quarantines the observer for a fork (#61).
func TestIngestAdoptsACheckpointPublishedBeforeACrash(t *testing.T) {
	ctx := context.Background()
	c := newFakeCluster(t)
	o := newTestObserver(t, c, &hookLog{})
	seed(t, c, o)
	require.NoError(t, o.Step(ctx))
	before, err := os.ReadFile(o.store.path("state.json"))
	require.NoError(t, err)
	c.push("alice/demo.git", "refs/heads/main", c.commit("two"))
	c.checkpoint()
	require.NoError(t, o.Step(ctx))
	name := o.State().Checkpoints[1].Name
	// The crash: the state as it was before the pass, the checkpoint
	// published, its hook never run.
	require.NoError(t, os.WriteFile(o.store.path("state.json"), before, 0o600))
	require.NoError(t, os.Remove(o.store.path("checkpoints", name, "PROTECTED")))
	o = reopen(t, o)
	require.NoError(t, o.Step(ctx))
	st := o.State()
	require.Nil(t, st.Quarantine)
	require.Len(t, st.Checkpoints, 2)
	assert.Equal(t, name, st.Checkpoints[1].Name)
	assert.Equal(t, StateProtected, st.Checkpoints[1].State)
}

// A checkpoint published under the same name with another description is
// still a fork.
func TestIngestRefusesAPublishedCheckpointDescribedOtherwise(t *testing.T) {
	ctx := context.Background()
	c := newFakeCluster(t)
	o := newTestObserver(t, c, &hookLog{})
	seed(t, c, o)
	require.NoError(t, o.Step(ctx))
	before, err := os.ReadFile(o.store.path("state.json"))
	require.NoError(t, err)
	c.push("alice/demo.git", "refs/heads/main", c.commit("two"))
	c.checkpoint()
	require.NoError(t, o.Step(ctx))
	name := o.State().Checkpoints[1].Name
	m, err := o.store.ReadManifest(name)
	require.NoError(t, err)
	m.Checkpoint.Root = "another root"
	b, err := json.Marshal(m)
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(o.store.path("checkpoints", name, "manifest.json"), b, 0o600))
	require.NoError(t, os.WriteFile(o.store.path("state.json"), before, 0o600))
	o = reopen(t, o)
	require.ErrorIs(t, o.Step(ctx), ErrQuarantined)
}
