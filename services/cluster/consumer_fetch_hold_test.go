// Copyright 2026 The Forgejo Authors. All rights reserved.
// SPDX-License-Identifier: GPL-3.0-or-later

package cluster

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"forgejo.org/services/cluster/consensus"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// The leader's consumer fetches, in the background, the commits the
// post-receive work of an event reads and this node lacks (consumer.obtain,
// startFetch, #224), through the object channel's fetchMissing, which ends
// in receivePack under the repository's directory. It entered no hold of
// that directory: the hold of an owner's rename, or of a repository's,
// returned at once, Forgejo moved the directory, and the member's answer,
// arriving after, created the old directory again under its quarantine.
// The move back of a refused rename, and every later rename to that name,
// met it ("file exists", #257). The consumer's fetch now enters the
// directory's hold: the hold cancels it and waits for its end, no fetch
// starts there until it is released, and one starts again after (#275).
//
// The fetch below is fetchMissing's own sequence (its refs read, then the
// member's pack through receivePack); only the network is modelled: the
// member answers once the gate opens, and the request, like fetchFrom's,
// ends with its context.
func TestADirectoryHoldEndsTheConsumersFetch(t *testing.T) {
	t.Parallel()
	for _, tt := range []struct {
		name string
		// held is what the rename holds; moved and to the directory it
		// moves, relative to the repository root.
		held      []string
		moved, to string
	}{
		{"owner", []string{"user4", "heir"}, "user4", "heir"},
		{"repository", []string{"user4/c.git", "user4/c.wiki.git", "user4/d.git", "user4/d.wiki.git"}, "user4/c.git", "user4/d.git"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			b := newFetchBench(t, "user4/c.git") // index 1: HEAD + main = one
			p := b.p
			ctx := t.Context()
			p.unfence()
			require.Eventually(t, func() bool { p.pass(ctx); return p.Projected("user4/c.git") >= 1 }, 10*time.Second, 10*time.Millisecond)
			dir := filepath.Join(p.root, "user4", "c.git")
			moved := filepath.Join(p.root, filepath.FromSlash(tt.moved))
			to := filepath.Join(p.root, filepath.FromSlash(tt.to))

			// A commit no ref of the table names (a push forced over it
			// before this node received it): the consumer fetches it for
			// the work.
			one, err := gitRun(ctx, b.work, nil, "rev-parse", "HEAD")
			require.NoError(t, err)
			two := commit(t, b.work, "two")
			var pack bytes.Buffer
			require.NoError(t, packObjects(ctx, b.work, []string{two}, []string{one}, &pack))
			answered := make(chan struct{})
			reached := make(chan struct{}, 1)

			c := newConsumer(&Replicator{projector: p, table: p.table, root: p.root})
			c.fetch = func(ctx context.Context, repo, dir string, shas []string) error {
				if _, err := readGitRefs(ctx, dir); err != nil {
					return err
				}
				select {
				case reached <- struct{}{}:
				default:
				}
				select {
				case <-answered:
				case <-ctx.Done():
					return ctx.Err()
				}
				return receivePack(ctx, dir, bytes.NewReader(pack.Bytes()))
			}
			done := c.startFetch(ctx, "user4/c.git", dir, []string{two})
			require.NotNil(t, done)
			select {
			case <-reached:
			case <-time.After(10 * time.Second):
				t.Fatal("the fetch did not start")
			}

			// Forgejo renames on this node: the hold first.
			hctx, cancel := context.WithTimeout(ctx, 10*time.Second)
			defer cancel()
			release, err := p.holdDirs(hctx, tt.held)
			require.NoError(t, err, "the consumer's fetch was cancelled, not waited for")
			select {
			case <-done:
			default:
				assert.Fail(t, "the hold returned while the consumer's fetch still writes under the directory")
			}
			assert.Nil(t, c.startFetch(ctx, "user4/c.git", dir, []string{two}), "no fetch starts under a held directory")
			require.NoError(t, os.Rename(moved, to))

			// The member's answer arrives; whatever fetch still runs ends.
			close(answered)
			select {
			case <-done:
			case <-time.After(10 * time.Second):
				t.Fatal("the fetch did not end")
			}
			_, err = os.Stat(moved)
			assert.True(t, os.IsNotExist(err), "nothing is created under the old name while it is held: %v", err)

			// The cluster refused the rename: Forgejo moves the directory
			// back, then releases the hold.
			require.NoError(t, os.Rename(to, moved), "the move back of a refused rename")
			release()

			done = c.startFetch(ctx, "user4/c.git", dir, []string{two})
			require.NotNil(t, done, "a fetch starts again once released")
			select {
			case <-done:
			case <-time.After(10 * time.Second):
				t.Fatal("the fetch did not end")
			}
			_, err = gitRun(ctx, dir, nil, "cat-file", "-e", two)
			assert.NoError(t, err, "the commit is fetched once released")
		})
	}
}

// The operator's backfill (`forgejo cluster objects backfill`) fetches
// through fetchMissing too, into the directory of every key missing
// objects: while Forgejo moves that directory on this node it is refused,
// the operator told to retry, and released, it fetches again (#275).
func TestTheBackfillIsRefusedUnderADirectoryAHoldCovers(t *testing.T) {
	t.Parallel()
	b := newFetchBench(t, "user4/c.git") // main names a commit the directory lacks
	p := b.p
	ctx := t.Context()
	require.NoError(t, initBare(ctx, filepath.Join(p.root, "user4", "c.git"), "sha1"))
	r := &Replicator{projector: p, table: p.table, root: p.root, objects: &objectChannel{
		engine: func() consensus.Engine { return nil },
	}}

	release, err := p.holdDirs(ctx, []string{"user4", "heir"})
	require.NoError(t, err)
	rep, err := r.VerifyObjects(ctx, true)
	require.NoError(t, err)
	require.Len(t, rep.Errors, 1)
	assert.Contains(t, rep.Errors[0], "moves with a rename on this node", "refused under the hold")
	assert.Equal(t, 1, rep.Missing["user4/c.git"])
	release()

	rep, err = r.VerifyObjects(ctx, true)
	require.NoError(t, err)
	require.Len(t, rep.Errors, 1)
	assert.Contains(t, rep.Errors[0], "no membership yet", "fetched once released")
}
