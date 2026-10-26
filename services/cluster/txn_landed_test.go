// Copyright 2026 The Forgejo Authors. All rights reserved.
// SPDX-License-Identifier: GPL-3.0-or-later

package cluster

import (
	"context"
	"strings"
	"testing"
	"time"

	"forgejo.org/modules/clusterhook"
	"forgejo.org/services/cluster/reftable"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// applyCmd applies a command the other members committed while this node
// was cut off, which must be accepted.
func (e *cutOffEngine) applyCmd(kind reftable.Kind, body any) {
	data, err := reftable.EncodeVersion(e.table.WriteVersion(), kind, body)
	if err != nil {
		panic(err)
	}
	if res := e.apply(data); !res.OK {
		panic(res.Reason)
	}
}

// renameA renames o/a.git and its wiki to o/b.git.
func renameA(e *cutOffEngine) {
	e.applyCmd(reftable.KindRepoRename, &reftable.RepoRename{
		From: "o/a.git", To: "o/b.git", Also: [][2]string{{"o/a.wiki.git", "o/b.wiki.git"}}, Time: 2, Nonce: "ren",
	})
}

// deleteMain is a push of the node cut off that deletes refs/heads/main
// of o/a.git, prepared by the real buildTxn on a node that has not applied
// yet what the others commit meanwhile. nonce is the git process's, ""
// for a git run outside Forgejo's wrapper, whose transaction carries none.
func deleteMain(t *testing.T, r *Replicator, nonce string) (*reftable.RefTxn, []byte) {
	t.Helper()
	cur, ok := r.table.Get("o/a.git", "refs/heads/main")
	require.True(t, ok)
	req := &clusterhook.Request{
		Op: clusterhook.OpPrepared, Nonce: nonce, GitDir: t.TempDir(), RepoID: 7, PusherID: 3,
		Lines: []clusterhook.Line{{Old: cur.SHA, New: strings.Repeat("0", 40), Ref: "refs/heads/main"}},
	}
	tx, _, err := r.buildTxn(context.Background(), "o/a.git", req)
	require.NoError(t, err)
	require.Len(t, tx.Updates, 1)
	require.True(t, tx.Updates[0].Delete)
	data, err := reftable.EncodeVersion(r.table.WriteVersion(), reftable.KindRefTxn, tx)
	require.NoError(t, err)
	return tx, data
}

// A push deleting a branch, whose proposal's outcome was unknown, was
// settled by the values of the table before its nonce: a rename, a
// transfer or an owner's rename of the repository, committed meanwhile,
// tombstones every live ref of the source key at its revision plus one,
// the value the deletion would have left, and a deletion of the
// repository does the same. The push was answered OK, git deleted the
// branch on this node and the client read it deleted, while the cluster
// never committed the deletion and served the branch under the new name
// on every member. While the ring of nonces covers the proposal, the
// absence of its nonce is a certain refusal, whatever the values show,
// and the push is refused, with the reason Apply gave to its copy (#252).
func TestABranchDeletionLostWhileItsRepositoryMovedIsRefused(t *testing.T) {
	cases := []struct {
		name string
		// meanwhile is what the others commit while this node is cut off;
		// data is this node's proposal.
		meanwhile func(e *cutOffEngine, data []byte)
		// moved is where the branch lives on, "" when it went with its
		// repository; why is Apply's refusal of the copy.
		moved, why string
	}{
		{
			name:      "proposal lost, repository renamed",
			meanwhile: func(e *cutOffEngine, _ []byte) { renameA(e) },
			moved:     "o/b.git", why: "renamed to o/b.git",
		},
		{
			name: "proposal committed after the rename, refused by Apply",
			meanwhile: func(e *cutOffEngine, data []byte) {
				renameA(e)
				if res := e.apply(data); res.OK {
					panic("the deletion committed after the rename")
				}
			},
			moved: "o/b.git", why: "renamed to o/b.git",
		},
		{
			name: "proposal lost, repository transferred",
			meanwhile: func(e *cutOffEngine, _ []byte) {
				e.applyCmd(reftable.KindRepoRename, &reftable.RepoRename{
					From: "o/a.git", To: "q/a.git", Also: [][2]string{{"o/a.wiki.git", "q/a.wiki.git"}}, Time: 2, Nonce: "transfer",
				})
			},
			moved: "q/a.git", why: "renamed to q/a.git",
		},
		{
			name: "proposal lost, owner renamed",
			meanwhile: func(e *cutOffEngine, _ []byte) {
				e.applyCmd(reftable.KindOwnerRename, &reftable.OwnerRename{From: "o", To: "p", Time: 2, Nonce: "own"})
			},
			moved: "p/a.git", why: "renamed to p/a.git",
		},
		{
			name: "proposal lost, repository deleted",
			meanwhile: func(e *cutOffEngine, _ []byte) {
				e.applyCmd(reftable.KindRepoDelete, &reftable.RepoDelete{Repo: "o/a.git", RepoID: 7, Time: 2, Nonce: "del"})
			},
			why: "the repository o/a.git was deleted",
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			r, eng := cutOff(t, reftable.PayloadVersion, 0, 0)
			eng.lost = true
			tx, data := deleteMain(t, r, "proc")
			eng.meanwhile = func(e *cutOffEngine) { c.meanwhile(e, data) }
			ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
			defer cancel()

			resp := r.proposeWithRetry(ctx, "o/a.git", "proc", tx, data)

			_, kept := r.table.CommittedNonce(tx.Nonce)
			require.False(t, kept, "the deletion never committed")
			require.True(t, r.table.NoncesKeptSince(eng.tok().Identity(), 0), "the ring covers the proposal")
			src, _ := r.table.Get("o/a.git", "refs/heads/main")
			require.Equal(t, tx.Updates[0].ExpectRev+1, src.Rev, "the values read as the deletion's own")
			require.True(t, src.Tombstone)
			if c.moved != "" {
				dst, live := r.table.Get(c.moved, "refs/heads/main")
				require.True(t, live && dst.Live(), "the branch lives on under %s", c.moved)
			}
			assert.False(t, resp.OK, "answered OK to git, which deletes the branch and tells the client it is deleted")
			assert.Contains(t, resp.Message, "the cluster refused the write")
			assert.Contains(t, resp.Message, c.why)
		})
	}
}

// git init's HEAD for a repository created anew under a key whose former
// repository was deleted, whose proposal's outcome was unknown, while a
// rename into that key committed meanwhile: the rename moves the source's
// HEAD into the key at the revision of the key's tombstone plus one, the
// revision the creation expected plus one, and with the same target. The
// creation was answered OK while the key held the renamed repository. It
// is refused, its nonce absent while the ring covers it (#252).
func TestAGitInitLostWhileARenameTookItsKeyIsRefused(t *testing.T) {
	r, eng := cutOff(t, reftable.PayloadVersion, 0, 0)
	// o/k.git lived, its HEAD changed once, and it was deleted: its HEAD's
	// tombstone stands at a revision above o/a.git's HEAD.
	eng.applyTxn(&reftable.RefTxn{Repo: "o/k.git", RepoID: 5, Format: "sha1", Nonce: "init-k", Init: true, Updates: []reftable.RefUpdate{{Ref: "HEAD", Symbolic: "refs/heads/main"}}})
	eng.applyTxn(&reftable.RefTxn{Repo: "o/k.git", RepoID: 5, Format: "sha1", Nonce: "head-k", Updates: []reftable.RefUpdate{{Ref: "HEAD", Symbolic: "refs/heads/dev", ExpectRev: 1}}})
	eng.applyCmd(reftable.KindRepoDelete, &reftable.RepoDelete{Repo: "o/k.git", RepoID: 5, Time: 1, Nonce: "del-k"})
	head, ok := r.table.Get("o/k.git", "HEAD")
	require.True(t, ok)
	require.True(t, head.Tombstone)
	src, _ := r.table.Get("o/a.git", "HEAD")
	require.LessOrEqual(t, src.Rev, head.Rev)

	read, _ := r.table.Applied()
	mine := &reftable.RefTxn{
		Repo: "o/k.git", RepoID: 8, Format: "sha1", Time: 3, Nonce: "mine", Init: true, ReadAt: read.Index,
		Updates: []reftable.RefUpdate{{Ref: "HEAD", Symbolic: "refs/heads/main", ExpectRev: head.Rev}},
	}
	data, err := reftable.EncodeVersion(r.table.WriteVersion(), reftable.KindRefTxn, mine)
	require.NoError(t, err)
	eng.lost = true
	eng.meanwhile = func(e *cutOffEngine) {
		e.applyCmd(reftable.KindRepoRename, &reftable.RepoRename{
			From: "o/a.git", To: "o/k.git", Also: [][2]string{{"o/a.wiki.git", "o/k.wiki.git"}}, Time: 2, Nonce: "ren",
		})
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	resp := r.proposeWithRetry(ctx, "o/k.git", "proc", mine, data)

	_, kept := r.table.CommittedNonce("mine")
	require.False(t, kept, "the creation never committed")
	now, _ := r.table.Get("o/k.git", "HEAD")
	require.Equal(t, head.Rev+1, now.Rev, "the values read as the creation's own")
	require.Equal(t, "refs/heads/main", now.Symbolic)
	from, _, renamed := r.table.RenamedFrom("o/k.git")
	require.True(t, renamed)
	require.Equal(t, "o/a.git", from, "the key holds the renamed repository")
	assert.False(t, resp.OK, "the creation was answered OK")
	assert.Contains(t, resp.Message, "the cluster refused the write")
}

// Past the ring of nonces, a ref transaction is told by its values (#234),
// but not on a key a deletion, a rename, a transfer or an owner's rename
// rewrote since it read its revisions: their values are the ones it would
// have left, and the outcome stays unknown, never a success nor a refusal.
// A transaction without a nonce is told by its values alone, with the same
// reservation. On a key nothing else rewrote, the values still tell (#252).
func TestPastTheRingOrWithoutANonceADeletionIsNotToldByARenamesValues(t *testing.T) {
	cases := []struct {
		name   string
		nonce  string
		others int
		lost   bool
		rename bool
		// ok: answered OK; otherwise answered unknown.
		ok bool
	}{
		{name: "past the ring, lost, repository renamed", nonce: "proc", others: reftable.NoncesKept + 1, lost: true, rename: true},
		{name: "past the ring, committed, repository renamed after it", nonce: "proc", others: reftable.NoncesKept + 1, rename: true},
		{name: "past the ring, committed, nothing else", nonce: "proc", others: reftable.NoncesKept + 1, ok: true},
		{name: "without a nonce, lost, repository renamed", lost: true, rename: true},
		{name: "without a nonce, committed, nothing else", ok: true},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			r, eng := cutOff(t, reftable.PayloadVersion, c.others, 0)
			eng.lost = c.lost
			tx, data := deleteMain(t, r, c.nonce)
			require.Equal(t, c.nonce == "", tx.Nonce == "")
			if c.rename {
				eng.meanwhile = renameA
			}
			ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
			defer cancel()

			resp := r.proposeWithRetry(ctx, "o/a.git", c.nonce, tx, data)

			src, _ := r.table.Get("o/a.git", "refs/heads/main")
			require.Equal(t, tx.Updates[0].ExpectRev+1, src.Rev, "the values read as the deletion's own")
			require.True(t, src.Tombstone)
			if c.ok {
				assert.True(t, resp.OK, resp.Message)
				return
			}
			assert.False(t, resp.OK, "the table cannot tell the deletion from the rename's tombstone")
			assert.NotContains(t, resp.Message, "the cluster refused the write", "it may have committed")
			assert.Contains(t, resp.Message, "the outcome of the write is unknown")
		})
	}
}
