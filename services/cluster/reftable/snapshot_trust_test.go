// Copyright 2026 The Forgejo Authors. All rights reserved.
// SPDX-License-Identifier: GPL-3.0-or-later

package reftable

import (
	"errors"
	"fmt"
	"strings"
	"testing"

	"forgejo.org/services/cluster/consensus"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// A snapshot cut at a payload version this binary does not apply is
// refused with ErrVersion, which stops the node, instead of being restored
// with the state it cannot keep (#50).
func TestNewerSnapshotIsRefused(t *testing.T) {
	tb := New()
	err := tb.Restore(strings.NewReader(fmt.Sprintf(`{"format":1,"payload":%d,"repos":{},"last":{},"deleted":{},"renamed":{}}`, PayloadVersion+1)), consensus.SnapshotMeta{})
	require.ErrorIs(t, err, ErrVersion, "WriteVersion %d", tb.WriteVersion())
	assert.Equal(t, uint8(BaseVersion), tb.WriteVersion(), "the refused snapshot left the table as it was")
}

// The snapshot this binary cuts at its own payload version still restores.
func TestHuntR1ProveNewerSnapshotIsRefused(t *testing.T) {
	h := newHarness(t)
	for v := BaseVersion + 1; v <= PayloadVersion; v++ {
		require.True(t, h.apply(KindAudit, &Audit{Operator: "op", Action: AuditPayloadVersion, Target: fmt.Sprint(v), Outcome: AuditRaised}).OK)
	}
	require.Equal(t, uint8(PayloadVersion), h.table.WriteVersion())
	snap, err := h.table.Snapshot()
	require.NoError(t, err)
	var b strings.Builder
	_, err = snap.WriteTo(&b)
	require.NoError(t, err)
	tb := New()
	require.NoError(t, tb.Restore(strings.NewReader(b.String()), consensus.SnapshotMeta{}))
	assert.Equal(t, uint8(PayloadVersion), tb.WriteVersion())

	newer := strings.Replace(b.String(), fmt.Sprintf(`"payload":%d`, PayloadVersion), fmt.Sprintf(`"payload":%d`, PayloadVersion+1), 1)
	require.NotEqual(t, b.String(), newer)
	require.ErrorIs(t, New().Restore(strings.NewReader(newer), consensus.SnapshotMeta{}), ErrVersion)
}

// Every repository key and ref name a snapshot holds passes the checks
// Apply makes: the projector, the observer and a recover turn the keys into
// paths, and a snapshot comes with its sender's digest only (#81).
func TestHuntR4ProveSnapKeyEscape(t *testing.T) {
	for name, body := range map[string]string{
		"repo":          `"repos":{"../raft-data":{}},"last":{},"deleted":{},"renamed":{}`,
		"deleted":       `"repos":{},"last":{},"deleted":{"../raft-data":5},"renamed":{}`,
		"rename source": `"repos":{"evil/stolen.git":{}},"last":{},"deleted":{},"renamed":{"evil/stolen.git":{"from":"../tls","i":6}}`,
		"rename target": `"repos":{},"last":{},"deleted":{},"renamed":{"../../x":{"from":"a/b.git","i":6}}`,
		"absolute":      `"repos":{"/etc/x.git":{}},"last":{},"deleted":{},"renamed":{}`,
		"last":          `"repos":{},"last":{"../x":1},"deleted":{},"renamed":{}`,
		"ref":           `"repos":{"a/b.git":{"refs/heads/../../x":{"sha":"` + strings.Repeat("a", 40) + `","rev":1}}},"last":{},"deleted":{},"renamed":{}`,
		"ref control":   `"repos":{"a/b.git":{"refs/heads/x\n":{"sha":"` + strings.Repeat("a", 40) + `","rev":1}}},"last":{},"deleted":{},"renamed":{}`,
		"symbolic":      `"repos":{"a/b.git":{"HEAD":{"sym":"../../x","rev":1}}},"last":{},"deleted":{},"renamed":{}`,
	} {
		t.Run(name, func(t *testing.T) {
			tb := New()
			var changed bool
			tb.OnChange(func(Change) { changed = true })
			err := tb.Restore(strings.NewReader(`{"format":1,`+body+`}`), consensus.SnapshotMeta{})
			require.Error(t, err)
			assert.True(t, errors.Is(err, consensus.ErrStateMachineFailed), "%v", err)
			assert.False(t, changed, "a refused snapshot reached the projector")
			assert.Empty(t, tb.Repos())
		})
	}
	tb := New()
	require.NoError(t, tb.Restore(strings.NewReader(`{"format":1,"repos":{"a/b.git":{"HEAD":{"sym":"refs/heads/main","rev":1}}},"last":{"a/b.git":1},"deleted":{"c/d.wiki.git":2},"renamed":{"e/f.git":{"from":"e/g.git","i":3}}}`), consensus.SnapshotMeta{}))
	assert.Equal(t, []string{"a/b.git"}, tb.Repos())
}
