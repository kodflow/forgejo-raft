// Copyright 2026 The Forgejo Authors. All rights reserved.
// SPDX-License-Identifier: GPL-3.0-or-later

package observer

import (
	"bytes"
	"context"
	"sync"
	"testing"

	"forgejo.org/modules/clusterhook"
	"forgejo.org/modules/json"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// A member that serves other bytes than the digest listed for a segment is
// passed over for one that serves the listed bytes, and a name planted
// past the checkpoint's position is never fetched (#105).
func TestObserverChecksTheWALAgainstItsDigestAndPosition(t *testing.T) {
	ctx := context.Background()
	c := newFakeCluster(t)
	o := newTestObserver(t, c, &hookLog{})
	planted := SegmentName(1, 1<<40, fakeSegSize)
	var mu sync.Mutex
	var asked []string
	c.tamper = func(slot string, req *clusterhook.ExportRequest, data []byte) []byte {
		switch req.Op {
		case clusterhook.OpExportWAL:
			mu.Lock()
			asked = append(asked, slot+" "+req.Name)
			mu.Unlock()
			if slot == "node-a" {
				return bytes.ToUpper(data)
			}
		case clusterhook.OpExportWALList:
			if slot == "node-a" {
				var files []clusterhook.WALFile
				require.NoError(t, json.Unmarshal(data, &files))
				files = append(files, clusterhook.WALFile{Name: planted, Size: 64, SHA256: digest(make([]byte, 64))})
				b, err := json.Marshal(files)
				require.NoError(t, err)
				return b
			}
		}
		return data
	}
	seed(t, c, o)
	require.NoError(t, o.Step(ctx))
	st := o.State()
	require.Len(t, st.Checkpoints, 1)
	m, err := o.store.ReadManifest(st.Checkpoints[0].Name)
	require.NoError(t, err)
	rows := replay(t, o, m)
	assert.NotEmpty(t, rows["branch alice/demo.git refs/heads/main"])
	mu.Lock()
	defer mu.Unlock()
	assert.Contains(t, asked, "node-b "+m.PG.Segments[0], "taken from a member that serves the listed bytes")
	for _, a := range asked {
		assert.NotContains(t, a, planted)
	}
}
