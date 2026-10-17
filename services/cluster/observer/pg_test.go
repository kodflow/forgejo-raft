// Copyright 2026 The Forgejo Authors. All rights reserved.
// SPDX-License-Identifier: GPL-3.0-or-later

package observer

import (
	"archive/tar"
	"bytes"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestWALNamesAndPositions(t *testing.T) {
	l, err := ParseLSN("1/2A000028")
	require.NoError(t, err)
	assert.Equal(t, "1/2A000028", l.String())
	assert.Equal(t, "00000003000000010000002A", SegmentName(3, l, 16<<20))
	tli, segno, ok := parseSegment("00000003000000010000002A", 16<<20)
	require.True(t, ok)
	assert.EqualValues(t, 3, tli)
	assert.EqualValues(t, 256+0x2a, segno)
	_, err = ParseLSN("nope")
	require.Error(t, err)
}

// The coverage follows the timeline path: the segment of the switch comes
// from the new timeline, a gap is no coverage, and a position past the
// point where its timeline was abandoned is refused.
func TestCoverFollowsTheTimelinePath(t *testing.T) {
	const seg = 16 << 20
	held := map[string]bool{
		"000000010000000000000002": true, "000000010000000000000003": true,
		"000000020000000000000004": true, "000000020000000000000005": true, "00000002.history": true,
	}
	have := func(n string) bool { return held[n] }
	history := func(tli uint32) ([]Switch, error) {
		return []Switch{{Timeline: 1, At: LSN(4*seg + 0x100)}}, nil
	}
	bases := []BaseInfo{{Label: "b1", Start: "0/2000028", Timeline: 1, SystemID: "42"}}

	cov, err := Cover(bases, "42", LSN(5*seg+0x10), 2, seg, have, history, nil)
	require.NoError(t, err)
	assert.Equal(t, []string{"000000010000000000000002", "000000010000000000000003", "000000020000000000000004", "000000020000000000000005"}, cov.Segments)
	assert.Equal(t, []string{"00000002.history"}, cov.History)

	// Another system's base never covers.
	_, err = Cover(bases, "43", LSN(5*seg+0x10), 2, seg, have, history, nil)
	require.ErrorIs(t, err, ErrNoCoverage)

	// A gap is no coverage.
	delete(held, "000000010000000000000003")
	_, err = Cover(bases, "42", LSN(5*seg+0x10), 2, seg, have, history, nil)
	require.ErrorIs(t, err, ErrNoCoverage)
	held["000000010000000000000003"] = true

	// Timeline 1 past its switch point is a lost tail.
	_, err = Cover(bases, "42", LSN(4*seg+0x200), 1, seg, have, history, []Switch{{Timeline: 1, At: LSN(4*seg + 0x100)}})
	require.ErrorContains(t, err, "abandoned")
}

func tarOf(t *testing.T, entries ...tar.Header) []byte {
	var buf bytes.Buffer
	tw := tar.NewWriter(&buf)
	for _, h := range entries {
		body := strings.Repeat("x", int(h.Size))
		if h.Name == "backup_label" {
			body = "START WAL LOCATION: 0/2000028 (file 000000010000000000000002)\nSTART TIMELINE: 1\n"
			h.Size = int64(len(body))
		}
		require.NoError(t, tw.WriteHeader(&h))
		_, err := tw.Write([]byte(body))
		require.NoError(t, err)
	}
	require.NoError(t, tw.Close())
	return buf.Bytes()
}

// A base backup is read, never extracted, on ingest; what a trusted
// restore would not extract is refused at once.
func TestScanBaseTarRefusesWhatARestoreWouldNotExtract(t *testing.T) {
	ok := []tar.Header{{Name: "backup_label", Typeflag: tar.TypeReg, Mode: 0o600}, {Name: "global/pg_control", Typeflag: tar.TypeReg, Size: 16, Mode: 0o600}}
	info, err := ScanBaseTar(bytes.NewReader(tarOf(t, ok...)))
	require.NoError(t, err)
	assert.Equal(t, "0/2000028", info.Start)
	assert.EqualValues(t, 1, info.Timeline)

	for name, extra := range map[string]tar.Header{
		"tablespace": {Name: "pg_tblspc/16384", Typeflag: tar.TypeSymlink, Linkname: "/mnt/fast"},
		"escape":     {Name: "../../etc/passwd", Typeflag: tar.TypeReg},
		"absolute":   {Name: "/etc/passwd", Typeflag: tar.TypeReg},
		"device":     {Name: "dev", Typeflag: tar.TypeChar},
		"hard link":  {Name: "base/1", Typeflag: tar.TypeLink, Linkname: "global/pg_control"},
	} {
		_, err := ScanBaseTar(bytes.NewReader(tarOf(t, append(append([]tar.Header{}, ok...), extra)...)))
		require.Error(t, err, name)
	}
	_, err = ScanBaseTar(bytes.NewReader(tarOf(t, ok[0])))
	require.ErrorContains(t, err, "pg_control")
}
