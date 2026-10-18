// Copyright 2026 The Forgejo Authors. All rights reserved.
// SPDX-License-Identifier: GPL-3.0-or-later

package observer

import (
	"archive/tar"
	"bufio"
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"path"
	"regexp"
	"sort"
	"strconv"
	"strings"

	"forgejo.org/modules/json"
)

// PostgreSQL as the observer keeps it: base backups and the WAL archive as
// files, never booted on ingest (observer plan v3 D7, v4 PostgreSQL). A
// checkpoint is complete only when a base backup that starts before its
// position and the WAL from that start to the position, on the timeline
// path to the checkpoint's timeline, are all here.

// LSN is a WAL position.
type LSN uint64

// ParseLSN reads the X/Y form PostgreSQL prints.
func ParseLSN(s string) (LSN, error) {
	hi, lo, ok := strings.Cut(s, "/")
	if !ok {
		return 0, fmt.Errorf("WAL position %q", s)
	}
	h, err1 := strconv.ParseUint(hi, 16, 32)
	l, err2 := strconv.ParseUint(lo, 16, 32)
	if err1 != nil || err2 != nil {
		return 0, fmt.Errorf("WAL position %q", s)
	}
	return LSN(h<<32 | l), nil
}

func (l LSN) String() string { return fmt.Sprintf("%X/%X", uint64(l)>>32, uint64(l)&0xffffffff) }

// SegmentName names the WAL segment of a timeline holding position l.
func SegmentName(tli uint32, l LSN, segSize int64) string {
	segno := uint64(l) / uint64(segSize)
	perID := uint64(0x100000000) / uint64(segSize)
	return fmt.Sprintf("%08X%08X%08X", tli, segno/perID, segno%perID)
}

var segmentPattern = regexp.MustCompile(`^([0-9A-F]{8})([0-9A-F]{8})([0-9A-F]{8})$`)

// parseSegment reads a segment's timeline and number.
func parseSegment(name string, segSize int64) (uint32, uint64, bool) {
	m := segmentPattern.FindStringSubmatch(name)
	if m == nil {
		return 0, 0, false
	}
	tli, _ := strconv.ParseUint(m[1], 16, 32)
	hi, _ := strconv.ParseUint(m[2], 16, 32)
	lo, _ := strconv.ParseUint(m[3], 16, 32)
	perID := uint64(0x100000000) / uint64(segSize)
	return uint32(tli), hi*perID + lo, true
}

// HistoryName names a timeline's history file.
func HistoryName(tli uint32) string { return fmt.Sprintf("%08X.history", tli) }

// Switch is one line of a timeline history: Timeline ended at At.
type Switch struct {
	Timeline uint32
	At       LSN
}

// ParseHistory reads a timeline history file: one line per ancestor, its
// timeline and the position where it ended, oldest first.
func ParseHistory(r io.Reader) ([]Switch, error) {
	var out []Switch
	sc := bufio.NewScanner(r)
	for sc.Scan() {
		line := strings.TrimSpace(sc.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		f := strings.Fields(line)
		if len(f) < 2 {
			return nil, fmt.Errorf("history line %q", line)
		}
		tli, err := strconv.ParseUint(f[0], 10, 32)
		if err != nil {
			return nil, fmt.Errorf("history line %q", line)
		}
		at, err := ParseLSN(f[1])
		if err != nil {
			return nil, err
		}
		if len(out) > 0 && (uint32(tli) <= out[len(out)-1].Timeline || at < out[len(out)-1].At) {
			return nil, errors.New("timeline history out of order")
		}
		out = append(out, Switch{Timeline: uint32(tli), At: at})
	}
	return out, sc.Err()
}

// TimelinePath is the timelines a target timeline descends through, with
// where each began: path[i] began at begin[i].
type TimelinePath struct {
	Timelines []uint32
	Begin     []LSN
}

// PathTo builds the path to target from its history (nil for timeline 1).
func PathTo(target uint32, history []Switch) (TimelinePath, error) {
	var p TimelinePath
	var begin LSN
	for _, s := range history {
		if s.Timeline >= target {
			return p, fmt.Errorf("the history of timeline %d names timeline %d", target, s.Timeline)
		}
		p.Timelines = append(p.Timelines, s.Timeline)
		p.Begin = append(p.Begin, begin)
		begin = s.At
	}
	p.Timelines = append(p.Timelines, target)
	p.Begin = append(p.Begin, begin)
	return p, nil
}

// timelineFor is the timeline of the path whose file holds the segment
// [start, end): the newest one begun before its end, since a promotion
// copies the segment of the switch onto the new timeline.
func (p TimelinePath) timelineFor(end LSN) uint32 {
	tli := p.Timelines[0]
	for i, t := range p.Timelines {
		if p.Begin[i] < end {
			tli = t
		}
	}
	return tli
}

// contains reports whether timeline tli of the path holds position l.
func (p TimelinePath) contains(tli uint32, l LSN) bool {
	for i, t := range p.Timelines {
		if t != tli {
			continue
		}
		if l < p.Begin[i] {
			return false
		}
		return i == len(p.Timelines)-1 || l < p.Begin[i+1]
	}
	return false
}

// BaseInfo describes a base backup the observer holds.
type BaseInfo struct {
	Label string `json:"label"`
	Start string `json:"start"`
	// End is where the backup becomes consistent, from its manifest's
	// WAL-Ranges: a recovery cannot stop before it. Empty for a backup
	// taken without a manifest, or ingested before it was read.
	End      string `json:"end,omitempty"`
	Timeline uint32 `json:"timeline"`
	SystemID string `json:"sysid"`
	Size     int64  `json:"size"`
	SHA256   string `json:"sha256"`
}

// Coverage is what restores a checkpoint's PostgreSQL: a base and the WAL
// files from its start to the checkpoint's position, histories included.
type Coverage struct {
	Base     BaseInfo `json:"base"`
	Segments []string `json:"segments"`
	History  []string `json:"history"`
}

// ErrNoCoverage says the base backups and the WAL held do not reach a
// checkpoint: a later checkpoint, or a new base backup, will.
var ErrNoCoverage = errors.New("no base backup and contiguous WAL reach the checkpoint")

// Cover finds the newest base whose WAL reaches target on timeline tli.
// have reports whether a WAL file is held, history reads a timeline's
// history. The position must not lie past the point where its timeline
// was abandoned: that tail was lost in a failover and is in no history.
func Cover(bases []BaseInfo, sysid string, target LSN, tli uint32, segSize int64, have func(string) bool, history func(uint32) ([]Switch, error), children []Switch) (Coverage, error) {
	for _, c := range children {
		if c.Timeline == tli && target > c.At {
			return Coverage{}, fmt.Errorf("the position %s is past %s, where timeline %d was abandoned", target, c.At, tli)
		}
	}
	var hist []Switch
	var histFiles []string
	if tli > 1 {
		h, err := history(tli)
		if err != nil {
			return Coverage{}, fmt.Errorf("the history of timeline %d: %w", tli, err)
		}
		hist = h
		for _, s := range h {
			if s.Timeline > 1 {
				histFiles = append(histFiles, HistoryName(s.Timeline))
			}
		}
		histFiles = append(histFiles, HistoryName(tli))
	}
	path, err := PathTo(tli, hist)
	if err != nil {
		return Coverage{}, err
	}
	sorted := append([]BaseInfo(nil), bases...)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i].Start > sorted[j].Start })
	lastErr := ErrNoCoverage
	for _, b := range sorted {
		start, err := ParseLSN(b.Start)
		if err != nil || b.SystemID != sysid || start > target || !path.contains(b.Timeline, start) {
			continue
		}
		if b.End != "" {
			// PostgreSQL refuses to stop a recovery before the backup's
			// end: a base still being copied at the position does not
			// restore it, an older one may.
			if end, err := ParseLSN(b.End); err != nil || end > target {
				continue
			}
		}
		var segs []string
		missing := ""
		seg := uint64(segSize)
		for n := uint64(start) / seg; n <= uint64(target)/seg; n++ {
			end := LSN((n + 1) * seg)
			name := SegmentName(path.timelineFor(end), LSN(n*seg), segSize)
			if !have(name) {
				missing = name
				break
			}
			segs = append(segs, name)
		}
		if missing != "" {
			lastErr = fmt.Errorf("%w: %s is missing after the base at %s", ErrNoCoverage, missing, b.Start)
			continue
		}
		for _, h := range histFiles {
			if !have(h) {
				return Coverage{}, fmt.Errorf("%w: %s is missing", ErrNoCoverage, h)
			}
		}
		return Coverage{Base: b, Segments: segs, History: histFiles}, nil
	}
	return Coverage{}, lastErr
}

// ---- base backups ----

// BaseBackupLimits bound what a base backup's tar may hold.
const maxTarEntryName = 4096

// ScanBaseTar reads a base backup's tar as pg_basebackup -Ft -D - writes
// it, refusing what a trusted restore would not extract: absolute or
// escaping names, links, devices, tablespaces. It returns the backup's
// start, timeline and system identifier.
func ScanBaseTar(r io.Reader) (BaseInfo, error) {
	var info BaseInfo
	var label, control bool
	tr := tar.NewReader(r)
	for {
		h, err := tr.Next()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return info, fmt.Errorf("base backup tar: %w", err)
		}
		name, err := safeTarName(h.Name)
		if err != nil {
			return info, err
		}
		switch h.Typeflag {
		case tar.TypeReg, tar.TypeDir:
		case tar.TypeSymlink:
			if strings.HasPrefix(name, "pg_tblspc/") {
				return info, errors.New("the base backup holds a tablespace; observers support pg_default only")
			}
			return info, fmt.Errorf("the base backup holds a link %s", name)
		default:
			return info, fmt.Errorf("the base backup holds %s of type %q", name, h.Typeflag)
		}
		switch name {
		case "backup_label":
			b, err := io.ReadAll(io.LimitReader(tr, 1<<16))
			if err != nil {
				return info, err
			}
			if info.Start, info.Timeline, err = parseBackupLabel(string(b)); err != nil {
				return info, err
			}
			label = true
		case "backup_manifest":
			// pg_basebackup -Ft -D - injects the manifest into the tar; its
			// WAL-Ranges, at the end, give where the backup is consistent.
			tail := &tailBuffer{max: 1 << 20}
			if _, err := io.Copy(tail, tr); err != nil {
				return info, err
			}
			if info.End, err = manifestEnd(tail.b); err != nil {
				return info, err
			}
		case "global/pg_control":
			var head [8]byte
			if _, err := io.ReadFull(tr, head[:]); err != nil {
				return info, fmt.Errorf("pg_control: %w", err)
			}
			// ControlFileData opens with the system identifier, in the
			// server's byte order: little-endian on amd64 and arm64.
			info.SystemID = strconv.FormatUint(binary.LittleEndian.Uint64(head[:]), 10)
			control = true
		}
	}
	if !label || !control {
		return info, errors.New("the base backup has no backup_label or no global/pg_control")
	}
	return info, nil
}

// tailBuffer keeps the last max bytes written to it.
type tailBuffer struct {
	b   []byte
	max int
}

func (t *tailBuffer) Write(p []byte) (int, error) {
	t.b = append(t.b, p...)
	if len(t.b) > t.max {
		t.b = append(t.b[:0], t.b[len(t.b)-t.max:]...)
	}
	return len(p), nil
}

// manifestEnd reads the highest End-LSN of a backup manifest's WAL-Ranges,
// from the manifest's tail.
func manifestEnd(tail []byte) (string, error) {
	_, rest, ok := bytes.CutLast(tail, []byte(`"WAL-Ranges"`))
	if !ok {
		return "", errors.New("the backup manifest has no WAL-Ranges")
	}
	open := bytes.IndexByte(rest, '[')
	end := bytes.IndexByte(rest, ']')
	if open < 0 || end < open {
		return "", errors.New("the backup manifest's WAL-Ranges is malformed")
	}
	var ranges []struct {
		End string `json:"End-LSN"`
	}
	if err := json.Unmarshal(rest[open:end+1], &ranges); err != nil {
		return "", fmt.Errorf("the backup manifest's WAL-Ranges: %w", err)
	}
	var max LSN
	for _, r := range ranges {
		l, err := ParseLSN(r.End)
		if err != nil {
			return "", fmt.Errorf("the backup manifest's End-LSN: %w", err)
		}
		if l > max {
			max = l
		}
	}
	if len(ranges) == 0 {
		return "", errors.New("the backup manifest's WAL-Ranges is empty")
	}
	return max.String(), nil
}

func safeTarName(n string) (string, error) {
	clean := path.Clean(strings.TrimPrefix(n, "./"))
	if len(n) > maxTarEntryName || n == "" || path.IsAbs(n) || clean == ".." || strings.HasPrefix(clean, "../") || strings.Contains(n, "\x00") {
		return "", fmt.Errorf("the base backup holds an unsafe name %q", n)
	}
	return clean, nil
}

func parseBackupLabel(s string) (string, uint32, error) {
	var start string
	var tli uint64
	for line := range strings.SplitSeq(s, "\n") {
		if v, ok := strings.CutPrefix(line, "START WAL LOCATION: "); ok {
			start, _, _ = strings.Cut(v, " ")
		}
		if v, ok := strings.CutPrefix(line, "START TIMELINE: "); ok {
			tli, _ = strconv.ParseUint(strings.TrimSpace(v), 10, 32)
		}
	}
	if _, err := ParseLSN(start); err != nil || tli == 0 {
		return "", 0, errors.New("backup_label has no start position or timeline")
	}
	return start, uint32(tli), nil
}
