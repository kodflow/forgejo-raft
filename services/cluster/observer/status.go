// Copyright 2026 The Forgejo Authors. All rights reserved.
// SPDX-License-Identifier: GPL-3.0-or-later

package observer

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"forgejo.org/modules/json"
	"forgejo.org/modules/log"

	"golang.org/x/crypto/curve25519"
)

// StatePruned marks a protected checkpoint the live directory let go of:
// it lives on in the hook's snapshots.
const StatePruned = "PRUNED"

// Status is STATUS.json, what the observer host's monitoring reads.
// Freshness follows the newest PROTECTED checkpoint alone: a verified one
// whose snapshot failed restores nothing yet.
type Status struct {
	Name string    `json:"name"`
	At   time.Time `json:"at"`
	// StepStarted is when the running pass began; set, it is a heartbeat
	// written before the pass, and its age shows a pass that hangs.
	StepStarted     *time.Time `json:"step_started,omitempty"`
	Lineage         string     `json:"lineage"`
	Leader          string     `json:"leader"`
	Term            uint64     `json:"term"`
	Commit          uint64     `json:"commit"`
	LastProtected   string     `json:"last_protected,omitempty"`
	FreshnessSecond int64      `json:"freshness_seconds"`
	LastVerified    string     `json:"last_verified,omitempty"`
	HookFailing     string     `json:"hook_failing,omitempty"`
	Quarantine      string     `json:"quarantine,omitempty"`
	Rebootstrap     string     `json:"rebootstrap,omitempty"`
	StagingBytes    int64      `json:"staging_bytes"`
	FreeBytes       uint64     `json:"free_bytes"`
	LastError       string     `json:"last_error,omitempty"`
	Alerts          []string   `json:"alerts"`
}

// FreshnessAlert is the age of the newest protected checkpoint past which
// the observer alerts.
var FreshnessAlert = 15 * time.Minute

// Status computes the observer's status after a step that ended with err.
func (o *Observer) Status(err error) Status {
	st := o.state
	now := o.cfg.Now()
	s := Status{
		Name: o.cfg.Name, At: now.UTC(), Lineage: st.Lineage, Leader: st.Leader, Term: st.Term, Commit: st.Commit,
		HookFailing: st.HookFailing, Rebootstrap: st.Rebootstrap, StagingBytes: o.store.stagingSize(), Alerts: []string{},
	}
	if err != nil {
		s.LastError = err.Error()
	}
	if !o.stepStarted.IsZero() {
		t := o.stepStarted.UTC()
		s.StepStarted = &t
	}
	if a, ok := st.LastProtected(); ok {
		s.LastProtected = a.Name
		s.FreshnessSecond = int64(now.Sub(time.Unix(0, a.Time)).Seconds())
		if now.Sub(time.Unix(0, a.Time)) > FreshnessAlert {
			s.Alerts = append(s.Alerts, fmt.Sprintf("the newest protected checkpoint is %s old", now.Sub(time.Unix(0, a.Time)).Round(time.Second)))
		}
	} else {
		s.FreshnessSecond = -1
		s.Alerts = append(s.Alerts, "no protected checkpoint yet")
	}
	if a, ok := st.newest(); ok {
		s.LastVerified = a.Name
	}
	if q := st.Quarantine; q != nil {
		s.Quarantine = q.Reason
		s.Alerts = append(s.Alerts, "quarantined: "+q.Reason)
	}
	if st.HookFailing != "" {
		s.Alerts = append(s.Alerts, "the snapshot hook fails: "+st.HookFailing)
	}
	if free, ok := freeBytes(o.cfg.Dir); ok {
		s.FreeBytes = free
		if o.cfg.StagingQuota > 0 && free < uint64(2*o.cfg.StagingQuota) {
			s.Alerts = append(s.Alerts, fmt.Sprintf("%d bytes free, under twice the staging quota", free))
		}
	}
	if o.cfg.StagingQuota > 0 && s.StagingBytes >= o.cfg.StagingQuota {
		s.Alerts = append(s.Alerts, "the staging quota is reached; ingestion stopped")
	}
	return s
}

// writeStatus writes STATUS.json and ALERTS, and logs each alert.
func (o *Observer) writeStatus(err error) {
	s := o.Status(err)
	b, _ := json.MarshalIndent(s, "", "  ")
	if werr := writeAtomic(o.store.path("STATUS.json"), b, true); werr != nil {
		log.Error("cluster observer: writing its status: %v", werr)
	}
	_ = writeAtomic(o.store.path("ALERTS"), []byte(strings.Join(s.Alerts, "\n")+"\n"), true)
	for _, a := range s.Alerts {
		log.Error("cluster observer alert: %s", a)
	}
}

// prune lets the live directory go of protected checkpoints older than
// LiveKeep, never the newest protected one, then of the base backups, WAL
// and stored files no remaining checkpoint needs. Only the hook's snapshots
// keep versions: this never runs while the hook fails, which Step checks
// first.
func (o *Observer) prune(ctx context.Context) error {
	if o.cfg.LiveKeep <= 0 || o.state.HookFailing != "" {
		return nil
	}
	last, ok := o.state.LastProtected()
	if !ok {
		return nil
	}
	cut := o.cfg.Now().Add(-o.cfg.LiveKeep)
	changed := false
	for i := range o.state.Checkpoints {
		a := &o.state.Checkpoints[i]
		if a.State != StateProtected || a.Name == last.Name || !a.Verified.Before(cut) {
			continue
		}
		// The refs go before the directory: a manifest that is gone is an
		// earlier prune stopped after it, before the state was saved, and
		// what is left of the directory goes the same way (#61).
		m, err := o.store.ReadManifest(a.Name)
		if err != nil && !errors.Is(err, os.ErrNotExist) {
			return err
		}
		if m != nil {
			for repo, rm := range m.Repos {
				if err := o.store.git.dropRefs(ctx, o.store.git.dirFor(repo, rm.Format), a.Name); err != nil {
					return err
				}
			}
		}
		if err := os.RemoveAll(o.store.path("checkpoints", a.Name)); err != nil {
			return err
		}
		a.State = StatePruned
		changed = true
		// Each checkpoint let go is saved at once: a stop in the next one
		// never leaves this one marked protected without its directory.
		if err := o.store.saveState(o.state); err != nil {
			return err
		}
	}
	if !changed {
		return nil
	}
	return o.pruneData()
}

// pruneData removes the bases, segments and blobs no live checkpoint needs.
// Segments are compared within their system only: an initdb restarts
// their numbers (#60).
func (o *Observer) pruneData() error {
	bases, blobs := map[string]bool{}, map[string]bool{}
	minSeg := map[string]uint64{}
	keep := func(sysid, start string, segSize int64) {
		l, err := ParseLSN(start)
		if err != nil || segSize <= 0 {
			return
		}
		if n, ok := minSeg[sysid]; !ok || uint64(l)/uint64(segSize) < n {
			minSeg[sysid] = uint64(l) / uint64(segSize)
		}
	}
	var segSize int64
	for _, name := range o.store.Published() {
		m, err := o.store.ReadManifest(name)
		if err != nil {
			return err
		}
		bases[m.PG.Base.Label] = true
		for _, f := range m.Files {
			blobs[f.SHA256] = true
		}
		segSize = m.Checkpoint.WALSegmentSize
		keep(m.Checkpoint.SystemID, m.PG.Base.Start, segSize)
	}
	if segSize == 0 {
		return nil
	}
	var kept []BaseInfo
	newest := ""
	for _, b := range o.state.Bases {
		if b.Label > newest {
			newest = b.Label
		}
	}
	for _, b := range o.state.Bases {
		// The newest base stays: the next checkpoints start from it.
		if bases[b.Label] || b.Label == newest {
			kept = append(kept, b)
			continue
		}
		if err := os.RemoveAll(o.store.path("pg", "base", b.Label)); err != nil {
			return err
		}
	}
	o.state.Bases = kept
	for _, b := range kept {
		keep(b.SystemID, b.Start, segSize)
	}
	// The archive written flat before #60 held one system's segments.
	legacyMin := uint64(1<<64 - 1)
	for _, n := range minSeg {
		legacyMin = min(legacyMin, n)
	}
	entries, _ := os.ReadDir(o.store.path("pg", "wal"))
	for _, e := range entries {
		if e.IsDir() {
			if !sysidPattern.MatchString(e.Name()) {
				continue
			}
			n, ok := minSeg[e.Name()]
			if !ok {
				// No live checkpoint nor base is of this system.
				_ = os.RemoveAll(o.store.path("pg", "wal", e.Name()))
				continue
			}
			pruneSegments(o.store.path("pg", "wal", e.Name()), n, segSize)
		}
	}
	pruneSegments(o.store.path("pg", "wal"), legacyMin, segSize)
	dirs, _ := os.ReadDir(o.store.path("blobs"))
	for _, d := range dirs {
		files, _ := os.ReadDir(o.store.path("blobs", d.Name()))
		for _, f := range files {
			if !blobs[f.Name()] {
				_ = os.Remove(o.store.path("blobs", d.Name(), f.Name()))
			}
		}
	}
	return o.store.saveState(o.state)
}

// pruneSegments removes the segments of dir numbered below minSeg.
func pruneSegments(dir string, minSeg uint64, segSize int64) {
	entries, _ := os.ReadDir(dir)
	for _, e := range entries {
		name, ok := strings.CutSuffix(e.Name(), ".gz")
		if !ok || !e.Type().IsRegular() {
			continue
		}
		if _, segno, ok := parseSegment(name, segSize); ok && segno < minSeg {
			_ = os.Remove(filepath.Join(dir, e.Name()))
		}
	}
}

// curve25519Base derives an X25519 public key from its private key.
func curve25519Base(pub, priv *[32]byte) {
	b, err := curve25519.X25519(priv[:], curve25519.Basepoint)
	if err == nil {
		copy(pub[:], b)
	}
}
