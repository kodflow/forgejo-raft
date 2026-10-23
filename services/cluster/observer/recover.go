// Copyright 2026 The Forgejo Authors. All rights reserved.
// SPDX-License-Identifier: GPL-3.0-or-later

package observer

import (
	"archive/tar"
	"cmp"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"slices"
	"sort"
	"strings"
	"time"

	"forgejo.org/modules/json"
	"forgejo.org/services/cluster/reftable"

	"golang.org/x/crypto/nacl/box"
)

// Archive is an observer directory opened read-only, for a human choosing
// a recovery point and for `forgejo cluster recover --from-observer`: the
// live directory, or one of the hook's snapshots mounted read-only. It
// writes nothing there.
type Archive struct {
	store *store
	state *State
}

// OpenArchive opens an observer directory read-only.
func OpenArchive(dir, git string) (*Archive, error) {
	if git == "" {
		git = "git"
	}
	st := newStore(dir, git)
	state, err := st.loadState()
	if err != nil {
		return nil, err
	}
	return &Archive{store: st, state: state}, nil
}

// State returns the archive's state.
func (a *Archive) State() State { return *a.state }

// CheckpointInfo is one line of the list a human chooses from.
type CheckpointInfo struct {
	Name     string    `json:"name"`
	Index    uint64    `json:"index"`
	Time     time.Time `json:"time"`
	State    string    `json:"state"`
	Snapshot string    `json:"snapshot,omitempty"`
	LSN      string    `json:"lsn"`
	Timeline uint32    `json:"timeline"`
	// Problem says why the checkpoint does not restore from this
	// directory; empty when Check found everything.
	Problem string `json:"problem,omitempty"`
}

// List lists the published checkpoints, newest first, each checked.
func (a *Archive) List() []CheckpointInfo {
	var out []CheckpointInfo
	for _, name := range a.store.Published() {
		info := CheckpointInfo{Name: name, State: StateVerified}
		m, err := a.store.ReadManifest(name)
		if err != nil {
			info.Problem = err.Error()
			out = append(out, info)
			continue
		}
		info.Index, info.LSN, info.Timeline = m.Checkpoint.Index, m.Checkpoint.LSN, m.Checkpoint.Timeline
		info.Time = time.Unix(0, m.Checkpoint.Time).UTC()
		if p, ok := a.store.Protection(name); ok {
			info.State, info.Snapshot = StateProtected, p.Snapshot
		}
		if err := a.Check(context.Background(), name); err != nil {
			info.Problem = err.Error()
		}
		out = append(out, info)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name > out[j].Name })
	return out
}

// Manifest reads a checkpoint's manifest.
func (a *Archive) Manifest(name string) (*Manifest, error) { return a.store.ReadManifest(name) }

// Check verifies that everything a checkpoint restores from is here: its
// base backup by digest, its WAL files, its stored files, and its refs.
func (a *Archive) Check(ctx context.Context, name string) error {
	m, err := a.store.ReadManifest(name)
	if err != nil {
		return err
	}
	base := a.store.path("pg", "base", m.PG.Base.Label, "base.tar")
	if sum, err := fileSHA256(base); err != nil || sum != m.PG.Base.SHA256 {
		return fmt.Errorf("the base backup %s is missing or altered", m.PG.Base.Label)
	}
	for _, w := range append(append([]string(nil), m.PG.Segments...), m.PG.History...) {
		if !a.store.haveWAL(m.Checkpoint.SystemID, w) {
			return fmt.Errorf("the WAL file %s is missing", w)
		}
	}
	for _, f := range m.Files {
		if !a.store.haveBlob(f.SHA256) {
			return fmt.Errorf("the stored file %s/%s is missing", f.Storage, f.Path)
		}
	}
	for repo, rm := range m.Repos {
		if len(rm.Refs) == 0 {
			continue
		}
		refs, err := a.store.git.refs(ctx, a.store.git.dirFor(repo, rm.Format), name)
		if err != nil || len(refs) != len(rm.Refs) {
			return fmt.Errorf("the refs of %s are missing", repo)
		}
		for ref, sha := range rm.Refs {
			if refs[ref] != sha {
				return fmt.Errorf("%s %s does not hold the checkpoint's value", repo, ref)
			}
		}
	}
	return nil
}

func fileSHA256(path string) (string, error) {
	f, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer f.Close()
	h := sha256.New()
	if _, err := io.Copy(h, f); err != nil {
		return "", err
	}
	return hex.EncodeToString(h.Sum(nil)), nil
}

// Bundle is the secrets bundle, opened.
type Bundle struct {
	SecretKey string `json:"secret_key"`
	Time      int64  `json:"time"`
}

// OpenSecrets opens a checkpoint's secrets bundle with the observer's key.
func (a *Archive) OpenSecrets(name string, priv *[32]byte) (*Bundle, error) {
	sealed, err := os.ReadFile(a.store.path("checkpoints", name, "secrets.box"))
	if err != nil {
		return nil, err
	}
	pub := new([32]byte)
	curve25519Base(pub, priv)
	b, ok := box.OpenAnonymous(nil, sealed, pub, priv)
	if !ok {
		return nil, errors.New("the secrets bundle does not open with this key")
	}
	var out Bundle
	if err := json.Unmarshal(b, &out); err != nil {
		return nil, err
	}
	return &out, nil
}

// RestoreRepos writes every repository of a checkpoint under root, which
// must hold none of them yet. Run again after an interruption, it keeps a
// repository an earlier run wrote whole (the checkpoint's refs exactly, no
// mark left), writes again one it left marked incomplete, and refuses any
// other.
func (a *Archive) RestoreRepos(ctx context.Context, name, root string) (int, error) {
	m, err := a.store.ReadManifest(name)
	if err != nil {
		return 0, err
	}
	n := 0
	repos := make([]string, 0, len(m.Repos))
	for repo := range m.Repos {
		// The key becomes a path under root: one Apply would refuse is
		// never written, wherever the manifest came from (#81).
		if err := reftable.ValidRepoKey(repo); err != nil {
			return 0, fmt.Errorf("the checkpoint %s names the repository %q: %w", name, repo, err)
		}
		repos = append(repos, repo)
	}
	sort.Strings(repos)
	for _, repo := range repos {
		rm := m.Repos[repo]
		// A repository with no ref but its symbolic HEAD (created empty, or
		// every branch deleted) exists all the same: it is written empty.
		if len(rm.Refs) == 0 && len(rm.Symbolic) == 0 {
			continue
		}
		target := filepath.Join(root, filepath.FromSlash(repo))
		if entries, err := os.ReadDir(target); err == nil && len(entries) > 0 {
			if _, err := os.Stat(filepath.Join(target, IncompleteMark)); err == nil {
				if err := os.RemoveAll(target); err != nil {
					return n, fmt.Errorf("%s: %w", repo, err)
				}
			} else if a.store.git.restoredRefs(ctx, target, rm.Refs) {
				n++
				continue
			}
		}
		if err := a.store.git.Materialize(ctx, repo, name, target, rm.Format, len(rm.Refs) > 0, rm.Symbolic); err != nil {
			return n, fmt.Errorf("%s: %w", repo, err)
		}
		// git syncs its packs, not its refs nor its directories.
		if err := SyncTree(target); err != nil {
			return n, fmt.Errorf("%s: %w", repo, err)
		}
		if err := syncParents(target, root); err != nil {
			return n, fmt.Errorf("%s: %w", repo, err)
		}
		if err := os.Remove(filepath.Join(target, IncompleteMark)); err != nil {
			return n, fmt.Errorf("%s: %w", repo, err)
		}
		if err := syncDir(target); err != nil {
			return n, fmt.Errorf("%s: %w", repo, err)
		}
		n++
	}
	return n, nil
}

// RestoreFiles copies a checkpoint's stored files under their storages'
// roots, checked by digest; a file already there with other bytes stops it.
func (a *Archive) RestoreFiles(name string, roots map[string]string) (int, error) {
	m, err := a.store.ReadManifest(name)
	if err != nil {
		return 0, err
	}
	for i, f := range m.Files {
		root, ok := roots[f.Storage]
		if !ok {
			return i, fmt.Errorf("no local root for the %s storage", f.Storage)
		}
		clean, err := safeTarName(f.Path)
		if err != nil || clean != f.Path {
			return i, fmt.Errorf("unsafe stored path %q", f.Path)
		}
		dst := filepath.Join(root, filepath.FromSlash(clean))
		if sum, err := fileSHA256(dst); err == nil {
			if sum != f.SHA256 {
				return i, fmt.Errorf("%s exists with other bytes", dst)
			}
			continue
		}
		if err := os.MkdirAll(filepath.Dir(dst), 0o750); err != nil {
			return i, err
		}
		src, err := os.Open(a.store.blobPath(f.SHA256))
		if err != nil {
			return i, err
		}
		_, sum, err := writeStream(dst, src, f.Size, false)
		src.Close()
		if err != nil {
			return i, err
		}
		if sum != f.SHA256 {
			os.Remove(dst)
			return i, fmt.Errorf("%s/%s does not match its digest", f.Storage, f.Path)
		}
		// writeStream synced the file and its directory; the directories
		// created on the way to it are synced too.
		if err := syncParents(dst, root); err != nil {
			return i, err
		}
	}
	return len(m.Files), nil
}

// ---- PostgreSQL ----

// PGOptions are the recovering PostgreSQL's settings: the observer's
// trusted configuration, never the source's.
type PGOptions struct {
	Port      int
	SocketDir string
	// RestoreDir receives the checkpoint's WAL; restore_command reads
	// nothing else.
	RestoreDir string
	// HBA is the recovered PostgreSQL's pg_hba.conf, the operator's, never
	// the source's; local peer connections only by default.
	HBA string
	// Listen, ArchiveDir and ArchiveCommand are for the configuration the
	// recovered primary runs with.
	Listen         string
	ArchiveCommand string
}

func quoteOK(s string) error {
	if strings.ContainsAny(s, "'\"\\\n%") {
		return fmt.Errorf("%q holds a character a PostgreSQL setting cannot carry here", s)
	}
	return nil
}

// RecoveryConfig is postgresql.conf for the replay: a socket only, no
// archive, no hot standby, nothing preloaded, the WAL from the restore
// directory alone, and the checkpoint's target and timeline, explicit.
func RecoveryConfig(m *Manifest, opt PGOptions) (string, error) {
	for _, s := range []string{opt.SocketDir, opt.RestoreDir} {
		if err := quoteOK(s); err != nil {
			return "", err
		}
	}
	if _, err := ParseLSN(m.Checkpoint.LSN); err != nil {
		return "", err
	}
	return fmt.Sprintf(`# Written by forgejo cluster recover --from-observer for %s.
# The source's configuration is never used: only these settings.
listen_addresses = ''
port = %d
unix_socket_directories = '%s'
hot_standby = off
archive_mode = off
shared_preload_libraries = ''
max_wal_senders = 0
wal_level = replica
restore_command = 'cp "%s/%%f" "%%p"'
recovery_target_lsn = '%s'
recovery_target_timeline = '%d'
recovery_target_inclusive = off
recovery_target_action = 'promote'
`, m.Checkpoint.Name, opt.Port, opt.SocketDir, opt.RestoreDir, m.Checkpoint.LSN, m.Checkpoint.Timeline), nil
}

// PrimaryConfig is postgresql.conf for the recovered primary: a fresh
// archive, as on every member.
func PrimaryConfig(opt PGOptions) (string, error) {
	for _, s := range []string{opt.SocketDir, opt.Listen} {
		if err := quoteOK(s); err != nil {
			return "", err
		}
	}
	if strings.ContainsAny(opt.ArchiveCommand, "'\n") {
		return "", errors.New("the archive command holds a quote or a newline")
	}
	return fmt.Sprintf(`# Written by forgejo cluster recover --from-observer: the recovered
# primary, archiving afresh.
listen_addresses = '%s'
port = %d
unix_socket_directories = '%s'
wal_level = replica
archive_mode = on
archive_command = '%s'
archive_timeout = 30s
hot_standby = on
`, opt.Listen, opt.Port, opt.SocketDir, opt.ArchiveCommand), nil
}

// sourceConfig are what a base backup may carry that a trusted restore
// never keeps: configuration, includes, signals, a leftover pid.
var sourceConfig = []string{
	"postgresql.conf", "postgresql.auto.conf", "postgresql.base.conf", "pg_hba.conf", "pg_ident.conf",
	"recovery.conf", "recovery.signal", "standby.signal", "postmaster.pid", "postmaster.opts", "tablespace_map",
}

// RestorePG extracts a checkpoint's base backup into pgdata, an empty or
// absent directory, without the source's configuration; writes the trusted
// recovery configuration and the signal; and copies the checkpoint's WAL,
// decompressed, into the restore directory.
func (a *Archive) RestorePG(name, pgdata string, opt PGOptions) error {
	m, err := a.store.ReadManifest(name)
	if err != nil {
		return err
	}
	conf, err := RecoveryConfig(m, opt)
	if err != nil {
		return err
	}
	for _, d := range []string{pgdata, opt.RestoreDir} {
		if entries, err := os.ReadDir(d); err == nil && len(entries) > 0 {
			return fmt.Errorf("%s is not empty", d)
		}
		if err := os.MkdirAll(d, 0o700); err != nil {
			return err
		}
	}
	base := a.store.path("pg", "base", m.PG.Base.Label, "base.tar")
	if sum, err := fileSHA256(base); err != nil || sum != m.PG.Base.SHA256 {
		return fmt.Errorf("the base backup %s is missing or altered", m.PG.Base.Label)
	}
	if err := extractBase(base, pgdata); err != nil {
		return err
	}
	for _, f := range sourceConfig {
		if err := os.Remove(filepath.Join(pgdata, f)); err != nil && !errors.Is(err, os.ErrNotExist) {
			return err
		}
	}
	if err := os.RemoveAll(filepath.Join(pgdata, "conf.d")); err != nil {
		return err
	}
	for f, body := range map[string]string{
		"postgresql.conf":      conf,
		"postgresql.auto.conf": "# Do not edit: forgejo cluster recover --from-observer keeps it empty.\n",
		"pg_hba.conf":          cmp.Or(opt.HBA, "local all all peer\n"),
		"pg_ident.conf":        "",
		"recovery.signal":      "",
	} {
		if err := os.WriteFile(filepath.Join(pgdata, f), []byte(body), 0o600); err != nil {
			return err
		}
	}
	// Every timeline history the archive holds, not only those on the
	// checkpoint's path: the promotion takes the newest timeline it finds
	// plus one, and a number the old system used after the checkpoint is
	// never taken again, so its segments are never mistaken for ours.
	wals, err := a.store.histories(m.Checkpoint.SystemID)
	if err != nil {
		return err
	}
	for _, h := range m.PG.History {
		if !slices.Contains(wals, h) {
			wals = append(wals, h)
		}
	}
	for _, w := range append(wals, m.PG.Segments...) {
		r, err := a.store.OpenWAL(m.Checkpoint.SystemID, w)
		if err != nil {
			return fmt.Errorf("WAL %s: %w", w, err)
		}
		_, _, err = writeStream(filepath.Join(opt.RestoreDir, w), r, m.Checkpoint.WALSegmentSize, false)
		r.Close()
		if err != nil {
			return fmt.Errorf("WAL %s: %w", w, err)
		}
	}
	// The extracted base and its configuration are durable before the
	// recovery records this step: it never extracts them again.
	if err := SyncTree(pgdata); err != nil {
		return err
	}
	return SyncTree(opt.RestoreDir)
}

// extractBase extracts the regular files and directories of a base tar,
// owner-only, refusing anything else as ScanBaseTar does.
func extractBase(tarPath, dst string) error {
	f, err := os.Open(tarPath)
	if err != nil {
		return err
	}
	defer f.Close()
	tr := tar.NewReader(f)
	for {
		h, err := tr.Next()
		if errors.Is(err, io.EOF) {
			return nil
		}
		if err != nil {
			return err
		}
		name, err := safeTarName(h.Name)
		if err != nil {
			return err
		}
		path := filepath.Join(dst, filepath.FromSlash(name))
		switch h.Typeflag {
		case tar.TypeDir:
			if err := os.MkdirAll(path, 0o700); err != nil {
				return err
			}
		case tar.TypeReg:
			if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
				return err
			}
			out, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
			if err != nil {
				return err
			}
			_, err = io.Copy(out, io.LimitReader(tr, h.Size))
			if cerr := out.Close(); err == nil {
				err = cerr
			}
			if err != nil {
				return err
			}
		default:
			return fmt.Errorf("the base backup holds %s of type %q", name, h.Typeflag)
		}
	}
}
