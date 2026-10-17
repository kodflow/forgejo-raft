// Copyright 2026 The Forgejo Authors. All rights reserved.
// SPDX-License-Identifier: GPL-3.0-or-later

package cmd

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"time"

	auth_model "forgejo.org/models/auth"
	"forgejo.org/models/db"
	"forgejo.org/modules/clusterhook"
	"forgejo.org/modules/generate"
	"forgejo.org/modules/json"
	"forgejo.org/modules/setting"
	"forgejo.org/services/cluster"
	"forgejo.org/services/cluster/consensus"
	"forgejo.org/services/cluster/observer"

	"github.com/urfave/cli/v3"
)

// recoverFromObserverFlags are `forgejo cluster recover --from-observer`'s.
func recoverFromObserverFlags() []cli.Flag {
	return []cli.Flag{
		&cli.StringFlag{Name: "from-observer", Usage: "the checkpoint to recover, as `forgejo cluster observe list` names it; a human's choice"},
		&cli.StringFlag{Name: "observer-dir", Usage: "the observer directory or a snapshot of it, mounted read-only"},
		&cli.StringFlag{Name: "secrets-key", Usage: "the observer's secrets key file, which opens the checkpoint's SECRET_KEY"},
		&cli.StringFlag{Name: "new-observer-recipient", Usage: "name=hex of the observer's NEW secrets key: observers are rotated before anything is restored"},
		&cli.StringFlag{Name: "out", Usage: "where the recovery keeps its progress and the rotated secrets"},
		&cli.StringFlag{Name: "pgdata", Usage: "the recovered PostgreSQL's data directory, empty"},
		&cli.StringFlag{Name: "pg-restore-dir", Usage: "where the checkpoint's WAL is copied for restore_command (default: <out>/wal-restore)"},
		&cli.StringFlag{Name: "pg-socket-dir", Usage: "the recovered PostgreSQL's socket directory (default: <pgdata>)"},
		&cli.IntFlag{Name: "pg-port", Value: 5432},
		&cli.StringFlag{Name: "pg-listen", Value: "localhost", Usage: "listen_addresses of the recovered primary"},
		&cli.StringFlag{Name: "pg-hba-file", Usage: "the recovered PostgreSQL's pg_hba.conf, the operator's (default: local peer only)"},
		&cli.StringFlag{Name: "pg-ctl", Usage: "pg_ctl, run as PostgreSQL's owner; without it, the recovery stops for the operator to start PostgreSQL"},
		&cli.StringFlag{Name: "wal-archive-dir", Usage: "the recovered primary's fresh WAL archive (default: [cluster] WAL_ARCHIVE_DIR)"},
	}
}

// recoverProgress is what a recovery did, kept in <out>/progress.json so a
// stop for the operator resumes where it was.
type recoverProgress struct {
	Checkpoint string    `json:"checkpoint"`
	NewCluster string    `json:"new_cluster"`
	Done       []string  `json:"done"`
	Started    time.Time `json:"started"`
}

func (p *recoverProgress) done(step string) bool { return slices.Contains(p.Done, step) }

// The steps of plan v4's order, by name.
const (
	stepRotate   = "2-rotate"
	stepPG       = "3-postgresql"
	stepPGUp     = "3-postgresql-promoted"
	stepRefs     = "4-refs-objects-files"
	stepArchive  = "5-fresh-archive"
	stepVerify   = "6-verify"
	stepLineage  = "7-new-lineage"
	rotatedFile  = "rotated.ini"
	progressFile = "progress.json"
)

// errRecoverStopped is a stop for the operator: the message says what to
// do, and running the same command again resumes.
var errRecoverStopped = errors.New("the recovery stopped for the operator; run the same command again once done")

// runRecoverFromObserver recovers a cluster from an observer's checkpoint
// (observer plan v4, recover order): Forgejo stays fenced and offline; the
// secrets and the observers' keys are rotated first; PostgreSQL is replayed
// to the checkpoint's position with a trusted configuration; the refs,
// objects and files are restored at its index; a fresh archive is set; the
// import is verified; and only then does a new lineage start with this node
// as its only voter. The checkpoint's loss is stated at the end.
func runRecoverFromObserver(ctx context.Context, c *cli.Command) error {
	w := c.Root().Writer
	name := c.String("from-observer")
	for _, f := range []string{"observer-dir", "secrets-key", "new-observer-recipient", "out", "pgdata", "new-cluster"} {
		if c.String(f) == "" {
			return fmt.Errorf("--%s is required with --from-observer", f)
		}
	}
	d, err := clusterDriver()
	if err != nil {
		return err
	}
	cid, err := consensus.ParseClusterID(c.String("new-cluster"))
	if err != nil {
		return err
	}
	out := c.String("out")
	if err := os.MkdirAll(out, 0o700); err != nil {
		return err
	}
	prog := &recoverProgress{Checkpoint: name, NewCluster: cid.String(), Started: time.Now().UTC()}
	if b, err := os.ReadFile(filepath.Join(out, progressFile)); err == nil {
		if err := json.Unmarshal(b, prog); err != nil {
			return err
		}
		if prog.Checkpoint != name || prog.NewCluster != cid.String() {
			return fmt.Errorf("%s holds the recovery of %s into %s; use another --out", out, prog.Checkpoint, prog.NewCluster)
		}
	}
	save := func(step string) error {
		prog.Done = append(prog.Done, step)
		b, _ := json.MarshalIndent(prog, "", "  ")
		fmt.Fprintf(w, "done: %s\n", step)
		return os.WriteFile(filepath.Join(out, progressFile), b, 0o600)
	}

	archive, err := observer.OpenArchive(c.String("observer-dir"), setting.Git.Path)
	if err != nil {
		return err
	}
	m, err := archive.Manifest(name)
	if err != nil {
		return fmt.Errorf("checkpoint %s: %w", name, err)
	}
	if err := archive.Check(ctx, name); err != nil {
		return fmt.Errorf("checkpoint %s does not restore from this directory: %w", name, err)
	}

	// 1. Forgejo fenced and offline: no node answers on this socket, and
	// the cluster's data directory is new.
	if _, err := clusterhook.Call(ctx, setting.Cluster.HookSocket, &clusterhook.Request{Op: clusterhook.OpStatus}); err == nil {
		return errors.New("Forgejo is running on this node: stop it; it stays offline until the recovery ends")
	}

	// 2. Rotation first: new JWT secrets, the SECRET_KEY the database needs,
	// and the observers' new key; the configuration must carry them before
	// anything is restored.
	if !prog.done(stepRotate) {
		key, err := readSecretsKey(c.String("secrets-key"))
		if err != nil {
			return err
		}
		bundle, err := archive.OpenSecrets(name, key)
		if err != nil {
			return err
		}
		rot, err := rotatedSecrets(filepath.Join(out, rotatedFile), bundle.SecretKey, c.String("new-observer-recipient"))
		if err != nil {
			return err
		}
		jwt, err := generate.DecodeJwtSecret(rot["JWT_SECRET"])
		if err != nil {
			return fmt.Errorf("%s: %w", rotatedFile, err)
		}
		if setting.SecretKey != rot["SECRET_KEY"] || !bytes.Equal(setting.GetGeneralTokenSigningSecret(), jwt) || !recipientSet(c.String("new-observer-recipient")) {
			fmt.Fprintf(w, "The rotated secrets are in %s. Put them in this node's app.ini, then run the same command again.\n", filepath.Join(out, rotatedFile))
			return errRecoverStopped
		}
		if err := save(stepRotate); err != nil {
			return err
		}
	}

	// 3. PostgreSQL: the base, the WAL up to the checkpoint's position.
	socket := c.String("pg-socket-dir")
	if socket == "" {
		socket = c.String("pgdata")
	}
	restoreDir := c.String("pg-restore-dir")
	if restoreDir == "" {
		restoreDir = filepath.Join(out, "wal-restore")
	}
	opt := observer.PGOptions{Port: c.Int("pg-port"), SocketDir: socket, RestoreDir: restoreDir, Listen: c.String("pg-listen")}
	if f := c.String("pg-hba-file"); f != "" {
		b, err := os.ReadFile(f)
		if err != nil {
			return err
		}
		opt.HBA = string(b)
	}
	if !prog.done(stepPG) {
		if err := archive.RestorePG(name, c.String("pgdata"), opt); err != nil {
			return err
		}
		if err := save(stepPG); err != nil {
			return err
		}
	}
	if !prog.done(stepPGUp) {
		if ctl := c.String("pg-ctl"); ctl != "" {
			if err := runPGCtl(ctx, ctl, c.String("pgdata"), "start"); err != nil {
				return err
			}
		}
		if err := initDB(ctx); err != nil {
			fmt.Fprintf(w, "Start PostgreSQL on %s as its owner and let it replay to %s (timeline %d); this node's [database] must reach it. Then run the same command again.\n", c.String("pgdata"), m.Checkpoint.LSN, m.Checkpoint.Timeline)
			return fmt.Errorf("%w (%v)", errRecoverStopped, err)
		}
		if err := checkPromoted(ctx, m); err != nil {
			return err
		}
		if err := save(stepPGUp); err != nil {
			return err
		}
	} else if err := initDB(ctx); err != nil {
		return err
	}

	// 4. The refs, their objects and the stored files at the index.
	if !prog.done(stepRefs) {
		n, err := archive.RestoreRepos(ctx, name, setting.RepoRootPath)
		if err != nil {
			return err
		}
		roots := map[string]string{}
		for st, s := range map[string]*setting.Storage{cluster.StorageLFS: setting.LFS.Storage, cluster.StorageAttachments: setting.Attachment.Storage, cluster.StoragePackages: setting.Packages.Storage} {
			if s != nil && s.Type == setting.LocalStorageType {
				roots[st] = s.Path
			}
		}
		files, err := archive.RestoreFiles(name, roots)
		if err != nil {
			return err
		}
		fmt.Fprintf(w, "restored %d repositories and %d stored files\n", n, files)
		if err := save(stepRefs); err != nil {
			return err
		}
	}

	// 5. A fresh archive for the recovered primary.
	if !prog.done(stepArchive) {
		dir := c.String("wal-archive-dir")
		if dir == "" {
			dir = setting.Cluster.WALArchiveDir
		}
		if dir == "" {
			return errors.New("--wal-archive-dir or [cluster] WAL_ARCHIVE_DIR is required: the recovered primary archives afresh")
		}
		exe, _ := os.Executable()
		opt.ArchiveCommand = fmt.Sprintf("%s cluster wal-archive --dir %s %%p %%f", exe, dir)
		conf, err := observer.PrimaryConfig(opt)
		if err != nil {
			return err
		}
		if err := os.WriteFile(filepath.Join(out, "postgresql.recovered.conf"), []byte(conf), 0o600); err != nil {
			return err
		}
		if ctl := c.String("pg-ctl"); ctl != "" {
			if err := os.WriteFile(filepath.Join(c.String("pgdata"), "postgresql.conf"), []byte(conf), 0o600); err != nil {
				return err
			}
			if err := runPGCtl(ctx, ctl, c.String("pgdata"), "restart"); err != nil {
				return err
			}
		} else {
			fmt.Fprintf(w, "Install %s as %s/postgresql.conf and restart PostgreSQL: it archives afresh into %s.\n", filepath.Join(out, "postgresql.recovered.conf"), c.String("pgdata"), dir)
		}
		if err := save(stepArchive); err != nil {
			return err
		}
	}

	// 6. Verify the import: refs and SQL rows match, every LFS object is
	// here, every 2FA secret opens; then every token is revoked.
	if !prog.done(stepVerify) {
		if err := verifyRecovered(ctx, m, w); err != nil {
			return err
		}
		if err := save(stepVerify); err != nil {
			return err
		}
	}

	// 7. Only then the new lineage, this node its only voter.
	if !prog.done(stepLineage) {
		spki, err := certSPKI()
		if err != nil {
			return fmt.Errorf("the node's certificate, issued for the new cluster %s: %w", cid, err)
		}
		id, inc, err := d.Init(setting.Cluster.DataDir, cid, setting.Cluster.Slot)
		if err != nil {
			return err
		}
		self := consensus.Member{ID: id, Incarnation: inc, Slot: setting.Cluster.Slot, SPKI: spki, Address: setting.Cluster.ListenAddr, Voter: true}
		cfg := nodeConfig()
		cfg.MinVoters = 1
		if err := d.Bootstrap(ctx, cfg, []consensus.Member{self}); err != nil {
			return err
		}
		if err := save(stepLineage); err != nil {
			return err
		}
	}

	// 8. The loss, stated.
	fmt.Fprintf(w, "\n%s\n\nNext: start Forgejo on this node with the rotated secrets; run `forgejo cluster import` to put the restored refs in the new table, `forgejo doctor check --all --fix` and `forgejo cluster reconcile --apply` (pull request heads, branches); then add the other members, and re-key the observers with their new key.\n", m.Loss)
	return nil
}

// rotatedSecrets writes, once, the secrets the recovered node runs with,
// and reads them back on a later run.
func rotatedSecrets(path, secretKey, recipient string) (map[string]string, error) {
	if b, err := os.ReadFile(path); err == nil {
		out := map[string]string{}
		for line := range strings.SplitSeq(string(b), "\n") {
			k, v, ok := strings.Cut(line, " = ")
			if ok {
				out[strings.TrimSpace(k)] = strings.TrimSpace(v)
			}
		}
		return out, nil
	}
	_, lfs := generate.NewJwtSecret()
	_, oauth := generate.NewJwtSecret()
	body := fmt.Sprintf(`; Written by forgejo cluster recover --from-observer: rotated before anything was restored.
[security]
SECRET_KEY = %s

[server]
LFS_JWT_SECRET = %s

[oauth2]
JWT_SECRET = %s

[cluster]
OBSERVER_RECIPIENTS = %s
`, secretKey, lfs, oauth, recipient)
	if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
		return nil, err
	}
	return map[string]string{"SECRET_KEY": secretKey, "JWT_SECRET": oauth}, nil
}

func recipientSet(nameKey string) bool {
	name, key, _ := strings.Cut(nameKey, "=")
	return setting.Cluster.ObserverRecipients[name] == strings.ToLower(key)
}

func runPGCtl(ctx context.Context, ctl, pgdata, action string) error {
	cmd := exec.CommandContext(ctx, ctl, "-D", pgdata, "-w", "-t", "3600", "-l", filepath.Join(pgdata, "recover.log"), action)
	if out, err := cmd.CombinedOutput(); err != nil {
		return fmt.Errorf("pg_ctl %s: %w: %s", action, err, strings.TrimSpace(string(out)))
	}
	return nil
}

// checkPromoted verifies the replay before anything else is restored: the
// system is the checkpoint's, it left recovery, and promoted past the
// checkpoint's timeline, which PostgreSQL does only once the target is
// reached (an unreached target stops it instead).
func checkPromoted(ctx context.Context, m *observer.Manifest) error {
	rows, err := db.GetEngine(ctx).Query(`SELECT pg_is_in_recovery()::text AS rec,
		(SELECT system_identifier::text FROM pg_control_system()) AS sysid,
		(SELECT timeline_id::text FROM pg_control_checkpoint()) AS tli`)
	if err != nil {
		return err
	}
	if len(rows) != 1 {
		return errors.New("no answer from PostgreSQL")
	}
	r := rows[0]
	tli, _ := strconv.ParseUint(string(r["tli"]), 10, 32)
	switch {
	case string(r["rec"]) != "false":
		return fmt.Errorf("%w: PostgreSQL is still replaying; wait for its promotion", errRecoverStopped)
	case string(r["sysid"]) != m.Checkpoint.SystemID:
		return fmt.Errorf("PostgreSQL is system %s, not the checkpoint's %s", r["sysid"], m.Checkpoint.SystemID)
	case uint32(tli) <= m.Checkpoint.Timeline:
		return fmt.Errorf("PostgreSQL is on timeline %d: it did not promote past the checkpoint's %d", tli, m.Checkpoint.Timeline)
	}
	return nil
}

// verifyRecovered checks what the recovery restored, then revokes every
// token: a recovered forge trusts none it held.
func verifyRecovered(ctx context.Context, m *observer.Manifest, w interface{ Write([]byte) (int, error) }) error {
	e := db.GetEngine(ctx)
	rows, err := e.Query(`SELECT r.owner_name AS owner, r.lower_name AS repo, b.name AS branch, b.commit_id AS sha
		FROM branch b JOIN repository r ON r.id = b.repo_id WHERE b.is_deleted = ?`, false)
	if err != nil {
		return err
	}
	sql := map[string]string{}
	for _, r := range rows {
		sql[strings.ToLower(string(r["owner"]))+"/"+string(r["repo"])+".git refs/heads/"+string(r["branch"])] = string(r["sha"])
	}
	var mismatch []string
	refs := 0
	for repo, rm := range m.Repos {
		if strings.HasSuffix(repo, ".wiki.git") {
			continue
		}
		for ref, sha := range rm.Refs {
			if !strings.HasPrefix(ref, "refs/heads/") {
				continue
			}
			refs++
			key := repo + " " + ref
			if sql[key] != sha {
				mismatch = append(mismatch, fmt.Sprintf("%s: refs %s, SQL %q", key, sha, sql[key]))
			}
			delete(sql, key)
		}
	}
	for key := range sql {
		mismatch = append(mismatch, key+": in SQL, not in the refs")
	}
	if len(mismatch) > 0 {
		return fmt.Errorf("the refs and the SQL rows differ at the checkpoint:\n  %s", strings.Join(mismatch, "\n  "))
	}
	fmt.Fprintf(w, "verified: %d branches, refs and SQL rows match\n", refs)

	if setting.LFS.Storage != nil && setting.LFS.Storage.Type == setting.LocalStorageType {
		objs, err := e.Query("SELECT DISTINCT oid FROM lfs_meta_object")
		if err != nil {
			return err
		}
		for _, o := range objs {
			oid := string(o["oid"])
			if len(oid) < 5 {
				continue
			}
			if _, err := os.Stat(filepath.Join(setting.LFS.Storage.Path, oid[:2], oid[2:4], oid[4:])); err != nil {
				return fmt.Errorf("the LFS object %s is in SQL and not in the restored storage", oid)
			}
		}
		fmt.Fprintf(w, "verified: %d LFS objects present\n", len(objs))
	}

	var tfs []*auth_model.TwoFactor
	if err := e.Find(&tfs); err != nil {
		return err
	}
	for _, tf := range tfs {
		if _, err := tf.ValidateTOTP("000000"); err != nil {
			return fmt.Errorf("the 2FA secret of user %d does not open with the restored SECRET_KEY: %w", tf.UID, err)
		}
	}
	fmt.Fprintf(w, "verified: %d 2FA secrets open\n", len(tfs))

	for _, table := range []string{"access_token", "oauth2_grant", "oauth2_authorization_code", "session", "action_runner_token"} {
		if ok, err := e.IsTableExist(table); err != nil || !ok {
			continue
		}
		res, err := e.Exec("DELETE FROM " + table)
		if err != nil {
			return fmt.Errorf("revoking %s: %w", table, err)
		}
		n, _ := res.RowsAffected()
		fmt.Fprintf(w, "revoked: %d rows of %s\n", n, table)
	}
	return nil
}
