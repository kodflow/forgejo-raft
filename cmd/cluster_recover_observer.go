// Copyright 2026 The Forgejo Authors. All rights reserved.
// SPDX-License-Identifier: GPL-3.0-or-later

package cmd

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"maps"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"slices"
	"sort"
	"strconv"
	"strings"
	"time"

	auth_model "forgejo.org/models/auth"
	cluster_model "forgejo.org/models/cluster"
	"forgejo.org/models/db"
	gitea_migrations_model "forgejo.org/models/gitea_migrations"
	repo_model "forgejo.org/models/repo"
	"forgejo.org/modules/clusterhook"
	"forgejo.org/modules/generate"
	"forgejo.org/modules/git"
	"forgejo.org/modules/gitrepo"
	"forgejo.org/modules/json"
	"forgejo.org/modules/jwtx"
	repo_module "forgejo.org/modules/repository"
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
		&cli.BoolFlag{Name: "accept-orphans", Usage: "step 6 restores as orphans the repositories the checkpoint holds and SQL has no row for, also when SQL holds repositories the checkpoint does not, as one half of a rename or a transfer cut in two would; the operator's decision, see observer.md"},
	}
}

// recoverProgress is what a recovery did, kept in <out>/progress.json so a
// stop for the operator resumes where it was.
type recoverProgress struct {
	Checkpoint string   `json:"checkpoint"`
	NewCluster string   `json:"new_cluster"`
	Done       []string `json:"done"`
	// Begun are the steps that started writing into a target they found
	// empty: an interrupted one empties it again before it is replayed.
	Begun []string `json:"begun,omitempty"`
	// BegunTargets are the directories each begun step found empty, the
	// only ones a resumed run may empty again.
	BegunTargets map[string][]string `json:"begun_targets,omitempty"`
	Started      time.Time           `json:"started"`
}

func (p *recoverProgress) done(step string) bool { return slices.Contains(p.Done, step) }

func (p *recoverProgress) begun(step string) bool { return slices.Contains(p.Begun, step) }

// The steps of plan v4's order, by name.
const (
	stepRotate   = "2-rotate"
	stepPG       = "3-postgresql"
	stepPGUp     = "3-postgresql-promoted"
	stepRefs     = "4-refs-objects-files"
	stepArchive  = "5-fresh-archive"
	stepVerify   = "6-verify"
	stepAdvance  = "6b-advance-identifiers"
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
	// A step is recorded only after what it restored is durable, and the
	// record itself replaces the previous one durably: a run resumed after
	// a power loss skips exactly the steps whose output survived.
	save := func(step string) error {
		prog.Done = append(prog.Done, step)
		b, _ := json.MarshalIndent(prog, "", "  ")
		fmt.Fprintf(w, "done: %s\n", step)
		return observer.WriteFileDurable(filepath.Join(out, progressFile), b)
	}
	begin := func(step string, targets []string) error {
		prog.Begun = append(prog.Begun, step)
		if prog.BegunTargets == nil {
			prog.BegunTargets = map[string][]string{}
		}
		prog.BegunTargets[step] = targets
		b, _ := json.MarshalIndent(prog, "", "  ")
		return observer.WriteFileDurable(filepath.Join(out, progressFile), b)
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
	// The old cluster is fenced and silent, as for `recover`: both
	// witnesses recorded the supersession for every voter the checkpoint
	// names, so an old member that comes back stops at its boot; and no
	// old member answers now.
	if err := checkObserverFence(ctx, c, m, cid); err != nil {
		return err
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
		pending, vars, err := rotationGate(rot, c.String("new-observer-recipient"), deploymentEnviron())
		if err != nil {
			return err
		}
		if len(pending) > 0 {
			fmt.Fprintf(w, "The rotated secrets are in %s. Put them in this node's app.ini, then run the same command again. Not carried yet:\n  %s\n", filepath.Join(out, rotatedFile), strings.Join(pending, "\n  "))
			if len(vars) > 0 {
				fmt.Fprintf(w, "This process's environment sets some of them, as the deployment does at every start of Forgejo (the image's setup writes each FORGEJO__* and GITEA__* variable into app.ini): give %s rotated.ini's values where the deployment keeps them (its .env, Kubernetes Secret or Nomad variable), not in app.ini alone (contrib/cluster/observer.md).\n", strings.Join(vars, ", "))
			}
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
		// The data directory and the restore directory are emptied again
		// only when this recovery found them empty and began writing them.
		resumed, err := preparePGTargets(prog, []string{c.String("pgdata"), restoreDir}, begin)
		if err != nil {
			return err
		}
		if resumed != nil {
			fmt.Fprintf(w, "resuming: %s emptied of what the interrupted run wrote\n", strings.Join(resumed, " and "))
		}
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
		// pg_ctl -w returns while PostgreSQL still replays, and without hot
		// standby it takes no connection until it reached the target and
		// promoted.
		if err := waitAccepting(ctx, func() error { _, err := db.GetEngine(ctx).Exec("SELECT 1"); return err }, time.Second, replayWait); err != nil {
			return err
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
		// The promotion wrote the new timeline's history with archiving
		// off: PostgreSQL never hands it to archive_command, and without
		// it the observer covers no checkpoint of the new timeline.
		if err := archiveHistories(c.String("pgdata"), dir, m.Checkpoint.Timeline); err != nil {
			return err
		}
		exe, _ := os.Executable()
		opt.ArchiveCommand = fmt.Sprintf("%s cluster wal-archive --dir %s %%p %%f", exe, dir)
		conf, err := observer.PrimaryConfig(opt)
		if err != nil {
			return err
		}
		if err := observer.WriteFileDurable(filepath.Join(out, "postgresql.recovered.conf"), []byte(conf)); err != nil {
			return err
		}
		if ctl := c.String("pg-ctl"); ctl != "" {
			if err := observer.WriteFileDurable(filepath.Join(c.String("pgdata"), "postgresql.conf"), []byte(conf)); err != nil {
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

	// 6. Verify the import: refs and SQL rows match, the rows that derive
	// from the refs derived again, every LFS object is here, every 2FA
	// secret opens; then every token is revoked.
	if !prog.done(stepVerify) {
		// Deriving branch rows reads the restored repositories with git.
		if err := git.InitSimple(ctx); err != nil {
			return err
		}
		if err := verifyRecovered(ctx, m, w, c.Bool("accept-orphans")); err != nil {
			return err
		}
		if err := save(stepVerify); err != nil {
			return err
		}
	}

	// 6b. Every ID and every repository's numbers handed out after the
	// checkpoint are held elsewhere (mails, webhooks, runners, other
	// forges): the sequences and the numbering move past them, once.
	if !prog.done(stepAdvance) {
		// The schema first: a binary newer than the checkpoint's schema
		// creates tables, and their sequences, on its first start; advanced
		// before they exist, they would hand out again IDs the old cluster
		// handed out after the checkpoint.
		if err := migrateThenAdvance(ctx, migrateRecovered, func(ctx context.Context) error {
			_, err := cluster_model.AdvanceIdentifiers(ctx, recoveryMarker(name, cid.String()), func(f string, a ...any) {
				fmt.Fprintf(w, f+"\n", a...)
			})
			return err
		}); err != nil {
			return err
		}
		if err := save(stepAdvance); err != nil {
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
	fmt.Fprintf(w, "\n%s\n\nNext: start Forgejo on this node with the rotated secrets; run `forgejo cluster import` to put the restored refs in the new table, `forgejo doctor check --all --fix` and `forgejo cluster reconcile --apply` (pull request heads, branches; it names the orphans step 6 restored, to adopt or to delete); then add the other members, and re-key the observers with their new key.\n", m.Loss)
	return nil
}

// checkObserverFence refuses a recovery from an observer while the old
// cluster is not fenced by both witnesses for every voter the checkpoint
// names, or while an old member answers.
func checkObserverFence(ctx context.Context, c *cli.Command, m *observer.Manifest, cid consensus.ClusterID) error {
	lineage, _, _ := strings.Cut(m.Checkpoint.Lineage, "/")
	old, err := consensus.ParseClusterID(lineage)
	if err != nil {
		return fmt.Errorf("the checkpoint's lineage %q: %w", m.Checkpoint.Lineage, err)
	}
	if old == cid {
		return errors.New("RECOVER_REFUSED: --new-cluster is the checkpoint's own cluster id")
	}
	fenced, err := parseFenced(c.StringSlice("fenced"))
	if err != nil {
		return err
	}
	if err := fenceCovers(m.Voters, fenced); err != nil {
		return err
	}
	if _, err := verifyWitnessFence(c, old, cid, fenced); err != nil {
		return err
	}
	return checkOldMembersSilent(ctx, c, old, cid)
}

// fenceCovers checks that every voter the checkpoint names has a fence. A
// checkpoint taken before the voters were recorded names none: at least
// one fence is then required, and the operator answers for the others.
func fenceCovers(voters []string, fenced []consensus.FencedVoter) error {
	if len(fenced) == 0 {
		return errors.New("RECOVER_REFUSED: --fenced id:method:receipt-file[+receipt-file] is required for every voter of the old cluster")
	}
	have := map[consensus.NodeID]bool{}
	for _, f := range fenced {
		have[f.Voter] = true
	}
	for _, v := range voters {
		id, err := cluster.ParseNodeID(v)
		if err != nil {
			return fmt.Errorf("the checkpoint's voter %q: %w", v, err)
		}
		if !have[id] {
			return fmt.Errorf("RECOVER_REFUSED: voter %s of the old cluster has no --fenced receipt", id)
		}
	}
	return nil
}

// migrateRecovered brings the restored schema to this binary's.
var migrateRecovered = func(ctx context.Context) error {
	return db.InitEngineWithMigration(ctx, func(e db.Engine) error {
		master, err := db.GetMasterEngine(e)
		if err != nil {
			return err
		}
		return gitea_migrations_model.Migrate(master)
	})
}

// migrateThenAdvance migrates the schema, then advances the identifiers:
// the sequences a migration creates are advanced with the others.
func migrateThenAdvance(ctx context.Context, migrate, advance func(context.Context) error) error {
	if err := migrate(ctx); err != nil {
		return fmt.Errorf("migrating the restored schema before advancing the identifiers: %w", err)
	}
	if err := advance(ctx); err != nil {
		return fmt.Errorf("advancing the identifiers past the checkpoint: %w", err)
	}
	return nil
}

// recoveryMarker names one recovery, a checkpoint into a new cluster, in
// the 64 characters a cursor's lineage holds.
func recoveryMarker(checkpoint, newCluster string) string {
	sum := sha256.Sum256([]byte(checkpoint + "\x00" + newCluster))
	return hex.EncodeToString(sum[:])
}

// rotatedSecrets writes, once, the secrets the recovered node runs with,
// and reads them back on a later run. The first run returns what a later
// one reads: every key the file holds, the gate of step 2 checks each.
func rotatedSecrets(path, secretKey, recipient string) (map[string]string, error) {
	if b, err := os.ReadFile(path); err == nil {
		return parseRotated(b), nil
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
	if err := observer.WriteFileDurable(path, []byte(body)); err != nil {
		return nil, err
	}
	return parseRotated([]byte(body)), nil
}

// parseRotated reads rotated.ini's keys, whatever their section: no two
// sections share one.
func parseRotated(b []byte) map[string]string {
	out := map[string]string{}
	for line := range strings.SplitSeq(string(b), "\n") {
		k, v, ok := strings.Cut(line, " = ")
		if ok {
			out[strings.TrimSpace(k)] = strings.TrimSpace(v)
		}
	}
	return out
}

// startEnviron is this process's environment as it started: an installed
// instance clears its FORGEJO__* and GITEA__* variables while it reads its
// configuration (setting.ClearEnvConfigKeys), before step 2 runs.
var startEnviron = os.Environ()

// deploymentEnviron is the environment step 2 takes for the one Forgejo
// starts with on this node: this process's, as it started then as it is.
func deploymentEnviron() []string {
	return append(slices.Clone(startEnviron), os.Environ()...)
}

// rotationPending names what of rotated.ini this node's configuration does
// not carry yet, as the server loads it, or the deployment's environment
// sets back: step 2 stops until it carries every secret, the old cluster's
// being the compromised ones (#284, #305). recipient is
// --new-observer-recipient.
func rotationPending(rot map[string]string, recipient string) ([]string, error) {
	pending, _, err := rotationGate(rot, recipient, deploymentEnviron())
	return pending, err
}

// rotationGate is step 2's gate: what this node's configuration does not
// carry yet, then what environ sets to another value, and the variables
// that do. A key the configuration lacks is named once.
func rotationGate(rot map[string]string, recipient string, environ []string) (pending, vars []string, err error) {
	pending, err = configPending(rot, recipient)
	if err != nil {
		return nil, nil, err
	}
	fromEnv, vars, err := environmentPending(rot, recipient, environ)
	if err != nil {
		return nil, nil, err
	}
	key := func(p string) string { k, _, _ := strings.Cut(p, " ("); return k }
	for _, p := range fromEnv {
		if !slices.ContainsFunc(pending, func(q string) bool { return key(q) == key(p) }) {
			pending = append(pending, p)
		}
	}
	return pending, vars, nil
}

// configPending names what of rotated.ini this node's configuration does
// not carry yet, as the server loads it.
func configPending(rot map[string]string, recipient string) ([]string, error) {
	var pending []string
	if setting.SecretKey != rot["SECRET_KEY"] {
		pending = append(pending, "[security] SECRET_KEY")
	}
	jwt, err := generate.DecodeJwtSecret(rot["JWT_SECRET"])
	if err != nil {
		return nil, fmt.Errorf("%s: JWT_SECRET: %w", rotatedFile, err)
	}
	// The general signing secret: the registry's, the Actions' and, with
	// an HS* algorithm, OAuth2's tokens.
	if !bytes.Equal(setting.GetGeneralTokenSigningSecret(), jwt) {
		pending = append(pending, "[oauth2] JWT_SECRET")
	}
	lfs, err := generate.DecodeJwtSecret(rot["LFS_JWT_SECRET"])
	if err != nil {
		return nil, fmt.Errorf("%s: LFS_JWT_SECRET: %w", rotatedFile, err)
	}
	why, err := lfsSecretPending(lfs)
	if err != nil {
		return nil, err
	}
	if why != "" {
		pending = append(pending, why)
	}
	if !recipientIn(setting.Cluster.ObserverRecipients, recipient) {
		pending = append(pending, "[cluster] OBSERVER_RECIPIENTS")
	}
	return pending, nil
}

// environmentPending names the settings of rotated.ini that environ sets
// to other values than rotated.ini's, and the variables that set them. The
// image's setup writes every FORGEJO__* and GITEA__* variable into app.ini
// at each start (environment-to-ini), as setting.EnvironmentToConfig reads
// them, __FILE and _URI included. The shipped deployments set SECRET_KEY,
// JWT_SECRET and LFS_JWT_SECRET so, from their store: app.ini carrying the
// rotated secrets holds until Forgejo starts, which puts the store's back,
// the old cluster's (#305). Only what this process's environment says is
// seen: run outside the deployment's, the command cannot see its store.
func environmentPending(rot map[string]string, recipient string, environ []string) (pending, vars []string, err error) {
	cfg, err := setting.NewConfigProviderFromData("")
	if err != nil {
		return nil, nil, err
	}
	setting.EnvironmentToConfig(cfg, environ)
	// The variables that set each section's keys, in environ's order.
	setBy := map[string][]string{}
	for _, kv := range environ {
		one, err := setting.NewConfigProviderFromData("")
		if err != nil {
			return nil, nil, err
		}
		if !setting.EnvironmentToConfig(one, []string{kv}) {
			continue
		}
		name, _, _ := strings.Cut(kv, "=")
		for _, sec := range one.Sections() {
			for _, k := range sec.Keys() {
				id := sec.Name() + "\x00" + k.Name()
				if !slices.Contains(setBy[id], name) {
					setBy[id] = append(setBy[id], name)
				}
			}
		}
	}
	sets := func(section string, keys ...string) []string {
		var out []string
		for _, k := range keys {
			out = append(out, setBy[section+"\x00"+k]...)
		}
		return out
	}
	name := func(p string, by []string) {
		pending = append(pending, p)
		for _, v := range by {
			if !slices.Contains(vars, v) {
				vars = append(vars, v)
			}
		}
	}
	// differs says whether a symmetric secret the environment configures,
	// in unpadded base64url, is not rotated.ini's; one the server could not
	// read is not either: it would draw another, or stop.
	differs := func(v string, err error, rotated []byte) bool {
		if err != nil {
			return true
		}
		b, err := generate.DecodeJwtSecret(v)
		return err != nil || !bytes.Equal(b, rotated)
	}
	jwt, err := generate.DecodeJwtSecret(rot["JWT_SECRET"])
	if err != nil {
		return nil, nil, fmt.Errorf("%s: JWT_SECRET: %w", rotatedFile, err)
	}
	lfs, err := generate.DecodeJwtSecret(rot["LFS_JWT_SECRET"])
	if err != nil {
		return nil, nil, fmt.Errorf("%s: LFS_JWT_SECRET: %w", rotatedFile, err)
	}

	if by := sets("security", "SECRET_KEY", "SECRET_KEY_URI"); len(by) > 0 {
		if v, err := setting.ConfiguredSecret(cfg.Section("security"), "SECRET_KEY_URI", "SECRET_KEY"); err != nil || v != rot["SECRET_KEY"] {
			name("[security] SECRET_KEY", by)
		}
	}
	if by := sets("oauth2", "JWT_SECRET", "JWT_SECRET_URI"); len(by) > 0 {
		if v, err := setting.ConfiguredSecret(cfg.Section("oauth2"), "JWT_SECRET_URI", "JWT_SECRET"); differs(v, err, jwt) {
			name("[oauth2] JWT_SECRET", by)
		}
	}
	if by := sets("server", "LFS_JWT_SECRET", "LFS_JWT_SECRET_URI"); len(by) > 0 {
		if v, err := setting.LFSJWTSecretConfigured(cfg); differs(v, err, lfs) {
			name("[server] LFS_JWT_SECRET", by)
		}
	}
	if by := sets("server", "LFS_JWT_SIGNING_ALGORITHM"); len(by) > 0 {
		if alg := cfg.Section("server").Key("LFS_JWT_SIGNING_ALGORITHM").String(); jwtx.IsValidAsymmetricAlgorithm(alg) {
			name(lfsAsymmetric(alg), by)
		}
	}
	if by := sets("cluster", "OBSERVER_RECIPIENTS"); len(by) > 0 {
		if !recipientIn(parseRecipients(cfg.Section("cluster").Key("OBSERVER_RECIPIENTS").Strings(",")), recipient) {
			name("[cluster] OBSERVER_RECIPIENTS", by)
		}
	}
	return pending, vars, nil
}

// lfsSecretPending says why this node's LFS signing key is not rotated's,
// "" when it is. A token it signs authenticates its user on its repository,
// to read and to write the LFS objects. The configuration carries the
// rotated secret, and a symmetric LFS_JWT_SIGNING_ALGORITHM, whether the
// server runs or not: turned on later, the server would sign with whatever
// it carries, and an asymmetric algorithm signs with a key file of
// APP_DATA_PATH, which no step rotates. Running, the server's loaded key
// must be the rotated secret.
func lfsSecretPending(rotated []byte) (string, error) {
	const name = "[server] LFS_JWT_SECRET"
	configured, err := setting.LFSJWTSecretConfigured(setting.CfgProvider)
	if err != nil {
		return "", fmt.Errorf("[server] %w", err)
	}
	if b, err := generate.DecodeJwtSecret(configured); err != nil || !bytes.Equal(b, rotated) {
		return name, nil
	}
	// As the server reads it (loadSigningKeyCfg), with the server off too;
	// unset is HS256.
	if alg := setting.CfgProvider.Section("server").Key("LFS_JWT_SIGNING_ALGORITHM").String(); jwtx.IsValidAsymmetricAlgorithm(alg) {
		return lfsAsymmetric(alg), nil
	}
	if !setting.LFS.StartServer {
		return "", nil
	}
	k := setting.LFS.SigningKey
	if k == nil {
		return name + " (the LFS server's key is not loaded)", nil
	}
	if !k.IsSymmetric() {
		return lfsAsymmetric(k.SigningMethod().Alg()), nil
	}
	if b, ok := k.VerifyKey().([]byte); !ok || !bytes.Equal(b, rotated) {
		return name, nil
	}
	return "", nil
}

// lfsAsymmetric names an asymmetric LFS_JWT_SIGNING_ALGORITHM as pending.
func lfsAsymmetric(alg string) string {
	return fmt.Sprintf("[server] LFS_JWT_SECRET (LFS_JWT_SIGNING_ALGORITHM %s signs with a key file the recovery does not rotate: recover with HS256, the default)", alg)
}

// parseRecipients reads OBSERVER_RECIPIENTS as the settings do.
func parseRecipients(entries []string) map[string]string {
	out := map[string]string{}
	for _, p := range entries {
		name, key, _ := strings.Cut(strings.TrimSpace(p), "=")
		out[strings.TrimSpace(name)] = strings.ToLower(strings.TrimSpace(key))
	}
	return out
}

// recipientIn says whether recipients hold nameKey, name=hex.
func recipientIn(recipients map[string]string, nameKey string) bool {
	name, key, _ := strings.Cut(nameKey, "=")
	got, ok := recipients[name]
	return ok && got == strings.ToLower(key)
}

// replayWait bounds the wait for PostgreSQL's replay, as pg_ctl's -t does.
const replayWait = time.Hour

// waitAccepting retries probe while PostgreSQL answers that it accepts no
// connection yet (SQLSTATE 57P03: starting, or replaying without hot
// standby), every interval, for bound at most. Any other answer ends it.
func waitAccepting(ctx context.Context, probe func() error, interval, bound time.Duration) error {
	deadline := time.Now().Add(bound)
	for {
		err := probe()
		if err == nil || !strings.Contains(err.Error(), "57P03") {
			return err
		}
		if !time.Now().Before(deadline) {
			return fmt.Errorf("PostgreSQL still accepts no connection after %s: %w", bound, err)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(interval):
		}
	}
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
// preparePGTargets readies the directories step 3 writes. A step not begun
// yet requires them empty and records them, durably, before it writes. A
// begun one empties again only the directories it recorded, and refuses
// others: a mistyped flag on the rerun must not wipe a directory the
// interrupted run never wrote. It returns the directories it emptied.
func preparePGTargets(prog *recoverProgress, targets []string, begin func(string, []string) error) ([]string, error) {
	step := stepPG
	abs := make([]string, 0, len(targets))
	for _, d := range targets {
		a, err := filepath.Abs(d)
		if err != nil {
			return nil, err
		}
		abs = append(abs, a)
	}
	if !prog.begun(step) {
		for _, d := range abs {
			entries, err := os.ReadDir(d)
			if err != nil && !errors.Is(err, os.ErrNotExist) {
				// Unreadable is not empty: recorded, it would be wiped by a
				// resumed run once readable again.
				return nil, fmt.Errorf("inspecting %s: %w", d, err)
			}
			if len(entries) > 0 {
				return nil, fmt.Errorf("%s is not empty", d)
			}
		}
		return nil, begin(step, abs)
	}
	recorded, ok := prog.BegunTargets[step]
	if !ok {
		return nil, fmt.Errorf("the interrupted step %s did not record its directories (an older binary): empty %s yourself if it wrote them, and remove %s from begun in %s", step, strings.Join(abs, " and "), step, progressFile)
	}
	if !slices.Equal(recorded, abs) {
		return nil, fmt.Errorf("the interrupted step %s wrote %s, not %s: run it again with the same directories", step, strings.Join(recorded, " and "), strings.Join(abs, " and "))
	}
	return abs, emptyDirs(abs...)
}

// emptyDirs removes what each directory holds, keeping the directory: a
// data directory may be a mount point.
func emptyDirs(dirs ...string) error {
	for _, d := range dirs {
		entries, err := os.ReadDir(d)
		if errors.Is(err, os.ErrNotExist) {
			continue
		}
		if err != nil {
			return err
		}
		for _, e := range entries {
			if err := os.RemoveAll(filepath.Join(d, e.Name())); err != nil {
				return err
			}
		}
	}
	return nil
}

// verifyRestoredRepos checks that every repository of the checkpoint,
// empty ones included, has its directory under root, and reports the SQL
// repository rows that have none: the forge marks those broken.
func verifyRestoredRepos(ctx context.Context, m *observer.Manifest, root string, w interface{ Write([]byte) (int, error) }) error {
	var missing []string
	for repo, rm := range m.Repos {
		if len(rm.Refs) == 0 && len(rm.Symbolic) == 0 {
			continue
		}
		if _, err := os.Stat(filepath.Join(root, filepath.FromSlash(repo), "HEAD")); err != nil {
			missing = append(missing, repo)
		}
	}
	if len(missing) > 0 {
		sort.Strings(missing)
		return fmt.Errorf("repositories of the checkpoint were not restored: %s", strings.Join(missing, ", "))
	}
	rows, err := db.GetEngine(ctx).Query(`SELECT owner_name AS owner, lower_name AS repo FROM repository`)
	if err != nil {
		return err
	}
	var rowsOnly []string
	for _, r := range rows {
		repo := strings.ToLower(string(r["owner"])) + "/" + string(r["repo"]) + ".git"
		if _, err := os.Stat(filepath.Join(root, filepath.FromSlash(repo), "HEAD")); err != nil {
			rowsOnly = append(rowsOnly, repo)
		}
	}
	sort.Strings(rowsOnly)
	for _, repo := range rowsOnly {
		fmt.Fprintf(w, "WARNING: %s has a repository row and no directory: the checkpoint does not hold it\n", repo)
	}
	fmt.Fprintf(w, "verified: %d repositories restored, empty ones included\n", len(rows)-len(rowsOnly))
	return nil
}

// historyName is a timeline history file in pg_wal.
var historyName = regexp.MustCompile(`^[0-9A-F]{8}\.history$`)

// archiveHistories copies every timeline history of pgdata's pg_wal into
// the fresh archive, then checks that it holds the history of a timeline
// past the checkpoint's: the one the promotion started.
func archiveHistories(pgdata, dir string, from uint32) error {
	walDir := filepath.Join(pgdata, "pg_wal")
	entries, err := os.ReadDir(walDir)
	if err != nil {
		return fmt.Errorf("reading the promoted timeline's history: %w", err)
	}
	newest := uint64(0)
	for _, e := range entries {
		if e.IsDir() || !historyName.MatchString(e.Name()) {
			continue
		}
		if err := cluster.ArchiveWAL(filepath.Join(walDir, e.Name()), e.Name(), dir); err != nil {
			return fmt.Errorf("archiving %s: %w", e.Name(), err)
		}
		tli, _ := strconv.ParseUint(strings.TrimSuffix(e.Name(), ".history"), 16, 32)
		newest = max(newest, tli)
	}
	if newest <= uint64(from) {
		return fmt.Errorf("%s holds no history past timeline %d: PostgreSQL did not promote", walDir, from)
	}
	if _, err := os.Stat(filepath.Join(dir, fmt.Sprintf("%08X.history", newest))); err != nil {
		return fmt.Errorf("the fresh archive does not hold the history of timeline %d: %w", newest, err)
	}
	return nil
}

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

// storedPresent checks that the file of every key SQL names is in the
// restored storage root, at rel(key); a key too short to name a file is
// passed over.
func storedPresent(what, root string, keys []string, rel func(string) string) (int, error) {
	n := 0
	for _, k := range keys {
		if len(k) < 5 {
			continue
		}
		if _, err := os.Stat(filepath.Join(root, filepath.FromSlash(rel(k)))); err != nil {
			return n, fmt.Errorf("the %s %s is in SQL and not in the restored storage", what, k)
		}
		n++
	}
	return n, nil
}

// branchMismatch compares the branch rows of SQL with the refs/heads of
// the checkpoint, and returns what differs, the repositories it differs
// in, and how many branches the refs hold.
func branchMismatch(ctx context.Context, m *observer.Manifest) (mismatch []string, repos map[string]bool, refs int, err error) {
	rows, err := db.GetEngine(ctx).Query(`SELECT r.owner_name AS owner, r.lower_name AS repo, b.name AS branch, b.commit_id AS sha
		FROM branch b JOIN repository r ON r.id = b.repo_id WHERE b.is_deleted = ?`, false)
	if err != nil {
		return nil, nil, 0, err
	}
	sql := map[string]string{}
	for _, r := range rows {
		sql[strings.ToLower(string(r["owner"]))+"/"+string(r["repo"])+".git refs/heads/"+string(r["branch"])] = string(r["sha"])
	}
	repos = map[string]bool{}
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
				repos[repo] = true
			}
			delete(sql, key)
		}
	}
	for key := range sql {
		mismatch = append(mismatch, key+": in SQL, not in the refs")
		repo, _, _ := strings.Cut(key, " ")
		repos[repo] = true
	}
	sort.Strings(mismatch)
	return mismatch, repos, refs, nil
}

// checkpointOrphans compares the repositories the checkpoint restores
// with SQL's. orphans are those SQL has no repository row for, wikis
// included (a wiki is its repository's): what the table of the old cluster
// held alive and SQL did not know, as `forgejo cluster reconcile` names it,
// left by a deletion or a creation the cluster did not record, or by a loss
// on the SQL side (contrib/cluster/README.md, "Recovery"). branched are the
// orphans holding a branch, which the comparison of the branch rows would
// refuse. unheld are the repositories SQL holds and the checkpoint does not
// restore.
func checkpointOrphans(ctx context.Context, m *observer.Manifest) (orphans, branched, unheld []string, err error) {
	rows, err := db.GetEngine(ctx).Query(`SELECT owner_name AS owner, lower_name AS repo FROM repository`)
	if err != nil {
		return nil, nil, nil, err
	}
	inSQL := make(map[string]bool, len(rows))
	for _, r := range rows {
		inSQL[strings.ToLower(string(r["owner"]))+"/"+string(r["repo"])+".git"] = true
	}
	held := map[string]bool{}
	for repo, rm := range m.Repos {
		if len(rm.Refs) == 0 && len(rm.Symbolic) == 0 {
			// Nothing of it is restored (verifyRestoredRepos).
			continue
		}
		base, wiki := strings.CutSuffix(repo, ".wiki.git")
		if wiki {
			base += ".git"
		} else {
			held[repo] = true
		}
		if inSQL[base] {
			continue
		}
		orphans = append(orphans, repo)
		if wiki {
			continue
		}
		for ref := range rm.Refs {
			if strings.HasPrefix(ref, "refs/heads/") {
				branched = append(branched, repo)
				break
			}
		}
	}
	for repo := range inSQL {
		if !held[repo] {
			unheld = append(unheld, repo)
		}
	}
	sort.Strings(orphans)
	sort.Strings(branched)
	sort.Strings(unheld)
	return orphans, branched, unheld, nil
}

// verifyBranches checks that the branch rows of SQL at L are the refs at
// B. Some rows are written long after the refs they describe, by no
// operation that holds the cut: a mirror's synchronisation writes them
// after its fetch and its LFS objects, a migration after its clone and its
// wiki, and the consumer derives none from a push it parked (#159). Those
// rows derive from the refs: the branches of a repository SQL holds and
// the checkpoint restored are synchronised with its restored refs, as the
// mirror or the migration would have done, and the comparison runs again.
//
// An orphan of the old cluster, a repository its table held alive and SQL
// did not know, is in every checkpoint taken while it lasts, and the
// observer protects them (#255): it is restored as the old cluster held
// it, its refs left out of the comparison, and named, to be adopted or
// deleted once the new cluster runs. It is told apart from one half of a
// rename or a transfer cut in two only when SQL holds no repository the
// checkpoint does not, as the other half would be: otherwise the step
// refuses, naming both, and only acceptOrphans, the operator's decision,
// restores them as orphans. The refusal offers it only when the orphans
// are all that differs.
//
// What does not derive from the refs is still refused: rows of a
// repository the checkpoint does not hold, which a deletion, a rename, a
// transfer or a creation cut in two leaves (#88), and the refs of a
// repository SQL does not hold while they cannot be told from the other
// half of such a cut. A deletion or a creation cut in two that leaves the
// refs alone leaves what a deletion or a creation the cluster did not
// record leaves, an orphan, and is restored as one.
func verifyBranches(ctx context.Context, m *observer.Manifest, w interface{ Write([]byte) (int, error) }, acceptOrphans bool) error {
	orphans, branched, unheld, err := checkpointOrphans(ctx, m)
	if err != nil {
		return err
	}
	accepted := len(branched) == 0 || len(unheld) == 0 || acceptOrphans
	withoutOrphans := &observer.Manifest{Repos: maps.Clone(m.Repos)}
	for _, repo := range orphans {
		delete(withoutOrphans.Repos, repo)
	}
	compared := m
	if accepted {
		compared = withoutOrphans
		for _, repo := range orphans {
			fmt.Fprintf(w, "WARNING: %s is an orphan: the checkpoint holds it and SQL has no repository row for it, as the old cluster held it (a deletion or a creation it did not record, a loss on the SQL side). It is restored; once the new cluster runs, `forgejo cluster reconcile` names it: adopt it (its owner recreated first if SQL lost it too), or delete it with `forgejo cluster reconcile --delete-orphan`\n", repo)
		}
		if len(branched) > 0 && len(unheld) > 0 {
			fmt.Fprintf(w, "WARNING: orphans accepted with --accept-orphans, SQL holding repositories the checkpoint does not: %s\n", strings.Join(unheld, ", "))
		}
	}
	mismatch, repos, refs, err := branchMismatch(ctx, compared)
	if err != nil {
		return err
	}
	if len(mismatch) > 0 {
		keys := make([]string, 0, len(repos))
		for repo := range repos {
			keys = append(keys, repo)
		}
		sort.Strings(keys)
		for _, key := range keys {
			done, err := syncRestoredBranches(ctx, key)
			if err != nil {
				return fmt.Errorf("deriving the branch rows of %s from its restored refs: %w", key, err)
			}
			if done {
				fmt.Fprintf(w, "reconciled: %s, its branch rows derived from its restored refs\n", key)
			}
		}
		if mismatch, _, refs, err = branchMismatch(ctx, compared); err != nil {
			return err
		}
	}
	if len(mismatch) > 0 {
		msg := "the refs and the SQL rows differ at the checkpoint:\n  " + strings.Join(mismatch, "\n  ")
		if !accepted {
			// --accept-orphans is offered only when the orphans are all
			// that differs: it never passes the rows of a repository the
			// checkpoint does not hold.
			rest, _, _, err := branchMismatch(ctx, withoutOrphans)
			if err != nil {
				return err
			}
			if len(rest) > 0 {
				return errors.New(msg)
			}
			msg += fmt.Sprintf("\n%s has no repository row in SQL: an orphan of the old cluster (a deletion or a creation it did not record, a loss on the SQL side), or one half of a rename or a transfer cut in two, since SQL holds repositories the checkpoint does not: %s. If they are orphans, as `forgejo cluster reconcile` named them on the old cluster, run the same command again with --accept-orphans", strings.Join(branched, ", "), strings.Join(unheld, ", "))
		}
		return errors.New(msg)
	}
	fmt.Fprintf(w, "verified: %d branches, refs and SQL rows match\n", refs)
	return nil
}

// syncRestoredBranches derives the branch rows of key, owner/name.git,
// from its restored refs, when SQL holds the repository and the checkpoint
// restored it; false when either is missing.
func syncRestoredBranches(ctx context.Context, key string) (bool, error) {
	owner, name, ok := strings.Cut(strings.TrimSuffix(key, ".git"), "/")
	if !ok {
		return false, nil
	}
	repo, err := repo_model.GetRepositoryByOwnerAndName(ctx, owner, name)
	if repo_model.IsErrRepoNotExist(err) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	if _, err := os.Stat(filepath.Join(repo.RepoPath(), "HEAD")); err != nil {
		return false, nil
	}
	gitRepo, err := gitrepo.OpenRepository(ctx, repo)
	if err != nil {
		return false, err
	}
	defer gitRepo.Close()
	if _, err := repo_module.SyncRepoBranchesWithRepo(ctx, repo, gitRepo, 0); err != nil {
		return false, err
	}
	return true, nil
}

// revokedTables are the tables step 6 empties, a table holding a foreign
// key to another one before it (TestRecoverRevokesReferencingTablesFirst):
// the access tokens and the repositories some are restricted to, the
// OAuth2 grants and codes, the sessions of the database provider, the
// runners' registration tokens, forgejo_auth_token, every purpose of it
// (#256): the long-term authentication cookies ("remember me", with a
// password or through an SSO source) and the codes mailed to reset a
// password or to activate an account or an address, and the invitations
// to a team mailed with a link, which any account holding it accepts.
var revokedTables = []string{"access_token_resource_repo", "access_token", "oauth2_grant", "oauth2_authorization_code", "session", "action_runner_token", "forgejo_auth_token", "team_invite"}

// verifyRecovered checks what the recovery restored, then revokes every
// token the old forge handed out to sign in or to be let in: a recovered
// forge trusts none it held. The credentials configured elsewhere, a
// registered runner's or an OAuth2 application's secret, are kept
// (observer.md, step 6).
func verifyRecovered(ctx context.Context, m *observer.Manifest, w interface{ Write([]byte) (int, error) }, acceptOrphans bool) error {
	e := db.GetEngine(ctx)
	if err := verifyBranches(ctx, m, w, acceptOrphans); err != nil {
		return err
	}
	if err := verifyRestoredRepos(ctx, m, setting.RepoRootPath, w); err != nil {
		return err
	}

	// Every stored file SQL names is in the restored storage: an LFS
	// object, and also an attachment (but a link to an external URL, which
	// has no file) and a package blob, whose absence
	// would otherwise pass in silence (#90).
	for _, sf := range []struct {
		what, query, column string
		storage             *setting.Storage
		rel                 func(string) string
	}{
		{"LFS object", "SELECT DISTINCT oid FROM lfs_meta_object", "oid", setting.LFS.Storage, func(k string) string { return k[:2] + "/" + k[2:4] + "/" + k[4:] }},
		{"attachment", "SELECT uuid FROM attachment WHERE external_url IS NULL OR external_url = ''", "uuid", setting.Attachment.Storage, func(k string) string { return k[:1] + "/" + k[1:2] + "/" + k }},
		{"package blob", "SELECT hash_sha256 FROM package_blob", "hash_sha256", setting.Packages.Storage, func(k string) string { return k[:2] + "/" + k[2:4] + "/" + k }},
	} {
		if sf.storage == nil || sf.storage.Type != setting.LocalStorageType {
			continue
		}
		rows, err := e.Query(sf.query)
		if err != nil {
			return err
		}
		keys := make([]string, 0, len(rows))
		for _, r := range rows {
			keys = append(keys, string(r[sf.column]))
		}
		n, err := storedPresent(sf.what, sf.storage.Path, keys, sf.rel)
		if err != nil {
			return err
		}
		fmt.Fprintf(w, "verified: %d %ss present\n", n, sf.what)
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

	for _, table := range revokedTables {
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
