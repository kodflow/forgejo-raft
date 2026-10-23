// Copyright 2026 The Forgejo Authors. All rights reserved.
// SPDX-License-Identifier: GPL-3.0-or-later

package cmd

import (
	"bufio"
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/hex"
	"encoding/pem"
	"errors"
	"fmt"
	"io"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"text/tabwriter"
	"time"

	"forgejo.org/modules/clusterhook"
	"forgejo.org/modules/json"
	"forgejo.org/modules/log"
	"forgejo.org/modules/setting"
	"forgejo.org/services/cluster"
	"forgejo.org/services/cluster/observer"

	"github.com/urfave/cli/v3"
	"golang.org/x/crypto/nacl/box"
	"golang.org/x/crypto/ssh"
)

// The observers' side of the command (observer plan v3, v4): export, the
// SSH forced command a member serves them; observe, the observer itself;
// wal-archive, the members' archive_command.

// logToStderr keeps stdout for an answer: the export's stream, the
// archive's silence.
func logToStderr(ctx context.Context, _ *cli.Command) (context.Context, error) {
	w := log.NewEventWriterConsole("console-default", log.WriterMode{Level: log.WARN, WriterOption: log.WriterConsoleOption{Stderr: true}})
	log.GetManager().GetLogger(log.DEFAULT).ReplaceAllWriters(w)
	return ctx, nil
}

func cmdClusterExport() *cli.Command {
	return &cli.Command{
		Name:   "export",
		Usage:  "An observer's SSH forced command: answer one read-only export call read on stdin",
		Before: logToStderr,
		Flags: []cli.Flag{&cli.StringFlag{
			Name: "observer", Required: true,
			Usage: "the observer this key belongs to, set in authorized_keys, never by the caller",
		}},
		Action: runClusterExport,
	}
}

// runClusterExport reads one request, forwards it to the running node's
// socket and copies the answer to stdout. It opens no data directory and
// forwards the export ops only. Under sshd it refuses a session that has a
// terminal or an agent: the key's authorized_keys line must carry
// `restrict`, and Forgejo's own SSH server never runs this command.
func runClusterExport(ctx context.Context, c *cli.Command) error {
	if os.Getenv("SSH_TTY") != "" || os.Getenv("SSH_AUTH_SOCK") != "" {
		return errors.New("the export refuses a session with a terminal or an agent: give the key `restrict` in authorized_keys")
	}
	line, err := bufio.NewReader(io.LimitReader(os.Stdin, clusterhook.MaxExportRequest)).ReadBytes('\n')
	if err != nil {
		return fmt.Errorf("reading the request: %w", err)
	}
	var req clusterhook.ExportRequest
	if err := json.Unmarshal(line, &req); err != nil {
		return fmt.Errorf("the request: %w", err)
	}
	if !clusterhook.IsExportOp(req.Op) {
		return fmt.Errorf("%q is not an export op", req.Op)
	}
	req.Observer = c.String("observer")
	// A base backup runs as long as the node's export budget allows.
	ctx, cancel := context.WithTimeout(ctx, clusterhook.ExportTimeout)
	defer cancel()
	ans, body, err := clusterhook.CallExport(ctx, setting.Cluster.HookSocket, &req)
	if err != nil {
		return err
	}
	defer body.Close()
	out := bufio.NewWriter(os.Stdout)
	b, err := json.Marshal(ans)
	if err != nil {
		return err
	}
	if _, err := out.Write(append(b, '\n')); err != nil {
		return err
	}
	if _, err := io.Copy(out, body); err != nil {
		return err
	}
	return out.Flush()
}

func cmdClusterWALArchive() *cli.Command {
	return &cli.Command{
		Name:      "wal-archive",
		Usage:     "PostgreSQL's archive_command on a member: archive %p as %f, idempotently",
		ArgsUsage: "<path> <name>",
		Before:    logToStderr,
		Flags:     []cli.Flag{&cli.StringFlag{Name: "dir", Usage: "the archive (default: [cluster] WAL_ARCHIVE_DIR)"}},
		Action: func(_ context.Context, c *cli.Command) error {
			if c.Args().Len() != 2 {
				return errors.New("want <path> <name>, as %p %f")
			}
			dir := c.String("dir")
			if dir == "" {
				dir = setting.Cluster.WALArchiveDir
			}
			return cluster.ArchiveWAL(c.Args().Get(0), c.Args().Get(1), dir)
		},
	}
}

func cmdClusterObserve() *cli.Command {
	dirFlag := &cli.StringFlag{Name: "dir", Usage: "an observer directory or a snapshot of it (default: [cluster] OBSERVER_DIR)"}
	return &cli.Command{
		Name:   "observe",
		Usage:  "The observer: pull and protect checkpoints from the leader ([cluster] ROLE = observer)",
		Action: runClusterObserve(false),
		Commands: []*cli.Command{
			{Name: "once", Usage: "One pass, then exit", Action: runClusterObserve(true)},
			{
				Name: "list", Usage: "The checkpoints of a directory, newest first, each checked; read-only",
				Flags: []cli.Flag{dirFlag, &cli.BoolFlag{Name: "json"}}, Action: runClusterObserveList,
			},
			{Name: "status", Usage: "STATUS.json of this observer", Flags: []cli.Flag{dirFlag}, Action: runClusterObserveStatus},
			{
				Name: "accept", ArgsUsage: "<lineage>",
				Usage:  "End a quarantine: follow a new lineage, or, naming the current one, restart after a fork",
				Action: runClusterObserveAccept,
			},
			{
				Name: "keygen", Usage: "Make an observer's SSH key and secrets key; print what the members need",
				Flags:  []cli.Flag{&cli.StringFlag{Name: "out", Required: true}, &cli.StringFlag{Name: "name", Required: true}},
				Action: runClusterObserveKeygen,
			},
		},
	}
}

// readSecretsKey reads an X25519 private key, 64 hex digits.
func readSecretsKey(path string) (*[32]byte, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	raw, err := hex.DecodeString(strings.TrimSpace(string(b)))
	if err != nil || len(raw) != 32 {
		return nil, fmt.Errorf("%s does not hold 64 hex digits", path)
	}
	return (*[32]byte)(raw), nil
}

func newObserverFromSettings() (*observer.Observer, error) {
	if setting.Cluster.Role != setting.ClusterRoleObserver {
		return nil, errors.New("[cluster] ROLE is not observer on this node")
	}
	if setting.Cluster.ConfigError != nil {
		return nil, setting.Cluster.ConfigError
	}
	o := setting.Cluster.Observer
	key, err := readSecretsKey(o.SecretsKeyFile)
	if err != nil {
		return nil, err
	}
	eps := map[string]observer.Source{}
	for slot, target := range o.Endpoints {
		src, err := observer.NewSSHSource(target, o.SSHKeyFile, o.KnownHostsFile, o.ExportSilence)
		if err != nil {
			return nil, fmt.Errorf("%s: %w", slot, err)
		}
		eps[slot] = src
	}
	return observer.New(observer.Config{
		Name: o.Name, Dir: o.Dir, Endpoints: eps, SecretsKey: key, HookCommand: o.SnapshotHook,
		StagingQuota: o.StagingQuota, LiveKeep: 72 * time.Hour, Git: setting.Git.Path,
	})
}

func runClusterObserve(once bool) cli.ActionFunc {
	return func(ctx context.Context, c *cli.Command) error {
		o, err := newObserverFromSettings()
		if err != nil {
			return err
		}
		defer o.Close()
		if once {
			return o.Step(ctx)
		}
		ctx, stop := signal.NotifyContext(ctx, os.Interrupt, syscall.SIGTERM)
		defer stop()
		log.Info("cluster observer %s: pulling every %s from %d endpoints", setting.Cluster.Observer.Name, setting.Cluster.Observer.PollInterval, len(setting.Cluster.Observer.Endpoints))
		o.Run(ctx, setting.Cluster.Observer.PollInterval)
		return nil
	}
}

func observerDir(c *cli.Command) string {
	if d := c.String("dir"); d != "" {
		return d
	}
	return setting.Cluster.Observer.Dir
}

func runClusterObserveList(ctx context.Context, c *cli.Command) error {
	a, err := observer.OpenArchive(observerDir(c), setting.Git.Path)
	if err != nil {
		return err
	}
	list := a.List()
	w := c.Root().Writer
	if c.Bool("json") {
		b, err := json.MarshalIndent(list, "", "  ")
		if err != nil {
			return err
		}
		_, err = w.Write(append(b, '\n'))
		return err
	}
	tw := tabwriter.NewWriter(w, 0, 4, 2, ' ', 0)
	fmt.Fprintln(tw, "CHECKPOINT\tTIME\tAGE\tSTATE\tPOSTGRESQL\tRESTORES")
	for _, cp := range list {
		ok := "yes"
		if cp.Problem != "" {
			ok = "no: " + cp.Problem
		}
		fmt.Fprintf(tw, "%s\t%s\t%s\t%s\t%s tl %d\t%s\n", cp.Name, cp.Time.Format(time.RFC3339), time.Since(cp.Time).Round(time.Minute), cp.State, cp.LSN, cp.Timeline, ok)
	}
	return tw.Flush()
}

func runClusterObserveStatus(_ context.Context, c *cli.Command) error {
	b, err := os.ReadFile(filepath.Join(observerDir(c), "STATUS.json"))
	if err != nil {
		return err
	}
	_, err = c.Root().Writer.Write(b)
	return err
}

// runClusterObserveAccept ends a quarantine. Beside the running service,
// which holds the directory and its state in memory, it leaves the request
// for the service's next pass instead.
func runClusterObserveAccept(_ context.Context, c *cli.Command) error {
	o, err := newObserverFromSettings()
	busy := errors.Is(err, observer.ErrBusy)
	if err != nil && !busy {
		return err
	}
	if err := confirmTyped(c, "the lineage", c.Args().First()); err != nil {
		if o != nil {
			o.Close()
		}
		return err
	}
	if busy {
		if err := observer.RequestAccept(setting.Cluster.Observer.Dir, c.Args().First()); err != nil {
			return err
		}
		fmt.Fprintln(c.Root().Writer, "The running observer takes the accept at its next pass; `forgejo cluster observe status` shows the outcome.")
		return nil
	}
	defer o.Close()
	return o.Accept(c.Args().First())
}

func runClusterObserveKeygen(_ context.Context, c *cli.Command) error {
	out, name := c.String("out"), c.String("name")
	if err := os.MkdirAll(out, 0o700); err != nil {
		return err
	}
	pub, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		return err
	}
	block, err := ssh.MarshalPrivateKey(priv, "forgejo observer "+name)
	if err != nil {
		return err
	}
	sshPub, err := ssh.NewPublicKey(pub)
	if err != nil {
		return err
	}
	boxPub, boxPriv, err := box.GenerateKey(rand.Reader)
	if err != nil {
		return err
	}
	for file, body := range map[string][]byte{
		"id_ed25519":     pem.EncodeToMemory(block),
		"id_ed25519.pub": ssh.MarshalAuthorizedKey(sshPub),
		"secrets.key":    []byte(hex.EncodeToString(boxPriv[:]) + "\n"),
	} {
		if err := os.WriteFile(filepath.Join(out, file), body, 0o600); err != nil {
			return err
		}
	}
	w := c.Root().Writer
	fmt.Fprintf(w, "On every member, in the export account's authorized_keys (the observer's address in from=):\n")
	fmt.Fprintf(w, "restrict,command=\"forgejo cluster export --observer %s\",from=\"<observer address>\" %s", name, ssh.MarshalAuthorizedKey(sshPub))
	fmt.Fprintf(w, "On every member, in [cluster]:\nOBSERVER_RECIPIENTS = %s=%s\n", name, hex.EncodeToString(boxPub[:]))
	return nil
}
