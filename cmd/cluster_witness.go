// Copyright 2026 The Forgejo Authors. All rights reserved.
// SPDX-License-Identifier: GPL-3.0-or-later

package cmd

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"forgejo.org/modules/setting"
	"forgejo.org/services/cluster"
	"forgejo.org/services/cluster/consensus"
	"forgejo.org/services/cluster/witness"

	"github.com/urfave/cli/v3"
)

// cmdClusterWitness is the Recover witness (kitsunium ADR 0152 R9-08 to
// R9-11). On each witness host: init, serve, and the operator's supersede, which
// writes the state directory directly: there is no write over the network.
// On a Forgejo node: check, the boot check alone, and receipt, which
// fetches a receipt for Recover.
func cmdClusterWitness() *cli.Command {
	dirFlag := &cli.StringFlag{Name: "dir", Required: true, Usage: "the witness state directory (mode 0700)"}
	return &cli.Command{
		Name:  "witness",
		Usage: "The Recover witness: run it on each witness host, ask it from the nodes",
		Commands: []*cli.Command{
			{
				Name:   "init",
				Usage:  "Witness host: create the state directory and the Ed25519 key; prints the public key",
				Flags:  []cli.Flag{dirFlag},
				Action: runWitnessInit,
			},
			{
				Name:   "serve",
				Usage:  "Witness host: answer the status and receipt questions, signed",
				Flags:  []cli.Flag{dirFlag, &cli.StringFlag{Name: "listen", Value: "127.0.0.1:7482", Usage: "the address on the management network"}},
				Action: runWitnessServe,
			},
			{
				Name:  "supersede",
				Usage: "Witness host, operator only: record that a Recover superseded a cluster and fenced its old voters",
				Flags: []cli.Flag{
					dirFlag,
					&cli.StringFlag{Name: "old", Required: true},
					&cli.StringFlag{Name: "new", Required: true},
					&cli.StringFlag{Name: "voters", Required: true, Usage: "the old voters' node ids, comma-separated"},
					&cli.StringFlag{Name: "method", Required: true, Usage: "how they were fenced: stopped, powered-off or cut"},
				},
				Action: runWitnessSupersede,
			},
			{
				Name:   "check",
				Usage:  "Node: the boot check alone, for a prestart task; exit 77 when fenced",
				Action: runWitnessCheck,
			},
			{
				Name:  "receipt",
				Usage: "Node: fetch and verify a fenced-voter receipt from one witness, for Recover",
				Flags: []cli.Flag{
					&cli.StringFlag{Name: "url", Required: true},
					&cli.StringFlag{Name: "key", Required: true, Usage: "the witness's public key, hex"},
					&cli.StringFlag{Name: "old", Required: true},
					&cli.StringFlag{Name: "new", Required: true},
					&cli.StringFlag{Name: "voter", Required: true},
					&cli.StringFlag{Name: "out", Required: true, Usage: "where to write the receipt"},
				},
				Action: runWitnessReceipt,
			},
		},
	}
}

func runWitnessInit(_ context.Context, c *cli.Command) error {
	pub, err := witness.Init(c.String("dir"))
	if err != nil {
		return err
	}
	fmt.Fprintln(c.Root().Writer, pub)
	return nil
}

func runWitnessServe(ctx context.Context, c *cli.Command) error {
	st, err := witness.Open(c.String("dir"))
	if err != nil {
		return err
	}
	srv := &http.Server{Addr: c.String("listen"), Handler: st.Handler(), ReadHeaderTimeout: 5 * time.Second}
	ctx, stop := signal.NotifyContext(ctx, os.Interrupt, syscall.SIGTERM)
	defer stop()
	go func() {
		<-ctx.Done()
		_ = srv.Close()
	}()
	fmt.Fprintf(c.Root().Writer, "witness listening on %s\n", srv.Addr)
	if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		return err
	}
	return nil
}

func runWitnessSupersede(_ context.Context, c *cli.Command) error {
	st, err := witness.Open(c.String("dir"))
	if err != nil {
		return err
	}
	old, err := consensus.ParseClusterID(c.String("old"))
	if err != nil {
		return err
	}
	newCluster, err := consensus.ParseClusterID(c.String("new"))
	if err != nil {
		return err
	}
	var voters []consensus.NodeID
	for v := range strings.SplitSeq(c.String("voters"), ",") {
		id, err := cluster.ParseNodeID(v)
		if err != nil {
			return err
		}
		voters = append(voters, id)
	}
	switch c.String("method") {
	case "stopped", "powered-off", "cut":
	default:
		return errors.New("--method must be stopped, powered-off or cut")
	}
	if err := st.Supersede(old, newCluster, voters, c.String("method")); err != nil {
		return err
	}
	fmt.Fprintf(c.Root().Writer, "cluster %s superseded by %s; %d voters fenced\n", old, newCluster, len(voters))
	return nil
}

func runWitnessCheck(ctx context.Context, c *cli.Command) error {
	if !setting.Cluster.Enabled {
		return errors.New("[cluster] ENABLED is false in this configuration")
	}
	d, err := clusterDriver()
	if err != nil {
		return err
	}
	rep, err := d.Inspect(setting.Cluster.DataDir)
	if err != nil {
		return err
	}
	if err := cluster.FenceCheck(ctx, rep.Cluster); err != nil {
		fmt.Fprintln(c.Root().ErrWriter, "fenced:", err)
		return cli.Exit("", cluster.ExitFenced)
	}
	fmt.Fprintf(c.Root().Writer, "cluster %s is clear\n", rep.Cluster)
	return nil
}

func runWitnessReceipt(ctx context.Context, c *cli.Command) error {
	key, err := witness.ParsePublicKey(c.String("key"))
	if err != nil {
		return err
	}
	old, err := consensus.ParseClusterID(c.String("old"))
	if err != nil {
		return err
	}
	newCluster, err := consensus.ParseClusterID(c.String("new"))
	if err != nil {
		return err
	}
	voter, err := cluster.ParseNodeID(c.String("voter"))
	if err != nil {
		return err
	}
	sig, err := witness.FetchReceipt(ctx, witness.Witness{URL: strings.TrimRight(c.String("url"), "/"), Key: key}, old, newCluster, voter)
	if err != nil {
		return err
	}
	return os.WriteFile(c.String("out"), []byte(hex.EncodeToString(sig)+"\n"), 0o600)
}
