// Copyright 2026 The Forgejo Authors. All rights reserved.
// SPDX-License-Identifier: GPL-3.0-or-later

package cmd

import (
	"context"
	"crypto/ed25519"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"encoding/hex"
	"errors"
	"fmt"
	"math"
	"net"
	"os"
	"strconv"
	"strings"
	"time"

	cluster_model "forgejo.org/models/cluster"
	user_model "forgejo.org/models/user"
	"forgejo.org/modules/clusterhook"
	"forgejo.org/modules/json"
	"forgejo.org/modules/log"
	"forgejo.org/modules/setting"
	"forgejo.org/services/cluster"
	"forgejo.org/services/cluster/consensus"
	"forgejo.org/services/cluster/reftable"
	"forgejo.org/services/cluster/witness"

	"github.com/urfave/cli/v3"
)

// cmdCluster is the operator's side of the ref replicator (kitsunium ADR
// 0152 D17). The offline subcommands run on a STOPPED node, under the data
// directory's lock, from the same binary as the server. The online ones
// reach the running server through its private socket only; every online
// membership operation is in the site administration, never here.
func cmdCluster() *cli.Command {
	return &cli.Command{
		Name:  "cluster",
		Usage: "Operate this node of the ref replicator",
		Commands: []*cli.Command{
			{
				Name:   "init",
				Usage:  "Offline: stamp an empty data directory as a joining node and print its member line",
				Flags:  []cli.Flag{&cli.StringFlag{Name: "cluster", Usage: "cluster id, 32 hex digits (default: [cluster] CLUSTER_ID)"}},
				Action: runClusterInit,
			},
			{
				Name:  "bootstrap",
				Usage: "Offline, once, on every founding node with the identical list: found the cluster",
				Flags: []cli.Flag{&cli.StringFlag{
					Name: "members", Required: true,
					Usage: "comma-separated member lines id:incarnation:slot:spki:host:port, as `cluster init` printed them",
				}},
				Action: runClusterBootstrap,
			},
			{
				Name:   "status",
				Usage:  "Online: this node's view, read through the private socket",
				Flags:  []cli.Flag{&cli.BoolFlag{Name: "json"}},
				Action: runClusterStatus,
			},
			{
				Name:   "inspect",
				Usage:  "Offline, read-only: the data directory's stamp, log, snapshot and quarantine",
				Action: runClusterInspect,
			},
			{
				Name:  "recover",
				Usage: "Offline, after a lost majority: force a new configuration under a new cluster id; or, with --from-observer, rebuild from an observer's checkpoint",
				Flags: append([]cli.Flag{
					&cli.StringFlag{Name: "new-cluster", Required: true},
					&cli.StringFlag{Name: "members", Usage: "required unless --from-observer"},
					&cli.StringSliceFlag{Name: "fenced", Usage: "old voter fence: id:method:receipt-file[+receipt-file], one per old voter; required unless --from-observer"},
					&cli.StringSliceFlag{Name: "witness-key", Usage: "hex Ed25519 public key of a witness"},
					&cli.StringSliceFlag{Name: "joining", Usage: "member lines of nodes stamped joining under the new cluster id"},
					&cli.StringSliceFlag{Name: "old-peer", Usage: "slot=host:port of an old member's consensus address, probed with PEERS before recovering"},
					&cli.BoolFlag{Name: "force-old-members-answer", Usage: "recover although an old member answers the probe; logged and kept in the output"},
				}, recoverFromObserverFlags()...),
				Action: runClusterRecover,
			},
			{
				Name:  "restore",
				Usage: "Offline, into an empty data directory: restore the ref snapshot of a backup",
				Flags: []cli.Flag{
					&cli.StringFlag{Name: "file", Required: true},
					&cli.StringFlag{Name: "sha256", Required: true, Usage: "the digest read from the authenticated backup repository"},
					&cli.StringFlag{Name: "new-cluster", Required: true},
					&cli.StringFlag{Name: "members", Required: true},
				},
				Action: runClusterRestore,
			},
			{
				Name:  "quarantine",
				Usage: "Offline: leave a LOG_CORRUPT quarantine",
				Commands: []*cli.Command{{
					Name:   "clear",
					Flags:  []cli.Flag{&cli.StringFlag{Name: "exit", Required: true, Usage: "wipe-and-rejoin or truncate-and-stay-silent"}},
					Action: runClusterQuarantineClear,
				}},
			},
			{
				Name:  "snapshot",
				Usage: "Online: save the ref snapshot and its SHA-256 side by side",
				Commands: []*cli.Command{{
					Name:   "save",
					Flags:  []cli.Flag{&cli.StringFlag{Name: "out", Required: true}},
					Action: runClusterSnapshotSave,
				}},
			},
			{
				Name:  "operator",
				Usage: "Grant or withdraw the cluster-operator permission, from a node: the way back in when nobody holds it",
				Commands: []*cli.Command{
					{
						Name: "add", ArgsUsage: "<login>", Action: runClusterOperator("add"),
						Flags: []cli.Flag{&cli.StringFlag{Name: "oidc-subject", Usage: "the subject it steps up as at STEP_UP_OIDC_SOURCE (default: its current link there)"}},
					},
					{Name: "remove", ArgsUsage: "<login>", Action: runClusterOperator("remove")},
					{
						Name: "pin", ArgsUsage: "<login>", Action: runClusterOperator("pin"),
						Usage: "Pin the OpenID subject an operator steps up as: --oidc-subject, or its current link at STEP_UP_OIDC_SOURCE",
						Flags: []cli.Flag{&cli.StringFlag{Name: "oidc-subject"}},
					},
					{Name: "list", Action: runClusterOperator("list")},
				},
			},
			cmdClusterWitness(),
			cmdClusterExport(),
			cmdClusterObserve(),
			cmdClusterWALArchive(),
			{
				Name:  "cert",
				Usage: "The node certificate",
				Commands: []*cli.Command{{
					Name:   "renew",
					Usage:  "Renew now through Vault ([cluster] RENEW_*): a CSR from the node key, same SPKI and URI SAN",
					Action: runClusterCertRenew,
				}},
			},
			{
				Name:  "objects",
				Usage: "Online: check that this node holds every object the table names, or fetch the missing ones",
				Commands: []*cli.Command{
					{Name: "verify", Action: runClusterObjects(clusterhook.OpObjectsVerify)},
					{Name: "backfill", Action: runClusterObjects(clusterhook.OpObjectsFill)},
				},
			},
			{
				Name:  "reconcile",
				Usage: "Online: compare the table with SQL's branch rows; --apply rebuilds the regenerable ones",
				Flags: []cli.Flag{
					&cli.BoolFlag{Name: "dry-run", Usage: "report only (the default)"},
					&cli.BoolFlag{Name: "apply", Usage: "rebuild regenerable rows from the table"},
					&cli.StringFlag{Name: "delete-orphan", Usage: "delete from the cluster one repository (owner/repo) the table holds and SQL does not know; restore it by adoption instead when SQL lost it"},
					&cli.Uint64Flag{Name: "expect-index", Usage: "with --delete-orphan: the index reconcile reported; refused if the repository changed since"},
					&cli.StringFlag{Name: "reason", Usage: "with --delete-orphan: why it is deleted (required, kept in the audit)"},
				},
				Action: runClusterReconcile,
			},
			{
				Name:  "parked",
				Usage: "Online: the events whose post-receive work waits parked",
				Commands: []*cli.Command{
					{Name: "list", Usage: "List the parked events with their error", Action: runClusterParkedList},
					{
						Name:      "resolve",
						Usage:     "Give up the work of a parked event that can never succeed; the event and the reason are kept in cluster_parked_resolution and the audit",
						ArgsUsage: "<index>",
						Flags: []cli.Flag{
							&cli.StringFlag{Name: "reason", Usage: "why the event is given up (required, kept with it)"},
							&cli.StringFlag{Name: "lineage", Usage: "the lineage, when the index is parked in several"},
						},
						Action: runClusterParkedResolve,
					},
				},
			},
			{
				Name:      "import",
				Usage:     "Online: put the refs of repositories that predate the cluster into the table, objects first",
				ArgsUsage: "[owner/name.git]",
				Action:    runClusterImport,
			},
			{
				Name:      "reproject",
				Usage:     "Online: compare a repository's git refs with the table and reproject it on a difference",
				ArgsUsage: "owner/name.git",
				Action:    runClusterReproject,
			},
		},
	}
}

func clusterDriver() (consensus.Driver, error) {
	if !setting.Cluster.Enabled {
		return nil, errors.New("[cluster] ENABLED is false in this configuration")
	}
	if setting.Cluster.ConfigError != nil {
		return nil, setting.Cluster.ConfigError
	}
	return consensus.Lookup(setting.Cluster.Driver)
}

// localOperator is the operator of an offline command: the local account.
func localOperator() consensus.Operator {
	return consensus.Operator("local-uid:" + strconv.Itoa(os.Getuid()))
}

// confirmTyped asks the operator to type a value back (D17).
func confirmTyped(c *cli.Command, what, want string) error {
	fmt.Fprintf(c.Root().Writer, "Type %s to confirm: ", what)
	var got string
	if _, err := fmt.Fscanln(os.Stdin, &got); err != nil || got != want {
		return errors.New("confirmation does not match; nothing was done")
	}
	return nil
}

func parseMembers(s string) ([]consensus.Member, error) {
	var out []consensus.Member
	for line := range strings.SplitSeq(s, ",") {
		if strings.TrimSpace(line) == "" {
			continue
		}
		m, err := cluster.ParseMember(line)
		if err != nil {
			return nil, fmt.Errorf("%q: %w", line, err)
		}
		m.Voter = true
		out = append(out, m)
	}
	if len(out) == 0 {
		return nil, errors.New("no member")
	}
	return out, nil
}

// certSPKI reads the SPKI pin of the node's own certificate.
func certSPKI() ([32]byte, error) {
	pair, err := tls.LoadX509KeyPair(setting.Cluster.TLSCertFile, setting.Cluster.TLSKeyFile)
	if err != nil {
		return [32]byte{}, err
	}
	leaf, err := x509.ParseCertificate(pair.Certificate[0])
	if err != nil {
		return [32]byte{}, err
	}
	return sha256.Sum256(leaf.RawSubjectPublicKeyInfo), nil
}

func nodeConfig() consensus.Config {
	return consensus.Config{
		DataDir: setting.Cluster.DataDir, Slot: setting.Cluster.Slot, ListenAddress: setting.Cluster.ListenAddr,
		Seeds:   setting.Cluster.Peers,
		TLS:     consensus.TLSFiles{CertFile: setting.Cluster.TLSCertFile, KeyFile: setting.Cluster.TLSKeyFile, CAFile: setting.Cluster.TLSCAFile},
		Machine: reftable.New(), MaxLogBytes: setting.Cluster.MaxLogBytes, MaxSnapshotBytes: setting.Cluster.MaxSnapshotBytes,
	}
}

func runClusterInit(ctx context.Context, c *cli.Command) error {
	d, err := clusterDriver()
	if err != nil {
		return err
	}
	idHex := c.String("cluster")
	if idHex == "" {
		idHex = setting.Cluster.ClusterID
	}
	cid, err := consensus.ParseClusterID(idHex)
	if err != nil {
		return err
	}
	spki, err := certSPKI()
	if err != nil {
		return fmt.Errorf("the node's certificate: %w", err)
	}
	id, inc, err := d.Init(setting.Cluster.DataDir, cid, setting.Cluster.Slot)
	if err != nil {
		return err
	}
	fmt.Fprintf(c.Root().Writer, "%s:%s:%s:%s:%s\n", id, inc, setting.Cluster.Slot, hex.EncodeToString(spki[:]), setting.Cluster.ListenAddr)
	return nil
}

func runClusterBootstrap(ctx context.Context, c *cli.Command) error {
	d, err := clusterDriver()
	if err != nil {
		return err
	}
	members, err := parseMembers(c.String("members"))
	if err != nil {
		return err
	}
	return d.Bootstrap(ctx, nodeConfig(), members)
}

func runClusterInspect(ctx context.Context, c *cli.Command) error {
	d, err := clusterDriver()
	if err != nil {
		return err
	}
	r, err := d.Inspect(setting.Cluster.DataDir)
	if err != nil {
		return err
	}
	b, err := json.MarshalIndent(r, "", "  ")
	if err != nil {
		return err
	}
	_, err = c.Root().Writer.Write(append(b, '\n'))
	return err
}

func runClusterRecover(ctx context.Context, c *cli.Command) error {
	if c.String("from-observer") != "" {
		return runRecoverFromObserver(ctx, c)
	}
	if c.String("members") == "" || len(c.StringSlice("fenced")) == 0 {
		return errors.New("--members and --fenced are required, unless --from-observer")
	}
	d, err := clusterDriver()
	if err != nil {
		return err
	}
	cid, err := consensus.ParseClusterID(c.String("new-cluster"))
	if err != nil {
		return err
	}
	members, err := parseMembers(c.String("members"))
	if err != nil {
		return err
	}
	opt := consensus.RecoverOptions{Operator: localOperator()}
	if opt.Fenced, err = parseFenced(c.StringSlice("fenced")); err != nil {
		return err
	}
	rep, err := d.Inspect(setting.Cluster.DataDir)
	if err != nil {
		return err
	}
	if err := checkRecoverVoters(members, rep); err != nil {
		return err
	}
	old := rep.Cluster
	pubs, err := verifyWitnessFence(c, old, cid, opt.Fenced)
	if err != nil {
		return err
	}
	for _, pub := range pubs {
		opt.WitnessKeys = append(opt.WitnessKeys, pub)
	}
	if err := checkOldMembersSilent(ctx, c, old, cid); err != nil {
		return err
	}
	if j := c.StringSlice("joining"); len(j) > 0 {
		if opt.Joining, err = parseMembers(strings.Join(j, ",")); err != nil {
			return err
		}
	}
	if err := confirmTyped(c, "the new cluster id", cid.String()); err != nil {
		return err
	}
	opt.Confirm = cid.String()
	return d.Recover(setting.Cluster.DataDir, cid, members, opt)
}

// parseFenced reads the --fenced attestations, id:method:receipt-file
// [+receipt-file], one per old voter.
func parseFenced(flags []string) ([]consensus.FencedVoter, error) {
	var out []consensus.FencedVoter
	for _, f := range flags {
		parts := strings.SplitN(f, ":", 3)
		if len(parts) != 3 {
			return nil, fmt.Errorf("--fenced %q: want id:method:receipt-file[+receipt-file]", f)
		}
		id, err := cluster.ParseNodeID(parts[0])
		if err != nil {
			return nil, err
		}
		fv := consensus.FencedVoter{Voter: id, Method: parts[1]}
		for file := range strings.SplitSeq(parts[2], "+") {
			b, err := os.ReadFile(file)
			if err != nil {
				return nil, err
			}
			// A receipt file holds the signature, raw or in hex as
			// `cluster witness receipt` writes it.
			if raw, err := hex.DecodeString(strings.TrimSpace(string(b))); err == nil && len(raw) == ed25519.SignatureSize {
				b = raw
			}
			fv.Receipts = append(fv.Receipts, b)
		}
		out = append(out, fv)
	}
	return out, nil
}

// verifyWitnessFence checks that both witnesses recorded the fence before
// the survivor serves (R9-09): a receipt from every key, for every old
// voter, and every key the nodes ask at boot among them.
func verifyWitnessFence(c *cli.Command, old, cid consensus.ClusterID, fenced []consensus.FencedVoter) ([]ed25519.PublicKey, error) {
	keys := c.StringSlice("witness-key")
	if len(keys) == 0 {
		keys = setting.Cluster.WitnessKeys
	}
	pubs, err := witness.RecoverKeys(keys, setting.Cluster.WitnessKeys)
	if err != nil {
		return nil, fmt.Errorf("RECOVER_REFUSED: %w", err)
	}
	if err := witness.VerifyReceipts(pubs, old, cid, fenced); err != nil {
		return nil, fmt.Errorf("RECOVER_REFUSED: %w", err)
	}
	return pubs, nil
}

// checkRecoverVoters refuses a Recover whose new configuration names
// another voter than this node: another survivor listed as a voter would
// either never vote (a majority nobody reaches) or run its own Recover into
// the same cluster id, two histories under one id. The others enter as
// learners, with --joining.
func checkRecoverVoters(members []consensus.Member, rep consensus.Report) error {
	for _, m := range members {
		if m.ID != rep.Node || m.Slot != rep.Slot {
			return fmt.Errorf("RECOVER_REFUSED: --members names %s (slot %s): only this node (%s, slot %s) is a voter after a Recover; the others join with --joining", m.ID, m.Slot, rep.Node, rep.Slot)
		}
	}
	return nil
}

// oldPeerAddrs is what the probe asks: PEERS and every --old-peer, this
// node's slot excepted.
func oldPeerAddrs(c *cli.Command) (map[string]string, error) {
	addrs := map[string]string{}
	for slot, addr := range setting.Cluster.Peers {
		if slot != setting.Cluster.Slot {
			addrs[slot] = addr
		}
	}
	for _, f := range c.StringSlice("old-peer") {
		slot, addr, ok := strings.Cut(f, "=")
		if !ok || slot == "" || addr == "" {
			return nil, fmt.Errorf("--old-peer %q: want slot=host:port", f)
		}
		if _, _, err := net.SplitHostPort(addr); err != nil {
			return nil, fmt.Errorf("--old-peer %q: %w", f, err)
		}
		if slot != setting.Cluster.Slot {
			addrs[slot] = addr
		}
	}
	return addrs, nil
}

// checkOldMembersSilent refuses while an old member answers (ADR 0152
// R3-27): the witnesses' receipts fence the old cluster at its next boot,
// they do not stop a member that is running and may still hold a
// majority. With no address to probe, the operator names them, or forces.
// A forced Recover is logged and printed.
func checkOldMembersSilent(ctx context.Context, c *cli.Command, old, cid consensus.ClusterID) error {
	w := c.Root().Writer
	force := c.Bool("force-old-members-answer")
	addrs, err := oldPeerAddrs(c)
	if err != nil {
		return err
	}
	if len(addrs) == 0 {
		if !force {
			return errors.New("RECOVER_REFUSED: no old member to probe: give --old-peer slot=host:port for each old member, or --force-old-members-answer once you know they are all stopped")
		}
		log.Warn("cluster recover into %s: no old member of %s probed, forced by the operator", cid, old)
		fmt.Fprintf(w, "WARNING: no old member of %s was probed; the operator forced the recovery.\n", old)
		return nil
	}
	var blocking []string
	for _, a := range cluster.ProbeOldMembers(ctx, old.String(), addrs, setting.Cluster.TLSCAFile) {
		if a.Blocks() {
			blocking = append(blocking, fmt.Sprintf("%s at %s: %s", a.Slot, a.Addr, a.Detail))
		}
	}
	if len(blocking) == 0 {
		return nil
	}
	if !force {
		return fmt.Errorf("RECOVER_REFUSED: old member answers: %s; stop it (or cut it off), or pass --force-old-members-answer", strings.Join(blocking, "; "))
	}
	log.Warn("cluster recover into %s: old members of %s answer and the operator forced the recovery: %s", cid, old, strings.Join(blocking, "; "))
	fmt.Fprintf(w, "WARNING: old members answer and the recovery is forced: %s\n", strings.Join(blocking, "; "))
	return nil
}

func runClusterRestore(ctx context.Context, c *cli.Command) error {
	d, err := clusterDriver()
	if err != nil {
		return err
	}
	cid, err := consensus.ParseClusterID(c.String("new-cluster"))
	if err != nil {
		return err
	}
	members, err := parseMembers(c.String("members"))
	if err != nil {
		return err
	}
	sum, err := hex.DecodeString(c.String("sha256"))
	if err != nil || len(sum) != 32 {
		return errors.New("--sha256 must be 64 hex digits")
	}
	meta, err := os.ReadFile(c.String("file") + ".meta")
	if err != nil {
		return fmt.Errorf("the snapshot's metadata, written beside it by `snapshot save`: %w", err)
	}
	f, err := os.Open(c.String("file"))
	if err != nil {
		return err
	}
	defer f.Close()
	if err := confirmTyped(c, "the new cluster id", cid.String()); err != nil {
		return err
	}
	opt := consensus.RestoreOptions{Meta: meta, Expected: [32]byte(sum), Confirm: cid.String(), Operator: localOperator(), MaxBytes: setting.Cluster.MaxSnapshotBytes}
	restored, err := d.Restore(setting.Cluster.DataDir, f, cid, members, opt)
	if err != nil {
		return err
	}
	fmt.Fprintf(c.Root().Writer, "restored snapshot %s at index %d\n", restored.ID, restored.Position.Index)
	return nil
}

func runClusterQuarantineClear(ctx context.Context, c *cli.Command) error {
	d, err := clusterDriver()
	if err != nil {
		return err
	}
	var exit consensus.QuarantineExit
	switch c.String("exit") {
	case "wipe-and-rejoin":
		exit = consensus.WipeAndRejoin
	case "truncate-and-stay-silent":
		exit = consensus.TruncateAndStaySilent
	default:
		return errors.New("--exit must be wipe-and-rejoin or truncate-and-stay-silent")
	}
	if err := confirmTyped(c, "the slot", setting.Cluster.Slot); err != nil {
		return err
	}
	return d.ClearQuarantine(setting.Cluster.DataDir, exit, localOperator())
}

func callReplicator(ctx context.Context, req *clusterhook.Request) (*clusterhook.Response, error) {
	if !setting.Cluster.Enabled {
		return nil, errors.New("[cluster] ENABLED is false in this configuration")
	}
	ctx, cancel := context.WithTimeout(ctx, clusterhook.AdminTimeout)
	defer cancel()
	resp, err := clusterhook.Call(ctx, setting.Cluster.HookSocket, req)
	if err != nil {
		return nil, err
	}
	if !resp.OK {
		return nil, errors.New(resp.Message)
	}
	return resp, nil
}

func runClusterStatus(ctx context.Context, c *cli.Command) error {
	resp, err := callReplicator(ctx, &clusterhook.Request{Op: clusterhook.OpStatus})
	if err != nil {
		return err
	}
	if c.Bool("json") {
		_, err := c.Root().Writer.Write(append(resp.Data, '\n'))
		return err
	}
	var v cluster.View
	if err := json.Unmarshal(resp.Data, &v); err != nil {
		return err
	}
	w := c.Root().Writer
	fmt.Fprintf(w, "slot %s  role %s  term %d  leader %s  commit %d  applied %d\n", v.Slot, v.Role, v.Term, v.Leader, v.Commit, v.Applied)
	fmt.Fprintf(w, "ready %t  fenced %t  failure tolerance %d  lineage %s\n", v.Ready, v.Fenced, v.FailureTolerance, v.Identity)
	if v.ExpectedNodes > 0 {
		voters := 0
		for _, m := range v.Members {
			if m.Voter {
				voters++
			}
		}
		fmt.Fprintf(w, "expected nodes %d  members %d  voters %d (the rule wants %d)\n", v.ExpectedNodes, len(v.Members), voters, v.VoterTarget)
	}
	for _, m := range v.Members {
		kind := "learner"
		if m.Voter {
			kind = "voter"
		}
		since := ""
		if !m.Since.IsZero() {
			since = "  since " + m.Since.UTC().Format(time.RFC3339)
		}
		fmt.Fprintf(w, "  #%d %s %-8s %-7s %-22s match %d lag %d unreachable %t%s\n", m.Rank, m.ID, m.Slot, kind, m.Address, m.Match, m.Lag, m.Unreachable, since)
	}
	for _, m := range v.Removed {
		since := ""
		if !m.Since.IsZero() {
			since = "  since " + m.Since.UTC().Format(time.RFC3339)
		}
		fmt.Fprintf(w, "     %s %-8s removed%s\n", m.ID, m.Slot, since)
	}
	for _, a := range v.Alarms {
		fmt.Fprintf(w, "  alarm %d %s %v\n", a.ID, a.Kind, a.Nodes)
	}
	// What a joining node puts in [cluster] PEER_PINS to trust these members.
	var pins []string
	for _, m := range v.Members {
		if m.SPKI != "" {
			pins = append(pins, m.Slot+"="+m.SPKI)
		}
	}
	if len(pins) > 0 {
		fmt.Fprintf(w, "PEER_PINS = %s\n", strings.Join(pins, ","))
	}
	return nil
}

func runClusterSnapshotSave(ctx context.Context, c *cli.Command) error {
	resp, err := callReplicator(ctx, &clusterhook.Request{Op: clusterhook.OpSnapshot, Path: c.String("out"), Operator: string(localOperator())})
	if err != nil {
		return err
	}
	_, err = c.Root().Writer.Write(append(resp.Data, '\n'))
	return err
}

func runClusterReproject(ctx context.Context, c *cli.Command) error {
	repo := c.Args().First()
	if err := reftable.ValidRepoKey(repo); err != nil {
		return err
	}
	resp, err := callReplicator(ctx, &clusterhook.Request{Op: clusterhook.OpReproject, Repo: repo})
	if err != nil {
		return err
	}
	_, err = c.Root().Writer.Write(append(resp.Data, '\n'))
	return err
}

func runClusterImport(ctx context.Context, c *cli.Command) error {
	repo := c.Args().First()
	if repo != "" {
		if err := reftable.ValidRepoKey(repo); err != nil {
			return err
		}
	}
	resp, err := callReplicator(ctx, &clusterhook.Request{Op: clusterhook.OpImport, Repo: repo})
	if err != nil {
		return err
	}
	_, err = c.Root().Writer.Write(append(resp.Data, '\n'))
	return err
}

func runClusterObjects(op string) cli.ActionFunc {
	return func(ctx context.Context, c *cli.Command) error {
		resp, err := callReplicator(ctx, &clusterhook.Request{Op: op})
		if err != nil {
			return err
		}
		_, err = c.Root().Writer.Write(append(resp.Data, '\n'))
		return err
	}
}

// reconcileRequest checks the arguments of `reconcile`: a report, a
// rebuild, or the explicit deletion of one orphaned repository.
func reconcileRequest(c *cli.Command) (*clusterhook.Request, error) {
	if c.Bool("apply") && c.Bool("dry-run") {
		return nil, errors.New("--apply and --dry-run exclude each other")
	}
	repo := strings.TrimSpace(c.String("delete-orphan"))
	if repo == "" {
		return &clusterhook.Request{Op: clusterhook.OpReconcile, DryRun: !c.Bool("apply")}, nil
	}
	if c.Bool("apply") || c.Bool("dry-run") {
		return nil, errors.New("--delete-orphan is its own operation: no --apply nor --dry-run")
	}
	index := c.Uint64("expect-index")
	if index == 0 || index > math.MaxInt64 {
		return nil, errors.New("--expect-index is required: the index `forgejo cluster reconcile` reported for the repository")
	}
	reason := strings.TrimSpace(c.String("reason"))
	if reason == "" {
		return nil, errors.New("--reason is required: it is kept in the audit")
	}
	return &clusterhook.Request{Op: clusterhook.OpReconcile, Repo: repo, Index: int64(index), Reason: reason, Operator: string(localOperator())}, nil
}

func runClusterReconcile(ctx context.Context, c *cli.Command) error {
	req, err := reconcileRequest(c)
	if err != nil {
		return err
	}
	resp, err := callReplicator(ctx, req)
	if err != nil {
		return err
	}
	_, err = c.Root().Writer.Write(append(resp.Data, '\n'))
	return err
}

func runClusterParkedList(ctx context.Context, c *cli.Command) error {
	resp, err := callReplicator(ctx, &clusterhook.Request{Op: clusterhook.OpParkedList})
	if err != nil {
		return err
	}
	_, err = c.Root().Writer.Write(append(resp.Data, '\n'))
	return err
}

// parkedResolveRequest checks the arguments of `parked resolve`: an index,
// and a reason, without which nothing is given up.
func parkedResolveRequest(c *cli.Command) (*clusterhook.Request, error) {
	if c.Args().Len() != 1 {
		return nil, errors.New("usage: forgejo cluster parked resolve <index> --reason \"...\"")
	}
	index, err := strconv.ParseInt(c.Args().First(), 10, 64)
	if err != nil || index <= 0 {
		return nil, fmt.Errorf("%q is not a log index", c.Args().First())
	}
	reason := strings.TrimSpace(c.String("reason"))
	if reason == "" {
		return nil, errors.New("--reason is required: it is kept with the event given up")
	}
	return &clusterhook.Request{Op: clusterhook.OpParkedResolve, Index: index, Lineage: c.String("lineage"), Reason: reason, Operator: string(localOperator())}, nil
}

func runClusterParkedResolve(ctx context.Context, c *cli.Command) error {
	req, err := parkedResolveRequest(c)
	if err != nil {
		return err
	}
	resp, err := callReplicator(ctx, req)
	if err != nil {
		return err
	}
	_, err = c.Root().Writer.Write(append(resp.Data, '\n'))
	return err
}

func runClusterOperator(what string) cli.ActionFunc {
	return func(ctx context.Context, c *cli.Command) error {
		if err := initDB(ctx); err != nil {
			return err
		}
		w := c.Root().Writer
		if what == "list" {
			ops, err := cluster_model.ListOperators(ctx)
			if err != nil {
				return err
			}
			for _, o := range ops {
				if u, err := user_model.GetUserByID(ctx, o.UserID); err == nil {
					fmt.Fprintln(w, u.Name)
				}
			}
			return nil
		}
		u, err := user_model.GetUserByName(ctx, c.Args().First())
		if err != nil {
			return err
		}
		subject := c.String("oidc-subject")
		if subject == "" {
			subject = cluster.StepUpSubjectOf(ctx, u.ID)
		}
		switch what {
		case "add":
			if !u.IsAdmin {
				return fmt.Errorf("%s is not a site administrator", u.Name)
			}
			err = cluster_model.AddOperator(ctx, u.ID, 0, subject)
		case "pin":
			if subject == "" {
				return fmt.Errorf("%s has no link at [cluster] STEP_UP_OIDC_SOURCE: give --oidc-subject", u.Name)
			}
			err = cluster_model.PinOperatorSubject(ctx, u.ID, subject)
		default:
			// The way back in: a node may withdraw even the last grant.
			err = cluster_model.RemoveOperator(ctx, u.ID)
		}
		if err == nil {
			fmt.Fprintf(w, "%s: cluster operator permission %s\n", u.Name, map[string]string{"add": "granted", "remove": "withdrawn", "pin": "pinned to OpenID subject " + subject}[what])
		}
		return err
	}
}

func runClusterCertRenew(ctx context.Context, c *cli.Command) error {
	r := cluster.NewRenewer()
	if r == nil {
		return errors.New("[cluster] RENEW_VAULT_ADDR is empty: no renewer is configured")
	}
	cert, err := r.RenewNow(ctx)
	if err != nil {
		return err
	}
	fmt.Fprintf(c.Root().Writer, "renewed: serial %s, valid until %s\n", cert.SerialNumber, cert.NotAfter.Format(time.RFC3339))
	return nil
}
