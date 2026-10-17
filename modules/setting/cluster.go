// Copyright 2026 The Forgejo Authors. All rights reserved.
// SPDX-License-Identifier: GPL-3.0-or-later

package setting

import (
	"encoding/hex"
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"time"

	"forgejo.org/modules/container"
	"forgejo.org/modules/log"
)

// Cluster settings: the in-process replicator of refs (kitsunium ADR 0152,
// D19). Nothing here is read by the consensus engine at run time except
// through the replicator, and nothing of it is replicated: a value that
// changes an Apply verdict is pinned in code, never configured.
var Cluster = struct {
	Enabled bool
	// ExpectedNodes is how many nodes the cluster is founded with: 1, or 3
	// to 5. At 1 the section is off and Forgejo is the standard product;
	// from 3 the node founds or joins the cluster by itself from PEERS, and
	// afterwards every live member votes. 2 is refused: two nodes cannot
	// fail over safely without a third vote. 0 is the manual mode:
	// `forgejo cluster init` and `bootstrap` by hand.
	ExpectedNodes int
	// DeadMemberTimeout is how long a member stays out of the leader's
	// reach before the autonomous mode removes it; one minute at least.
	DeadMemberTimeout time.Duration
	// Driver names the compiled consensus binding.
	Driver string
	// DataDir holds the consensus log and snapshots only.
	DataDir string
	// Slot is the member's slot, the one its certificate's URI SAN names.
	Slot string
	// ClusterID is the cluster's id in lowercase hex, used by `cluster init`.
	ClusterID string
	// ListenAddr is the consensus listener, bound to the node's management
	// service address. ObjectListenAddr is the object channel's.
	ListenAddr       string
	ObjectListenAddr string
	// Peers is the seed map slot -> consensus address. Addresses that
	// matter come from the committed membership afterwards (D8).
	Peers map[string]string
	// PeerPins are the SPKI SHA-256 (hex) of the members a joining node
	// trusts, slot=hex: its JOIN counts only an answer from one of them.
	// Required to join by itself; read from `forgejo cluster status` or the
	// administration page of a member.
	PeerPins map[string]string
	// ObjectPort is where every member's object channel listens.
	ObjectPort string
	// ObjectPeers overrides, per slot, the object channel's address, for
	// members that do not share a port (several nodes on one host).
	ObjectPeers map[string]string
	// TLS identity of the node: certificate and key (one file is allowed,
	// replaced atomically by the renewer), and the cluster's CA.
	TLSCertFile string
	TLSKeyFile  string
	TLSCAFile   string
	// HookSocket is the private socket the reference-transaction hook and
	// the offline commands reach the replicator on (ADR 0148).
	HookSocket string
	// Operators seed the cluster-operator permission, held in the database,
	// when nobody holds it yet; afterwards they are ignored.
	Operators container.Set[string]
	// StepUpMaxAge is how long a re-authentication allows mutating
	// operations.
	StepUpMaxAge time.Duration
	// StepUpOIDCSource names the OpenID Connect authentication source an
	// operator linked to it re-authenticates with; StepUpACR, when set, is
	// the authentication class required (for example a WebAuthn class).
	StepUpOIDCSource string
	StepUpACR        string
	// StepUpPasswordOperators keeps the password and TOTP step-up for these
	// logins (break-glass) when STEP_UP_OIDC_SOURCE is set.
	StepUpPasswordOperators container.Set[string]
	// ReadAfterWriteTimeout bounds how long a request waits for the local
	// projection to reach the caller's last write.
	ReadAfterWriteTimeout time.Duration
	MaxLogBytes           int64
	MaxSnapshotBytes      int64
	// AckMargin is how far event trimming lags the consumer's cursor.
	AckMargin uint64
	// TxnTTL bounds a native git transaction's registration (R6-06).
	TxnTTL time.Duration
	// JoinGrantTTL is how long an operator's grant lets a node join a slot.
	JoinGrantTTL  time.Duration
	MaxTxnPerRepo int
	MaxTxnTotal   int
	// Witnesses are the Recover witnesses' URLs (one per witness host), paired in
	// order with WitnessKeys, their Ed25519 public keys in hex.
	Witnesses   []string
	WitnessKeys []string
	// WitnessOverrideSHA256 is the hash of the operator's boot override.
	WitnessOverrideSHA256 string
	// The in-process certificate renewer: Vault's PKI engine signs a CSR
	// made from the host key. Off while RenewVaultAddr is empty.
	RenewVaultAddr      string
	RenewVaultMount     string
	RenewVaultRole      string
	RenewVaultTokenFile string
	RenewTTL            time.Duration
	// RenewVaultJWTFile is the allocation's workload identity JWT, logged in
	// with at <RenewVaultAuthMount> under <RenewVaultAuthRole> (one role per
	// slot); RenewVaultTokenFile is then the fallback when the login fails.
	RenewVaultJWTFile   string
	RenewVaultAuthMount string
	RenewVaultAuthRole  string
	// RenewVaultCAFile verifies Vault's HTTPS certificate when it comes from
	// a private PKI; the system roots otherwise.
	RenewVaultCAFile string
	// The Nomad API, read-only (node:read), for the JOIN drain check: a
	// self-join is refused while the joiner's Nomad node is draining or
	// ineligible. NomadAddr is http(s):// or unix:// (the task API socket);
	// off while empty. The joiner's node is the one whose meta
	// NomadSlotMeta names its slot.
	NomadAddr      string
	NomadTokenFile string
	NomadSlotMeta  string
	// NomadCAFile verifies Nomad's HTTPS certificate (private PKI).
	NomadCAFile string
	// ConfigError is set when the section is enabled and invalid. The
	// replicator refuses to start on it; the hook refuses every push.
	ConfigError error
}{
	Driver:                "kitsunium",
	ObjectPort:            "7481",
	StepUpMaxAge:          5 * time.Minute,
	ReadAfterWriteTimeout: 2 * time.Second,
	MaxLogBytes:           1 << 30,
	MaxSnapshotBytes:      1 << 30,
	AckMargin:             1024,
	TxnTTL:                30 * time.Second,
	JoinGrantTTL:          time.Hour,
	DeadMemberTimeout:     5 * time.Minute,
	MaxTxnPerRepo:         64,
	MaxTxnTotal:           4096,
}

var clusterSlotPattern = regexp.MustCompile(`^[a-z0-9-]{1,32}$`)

func loadClusterFrom(rootCfg ConfigProvider) {
	sec := rootCfg.Section("cluster")
	// Asked before Key, which creates the key it reads.
	expectedSet, enabledSet := sec.HasKey("EXPECTED_NODES"), sec.HasKey("ENABLED")
	Cluster.ExpectedNodes = sec.Key("EXPECTED_NODES").MustInt(0)
	// A size of three or more turns the replicator on by itself.
	Cluster.Enabled = sec.Key("ENABLED").MustBool(expectedSet && Cluster.ExpectedNodes >= 3)
	Cluster.Driver = sec.Key("DRIVER").MustString(Cluster.Driver)
	Cluster.DataDir = sec.Key("DATA_DIR").MustString(filepath.Join(AppDataPath, "cluster"))
	if !filepath.IsAbs(Cluster.DataDir) {
		Cluster.DataDir = filepath.Join(AppWorkPath, Cluster.DataDir)
	}
	Cluster.Slot = sec.Key("SLOT").String()
	Cluster.ClusterID = strings.ToLower(sec.Key("CLUSTER_ID").String())
	Cluster.ListenAddr = sec.Key("LISTEN_ADDR").String()
	Cluster.ObjectPort = sec.Key("OBJECT_PORT").MustString(Cluster.ObjectPort)
	Cluster.ObjectListenAddr = sec.Key("OBJECT_LISTEN_ADDR").String()
	if Cluster.ObjectListenAddr == "" && Cluster.ListenAddr != "" {
		if host, _, err := net.SplitHostPort(Cluster.ListenAddr); err == nil {
			Cluster.ObjectListenAddr = net.JoinHostPort(host, Cluster.ObjectPort)
		}
	}
	Cluster.Peers = map[string]string{}
	for _, p := range sec.Key("PEERS").Strings(",") {
		slot, addr, ok := strings.Cut(strings.TrimSpace(p), "=")
		if ok {
			Cluster.Peers[strings.TrimSpace(slot)] = strings.TrimSpace(addr)
		}
	}
	Cluster.PeerPins = map[string]string{}
	for _, p := range sec.Key("PEER_PINS").Strings(",") {
		slot, pin, ok := strings.Cut(strings.TrimSpace(p), "=")
		if !ok || strings.TrimSpace(slot) == "" {
			// Kept so validation refuses it: a dropped entry would leave a
			// joining node trusting fewer members than configured.
			Cluster.PeerPins[strings.TrimSpace(p)] = ""
			continue
		}
		Cluster.PeerPins[strings.TrimSpace(slot)] = strings.ToLower(strings.TrimSpace(pin))
	}
	Cluster.ObjectPeers = map[string]string{}
	for _, p := range sec.Key("OBJECT_PEERS").Strings(",") {
		slot, addr, ok := strings.Cut(strings.TrimSpace(p), "=")
		if ok {
			Cluster.ObjectPeers[strings.TrimSpace(slot)] = strings.TrimSpace(addr)
		}
	}
	Cluster.TLSCertFile = sec.Key("TLS_CERT_FILE").String()
	Cluster.TLSKeyFile = sec.Key("TLS_KEY_FILE").MustString(Cluster.TLSCertFile)
	Cluster.TLSCAFile = sec.Key("TLS_CA_FILE").String()
	Cluster.HookSocket = sec.Key("HOOK_SOCKET").MustString(filepath.Join(AppDataPath, "cluster-run", "hook.sock"))
	Cluster.Operators = container.SetOf(sec.Key("OPERATORS").Strings(",")...)
	Cluster.StepUpMaxAge = clusterDuration(sec, "STEP_UP_MAX_AGE", Cluster.StepUpMaxAge)
	Cluster.ReadAfterWriteTimeout = clusterDuration(sec, "READ_AFTER_WRITE_TIMEOUT", Cluster.ReadAfterWriteTimeout)
	Cluster.MaxLogBytes = sec.Key("MAX_LOG_BYTES").MustInt64(Cluster.MaxLogBytes)
	Cluster.MaxSnapshotBytes = sec.Key("MAX_SNAPSHOT_BYTES").MustInt64(Cluster.MaxSnapshotBytes)
	Cluster.AckMargin = uint64(sec.Key("ACK_MARGIN").MustInt64(int64(Cluster.AckMargin)))
	Cluster.TxnTTL = clusterDuration(sec, "TXN_TTL", Cluster.TxnTTL)
	Cluster.MaxTxnPerRepo = sec.Key("MAX_TXN_PER_REPO").MustInt(Cluster.MaxTxnPerRepo)
	Cluster.JoinGrantTTL = clusterDuration(sec, "JOIN_GRANT_TTL", Cluster.JoinGrantTTL)
	Cluster.DeadMemberTimeout = clusterDuration(sec, "DEAD_MEMBER_TIMEOUT", 5*time.Minute)
	Cluster.Witnesses = sec.Key("WITNESSES").Strings(",")
	Cluster.WitnessKeys = sec.Key("WITNESS_KEYS").Strings(",")
	Cluster.WitnessOverrideSHA256 = sec.Key("WITNESS_OVERRIDE_SHA256").String()
	Cluster.StepUpOIDCSource = sec.Key("STEP_UP_OIDC_SOURCE").String()
	Cluster.StepUpACR = sec.Key("STEP_UP_ACR").String()
	Cluster.StepUpPasswordOperators = container.Set[string]{}
	for _, name := range sec.Key("STEP_UP_PASSWORD_OPERATORS").Strings(",") {
		Cluster.StepUpPasswordOperators.Add(strings.ToLower(name))
	}
	Cluster.RenewVaultAddr = sec.Key("RENEW_VAULT_ADDR").String()
	Cluster.RenewVaultMount = sec.Key("RENEW_VAULT_MOUNT").MustString("pki-consensus")
	Cluster.RenewVaultRole = sec.Key("RENEW_VAULT_ROLE").MustString(Cluster.Slot)
	Cluster.RenewVaultTokenFile = sec.Key("RENEW_VAULT_TOKEN_FILE").String()
	Cluster.RenewTTL = clusterDuration(sec, "RENEW_TTL", 720*time.Hour)
	Cluster.RenewVaultJWTFile = sec.Key("RENEW_VAULT_JWT_FILE").String()
	Cluster.RenewVaultAuthMount = sec.Key("RENEW_VAULT_AUTH_MOUNT").MustString("jwt-consensus")
	Cluster.RenewVaultAuthRole = sec.Key("RENEW_VAULT_AUTH_ROLE").MustString(Cluster.Slot)
	Cluster.NomadAddr = sec.Key("NOMAD_ADDR").String()
	Cluster.NomadTokenFile = sec.Key("NOMAD_TOKEN_FILE").String()
	Cluster.NomadSlotMeta = sec.Key("NOMAD_SLOT_META").MustString("forgejo_slot")
	Cluster.RenewVaultCAFile = sec.Key("RENEW_VAULT_CA_FILE").String()
	Cluster.NomadCAFile = sec.Key("NOMAD_CA_FILE").String()
	Cluster.MaxTxnTotal = sec.Key("MAX_TXN_TOTAL").MustInt(Cluster.MaxTxnTotal)

	Cluster.ConfigError = nil
	if expectedSet {
		explicitOff := enabledSet && !Cluster.Enabled
		if err := clusterSize(explicitOff); err != nil {
			log.Error("[cluster] %v", err)
			if !explicitOff {
				// Fail closed: a typo must never turn a member into a
				// standalone forge that accepts writes on its own.
				Cluster.Enabled = true
				Cluster.ConfigError = err
			}
			return
		}
	}
	if Cluster.Enabled {
		Cluster.ConfigError = validateCluster()
		if Cluster.ConfigError != nil {
			log.Error("[cluster] is enabled and invalid: %v", Cluster.ConfigError)
		}
	}
}

// clusterSize applies EXPECTED_NODES. At 1 the section is off, exactly as
// if it were absent, unless the data directory holds a cluster's log: that
// node was a member, and alone it would fork the repositories. An explicit
// ENABLED = false is the operator saying so, and is obeyed.
func clusterSize(explicitOff bool) error {
	n := Cluster.ExpectedNodes
	if n == 2 {
		return errors.New("EXPECTED_NODES = 2 is refused: two nodes cannot fail over safely without a third vote, since neither can tell a dead partner from a cut link; choose 1 (the standard product, one node) or 3 and more")
	}
	if n < 1 || n > 5 {
		return fmt.Errorf("EXPECTED_NODES = %d: it must be 1 (the standard product) or 3 to 5", n)
	}
	if n > 1 {
		return nil
	}
	if entries, err := os.ReadDir(Cluster.DataDir); err == nil && len(entries) > 0 && !explicitOff {
		return fmt.Errorf("EXPECTED_NODES = 1, but DATA_DIR %s holds a cluster's data: this node was a member, and alone it would fork the repositories; move the directory away deliberately to run it standalone", Cluster.DataDir)
	}
	Cluster.Enabled = false
	return nil
}

func clusterDuration(sec ConfigSection, key string, def time.Duration) time.Duration {
	d, err := sec.Key(key).MustDuration(def)
	if err != nil {
		log.Error("[cluster] %s: %v, using %s", key, err, def)
		return def
	}
	return d
}

func validateCluster() error {
	var errs []error
	if !clusterSlotPattern.MatchString(Cluster.Slot) {
		errs = append(errs, fmt.Errorf("SLOT %q must match [a-z0-9-]{1,32}", Cluster.Slot))
	}
	if Cluster.ListenAddr == "" {
		errs = append(errs, errors.New("LISTEN_ADDR is required: the node's management service address"))
	} else if host, _, err := net.SplitHostPort(Cluster.ListenAddr); err != nil || host == "" || host == "0.0.0.0" || host == "::" {
		errs = append(errs, errors.New("LISTEN_ADDR must name the management address explicitly, never a wildcard"))
	}
	if Cluster.TLSCertFile == "" || Cluster.TLSCAFile == "" {
		errs = append(errs, errors.New("TLS_CERT_FILE and TLS_CA_FILE are required"))
	}
	if !DisableGitHooks {
		errs = append(errs, errors.New("[security] DISABLE_GIT_HOOKS must be true: a user hook would run inside the replication path"))
	}
	// sun_path holds 104 bytes on BSD and macOS, 108 on Linux, NUL included.
	if len(Cluster.HookSocket) > 103 {
		errs = append(errs, fmt.Errorf("HOOK_SOCKET %q is longer than a Unix socket path may be (103 bytes)", Cluster.HookSocket))
	}
	if Cluster.MaxLogBytes <= 0 || Cluster.MaxSnapshotBytes <= 0 {
		errs = append(errs, errors.New("MAX_LOG_BYTES and MAX_SNAPSHOT_BYTES must be positive"))
	}
	if len(Cluster.Witnesses) != len(Cluster.WitnessKeys) {
		errs = append(errs, errors.New("WITNESSES and WITNESS_KEYS must pair one key with each witness"))
	}
	if Cluster.RenewVaultAddr != "" && Cluster.RenewVaultTokenFile == "" && Cluster.RenewVaultJWTFile == "" {
		errs = append(errs, errors.New("RENEW_VAULT_JWT_FILE or RENEW_VAULT_TOKEN_FILE is required with RENEW_VAULT_ADDR"))
	}
	if Cluster.NomadAddr != "" && !strings.HasPrefix(Cluster.NomadAddr, "http://") && !strings.HasPrefix(Cluster.NomadAddr, "https://") && !strings.HasPrefix(Cluster.NomadAddr, "unix://") {
		errs = append(errs, fmt.Errorf("NOMAD_ADDR %q must start with http://, https:// or unix://", Cluster.NomadAddr))
	}
	for slot, pin := range Cluster.PeerPins {
		if b, err := hex.DecodeString(pin); err != nil || len(b) != 32 {
			errs = append(errs, fmt.Errorf("PEER_PINS: %q is not slot=<64 hex digits>", slot))
		}
	}
	for slot := range Cluster.Peers {
		if !clusterSlotPattern.MatchString(slot) {
			errs = append(errs, fmt.Errorf("PEERS: slot %q is invalid", slot))
		}
	}
	if Cluster.ExpectedNodes >= 3 {
		errs = append(errs, validateAutonomous()...)
	}
	return errors.Join(errs...)
}

// validateAutonomous checks what the autonomous mode reads: PEERS names
// every other slot of the cluster, no more, no fewer, the same list on
// every node, so the founder is the same for all.
func validateAutonomous() []error {
	var errs []error
	others := 0
	for slot := range Cluster.Peers {
		if slot != Cluster.Slot {
			others++
		}
	}
	if Cluster.DeadMemberTimeout < time.Minute {
		errs = append(errs, fmt.Errorf("DEAD_MEMBER_TIMEOUT %s is below one minute: a member restarting would be removed", Cluster.DeadMemberTimeout))
	}
	if others != Cluster.ExpectedNodes-1 {
		errs = append(errs, fmt.Errorf("PEERS names %d other slots: EXPECTED_NODES = %d needs exactly %d, the same list on every node", others, Cluster.ExpectedNodes, Cluster.ExpectedNodes-1))
	}
	if Cluster.ClusterID != "" {
		if b, err := hex.DecodeString(Cluster.ClusterID); err != nil || len(b) != 16 {
			errs = append(errs, fmt.Errorf("CLUSTER_ID %q must be 32 hex digits", Cluster.ClusterID))
		}
	}
	return errs
}
