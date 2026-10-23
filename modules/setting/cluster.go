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
	// HookTimeout bounds one call of the reference-transaction hook: the
	// objects shipped to a majority, the proposal and its commit. A push
	// that needs longer fails, nothing written, and its retry ships only
	// what the peers still lack.
	HookTimeout time.Duration
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
	// SQLStandalone declares that no replica of the database can ever be
	// promoted: the consumer's durability check then requires local
	// durability only, not a synchronous standby that holds every commit.
	SQLStandalone bool
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
	// Role is "voter" (the default: a member of the cluster) or
	// "observer": a node that never joins, opens no port and pulls
	// protected checkpoints from the leader over SSH (observer.go).
	Role string
	// Checkpoints, on the members: the coordinator's period (0 is off,
	// which every member must allow before it is turned on), the PostgreSQL
	// role that creates restore points, where the leader keeps each
	// checkpoint's artifacts, and the local WAL archive the export serves.
	CheckpointInterval time.Duration
	CheckpointRole     string
	CheckpointDir      string
	WALArchiveDir      string
	// PGBaseBackup is the pg_basebackup binary, PGBaseBackupDSN its
	// connection string (a replication role, on the members only).
	PGBaseBackup    string
	PGBaseBackupDSN string
	// ObserverRecipients are the observers' X25519 public keys, name=hex:
	// the secrets bundle is sealed to the one an export names.
	ObserverRecipients map[string]string
	// Observer is the observer's side, read when Role is "observer".
	Observer ObserverSettings
	// ConfigError is set when the section is enabled and invalid. The
	// replicator refuses to start on it; the hook refuses every push.
	ConfigError error
}{
	Driver:                "kitsunium",
	ObjectPort:            "7481",
	StepUpMaxAge:          5 * time.Minute,
	ReadAfterWriteTimeout: 2 * time.Second,
	HookTimeout:           60 * time.Second,
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

// The roles of [cluster] ROLE.
const (
	ClusterRoleVoter    = "voter"
	ClusterRoleObserver = "observer"
)

// ObserverSettings configure `forgejo cluster observe`.
type ObserverSettings struct {
	// Name is the observer's name at the members (OBSERVER_RECIPIENTS).
	Name string
	// Dir holds every checkpoint, on the volume the snapshot hook covers.
	Dir string
	// Endpoints map a member's slot to the SSH address its export answers
	// on, user@host:port; an export's leader hint is followed only to them.
	Endpoints map[string]string
	// KnownHostsFile pins each endpoint's host key: an unknown key is
	// refused, never learnt.
	KnownHostsFile string
	// SSHKeyFile is the observer's private key; SecretsKeyFile the X25519
	// key the secrets bundle is sealed to. Both live in an encrypted volume.
	SSHKeyFile     string
	SecretsKeyFile string
	// SnapshotHook is run after each verified checkpoint and must snapshot
	// the whole of Dir atomically; its success makes the checkpoint
	// PROTECTED.
	SnapshotHook string
	PollInterval time.Duration
	// StagingQuota stops ingestion when the staging area reaches it; it
	// never deletes a protected checkpoint.
	StagingQuota int64
	// ExportSilence is how long an export may send no data before the
	// observer cuts it. Keepalives hold the SSH connection open while the
	// member's sshd answers, so this alone bounds an export stuck behind a
	// live sshd; it must outlast a big repository's pack-objects.
	ExportSilence time.Duration
}

// observerRefusedKeys are what an observer may not hold: it never listens,
// never has a slot, is never granted a join and never founds a cluster.
var observerRefusedKeys = []string{
	"ENABLED", "LISTEN_ADDR", "OBJECT_LISTEN_ADDR", "OBJECT_PORT", "OBJECT_PEERS",
	"SLOT", "PEERS", "PEER_PINS", "EXPECTED_NODES", "CLUSTER_ID",
	"JOIN_GRANT_TTL", "OPERATORS", "CHECKPOINT_INTERVAL",
}

func loadClusterFrom(rootCfg ConfigProvider) {
	sec := rootCfg.Section("cluster")
	// Asked before Key, which creates the key it reads.
	expectedSet, enabledSet := sec.HasKey("EXPECTED_NODES"), sec.HasKey("ENABLED")
	var refused []string
	for _, k := range observerRefusedKeys {
		if sec.HasKey(k) {
			refused = append(refused, k)
		}
	}
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
	Cluster.HookTimeout = clusterDuration(sec, "HOOK_TIMEOUT", time.Minute)
	Cluster.MaxLogBytes = sec.Key("MAX_LOG_BYTES").MustInt64(Cluster.MaxLogBytes)
	Cluster.MaxSnapshotBytes = sec.Key("MAX_SNAPSHOT_BYTES").MustInt64(Cluster.MaxSnapshotBytes)
	Cluster.AckMargin = uint64(sec.Key("ACK_MARGIN").MustInt64(int64(Cluster.AckMargin)))
	Cluster.SQLStandalone = sec.Key("SQL_STANDALONE").MustBool(false)
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

	Cluster.CheckpointInterval = clusterDuration(sec, "CHECKPOINT_INTERVAL", 0)
	Cluster.CheckpointRole = sec.Key("CHECKPOINT_ROLE").MustString("forgejo_checkpoint")
	Cluster.CheckpointDir = sec.Key("CHECKPOINT_DIR").MustString(filepath.Join(AppDataPath, "cluster-checkpoints"))
	Cluster.WALArchiveDir = sec.Key("WAL_ARCHIVE_DIR").String()
	Cluster.PGBaseBackup = sec.Key("PG_BASEBACKUP").MustString("pg_basebackup")
	Cluster.PGBaseBackupDSN = sec.Key("PG_BASEBACKUP_DSN").String()
	Cluster.ObserverRecipients = map[string]string{}
	for _, p := range sec.Key("OBSERVER_RECIPIENTS").Strings(",") {
		name, key, _ := strings.Cut(strings.TrimSpace(p), "=")
		Cluster.ObserverRecipients[strings.TrimSpace(name)] = strings.ToLower(strings.TrimSpace(key))
	}

	Cluster.ConfigError = nil
	Cluster.Role = strings.ToLower(sec.Key("ROLE").MustString(ClusterRoleVoter))
	if Cluster.Role == ClusterRoleObserver {
		// An observer is never a member: the replicator stays off, and a
		// section that would make it one is refused.
		Cluster.Enabled = false
		Cluster.ConfigError = loadObserver(sec, refused)
		if Cluster.ConfigError != nil {
			log.Error("[cluster] ROLE = observer is invalid: %v", Cluster.ConfigError)
		}
		return
	}
	if Cluster.Role != ClusterRoleVoter {
		Cluster.Enabled = true
		Cluster.ConfigError = fmt.Errorf("ROLE %q must be voter or observer", Cluster.Role)
		log.Error("[cluster] %v", Cluster.ConfigError)
		return
	}
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

// loadObserver reads and validates the observer's side of [cluster].
func loadObserver(sec ConfigSection, refused []string) error {
	var errs []error
	for _, k := range refused {
		errs = append(errs, fmt.Errorf("%s is refused with ROLE = observer: an observer never listens, never holds a slot, is never granted a join and never founds a cluster", k))
	}
	o := ObserverSettings{
		Name:           sec.Key("OBSERVER_NAME").String(),
		Dir:            sec.Key("OBSERVER_DIR").String(),
		Endpoints:      map[string]string{},
		KnownHostsFile: sec.Key("OBSERVER_KNOWN_HOSTS").String(),
		SSHKeyFile:     sec.Key("OBSERVER_SSH_KEY_FILE").String(),
		SecretsKeyFile: sec.Key("OBSERVER_SECRETS_KEY_FILE").String(),
		SnapshotHook:   sec.Key("OBSERVER_SNAPSHOT_HOOK").String(),
		PollInterval:   clusterDuration(sec, "OBSERVER_POLL_INTERVAL", 15*time.Second),
		StagingQuota:   sec.Key("OBSERVER_STAGING_QUOTA").MustInt64(10 << 30),
		ExportSilence:  clusterDuration(sec, "OBSERVER_EXPORT_SILENCE", 30*time.Minute),
	}
	for _, p := range sec.Key("OBSERVER_ENDPOINTS").Strings(",") {
		slot, addr, ok := strings.Cut(strings.TrimSpace(p), "=")
		slot, addr = strings.TrimSpace(slot), strings.TrimSpace(addr)
		if !ok || !clusterSlotPattern.MatchString(slot) || !validSSHTarget(addr) {
			errs = append(errs, fmt.Errorf("OBSERVER_ENDPOINTS: %q is not slot=user@host:port", p))
			continue
		}
		o.Endpoints[slot] = addr
	}
	if !clusterSlotPattern.MatchString(o.Name) {
		errs = append(errs, fmt.Errorf("OBSERVER_NAME %q must match [a-z0-9-]{1,32}", o.Name))
	}
	if len(o.Endpoints) == 0 {
		errs = append(errs, errors.New("OBSERVER_ENDPOINTS is required: the members' SSH exports"))
	}
	if !filepath.IsAbs(o.Dir) {
		errs = append(errs, errors.New("OBSERVER_DIR must be an absolute path"))
	}
	for key, v := range map[string]string{
		"OBSERVER_KNOWN_HOSTS": o.KnownHostsFile, "OBSERVER_SSH_KEY_FILE": o.SSHKeyFile,
		"OBSERVER_SECRETS_KEY_FILE": o.SecretsKeyFile, "OBSERVER_SNAPSHOT_HOOK": o.SnapshotHook,
	} {
		if v == "" {
			errs = append(errs, fmt.Errorf("%s is required with ROLE = observer", key))
		}
	}
	if o.PollInterval < time.Second {
		errs = append(errs, errors.New("OBSERVER_POLL_INTERVAL must be one second at least"))
	}
	if o.StagingQuota <= 0 {
		errs = append(errs, errors.New("OBSERVER_STAGING_QUOTA must be positive"))
	}
	if o.ExportSilence < time.Minute {
		errs = append(errs, errors.New("OBSERVER_EXPORT_SILENCE must be one minute at least"))
	}
	Cluster.Observer = o
	return errors.Join(errs...)
}

// validSSHTarget accepts user@host:port, nothing that ssh would read as an
// option.
func validSSHTarget(s string) bool {
	user, hostPort, ok := strings.Cut(s, "@")
	if !ok || user == "" || strings.HasPrefix(user, "-") {
		return false
	}
	host, port, err := net.SplitHostPort(hostPort)
	return err == nil && host != "" && port != "" && !strings.HasPrefix(host, "-")
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
	if Cluster.HookTimeout < 20*time.Second || Cluster.HookTimeout > time.Hour {
		errs = append(errs, fmt.Errorf("HOOK_TIMEOUT %s must be between 20s and 1h", Cluster.HookTimeout))
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
	seenWitnessKeys := map[string]bool{}
	for _, k := range Cluster.WitnessKeys {
		// Each witness signs its own fence receipts: one key listed twice
		// would count one witness for two.
		k = strings.ToLower(strings.TrimSpace(k))
		if seenWitnessKeys[k] {
			errs = append(errs, fmt.Errorf("WITNESS_KEYS lists the key %s twice: each witness has its own", k))
		}
		seenWitnessKeys[k] = true
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
	if Cluster.CheckpointInterval < 0 || Cluster.CheckpointInterval > 0 && Cluster.CheckpointInterval < 10*time.Second {
		errs = append(errs, errors.New("CHECKPOINT_INTERVAL must be 0 (off) or 10s at least"))
	}
	for name, key := range Cluster.ObserverRecipients {
		if b, err := hex.DecodeString(key); err != nil || len(b) != 32 || !clusterSlotPattern.MatchString(name) {
			errs = append(errs, fmt.Errorf("OBSERVER_RECIPIENTS: %q is not name=<64 hex digits>", name))
		}
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
