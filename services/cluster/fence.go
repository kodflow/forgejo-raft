// Copyright 2026 The Forgejo Authors. All rights reserved.
// SPDX-License-Identifier: GPL-3.0-or-later

package cluster

import (
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"strings"
	"time"

	"forgejo.org/modules/log"
	"forgejo.org/modules/setting"
	"forgejo.org/services/cluster/consensus"
	"forgejo.org/services/cluster/witness"
)

// ExitFenced is the exit code of a node the witnesses fence: distinct, so a
// supervisor, Nomad or systemd, tells it from a crash and does not loop.
const ExitFenced = 77

// ExitWitnessConfig is the exit code of `witness check` when the
// witnesses' settings are wrong: neither a fence nor an outage.
const ExitWitnessConfig = 78

// ErrWitnessConfig wraps an error of the [cluster] WITNESSES settings.
var ErrWitnessConfig = errors.New("[cluster] WITNESSES or WITNESS_KEYS")

// EnvWitnessOverride carries the operator's override secret at boot.
const EnvWitnessOverride = "FORGEJO_CLUSTER_WITNESS_OVERRIDE"

// FenceCheck is the boot check of ADR 0152 R9-09, run before the data
// directory is opened: the witnesses are asked whether this node's cluster
// was superseded by a Recover. It returns nil to start, and an error to stay
// down. An operator override, whose secret hashes to
// WITNESS_OVERRIDE_SHA256, lets the node start anyway, and is logged.
func FenceCheck(ctx context.Context, cluster consensus.ClusterID) error {
	if len(setting.Cluster.Witnesses) == 0 {
		log.Warn("cluster: no [cluster] WITNESSES configured: the Recover fence is not checked at boot")
		return nil
	}
	ws, err := witness.ParseWitnesses(setting.Cluster.Witnesses, setting.Cluster.WitnessKeys)
	if err != nil {
		return fmt.Errorf("%w: %w", ErrWitnessConfig, err)
	}
	ctx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()
	answers, err := witness.Check(ctx, ws, cluster)
	for _, a := range answers {
		switch a.Verdict {
		case witness.Clear:
			log.Info("cluster: witness %s says cluster %s is clear", a.URL, cluster)
		case witness.Superseded:
			log.Error("cluster: witness %s says cluster %s was superseded by %s", a.URL, cluster, a.By)
		default:
			log.Warn("cluster: witness %s did not answer: %v", a.URL, a.Err)
		}
	}
	if err == nil {
		return nil
	}
	if overridden() {
		logOverride(err)
		return nil
	}
	return err
}

// witnessRetryFirst and witnessRetryMax bound the pause between two rounds
// of FenceWait.
var (
	witnessRetryFirst = 5 * time.Second
	witnessRetryMax   = time.Minute
)

// FenceWait is FenceCheck that waits out witnesses that do not answer: the
// node stays down, as no witness said "clear", and asks again with a
// growing pause until one answers or ctx ends (#68). An outage of the
// witnesses is not a verdict: exiting with ExitFenced on it kept the node
// down for good after the witnesses came back, as a supervisor does not
// restart a fenced node.
func FenceWait(ctx context.Context, cluster consensus.ClusterID) error {
	pause := witnessRetryFirst
	for {
		err := FenceCheck(ctx, cluster)
		if !errors.Is(err, witness.ErrNoWitness) {
			return err
		}
		log.Warn("cluster: no witness answered for cluster %s: this node stays down and asks again in %s", cluster, pause)
		select {
		case <-ctx.Done():
			return err
		case <-time.After(pause):
		}
		pause = min(2*pause, witnessRetryMax)
	}
}

// logOverride records an operator override; tests watch it.
var logOverride = func(err error) {
	log.Warn("cluster: FENCE OVERRIDDEN by the operator at boot (%s): starting although %v", EnvWitnessOverride, err)
}

// overridden reports whether the override secret in the environment
// matches the configured hash.
func overridden() bool {
	secret := os.Getenv(EnvWitnessOverride)
	want := strings.ToLower(setting.Cluster.WitnessOverrideSHA256)
	if secret == "" || want == "" {
		return false
	}
	sum := sha256.Sum256([]byte(secret))
	return subtle.ConstantTimeCompare([]byte(hex.EncodeToString(sum[:])), []byte(want)) == 1
}

// fenceOrExit runs the boot check and ends the process with ExitFenced when
// it refuses. Only Forgejo stops: Consul, Nomad and Vault are not concerned
// (R9-11).
func fenceOrExit(ctx context.Context, dataDir string, driver consensus.Driver) error {
	rep, err := driver.Inspect(dataDir)
	if errors.Is(err, os.ErrNotExist) {
		return nil // no data directory yet: no lineage to be fenced from
	}
	if err != nil {
		// A data directory that cannot be read cannot be cleared either.
		return fmt.Errorf("[cluster] witness check: reading %s: %w", dataDir, err)
	}
	if rep.Cluster == (consensus.ClusterID{}) {
		return nil // not stamped yet: Open says why, or the autonomous mode checks the id it stamps
	}
	return fenceClusterOrExit(ctx, rep.Cluster)
}

// fenceClusterOrExit is the boot check for one cluster id.
func fenceClusterOrExit(ctx context.Context, cluster consensus.ClusterID) error {
	if err := FenceWait(ctx, cluster); err != nil {
		if errors.Is(err, witness.ErrSuperseded) {
			log.Error("cluster: refusing to start: %v; exit %d", err, ExitFenced)
			log.GetManager().Close()
			os.Exit(ExitFenced)
		}
		return fmt.Errorf("[cluster] witness check: %w", err)
	}
	return nil
}
