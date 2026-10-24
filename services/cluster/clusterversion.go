// Copyright 2026 The Forgejo Authors. All rights reserved.
// SPDX-License-Identifier: GPL-3.0-or-later

package cluster

import (
	"context"
	"strconv"
	"time"

	"forgejo.org/modules/log"
	"forgejo.org/services/cluster/consensus"
)

// clusterProtocol is the engine's protocol version this binary has the
// cluster speak (ADR 0152 D18). The engine never raises its cluster version
// by itself: a cluster founded by Bootstrap stays at 0, where the
// protections of protocol 2 never apply: the leader record that lets a voter
// stopped while another member was admitted and made leader hear that
// leader again (kitsunium/sdk#542), the checksummed snapshot metadata
// (kitsunium/sdk#546), the origin of an administrative entry
// (kitsunium/sdk#561). Without it, such a voter stays a pre-candidate for as
// long as that leader leads, and one more failure costs the majority
// (#170). The leader raises it in both modes, as the autopilot raises the
// payload version: it is a protocol version, not an operator's decision. An
// SDK speaking a later protocol is raised to by a release that says so,
// never by its pin alone.
const clusterProtocol uint32 = 2

// ActionRaiseCluster is what the replicated audit records of a raise of the
// cluster version (Target). No operator asks for it: the leader makes it
// once every member announces the protocol.
const ActionRaiseCluster Action = "raise-cluster-version"

// raiseClusterVersion raises, on a leader that a majority answers, the
// cluster version to clusterProtocol, against the lineage and the
// generation of the status it reads. The engine refuses it while a member of
// the latest configuration, voter or learner, was not heard in this term
// announcing the protocol: a member still on a binary of protocol 1 would
// stop on the raise and on every format written after it. A rolling upgrade
// is therefore raised once its last member runs this binary, and a refusal
// is retried at the next pass. The raise is never undone: from it on, a
// member rolled back to a binary of protocol 1 stops, it never forks.
func (r *Replicator) raiseClusterVersion(ctx context.Context, eng consensus.Engine) {
	st := eng.Status()
	if st.Role != consensus.RoleLeader || st.ClusterVersion >= clusterProtocol || !eng.Health().QuorumReachable {
		return
	}
	pre := consensus.Precondition{Identity: st.Identity, Generation: st.ClusterGeneration}
	if err := eng.RaiseClusterVersion(ctx, autopilotOperator, pre, clusterProtocol); err != nil {
		log.Debug("cluster: raising the cluster version to protocol %d: %v; retried", clusterProtocol, err)
		return
	}
	why := "every member announces the protocol"
	log.Warn("cluster: %s %d: the cluster version is raised to it", why, clusterProtocol)
	r.audit(ctx, autopilotOperator, Operation{Action: ActionRaiseCluster, Target: strconv.FormatUint(uint64(clusterProtocol), 10)}, "ok", why)
}

// maybeRaiseCluster is the autopilot's raise of the cluster version, at
// most once per payloadProbeEvery of its clock, as maybeRaisePayload.
func (r *Replicator) maybeRaiseCluster(ctx context.Context, eng consensus.Engine) {
	if eng.Status().ClusterVersion >= clusterProtocol || r.auto.now().Before(r.clusterProbe) {
		return
	}
	r.clusterProbe = r.auto.now().Add(payloadProbeEvery)
	r.raiseClusterVersion(ctx, eng)
}

// clusterVersionPilot raises the cluster version outside the autonomous
// mode, where no autopilot runs: at every tick, on the leader alone.
func (r *Replicator) clusterVersionPilot(ctx context.Context, every time.Duration) {
	t := time.NewTicker(every)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
		eng := r.Engine()
		if eng == nil || eng.Status().ClusterVersion >= clusterProtocol {
			continue
		}
		pctx, cancel := context.WithTimeout(ctx, 10*time.Second)
		r.raiseClusterVersion(pctx, eng)
		cancel()
	}
}
