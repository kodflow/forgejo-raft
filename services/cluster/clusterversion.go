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
// never by its pin alone. The SDK speaks 3 since kitsunium/sdk#659, and
// ADR 0152's rule for the raise to 3 (D19, amendment of kitsunium/sdk#665)
// orders three steps:
//  1. a release with an SDK holding that amendment, still at protocol 2,
//     runs on every member: its leader holds every member to the vote
//     window, and withholds from a peer of protocol 2 a snapshot it cannot
//     read;
//  2. no binary of protocol 2, a Forgejo built before #227, can come back:
//     none is left in the deployment, kept as a rollback target, allowed to
//     JOIN or used for an offline command (cluster recover, restore,
//     inspect) over a data directory of the cluster, and the floor of a
//     rollback is the release of step 1. Such a node, back after the raise
//     to 3, does not stop: it is withheld every snapshot it cannot read,
//     answers the heartbeats, is counted by CheckQuorum and holds no new
//     entry;
//  3. only then a later release sets clusterProtocol to 3, which the
//     engine admits once every member answers within the vote window
//     announcing it.
//
// A release with this pin is step 1: clusterProtocol stays 2.
const clusterProtocol uint32 = 2

// ActionRaiseCluster is what the replicated audit records of a raise of the
// cluster version (Target). No operator asks for it: the leader makes it
// once every member announces the protocol.
const ActionRaiseCluster Action = "raise-cluster-version"

// raiseClusterVersion raises, on a leader that a majority answers, the
// cluster version to clusterProtocol, against the lineage and the
// generation of the status it reads. The engine refuses it
// VERSION_UNSUPPORTED while a member of the latest configuration, voter or
// learner, was not heard in this term announcing the protocol: a member
// still on a binary of protocol 1 would stop on the raise and on every
// format written after it. It refuses it as well while such a member was
// last heard a vote window ago or more, the minimum election timeout,
// whatever it announced: a member that announced the protocol and then went
// down may come back on an older binary (kitsunium/sdk#665). A rolling
// upgrade is therefore raised once its last member runs this binary and
// answers, a raise just after the start waits for every member to be heard,
// and a refusal is retried at the next pass: the cluster keeps writing
// meanwhile. The raise is never undone: from it on, a member rolled back to
// a binary of protocol 1 stops, it never forks.
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

// leaderPilot is what the leader does by itself outside the autonomous
// mode, where no autopilot runs, at every tick: raise the cluster version
// (#170), and hand the leadership to a member that takes the checkpoints
// when this one takes none (#176). On every member, it keeps the hello
// saying that this node could not derive the SQL when it last led, so the
// leadership is not handed back to it for its checkpoints (#217).
func (r *Replicator) leaderPilot(ctx context.Context, every time.Duration) {
	t := time.NewTicker(every)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
		r.keepSQLYield(r.clock())
		eng := r.Engine()
		if eng == nil || eng.Status().Role != consensus.RoleLeader {
			continue
		}
		pctx, cancel := context.WithTimeout(ctx, 10*time.Second)
		if !r.maybeYieldForCheckpoints(pctx, eng, eng.Membership()) {
			r.raiseClusterVersion(pctx, eng)
		}
		cancel()
	}
}
