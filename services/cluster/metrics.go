// Copyright 2026 The Forgejo Authors. All rights reserved.
// SPDX-License-Identifier: GPL-3.0-or-later

package cluster

import (
	"time"

	"forgejo.org/services/cluster/consensus"

	"github.com/prometheus/client_golang/prometheus"
)

// The replicator's metrics, spelled as Prometheus exposes them: underscores,
// a _total suffix on counters, units in the name (ADR 0152 D13, R7-37). The
// engine's own consensus_* metrics are the SDK's; these are Forgejo's view:
// projection, event queue, consumer and git transactions.
const metricsNamespace = "forgejo_cluster_"

var (
	proposalsTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: metricsNamespace + "ref_proposals_total",
		Help: "Ref transactions proposed by the hook path, by outcome: committed, refused, unknown, error.",
	}, []string{"outcome"})
	objectShipsTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: metricsNamespace + "object_ships_total",
		Help: "Object shipments to a majority before a ref is proposed, by outcome: ok, failed.",
	}, []string{"outcome"})
	dispatchedTotal = prometheus.NewCounter(prometheus.CounterOpts{
		Name: metricsNamespace + "consumer_dispatched_events_total",
		Help: "Events whose post-receive work the consumer committed with its cursor.",
	})
	dispatchFailuresTotal = prometheus.NewCounter(prometheus.CounterOpts{
		Name: metricsNamespace + "consumer_failures_total",
		Help: "Batches the consumer rolled back because their work failed.",
	})
	parkedTotal = prometheus.NewCounter(prometheus.CounterOpts{
		Name: metricsNamespace + "consumer_parked_events_total",
		Help: "Events whose post-receive work the consumer parked, never dropped, until a retry runs it.",
	})
	unparkedTotal = prometheus.NewCounter(prometheus.CounterOpts{
		Name: metricsNamespace + "consumer_unparked_events_total",
		Help: "Parked events whose post-receive work a retry committed.",
	})
	trimHeldTotal = prometheus.NewCounter(prometheus.CounterOpts{
		Name: metricsNamespace + "consumer_trim_held_total",
		Help: "Trims of the event queue held back because SQL's durability was not confirmed.",
	})
	backupTrimHeldTotal = prometheus.NewCounter(prometheus.CounterOpts{
		Name: metricsNamespace + "consumer_backup_trim_held_total",
		Help: "Trims of the event queue held at the backup cursor while the PostgreSQL backup lags beyond its bound.",
	})
	cursorRegressionsTotal = prometheus.NewCounter(prometheus.CounterOpts{
		Name: metricsNamespace + "consumer_cursor_regressions_total",
		Help: "Times the consumer read a cursor behind one it had seen (a database failover or restore): the events past it are replayed from the log.",
	})
	checkpointsTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: metricsNamespace + "checkpoints_total",
		Help: "Checkpoints the coordinator took on this node while it led, by outcome: closed, failed.",
	}, []string{"outcome"})
	exportsTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: metricsNamespace + "exports_total",
		Help: "Export calls served to observers, by op and outcome: ok, refused.",
	}, []string{"op", "outcome"})
)

type collector struct {
	role, term, commit, applied, tolerance, hasLeader, ready, fenced *prometheus.Desc
	reposPending, reposFailing, eventsPending, eventBytes            *prometheus.Desc
	consumerLag, txns                                                *prometheus.Desc
	certExpiry, applyLag                                             *prometheus.Desc
	peerLag, peerUnreachable, peerLastContact                        *prometheus.Desc
	refused, fsFree                                                  *prometheus.Desc
	consumerStalled                                                  *prometheus.Desc
	expected, voters                                                 *prometheus.Desc
}

// NewCollector returns the replicator's collector. It reads the running
// replicator at each scrape and exposes nothing when the cluster is off.
func NewCollector() prometheus.Collector {
	d := func(name, help string, labels ...string) *prometheus.Desc {
		return prometheus.NewDesc(metricsNamespace+name, help, labels, nil)
	}
	return &collector{
		role:            d("role", "1 for the node's current role.", "role"),
		term:            d("term", "The node's current term."),
		commit:          d("commit_index", "The highest index the node knows committed."),
		applied:         d("applied_index", "The highest index the node applied to the ref table."),
		tolerance:       d("failure_tolerance", "Voters that can fail with a majority left; known by the leader, -1 elsewhere."),
		hasLeader:       d("has_leader", "1 when the node knows a leader."),
		ready:           d("ready", "1 when the replicator is ready: engine ready, projection caught up once."),
		fenced:          d("projection_fenced", "1 while the projector waits for the start-up barrier."),
		reposPending:    d("projection_pending_repositories", "Repositories not yet projected at their last change."),
		reposFailing:    d("projection_failing_repositories", "Repositories whose last projection failed: they stay behind the table until it succeeds (a lag that does not drain)."),
		eventsPending:   d("events_pending", "Events in the table not yet trimmed."),
		eventBytes:      d("event_queue_bytes", "Accounted size of the event queue."),
		consumerLag:     d("consumer_lag_events", "Events waiting for the dispatcher, as of its last pass."),
		txns:            d("git_transactions_in_flight", "Native git transactions registered and not yet finished."),
		certExpiry:      d("tls_cert_expiry_seconds", "Seconds until the node's certificate expires."),
		applyLag:        d("apply_lag_entries", "Committed entries the node has not applied yet."),
		peerLag:         d("peer_lag_entries", "The leader's view of a peer's lag.", "peer"),
		peerUnreachable: d("peer_unreachable", "1 when the leader cannot reach a peer.", "peer"),
		peerLastContact: d("peer_last_contact_seconds", "Seconds since the leader last heard from a peer.", "peer"),
		refused:         d("transport_refused_before_handshake_total", "Connections the consensus transport refused before TLS, over its per-address budget."),
		consumerStalled: d("consumer_stalled_seconds", "Seconds the dispatcher has waited for this repository to be projected before its event.", "repo"),
		fsFree:          d("fs_free_bytes", "Free bytes of a filesystem the node depends on besides the consensus data directory, which the engine reports.", "fs"),
		expected:        d("expected_nodes", "[cluster] EXPECTED_NODES; 0 in the manual mode."),
		voters:          d("voters", "Voting members of the committed membership, as this node sees it."),
	}
}

func (c *collector) Describe(ch chan<- *prometheus.Desc) {
	for _, d := range []*prometheus.Desc{
		c.role, c.term, c.commit, c.applied, c.tolerance, c.hasLeader, c.ready, c.fenced,
		c.reposPending, c.reposFailing, c.eventsPending, c.eventBytes, c.consumerLag, c.txns, c.certExpiry, c.applyLag,
		c.peerLag, c.peerUnreachable, c.peerLastContact, c.refused, c.fsFree, c.consumerStalled,
		c.expected, c.voters,
	} {
		ch <- d
	}
	proposalsTotal.Describe(ch)
	objectShipsTotal.Describe(ch)
	dispatchedTotal.Describe(ch)
	dispatchFailuresTotal.Describe(ch)
	parkedTotal.Describe(ch)
	unparkedTotal.Describe(ch)
	trimHeldTotal.Describe(ch)
	backupTrimHeldTotal.Describe(ch)
	cursorRegressionsTotal.Describe(ch)
	checkpointsTotal.Describe(ch)
	exportsTotal.Describe(ch)
}

func b2f(b bool) float64 {
	if b {
		return 1
	}
	return 0
}

func (c *collector) Collect(ch chan<- prometheus.Metric) {
	proposalsTotal.Collect(ch)
	objectShipsTotal.Collect(ch)
	dispatchedTotal.Collect(ch)
	dispatchFailuresTotal.Collect(ch)
	parkedTotal.Collect(ch)
	unparkedTotal.Collect(ch)
	trimHeldTotal.Collect(ch)
	backupTrimHeldTotal.Collect(ch)
	cursorRegressionsTotal.Collect(ch)
	checkpointsTotal.Collect(ch)
	exportsTotal.Collect(ch)
	r := Current()
	if r == nil {
		return
	}
	g := func(d *prometheus.Desc, v float64, labels ...string) {
		ch <- prometheus.MustNewConstMetric(d, prometheus.GaugeValue, v, labels...)
	}
	g(c.ready, b2f(r.Ready()))
	g(c.fenced, b2f(r.projector.Fenced()))
	lag := r.projector.Lag()
	failing := 0
	for _, l := range lag {
		if l.Error != "" {
			failing++
		}
	}
	g(c.reposPending, float64(len(lag)))
	g(c.reposFailing, float64(failing))
	g(c.eventsPending, float64(r.table.EventCount()))
	g(c.eventBytes, float64(r.table.EventBytes()))
	g(c.txns, float64(r.registry.count()))
	// The repositories share the disk with the log; a full disk must show
	// as surely as a full quota (ADR 0152 D17, R7-33).
	if free, ok := freeBytes(r.root); ok {
		g(c.fsFree, float64(free), "repositories")
	}
	if r.consumer != nil {
		g(c.consumerLag, float64(r.consumer.Lag()))
		if repo, d := r.consumer.Stalled(); repo != "" {
			g(c.consumerStalled, d.Seconds(), repo)
		}
	}
	if r.objects != nil && r.objects.id != nil {
		if exp := r.objects.id.Expiry(); !exp.IsZero() {
			g(c.certExpiry, time.Until(exp).Seconds())
		}
	}
	eng := r.Engine()
	if eng == nil {
		return
	}
	if t, ok := eng.(interface{ RefusedBeforeHandshake() int64 }); ok {
		ch <- prometheus.MustNewConstMetric(c.refused, prometheus.CounterValue, float64(t.RefusedBeforeHandshake()))
	}
	st := eng.Status()
	h := eng.Health()
	for _, role := range []consensus.Role{consensus.RoleFollower, consensus.RoleCandidate, consensus.RoleLeader, consensus.RoleLearner} {
		g(c.role, b2f(st.Role == role), role.String())
	}
	g(c.term, float64(st.Term))
	g(c.commit, float64(st.Commit))
	g(c.applied, float64(st.Applied))
	g(c.tolerance, float64(h.FailureTolerance))
	g(c.hasLeader, b2f(h.LeaderKnown))
	g(c.applyLag, float64(h.ApplyLagEntries))
	g(c.voters, float64(eng.Membership().Voters()))
	if r.auto != nil {
		g(c.expected, float64(r.auto.expected))
	} else {
		g(c.expected, 0)
	}
	for _, p := range h.Peers {
		id := p.ID.String()
		g(c.peerLag, float64(p.Lag), id)
		g(c.peerUnreachable, b2f(p.Unreachable), id)
		if p.LastContact >= 0 {
			g(c.peerLastContact, p.LastContact.Seconds(), id)
		}
	}
}
