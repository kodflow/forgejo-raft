// Copyright 2026 The Forgejo Authors. All rights reserved.
// SPDX-License-Identifier: GPL-3.0-or-later

package cluster

import (
	"context"
	"errors"
	"fmt"
	"time"

	cluster_model "forgejo.org/models/cluster"
	"forgejo.org/models/db"
	"forgejo.org/modules/log"
	"forgejo.org/services/cluster/consensus"
)

// ActionResolveParked is the audit's name of an operator's resolution of a
// parked event.
const ActionResolveParked Action = "resolve-parked-event"

// ParkedView is one parked entry, as the operator sees it.
type ParkedView struct {
	Lineage  string    `json:"lineage"`
	Index    int64     `json:"index"`
	Repo     string    `json:"repo"`
	Events   int       `json:"events"`
	Error    string    `json:"error"`
	Attempts int       `json:"attempts"`
	Since    time.Time `json:"since"`
}

// ListParked lists the parked entries, oldest first.
func (r *Replicator) ListParked(ctx context.Context) ([]ParkedView, error) {
	if db.DefaultContext == nil {
		return nil, errors.New("database not ready")
	}
	rows, err := cluster_model.ListParked(ctx)
	if err != nil {
		return nil, err
	}
	var out []ParkedView
	for _, p := range rows {
		if n := len(out); n > 0 && out[n-1].Lineage == p.Lineage && out[n-1].Index == p.LogIndex {
			out[n-1].Events++
			continue
		}
		out = append(out, ParkedView{
			Lineage: p.Lineage, Index: p.LogIndex, Repo: p.Repo, Events: 1,
			Error: p.Reason, Attempts: p.Attempts, Since: p.CreatedUnix.AsTime(),
		})
	}
	return out, nil
}

// ResolveParked gives up an operator-chosen parked entry: its events and
// the operator's reason are kept in cluster_parked_resolution and in the
// replicated audit, and the trim may then proceed past it. Never without a
// reason.
func (r *Replicator) ResolveParked(ctx context.Context, op consensus.Operator, lineage string, index int64, reason string) ([]*cluster_model.ParkedResolution, error) {
	if db.DefaultContext == nil {
		return nil, errors.New("database not ready")
	}
	res, err := cluster_model.ResolveParked(ctx, lineage, index, string(op), reason)
	if err != nil {
		return nil, err
	}
	target := fmt.Sprintf("%s@%d %s", res[0].Lineage, index, res[0].Repo)
	for _, p := range res {
		log.Warn("cluster: %s resolved the parked event %s@%d of %s without its work (%s); last error: %s; event: %s", op, p.Lineage, p.LogIndex, p.Repo, reason, p.LastError, p.Event)
	}
	if r.Engine() != nil {
		r.audit(ctx, op, Operation{Action: ActionResolveParked, Target: target}, "resolved", reason)
	}
	r.signalConsumer()
	return res, nil
}
