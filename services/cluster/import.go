// Copyright 2026 The Forgejo Authors. All rights reserved.
// SPDX-License-Identifier: GPL-3.0-or-later

package cluster

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"forgejo.org/modules/log"
	"forgejo.org/services/cluster/reftable"
)

// TriggerImport marks the entries of an import: they describe refs that
// already existed, so the consumer derives no push from them.
const TriggerImport = "cluster-import"

// ImportReport counts what an import did.
type ImportReport struct {
	Repos    int      `json:"repos"`
	Refs     int      `json:"refs"`
	Skipped  int      `json:"skipped"`
	Failures []string `json:"failures,omitempty"`
}

// Import puts the refs of the repositories this node holds, and the table
// does not, into the table, after shipping their objects to a majority. It
// is how a forge that ran without the cluster joins it: until a ref is in
// the table, a push to it is refused. A ref the table already holds is left
// alone, whatever git says.
func (r *Replicator) Import(ctx context.Context, only string) (ImportReport, error) {
	var rep ImportReport
	owners, err := os.ReadDir(r.root)
	if err != nil {
		return rep, err
	}
	for _, o := range owners {
		if !o.IsDir() {
			continue
		}
		repos, err := os.ReadDir(filepath.Join(r.root, o.Name()))
		if err != nil {
			continue
		}
		for _, d := range repos {
			key := o.Name() + "/" + d.Name()
			if !d.IsDir() || reftable.ValidRepoKey(key) != nil || (only != "" && key != only) {
				continue
			}
			if ctx.Err() != nil {
				return rep, ctx.Err()
			}
			n, err := r.importRepo(ctx, key)
			rep.Repos++
			rep.Refs += n
			if err != nil {
				rep.Failures = append(rep.Failures, fmt.Sprintf("%s: %v", key, err))
				log.Warn("cluster import: %s: %v", key, err)
			}
		}
	}
	return rep, nil
}

func (r *Replicator) importRepo(ctx context.Context, key string) (int, error) {
	dir := filepath.Join(r.root, filepath.FromSlash(key))
	have, err := readGitRefs(ctx, dir)
	if err != nil {
		return 0, err
	}
	current, _ := r.table.Refs(key)
	format := "sha1"
	if out, err := gitRun(ctx, dir, nil, "rev-parse", "--show-object-format"); err == nil && strings.TrimSpace(out) == "sha256" {
		format = "sha256"
	}
	names := make([]string, 0, len(have))
	for name := range have {
		if _, known := current[name]; !known && reftable.ValidRefName(name) == nil {
			names = append(names, name)
		}
	}
	sort.Strings(names)
	total := 0
	for len(names) > 0 {
		n := min(len(names), 512)
		chunk := names[:n]
		names = names[n:]
		tx := &reftable.RefTxn{Repo: key, Format: format, Time: time.Now().UnixNano(), Trigger: TriggerImport}
		var wants []string
		for _, name := range chunk {
			g := have[name]
			if g.Symbolic != "" {
				tx.Updates = append(tx.Updates, reftable.RefUpdate{Ref: name, Symbolic: g.Symbolic})
				continue
			}
			tx.Updates = append(tx.Updates, reftable.RefUpdate{Ref: name, New: g.SHA})
			wants = append(wants, g.SHA)
		}
		if len(wants) > 0 {
			if err := r.objects.shipToMajority(ctx, key, dir, format, wants, nil); err != nil {
				return total, err
			}
		}
		data, err := reftable.Encode(reftable.KindRefTxn, tx)
		if err != nil {
			return total, err
		}
		_, result, err := r.proposeCommand(ctx, data, func() bool { return r.committed(key, tx) })
		if err != nil {
			return total, err
		}
		if !result.OK {
			return total, fmt.Errorf("refused: %s", result.Reason)
		}
		total += len(chunk)
	}
	return total, nil
}
