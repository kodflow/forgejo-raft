// Copyright 2026 The Forgejo Authors. All rights reserved.
// SPDX-License-Identifier: GPL-3.0-or-later

package cluster

import (
	"context"
	"errors"
	"sync"

	"forgejo.org/modules/log"
	"forgejo.org/services/cluster/consensus"
)

// WriterHook is the extension point for what must follow the right to
// write, in the autonomous mode. The writer is the node that leads the
// consensus, and it changes with every election. A component that must
// move with it, such as a database whose primary lives on the writer and
// streaming replicas on the other nodes, implements this hook.
//
// Promote runs on a node that became the writer, BEFORE it accepts a
// write: until it returns nil, the node refuses writes, and it is called
// again at the next pass. Demote runs on a node that is not the writer:
// once at start-up when it does not lead, and whenever it loses the
// leadership. Both must be idempotent, since a restart calls them again.
type WriterHook interface {
	Promote(ctx context.Context, t WriterTransition) error
	Demote(ctx context.Context, t WriterTransition) error
}

// WriterTransition says which node changes and in which lineage: a new
// Identity follows a Recover.
type WriterTransition struct {
	Slot     string
	Node     consensus.NodeID
	Identity consensus.LogIdentity
}

var (
	writerHookMu sync.RWMutex
	writerHook   WriterHook
)

// SetWriterHook registers the hook a starting replicator uses; nil removes
// it. It is read once, when the replicator starts.
func SetWriterHook(h WriterHook) {
	writerHookMu.Lock()
	writerHook = h
	writerHookMu.Unlock()
}

func registeredWriterHook() WriterHook {
	writerHookMu.RLock()
	defer writerHookMu.RUnlock()
	return writerHook
}

// writerState is where a node stands with its hook.
type writerState uint8

const (
	writerUnknown writerState = iota
	writerReady
	writerDemoted
)

// writerMark is the term and lineage a node was promoted in: a leadership
// lost and won back between two passes of the autopilot is another term,
// and the hook runs again before the node writes, since another node may
// have been promoted in between.
type writerMark struct {
	Term     uint64
	Identity consensus.LogIdentity
}

func (r *Replicator) promotedFor(st consensus.Status) bool {
	m := r.writerAt.Load()
	return writerState(r.writer.Load()) == writerReady && m != nil && m.Term == st.Term && m.Identity == st.Identity
}

// errBecomingWriter refuses a write on a leader whose hook has not
// promoted it yet.
var errBecomingWriter = errors.New("this node leads the cluster but is not ready to write yet (its writer hook has not completed); retry shortly")

// stepWriter runs the hook for the node's role, from the autopilot.
func (r *Replicator) stepWriter(ctx context.Context, eng consensus.Engine) {
	if r.writerHook == nil || eng == nil {
		return
	}
	st := eng.Status()
	t := WriterTransition{Slot: st.Slot, Node: st.ID, Identity: st.Identity}
	// Only the autopilot's goroutine steps: no two hook calls overlap.
	cur := writerState(r.writer.Load())
	switch leads := st.Role == consensus.RoleLeader; {
	case leads && !r.promotedFor(st):
		// Writes stop here first: a promotion of an earlier term does not
		// cover this one.
		r.writer.Store(uint32(writerUnknown))
		if err := r.writerHook.Promote(ctx, t); err != nil {
			log.Error("cluster: writer hook: promoting %s: %v; writes stay refused, retried", st.Slot, err)
			return
		}
		r.writerAt.Store(&writerMark{Term: st.Term, Identity: st.Identity})
		r.writer.Store(uint32(writerReady))
		log.Warn("cluster: writer hook: %s promoted, writes accepted", st.Slot)
	case !leads && cur != writerDemoted:
		// Writes stop here first, whatever the hook says.
		r.writer.Store(uint32(writerUnknown))
		if err := r.writerHook.Demote(ctx, t); err != nil {
			log.Error("cluster: writer hook: demoting %s: %v; retried", st.Slot, err)
			return
		}
		r.writer.Store(uint32(writerDemoted))
		log.Warn("cluster: writer hook: %s demoted", st.Slot)
	}
}

// writerGate refuses a write on a leader whose hook has not promoted it,
// and holds the consumer's SQL there until then. A proposal another member
// forwards is committed by the engine without it: what the hook moves must
// not make the ref log wait, only what derives from it.
func (r *Replicator) writerGate(eng consensus.Engine) error {
	if r.writerHook == nil || eng == nil {
		return nil
	}
	st := eng.Status()
	if st.Role != consensus.RoleLeader {
		return nil
	}
	if !r.promotedFor(st) {
		return errBecomingWriter
	}
	return nil
}
