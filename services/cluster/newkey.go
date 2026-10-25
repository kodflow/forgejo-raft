// Copyright 2026 The Forgejo Authors. All rights reserved.
// SPDX-License-Identifier: GPL-3.0-or-later

package cluster

import (
	"context"
	"crypto/x509"
	"errors"
	"fmt"
	"time"

	"forgejo.org/modules/log"
	"forgejo.org/services/cluster/consensus"
	"forgejo.org/services/cluster/reftable"
)

// A key serves one node id for the life of the cluster (#209, decision C of
// kitsunium/sdk#648). A node that takes a new node id, on an empty data
// directory (a lost disk, `quarantine clear --exit wipe-and-rejoin`, the
// replacement of a lost voter) or after its id was retired, first gets a
// new key and its certificate from the renewer, made on the host and
// signed by the CA for its slot (certrenew.Rekey). The cluster's side of
// the rule is the revocation of the key of a member that left: an
// operator's Remove or Deny pin, and the autopilot's removal of a dead
// member (autopilot.go).
//
// Without a renewer ([cluster] RENEW_VAULT_ADDR empty) no key is made: an
// operator installs a new key and certificate. A new stamp on an empty
// directory then keeps the key in use, since nothing on this node says
// whether a member held it, and the cluster refuses it if one did: the
// operator installs a new key and restarts the node, whose open engine
// keeps the key it opened with (keyChangedSinceOpen). A retired id's own
// key is never presented again (keyForRejoin).
//
// The SDK will refuse the key of a retired id at AddLearner and JOIN once
// its protocol is raised (kitsunium/sdk#648, decision A). A new key passes
// both today's rule and that one.

// newKeyRetry is the first wait before a refused new key is asked again,
// doubled up to a minute.
var newKeyRetry = 5 * time.Second

// errNeedsNewKey refuses to rejoin under a new id with the key of the
// retired one when no renewer can make another.
var errNeedsNewKey = errors.New("this node's key is the one its retired node id held, and RENEW_VAULT_ADDR is empty, so no new key is made: install a new key and its certificate, signed by the cluster's CA for this slot, in TLS_KEY_FILE and TLS_CERT_FILE; the node rejoins once it reads them")

// presentedKey is the key the node's certificate on disk carries, which the
// object channel already presents and the next engine opened presents.
func (r *Replicator) presentedKey() [32]byte {
	if r.objects == nil || r.objects.id == nil {
		return [32]byte{}
	}
	return r.objects.id.SPKI()
}

// setOpenedKey records the key the engine about to open presents.
func (r *Replicator) setOpenedKey(k [32]byte) { r.engineKey.Store(&k) }

// openedKey is the key the open engine's transport presents, zero when no
// engine opened through Init or a rejoin.
func (r *Replicator) openedKey() [32]byte {
	if k := r.engineKey.Load(); k != nil {
		return *k
	}
	return [32]byte{}
}

// keyChangedSinceOpen reports whether the certificate on disk carries
// another key than the one the open engine's transport presents: an
// operator installed a new key, or a rejoin made it before its engine
// opened. The engine's transport refuses a reload with another key, so
// the node presents both keys until its engine opens again.
func (r *Replicator) keyChangedSinceOpen() bool {
	opened, cur := r.openedKey(), r.presentedKey()
	return opened != ([32]byte{}) && cur != ([32]byte{}) && cur != opened
}

// keyForNewStamp gives the node a new key before it stamps a new node id on
// an empty data directory. A key the signer refuses is asked again, until
// ctx ends: the hello says the node has not read its data directory yet
// meanwhile, so nothing is founded without it.
func (r *Replicator) keyForNewStamp(ctx context.Context) error {
	if r.renewer == nil {
		// Every founder's first start comes here too: nothing is wrong yet.
		log.Info("cluster: this node takes a new node id with the key of TLS_KEY_FILE, RENEW_VAULT_ADDR being empty: if a member of this cluster holds or held that key, the cluster refuses it; install a new key and its certificate, signed by the cluster's CA for this slot, then restart the node")
		return nil
	}
	wait := newKeyRetry
	for {
		cert, err := r.renewer.Rekey(ctx)
		if err == nil {
			log.Warn("cluster: a new key and its certificate (serial %s) before this node takes a new node id", cert.SerialNumber)
			return nil
		}
		log.Error("cluster: making a new key before this node takes a new node id: %v; asked again in %s", err, wait)
		select {
		case <-ctx.Done():
			return fmt.Errorf("[cluster] making a new key before the node takes a new node id: %w", err)
		case <-time.After(wait):
		}
		wait = min(2*wait, time.Minute)
	}
}

// retiredKey is the key the retired id of this node held, as the last
// membership that listed it says: the open engine's, else the one the
// autopilot kept from before a failed rejoin closed it (lastOwn). A node
// that applied its own removal lists itself nowhere, and it is not known.
func (r *Replicator) retiredKey() ([32]byte, bool) {
	eng := r.Engine()
	if eng == nil {
		return [32]byte{}, false
	}
	self := eng.Status().ID
	for _, m := range []consensus.Membership{eng.Membership(), r.auto.lastOwn} {
		if i := Rank(m, self) - 1; i >= 0 && m.Members[i].SPKI != ([32]byte{}) {
			return m.Members[i].SPKI, true
		}
	}
	return [32]byte{}, false
}

// freshForRejoin reports whether the key on disk may serve the new id of a
// node whose id the cluster retired: neither revoked nor asked for
// revocation in this node's table, and another than the key the retired id
// held. That key is the one its membership lists, whatever key the engine
// opened with: an operator who installs a new key may restart the node
// before it rejoins. When no membership lists the node any more, it
// applied its own removal, and with it the request for the revocation of
// its key, which the cluster commits before the removal from
// reftable.WithdrawVersion: a key the table neither revoked nor asked for
// is then another one. Below that version, or on an empty table, only a key
// that changed since the engine opened is known to be another.
func (r *Replicator) freshForRejoin() bool {
	cur := r.presentedKey()
	if cur == ([32]byte{}) || r.revokedOrAsked(cur) {
		return false
	}
	if held, ok := r.retiredKey(); ok {
		return cur != held
	}
	return r.table.WriteVersion() >= reftable.WithdrawVersion || r.keyChangedSinceOpen()
}

// keyForRejoin gives a node whose id the cluster retired a new key before it
// rejoins under a new id, unless the key on disk is already a fresh one
// (freshForRejoin): an operator installed it, or a rejoin that stopped
// after its new key made it. Without a renewer, the retired id's key is
// refused (errNeedsNewKey) and nothing else is done.
func (r *Replicator) keyForRejoin(ctx context.Context) error {
	if r.freshForRejoin() {
		return nil
	}
	if r.renewer == nil {
		return errNeedsNewKey
	}
	cert, err := r.renewer.Rekey(ctx)
	if err != nil {
		return fmt.Errorf("making a new key before rejoining under a new id: %w", err)
	}
	log.Warn("cluster: a new key and its certificate (serial %s): the retired node id's key never joins again", cert.SerialNumber)
	return nil
}

// ErrNoRenewer says no new key was made: [cluster] RENEW_VAULT_ADDR is
// empty, and the key in use stays.
var ErrNoRenewer = errors.New("RENEW_VAULT_ADDR is empty: no new key is made")

// RekeyForNewID gives the node a new key and its certificate before an
// offline command stamps a new node id (`forgejo cluster init`), when
// [cluster] RENEW_VAULT_ADDR names a renewer, and returns the new
// certificate. Without a renewer it returns ErrNoRenewer: the key in use
// then stays, and the cluster refuses it if one of its members holds or
// held it.
func RekeyForNewID(ctx context.Context) (*x509.Certificate, error) {
	rn := NewRenewer()
	if rn == nil {
		return nil, ErrNoRenewer
	}
	return rn.Rekey(ctx)
}
