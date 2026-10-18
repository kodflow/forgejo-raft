// Copyright 2026 The Forgejo Authors. All rights reserved.
// SPDX-License-Identifier: GPL-3.0-or-later

// Package witness is the Recover witness of kitsunium ADR 0152 (D17, Q17,
// Q19, Q20, R8-20, R9-08 to R9-11). Each witness host runs one. It keeps
// the lineages a Recover superseded, written only by the operator on the
// witness host itself, and answers two read questions with Ed25519 signatures: is this
// cluster superseded (the boot check of every Forgejo node), and the
// receipt that an old voter of a superseded cluster is fenced (what Recover
// requires from both witnesses).
package witness

import (
	"crypto/ed25519"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"

	"forgejo.org/modules/json"
	"forgejo.org/services/cluster/consensus"

	sdk "github.com/kitsunium/sdk/pkg/v1/app/consensus"
)

const (
	keyFile   = "witness.key"
	stateFile = "superseded.json"
)

// Record is one superseded lineage: the cluster a Recover replaced, the
// cluster that replaced it, and the old voters fenced by the operator.
type Record struct {
	Old     string   `json:"old"`
	New     string   `json:"new"`
	Voters  []string `json:"voters"`
	Method  string   `json:"method"`
	Written int64    `json:"written"`
}

// Store is a witness's state directory: its key and its records. Only the
// operator writes it, on the witness host, with the CLI; the server reads it.
type Store struct {
	dir string
	mu  sync.Mutex
}

// Open opens a state directory, which must exist with mode 0700.
func Open(dir string) (*Store, error) {
	st, err := os.Stat(dir)
	if err != nil {
		return nil, err
	}
	if !st.IsDir() || st.Mode().Perm()&0o077 != 0 {
		return nil, fmt.Errorf("%s must be a directory of mode 0700", dir)
	}
	return &Store{dir: dir}, nil
}

// Init creates the state directory and the witness key, and returns the
// public key in hex, the one every Forgejo node and Recover are given.
// Running it again returns the existing key.
func Init(dir string) (string, error) {
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return "", err
	}
	if err := os.Chmod(dir, 0o700); err != nil {
		return "", err
	}
	s := &Store{dir: dir}
	k, err := s.key()
	if err == nil {
		return hex.EncodeToString(k.Public().(ed25519.PublicKey)), nil
	}
	// Only a missing key is created: an unreadable one is the witness's
	// identity damaged, and replacing it would silently change the key every
	// node and Recover trust.
	if !errors.Is(err, os.ErrNotExist) {
		return "", fmt.Errorf("%s: %w; the key is left untouched", filepath.Join(dir, keyFile), err)
	}
	pub, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		return "", err
	}
	f, err := os.OpenFile(filepath.Join(dir, keyFile), os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	if err != nil {
		return "", err
	}
	_, err = f.WriteString(hex.EncodeToString(priv.Seed()) + "\n")
	if err == nil {
		err = f.Sync()
	}
	if cerr := f.Close(); err == nil {
		err = cerr
	}
	if err != nil {
		return "", err
	}
	if err := syncDir(dir); err != nil {
		return "", err
	}
	return hex.EncodeToString(pub), nil
}

// syncDir makes a rename or a creation in dir durable.
func syncDir(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	err = d.Sync()
	if cerr := d.Close(); err == nil {
		err = cerr
	}
	return err
}

func (s *Store) key() (ed25519.PrivateKey, error) {
	b, err := os.ReadFile(filepath.Join(s.dir, keyFile))
	if err != nil {
		return nil, err
	}
	seed, err := hex.DecodeString(string(trimSpace(b)))
	if err != nil || len(seed) != ed25519.SeedSize {
		return nil, errors.New("the witness key is not a 32-byte hex seed")
	}
	return ed25519.NewKeyFromSeed(seed), nil
}

func trimSpace(b []byte) []byte {
	for len(b) > 0 && (b[len(b)-1] == '\n' || b[len(b)-1] == ' ' || b[len(b)-1] == '\r') {
		b = b[:len(b)-1]
	}
	return b
}

// Records reads the superseded lineages.
func (s *Store) Records() (map[string]Record, error) {
	b, err := os.ReadFile(filepath.Join(s.dir, stateFile))
	if errors.Is(err, os.ErrNotExist) {
		return map[string]Record{}, nil
	}
	if err != nil {
		return nil, err
	}
	out := map[string]Record{}
	if err := json.Unmarshal(b, &out); err != nil {
		return nil, fmt.Errorf("%s: %w", stateFile, err)
	}
	return out, nil
}

// Supersede records that newCluster replaced old, with its fenced voters.
// It is the operator's write, on the witness host, before the recovered cluster
// serves (R9-09). It refuses to change a record already written, so a
// lineage once superseded stays so.
func (s *Store) Supersede(old, newCluster consensus.ClusterID, voters []consensus.NodeID, method string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	recs, err := s.Records()
	if err != nil {
		return err
	}
	if old == newCluster {
		return errors.New("the new cluster must differ from the old one")
	}
	r, ok := recs[old.String()]
	if ok && r.New != newCluster.String() {
		return fmt.Errorf("cluster %s is already superseded by %s", old, r.New)
	}
	if !ok {
		r = Record{Old: old.String(), New: newCluster.String(), Method: method, Written: time.Now().Unix()}
	}
	// A second call for the same recovery adds voters, never drops one: the
	// receipts of the first call's voters stay obtainable.
	seen := map[string]bool{}
	for _, v := range r.Voters {
		seen[v] = true
	}
	for _, v := range voters {
		if !seen[v.String()] {
			seen[v.String()] = true
			r.Voters = append(r.Voters, v.String())
		}
	}
	sort.Strings(r.Voters)
	recs[old.String()] = r
	b, err := json.MarshalIndent(recs, "", "  ")
	if err != nil {
		return err
	}
	tmp := filepath.Join(s.dir, stateFile+".tmp")
	f, err := os.OpenFile(tmp, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o600)
	if err != nil {
		return err
	}
	_, err = f.Write(b)
	if err == nil {
		err = f.Sync()
	}
	if cerr := f.Close(); err == nil {
		err = cerr
	}
	if err != nil {
		return err
	}
	if err := os.Rename(tmp, filepath.Join(s.dir, stateFile)); err != nil {
		return err
	}
	// The fence is what the boot check relies on: a power loss must not
	// roll the directory entry back to the previous state.
	return syncDir(s.dir)
}

// ---- what is signed ----

// statusMessage is what a witness signs to answer the boot check: the
// cluster asked about, the caller's nonce, whether it is superseded and by
// what. The nonce makes an old "clear" answer useless to a replay.
func statusMessage(cluster, nonce string, superseded bool, by string) []byte {
	b := []byte("forgejo-cluster-witness/status/v1\x00")
	b = append(b, cluster...)
	b = append(b, 0)
	b = append(b, nonce...)
	b = append(b, 0)
	if superseded {
		b = append(b, 1)
	} else {
		b = append(b, 0)
	}
	return append(b, by...)
}

// ReceiptMessage is the message of a fenced-voter receipt: the SDK's own,
// so Recover verifies it with the same bytes.
func ReceiptMessage(old, newCluster consensus.ClusterID, voter consensus.NodeID) []byte {
	return sdk.ReceiptMessage(sdk.ClusterID(old), sdk.ClusterID(newCluster), sdk.NodeID(voter))
}
