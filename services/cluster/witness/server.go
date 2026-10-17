// Copyright 2026 The Forgejo Authors. All rights reserved.
// SPDX-License-Identifier: GPL-3.0-or-later

package witness

import (
	"crypto/ed25519"
	"encoding/hex"
	"net/http"
	"slices"
	"strings"

	"forgejo.org/modules/json"
	"forgejo.org/services/cluster/consensus"
)

// StatusAnswer is the signed answer to the boot check.
type StatusAnswer struct {
	Cluster    string `json:"cluster"`
	Nonce      string `json:"nonce"`
	Superseded bool   `json:"superseded"`
	By         string `json:"by,omitempty"`
	Signature  string `json:"signature"`
}

// ReceiptAnswer carries a fenced-voter receipt.
type ReceiptAnswer struct {
	Old       string `json:"old"`
	New       string `json:"new"`
	Voter     string `json:"voter"`
	Signature string `json:"signature"`
}

// Handler serves the two read questions; there is no write over the
// network: the operator writes on the witness host with `forgejo cluster witness
// supersede` (Q19, R9-10).
func (s *Store) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /v1/status", s.status)
	mux.HandleFunc("GET /v1/receipt", s.receipt)
	return mux
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}

func (s *Store) status(w http.ResponseWriter, r *http.Request) {
	cluster, err := consensus.ParseClusterID(r.URL.Query().Get("cluster"))
	nonce := r.URL.Query().Get("nonce")
	if err != nil || len(nonce) < 16 || len(nonce) > 128 || !nonceChars(nonce) {
		http.Error(w, "cluster (32 hex) and nonce (16 to 128 of [A-Za-z0-9_-]) are required", http.StatusBadRequest)
		return
	}
	key, err := s.key()
	if err != nil {
		http.Error(w, "witness key unavailable", http.StatusServiceUnavailable)
		return
	}
	recs, err := s.Records()
	if err != nil {
		http.Error(w, "witness state unreadable", http.StatusServiceUnavailable)
		return
	}
	a := StatusAnswer{Cluster: cluster.String(), Nonce: nonce}
	if rec, ok := recs[cluster.String()]; ok {
		a.Superseded, a.By = true, rec.New
	}
	a.Signature = hex.EncodeToString(ed25519.Sign(key, statusMessage(a.Cluster, a.Nonce, a.Superseded, a.By)))
	writeJSON(w, http.StatusOK, a)
}

func (s *Store) receipt(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	old, err1 := consensus.ParseClusterID(q.Get("old"))
	newCluster, err2 := consensus.ParseClusterID(q.Get("new"))
	voter := strings.ToLower(q.Get("voter"))
	if err1 != nil || err2 != nil || len(voter) != 16 {
		http.Error(w, "old, new (32 hex) and voter (16 hex) are required", http.StatusBadRequest)
		return
	}
	id, err := parseNodeID(voter)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	recs, err := s.Records()
	if err != nil {
		http.Error(w, "witness state unreadable", http.StatusServiceUnavailable)
		return
	}
	rec, ok := recs[old.String()]
	if !ok || rec.New != newCluster.String() || !slices.Contains(rec.Voters, id.String()) {
		// No receipt for a fence the operator has not written.
		http.Error(w, "no such superseded lineage and fenced voter", http.StatusNotFound)
		return
	}
	key, err := s.key()
	if err != nil {
		http.Error(w, "witness key unavailable", http.StatusServiceUnavailable)
		return
	}
	writeJSON(w, http.StatusOK, ReceiptAnswer{
		Old: old.String(), New: newCluster.String(), Voter: id.String(),
		Signature: hex.EncodeToString(ed25519.Sign(key, ReceiptMessage(old, newCluster, id))),
	})
}

// nonceChars keeps the signed message's framing unambiguous: the nonce is
// delimited by a NUL byte there, so it may hold none (nor any other control
// byte).
func nonceChars(n string) bool {
	for _, c := range n {
		if !(c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9' || c == '-' || c == '_') {
			return false
		}
	}
	return true
}
