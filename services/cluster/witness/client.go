// Copyright 2026 The Forgejo Authors. All rights reserved.
// SPDX-License-Identifier: GPL-3.0-or-later

package witness

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"

	"forgejo.org/modules/json"
	"forgejo.org/services/cluster/consensus"
)

// Witness is one witness as a Forgejo node knows it: where to ask, and the
// public key its answers must verify with.
type Witness struct {
	URL string
	Key ed25519.PublicKey
}

// ParseWitnesses pairs URLs and hex public keys, in order.
func ParseWitnesses(urls, keys []string) ([]Witness, error) {
	if len(urls) != len(keys) {
		return nil, fmt.Errorf("%d witness URLs for %d keys", len(urls), len(keys))
	}
	var out []Witness
	for i := range urls {
		k, err := ParsePublicKey(keys[i])
		if err != nil {
			return nil, err
		}
		out = append(out, Witness{URL: strings.TrimRight(strings.TrimSpace(urls[i]), "/"), Key: k})
	}
	return out, nil
}

// ParsePublicKey reads a hex Ed25519 public key.
func ParsePublicKey(s string) (ed25519.PublicKey, error) {
	b, err := hex.DecodeString(strings.TrimSpace(s))
	if err != nil || len(b) != ed25519.PublicKeySize {
		return nil, fmt.Errorf("witness key %q is not a 32-byte hex Ed25519 public key", s)
	}
	return ed25519.PublicKey(b), nil
}

func parseNodeID(s string) (consensus.NodeID, error) {
	v, err := strconv.ParseUint(s, 16, 64)
	if err != nil || v == 0 {
		return 0, fmt.Errorf("voter %q must be 16 hex digits, not zero", s)
	}
	return consensus.NodeID(v), nil
}

// Verdict is a witness's answer to the boot check.
type Verdict int

const (
	Unanswered Verdict = iota
	Clear
	Superseded
)

// Answer is one witness's verdict, and why when it did not answer.
type Answer struct {
	URL     string
	Verdict Verdict
	By      string
	Err     error
}

var client = &http.Client{Timeout: 5 * time.Second}

// Ask asks one witness whether cluster is superseded. An answer whose
// signature does not verify, or that names another cluster or nonce, is no
// answer.
func Ask(ctx context.Context, w Witness, cluster consensus.ClusterID) Answer {
	nb := make([]byte, 16)
	_, _ = rand.Read(nb)
	nonce := hex.EncodeToString(nb)
	a := Answer{URL: w.URL}
	u := w.URL + "/v1/status?cluster=" + cluster.String() + "&nonce=" + url.QueryEscape(nonce)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	if err != nil {
		a.Err = err
		return a
	}
	resp, err := client.Do(req)
	if err != nil {
		a.Err = err
		return a
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		a.Err = fmt.Errorf("answered %s", resp.Status)
		return a
	}
	var s StatusAnswer
	if err := json.NewDecoder(io.LimitReader(resp.Body, 1<<16)).Decode(&s); err != nil {
		a.Err = err
		return a
	}
	sig, err := hex.DecodeString(s.Signature)
	if err != nil || s.Cluster != cluster.String() || s.Nonce != nonce ||
		!ed25519.Verify(w.Key, statusMessage(s.Cluster, s.Nonce, s.Superseded, s.By), sig) {
		a.Err = errors.New("the answer is not signed by this witness's key, or not for this question")
		return a
	}
	if s.Superseded {
		a.Verdict, a.By = Superseded, s.By
	} else {
		a.Verdict = Clear
	}
	return a
}

// Errors of the boot check.
var (
	ErrSuperseded = errors.New("this cluster was superseded by a Recover: the node is fenced")
	ErrNoWitness  = errors.New("no witness answered: the node refuses to start")
)

// Check is the boot check of R9-09: it asks every witness at once. It
// refuses when ANY witness that answers says the cluster is superseded; it
// passes only when at least one says clear and none says otherwise; with no
// answer at all, it refuses: it fails closed.
func Check(ctx context.Context, ws []Witness, cluster consensus.ClusterID) ([]Answer, error) {
	answers := make([]Answer, len(ws))
	var wg sync.WaitGroup
	for i, w := range ws {
		wg.Add(1)
		go func(i int, w Witness) {
			defer wg.Done()
			answers[i] = Ask(ctx, w, cluster)
		}(i, w)
	}
	wg.Wait()
	clear := 0
	for _, a := range answers {
		switch a.Verdict {
		case Superseded:
			return answers, fmt.Errorf("%w (by %s, says %s)", ErrSuperseded, a.By, a.URL)
		case Clear:
			clear++
		}
	}
	if clear == 0 {
		return answers, ErrNoWitness
	}
	return answers, nil
}

// FetchReceipt asks one witness for the receipt of a fenced voter and
// verifies it, returning the signature Recover takes.
func FetchReceipt(ctx context.Context, w Witness, old, newCluster consensus.ClusterID, voter consensus.NodeID) ([]byte, error) {
	u := fmt.Sprintf("%s/v1/receipt?old=%s&new=%s&voter=%s", w.URL, old, newCluster, voter)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	if err != nil {
		return nil, err
	}
	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		b, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
		return nil, fmt.Errorf("%s answered %s: %s", w.URL, resp.Status, strings.TrimSpace(string(b)))
	}
	var a ReceiptAnswer
	if err := json.NewDecoder(io.LimitReader(resp.Body, 1<<16)).Decode(&a); err != nil {
		return nil, err
	}
	sig, err := hex.DecodeString(a.Signature)
	if err != nil || !ed25519.Verify(w.Key, ReceiptMessage(old, newCluster, voter), sig) {
		return nil, fmt.Errorf("%s: the receipt does not verify with its key", w.URL)
	}
	return sig, nil
}

// VerifyReceipts checks, before Recover runs, that every old voter carries
// a receipt from EVERY witness key (R9-09). The SDK checks the same; this
// says which one is missing.
func VerifyReceipts(keys []ed25519.PublicKey, old, newCluster consensus.ClusterID, fenced []consensus.FencedVoter) error {
	if len(keys) == 0 {
		return errors.New("no witness key: Recover needs a receipt from every witness")
	}
	for _, f := range fenced {
		msg := ReceiptMessage(old, newCluster, f.Voter)
		for i, k := range keys {
			ok := false
			for _, sig := range f.Receipts {
				if ed25519.Verify(k, msg, sig) {
					ok = true
					break
				}
			}
			if !ok {
				return fmt.Errorf("voter %s has no valid receipt from witness %d", f.Voter, i+1)
			}
		}
	}
	return nil
}
