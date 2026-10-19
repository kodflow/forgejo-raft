// Copyright 2026 The Forgejo Authors. All rights reserved.
// SPDX-License-Identifier: GPL-3.0-or-later

package cluster

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"net"
	"os"
	"sort"
	"time"
)

// ProbeTimeout bounds the probe of one old member.
const ProbeTimeout = 3 * time.Second

// OldMemberAnswer is what the probe of one old member's address found.
type OldMemberAnswer struct {
	Slot string
	Addr string
	// Proven is set when the node completed a TLS handshake with a
	// certificate the cluster's CA signed for the old cluster: a member
	// of it, certainly alive. Unset, something accepted the connection.
	Proven bool
	// Cluster is the cluster the certificate names, when one was verified.
	Cluster string
	Detail  string
}

// Blocks reports whether the answer forbids a Recover: an old member, or a
// node whose cluster the probe could not read. A node proven to belong to
// another cluster (the new one being prepared) does not.
func (a OldMemberAnswer) Blocks() bool { return a.Proven || a.Cluster == "" }

// ProbeOldMembers asks each address of the old cluster whether a node
// still answers there (ADR 0152 R3-27): a Recover refuses while one does,
// since a live old member may still hold a majority and write. Anything
// that accepts the TCP connection counts: the probe cannot tell an old
// member that refuses this node's handshake from another service, and it
// errs towards refusing. caFile verifies the certificate when one is
// presented; an address that refuses or does not answer within
// ProbeTimeout is silent.
func ProbeOldMembers(ctx context.Context, oldCluster string, addrs map[string]string, caFile string) []OldMemberAnswer {
	var roots *x509.CertPool
	if b, err := os.ReadFile(caFile); err == nil {
		roots = x509.NewCertPool()
		roots.AppendCertsFromPEM(b)
	}
	var out []OldMemberAnswer
	for slot, addr := range addrs {
		if a, ok := probeOne(ctx, oldCluster, slot, addr, roots); ok {
			out = append(out, a)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Slot < out[j].Slot })
	return out
}

func probeOne(ctx context.Context, oldCluster, slot, addr string, roots *x509.CertPool) (OldMemberAnswer, bool) {
	pctx, cancel := context.WithTimeout(ctx, ProbeTimeout)
	defer cancel()
	var d net.Dialer
	conn, err := d.DialContext(pctx, "tcp", addr)
	if err != nil {
		return OldMemberAnswer{}, false
	}
	defer conn.Close()
	a := OldMemberAnswer{Slot: slot, Addr: addr, Detail: "the address accepts connections"}
	if roots == nil {
		return a, true
	}
	var proven peer
	tc := tls.Client(conn, &tls.Config{
		MinVersion: tls.VersionTLS13,
		// The chain is verified below against the cluster's CA: the server
		// name is not what identifies a member, its URI SAN is.
		InsecureSkipVerify: true, //nolint:gosec
		VerifyConnection: func(cs tls.ConnectionState) error {
			if len(cs.PeerCertificates) == 0 {
				return fmt.Errorf("no certificate")
			}
			opts := x509.VerifyOptions{Roots: roots, Intermediates: x509.NewCertPool(), KeyUsages: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth}}
			for _, c := range cs.PeerCertificates[1:] {
				opts.Intermediates.AddCert(c)
			}
			if _, err := cs.PeerCertificates[0].Verify(opts); err != nil {
				return err
			}
			p, err := peerFromChain(cs.PeerCertificates[0])
			if err != nil {
				return err
			}
			proven = p
			return nil
		},
	})
	_ = tc.SetDeadline(time.Now().Add(ProbeTimeout))
	if err := tc.HandshakeContext(pctx); err != nil && proven.Cluster == "" {
		return a, true
	}
	a.Cluster = proven.Cluster
	if proven.Cluster == oldCluster {
		a.Proven = true
		a.Detail = fmt.Sprintf("a member of cluster %s answers as slot %s", proven.Cluster, proven.Slot)
	} else if proven.Cluster != "" {
		a.Detail = fmt.Sprintf("a node of cluster %s answers as slot %s", proven.Cluster, proven.Slot)
	}
	return a, true
}
