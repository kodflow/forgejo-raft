// Copyright 2026 The Forgejo Authors. All rights reserved.
// SPDX-License-Identifier: GPL-3.0-or-later

package cluster

import (
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"os"
	"regexp"
	"strings"
	"sync"
	"time"
)

// identity is the node's TLS identity for the object channel, read from the
// same PEM files the consensus transport uses, and reloaded when they
// change, so a renewal is picked up without a restart (ADR 0152 D17, R7-25).
type identity struct {
	certFile, keyFile, caFile string

	mu      sync.Mutex
	cert    *tls.Certificate
	pool    *x509.CertPool
	version string
}

func newIdentity(certFile, keyFile, caFile string) (*identity, error) {
	id := &identity{certFile: certFile, keyFile: keyFile, caFile: caFile}
	if _, err := id.load(); err != nil {
		return nil, err
	}
	return id, nil
}

// filesVersion names the state of the three files: size, modification time
// and inode of each, so a renewal is seen whatever file it replaced (a new
// CA alone, a key file of its own) and however fast it came.
func (id *identity) filesVersion() (string, error) {
	var b strings.Builder
	for _, f := range []string{id.certFile, id.keyFile, id.caFile} {
		st, err := os.Stat(f)
		if err != nil {
			return "", err
		}
		fmt.Fprintf(&b, "%d/%d/%d;", st.Size(), st.ModTime().UnixNano(), inode(st))
	}
	return b.String(), nil
}

// load returns the pair in use, reloading the files when they changed. A
// reload that fails, the pair or the CA, keeps the previous pair and pool:
// a half-written renewal never takes the channel down.
func (id *identity) load() (*tls.Certificate, error) {
	id.mu.Lock()
	defer id.mu.Unlock()
	v, err := id.filesVersion()
	if err != nil {
		if id.cert != nil {
			return id.cert, nil
		}
		return nil, err
	}
	if id.cert != nil && v == id.version {
		return id.cert, nil
	}
	cert, pool, err := loadPair(id.certFile, id.keyFile, id.caFile)
	if err != nil {
		if id.cert != nil {
			return id.cert, nil // keep serving the old pair; the reload is retried
		}
		return nil, err
	}
	id.cert, id.pool, id.version = cert, pool, v
	return id.cert, nil
}

func loadPair(certFile, keyFile, caFile string) (*tls.Certificate, *x509.CertPool, error) {
	cert, err := tls.LoadX509KeyPair(certFile, keyFile)
	if err != nil {
		return nil, nil, err
	}
	ca, err := os.ReadFile(caFile)
	if err != nil {
		return nil, nil, err
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(ca) {
		return nil, nil, errors.New("TLS_CA_FILE holds no certificate")
	}
	return &cert, pool, nil
}

func (id *identity) roots() *x509.CertPool {
	_, _ = id.load()
	id.mu.Lock()
	defer id.mu.Unlock()
	return id.pool
}

// Expiry returns the NotAfter of the node's certificate.
func (id *identity) Expiry() time.Time {
	c, err := id.load()
	if err != nil || len(c.Certificate) == 0 {
		return time.Time{}
	}
	leaf, err := x509.ParseCertificate(c.Certificate[0])
	if err != nil {
		return time.Time{}
	}
	return leaf.NotAfter
}

// SPKI returns the SHA-256 of the node's SubjectPublicKeyInfo.
func (id *identity) SPKI() [32]byte {
	c, err := id.load()
	if err != nil || len(c.Certificate) == 0 {
		return [32]byte{}
	}
	leaf, err := x509.ParseCertificate(c.Certificate[0])
	if err != nil {
		return [32]byte{}
	}
	return sha256.Sum256(leaf.RawSubjectPublicKeyInfo)
}

// SPKIAndSerial names the certificate in use: it changes with a renewal,
// which keeps the key and changes the serial.
func (id *identity) SPKIAndSerial() string {
	c, err := id.load()
	if err != nil || len(c.Certificate) == 0 {
		return ""
	}
	leaf, err := x509.ParseCertificate(c.Certificate[0])
	if err != nil {
		return ""
	}
	return fmt.Sprintf("%x/%s", sha256.Sum256(leaf.RawSubjectPublicKeyInfo), leaf.SerialNumber)
}

// peer is what a handshake proved about the other side.
type peer struct {
	Cluster string
	Slot    string
	SPKI    [32]byte
}

var uriSANPattern = regexp.MustCompile(`^kitsunium-consensus://([0-9a-f]{32})/slot/([a-z0-9-]{1,32})$`)

// peerFromChain checks the canonical single URI SAN of ADR 0152 D4 on a
// certificate whose chain was already verified against the cluster's roots.
func peerFromChain(cert *x509.Certificate) (peer, error) {
	if len(cert.URIs) != 1 {
		return peer{}, errors.New("PEER_REJECTED: the certificate must carry exactly one URI SAN")
	}
	m := uriSANPattern.FindStringSubmatch(cert.URIs[0].String())
	if m == nil {
		return peer{}, fmt.Errorf("PEER_REJECTED: URI SAN %q is not canonical", cert.URIs[0].String())
	}
	return peer{Cluster: m[1], Slot: m[2], SPKI: sha256.Sum256(cert.RawSubjectPublicKeyInfo)}, nil
}

// serverConfig requires and verifies a client certificate.
func (id *identity) serverConfig() *tls.Config {
	return &tls.Config{
		MinVersion: tls.VersionTLS13,
		ClientAuth: tls.RequireAndVerifyClientCert,
		GetCertificate: func(*tls.ClientHelloInfo) (*tls.Certificate, error) {
			return id.load()
		},
		GetConfigForClient: func(*tls.ClientHelloInfo) (*tls.Config, error) {
			return &tls.Config{
				MinVersion: tls.VersionTLS13,
				ClientAuth: tls.RequireAndVerifyClientCert,
				ClientCAs:  id.roots(),
				GetCertificate: func(*tls.ClientHelloInfo) (*tls.Certificate, error) {
					return id.load()
				},
			}, nil
		},
	}
}

// clientConfig verifies the server's chain first, then its URI SAN, and
// presents the node's certificate.
func (id *identity) clientConfig(check func(peer) error) *tls.Config {
	return &tls.Config{
		MinVersion: tls.VersionTLS13,
		// The chain is verified below, in full, before the URI SAN: the
		// server name is not what identifies a member, its slot is.
		InsecureSkipVerify: true, //nolint:gosec
		GetClientCertificate: func(*tls.CertificateRequestInfo) (*tls.Certificate, error) {
			return id.load()
		},
		VerifyConnection: func(cs tls.ConnectionState) error {
			if len(cs.PeerCertificates) == 0 {
				return errors.New("PEER_REJECTED: no certificate")
			}
			opts := x509.VerifyOptions{
				Roots:         id.roots(),
				Intermediates: x509.NewCertPool(),
				KeyUsages:     []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
			}
			for _, c := range cs.PeerCertificates[1:] {
				opts.Intermediates.AddCert(c)
			}
			if _, err := cs.PeerCertificates[0].Verify(opts); err != nil {
				return fmt.Errorf("PEER_REJECTED: %w", err)
			}
			p, err := peerFromChain(cs.PeerCertificates[0])
			if err != nil {
				return err
			}
			return check(p)
		},
	}
}
