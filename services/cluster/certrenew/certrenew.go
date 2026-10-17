// Copyright 2026 The Forgejo Authors. All rights reserved.
// SPDX-License-Identifier: GPL-3.0-or-later

// Package certrenew renews a cluster node's certificate in process
// (kitsunium ADR 0152 D17, R7-25, R8-13). The CSR is made from the host
// key, so the key never leaves the host and the member's SPKI pin holds; the
// certificate is renewed at two thirds of its lifetime, with a per-node
// jitter, and replaced atomically together with its key when both share one
// file, so a reload never pairs a new certificate with an old key. Who signs
// is behind Signer: Vault's pki-consensus/sign/<slot> in production.
package certrenew

import (
	"context"
	"crypto"
	"crypto/rand"
	"crypto/sha256"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"errors"
	"fmt"
	"math/big"
	"net/url"
	"os"
	"path/filepath"
	"time"
)

// Signer turns a CSR into a certificate, PEM in and PEM out.
type Signer interface {
	Sign(ctx context.Context, csrPEM []byte, uriSAN string) (certPEM []byte, err error)
}

// Renewer keeps one node certificate fresh.
type Renewer struct {
	// CertFile and KeyFile name the node's PEM files; they may be the same
	// file, which then holds the certificate followed by the key.
	CertFile string
	KeyFile  string
	// CAFile verifies what the signer returns.
	CAFile string
	// Cluster and Slot make the single URI SAN.
	Cluster string
	Slot    string
	Signer  Signer
	// Jitter, between 0 and 1, moves this node's renewal point within the
	// last third, so three nodes never renew together.
	Jitter float64
	// Now is the clock, time.Now when nil.
	Now func() time.Time
}

// URISAN is the canonical single URI SAN of a node certificate (D4, R7-41).
func URISAN(cluster, slot string) string {
	return "kitsunium-consensus://" + cluster + "/slot/" + slot
}

func (r *Renewer) now() time.Time {
	if r.Now != nil {
		return r.Now()
	}
	return time.Now()
}

// Current reads the certificate in use.
func (r *Renewer) Current() (*x509.Certificate, error) {
	b, err := os.ReadFile(r.CertFile)
	if err != nil {
		return nil, err
	}
	for {
		var blk *pem.Block
		blk, b = pem.Decode(b)
		if blk == nil {
			return nil, errors.New("no certificate in " + r.CertFile)
		}
		if blk.Type == "CERTIFICATE" {
			return x509.ParseCertificate(blk.Bytes)
		}
	}
}

// Due says when the certificate in use is to be renewed: two thirds of its
// lifetime, plus the node's jitter over a sixth of it.
func (r *Renewer) Due(c *x509.Certificate) time.Time {
	life := c.NotAfter.Sub(c.NotBefore)
	return c.NotBefore.Add(life*2/3 + time.Duration(r.Jitter*float64(life/6)))
}

func (r *Renewer) key() (crypto.Signer, []byte, error) {
	b, err := os.ReadFile(r.KeyFile)
	if err != nil {
		return nil, nil, err
	}
	rest := b
	for {
		var blk *pem.Block
		blk, rest = pem.Decode(rest)
		if blk == nil {
			return nil, nil, errors.New("no private key in " + r.KeyFile)
		}
		var k any
		switch blk.Type {
		case "EC PRIVATE KEY":
			k, err = x509.ParseECPrivateKey(blk.Bytes)
		case "PRIVATE KEY":
			k, err = x509.ParsePKCS8PrivateKey(blk.Bytes)
		case "RSA PRIVATE KEY":
			k, err = x509.ParsePKCS1PrivateKey(blk.Bytes)
		default:
			continue
		}
		if err != nil {
			return nil, nil, err
		}
		s, ok := k.(crypto.Signer)
		if !ok {
			return nil, nil, errors.New("the node key cannot sign")
		}
		return s, pem.EncodeToMemory(blk), nil
	}
}

// RenewNow makes a CSR from the host key, has it signed, checks the result
// and installs it. The new certificate must carry the same SPKI, the one
// URI SAN, and chain to the CA.
func (r *Renewer) RenewNow(ctx context.Context) (*x509.Certificate, error) {
	key, keyPEM, err := r.key()
	if err != nil {
		return nil, err
	}
	san := URISAN(r.Cluster, r.Slot)
	u, err := url.Parse(san)
	if err != nil {
		return nil, err
	}
	der, err := x509.CreateCertificateRequest(rand.Reader, &x509.CertificateRequest{
		Subject: pkix.Name{CommonName: r.Slot}, URIs: []*url.URL{u},
	}, key)
	if err != nil {
		return nil, err
	}
	certPEM, err := r.Signer.Sign(ctx, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE REQUEST", Bytes: der}), san)
	if err != nil {
		return nil, fmt.Errorf("signing the CSR: %w", err)
	}
	cert, err := r.check(certPEM, key.Public(), san)
	if err != nil {
		return nil, err
	}
	leafPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: cert.Raw})
	out := leafPEM
	if filepath.Clean(r.CertFile) == filepath.Clean(r.KeyFile) {
		out = append(append([]byte{}, leafPEM...), keyPEM...)
	}
	if err := writeAtomic(r.CertFile, out); err != nil {
		return nil, err
	}
	return cert, nil
}

func (r *Renewer) check(certPEM []byte, pub crypto.PublicKey, san string) (*x509.Certificate, error) {
	blk, _ := pem.Decode(certPEM)
	if blk == nil || blk.Type != "CERTIFICATE" {
		return nil, errors.New("the signer returned no certificate")
	}
	cert, err := x509.ParseCertificate(blk.Bytes)
	if err != nil {
		return nil, err
	}
	want, err := x509.MarshalPKIXPublicKey(pub)
	if err != nil {
		return nil, err
	}
	if sha256.Sum256(cert.RawSubjectPublicKeyInfo) != sha256.Sum256(want) {
		return nil, errors.New("the new certificate carries another key: the SPKI pin would refuse it")
	}
	if len(cert.URIs) != 1 || cert.URIs[0].String() != san {
		return nil, fmt.Errorf("the new certificate must carry exactly the URI SAN %s", san)
	}
	ca, err := os.ReadFile(r.CAFile)
	if err != nil {
		return nil, err
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(ca) {
		return nil, errors.New("no CA in " + r.CAFile)
	}
	// VerifyOptions.KeyUsages accepts a chain with ANY of them: the node
	// needs both, as a server to its peers and as their client.
	var server, client bool
	for _, u := range cert.ExtKeyUsage {
		server = server || u == x509.ExtKeyUsageServerAuth
		client = client || u == x509.ExtKeyUsageClientAuth
	}
	if !server || !client {
		return nil, errors.New("the new certificate must allow both serverAuth and clientAuth")
	}
	if _, err := cert.Verify(x509.VerifyOptions{
		Roots: pool, CurrentTime: r.now(),
		KeyUsages: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth},
	}); err != nil {
		return nil, fmt.Errorf("the new certificate does not chain to the cluster CA: %w", err)
	}
	return cert, nil
}

func writeAtomic(path string, b []byte) error {
	tmp := path + ".renew"
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
		os.Remove(tmp)
		return err
	}
	return os.Rename(tmp, path)
}

// Run renews whenever the certificate in use is due, until ctx ends. A
// failure is retried every retry interval; the transport keeps serving the
// certificate in use meanwhile, and its expiry alert covers the rest.
func (r *Renewer) Run(ctx context.Context, retry time.Duration, onErr func(error), onRenew func(*x509.Certificate)) {
	for {
		wait := retry
		cur, err := r.Current()
		if err == nil {
			if due := r.Due(cur); r.now().Before(due) {
				wait = due.Sub(r.now())
			} else if c, err := r.RenewNow(ctx); err != nil {
				onErr(err)
			} else {
				onRenew(c)
				// A certificate due as soon as issued (a lifetime shorter
				// than the clock skew) is not renewed in a tight loop.
				if due := r.Due(c); r.now().Before(due) {
					wait = due.Sub(r.now())
				}
			}
		} else {
			onErr(err)
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(wait):
		}
	}
}

// JitterFor derives a stable jitter in [0, 1) from a node's slot, so each
// node renews at its own point and keeps it across restarts.
func JitterFor(slot string) float64 {
	h := sha256.Sum256([]byte(slot))
	return float64(new(big.Int).SetBytes(h[:4]).Uint64()) / float64(1<<32)
}
