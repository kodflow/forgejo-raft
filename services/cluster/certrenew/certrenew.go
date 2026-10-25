// Copyright 2026 The Forgejo Authors. All rights reserved.
// SPDX-License-Identifier: GPL-3.0-or-later

// Package certrenew renews a cluster node's certificate in process
// (kitsunium ADR 0152 D17, R7-25, R8-13). The CSR is made from the host
// key, so the key never leaves the host and the member's SPKI pin holds; the
// certificate is renewed at two thirds of its lifetime, with a per-node
// jitter, and replaced atomically together with its key when both share one
// file, so a reload never pairs a new certificate with an old key. Who signs
// is behind Signer: Vault's pki-consensus/sign/<slot> in production.
//
// Rekey is the one exception to the kept key: a node about to take a new
// node id is given a new key first, made on the host like the old one, so
// a key serves one node id for the life of the cluster (#209).
package certrenew

import (
	"bytes"
	"context"
	"crypto"
	"crypto/ecdsa"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"errors"
	"fmt"
	"math/big"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
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
	// Cluster and Slot are the node's [cluster] CLUSTER_ID and SLOT. With
	// Identity set, the single URI SAN is the one the certificate in use
	// names, which they must agree with when set: CLUSTER_ID may stay empty
	// in the autonomous mode, and a renewal that asked for the URI SAN of an
	// empty cluster was refused by the signer's role, or signed into a
	// certificate no member admits (#185). Without a certificate in use yet,
	// or with one whose URI SAN is not canonical, they make it.
	Cluster string
	Slot    string
	// Identity returns the cluster and the slot a certificate's canonical
	// URI SAN names, and refuses any other URI SAN.
	Identity func(*x509.Certificate) (cluster, slot string, err error)
	Signer   Signer
	// Jitter, between 0 and 1, moves this node's renewal point within the
	// last third, so three nodes never renew together.
	Jitter float64
	// Now is the clock, time.Now when nil.
	Now func() time.Time

	// mu keeps a renewal and a rekey of this renewer apart: a renewal that
	// read the old key and wrote its certificate after a rekey would pair
	// the new key with a certificate of the old one.
	mu sync.Mutex
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

// uriSAN is the URI SAN to ask for: the one the certificate in use names,
// checked against CLUSTER_ID and SLOT, or theirs. It is refused before the
// signer is asked when it would name no cluster, or not be canonical.
func (r *Renewer) uriSAN() (string, error) {
	cluster, slot := r.Cluster, r.Slot
	if r.Identity != nil {
		if cur, err := r.Current(); err == nil {
			switch c, s, err := r.Identity(cur); {
			case err != nil && cluster == "":
				return "", fmt.Errorf("refusing to renew: the certificate in use names no cluster (%v) and [cluster] CLUSTER_ID is empty; set CLUSTER_ID to the cluster's id", err)
			case err != nil:
				// The certificate in use is not canonical: CLUSTER_ID and
				// SLOT, which an operator set, renew it back.
			case cluster != "" && c != cluster:
				return "", fmt.Errorf("refusing to renew: [cluster] CLUSTER_ID is %s, the certificate in use names cluster %s", cluster, c)
			case slot != "" && s != slot:
				return "", fmt.Errorf("refusing to renew: [cluster] SLOT is %s, the certificate in use names slot %s", slot, s)
			default:
				cluster, slot = c, s
			}
		}
	}
	if cluster == "" || slot == "" {
		return "", errors.New("refusing to renew: the URI SAN would name no cluster or no slot; set [cluster] CLUSTER_ID and SLOT")
	}
	san := URISAN(cluster, slot)
	if r.Identity != nil {
		u, err := url.Parse(san)
		if err != nil {
			return "", err
		}
		if _, _, err := r.Identity(&x509.Certificate{URIs: []*url.URL{u}}); err != nil {
			return "", fmt.Errorf("refusing to renew: %w", err)
		}
	}
	return san, nil
}

// RenewNow makes a CSR from the host key, has it signed, checks the result
// and installs it. The new certificate must carry the same SPKI, the one
// URI SAN, and chain to the CA.
func (r *Renewer) RenewNow(ctx context.Context) (*x509.Certificate, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if err := ResumeRekey(r.CertFile, r.KeyFile); err != nil {
		return nil, err
	}
	key, keyPEM, err := r.key()
	if err != nil {
		return nil, err
	}
	cert, err := r.signed(ctx, key)
	if err != nil {
		return nil, err
	}
	// Another process (`forgejo cluster cert renew` beside the server) may
	// have given the node a new key while the signer answered: this
	// certificate is of the old one, and written it would undo that key or
	// pair the new key with it.
	if _, now, err := r.key(); err != nil || !bytes.Equal(now, keyPEM) {
		return nil, errors.New("the node key changed while its certificate was renewed: nothing is written; renew again")
	}
	leafPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: cert.Raw})
	out := leafPEM
	if r.sharedFile() {
		out = append(append([]byte{}, leafPEM...), keyPEM...)
	}
	if err := writeAtomic(r.CertFile, out); err != nil {
		return nil, err
	}
	return cert, nil
}

// signed has a certificate signed for key, under the URI SAN to ask for,
// and checks it.
func (r *Renewer) signed(ctx context.Context, key crypto.Signer) (*x509.Certificate, error) {
	san, err := r.uriSAN()
	if err != nil {
		return nil, err
	}
	u, err := url.Parse(san)
	if err != nil {
		return nil, err
	}
	der, err := x509.CreateCertificateRequest(rand.Reader, &x509.CertificateRequest{
		Subject: pkix.Name{CommonName: strings.TrimPrefix(u.Path, "/slot/")}, URIs: []*url.URL{u},
	}, key)
	if err != nil {
		return nil, err
	}
	certPEM, err := r.Signer.Sign(ctx, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE REQUEST", Bytes: der}), san)
	if err != nil {
		return nil, fmt.Errorf("signing the CSR: %w", err)
	}
	return r.check(certPEM, key.Public(), san)
}

func (r *Renewer) sharedFile() bool {
	return filepath.Clean(r.CertFile) == filepath.Clean(r.KeyFile)
}

// Rekey gives the node a new key and its certificate: a key of the kind of
// the one in use, made on the host, a CSR from it, signed and checked as a
// renewal is, then both installed. It is for a node about to take a new
// node id, and only before that id is stamped (#209): a member's own key
// never changes, since its peers pin it, and the consensus transport of a
// running engine keeps the key it opened with.
//
// Nothing is installed unless the signer's certificate passed every check.
// One file holding both is replaced at once; two files are replaced key
// first, both written aside beforehand, so a stop between the two renames
// leaves the new key and its certificate beside it, which ResumeRekey puts
// in place, and a stop before them leaves the old pair whole.
func (r *Renewer) Rekey(ctx context.Context) (*x509.Certificate, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if err := ResumeRekey(r.CertFile, r.KeyFile); err != nil {
		return nil, err
	}
	old, _, err := r.key()
	if err != nil {
		return nil, err
	}
	key, err := newKeyLike(old)
	if err != nil {
		return nil, err
	}
	cert, err := r.signed(ctx, key)
	if err != nil {
		return nil, err
	}
	der, err := x509.MarshalPKCS8PrivateKey(key)
	if err != nil {
		return nil, err
	}
	keyPEM := pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: der})
	leafPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: cert.Raw})
	if r.sharedFile() {
		if err := writeAtomic(r.CertFile, append(append([]byte{}, leafPEM...), keyPEM...)); err != nil {
			return nil, err
		}
		// The caller stamps a new node id next: the rename is made
		// durable first, so a power loss never leaves that id under the
		// old key.
		if err := syncDirs(r.CertFile); err != nil {
			return nil, err
		}
		return cert, nil
	}
	if err := installPair(r.CertFile, r.KeyFile, leafPEM, keyPEM); err != nil {
		return nil, err
	}
	return cert, nil
}

// newKeyLike makes a new key of the kind of old: the same curve, the same
// modulus size, or Ed25519.
func newKeyLike(old crypto.Signer) (crypto.Signer, error) {
	switch k := old.(type) {
	case *ecdsa.PrivateKey:
		return ecdsa.GenerateKey(k.Curve, rand.Reader)
	case *rsa.PrivateKey:
		return rsa.GenerateKey(rand.Reader, k.N.BitLen())
	case ed25519.PrivateKey:
		_, n, err := ed25519.GenerateKey(rand.Reader)
		return n, err
	}
	return nil, fmt.Errorf("the node key is a %T: a new one of that kind cannot be made", old)
}

// rekeySuffix names the new key and certificate written aside before a
// rekey puts them in place.
const rekeySuffix = ".rekey"

// installPair puts a new key and its certificate in place when they live in
// two files: both written aside and synced, then the key renamed in and
// made durable, then the certificate. The certificate's rename is never
// durable without the key's, even on two file systems. Between the two
// renames the pair on disk does not match: a reader keeps the pair it holds
// (both the transport and the object channel refuse a pair that does not
// match), and ResumeRekey completes it after a stop.
func installPair(certFile, keyFile string, certPEM, keyPEM []byte) error {
	if err := writeSynced(keyFile+rekeySuffix, keyPEM); err != nil {
		return err
	}
	if err := writeSynced(certFile+rekeySuffix, certPEM); err != nil {
		_ = os.Remove(keyFile + rekeySuffix)
		return err
	}
	if err := syncDirs(certFile, keyFile); err != nil {
		_ = os.Remove(keyFile + rekeySuffix)
		_ = os.Remove(certFile + rekeySuffix)
		return err
	}
	if err := os.Rename(keyFile+rekeySuffix, keyFile); err != nil {
		_ = os.Remove(keyFile + rekeySuffix)
		_ = os.Remove(certFile + rekeySuffix)
		return err
	}
	if err := syncDirs(keyFile); err != nil {
		return fmt.Errorf("the new key is in place and not made durable: %w; the next start completes it", err)
	}
	if err := os.Rename(certFile+rekeySuffix, certFile); err != nil {
		return fmt.Errorf("the new key is in place and its certificate is not: %w; the next start puts it in place", err)
	}
	return syncDirs(certFile)
}

// ResumeRekey completes a rekey a stop interrupted (installPair): when the
// key file holds the new key and its certificate waits aside, the
// certificate is put in place. Any other leftover of a rekey is removed:
// the pair in place is then the old one, whole. It does nothing when no
// rekey was interrupted, or when the key and the certificate share a file,
// which a rekey replaces at once.
func ResumeRekey(certFile, keyFile string) error {
	if filepath.Clean(certFile) == filepath.Clean(keyFile) {
		return nil
	}
	pending := certFile + rekeySuffix
	certPEM, err := os.ReadFile(pending)
	if errors.Is(err, os.ErrNotExist) {
		return resumeKeyAside(certFile, keyFile)
	}
	if err != nil {
		return err
	}
	keyPEM, err := os.ReadFile(keyFile)
	if err != nil {
		return err
	}
	if _, err := tls.X509KeyPair(certPEM, keyPEM); err == nil {
		// The key was renamed in: its certificate follows it.
		if err := os.Rename(pending, certFile); err != nil {
			return fmt.Errorf("completing an interrupted key change: %w", err)
		}
		if err := removeIfAny(keyFile + rekeySuffix); err != nil {
			return err
		}
		return syncDirs(certFile, keyFile)
	}
	// The key was not renamed in: the old pair stands.
	if err := removeIfAny(keyFile + rekeySuffix); err != nil {
		return err
	}
	return removeIfAny(pending)
}

// resumeKeyAside settles a new key left aside with no certificate waiting:
// written before the certificate was (the old pair stands, and it is
// removed), or, should a file system have kept the certificate's rename and
// lost the key's, the key of the certificate in place, which it then
// joins.
func resumeKeyAside(certFile, keyFile string) error {
	aside := keyFile + rekeySuffix
	keyPEM, err := os.ReadFile(aside)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	certPEM, err := os.ReadFile(certFile)
	if err != nil {
		return err
	}
	if _, err := tls.X509KeyPair(certPEM, keyPEM); err != nil {
		return removeIfAny(aside)
	}
	if err := os.Rename(aside, keyFile); err != nil {
		return fmt.Errorf("completing an interrupted key change: %w", err)
	}
	return syncDirs(keyFile)
}

func removeIfAny(path string) error {
	if err := os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	return nil
}

func writeSynced(path string, b []byte) error {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o600)
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
		_ = os.Remove(path)
	}
	return err
}

// syncDirs makes the renames of a rekey durable, in each directory they
// happened in.
func syncDirs(files ...string) error {
	done := map[string]bool{}
	for _, f := range files {
		dir := filepath.Dir(f)
		if done[dir] {
			continue
		}
		done[dir] = true
		d, err := os.Open(dir)
		if err != nil {
			return err
		}
		err = d.Sync()
		if cerr := d.Close(); err == nil {
			err = cerr
		}
		if err != nil {
			return err
		}
	}
	return nil
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
