// Copyright 2026 The Forgejo Authors. All rights reserved.
// SPDX-License-Identifier: GPL-3.0-or-later

package certrenew

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/sha256"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"errors"
	"math/big"
	"net/url"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const cluster = "0f079e10a1b2c3d4e5f60718293a4b5c"

type testCA struct {
	cert *x509.Certificate
	key  *ecdsa.PrivateKey
	file string
}

func newCA(t *testing.T, dir string) *testCA {
	key, _ := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	tpl := &x509.Certificate{
		SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "ca"},
		NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(24 * time.Hour),
		IsCA: true, BasicConstraintsValid: true, KeyUsage: x509.KeyUsageCertSign,
	}
	der, err := x509.CreateCertificate(rand.Reader, tpl, tpl, &key.PublicKey, key)
	require.NoError(t, err)
	cert, _ := x509.ParseCertificate(der)
	file := filepath.Join(dir, "ca.pem")
	require.NoError(t, os.WriteFile(file, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}), 0o600))
	return &testCA{cert: cert, key: key, file: file}
}

// localSigner is a CA in process, as Vault's sign endpoint behaves: it
// honours the CSR's key and the requested URI SAN.
type localSigner struct {
	ca      *testCA
	life    time.Duration
	san     string             // overrides the SAN, to test the check
	rekey   bool               // signs another key, to test the check
	eku     []x509.ExtKeyUsage // overrides the usages, to test the check
	called  int
	failing bool
}

func (s *localSigner) Sign(_ context.Context, csrPEM []byte, uriSAN string) ([]byte, error) {
	s.called++
	if s.failing {
		return nil, errors.New("vault sealed")
	}
	blk, _ := pem.Decode(csrPEM)
	csr, err := x509.ParseCertificateRequest(blk.Bytes)
	if err != nil {
		return nil, err
	}
	if err := csr.CheckSignature(); err != nil {
		return nil, err
	}
	pub := csr.PublicKey
	if s.rekey {
		k, _ := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
		pub = &k.PublicKey
	}
	if s.san != "" {
		uriSAN = s.san
	}
	u, _ := url.Parse(uriSAN)
	tpl := &x509.Certificate{
		SerialNumber: big.NewInt(time.Now().UnixNano()), Subject: pkix.Name{CommonName: "node"},
		NotBefore: time.Now().Add(-time.Minute), NotAfter: time.Now().Add(s.life), URIs: []*url.URL{u},
		KeyUsage: x509.KeyUsageDigitalSignature, ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth},
	}
	if s.eku != nil {
		tpl.ExtKeyUsage = s.eku
	}
	der, err := x509.CreateCertificate(rand.Reader, tpl, s.ca.cert, pub, s.ca.key)
	if err != nil {
		return nil, err
	}
	return pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}), nil
}

// a node whose key and first certificate share one file, as the lab writes.
func newNode(t *testing.T, s *localSigner) *Renewer {
	dir := t.TempDir()
	ca := newCA(t, dir)
	s.ca = ca
	key, _ := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	kb, _ := x509.MarshalECPrivateKey(key)
	keyPEM := pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: kb})
	file := filepath.Join(dir, "node.pem")
	require.NoError(t, os.WriteFile(file, keyPEM, 0o600))
	r := &Renewer{CertFile: file, KeyFile: file, CAFile: ca.file, Cluster: cluster, Slot: "node-a", Signer: s, Jitter: JitterFor("node-a")}
	_, err := r.RenewNow(context.Background())
	require.NoError(t, err)
	return r
}

func spki(c *x509.Certificate) [32]byte { return sha256.Sum256(c.RawSubjectPublicKeyInfo) }

func TestRenewKeepsTheKeyAndTheSAN(t *testing.T) {
	s := &localSigner{life: time.Hour}
	r := newNode(t, s)
	first, err := r.Current()
	require.NoError(t, err)
	second, err := r.RenewNow(context.Background())
	require.NoError(t, err)
	assert.Equal(t, spki(first), spki(second), "the SPKI pin holds")
	assert.NotEqual(t, first.SerialNumber, second.SerialNumber)
	require.Len(t, second.URIs, 1)
	assert.Equal(t, URISAN(cluster, "node-a"), second.URIs[0].String())
	// The file still holds the certificate and the key together.
	_, _, err = r.key()
	require.NoError(t, err)
	cur, err := r.Current()
	require.NoError(t, err)
	assert.Equal(t, second.SerialNumber, cur.SerialNumber)
}

func TestRenewRefusesWhatThePinWouldRefuse(t *testing.T) {
	s := &localSigner{life: time.Hour}
	r := newNode(t, s)
	before, _ := r.Current()

	s.rekey = true
	_, err := r.RenewNow(context.Background())
	require.ErrorContains(t, err, "another key")
	s.rekey, s.san = false, URISAN(cluster, "node-b")
	_, err = r.RenewNow(context.Background())
	require.ErrorContains(t, err, "URI SAN")
	s.san = ""
	for _, eku := range [][]x509.ExtKeyUsage{{x509.ExtKeyUsageServerAuth}, {x509.ExtKeyUsageClientAuth}} {
		s.eku = eku
		_, err = r.RenewNow(context.Background())
		require.ErrorContains(t, err, "both serverAuth and clientAuth")
	}
	s.eku = nil
	s.failing = true
	_, err = r.RenewNow(context.Background())
	require.Error(t, err)

	after, _ := r.Current()
	assert.Equal(t, before.SerialNumber, after.SerialNumber, "a refused renewal leaves the certificate in use")
}

// A URI SAN that would name no cluster, or no slot, is never asked of the
// signer (#185); with Identity, the certificate in use names them.
func TestRenewNeverAsksForAURISANWithoutACluster(t *testing.T) {
	s := &localSigner{life: time.Hour}
	r := newNode(t, s)
	called := s.called
	for _, c := range []struct{ cluster, slot string }{{"", "node-a"}, {cluster, ""}} {
		r.Cluster, r.Slot = c.cluster, c.slot
		_, err := r.RenewNow(context.Background())
		require.ErrorContains(t, err, "refusing to renew")
	}
	assert.Equal(t, called, s.called, "the signer was asked")

	var named []string
	r.Cluster, r.Slot = "", "node-a"
	r.Identity = func(c *x509.Certificate) (string, string, error) {
		named = append(named, c.URIs[0].String())
		return cluster, "node-a", nil
	}
	renewed, err := r.RenewNow(context.Background())
	require.NoError(t, err)
	assert.Equal(t, URISAN(cluster, "node-a"), renewed.URIs[0].String(), "the certificate in use names the cluster")
	assert.Equal(t, []string{URISAN(cluster, "node-a"), URISAN(cluster, "node-a")}, named, "the certificate in use, then the URI SAN to ask for, are checked")
}

func TestDueAtTwoThirdsWithJitter(t *testing.T) {
	c := &x509.Certificate{NotBefore: time.Unix(0, 0), NotAfter: time.Unix(0, 0).Add(30 * 24 * time.Hour)}
	r := &Renewer{Jitter: 0}
	assert.Equal(t, time.Unix(0, 0).Add(20*24*time.Hour), r.Due(c))
	r.Jitter = 0.999
	assert.True(t, r.Due(c).Before(c.NotAfter))
	// Three slots renew at three different points.
	a, b, d := JitterFor("node-a"), JitterFor("node-b"), JitterFor("node-c")
	assert.NotEqual(t, a, b)
	assert.NotEqual(t, b, d)
}

func TestRunRenewsWhenDue(t *testing.T) {
	s := &localSigner{life: 3 * time.Second}
	r := newNode(t, s)
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()
	renewed := make(chan *x509.Certificate, 64)
	done := make(chan struct{})
	go func() {
		defer close(done)
		r.Run(ctx, 200*time.Millisecond, func(error) {}, func(c *x509.Certificate) {
			select {
			case renewed <- c:
			default:
			}
		})
	}()
	select {
	case c := <-renewed:
		assert.True(t, c.NotAfter.After(time.Now()))
	case <-ctx.Done():
		t.Error("no renewal before the certificate's last third")
	}
	cancel()
	<-done
}

// Against a real Vault (the lab's dev container), when VAULT_ADDR,
// VAULT_TOKEN and RENEW_TEST_CA are set: the pki mount and role are the
// lab's (contrib/cluster-lab/scripts/vault-renew-e2e.sh prepares them).
func TestVaultSigns(t *testing.T) {
	addr, tok, caFile := os.Getenv("VAULT_ADDR"), os.Getenv("VAULT_TOKEN"), os.Getenv("RENEW_TEST_CA")
	if addr == "" || tok == "" || caFile == "" {
		t.Skip("VAULT_ADDR, VAULT_TOKEN and RENEW_TEST_CA not set")
	}
	dir := t.TempDir()
	key, _ := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	kb, _ := x509.MarshalECPrivateKey(key)
	file := filepath.Join(dir, "node.pem")
	require.NoError(t, os.WriteFile(file, pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: kb}), 0o600))
	v := &Vault{
		Addr: addr, Mount: "pki-consensus", Role: "node-a", TTL: time.Hour,
		Token: func(context.Context) (string, error) { return tok, nil },
	}
	r := &Renewer{CertFile: file, KeyFile: file, CAFile: caFile, Cluster: cluster, Slot: "node-a", Signer: v}
	c1, err := r.RenewNow(context.Background())
	require.NoError(t, err)
	c2, err := r.RenewNow(context.Background())
	require.NoError(t, err)
	assert.Equal(t, spki(c1), spki(c2))
	pub, _ := x509.MarshalPKIXPublicKey(&key.PublicKey)
	assert.Equal(t, sha256.Sum256(pub), spki(c2))
	// Another slot's SAN is refused by the role's exact allowed_uri_sans.
	r.Slot = "node-b"
	_, err = r.RenewNow(context.Background())
	require.Error(t, err)
}
