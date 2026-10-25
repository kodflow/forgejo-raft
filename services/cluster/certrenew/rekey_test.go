// Copyright 2026 The Forgejo Authors. All rights reserved.
// SPDX-License-Identifier: GPL-3.0-or-later

package certrenew

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"encoding/pem"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// a node whose key and certificate live in two files, as the deployments
// under contrib/cluster/deploy write them.
func newSplitNode(t *testing.T, s *localSigner) *Renewer {
	dir := t.TempDir()
	ca := newCA(t, dir)
	s.ca = ca
	key, _ := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	kb, _ := x509.MarshalECPrivateKey(key)
	keyFile, certFile := filepath.Join(dir, "node.key"), filepath.Join(dir, "node.pem")
	require.NoError(t, os.WriteFile(keyFile, pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: kb}), 0o600))
	require.NoError(t, os.WriteFile(certFile, nil, 0o600))
	r := &Renewer{CertFile: certFile, KeyFile: keyFile, CAFile: ca.file, Cluster: cluster, Slot: "node-a", Signer: s}
	_, err := r.RenewNow(context.Background())
	require.NoError(t, err)
	return r
}

// pairOnDisk loads the pair the node's files hold, as the transport and the
// object channel do, and returns the SPKI of its certificate.
func pairOnDisk(t *testing.T, r *Renewer) [32]byte {
	t.Helper()
	pair, err := tls.LoadX509KeyPair(r.CertFile, r.KeyFile)
	require.NoError(t, err, "the files hold a matching pair")
	leaf, err := x509.ParseCertificate(pair.Certificate[0])
	require.NoError(t, err)
	return spki(leaf)
}

// A node about to take a new node id is given a new key, and its
// certificate for the same URI SAN, chained to the CA; the files hold the
// matching pair, whether they share one file or not (#209).
func TestRekeyGivesANewKeyUnderTheSameSAN(t *testing.T) {
	for name, node := range map[string]func(*testing.T, *localSigner) *Renewer{"one file": newNode, "two files": newSplitNode} {
		t.Run(name, func(t *testing.T) {
			s := &localSigner{life: time.Hour}
			r := node(t, s)
			before := pairOnDisk(t, r)
			cert, err := r.Rekey(context.Background())
			require.NoError(t, err)
			assert.NotEqual(t, before, spki(cert), "a new key")
			assert.Equal(t, spki(cert), pairOnDisk(t, r), "the new key and its certificate are in place")
			require.Len(t, cert.URIs, 1)
			assert.Equal(t, URISAN(cluster, "node-a"), cert.URIs[0].String())
			_, err = os.Stat(r.CertFile + rekeySuffix)
			require.ErrorIs(t, err, os.ErrNotExist, "nothing is left aside")

			// A renewal afterwards keeps the new key.
			renewed, err := r.RenewNow(context.Background())
			require.NoError(t, err)
			assert.Equal(t, spki(cert), spki(renewed))
		})
	}
}

// A rekey the signer answers wrongly installs nothing: the pair in use
// stays, whole.
func TestARefusedRekeyLeavesThePairInUse(t *testing.T) {
	for name, node := range map[string]func(*testing.T, *localSigner) *Renewer{"one file": newNode, "two files": newSplitNode} {
		t.Run(name, func(t *testing.T) {
			s := &localSigner{life: time.Hour}
			r := node(t, s)
			before := pairOnDisk(t, r)
			s.rekey = true // signs another key than the CSR's
			_, err := r.Rekey(context.Background())
			require.ErrorContains(t, err, "another key")
			s.rekey, s.san = false, URISAN(cluster, "node-b")
			_, err = r.Rekey(context.Background())
			require.ErrorContains(t, err, "URI SAN")
			s.san, s.failing = "", true
			_, err = r.Rekey(context.Background())
			require.Error(t, err)
			assert.Equal(t, before, pairOnDisk(t, r))
		})
	}
}

// A stop between the two renames of a rekey leaves the new key in place and
// its certificate aside: the next start puts it in place. A stop before
// them leaves the old pair, and what was written aside is removed.
func TestAnInterruptedRekeyIsCompletedOrUndone(t *testing.T) {
	s := &localSigner{life: time.Hour}
	r := newSplitNode(t, s)
	old := pairOnDisk(t, r)
	oldKey, err := os.ReadFile(r.KeyFile)
	require.NoError(t, err)
	oldCert, err := os.ReadFile(r.CertFile)
	require.NoError(t, err)

	// The new pair, as Rekey makes it, written aside.
	newPair := func() ([]byte, []byte, [32]byte) {
		key, _ := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
		cert, err := r.signed(context.Background(), key)
		require.NoError(t, err)
		der, err := x509.MarshalPKCS8PrivateKey(key)
		require.NoError(t, err)
		return pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: cert.Raw}), pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: der}), spki(cert)
	}

	// Stopped before the renames: undone.
	certPEM, keyPEM, _ := newPair()
	require.NoError(t, os.WriteFile(r.KeyFile+rekeySuffix, keyPEM, 0o600))
	require.NoError(t, os.WriteFile(r.CertFile+rekeySuffix, certPEM, 0o600))
	require.NoError(t, ResumeRekey(r.CertFile, r.KeyFile))
	assert.Equal(t, old, pairOnDisk(t, r))
	for _, f := range []string{r.KeyFile + rekeySuffix, r.CertFile + rekeySuffix} {
		_, err := os.Stat(f)
		require.ErrorIs(t, err, os.ErrNotExist, f)
	}

	// Stopped between them: the key is new, its certificate waits aside.
	certPEM, keyPEM, want := newPair()
	require.NoError(t, os.WriteFile(r.KeyFile, keyPEM, 0o600))
	require.NoError(t, os.WriteFile(r.CertFile+rekeySuffix, certPEM, 0o600))
	_, err = tls.LoadX509KeyPair(r.CertFile, r.KeyFile)
	require.Error(t, err, "the files do not match until it is completed")
	require.NoError(t, ResumeRekey(r.CertFile, r.KeyFile))
	assert.Equal(t, want, pairOnDisk(t, r), "completed")

	// Nothing interrupted: nothing changes.
	require.NoError(t, ResumeRekey(r.CertFile, r.KeyFile))
	assert.Equal(t, want, pairOnDisk(t, r))
	assert.NotEqual(t, oldKey, keyPEM)
	assert.NotEqual(t, oldCert, certPEM)
}

// A renewal and a rekey of one renewer never interleave: whatever the
// order, the files end with a matching pair.
func TestRenewalAndRekeyNeverInterleave(t *testing.T) {
	s := &lockedSigner{s: &localSigner{life: time.Hour}}
	r := newSplitNode(t, s.s)
	r.Signer = s
	var wg sync.WaitGroup
	for range 4 {
		wg.Go(func() {
			_, err := r.RenewNow(context.Background())
			assert.NoError(t, err)
		})
		wg.Go(func() {
			_, err := r.Rekey(context.Background())
			assert.NoError(t, err)
		})
	}
	wg.Wait()
	pairOnDisk(t, r)
}

// lockedSigner serializes the test signer, which counts its calls.
type lockedSigner struct {
	mu sync.Mutex
	s  *localSigner
}

func (l *lockedSigner) Sign(ctx context.Context, csrPEM []byte, uriSAN string) ([]byte, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.s.Sign(ctx, csrPEM, uriSAN)
}
