// Copyright 2026 The Forgejo Authors. All rights reserved.
// SPDX-License-Identifier: GPL-3.0-or-later

package cluster

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"fmt"
	"io"
	"math/big"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"forgejo.org/modules/json"
	"forgejo.org/modules/setting"
	"forgejo.org/modules/test"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const renewCluster = "0f079e10a1b2c3d4e5f60718293a4b5c"

// renewVault is Vault's pki-consensus/sign/<slot> as vault-setup.sh
// configures the role: allowed_uri_sans is the slot's exact URI,
// use_csr_sans=false, so the URI SAN signed is the request's uri_sans, and
// any other is refused 400. permissive signs whatever is asked, to show
// what the node would then hold.
type renewVault struct {
	ca         *x509.Certificate
	caKey      *ecdsa.PrivateKey
	allowed    string
	permissive bool

	mu    sync.Mutex
	asked []string
}

func (v *renewVault) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	b, _ := io.ReadAll(r.Body)
	var req struct {
		CSR     string `json:"csr"`
		URISANs string `json:"uri_sans"`
	}
	_ = json.Unmarshal(b, &req)
	v.mu.Lock()
	v.asked = append(v.asked, req.URISANs)
	permissive := v.permissive
	v.mu.Unlock()
	if !permissive && req.URISANs != v.allowed {
		w.WriteHeader(http.StatusBadRequest)
		fmt.Fprintf(w, `{"errors":["URI Subject Alternative Names are not allowed in this role: %s"]}`, req.URISANs)
		return
	}
	blk, _ := pem.Decode([]byte(req.CSR))
	csr, err := x509.ParseCertificateRequest(blk.Bytes)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	u, _ := url.Parse(req.URISANs)
	tpl := &x509.Certificate{
		SerialNumber: big.NewInt(time.Now().UnixNano()), Subject: pkix.Name{CommonName: "node-a"},
		NotBefore: time.Now().Add(-time.Minute), NotAfter: time.Now().Add(720 * time.Hour), URIs: []*url.URL{u},
		KeyUsage: x509.KeyUsageDigitalSignature, ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth},
	}
	der, err := x509.CreateCertificate(rand.Reader, tpl, v.ca, csr.PublicKey, v.caKey)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	out, _ := json.Marshal(map[string]any{"data": map[string]string{"certificate": string(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}))}})
	_, _ = w.Write(out)
}

func (v *renewVault) setPermissive() {
	v.mu.Lock()
	v.permissive = true
	v.mu.Unlock()
}

func (v *renewVault) uriSANsAsked() []string {
	v.mu.Lock()
	defer v.mu.Unlock()
	return append([]string(nil), v.asked...)
}

// renewNode writes a CA, a node key and its first certificate, issued by
// hand with the URI SAN san, as vault-setup.sh tells the operator to (the
// canonical one), and serves a Vault whose role allows the canonical one.
func renewNode(t *testing.T, san string) (v *renewVault, certFile string) {
	t.Helper()
	dir := t.TempDir()
	caKey, _ := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	caTpl := &x509.Certificate{
		SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "ca"},
		NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(24 * 365 * time.Hour),
		IsCA: true, BasicConstraintsValid: true, KeyUsage: x509.KeyUsageCertSign,
	}
	caDER, err := x509.CreateCertificate(rand.Reader, caTpl, caTpl, &caKey.PublicKey, caKey)
	require.NoError(t, err)
	ca, _ := x509.ParseCertificate(caDER)
	caFile := filepath.Join(dir, "ca.pem")
	require.NoError(t, os.WriteFile(caFile, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: caDER}), 0o600))

	key, _ := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	kb, _ := x509.MarshalECPrivateKey(key)
	keyFile := filepath.Join(dir, "node.key")
	require.NoError(t, os.WriteFile(keyFile, pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: kb}), 0o600))

	u, _ := url.Parse(san)
	tpl := &x509.Certificate{
		SerialNumber: big.NewInt(2), Subject: pkix.Name{CommonName: "node-a"},
		// 30 days, two thirds gone: due for renewal now.
		NotBefore: time.Now().Add(-21 * 24 * time.Hour), NotAfter: time.Now().Add(9 * 24 * time.Hour), URIs: []*url.URL{u},
		KeyUsage: x509.KeyUsageDigitalSignature, ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth},
	}
	der, err := x509.CreateCertificate(rand.Reader, tpl, ca, &key.PublicKey, caKey)
	require.NoError(t, err)
	certFile = filepath.Join(dir, "node.pem")
	require.NoError(t, os.WriteFile(certFile, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}), 0o600))

	v = &renewVault{ca: ca, caKey: caKey, allowed: "kitsunium-consensus://" + renewCluster + "/slot/node-a"}
	srv := httptest.NewServer(v)
	t.Cleanup(srv.Close)
	tokenFile := filepath.Join(dir, "token")
	require.NoError(t, os.WriteFile(tokenFile, []byte("t"), 0o600))

	t.Cleanup(test.MockVariableValue(&setting.Cluster.RenewVaultAddr, srv.URL))
	t.Cleanup(test.MockVariableValue(&setting.Cluster.RenewVaultMount, "pki-consensus"))
	t.Cleanup(test.MockVariableValue(&setting.Cluster.RenewVaultRole, "node-a"))
	t.Cleanup(test.MockVariableValue(&setting.Cluster.RenewVaultTokenFile, tokenFile))
	t.Cleanup(test.MockVariableValue(&setting.Cluster.RenewVaultJWTFile, ""))
	t.Cleanup(test.MockVariableValue(&setting.Cluster.RenewVaultCAFile, ""))
	t.Cleanup(test.MockVariableValue(&setting.Cluster.TLSCertFile, certFile))
	t.Cleanup(test.MockVariableValue(&setting.Cluster.TLSKeyFile, keyFile))
	t.Cleanup(test.MockVariableValue(&setting.Cluster.TLSCAFile, caFile))
	t.Cleanup(test.MockVariableValue(&setting.Cluster.Slot, "node-a"))
	// The shipped Nomad job (contrib/cluster/deploy/nomad) set no
	// CLUSTER_ID, and the autonomous mode documents that it may stay
	// empty: the certificate names the cluster.
	t.Cleanup(test.MockVariableValue(&setting.Cluster.ClusterID, ""))
	return v, certFile
}

// With CLUSTER_ID empty, the in-process renewer asked Vault for the URI
// SAN kitsunium-consensus:///slot/node-a: the role refused it, at every
// retry, and the certificate was never renewed, until every node's expired
// and the cluster lost its majority. A Vault that signed it anyway handed
// the node a certificate no member admits, and that the node itself
// refused at its next start (#185). The renewer now asks for the URI SAN
// the certificate in use names.
func TestTheRenewerTakesTheClusterFromTheCertificate(t *testing.T) {
	canonical := "kitsunium-consensus://" + renewCluster + "/slot/node-a"
	v, _ := renewNode(t, canonical)

	// What the node's certificate says: the cluster is known.
	id, err := newIdentity(setting.Cluster.TLSCertFile, setting.Cluster.TLSKeyFile, setting.Cluster.TLSCAFile)
	require.NoError(t, err)
	cid, err := autoClusterID(id)
	require.NoError(t, err)
	require.Equal(t, renewCluster, cid.String())

	rn := NewRenewer()
	require.NotNil(t, rn)
	_, err = rn.RenewNow(context.Background())
	require.NoError(t, err, "the documented renewal never succeeds")
	assert.Equal(t, []string{canonical}, v.uriSANsAsked(), "the URI SAN asked of Vault")

	// A permissive signer signs what is asked: the canonical URI SAN, which
	// every member admits and the node starts again on.
	v.setPermissive()
	c, err := rn.RenewNow(context.Background())
	require.NoError(t, err)
	assert.Equal(t, canonical, c.URIs[0].String())
	_, perr := peerFromChain(c)
	require.NoError(t, perr, "a member admits the renewed certificate")
	id2, err := newIdentity(setting.Cluster.TLSCertFile, setting.Cluster.TLSKeyFile, setting.Cluster.TLSCAFile)
	require.NoError(t, err)
	_, cerr := autoClusterID(id2)
	require.NoError(t, cerr, "the node starts again on its renewed certificate")

	// `forgejo cluster cert renew` builds the same renewer.
	_, err = NewRenewer().RenewNow(context.Background())
	assert.NoError(t, err)
}

// A CLUSTER_ID or a SLOT that differs from what the certificate in use
// names is a misconfiguration: the renewal is refused before anything is
// asked of the signer, which would otherwise sign the node into another
// cluster or slot.
func TestTheRenewerRefusesAnIdentityTheCertificateDoesNotName(t *testing.T) {
	canonical := "kitsunium-consensus://" + renewCluster + "/slot/node-a"
	t.Run("another CLUSTER_ID", func(t *testing.T) {
		v, certFile := renewNode(t, canonical)
		v.setPermissive()
		before, err := os.ReadFile(certFile)
		require.NoError(t, err)
		defer test.MockVariableValue(&setting.Cluster.ClusterID, "aa079e10a1b2c3d4e5f60718293a4b5c")()
		_, err = NewRenewer().RenewNow(context.Background())
		require.Error(t, err)
		assert.Contains(t, err.Error(), "CLUSTER_ID")
		assert.Empty(t, v.uriSANsAsked(), "the signer was asked")
		after, err := os.ReadFile(certFile)
		require.NoError(t, err)
		assert.Equal(t, before, after, "the certificate in use was replaced")
	})
	t.Run("the same CLUSTER_ID", func(t *testing.T) {
		v, _ := renewNode(t, canonical)
		defer test.MockVariableValue(&setting.Cluster.ClusterID, renewCluster)()
		_, err := NewRenewer().RenewNow(context.Background())
		require.NoError(t, err)
		assert.Equal(t, []string{canonical}, v.uriSANsAsked())
	})
	t.Run("another SLOT", func(t *testing.T) {
		v, _ := renewNode(t, canonical)
		v.setPermissive()
		defer test.MockVariableValue(&setting.Cluster.Slot, "node-b")()
		_, err := NewRenewer().RenewNow(context.Background())
		require.Error(t, err)
		assert.Contains(t, err.Error(), "SLOT")
		assert.Empty(t, v.uriSANsAsked())
	})
}

// A certificate in use whose URI SAN names no cluster, as a permissive
// signer issued before this fix, is never renewed into another such: the
// renewer refuses before asking, unless CLUSTER_ID names the cluster, and
// then renews the node back to its canonical URI SAN.
func TestTheRenewerNeverAsksForAURISANWithoutACluster(t *testing.T) {
	broken := "kitsunium-consensus:///slot/node-a"
	t.Run("CLUSTER_ID empty", func(t *testing.T) {
		v, _ := renewNode(t, broken)
		v.setPermissive()
		_, err := NewRenewer().RenewNow(context.Background())
		require.Error(t, err)
		assert.Contains(t, err.Error(), "CLUSTER_ID")
		assert.Empty(t, v.uriSANsAsked(), "a URI SAN without a cluster was asked of the signer")
	})
	t.Run("CLUSTER_ID set", func(t *testing.T) {
		v, _ := renewNode(t, broken)
		defer test.MockVariableValue(&setting.Cluster.ClusterID, renewCluster)()
		c, err := NewRenewer().RenewNow(context.Background())
		require.NoError(t, err)
		canonical := "kitsunium-consensus://" + renewCluster + "/slot/node-a"
		assert.Equal(t, []string{canonical}, v.uriSANsAsked())
		_, err = peerFromChain(c)
		assert.NoError(t, err)
	})
}
