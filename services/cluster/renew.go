// Copyright 2026 The Forgejo Authors. All rights reserved.
// SPDX-License-Identifier: GPL-3.0-or-later

package cluster

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"

	"forgejo.org/modules/log"
	"forgejo.org/modules/setting"
	"forgejo.org/services/cluster/certrenew"
	"forgejo.org/services/cluster/nomadgate"
)

// NewRenewer builds the node's certificate renewer from [cluster], or nil
// when RENEW_VAULT_ADDR is empty.
func NewRenewer() *certrenew.Renewer {
	if setting.Cluster.RenewVaultAddr == "" {
		return nil
	}
	client, err := httpClientFor(setting.Cluster.RenewVaultAddr, setting.Cluster.RenewVaultCAFile, 30*time.Second)
	if err != nil {
		log.Error("cluster: the renewer is off: RENEW_VAULT_CA_FILE: %v", err)
		return nil
	}
	v := &certrenew.Vault{
		Addr: setting.Cluster.RenewVaultAddr, Mount: setting.Cluster.RenewVaultMount, Role: setting.Cluster.RenewVaultRole,
		TTL: setting.Cluster.RenewTTL, Client: client,
	}
	var sources []func(context.Context) (string, error)
	if setting.Cluster.RenewVaultJWTFile != "" {
		login := &certrenew.JWTLogin{
			Addr: setting.Cluster.RenewVaultAddr, Mount: setting.Cluster.RenewVaultAuthMount,
			Role: setting.Cluster.RenewVaultAuthRole, JWTFile: setting.Cluster.RenewVaultJWTFile, Client: client,
		}
		sources = append(sources, login.Token)
		v.Denied = login.Forget
	}
	if setting.Cluster.RenewVaultTokenFile != "" {
		sources = append(sources, certrenew.TokenFile(setting.Cluster.RenewVaultTokenFile))
	}
	v.Token = certrenew.FirstOf(func(err error) {
		log.Warn("cluster: the workload identity login to Vault failed (%v); using RENEW_VAULT_TOKEN_FILE", err)
	}, sources...)
	return &certrenew.Renewer{
		CertFile: setting.Cluster.TLSCertFile, KeyFile: setting.Cluster.TLSKeyFile, CAFile: setting.Cluster.TLSCAFile,
		Cluster: setting.Cluster.ClusterID, Slot: setting.Cluster.Slot, Jitter: certrenew.JitterFor(setting.Cluster.Slot),
		// The URI SAN the members check, from the certificate in use:
		// CLUSTER_ID may stay empty, the certificate names the cluster.
		Identity: certIdentity,
		Signer:   v,
	}
}

// certIdentity returns the cluster and the slot a node certificate's
// canonical URI SAN names, as a member checks it.
func certIdentity(c *x509.Certificate) (cluster, slot string, err error) {
	p, err := peerFromChain(c)
	return p.Cluster, p.Slot, err
}

// runRenewer keeps the node certificate fresh until ctx ends. The
// transport and the object channel reload the file on change, and
// watchIdentity rotates the open connections (see rotate.go).
func runRenewer(ctx context.Context, r *certrenew.Renewer) {
	r.Run(ctx, 10*time.Minute,
		func(err error) { log.Error("cluster: renewing the node certificate: %v; retrying", err) },
		func(c *x509.Certificate) {
			log.Info("cluster: node certificate renewed, serial %s, valid until %s", c.SerialNumber, c.NotAfter.Format(time.RFC3339))
		})
}

// nomadGate is the JOIN drain check, from [cluster] NOMAD_ADDR unless a
// test set one.
func (r *Replicator) nomadGate() *nomadgate.Gate {
	if r.gate != nil {
		return r.gate
	}
	if setting.Cluster.NomadAddr == "" {
		return nil
	}
	g := &nomadgate.Gate{Addr: setting.Cluster.NomadAddr, TokenFile: setting.Cluster.NomadTokenFile, SlotMeta: setting.Cluster.NomadSlotMeta}
	if !strings.HasPrefix(g.Addr, "unix://") {
		client, err := httpClientFor(g.Addr, setting.Cluster.NomadCAFile, 10*time.Second)
		if err != nil {
			// The gate fails closed: a JOIN is refused while it cannot read Nomad.
			log.Error("cluster: NOMAD_CA_FILE: %v", err)
			client = &http.Client{Transport: &http.Transport{Proxy: nil, TLSClientConfig: &tls.Config{RootCAs: x509.NewCertPool(), MinVersion: tls.VersionTLS12}}}
		}
		g.Client = client
	}
	return g
}

// httpClientFor builds the client of Vault or Nomad: the private CA when
// one is given. A plain http:// address off the loopback sends tokens in
// clear, which is only acceptable inside an encrypted network (a VPN):
// it is logged.
func httpClientFor(addr, caFile string, timeout time.Duration) (*http.Client, error) {
	if u, err := url.Parse(addr); err == nil && u.Scheme == "http" {
		if ip := net.ParseIP(u.Hostname()); u.Hostname() != "localhost" && (ip == nil || !ip.IsLoopback()) {
			log.Warn("cluster: %s is plain HTTP: tokens cross the network in clear, which only an encrypted network covers", addr)
		}
	}
	if caFile == "" {
		return &http.Client{Timeout: timeout}, nil
	}
	pem, err := os.ReadFile(caFile)
	if err != nil {
		return nil, err
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(pem) {
		return nil, fmt.Errorf("%s holds no certificate", caFile)
	}
	return &http.Client{Timeout: timeout, Transport: &http.Transport{
		Proxy: http.ProxyFromEnvironment, TLSClientConfig: &tls.Config{RootCAs: pool, MinVersion: tls.VersionTLS12},
	}}, nil
}
