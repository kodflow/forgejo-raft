// Copyright 2026 The Forgejo Authors. All rights reserved.
// SPDX-License-Identifier: GPL-3.0-or-later

package cluster

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"net/http"
	"strings"

	"forgejo.org/modules/clusterhook"
	"forgejo.org/modules/setting"
	"forgejo.org/services/cluster/consensus"
	"forgejo.org/services/cluster/reftable"
)

// rawCookie carries the caller's last write: its token and repository.
const rawCookie = "forgejo_cluster_raw"

// HeaderBehind tells the client the page was served before this node caught
// up with its last write: it may be behind, and is never presented as
// current (ADR 0152 D19, R8-24).
const HeaderBehind = "X-Forgejo-Cluster-Behind"

// ReadAfterWrite is the web middleware of the read-after-write cookie. A
// request that wrote refs through git gets a cookie naming its entry; the
// next request, on whichever node the load balancer picks, waits until that
// node has projected the repository and the consumer's cursor has passed
// the entry, within READ_AFTER_WRITE_TIMEOUT.
func ReadAfterWrite(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		r := Current()
		if r == nil {
			next.ServeHTTP(w, req)
			return
		}
		if c, err := req.Cookie(rawCookie); err == nil {
			if tok, repo, ok := parseRawCookie(c.Value); ok && !r.waitBounded(req, tok, repo) {
				w.Header().Set(HeaderBehind, "1")
			}
		}
		ctx, col := clusterhook.WithCollector(req.Context())
		next.ServeHTTP(&rawWriter{ResponseWriter: w, r: r, col: col}, req.WithContext(ctx))
	})
}

// rawWaiters bounds the requests waiting on a cookie at once: the wait
// happens before authentication, and must not be a lever on the node.
var rawWaiters = make(chan struct{}, 256)

// waitBounded waits for the cookie's write, unless the node does not know
// that entry committed (a cookie from elsewhere is not waited for) or too
// many requests already wait; both answer "maybe behind".
func (r *Replicator) waitBounded(req *http.Request, tok consensus.Token, repo string) bool {
	if eng := r.Engine(); eng == nil || tok.Index > eng.Status().Commit {
		return false
	}
	select {
	case rawWaiters <- struct{}{}:
		defer func() { <-rawWaiters }()
	default:
		return false
	}
	return r.WaitReadAfterWrite(req.Context(), tok, repo)
}

// rawMAC signs a cookie value with the instance's SECRET_KEY: only a value
// this forge issued is waited for.
func rawMAC(v string) string {
	m := hmac.New(sha256.New, []byte("forgejo-cluster-raw\x00"+setting.SecretKey))
	m.Write([]byte(v))
	return base64.RawURLEncoding.EncodeToString(m.Sum(nil))
}

func signRawCookie(tok consensus.Token, repo string) string {
	v := tok.String() + "." + repo
	return v + "~" + rawMAC(v)
}

func parseRawCookie(signed string) (consensus.Token, string, bool) {
	i := strings.LastIndexByte(signed, '~')
	if i < 0 || !hmac.Equal([]byte(signed[i+1:]), []byte(rawMAC(signed[:i]))) {
		return consensus.Token{}, "", false
	}
	hexTok, repo, ok := strings.Cut(signed[:i], ".")
	if !ok || reftable.ValidRepoKey(repo) != nil {
		return consensus.Token{}, "", false
	}
	tok, err := consensus.ParseTokenString(hexTok)
	if err != nil {
		return consensus.Token{}, "", false
	}
	return tok, repo, true
}

// rawWriter sets the cookie when the response starts, from the writes the
// request's git invocations made by then.
type rawWriter struct {
	http.ResponseWriter
	r     *Replicator
	col   *clusterhook.Collector
	wrote bool
}

func (w *rawWriter) WriteHeader(code int) {
	if !w.wrote {
		w.wrote = true
		if tok, repo, ok := w.r.LastWrite(w.col.Nonces()); ok {
			http.SetCookie(w.ResponseWriter, &http.Cookie{
				Name: rawCookie, Value: signRawCookie(tok, repo), Path: setting.AppSubURL + "/",
				MaxAge: 60, HttpOnly: true, Secure: setting.SessionConfig.Secure, SameSite: http.SameSiteLaxMode,
			})
		}
	}
	w.ResponseWriter.WriteHeader(code)
}

func (w *rawWriter) Write(b []byte) (int, error) {
	if !w.wrote {
		w.WriteHeader(http.StatusOK)
	}
	return w.ResponseWriter.Write(b)
}

func (w *rawWriter) Flush() {
	if f, ok := w.ResponseWriter.(http.Flusher); ok {
		f.Flush()
	}
}

func (w *rawWriter) Unwrap() http.ResponseWriter { return w.ResponseWriter }
