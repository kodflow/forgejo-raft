// Copyright 2026 The Forgejo Authors. All rights reserved.
// SPDX-License-Identifier: GPL-3.0-or-later

package cluster

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"crypto/x509"
	"encoding/hex"
	"sort"
	"strings"
	"time"

	"forgejo.org/modules/log"
	"forgejo.org/modules/setting"
	"forgejo.org/services/cluster/consensus"
)

// signingDigests tell a node's token-signing keys apart from another's.
// Behind the load balancer, a token one node signs comes back to whichever
// node the balancer picks, which verifies it with its own key: the members
// must all hold the same, or the others refuse it (#285). A node that is not
// given one draws its own at its first start, in its own app.ini. Each
// digest is an HMAC under the key of a fixed label: it tells two keys apart
// and says nothing of either, every one of them 32 random bytes or a public
// key. SECRET_KEY, which an operator may choose, is not digested.
type signingDigests struct {
	// JWT is the general signing secret's, [oauth2] JWT_SECRET: the
	// container registry's tokens, the Actions' runtime tokens and
	// artifact signatures, OAuth2's under an HS* algorithm.
	JWT string `json:"jwt,omitempty"`
	// LFS is the LFS server's key, when it runs: the tokens `git lfs`
	// receives over SSH ([server] LFS_JWT_SECRET).
	LFS string `json:"lfs,omitempty"`
}

func signingDigest(label string, key []byte) string {
	m := hmac.New(sha256.New, key)
	m.Write([]byte("forgejo-cluster-signing\x00" + label))
	return hex.EncodeToString(m.Sum(nil)[:16])
}

// localSigning is this node's digests.
func localSigning() *signingDigests {
	d := &signingDigests{JWT: signingDigest("jwt", setting.GetGeneralTokenSigningSecret())}
	if k := setting.LFS.SigningKey; setting.LFS.StartServer && k != nil {
		label := "lfs " + k.SigningMethod().Alg()
		if b, ok := k.VerifyKey().([]byte); ok {
			d.LFS = signingDigest(label, b)
		} else if pub, err := x509.MarshalPKIXPublicKey(k.VerifyKey()); err == nil {
			d.LFS = signingDigest(label, pub)
		}
	}
	return d
}

// signingDiffers names each member of m, but self, whose answer among seen
// says other keys than mine: its slot and which. A member that does not
// answer, or answers without digests (an older binary), is not compared,
// nor a key one of the two does not say (an LFS server off).
func signingDiffers(mine *signingDigests, m consensus.Membership, self consensus.NodeID, seen []hello, answerOf func(consensus.Member, []hello) (hello, bool)) []string {
	var out []string
	for _, x := range m.Members {
		if x.ID == self {
			continue
		}
		h, ok := answerOf(x, seen)
		if !ok || h.Signing == nil {
			continue
		}
		var which []string
		if mine.JWT != "" && h.Signing.JWT != "" && mine.JWT != h.Signing.JWT {
			which = append(which, "[oauth2] JWT_SECRET")
		}
		if mine.LFS != "" && h.Signing.LFS != "" && mine.LFS != h.Signing.LFS {
			which = append(which, "[server] LFS_JWT_SECRET")
		}
		if len(which) > 0 {
			out = append(out, x.Slot+" ("+strings.Join(which, ", ")+")")
		}
	}
	sort.Strings(out)
	return out
}

// signingProbeEvery paces the leader's comparison of the signing keys.
const signingProbeEvery = 5 * time.Minute

// maybeCheckSigning is the leader's comparison of the members' token-signing
// keys with its own, at most once per signingProbeEvery of its clock. What
// differs is logged as an error each time it changes, and its end once:
// nothing is refused, as a rolling change of the secrets differs until its
// last member restarted.
func (r *Replicator) maybeCheckSigning(ctx context.Context, eng consensus.Engine) {
	now := r.clock()
	if eng.Status().Role != consensus.RoleLeader || now.Before(r.signingProbe) {
		return
	}
	r.signingProbe = now.Add(signingProbeEvery)
	differs := strings.Join(signingDiffers(localSigning(), eng.Membership(), eng.Status().ID, r.memberAnswers(ctx, eng), r.answerOf), ", ")
	if differs == r.signingNoted {
		return
	}
	r.signingNoted = differs
	if differs == "" {
		log.Info("cluster: every member that answers signs its tokens with this node's keys")
		return
	}
	log.Error("cluster: %s sign their tokens with other keys than this node's: behind the load balancer, a container registry, Actions or LFS token one node hands out is refused by the others. Give every node the same [oauth2] JWT_SECRET and [server] LFS_JWT_SECRET, as SECRET_KEY (contrib/cluster/README.md, Requirements)", differs)
}
