#!/bin/sh
# The cluster's PKI for the single-host compose example, run once by the
# "pki" service (./bootstrap.sh does it).
#
# - One CA, kept in the pki volume, which no Forgejo node mounts.
# - Per slot, the private key is generated INSIDE that node's volume
#   (/nodes/<slot>/tls) and never leaves it. The certificate carries exactly
#   one URI SAN, kitsunium-consensus://<CLUSTER_ID>/slot/<slot>, and both
#   the serverAuth and clientAuth usages: every member is a TLS server and
#   a TLS client of the others.
# - Rerunning keeps every key. The member's identity is pinned on its key
#   (SPKI), so a renewal must reuse the key, never draw a new one.
set -eu

apk add --no-cache openssl >/dev/null

PKI=/pki
NODES=/nodes
NODE_UID="${FORGEJO_UID:-1000}"
echo "$CLUSTER_ID" | grep -Eq '^[0-9a-f]{32}$' || { echo "pki: CLUSTER_ID must be 32 lowercase hex digits" >&2; exit 1; }

umask 077
if [ ! -s "$PKI/ca.key" ]; then
  openssl genpkey -algorithm EC -pkeyopt ec_paramgen_curve:P-256 -out "$PKI/ca.key"
  openssl req -x509 -new -key "$PKI/ca.key" -sha256 -days 3650 \
    -subj "/CN=forgejo-raft CA $CLUSTER_ID" \
    -addext "basicConstraints=critical,CA:TRUE,pathlen:0" \
    -addext "keyUsage=critical,keyCertSign,cRLSign" -out "$PKI/ca.pem"
fi

for slot in $SLOTS; do
  dir="$NODES/$slot/tls"
  mkdir -p "$dir"
  uri="kitsunium-consensus://$CLUSTER_ID/slot/$slot"
  if [ ! -s "$dir/node.key" ]; then
    openssl genpkey -algorithm EC -pkeyopt ec_paramgen_curve:P-256 -out "$dir/node.key"
    rm -f "$dir/node.pem"
  fi
  if [ ! -s "$dir/node.pem" ]; then
    openssl req -new -key "$dir/node.key" -subj "/CN=$slot" -out "$dir/node.csr"
    printf '%s\n' \
      "basicConstraints=critical,CA:FALSE" \
      "keyUsage=critical,digitalSignature" \
      "extendedKeyUsage=serverAuth,clientAuth" \
      "subjectAltName=URI:$uri" >"$dir/node.ext"
    openssl x509 -req -in "$dir/node.csr" -CA "$PKI/ca.pem" -CAkey "$PKI/ca.key" \
      -CAcreateserial -days 825 -sha256 -extfile "$dir/node.ext" -out "$dir/node.pem"
    rm -f "$dir/node.csr" "$dir/node.ext"
  fi
  cp "$PKI/ca.pem" "$dir/ca.pem"
  chown -R "$NODE_UID:$NODE_UID" "$NODES/$slot"
  chmod 0700 "$dir"
  chmod 0600 "$dir/node.key"
  chmod 0644 "$dir/node.pem" "$dir/ca.pem"
  echo "pki: $slot $uri"
done
