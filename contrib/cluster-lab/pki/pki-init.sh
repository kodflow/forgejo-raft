#!/bin/sh
# One-shot PKI of the lab (kitsunium ADR 0152, D4, D19, D20, R7-41, R8-07).
#
# - One cluster id, 16 random bytes in lowercase hex (32 characters).
# - One CA, kept in the pki volume, which no Forgejo node mounts.
# - Per slot (node-a, node-b, node-c), the private key is generated INSIDE that node's
#   data volume, mounted here on /nodes/<slot>, and never leaves it. The CA
#   signs a CSR. The certificate carries exactly one URI SAN,
#   kitsunium-consensus://<cluster>/slot/<slot>, with serverAuth and clientAuth.
# - The SPKI pin is the SHA-256 of the key's DER SubjectPublicKeyInfo, in hex,
#   written beside the key and collected in /pki/spki.txt (slot=hex).
# - Rerunning keeps every key (a renewal reuses the key, R8-07) and reissues
#   a certificate only when it is missing or RENEW=1.
set -eu

PKI="${PKI:-/pki}"
NODES="${NODES:-/nodes}"
SLOTS="${CLUSTER_SLOTS:-node-a node-b node-c}"
NODE_UID="${FORGEJO_UID:-1000}"

umask 077
mkdir -p "$PKI"

[ -s "$PKI/cluster-id" ] || openssl rand -hex 16 >"$PKI/cluster-id"
CLUSTER_ID="$(cat "$PKI/cluster-id")"
echo "$CLUSTER_ID" | grep -Eq '^[0-9a-f]{32}$' || { echo "pki: bad cluster id '$CLUSTER_ID'" >&2; exit 1; }

if [ ! -s "$PKI/ca.key" ]; then
  openssl genpkey -algorithm EC -pkeyopt ec_paramgen_curve:P-256 -out "$PKI/ca.key"
  openssl req -x509 -new -key "$PKI/ca.key" -sha256 -days 3650 \
    -subj "/CN=forgejo-cluster-lab CA $CLUSTER_ID" \
    -addext "basicConstraints=critical,CA:TRUE,pathlen:0" \
    -addext "keyUsage=critical,keyCertSign,cRLSign" -out "$PKI/ca.pem"
fi
chmod 0644 "$PKI/ca.pem" "$PKI/cluster-id"

: >"$PKI/spki.txt"
for slot in $SLOTS; do
  [ -d "$NODES/$slot" ] || { echo "pki: $NODES/$slot is not mounted" >&2; exit 1; }
  dir="$NODES/$slot/tls"
  mkdir -p "$dir"
  uri="kitsunium-consensus://$CLUSTER_ID/slot/$slot"

  if [ ! -s "$dir/node.key" ]; then
    openssl genpkey -algorithm EC -pkeyopt ec_paramgen_curve:P-256 -out "$dir/node.key"
    rm -f "$dir/node.pem"
  fi
  if [ ! -s "$dir/node.pem" ] || [ "${RENEW:-0}" = 1 ]; then
    openssl req -new -key "$dir/node.key" -subj "/CN=$slot" -out "$dir/node.csr"
    cat >"$dir/node.ext" <<EOF
basicConstraints=critical,CA:FALSE
keyUsage=critical,digitalSignature
extendedKeyUsage=serverAuth,clientAuth
subjectAltName=URI:$uri
EOF
    openssl x509 -req -in "$dir/node.csr" -CA "$PKI/ca.pem" -CAkey "$PKI/ca.key" \
      -CAcreateserial -days 825 -sha256 -extfile "$dir/node.ext" -out "$dir/node.pem"
    rm -f "$dir/node.csr" "$dir/node.ext"
  fi

  # Exactly one URI SAN, the canonical one, and a chain to the CA.
  sans="$(openssl x509 -in "$dir/node.pem" -noout -ext subjectAltName | tail -n +2 | tr -d ' ')"
  [ "$sans" = "URI:$uri" ] || { echo "pki: $slot SAN is '$sans', expected URI:$uri" >&2; exit 1; }
  openssl verify -CAfile "$PKI/ca.pem" "$dir/node.pem" >/dev/null

  openssl pkey -in "$dir/node.key" -pubout -outform DER | openssl dgst -sha256 -r | cut -d' ' -f1 >"$dir/spki"
  cp "$PKI/ca.pem" "$dir/ca.pem"
  cp "$PKI/cluster-id" "$dir/cluster-id"
  echo "$slot=$(cat "$dir/spki")" >>"$PKI/spki.txt"
  if [ "$(id -u)" -eq 0 ]; then chown -R "$NODE_UID:$NODE_UID" "$dir"; fi
  chmod 0700 "$dir"
  chmod 0600 "$dir/node.key"
  chmod 0644 "$dir/node.pem" "$dir/ca.pem" "$dir/spki" "$dir/cluster-id"
  echo "pki: slot=$slot uri=$uri spki=$(cat "$dir/spki")"
done
chmod 0644 "$PKI/spki.txt"
