#!/bin/sh
# Reissues one node's certificate for a NEW cluster id, over the SAME key,
# after a Recover (kitsunium ADR 0152 D17): the recovered cluster keeps the
# member's SPKI pin, and its transport accepts only a certificate whose URI
# SAN names the cluster it now belongs to.
#   recert.sh <slot> <new cluster id>
set -eu
slot="$1" cluster="$2"
PKI="${PKI:-/pki}"
dir="${NODES:-/nodes}/$slot/tls"
NODE_UID="${FORGEJO_UID:-1000}"
echo "$cluster" | grep -Eq '^[0-9a-f]{32}$' || { echo "recert: bad cluster id '$cluster'" >&2; exit 1; }
[ -s "$dir/node.key" ] || { echo "recert: no key in $dir" >&2; exit 1; }
umask 077
uri="kitsunium-consensus://$cluster/slot/$slot"
openssl req -new -key "$dir/node.key" -subj "/CN=$slot" -out "$dir/node.csr"
cat >"$dir/node.ext" <<EXT
basicConstraints=critical,CA:FALSE
keyUsage=critical,digitalSignature
extendedKeyUsage=serverAuth,clientAuth
subjectAltName=URI:$uri
EXT
openssl x509 -req -in "$dir/node.csr" -CA "$PKI/ca.pem" -CAkey "$PKI/ca.key" \
  -CAcreateserial -days 825 -sha256 -extfile "$dir/node.ext" -out "$dir/node.pem"
rm -f "$dir/node.csr" "$dir/node.ext"
echo "$cluster" >"$dir/cluster-id"
if [ "$(id -u)" -eq 0 ]; then chown -R "$NODE_UID:$NODE_UID" "$dir"; fi
chmod 0644 "$dir/node.pem" "$dir/cluster-id"
echo "recert: slot=$slot uri=$uri spki=$(openssl pkey -in "$dir/node.key" -pubout -outform DER | openssl dgst -sha256 -r | cut -d' ' -f1)"
