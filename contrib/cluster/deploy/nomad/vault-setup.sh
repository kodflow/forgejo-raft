#!/usr/bin/env bash
# Vault for forgejo-raft under Nomad: the consensus PKI and the workload
# identity login of the certificate renewer. Idempotent, run by an operator
# with a Vault token allowed to manage mounts, policies and auth methods.
#
#   CLUSTER_ID=<32 hex> NOMAD_ADDR=https://nomad.example.internal:4646 ./vault-setup.sh
#
# What it sets up:
#   1. pki-consensus, a PKI mount. Its CA is the cluster's CA (TLS_CA_FILE on
#      every node). Without PKI_CA_BUNDLE an internal root is generated;
#      with it (a PEM file holding a CA certificate and its key), that CA
#      is imported instead.
#   2. One PKI role per slot. It signs a CSR for exactly one URI SAN,
#      kitsunium-consensus://<CLUSTER_ID>/slot/<slot>, and nothing else: no
#      common name required, no DNS or IP SAN, no SAN taken from the CSR.
#      Every member is a TLS client and a TLS server of the others, hence
#      both extended key usages.
#   3. One Vault policy per slot: update on pki-consensus/sign/<slot> only.
#   4. jwt-consensus, a JWT auth mount trusting the Nomad servers' JWKS, and
#      one role per slot bound to the exact job and task that runs the slot
#      (nomad_namespace, nomad_job_id, nomad_task). A task can only ever
#      obtain a certificate for its own slot.
set -euo pipefail

: "${CLUSTER_ID:?the cluster id, 32 lowercase hex digits}"
: "${NOMAD_ADDR:?the Nomad API, whose /.well-known/jwks.json Vault reads}"
SLOTS="${SLOTS:-node-a node-b node-c}"
JOB="${JOB:-forgejo-raft}"
NOMAD_NAMESPACE="${NOMAD_NAMESPACE:-default}"
PKI_MOUNT="${PKI_MOUNT:-pki-consensus}"
JWT_MOUNT="${JWT_MOUNT:-jwt-consensus}"
CERT_TTL="${CERT_TTL:-720h}"
echo "$CLUSTER_ID" | grep -Eq '^[0-9a-f]{32}$' || { echo "CLUSTER_ID must be 32 lowercase hex digits" >&2; exit 1; }

echo "== 1. $PKI_MOUNT"
vault secrets list -format=json | grep -q "\"$PKI_MOUNT/\"" ||
  vault secrets enable -path="$PKI_MOUNT" -max-lease-ttl=87600h pki
if [ -n "${PKI_CA_BUNDLE:-}" ]; then
  vault write "$PKI_MOUNT/config/ca" pem_bundle=@"$PKI_CA_BUNDLE" >/dev/null
elif ! vault read "$PKI_MOUNT/cert/ca" >/dev/null 2>&1; then
  vault write -field=certificate "$PKI_MOUNT/root/generate/internal" \
    common_name="forgejo-raft consensus CA $CLUSTER_ID" key_type=ec key_bits=256 ttl=87600h >/dev/null
fi
vault read -field=certificate "$PKI_MOUNT/cert/ca" >ca.pem
echo "   the cluster CA is in ./ca.pem: install it as TLS_CA_FILE on every node"

for slot in $SLOTS; do
  uri="kitsunium-consensus://$CLUSTER_ID/slot/$slot"

  echo "== 2. PKI role $slot ($uri)"
  vault write "$PKI_MOUNT/roles/$slot" \
    allowed_uri_sans="$uri" \
    require_cn=false use_csr_common_name=false use_csr_sans=false \
    allow_any_name=false allow_bare_domains=false allow_subdomains=false \
    allow_localhost=false allow_ip_sans=false enforce_hostnames=false \
    allowed_domains="" allowed_other_sans="" \
    key_type=ec key_bits=256 \
    key_usage=DigitalSignature ext_key_usage=ServerAuth,ClientAuth \
    max_ttl="$CERT_TTL" not_before_duration=30s >/dev/null

  echo "== 3. policy $PKI_MOUNT-$slot"
  vault policy write "$PKI_MOUNT-$slot" - >/dev/null <<EOF
path "$PKI_MOUNT/sign/$slot" {
  capabilities = ["update"]
}
EOF
done

echo "== 4. $JWT_MOUNT, trusting $NOMAD_ADDR/.well-known/jwks.json"
vault auth list -format=json | grep -q "\"$JWT_MOUNT/\"" ||
  vault auth enable -path="$JWT_MOUNT" jwt
vault write "auth/$JWT_MOUNT/config" \
  jwks_url="$NOMAD_ADDR/.well-known/jwks.json" \
  jwt_supported_algs=RS256,EdDSA >/dev/null
for slot in $SLOTS; do
  vault write "auth/$JWT_MOUNT/role/$slot" - >/dev/null <<EOF
{
  "role_type": "jwt",
  "bound_audiences": ["$JWT_MOUNT"],
  "user_claim": "nomad_task",
  "bound_claims": {
    "nomad_namespace": "$NOMAD_NAMESPACE",
    "nomad_job_id": "$JOB",
    "nomad_task": "forgejo-$slot"
  },
  "token_policies": ["$PKI_MOUNT-$slot"],
  "token_ttl": "1h",
  "token_max_ttl": "1h",
  "token_type": "service"
}
EOF
  echo "   role $slot: $NOMAD_NAMESPACE/$JOB task forgejo-$slot -> $PKI_MOUNT-$slot"
done

cat <<EOF

Done. The FIRST certificate of each node is issued by hand, once, from a key
born on the node's own disk (the renewer takes over afterwards and keeps
that key):

  # on the node, in the host volume, as the user Forgejo runs as
  openssl genpkey -algorithm EC -pkeyopt ec_paramgen_curve:P-256 -out tls/node.key
  openssl req -new -key tls/node.key -subj /CN=<slot> -out tls/node.csr
  # from the operator's workstation
  vault write -field=certificate $PKI_MOUNT/sign/<slot> csr=@node.csr \\
    uri_sans=kitsunium-consensus://$CLUSTER_ID/slot/<slot> ttl=$CERT_TTL > node.pem
EOF
