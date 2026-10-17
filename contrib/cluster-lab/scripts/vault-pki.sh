# The lab's Vault: a pki-consensus mount importing the lab CA, one role per
# slot whose allowed_uri_sans is the slot's exact URI SAN, require_cn=false,
# EC keys (kitsunium ADR 0152 D17, D20). Sourced after lib.sh; idempotent.
# shellcheck shell=bash

VAULT=http://127.0.0.1:8200
VAULT_TOKEN_LAB="$VAULT_DEV_TOKEN"
VH="X-Vault-Token: $VAULT_TOKEN_LAB"

vault_pki_setup() {
  local work cluster slot
  work="$(mktemp -d)"
  dc up -d --wait --no-build vault >/dev/null
  cluster="$(dc exec -T forgejo-1 cat /data/tls/cluster-id | tr -d '\r\n')"
  docker run --rm --user 0 -v "${COMPOSE_PROJECT_NAME}_pki:/pki:ro" -v "$work:/out" --entrypoint sh \
    forgejo-cluster-lab/patroni:4.1.5-pg18 -c 'cat /pki/ca.key /pki/ca.pem > /out/bundle.pem'
  curl -sf -o /dev/null -H "$VH" -X POST "$VAULT/v1/sys/mounts/pki-consensus" -d '{"type":"pki","config":{"max_lease_ttl":"720h"}}' || true
  jq -Rs '{pem_bundle: .}' "$work/bundle.pem" | curl -sf -o /dev/null -H "$VH" -X POST "$VAULT/v1/pki-consensus/config/ca" -d @-
  rm -rf "$work"
  for slot in node-a node-b node-c; do
    curl -sf -o /dev/null -H "$VH" -X POST "$VAULT/v1/pki-consensus/roles/$slot" -d "$(jq -n --arg uri "kitsunium-consensus://$cluster/slot/$slot" '{
      allowed_uri_sans: $uri, require_cn: false, allow_any_name: false, enforce_hostnames: false,
      allow_ip_sans: false, allow_localhost: false, key_type: "ec", key_bits: 256,
      key_usage: ["DigitalSignature"], ext_key_usage: ["ServerAuth", "ClientAuth"],
      max_ttl: "720h", not_before_duration: "1s", use_csr_common_name: false, use_csr_sans: false}')"
    ok "pki role $slot: kitsunium-consensus://$cluster/slot/$slot"
  done
}
