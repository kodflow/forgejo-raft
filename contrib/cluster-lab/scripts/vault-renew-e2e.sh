#!/usr/bin/env bash
# The in-process certificate renewer against a real Vault (kitsunium ADR
# 0152 D17, R7-25, R8-13), on a lab that is up and bootstrapped:
#   1. Vault in dev mode: a pki-consensus mount importing the lab CA, one
#      role per slot whose allowed_uri_sans is the slot's exact URI SAN,
#      require_cn=false, EC keys
#   2. the nodes recreated with [cluster] RENEW_VAULT_ADDR and a token file
#   3. on each node, `forgejo cluster cert renew`: a CSR from the node's key,
#      signed by Vault; the certificate changes, the SPKI and the URI SAN do
#      not, and the issuer is the lab CA
#   4. Vault refuses another slot's URI SAN under a slot's role
#   5. every node restarted in turn, so each handshake uses a renewed
#      certificate: every node is ready again and the cluster has a leader
set -euo pipefail
source "$(dirname "$0")/lib.sh"

source "$(dirname "$0")/vault-pki.sh"
TOKEN="$VAULT_TOKEN_LAB"
H="$VH"
WORK="$(mktemp -d)"
trap 'rm -rf "$WORK"' EXIT

say "1. Vault, the pki-consensus mount and one role per slot"
vault_pki_setup
CLUSTER="$(dc exec -T forgejo-1 cat /data/tls/cluster-id | tr -d '\r\n')"

say "2. The nodes with the renewer"
env_file="$LAB_DIR/.env"
# The copy holds every lab secret: private, like .env.
umask 077
touch "$env_file"
grep -vE '^(RENEW_VAULT_ADDR|RENEW_VAULT_TOKEN_FILE)=' "$env_file" >"$env_file.tmp" || true
{ cat "$env_file.tmp"; echo "RENEW_VAULT_ADDR=http://10.89.0.60:8200"; echo "RENEW_VAULT_TOKEN_FILE=/data/gitea/vault-token"; } >"$env_file"
rm -f "$env_file.tmp"
dc up -d --wait --no-deps --no-build forgejo-1 forgejo-2 forgejo-3
for n in 1 2 3; do
  # On stdin: the token never shows in a process list.
  printf '%s\n' "$TOKEN" | dc exec -T -u git "forgejo-$n" sh -c 'umask 077 && cat > /data/gitea/vault-token'
done

# The Forgejo image has no openssl: the certificate is read on the host.
node_cert() { dc exec -T "forgejo-$1" cat /data/tls/node.pem | tr -d '\r'; }
cert_field() { node_cert "$1" | openssl x509 -noout $2; }
spki_of() { node_cert "$1" | openssl x509 -pubkey -noout | openssl pkey -pubin -outform DER | openssl dgst -sha256 -r | cut -d' ' -f1; }

say "3. cluster cert renew on each node"
for n in 1 2 3; do
  slot=$(case $n in 1) echo node-a ;; 2) echo node-b ;; 3) echo node-c ;; esac)
  serial_before="$(cert_field "$n" -serial)" spki_before="$(spki_of "$n")"
  dc exec -T -u git "forgejo-$n" /usr/local/bin/gitea cluster cert renew | tail -n 1
  serial_after="$(cert_field "$n" -serial)" spki_after="$(spki_of "$n")"
  [ "$serial_before" != "$serial_after" ] || fail "forgejo-$n: the certificate did not change"
  [ "$spki_before" = "$spki_after" ] || fail "forgejo-$n: the key changed"
  san="$(cert_field "$n" '-ext subjectAltName' | tail -n +2 | tr -d ' ')"
  [ "$san" = "URI:kitsunium-consensus://$CLUSTER/slot/$slot" ] || fail "forgejo-$n: SAN is $san"
  ok "forgejo-$n renewed ($serial_after), same SPKI ${spki_after:0:16}, SAN unchanged"
done

say "4. Vault refuses another slot's URI SAN"
openssl req -new -newkey ec -pkeyopt ec_paramgen_curve:P-256 -nodes -keyout "$WORK/k" -subj /CN=x -out "$WORK/csr" 2>/dev/null
code="$(jq -n --rawfile csr "$WORK/csr" --arg uri "kitsunium-consensus://$CLUSTER/slot/node-b" '{csr: $csr, uri_sans: $uri}' |
  curl -s -o /dev/null -w '%{http_code}' -H "$H" -X POST "$VAULT/v1/pki-consensus/sign/node-a" -d @-)"
[ "$code" = 400 ] || fail "Vault signed node-b's SAN under node-a's role (HTTP $code)"
ok "node-a's role refuses node-b's SAN (HTTP 400)"

say "5. Each node restarted in turn on its renewed certificate"
for n in 1 2 3; do
  dc restart -t 5 "forgejo-$n" >/dev/null
  wait_for 90 "forgejo-$n ready again" bash -c "[ \"\$(curl -s -o /dev/null -w '%{http_code}' $(forgejo_url "$n")/api/healthz)\" = 200 ]"
done
for n in 1 2 3; do
  dc exec -T -u git "forgejo-$n" /usr/local/bin/gitea cluster status 2>/dev/null | head -n 1
done
echo
echo "vault-renew-e2e: three nodes renewed through Vault, same keys, cluster healthy"
