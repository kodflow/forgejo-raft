#!/usr/bin/env bash
# Forgejo under Nomad, on a lab that is up, bootstrapped, with its witnesses
# (kitsunium ADR 0152 "Deployment on Nomad", D17, R8-09, R8-13, R9-09,
# R9-13). A Nomad dev agent stands for a node machine:
#   1. Vault: pki-consensus, and the JWT auth mount jwt-consensus reading the
#      Nomad agent's JWKS, one role per slot binding nomad_namespace,
#      nomad_job_id and nomad_task, user_claim nomad_task, a policy that only
#      signs for its slot
#   2. two data directories: node-a's, of a clear lineage, and node-c's, of a
#      lineage both witnesses record as superseded
#   3. the lab job (nomad/forgejo-lab.nomad.hcl): group node-a passes its
#      witness prestart, its task logs in to Vault with its workload
#      identity and renews a copy of node-a's certificate; node-a's JWT is
#      refused under node-b's role
#   4. group node-c: the prestart exits 77, the main task never starts, the
#      allocation fails and Nomad does not reschedule it
#   5. the JOIN drain check against this Nomad: a granted JOIN of slot "node-d"
#      (the agent's node meta) is refused while the node is ineligible,
#      admitted once eligible, refused again while it drains
# Leaves the lab's consensus untouched; the job is purged at the end.
set -euo pipefail
source "$(dirname "$0")/lib.sh"
source "$(dirname "$0")/vault-pki.sh"

NOMAD=http://127.0.0.1:4646
TMP="$(mktemp -d)"
trap 'rm -rf "$TMP"' EXIT
# Nomad runs with ACLs: every call carries the lab's management token, read
# by curl from a private file (never on a command line).
NH="$TMP/nomad.header"
(umask 077 && printf 'X-Nomad-Token: %s\n' "$NOMAD_TOKEN" >"$NH")
FORGEJO_SRC="${FORGEJO_SRC:-$(cd "$LAB_DIR/../../../forgejo-raft-cluster" && pwd)}"
nomad() { dc exec -T nomad nomad "$@"; }
in_nomad() { dc exec -T nomad sh -c "$1"; }
hex16() { od -An -N16 -tx1 /dev/urandom | tr -d ' \n'; }

grep -q '^WITNESS_KEYS=' "$LAB_DIR/.env" 2>/dev/null || fail "run make witnesses first"
URLS="$(grep '^WITNESS_URLS=' "$LAB_DIR/.env" | cut -d= -f2)"
KEYS="$(grep '^WITNESS_KEYS=' "$LAB_DIR/.env" | cut -d= -f2)"
CLUSTER="$(dc exec -T forgejo-1 cat /data/tls/cluster-id | tr -d '\r\n')"

say "1. Vault: pki-consensus and the jwt-consensus login, one role per slot"
dc up -d --wait --no-build vault nomad >/dev/null
vault_pki_setup
curl -sf -o /dev/null -H "$VH" -X POST "$VAULT/v1/sys/auth/jwt-consensus" -d '{"type":"jwt"}' || true
curl -sf -o /dev/null -H "$VH" -X POST "$VAULT/v1/auth/jwt-consensus/config" \
  -d '{"jwks_url":"http://10.89.0.70:4646/.well-known/jwks.json","jwt_supported_algs":["RS256","EdDSA"]}'
for slot in node-a node-b node-c; do
  jq -n --arg p "path \"pki-consensus/sign/$slot\" { capabilities = [\"update\"] }" '{policy: $p}' |
    curl -sf -o /dev/null -H "$VH" -X PUT "$VAULT/v1/sys/policies/acl/pki-consensus-$slot" -d @-
  curl -sf -o /dev/null -H "$VH" -X POST "$VAULT/v1/auth/jwt-consensus/role/$slot" -d "$(jq -n --arg slot "$slot" '{
    role_type: "jwt", bound_audiences: ["jwt-consensus"], user_claim: "nomad_task",
    bound_claims: {nomad_namespace: "default", nomad_job_id: "forgejo", nomad_task: ("forgejo-" + $slot)},
    token_policies: ["pki-consensus-" + $slot], token_ttl: "1h", token_max_ttl: "1h", token_type: "service"}')"
  ok "jwt-consensus role $slot: job forgejo, task forgejo-$slot, policy pki-consensus-$slot"
done

say "2. A clear data directory for node-a, a superseded one for node-c"
in_nomad 'rm -rf /lab/nomad/node-a /lab/nomad/node-c /lab/nomad/*.jwt'
CLEAR="$(hex16)" OLD="$(hex16)" NEW="$(hex16)"
init_dir() { # <slot> <cluster id> -> the node id
  in_nomad "set -e; d=/lab/nomad/$1; mkdir -p \$d/tls; cp /lab/node-a/tls/node.pem /lab/node-a/tls/node.key /lab/node-a/tls/ca.pem \$d/tls/
cat >\$d/app.ini <<EOF
WORK_PATH = \$d/w
[cluster]
ENABLED = true
DATA_DIR = \$d/consensus
SLOT = $1
LISTEN_ADDR = 127.0.0.1:7480
OBJECT_LISTEN_ADDR = 127.0.0.1:7481
TLS_CERT_FILE = \$d/tls/node.pem
TLS_KEY_FILE = \$d/tls/node.key
TLS_CA_FILE = \$d/tls/ca.pem
HOOK_SOCKET = \$d/h.sock
EOF
GITEA_I_AM_BEING_UNSAFE_RUNNING_AS_ROOT=true forgejo --config \$d/app.ini --work-path \$d/w cluster init --cluster $2" | tail -n 1 | cut -d: -f1
}
NODE_A_ID="$(init_dir node-a "$CLEAR")"
NODE_C_ID="$(init_dir node-c "$OLD")"
ok "node-a: node $NODE_A_ID of cluster $CLEAR"
ok "node-c: node $NODE_C_ID of cluster $OLD"
for w in witness-a witness-b; do
  dc exec -T -u git "$w" /app/gitea/gitea cluster witness supersede --dir /witness/state \
    --old "$OLD" --new "$NEW" --voters "$NODE_C_ID" --method stopped >/dev/null
  ok "$w records $OLD superseded by $NEW"
done

say "3. The job: node-a passes its prestart and renews through its workload identity"
NODE_ID="$(curl -sf -H @"$NH" "$NOMAD/v1/nodes" | jq -r '.[0].ID')"
nomad job run -detach -var "node_id=$NODE_ID" -var "cluster_id=$CLUSTER" -var "witnesses=$URLS" \
  -var "witness_keys=$KEYS" -var "vault_addr=http://10.89.0.60:8200" - <"$LAB_DIR/nomad/forgejo-lab.nomad.hcl" >/dev/null
alloc_of() { curl -sf -H @"$NH" "$NOMAD/v1/job/forgejo/allocations" | jq -r --arg g "$1" '[.[] | select(.TaskGroup == $g)] | sort_by(.CreateIndex) | last | .ID'; }
task_state() { curl -sf -H @"$NH" "$NOMAD/v1/allocation/$1" | jq -r --arg t "$2" '.TaskStates[$t].State // "none"'; }
wait_for 90 "node-a allocated" bash -c "[ -n \"\$(curl -sf -H @$NH $NOMAD/v1/job/forgejo/allocations | jq -r '.[] | select(.TaskGroup==\"node-a\") | .ID')\" ]"
ALLOC_A="$(alloc_of node-a)"
wait_for 90 "node-a's main task forgejo-node-a running" bash -c "[ \"\$(curl -sf -H @$NH $NOMAD/v1/allocation/$ALLOC_A | jq -r '.TaskStates[\"forgejo-node-a\"].State // \"\"')\" = running ]"
wait_for 60 "the renewal reported" bash -c "curl -sf -H @$NH '$NOMAD/v1/client/fs/logs/$ALLOC_A?task=forgejo-node-a&type=stdout&plain=true&origin=start' | grep -q RENEW-"
logs="$(curl -sf -H @"$NH" "$NOMAD/v1/client/fs/logs/$ALLOC_A?task=forgejo-node-a&type=stdout&plain=true&origin=start")"
echo "$logs" | grep RENEW- | sed 's/^/      /'
echo "$logs" | grep -q '^RENEW-OK renewed: serial' || fail "the renewal through the workload identity failed"
ok "witness-check of node-a exited $(curl -sf -H @"$NH" "$NOMAD/v1/allocation/$ALLOC_A" | jq -r '.TaskStates["witness-check"].Events[] | select(.Type=="Terminated") | .ExitCode' | tail -n 1)"
ok "Vault login with node-a's workload identity, certificate signed under pki-consensus/sign/node-a"
JWT="$(in_nomad 'cat /lab/nomad/node-a.jwt')"
claims="$(echo "$JWT" | cut -d. -f2 | tr '_-' '/+' | { b="$(cat)"; while [ $(( ${#b} % 4 )) -ne 0 ]; do b="$b="; done; echo "$b"; } | base64 -d 2>/dev/null)"
echo "      claims: $(echo "$claims" | jq -c '{aud, nomad_namespace, nomad_job_id, nomad_task}')"
code="$(jq -n --arg jwt "$JWT" '{role: "node-b", jwt: $jwt}' | curl -s -o /dev/null -w '%{http_code}' -X POST "$VAULT/v1/auth/jwt-consensus/login" -d @-)"
[ "$code" = 400 ] || fail "node-a's identity logged in under node-b's role (HTTP $code)"
ok "node-a's identity refused under node-b's role (HTTP $code)"
tok="$(jq -n --arg jwt "$JWT" '{role: "node-a", jwt: $jwt}' | curl -sf -X POST "$VAULT/v1/auth/jwt-consensus/login" -d @- | jq -r .auth.client_token)"
code="$(curl -s -o /dev/null -w '%{http_code}' -H "X-Vault-Token: $tok" -X POST "$VAULT/v1/pki-consensus/sign/node-b" -d '{"csr":"x"}')"
[ "$code" = 403 ] || fail "node-a's Vault token reached pki-consensus/sign/node-b (HTTP $code)"
ok "node-a's Vault token cannot sign for node-b (HTTP $code)"

say "4. node-c: the witness prestart exits 77, Forgejo never starts, no reschedule"
ALLOC_C="$(alloc_of node-c)"
wait_for 90 "node-c's allocation failed" bash -c "[ \"\$(curl -sf -H @$NH $NOMAD/v1/allocation/$ALLOC_C | jq -r .ClientStatus)\" = failed ]"
exit_code="$(curl -sf -H @"$NH" "$NOMAD/v1/allocation/$ALLOC_C" | jq -r '.TaskStates["witness-check"].Events[] | select(.Type=="Terminated") | .ExitCode' | tail -n 1)"
[ "$exit_code" = 77 ] || fail "witness-check exited $exit_code, not 77"
ok "witness-check exited 77"
curl -sf -H @"$NH" "$NOMAD/v1/client/fs/logs/$ALLOC_C?task=witness-check&type=stderr&plain=true&origin=start" | grep -E 'fenced|superseded' | head -n 2 | sed 's/^/      /'
started="$(curl -sf -H @"$NH" "$NOMAD/v1/allocation/$ALLOC_C" | jq -r '[.TaskStates["forgejo-node-c"].Events[]? | select(.Type=="Started")] | length')"
[ "$started" = 0 ] || fail "forgejo-node-c started $started times"
ok "forgejo-node-c never started"
sleep 5
n="$(curl -sf -H @"$NH" "$NOMAD/v1/job/forgejo/allocations" | jq '[.[] | select(.TaskGroup=="node-c")] | length')"
[ "$n" = 1 ] || fail "node-c was rescheduled ($n allocations)"
ok "Nomad did not reschedule node-c (reschedule attempts = 0)"
[ "$(task_state "$ALLOC_A" forgejo-node-a)" = running ] || fail "node-a stopped"
ok "node-a still running"
nomad job stop -purge -detach forgejo >/dev/null

say "5. The JOIN drain check against this Nomad (slot node-d, its node's meta)"
OUT="$TMP/out"
(umask 077 && printf '%s\n' "$NOMAD_TOKEN" >"$TMP/nomad.token")
# Compiled once: a drain stays open only while the sleeper's kill_timeout runs.
(cd "$FORGEJO_SRC" && go test -c -tags 'sqlite sqlite_unlock_notify' -o "$TMP/cluster.test" ./services/cluster/)
join_test() { # <refused|admitted> [reason]: the SDK's real JOIN, gated by this Nomad
  (cd "$FORGEJO_SRC/services/cluster" && NOMAD_ADDR="$NOMAD" NOMAD_TOKEN_FILE="$TMP/nomad.token" NOMAD_E2E_SLOT=node-d NOMAD_E2E_EXPECT="$1" NOMAD_E2E_REASON="${2:-}" \
    "$TMP/cluster.test" -test.timeout 5m -test.count 1 -test.run '^TestSelfJoinRefusedWhileTheNomadNodeDrains$' -test.v) >"$OUT" 2>&1 || true
  grep -E 'JOIN of|^--- ' "$OUT" | sed 's/^ *//; s/^/      /'
  grep -q -- '^--- PASS' "$OUT"
}
nomad node eligibility -disable -self >/dev/null
join_test refused ineligible || fail "a JOIN on an ineligible node was not refused"
ok "granted JOIN refused: the Nomad node is ineligible"
nomad node eligibility -enable -self >/dev/null
join_test admitted || fail "a JOIN on an eligible node was not admitted"
ok "granted JOIN admitted as a learner: the node is eligible"
# A drain lasts while an allocation is stopping: a sleeper that ignores the
# stop signal holds it open for its kill_timeout.
nomad job run -detach - >/dev/null <<'JOB'
job "sleeper" {
  datacenters = ["*"]
  group "s" {
    task "s" {
      driver       = "raw_exec"
      kill_timeout = "2m"
      config {
        command = "/bin/sh"
        args    = ["-c", "trap '' INT TERM; while :; do sleep 1; done"]
      }
      resources {
        cpu    = 20
        memory = 32
      }
    }
  }
}
JOB
wait_for 60 "the sleeper runs" bash -c "curl -sf -H @$NH $NOMAD/v1/job/sleeper/allocations | jq -e 'any(.[]; .ClientStatus == \"running\")'"
nomad node drain -enable -self -yes -detach -deadline 1h >/dev/null
wait_for 30 "the node drains" bash -c "curl -sf -H @$NH $NOMAD/v1/node/$NODE_ID | jq -e '.DrainStrategy != null'"
join_test refused draining || fail "a JOIN on a draining node was not refused"
ok "granted JOIN refused: the Nomad node is draining"
nomad node drain -disable -self -yes >/dev/null
nomad node eligibility -enable -self >/dev/null
nomad job stop -purge -detach sleeper >/dev/null

echo
echo "nomad-e2e: Vault login by workload identity, witness prestart exit 77, JOIN refused on an ineligible or draining node"
