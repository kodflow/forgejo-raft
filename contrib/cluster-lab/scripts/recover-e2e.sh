#!/usr/bin/env bash
# The Recover fence, for real (kitsunium ADR 0152 D17, R8-20, R9-08 to
# R9-11), on a lab that is up, bootstrapped and running with the witnesses
# (make up bootstrap witnesses):
#   1. a repository and a pushed branch, replicated on the three nodes
#   2. the majority is lost: node-b and node-c stop, then node-a stops for Recover
#   3. the operator writes the fence on BOTH witnesses (supersede), fetches
#      a signed receipt per old voter from each, and recovers node-a alone into
#      a new cluster id; node-a's certificate is reissued over the same key
#   4. node-a starts (both witnesses say its new cluster is clear) and serves
#      the branch written before the loss, and takes a new push
#   5. node-b and node-c restart on their old data: refused at boot, exit 77,
#      never ready
#   6. the operator's override lets node-b start once, and the log says so
#   7. the override is withdrawn; node-b is refused again
# Exits non-zero at the first failure. The lab is left recovered: `make
# reset up bootstrap witnesses` to start over.
set -euo pipefail
source "$(dirname "$0")/lib.sh"

USER_NAME=lab-admin
USER_PASS="$LAB_ADMIN_PASSWORD"
REPO=recover-$(date +%s)
WORK="$(mktemp -d)"
trap 'rm -rf "$WORK"' EXIT

fj() { local n="$1"; shift; dc exec -T -u git "forgejo-$n" /usr/local/bin/gitea "$@"; }
# offline <service> <args...>: a one-off container of the stopped service,
# same volume and network, app.ini rendered as the server would (bootstrap.sh).
offline() {
  local svc="$1"
  shift
  dc run --rm --no-deps -T "$svc" bash -c \
    'cd /etc/s6/gitea && source ./setup >/dev/null && exec su-exec git /usr/local/bin/gitea cluster "$@"' \
    -- "$@"
}
# json <jq filter>: log lines may precede the JSON on stdout; keep from "{".
json() { sed -n '/^{/,$p' | jq -r "$1"; }
healthy() { [ "$(curl -s -o /dev/null -w '%{http_code}' "$(forgejo_url "$1")/api/healthz")" = 200 ]; }

grep -q '^WITNESS_KEYS=' "$LAB_DIR/.env" 2>/dev/null || fail "run make witnesses first"
KEYS="$(grep '^WITNESS_KEYS=' "$LAB_DIR/.env" | cut -d= -f2)"
KEY_A="${KEYS%%,*}" KEY_B="${KEYS##*,}"

say "1. A repository and a branch on the three nodes"
fj 1 admin user create --admin --username "$USER_NAME" --password "$USER_PASS" \
  --email "$USER_NAME@lab.invalid" --must-change-password=false >/dev/null 2>&1 || true
TOKEN="$(fj 1 admin user generate-access-token --username "$USER_NAME" --token-name "rec-$REPO" --scopes all --raw | tail -n 1)"
curl -sf -u "$USER_NAME:$TOKEN" -X POST -H 'Content-Type: application/json' \
  --data "{\"name\":\"$REPO\"}" "$(forgejo_url 1)/api/v1/user/repos" >/dev/null
git -C "$WORK" init -q -b main repo
echo before >"$WORK/repo/f.txt"
git -C "$WORK/repo" add f.txt
git -C "$WORK/repo" -c user.name=lab -c user.email=lab@lab.invalid commit -qm before
BEFORE="$(git -C "$WORK/repo" rev-parse HEAD)"
git -C "$WORK/repo" push -q "http://$USER_NAME:$TOKEN@127.0.0.1:3001/$USER_NAME/$REPO.git" main
for n in 1 2 3; do
  wait_for 20 "node $n serves main" bash -c "[ \"\$(git ls-remote http://$USER_NAME:$TOKEN@127.0.0.1:300$n/$USER_NAME/$REPO.git refs/heads/main | cut -f1)\" = $BEFORE ]"
done

STATUS="$(fj 1 cluster status --json)"
OLD="$(echo "$STATUS" | json '.Identity | split("/")[0]')"
member() { echo "$STATUS" | json ".Members[] | select(.Slot == \"$1\") | .$2"; }
NODE_A_ID="$(member node-a ID)" NODE_B_ID="$(member node-b ID)" NODE_C_ID="$(member node-c ID)"
NODE_A_SPKI="$(member node-a SPKI)" NODE_A_ADDR="$(member node-a Address)"
ok "cluster $OLD: node-a=$NODE_A_ID node-b=$NODE_B_ID node-c=$NODE_C_ID"

say "2. The majority is lost; node-a stops for Recover"
dc stop -t 5 forgejo-2 forgejo-3
dc stop -t 5 forgejo-1
NODE_A_INC="$(offline forgejo-1 inspect | json '.Incarnation')"
NEW="$(openssl rand -hex 16)"
ok "new cluster id $NEW"

say "3. The fence on both witnesses, the receipts, Recover"
for w in witness-a witness-b; do
  dc exec -T "$w" true || fail "$w is not running" # both are up: a missing one stops the run here
  dc exec -T -u git "$w" /app/gitea/gitea cluster witness supersede --dir /witness/state \
    --old "$OLD" --new "$NEW" --voters "$NODE_B_ID,$NODE_C_ID" --method stopped
  ok "$w recorded $OLD superseded by $NEW"
done
fenced=()
for v in "$NODE_B_ID" "$NODE_C_ID"; do
  offline forgejo-1 witness receipt --url http://10.89.0.51:7482 --key "$KEY_A" \
    --old "$OLD" --new "$NEW" --voter "$v" --out "/data/gitea/receipt-$v-a"
  offline forgejo-1 witness receipt --url http://10.89.0.52:7482 --key "$KEY_B" \
    --old "$OLD" --new "$NEW" --voter "$v" --out "/data/gitea/receipt-$v-b"
  fenced+=(--fenced "$v:stopped:/data/gitea/receipt-$v-a+/data/gitea/receipt-$v-b")
  ok "receipts for $v from both witnesses"
done
# Recover refuses with one receipt missing.
if echo "$NEW" | offline forgejo-1 recover --new-cluster "$NEW" \
  --members "$NODE_A_ID:$NODE_A_INC:node-a:$NODE_A_SPKI:$NODE_A_ADDR" \
  --fenced "$NODE_B_ID:stopped:/data/gitea/receipt-$NODE_B_ID-a" --fenced "$NODE_C_ID:stopped:/data/gitea/receipt-$NODE_C_ID-a" \
  >/dev/null 2>&1; then
  fail "recover accepted a fence with one witness's receipts only"
fi
ok "recover refuses receipts from one witness only"
echo "$NEW" | offline forgejo-1 recover --new-cluster "$NEW" \
  --members "$NODE_A_ID:$NODE_A_INC:node-a:$NODE_A_SPKI:$NODE_A_ADDR" "${fenced[@]}"
ok "node-a recovered into $NEW"
dc run --rm --no-deps -T -v "$LAB_DIR/pki/recert.sh:/lab/recert.sh:ro" \
  -v forgejo-cluster-lab_pki:/pki -v forgejo-cluster-lab_forgejo-1:/nodes/node-a \
  --user 0 --entrypoint /bin/sh pki /lab/recert.sh node-a "$NEW"

say "4. node-a serves, the old branch and a new push"
dc up -d --no-deps --no-build forgejo-1
wait_for 90 "node-a ready" healthy 1
dc logs --no-log-prefix forgejo-1 2>&1 | grep "says cluster $NEW is clear" >/dev/null || fail "node-a did not run the boot check"
[ "$(git ls-remote "http://$USER_NAME:$TOKEN@127.0.0.1:3001/$USER_NAME/$REPO.git" refs/heads/main | cut -f1)" = "$BEFORE" ] ||
  fail "node-a lost the branch written before the loss"
ok "node-a serves main at ${BEFORE:0:12}"
echo after >"$WORK/repo/f.txt"
git -C "$WORK/repo" -c user.name=lab -c user.email=lab@lab.invalid commit -qam after
git -C "$WORK/repo" push -q "http://$USER_NAME:$TOKEN@127.0.0.1:3001/$USER_NAME/$REPO.git" main
ok "node-a takes a push alone"

say "5. The old pair restarts on its old data: refused, exit 77"
since="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
sleep 1
dc up -d --no-deps --no-build forgejo-2 forgejo-3
for n in 2 3; do
  wait_for 60 "forgejo-$n refused by the witnesses, exit 77" bash -c \
    "docker compose --project-directory '$LAB_DIR' -f '$LAB_DIR/compose.yaml' logs --since $since --no-log-prefix forgejo-$n 2>&1 | grep -q 'refusing to start: this cluster was superseded.*exit 77'"
done
sleep 15
for n in 2 3; do
  healthy "$n" && fail "forgejo-$n is ready although its cluster was superseded"
  ok "forgejo-$n never ready"
done
fj 1 cluster status --json | json '.Members | length' | grep -x 1 >/dev/null || fail "the recovered cluster has members it should not"
ok "the recovered cluster holds node-a alone"

say "6. The operator's override, logged"
dc stop -t 5 forgejo-2
dc run -d --rm --no-deps --name forgejo-cluster-lab-node-b-override \
  -e FORGEJO_CLUSTER_WITNESS_OVERRIDE="$WITNESS_OVERRIDE_SECRET" forgejo-2 >/dev/null
wait_for 60 "the override is logged" bash -c \
  "docker logs forgejo-cluster-lab-node-b-override 2>&1 | grep -q 'FENCE OVERRIDDEN'"
docker rm -f forgejo-cluster-lab-node-b-override >/dev/null

say "7. Without the override, refused again"
since="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
sleep 1
dc up -d --no-deps --no-build forgejo-2
wait_for 60 "forgejo-2 refused again" bash -c \
  "docker compose --project-directory '$LAB_DIR' -f '$LAB_DIR/compose.yaml' logs --since $since --no-log-prefix forgejo-2 2>&1 | grep -q 'refusing to start: this cluster was superseded'"
dc stop -t 5 forgejo-2 forgejo-3

echo
echo "recover-e2e: the fence held: $OLD superseded by $NEW, node-a serves, node-b and node-c refused"
