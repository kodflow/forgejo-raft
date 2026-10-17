#!/usr/bin/env bash
# Chaos on a lab that is up and bootstrapped. Exits non-zero at the first
# failure.
#   1. partition a follower: pushes keep working, the follower catches up
#      once reconnected
#   2. partition the leader: the majority elects a new leader and accepts
#      pushes; the old leader cannot commit alone; it rejoins and catches up
#   3. stop a follower's container: same as 1, through a restart
set -euo pipefail
source "$(dirname "$0")/lib.sh"

USER_NAME=lab-admin
REPO=chaos-$(date +%s)
WORK="$(mktemp -d)"
NET="${COMPOSE_PROJECT_NAME}_lab"
trap 'rm -rf "$WORK"; "$LAB_DIR/scripts/chaos.sh" heal >/dev/null 2>&1 || true' EXIT

fj() { local n="$1"; shift; dc exec -T -u git "forgejo-$n" /usr/local/bin/gitea "$@"; }
url() { echo "http://$USER_NAME:$TOKEN@127.0.0.1:300$1/$USER_NAME/$REPO.git"; }
push() { # push <node> <branch>: prints the sha; explicit returns inside $(...)
  git -C "$WORK/repo" checkout -q -B "$2" || return 1
  if [ "$(git -C "$WORK/repo" log -1 --format=%s 2>/dev/null)" != "$2" ]; then
    echo "$2" >"$WORK/repo/$2.txt"
    git -C "$WORK/repo" add "$2.txt" || return 1
    git -C "$WORK/repo" -c user.name=lab -c user.email=lab@lab.invalid commit -qm "$2" || return 1
  fi
  git -C "$WORK/repo" push -q "$(url "$1")" "$2" 2>/dev/null || return 1
  git -C "$WORK/repo" rev-parse HEAD
}
push_retry() { local i; for ((i = 0; i < $1; i++)); do push "$2" "$3" && return 0; sleep 1; done; return 1; }
serves() { [ "$(git ls-remote "$(url "$1")" "refs/heads/$2" 2>/dev/null | cut -f1)" = "$3" ]; }
leader() {
  local n
  for n in 1 2 3; do
    if fj "$n" cluster status --json 2>/dev/null |
      python3 -c 'import json,sys; t=sys.stdin.read(); v=json.JSONDecoder().raw_decode(t[t.index("{"):])[0]; sys.exit(0 if v.get("Role")=="leader" else 1)'; then
      echo "$n"; return 0
    fi
  done
  return 1
}
others() { local n; for n in 1 2 3; do [ "$n" = "$1" ] || echo "$n"; done; }
cut_off() { docker network disconnect "$NET" "$(dc ps -q "forgejo-$1")"; ok "forgejo-$1 cut off the network"; }
reconnect() { docker network connect --ip "10.89.0.4$1" "$NET" "$(dc ps -q "forgejo-$1")"; ok "forgejo-$1 reconnected"; }

say "Setup"
TOKEN="$(fj 1 admin user generate-access-token --username "$USER_NAME" --token-name "$REPO" --scopes all --raw 2>/dev/null | tail -n 1)"
curl -sf -u "$USER_NAME:$TOKEN" -X POST -H 'Content-Type: application/json' \
  --data "{\"name\":\"$REPO\",\"auto_init\":false}" "http://127.0.0.1:3001/api/v1/user/repos" >/dev/null || fail "repo creation"
git init -q -b main "$WORK/repo"
sha="$(push 1 main)" || fail "first push"
ok "$USER_NAME/$REPO main at ${sha:0:12}"

say "1. Partition a follower"
l="$(leader)" || fail "no leader"
read -r f a <<<"$(others "$l" | tr "\n" " ")"
ok "leader forgejo-$l, partitioning follower forgejo-$f"
cut_off "$f"
sha="$(push_retry 30 "$l" p1)" || fail "push to the leader with a follower cut off"
ok "push to forgejo-$l accepted at ${sha:0:12}"
wait_for 10 "forgejo-$a serves p1" serves "$a" p1 "$sha"
reconnect "$f"
wait_for 60 "forgejo-$f caught up on p1" serves "$f" p1 "$sha"

say "2. Partition the leader"
l="$(leader)" || fail "no leader"
read -r x y <<<"$(others "$l" | tr "\n" " ")"
ok "partitioning leader forgejo-$l"
cut_off "$l"
wait_for 60 "the majority elects a new leader" bash -c "source '$LAB_DIR/scripts/lib.sh'; for n in $x $y; do dc exec -T -u git forgejo-\$n /usr/local/bin/gitea cluster status 2>/dev/null | grep -q 'role leader' && exit 0; done; exit 1"
sha="$(push_retry 60 "$x" p2)" || fail "push to the majority after the leader was cut off"
ok "push to forgejo-$x accepted at ${sha:0:12}"
wait_for 10 "forgejo-$y serves p2" serves "$y" p2 "$sha"
# The isolated old leader must not commit alone. Its HTTP port is still
# reachable from the host (only the lab network is cut).
if timeout_push="$(push "$l" p2-isolated 2>/dev/null)"; then
  fail "the isolated forgejo-$l accepted a write alone (${timeout_push:0:12})"
fi
ok "the isolated forgejo-$l refuses a write alone"
reconnect "$l"
wait_for 90 "forgejo-$l caught up on p2" serves "$l" p2 "$sha"
if serves "$l" p2-isolated "$(git -C "$WORK/repo" rev-parse p2-isolated)"; then
  fail "the refused write p2-isolated appeared on forgejo-$l"
fi
ok "the refused write never appeared"

say "3. Stop a follower's container"
l="$(leader)" || fail "no leader"
read -r f a <<<"$(others "$l" | tr "\n" " ")"
dc stop -t 5 "forgejo-$f" >/dev/null
ok "forgejo-$f stopped"
sha="$(push_retry 30 "$a" p3)" || fail "push to forgejo-$a with forgejo-$f stopped"
ok "push to forgejo-$a accepted at ${sha:0:12}"
dc start "forgejo-$f" >/dev/null
wait_for 120 "forgejo-$f healthy" curl -sf "$(forgejo_url "$f")/api/healthz"
wait_for 60 "forgejo-$f caught up on p3" serves "$f" p3 "$sha"

say "CHAOS PASSED ($USER_NAME/$REPO)"
