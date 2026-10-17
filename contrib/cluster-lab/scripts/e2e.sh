#!/usr/bin/env bash
# End to end, on a lab that is up and bootstrapped (make up bootstrap):
#   1. a user and a repository through node 1
#   2. an HTTP push to node 2, the branch read at once on node 3
#   3. stop the Raft leader, push again, read on another node
#   4. stop the Patroni primary, Forgejo recovers, push and read again
#   5. restore every node, the restored ones catch up
# Exits non-zero at the first failure.
set -euo pipefail
source "$(dirname "$0")/lib.sh"

USER_NAME=lab-admin
USER_PASS="$LAB_ADMIN_PASSWORD"
REPO=e2e-$(date +%s)
WORK="$(mktemp -d)"
trap 'rm -rf "$WORK"' EXIT

fj() { local n="$1"; shift; dc exec -T -u git "forgejo-$n" /usr/local/bin/gitea "$@"; }
api() { # api <node> <method> <path> [json]
  curl -sf -u "$USER_NAME:$TOKEN" -X "$2" -H 'Content-Type: application/json' \
    ${4:+--data "$4"} "$(forgejo_url "$1")/api/v1$3"
}
branch_sha() { # branch_sha <node> <branch>: what the node serves, over git
  git ls-remote "http://$USER_NAME:$TOKEN@127.0.0.1:300$1/$USER_NAME/$REPO.git" "refs/heads/$2" 2>/dev/null | cut -f1
}
push() { # push <node> <branch>: commit and push a new branch, prints its sha
  local n="$1" b="$2"
  # Explicit returns: errexit does not reach inside $(...).
  git -C "$WORK/repo" checkout -q -B "$b" || return 1
  if [ "$(git -C "$WORK/repo" log -1 --format=%s 2>/dev/null)" != "$b" ]; then
    echo "$b $(date +%s%N)" >"$WORK/repo/$b.txt"
    git -C "$WORK/repo" add "$b.txt" || return 1
    git -C "$WORK/repo" -c user.name=lab -c user.email=lab@lab.invalid commit -qm "$b" || return 1
  fi
  git -C "$WORK/repo" push -q "http://$USER_NAME:$TOKEN@127.0.0.1:300$n/$USER_NAME/$REPO.git" "$b" >&2 || return 1
  git -C "$WORK/repo" rev-parse HEAD
}
push_retry() { # push_retry <seconds> <node> <branch>: until Forgejo accepts it
  local t="$1" i
  for ((i = 0; i < t; i++)); do
    if sha="$(push "$2" "$3" 2>/dev/null)"; then echo "$sha"; return 0; fi
    sleep 1
  done
  return 1
}
expect_branch() { # expect_branch <seconds> <node> <branch> <sha>
  wait_for "$1" "node $2 serves $3 at ${4:0:12}" bash -c "[ \"\$(git ls-remote 'http://$USER_NAME:$TOKEN@127.0.0.1:300$2/$USER_NAME/$REPO.git' 'refs/heads/$3' | cut -f1)\" = '$4' ]"
}
# The node holding the consensus leadership: the one whose own
# `cluster status --json` (private socket, server running) says Role leader.
# Log lines may precede the JSON on stdout, hence the scan for "{".
raft_leader_node() {
  local n
  for n in 1 2 3; do
    if fj "$n" cluster status --json 2>/dev/null |
      python3 -c 'import json,sys; t=sys.stdin.read(); v=json.JSONDecoder().raw_decode(t[t.index("{"):])[0]; sys.exit(0 if str(v.get("Role","")).lower()=="leader" else 1)'; then
      echo "$n"
      return 0
    fi
  done
  return 1
}

say "0. The lab is up"
for n in 1 2 3; do
  wait_for 120 "forgejo-$n healthy" curl -sf "$(forgejo_url "$n")/api/healthz"
done

say "1. User and repository through node 1"
fj 1 admin user create --admin --username "$USER_NAME" --password "$USER_PASS" \
  --email "$USER_NAME@lab.invalid" --must-change-password=false >/dev/null 2>&1 || true
TOKEN="$(fj 1 admin user generate-access-token --username "$USER_NAME" --token-name "e2e-$REPO" \
  --scopes all --raw | tail -n 1)"
[ -n "$TOKEN" ] || fail "no access token"
# AUTO_INIT=1 lets Forgejo write the first commit itself (its own push from
# a temporary clone); by default the repository is empty and the first
# commit is an ordinary HTTP push to node 1.
auto_init=false
[ -z "${AUTO_INIT:-}" ] || auto_init=true
api 1 POST /user/repos "{\"name\":\"$REPO\",\"auto_init\":$auto_init,\"default_branch\":\"main\"}" >/dev/null ||
  fail "repository creation on node 1"
ok "repository $USER_NAME/$REPO created on node 1 (auto_init $auto_init)"
for n in 2 3; do
  wait_for 10 "node $n sees the repository" api "$n" GET "/repos/$USER_NAME/$REPO"
done
if [ -n "${AUTO_INIT:-}" ]; then
  git clone -q "http://$USER_NAME:$TOKEN@127.0.0.1:3001/$USER_NAME/$REPO.git" "$WORK/repo" ||
    fail "clone from node 1"
  sha="$(git -C "$WORK/repo" rev-parse HEAD)"
  ok "Forgejo's own first commit on node 1 at ${sha:0:12}"
  for n in 2 3; do expect_branch 10 "$n" main "$sha"; done
else
  git init -q -b main "$WORK/repo"
  sha="$(push 1 main)" || fail "first push to node 1"
  ok "first push to node 1 at ${sha:0:12}"
  for n in 2 3; do expect_branch 10 "$n" main "$sha"; done
fi

say "2. Push to node 2, read on node 3 at once"
sha="$(push 2 e2e-push-node2)" || fail "push to node 2"
got="$(branch_sha 3 e2e-push-node2)"
[ "$got" = "$sha" ] || fail "node 3 serves '${got:-nothing}' for e2e-push-node2, expected $sha (read-after-write)"
ok "node 3 serves e2e-push-node2 at ${sha:0:12} right after the push"

say "3. Stop the Raft leader, push again"
leader="$(raft_leader_node)" || fail "cannot tell the Raft leader (forgejo cluster status)"
ok "Raft leader is forgejo-$leader"
dc kill -s KILL "forgejo-$leader" >/dev/null
ok "forgejo-$leader killed (SIGKILL)"
survivors=()
for n in 1 2 3; do [ "$n" = "$leader" ] || survivors+=("$n"); done
# Timed from the leader's stop: one push, no retry, must land well under the
# hook's 60 s budget (objects return at majority, the proposal retries
# through the election).
t0=$(date +%s.%N)
sha="$(push "${survivors[0]}" e2e-after-leader)" || fail "push to forgejo-${survivors[0]} right after the leader stopped"
took="$(echo "$(date +%s.%N) - $t0" | bc)"
ok "push to forgejo-${survivors[0]} accepted at ${sha:0:12}, ${took}s after the leader stopped, first try"
[ "$(echo "$took < ${MAX_PUSH_AFTER_LEADER_STOP:-15}" | bc)" = 1 ] || fail "push took ${took}s (limit ${MAX_PUSH_AFTER_LEADER_STOP:-15}s)"
expect_branch 10 "${survivors[1]}" e2e-after-leader "$sha"
stopped_forgejo="$leader"

say "4. Stop the Patroni primary, Forgejo recovers"
pg="$(patroni_leader)"
dc stop -t 5 "$pg" >/dev/null
ok "$pg (Patroni leader) stopped"
wait_for 60 "a new Patroni leader" bash -c "source '$LAB_DIR/scripts/lib.sh'; l=\$(patroni_leader) && [ \"\$l\" != '$pg' ]"
wait_for 60 "forgejo-${survivors[0]} answers the API again" api "${survivors[0]}" GET "/repos/$USER_NAME/$REPO"
sha="$(push_retry 90 "${survivors[0]}" e2e-after-pg)" || fail "push after the PostgreSQL failover"
ok "push after the PostgreSQL failover at ${sha:0:12}"
expect_branch 10 "${survivors[1]}" e2e-after-pg "$sha"

say "5. Restore, the restored nodes catch up"
dc start "forgejo-$stopped_forgejo" "$pg" >/dev/null
wait_for 120 "forgejo-$stopped_forgejo healthy" curl -sf "$(forgejo_url "$stopped_forgejo")/api/healthz"
expect_branch 60 "$stopped_forgejo" e2e-after-leader "$(branch_sha "${survivors[1]}" e2e-after-leader)"
expect_branch 60 "$stopped_forgejo" e2e-after-pg "$sha"
wait_for 120 "$pg streaming again" bash -c "source '$LAB_DIR/scripts/lib.sh'; [ \"\$(patroni_streaming)\" = 2 ]"

say "E2E PASSED ($USER_NAME/$REPO)"
