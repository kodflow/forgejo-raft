# Helpers of the CI end-to-end suite (.github/workflows/e2e.yml). Sourced,
# never run. Unlike the lab's own scripts, a suite does not stop at the
# first failure: every checkpoint prints `CHECK <name>: PASS|FAIL`, and
# `finish` exits non-zero when any failed.
# shellcheck shell=bash

source "$(dirname "${BASH_SOURCE[0]}")/../lib.sh"

CHECKS_FAILED=0
# Every CHECK line is also appended here, for the job summary.
CHECK_LOG="${CHECK_LOG:-$LAB_DIR/.build/checks.txt}"
mkdir -p "$(dirname "$CHECK_LOG")"

check_line() { # check_line <name> <PASS|FAIL> [detail]
  local line="CHECK $1: $2${3:+ ($3)}"
  echo "$line"
  echo "$line" >>"$CHECK_LOG"
  [ "$2" = PASS ] || CHECKS_FAILED=$((CHECKS_FAILED + 1))
}
pass() { check_line "$1" PASS "${2:-}"; }
flunk() { check_line "$1" FAIL "${2:-}"; }
check() { # check <name> <command...>
  local name="$1"
  shift
  if "$@"; then pass "$name"; else flunk "$name"; fi
}
finish() {
  say "$CHECKS_FAILED checkpoint(s) failed"
  [ "$CHECKS_FAILED" = 0 ]
}

# shellcheck disable=SC2034 # POLL_TOOK is read by the suites.
# poll <seconds> <command...>: every half second until it holds; POLL_TOOK
# is the time it took, in seconds.
poll() {
  local t="$1" start now
  shift
  start=$(date +%s.%N)
  while :; do
    if "$@" >/dev/null 2>&1; then
      now=$(date +%s.%N)
      POLL_TOOK=$(printf '%.1f' "$(echo "$now - $start" | bc)")
      return 0
    fi
    now=$(date +%s.%N)
    if [ "$(echo "$now - $start >= $t" | bc)" = 1 ]; then
      POLL_TOOK="$t"
      return 1
    fi
    sleep 0.5
  done
}

node_url() { echo "http://127.0.0.1:300$1"; }
fj() { local n="$1"; shift; dc exec -T -u git "forgejo-$n" /usr/local/bin/gitea "$@"; }

# status <node>: `forgejo cluster status --json` through the node's private
# socket; log lines may precede the JSON.
status() {
  fj "$1" cluster status --json 2>/dev/null | python3 -c 'import json,sys; t=sys.stdin.read(); print(json.dumps(json.JSONDecoder().raw_decode(t[t.index("{"):])[0]))'
}
# field <node> <jq filter>
field() { status "$1" | jq -r "$2"; }
healthy() { curl -sf -m 5 "$(node_url "$1")/api/healthz" >/dev/null; }

# The nodes whose own status says leader, one per line.
leaders() {
  local n
  for n in 1 2 3; do
    [ "$(field "$n" .Role 2>/dev/null)" = leader ] && echo "$n"
  done
  return 0
}
leader() { local l; l="$(leaders)"; [ "$(echo "$l" | grep -c .)" = 1 ] && echo "$l"; }
others() { local n; for n in 1 2 3; do [ "$n" = "$1" ] || echo "$n"; done; }

# The suite's accounts. The passwords come from .env (scripts/secrets.sh).
ADMIN=e2e-admin
ensure_admin() {
  fj 1 admin user create --admin --username "$ADMIN" --password "$E2E_ADMIN_PASSWORD" \
    --email "$ADMIN@lab.invalid" --must-change-password=false >/dev/null 2>&1 || true
  TOKEN="$(fj 1 admin user generate-access-token --username "$ADMIN" \
    --token-name "e2e-$(date +%s%N)" --scopes all --raw 2>/dev/null | tail -n 1)"
  [ -n "$TOKEN" ]
}

# api <node> <method> <path> [json]: as the admin, body on stdout, fails on
# an HTTP error.
api() {
  curl -sS -f -m 30 -H "Authorization: token $TOKEN" -X "$2" -H 'Content-Type: application/json' \
    ${4:+--data "$4"} "$(node_url "$1")/api/v1$3"
}
# api_code <node> <method> <path> [json]: the HTTP status only.
api_code() {
  curl -s -o /dev/null -w '%{http_code}' -m 30 -H "Authorization: token $TOKEN" -X "$2" \
    -H 'Content-Type: application/json' ${4:+--data "$4"} "$(node_url "$1")/api/v1$3"
}

# remote <node> <owner/repo>: the HTTP remote with the admin's token.
remote() { echo "http://$ADMIN:$TOKEN@127.0.0.1:300$1/$2.git"; }
# ref_sha <node> <owner/repo> <ref>: what the node serves over git.
ref_sha() { git ls-remote "$(remote "$1" "$2")" "$3" 2>/dev/null | awk -v r="$3" '$2==r {print $1}'; }
serves() { [ "$(ref_sha "$1" "$2" "$3")" = "$4" ]; }
# all_refs <node> <owner/repo>: every ref and its sha, sorted.
all_refs() { git ls-remote "$(remote "$1" "$2")" 2>/dev/null | sort; }

# commit_to <dir> <branch>: a new commit on a branch of a work tree, its sha
# on stdout.
commit_to() {
  local dir="$1" b="$2"
  git -C "$dir" checkout -q -B "$b" 2>/dev/null || git -C "$dir" checkout -q --orphan "$b" || return 1
  echo "$b $(date +%s%N)" >"$dir/$b.txt"
  git -C "$dir" add "$b.txt" || return 1
  git -C "$dir" -c user.name=e2e -c user.email=e2e@lab.invalid commit -qm "$b" || return 1
  git -C "$dir" rev-parse HEAD
}
