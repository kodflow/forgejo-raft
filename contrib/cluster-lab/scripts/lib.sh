# Helpers shared by the lab scripts. Sourced, never run.
# shellcheck shell=bash

LAB_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
export COMPOSE_PROJECT_NAME="${COMPOSE_PROJECT_NAME:-forgejo-cluster-lab}"
export FORGEJO_DOCKERFILE="${FORGEJO_DOCKERFILE:-$LAB_DIR/.build/Dockerfile.forgejo}"

dc() { docker compose --project-directory "$LAB_DIR" -f "$LAB_DIR/compose.yaml" "$@"; }

# The lab's secrets (scripts/secrets.sh), for the scripts' own calls.
"$LAB_DIR/scripts/secrets.sh"
set -a
# shellcheck disable=SC1091
source "$LAB_DIR/.env"
set +a

say() { printf '\n== %s\n' "$*"; }
ok() { printf 'OK    %s\n' "$*"; }
fail() { printf 'FAIL  %s\n' "$*" >&2; exit 1; }

# wait_for <seconds> <description> <command...>: retries every second.
wait_for() {
  local t="$1" what="$2" i
  shift 2
  for ((i = 0; i < t; i++)); do
    if "$@" >/dev/null 2>&1; then
      ok "$what (${i}s)"
      return 0
    fi
    sleep 1
  done
  fail "$what: not true after ${t}s"
}

# patronictl through any running PostgreSQL node.
patronictl() {
  local n
  for n in pg-1 pg-2 pg-3; do
    if dc exec -T "$n" patronictl -c /etc/patroni/patroni.yml "$@" 2>/dev/null; then
      return 0
    fi
  done
  return 1
}

# The Patroni leader's member name, as the DCS sees it.
patroni_leader() {
  patronictl list -f json | python3 -c 'import json,sys; print(next(m["Member"] for m in json.load(sys.stdin) if m["Role"]=="Leader"))'
}

# Number of members streaming (sync or async standby) from the leader.
patroni_streaming() {
  patronictl list -f json | python3 -c 'import json,sys; print(sum(1 for m in json.load(sys.stdin) if m["Role"]!="Leader" and m["State"]=="streaming"))'
}

# The server HAProxy routes :5000 to (the one UP in the "primary" backend).
haproxy_primary() {
  curl -sf 'http://127.0.0.1:7000/;csv' | awk -F, '$1=="primary" && $2 ~ /^pg-/ && $18 ~ /^UP/ {print $2}'
}

# Runs SQL through HAProxy :5000 from any running PostgreSQL container.
via_haproxy() {
  local n
  for n in pg-1 pg-2 pg-3; do
    if dc exec -T -e PGPASSWORD="$PG_SUPERUSER_PASSWORD" "$n" \
      psql -h 10.89.0.30 -p 5000 -U postgres -d postgres -v ON_ERROR_STOP=1 -Atc "$1" 2>/dev/null; then
      return 0
    fi
  done
  return 1
}

forgejo_url() { echo "http://127.0.0.1:300${1}"; }
