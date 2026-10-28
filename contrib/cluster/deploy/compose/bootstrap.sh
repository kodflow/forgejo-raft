#!/usr/bin/env bash
# Founds the three-node cluster of compose.yaml. Run it once, from anywhere;
# a rerun on a founded cluster only starts it.
#
#   1. .env: random secrets and a cluster id, written once, never replaced
#   2. PostgreSQL up, the PKI (one key per node, born in its volume), the
#      shared volume of the stored files handed to the git user
#   3. `forgejo cluster init` on each node, offline, once: prints the node's
#      member line, id:incarnation:slot:spki:host:port
#   4. `forgejo cluster bootstrap --members a,b,c` on each node, offline,
#      with the IDENTICAL list everywhere, unless already founded
#   5. the three servers started; a leader is elected within seconds
#
# The offline commands run in one-off containers of each service: same
# volume, same configuration, the server itself stopped.
set -euo pipefail
cd "$(dirname "$0")"
dc() { docker compose "$@"; }
NODES=(node-a node-b node-c)

umask 077
touch .env
add() { grep -q "^$1=" .env || echo "$1=$2" >>.env; }
add POSTGRES_PASSWORD "$(openssl rand -hex 24)"
add FORGEJO_SECRET_KEY "$(openssl rand -hex 32)"
# [oauth2] JWT_SECRET and [server] LFS_JWT_SECRET, shared by the nodes: 32
# bytes in unpadded base64url.
add FORGEJO_JWT_SECRET "$(openssl rand 32 | base64 | tr '+/' '-_' | tr -d '=\n')"
add FORGEJO_LFS_JWT_SECRET "$(openssl rand 32 | base64 | tr '+/' '-_' | tr -d '=\n')"
add CLUSTER_ID "$(openssl rand -hex 16)"

# The image's own setup renders app.ini from FORGEJO__* as the server
# would, then the cluster command runs as the git user. "member-line" runs
# `cluster init` the first time, keeps the line in the volume, prints it.
offline() {
  local svc="$1"
  shift
  # shellcheck disable=SC2016 # expanded inside the container
  dc run --rm --no-deps -T "$svc" bash -c '
    cd /etc/s6/gitea && source ./setup >/dev/null
    fj() { su-exec git /usr/local/bin/gitea cluster "$@"; }
    if [ "$1" = member-line ]; then
      f=/data/gitea/cluster-member-line
      [ -s /data/gitea/cluster/STAMP ] || fj init | tr -d "\r" | tail -n 1 >"$f"
      cat "$f"
    else
      fj "$@"
    fi' -- "$@"
}

echo "== PostgreSQL, the PKI and the stored files' volume"
dc up -d --wait postgres
dc run --rm pki
# The nodes' `up` runs it too, the offline commands below (--no-deps) not.
dc run --rm stored-init

# The offline commands need the data directories: servers stopped.
dc stop "${NODES[@]}" 2>/dev/null || true

# A node still waiting to be founded is stamped "joining" with an empty
# log. A founded node is never bootstrapped again.
members=""
pending=()
for n in "${NODES[@]}"; do
  line="$(offline "$n" member-line | tr -d '\r' | tail -n 1)"
  echo "   $n $line"
  members="${members:+$members,}$line"
  state="$(offline "$n" inspect)"
  if echo "$state" | grep -q '"Joining": true' && echo "$state" | grep -q '"LastIndex": 0,'; then
    pending+=("$n")
  fi
done

# Only the nodes still waiting: after a partial failure, a rerun bootstraps
# the rest and leaves the founded ones alone.
if [ "${#pending[@]}" -gt 0 ]; then
  echo "== cluster bootstrap on ${pending[*]}, the identical list everywhere"
  for n in "${pending[@]}"; do
    offline "$n" bootstrap --members "$members"
  done
else
  echo "== already founded"
fi

echo "== start"
dc up -d --wait "${NODES[@]}"
for n in "${NODES[@]}"; do
  dc exec -T -u git "$n" /usr/local/bin/gitea cluster status 2>/dev/null | head -n 1
done
