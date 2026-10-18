#!/usr/bin/env bash
# Fault injection. Each scenario breaks one thing and says how to undo it.
#   chaos.sh forgejo <1|2|3>      stop a Forgejo node
#   chaos.sh pg-primary           stop the Patroni leader's container
#   chaos.sh partition <1|2|3>    cut a Forgejo node off the lab network
#   chaos.sh heal                 start everything stopped, reconnect partitions
set -euo pipefail
source "$(dirname "$0")/lib.sh"

net="${COMPOSE_PROJECT_NAME}_lab"
cid() { dc ps -q "$1"; }

case "${1:-}" in
  forgejo)
    n="${2:?node 1, 2 or 3}"
    dc stop -t 5 "forgejo-$n"
    ok "forgejo-$n stopped (undo: make heal)"
    ;;
  pg-primary)
    leader="$(patroni_leader)"
    dc stop -t 5 "$leader"
    ok "$leader (Patroni leader) stopped (undo: make heal)"
    ;;
  partition)
    n="${2:?node 1, 2 or 3}"
    docker network disconnect "$net" "$(cid "forgejo-$n")"
    ok "forgejo-$n cut off $net (undo: make heal)"
    ;;
  heal)
    for n in 1 2 3; do
      c="$(cid "forgejo-$n")"
      [ -n "$c" ] || continue
      if ! docker inspect -f '{{json .NetworkSettings.Networks}}' "$c" | grep -q "\"$net\""; then
        docker network connect --ip "10.89.0.4$n" "$net" "$c"
        ok "forgejo-$n reconnected"
      fi
    done
    dc up -d --wait --no-build
    ok "all services up"
    ;;
  *)
    sed -n '2,6p' "$0" >&2
    exit 64
    ;;
esac
