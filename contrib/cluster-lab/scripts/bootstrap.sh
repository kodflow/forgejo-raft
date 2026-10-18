#!/usr/bin/env bash
# Founds the Forgejo cluster in two phases (kitsunium ADR 0152, R2-24, R7-36,
# R9-04), servers stopped:
#   1. `cluster init` on every founding node prints
#      id:incarnation:slot:spki:host:port
#   2. `cluster bootstrap --members` on every founding node, with the
#      IDENTICAL list of the three lines
# then the servers start. Each phase is idempotent on the Forgejo side, so
# a rerun resumes. A node whose data directory is not initialised exits at
# start (and s6 takes its container down), so the commands run in one-off
# containers of the same service: same volume, same environment, same IP.
set -euo pipefail
source "$(dirname "$0")/lib.sh"

NODES=(forgejo-1 forgejo-2 forgejo-3)

# offline <service> <cluster args...>: app.ini rendered as the server would
# render it (the image's s6 setup), then the command as the git user.
offline() {
  local svc="$1"
  shift
  dc run --rm --no-deps -T "$svc" bash -c \
    'cd /etc/s6/gitea && source ./setup >/dev/null && exec su-exec git /usr/local/bin/gitea cluster "$@"' \
    -- "$@"
}

say "Stop the Forgejo servers"
dc stop -t 5 "${NODES[@]}"

say "cluster init"
members=""
for n in "${NODES[@]}"; do
  line="$(offline "$n" init | tr -d '\r' | tail -n 1)"
  echo "$line" | grep -Eq '^[0-9a-f]{16}:[0-9a-f]{32}:[a-z0-9-]{1,32}:[0-9a-f]{64}:[0-9.]+:[0-9]+$' ||
    fail "unexpected init output on $n: $line"
  ok "$n $line"
  members="${members:+$members,}$line"
done

say "cluster bootstrap (identical list on every node)"
for n in "${NODES[@]}"; do
  offline "$n" bootstrap --members "$members"
  ok "$n bootstrapped"
done

say "Start the Forgejo servers"
dc up -d --no-build "${NODES[@]}"
echo "bootstrap: founded with $members"
