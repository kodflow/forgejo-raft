#!/bin/sh
# Runs the lab's Nomad dev agent with ACLs on, and bootstraps them with the
# lab's management token (NOMAD_TOKEN, from the git-ignored .env): no
# anonymous call can submit a job. A dev agent keeps its state in memory, so
# every start bootstraps again.
set -eu
: "${NOMAD_TOKEN:?NOMAD_TOKEN comes from the lab .env}"
nomad agent "$@" &
agent=$!
trap 'kill -TERM "$agent"' TERM INT
i=0
until printf '%s' "$NOMAD_TOKEN" | nomad acl bootstrap - >/dev/null 2>/tmp/bootstrap.err; do
  if grep -qi 'already done' /tmp/bootstrap.err; then break; fi
  i=$((i + 1))
  if [ "$i" -ge 90 ]; then cat /tmp/bootstrap.err >&2; exit 1; fi
  sleep 1
done
echo "nomad: ACLs bootstrapped with the lab's token"
wait "$agent"
