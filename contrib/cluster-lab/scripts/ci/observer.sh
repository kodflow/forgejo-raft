#!/usr/bin/env bash
# CI suite D, on the lab under compose.observer.yaml, up and bootstrapped:
# checkpoints and the observer. It runs the lab's scripts/observer-e2e.sh
# and turns each of its steps into a checkpoint, and checks on its own, while
# the observer runs, that the observer opens no listening port. Prints one
# `CHECK <name>: PASS|FAIL` per checkpoint and exits non-zero when any
# failed.
set -uo pipefail
source "$(dirname "$0")/lib.sh"

LOG="$LAB_DIR/.build/observer-e2e.log"
PORTS="$LAB_DIR/.build/observer-ports.txt"
dco() {
  docker compose --project-directory "$LAB_DIR" -f "$LAB_DIR/compose.yaml" -f "$LAB_DIR/compose.observer.yaml" \
    --profile observer --profile recover "$@"
}

# The observer's sockets in LISTEN state (0A), TCP and UDP bound alike,
# from /proc: the image needs no ss or netstat for it.
listening() {
  dco exec -T observer sh -c 'cat /proc/net/tcp /proc/net/tcp6 2>/dev/null' |
    awk 'NR > 1 && $4 == "0A" { print $2 }' | grep -v '^local_address' || true
}
watch_ports() {
  local i seen=0
  : >"$PORTS"
  for ((i = 0; i < 900; i++)); do
    if dco exec -T observer pgrep -f 'cluster observe' >/dev/null 2>&1; then
      seen=$((seen + 1))
      listening >>"$PORTS"
      # Two minutes of samples while the observer runs and pulls.
      [ "$seen" -lt 60 ] || break
    fi
    sleep 2
  done
  echo "samples $seen" >>"$PORTS"
}

say "D. Checkpoints and the observer (scripts/observer-e2e.sh)"
watch_ports &
watcher=$!
"$LAB_DIR/scripts/observer-e2e.sh" 2>&1 | tee "$LOG"
rc=${PIPESTATUS[0]}
wait "$watcher"

# step <name> <pattern of the observer-e2e line that proves it>
step() {
  local line
  line="$(grep -E "^OK +.*$2" "$LOG" | head -n 1 | sed -E 's/^OK +//')"
  if [ -n "$line" ]; then pass "$1" "$line"; else flunk "$1" "not reached$(grep -E '^FAIL' "$LOG" | head -n 1 | sed 's/^FAIL */: /')"; fi
}
step D.checkpoint-role-granted 'checkpoint role granted'
step D.observer-started-without-published-port 'observer started'
step D.checkpoint-protected 'protected .* after the push'
step D.poisoned-checkpoint-protected-too 'poisoned history is checkpoint'
step D.checkpoint-across-pg-failover 'archive has no gap'
step D.cluster-lost 'old cluster is gone'
step D.protected-checkpoint-restorable 'PROTECTED and restorable'
step D.recover-rotation-first 'rotation first'
step D.recover-replay-consistent 'inclusive off and on'
step D.recover-from-observer-serves 'new cluster .* serves'
step D.recovered-refs-match 'main is the clean commit'
step D.recovered-sql-rows 'issue the attacker deleted is back'
step D.recovered-lfs-object 'LFS object fetches'
step D.recovered-2fa-login '2FA login works'
step D.old-tokens-revoked 'old access tokens are revoked'

samples="$(sed -n 's/^samples //p' "$PORTS")"
bound="$(grep -v '^samples' "$PORTS" | sort -u | tr '\n' ' ')"
if [ "${samples:-0}" -gt 0 ] && [ -z "$bound" ]; then
  pass D.observer-opens-no-listening-port "$samples samples of /proc/net/tcp{,6} in the observer: no LISTEN socket"
else
  flunk D.observer-opens-no-listening-port "${bound:-the observer was never seen running}"
fi
[ "$rc" = 0 ] || flunk D.observer-e2e-script "scripts/observer-e2e.sh exited $rc"

finish
