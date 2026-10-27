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
PORTS_STOP="$LAB_DIR/.build/observer-ports.stop"
rm -f "$PORTS_STOP"
dco() {
  docker compose --project-directory "$LAB_DIR" -f "$LAB_DIR/compose.yaml" -f "$LAB_DIR/compose.observer.yaml" \
    --profile observer --profile recover "$@"
}

# The LISTEN sockets the observer's own process holds: the inodes of the
# LISTEN entries of /proc/net/tcp{,6} that are among its file descriptors.
# Other listeners of the container's network namespace, such as Docker's
# embedded DNS on 127.0.0.11, are not the observer's. Only the script's own
# `socket inode` lines are kept: the suite stops the observer's container,
# and an exec that lands then makes docker print "cannot exec in a stopped
# container", which counted as a socket (#188). Such a sample is no sample,
# and listening fails; an exec that failed for another reason is printed,
# and fails the check, which must not go blind. Only docker's own message or
# a `compose ps` that answers and sees no running observer says it stopped:
# a `ps` that fails says nothing, and the sample stays a failure.
listening() {
  local out rc running ps_rc=0 why=""
  # shellcheck disable=SC2016 # expanded inside the container
  out="$(dco exec -T -u 0 observer sh -c '
    pid="$(pgrep -f "[g]itea cluster observe" | head -n 1)"
    [ -n "$pid" ] || exit 0
    for ino in $(cat /proc/net/tcp /proc/net/tcp6 /proc/net/udp /proc/net/udp6 2>/dev/null | awk "NR > 1 && (\$4 == \"0A\" || \$4 == \"07\") { print \$10 }"); do
      ls -l "/proc/$pid/fd" 2>/dev/null | grep -q "socket:\[$ino\]" && echo "socket inode $ino"
    done
    exit 0' 2>&1)"
  rc=$?
  if [ "$rc" = 0 ]; then
    printf '%s\n' "$out" | grep '^socket inode ' || true
    return 0
  fi
  if printf '%s\n' "$out" | grep -qE 'cannot exec in a stopped container|is not running|No such container'; then
    return 1
  fi
  running="$(dco ps -q --status running observer 2>/dev/null)" || ps_rc=$?
  if [ "$ps_rc" = 0 ] && [ -z "$running" ]; then
    return 1
  fi
  [ "$ps_rc" = 0 ] || why=", compose ps exit $ps_rc"
  echo "sample failed (exit $rc$why): $(printf '%s' "$out" | tr '\n' ' ')"
}
watch_ports() {
  local i seen=0
  : >"$PORTS"
  for ((i = 0; i < 900; i++)); do
    # The suite ended (the observer may never have started).
    [ ! -e "$PORTS_STOP" ] || break
    if dco exec -T observer pgrep -f 'cluster observe' >/dev/null 2>&1 && listening >>"$PORTS"; then
      seen=$((seen + 1))
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
touch "$PORTS_STOP"
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
step D.protected-checkpoint-restorable 'restorable from its snapshot'
step D.recover-rotation-first 'rotation first'
step D.recover-refuses-old-lfs-secret 'LFS_JWT_SECRET stops at the rotation'
step D.recover-replay-consistent 'inclusive off and on'
step D.recover-archives-history 'fresh archive holds the promoted timeline'
step D.recover-from-observer-serves 'new cluster .* serves'
step D.recovered-refs-match 'main is the clean commit'
step D.recovered-sql-rows 'issue the attacker deleted is back'
step D.recovered-lfs-object 'LFS object fetches'
step D.recovered-2fa-login '2FA login works'
step D.old-tokens-revoked 'old access tokens are revoked'

samples="$(sed -n 's/^samples //p' "$PORTS")"
bound="$(grep -v '^samples' "$PORTS" | sort -u | tr '\n' ' ')"
if [ "${samples:-0}" -gt 0 ] && [ -z "$bound" ]; then
  pass D.observer-opens-no-listening-port "$samples samples: no listening TCP or bound UDP socket held by the observer process"
else
  flunk D.observer-opens-no-listening-port "${bound:-the observer was never seen running}"
fi
[ "$rc" = 0 ] || flunk D.observer-e2e-script "scripts/observer-e2e.sh exited $rc"

finish
