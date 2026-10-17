#!/usr/bin/env bash
# Proves a Patroni failover behind HAProxy: stop the leader's container, a
# replica becomes primary, HAProxy routes to it, writes go through, and the
# old leader rejoins as a replica once restarted.
set -euo pipefail
source "$(dirname "$0")/lib.sh"

say "Before"
patronictl list
old="$(patroni_leader)"
[ "$(haproxy_primary)" = "$old" ] || fail "HAProxy routes to '$(haproxy_primary)', Patroni leader is $old"
ok "Patroni leader $old, HAProxy routes to $old"
via_haproxy "CREATE TABLE IF NOT EXISTS lab_failover(at timestamptz default now(), via text)" >/dev/null
via_haproxy "INSERT INTO lab_failover(via) VALUES ('$old')" >/dev/null
ok "write through HAProxy on $old"

say "Stop the leader container ($old)"
t0=$(date +%s)
dc stop -t 5 "$old" >/dev/null
wait_for 60 "a new Patroni leader is elected" bash -c "source '$LAB_DIR/scripts/lib.sh'; l=\$(patroni_leader) && [ -n \"\$l\" ] && [ \"\$l\" != '$old' ]"
new="$(patroni_leader)"
wait_for 30 "HAProxy routes to the new leader $new" bash -c "source '$LAB_DIR/scripts/lib.sh'; [ \"\$(haproxy_primary)\" = '$new' ]"
wait_for 30 "a write through HAProxy succeeds" via_haproxy "INSERT INTO lab_failover(via) VALUES ('$new')"
echo "failover: $old -> $new in $(( $(date +%s) - t0 ))s"
[ "$(via_haproxy 'SELECT pg_is_in_recovery()')" = "f" ] || fail "HAProxy reaches a node in recovery"
ok "HAProxy reaches a primary (pg_is_in_recovery = f)"
patronictl list

say "Restart $old"
dc start "$old" >/dev/null
wait_for 120 "$old is back and streaming (2 standbys)" bash -c "source '$LAB_DIR/scripts/lib.sh'; [ \"\$(patroni_streaming)\" = 2 ]"
[ "$(via_haproxy 'SELECT count(*) FROM lab_failover')" -ge 2 ] || fail "rows lost"
ok "both rows present"
patronictl list
