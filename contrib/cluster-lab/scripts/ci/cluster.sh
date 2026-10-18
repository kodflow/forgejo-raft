#!/usr/bin/env bash
# CI suite A and B, on the lab under compose.ci.yaml, just started (no
# bootstrap): the autonomous formation, then the Raft behaviour under
# failures. Prints one `CHECK <name>: PASS|FAIL` per checkpoint and exits
# non-zero when any failed.
#
#   A. formation: three nodes found one cluster by themselves; one leader,
#      three voters, the same status everywhere, the same commit index after
#      a write.
#   B. Raft: election at start-up; the leader killed, a new one within a
#      bound and writes resume; the old leader back as a follower; a follower
#      killed; two of three killed, no write accepted, then recovery; the
#      consensus network partitioned, the minority refuses and the majority
#      accepts, convergence after the heal; never two leaders in one term,
#      polled throughout; the leader killed again after all that; and the
#      lab's own e2e.sh (Patroni failover).
set -uo pipefail
source "$(dirname "$0")/lib.sh"

# Bounds, in seconds.
FORM_BOUND="${FORM_BOUND:-180}"
ELECTION_BOUND="${ELECTION_BOUND:-15}"
WRITE_RESUME_BOUND="${WRITE_RESUME_BOUND:-30}"
CATCH_UP_BOUND="${CATCH_UP_BOUND:-120}"
REFUSE_BOUND="${REFUSE_BOUND:-150}"

RAFT_NET="${COMPOSE_PROJECT_NAME}_raft"
REPO_NAME="raft-$(date +%s)"
REPO="$ADMIN/$REPO_NAME"
WORK="$(mktemp -d)"
WATCH_LOG="$LAB_DIR/.build/leader-watch.log"
WATCH_STOP="$WORK/watch.stop"
WATCH_PID=""
# The watcher is killed too: an early exit would otherwise remove its stop
# file with $WORK and leave it writing to tee's pipe, hanging the job.
trap 'touch "$WATCH_STOP"; [ -z "$WATCH_PID" ] || kill "$WATCH_PID" 2>/dev/null; rm -rf "$WORK"' EXIT

# ---------------------------------------------------------------- helpers

# push <node> <branch>: a new commit on <branch>, pushed in one try; the
# sha on stdout, git's error in $WORK/push.err.
push() {
  local sha
  sha="$(commit_to "$WORK/repo" "$2")" || return 1
  timeout "$REFUSE_BOUND" git -C "$WORK/repo" push -q "$(remote "$1" "$REPO")" "$2" 2>"$WORK/push.err" || return 1
  echo "$sha"
}
# push_until <seconds> <node> <branch>: retries the same commit until the
# node accepts it.
push_until() {
  local t="$1" n="$2" b="$3" start sha
  start=$(date +%s)
  sha="$(commit_to "$WORK/repo" "$b")" || return 1
  while :; do
    if timeout 60 git -C "$WORK/repo" push -q "$(remote "$n" "$REPO")" "$b" 2>"$WORK/push.err"; then
      echo "$sha"
      return 0
    fi
    [ $(($(date +%s) - start)) -lt "$t" ] || return 1
    sleep 1
  done
}
kill_node() { dc kill -s KILL "forgejo-$1" >/dev/null 2>&1; }
start_node() { dc start "forgejo-$1" >/dev/null 2>&1; }
cut_off() { docker network disconnect "$RAFT_NET" "$(dc ps -q "forgejo-$1")"; }
reconnect() { docker network connect --ip "10.89.1.4$1" "$RAFT_NET" "$(dc ps -q "forgejo-$1")"; }
# A leader among the given nodes, by their own status.
leader_among() {
  local n
  for n in "$@"; do
    [ "$(field "$n" .Role 2>/dev/null)" = leader ] && { echo "$n"; return 0; }
  done
  return 1
}
# The nodes agree on everything the consensus replicates: the commit and
# applied indexes, the table digest and the refs each serves.
converged() {
  local n c a r refs first_c first_r first_refs
  for n in "$@"; do
    c="$(field "$n" .Commit)" a="$(field "$n" .Applied)" r="$(field "$n" .Root)"
    refs="$(all_refs "$n" "$REPO")"
    [ -n "$c" ] && [ "$c" = "$a" ] && [ -n "$refs" ] || return 1
    if [ -z "${first_c:-}" ]; then
      first_c="$c" first_r="$r" first_refs="$refs"
    elif [ "$c" != "$first_c" ] || [ "$r" != "$first_r" ] || [ "$refs" != "$first_refs" ]; then
      return 1
    fi
  done
}
clear_refusal() { # the push error says the cluster refused, not a transport failure
  grep -qiE 'cluster|quorum|leader|nothing was written' "$WORK/push.err"
}

# The leader watch: every node's role and term from /metrics, three times a
# second, for the whole run. A node that does not answer is skipped.
leader_watch() {
  local n m role term ts
  : >"$WATCH_LOG"
  while [ ! -e "$WATCH_STOP" ]; do
    ts=$(date +%s.%N)
    for n in 1 2 3; do
      m="$(curl -s -m 1 "$(node_url "$n")/metrics" 2>/dev/null)" || continue
      role="$(echo "$m" | awk '/^forgejo_cluster_role\{/ && $NF+0 == 1 { if (match($0, /role="[a-z]+"/)) print substr($0, RSTART + 6, RLENGTH - 7) }')"
      term="$(echo "$m" | awk '/^forgejo_cluster_term / { print $2 + 0 }')"
      [ -n "$role" ] && echo "$ts $n $role ${term:-0}"
    done >>"$WATCH_LOG"
    sleep 0.3
  done
}

# ---------------------------------------------------------------- A

say "A. Formation"
all_ready=1
for n in 1 2 3; do poll "$FORM_BOUND" healthy "$n" || all_ready=0; done
if [ "$all_ready" = 1 ]; then pass A.nodes-ready; else flunk A.nodes-ready "a node is not ready after ${FORM_BOUND}s"; fi

leader_watch &
WATCH_PID=$!

autonomous() {
  local n id first=""
  for n in 1 2 3; do
    [ "$(field "$n" .ExpectedNodes)" = 3 ] || return 1
    id="$(field "$n" .Identity)"
    [ -n "$id" ] && [ "$id" != null ] || return 1
    [ -z "$first" ] || [ "$id" = "$first" ] || return 1
    first="$id"
  done
}
if poll 60 autonomous; then
  pass A.autonomous-formation "EXPECTED_NODES=3, one lineage $(field 1 .Identity), no init or bootstrap run"
else
  flunk A.autonomous-formation "$(for n in 1 2 3; do printf 'node %s: expected=%s lineage=%s; ' "$n" "$(field "$n" .ExpectedNodes)" "$(field "$n" .Identity)"; done)"
fi

one_leader() { [ "$(leaders | grep -c .)" = 1 ]; }
if poll 60 one_leader; then
  L="$(leader)"
  pass A.exactly-one-leader "forgejo-$L, term $(field "$L" .Term)"
  pass B.leader-elected-at-startup "forgejo-$L"
else
  flunk A.exactly-one-leader "leaders: $(leaders | tr '\n' ' ')"
  flunk B.leader-elected-at-startup
  L=1
fi

three_voters() {
  local n
  for n in 1 2 3; do
    [ "$(field "$n" '[.Members[] | select(.Voter)] | length')" = 3 ] || return 1
  done
}
if poll 120 three_voters; then
  pass A.three-voters "$(field "$L" '[.Members[] | "\(.Slot)=\(if .Voter then "voter" else "learner" end)"] | join(",")')"
else
  flunk A.three-voters "$(for n in 1 2 3; do printf 'node %s: %s; ' "$n" "$(field "$n" '[.Members[] | "\(.Slot)=\(.Voter)"] | join(",")')"; done)"
fi

status_agrees() {
  local n s first=""
  for n in 1 2 3; do
    s="$(field "$n" '[.Identity, .Leader, (.Term|tostring), ([.Members[] | "\(.ID):\(.Slot):\(.Voter)"] | sort | join(","))] | join(" ")')"
    [ -n "$s" ] || return 1
    [ -z "$first" ] || [ "$s" = "$first" ] || return 1
    first="$s"
  done
}
if poll 30 status_agrees; then
  pass A.status-agrees-on-every-node
else
  flunk A.status-agrees-on-every-node "$(for n in 1 2 3; do printf 'node %s: leader %s term %s; ' "$n" "$(field "$n" .Leader)" "$(field "$n" .Term)"; done)"
fi

# A write, then the same commit index everywhere.
ensure_admin || flunk setup.admin-token
api 1 POST /user/repos "{\"name\":\"$REPO_NAME\",\"auto_init\":false,\"default_branch\":\"main\"}" >/dev/null ||
  flunk setup.repository "creation of $REPO on node 1"
git init -q -b main "$WORK/repo"
if sha="$(push 1 main)"; then
  if poll 30 converged 1 2 3; then
    pass A.same-commit-index-after-write "commit $(field 1 .Commit) on every node, main at ${sha:0:12}"
  else
    flunk A.same-commit-index-after-write "$(for n in 1 2 3; do printf 'node %s: commit %s applied %s; ' "$n" "$(field "$n" .Commit)" "$(field "$n" .Applied)"; done)"
  fi
else
  flunk A.same-commit-index-after-write "the first push to node 1 failed: $(tail -n 3 "$WORK/push.err" | tr '\n' ' ')"
fi

# ---------------------------------------------------------------- B

say "B1. The leader killed"
L="$(leader)" || L=1
read -r S1 S2 <<<"$(others "$L" | tr '\n' ' ')"
t0=$(date +%s.%N)
kill_node "$L"
if poll "$ELECTION_BOUND" leader_among "$S1" "$S2"; then
  NL="$(leader_among "$S1" "$S2")"
  pass B.new-leader-after-leader-kill "forgejo-$NL in ${POLL_TOOK}s (bound ${ELECTION_BOUND}s), forgejo-$L killed"
else
  flunk B.new-leader-after-leader-kill "no leader among forgejo-$S1 and forgejo-$S2 within ${ELECTION_BOUND}s"
fi
if sha="$(push_until "$WRITE_RESUME_BOUND" "$S1" b1-after-leader-kill)"; then
  took="$(printf '%.1f' "$(echo "$(date +%s.%N) - $t0" | bc)")"
  if poll 10 serves "$S2" "$REPO" refs/heads/b1-after-leader-kill "$sha"; then
    pass B.writes-resume-after-leader-kill "push to forgejo-$S1 ${took}s after the kill, read on forgejo-$S2"
  else
    flunk B.writes-resume-after-leader-kill "forgejo-$S2 does not serve the push to forgejo-$S1"
  fi
else
  flunk B.writes-resume-after-leader-kill "no push accepted within ${WRITE_RESUME_BOUND}s: $(tail -n 2 "$WORK/push.err" | tr '\n' ' ')"
fi

say "B2. The old leader comes back"
start_node "$L"
if poll "$CATCH_UP_BOUND" healthy "$L" && poll "$CATCH_UP_BOUND" serves "$L" "$REPO" refs/heads/b1-after-leader-kill "$sha"; then
  role="$(field "$L" .Role)"
  if [ "$role" = follower ] && poll 30 converged 1 2 3; then
    pass B.old-leader-rejoins-as-follower "forgejo-$L follower, caught up at commit $(field "$L" .Commit)"
  else
    flunk B.old-leader-rejoins-as-follower "forgejo-$L role $role, commit $(field "$L" .Commit) vs $(field "$S1" .Commit)"
  fi
else
  flunk B.old-leader-rejoins-as-follower "forgejo-$L not ready or not caught up within ${CATCH_UP_BOUND}s"
fi

say "B3. A follower killed"
L="$(leader)" || L=1
read -r F A <<<"$(others "$L" | tr '\n' ' ')"
kill_node "$F"
if sha="$(push_until "$WRITE_RESUME_BOUND" "$A" b3-follower-down)" &&
  poll 10 serves "$L" "$REPO" refs/heads/b3-follower-down "$sha"; then
  pass B.writes-continue-with-a-follower-down "forgejo-$F killed, push to forgejo-$A, read on forgejo-$L"
else
  flunk B.writes-continue-with-a-follower-down "$(tail -n 2 "$WORK/push.err" | tr '\n' ' ')"
fi
start_node "$F"
if poll "$CATCH_UP_BOUND" serves "$F" "$REPO" refs/heads/b3-follower-down "$sha" && poll 30 converged 1 2 3; then
  pass B.killed-follower-catches-up "forgejo-$F"
else
  flunk B.killed-follower-catches-up "forgejo-$F"
fi

say "B4. Two of three killed"
L="$(leader)" || L=1
read -r F SURV <<<"$(others "$L" | tr '\n' ' ')"
before_refs="$(all_refs "$SURV" "$REPO")"
kill_node "$L"
kill_node "$F"
# The survivor keeps its HTTP side and PostgreSQL; only its peers are gone.
if out="$(push "$SURV" b4-no-quorum)"; then
  flunk B.no-write-without-quorum "forgejo-$SURV accepted b4-no-quorum alone at ${out:0:12}"
elif clear_refusal; then
  pass B.no-write-without-quorum "git push to forgejo-$SURV refused: $(grep -iE 'cluster|quorum|leader|nothing was written' "$WORK/push.err" | head -n 1 | sed 's/^remote: *//' | cut -c1-160)"
else
  flunk B.no-write-without-quorum "refused, but without a clear reason: $(tail -n 2 "$WORK/push.err" | tr '\n' ' ')"
fi
code="$(curl -s -o "$WORK/api.out" -w '%{http_code}' -m 60 -H "Authorization: token $TOKEN" -H 'Content-Type: application/json' \
  -X POST --data '{"new_branch_name":"b4-api-no-quorum","old_branch_name":"main"}' "$(node_url "$SURV")/api/v1/repos/$REPO/branches")"
if [ "$code" = 503 ] && grep -q 'nothing was written' "$WORK/api.out"; then
  pass B.no-api-ref-write-without-quorum "branch creation through the API on forgejo-$SURV: HTTP 503, $(jq -r .message "$WORK/api.out" | cut -c1-100)"
else
  flunk B.no-api-ref-write-without-quorum "HTTP $code: $(head -c 200 "$WORK/api.out")"
fi
if [ "$(all_refs "$SURV" "$REPO")" = "$before_refs" ] && [ "$(field "$SURV" .Role)" != leader ]; then
  pass B.minority-node-is-read-only "forgejo-$SURV serves the same refs, role $(field "$SURV" .Role)"
else
  flunk B.minority-node-is-read-only "refs changed or role $(field "$SURV" .Role)"
fi
start_node "$F"
if poll "$CATCH_UP_BOUND" healthy "$F" && sha="$(push_until "$CATCH_UP_BOUND" "$SURV" b4-quorum-back)"; then
  pass B.writes-recover-with-quorum-back "forgejo-$F back, push to forgejo-$SURV accepted"
else
  flunk B.writes-recover-with-quorum-back "$(tail -n 2 "$WORK/push.err" | tr '\n' ' ')"
fi
start_node "$L"
if poll "$CATCH_UP_BOUND" healthy "$L" && poll "$CATCH_UP_BOUND" converged 1 2 3 &&
  [ -z "$(ref_sha "$L" "$REPO" refs/heads/b4-no-quorum)" ] &&
  [ -z "$(ref_sha "$L" "$REPO" refs/heads/b4-api-no-quorum)" ]; then
  pass B.all-converge-after-quorum-loss "the refused writes appear nowhere"
else
  flunk B.all-converge-after-quorum-loss
fi

say "B5. A follower partitioned off the consensus network"
L="$(leader)" || L=1
read -r F A <<<"$(others "$L" | tr '\n' ' ')"
cut_off "$F"
if sha="$(push_until "$WRITE_RESUME_BOUND" "$L" b5-majority)" && poll 10 serves "$A" "$REPO" refs/heads/b5-majority "$sha"; then
  pass B.partition-majority-accepts "follower forgejo-$F cut off, push to forgejo-$L, read on forgejo-$A"
else
  flunk B.partition-majority-accepts "$(tail -n 2 "$WORK/push.err" | tr '\n' ' ')"
fi
if out="$(push "$F" b5-minority)"; then
  flunk B.partition-minority-refuses "the cut-off follower forgejo-$F accepted b5-minority at ${out:0:12}"
elif clear_refusal; then
  pass B.partition-minority-refuses "forgejo-$F: $(grep -iE 'cluster|quorum|leader|nothing was written' "$WORK/push.err" | head -n 1 | sed 's/^remote: *//' | cut -c1-160)"
else
  flunk B.partition-minority-refuses "refused without a clear reason: $(tail -n 2 "$WORK/push.err" | tr '\n' ' ')"
fi
reconnect "$F"
if poll "$CATCH_UP_BOUND" converged 1 2 3 && [ -z "$(ref_sha "$F" "$REPO" refs/heads/b5-minority)" ]; then
  pass B.partitioned-follower-converges "same refs, commit and table digest on the three nodes"
else
  flunk B.partitioned-follower-converges
fi

say "B6. The leader partitioned off the consensus network"
L="$(leader)" || L=1
read -r X Y <<<"$(others "$L" | tr '\n' ' ')"
cut_off "$L"
if poll 30 leader_among "$X" "$Y"; then
  pass B.partition-majority-elects "forgejo-$(leader_among "$X" "$Y") in ${POLL_TOOK}s, old leader forgejo-$L cut off"
else
  flunk B.partition-majority-elects
fi
if sha="$(push_until "$WRITE_RESUME_BOUND" "$X" b6-majority)" && poll 10 serves "$Y" "$REPO" refs/heads/b6-majority "$sha"; then
  pass B.partition-majority-accepts-writes "push to forgejo-$X, read on forgejo-$Y"
else
  flunk B.partition-majority-accepts-writes "$(tail -n 2 "$WORK/push.err" | tr '\n' ' ')"
fi
if out="$(push "$L" b6-minority)"; then
  flunk B.partition-old-leader-refuses "the cut-off old leader forgejo-$L accepted b6-minority at ${out:0:12}"
elif clear_refusal; then
  pass B.partition-old-leader-refuses "forgejo-$L: $(grep -iE 'cluster|quorum|leader|nothing was written' "$WORK/push.err" | head -n 1 | sed 's/^remote: *//' | cut -c1-160)"
else
  flunk B.partition-old-leader-refuses "refused without a clear reason: $(tail -n 2 "$WORK/push.err" | tr '\n' ' ')"
fi
reconnect "$L"
if poll "$CATCH_UP_BOUND" serves "$L" "$REPO" refs/heads/b6-majority "$sha" && poll "$CATCH_UP_BOUND" converged 1 2 3 &&
  [ -z "$(ref_sha "$L" "$REPO" refs/heads/b6-minority)" ]; then
  pass B.partition-heals-without-divergence "same refs, commit and table digest; b6-minority nowhere"
else
  flunk B.partition-heals-without-divergence "$(for n in 1 2 3; do printf 'node %s: commit %s root %s; ' "$n" "$(field "$n" .Commit)" "$(field "$n" .Root | cut -c1-12)"; done)"
fi
if poll 30 one_leader; then pass B.one-leader-after-heal "forgejo-$(leader)"; else flunk B.one-leader-after-heal "$(leaders | tr '\n' ' ')"; fi

say "B7. The leader killed again, after restarts and partitions"
L="$(leader)" || L=1
read -r S1 S2 <<<"$(others "$L" | tr '\n' ' ')"
t0=$(date +%s.%N)
kill_node "$L"
if poll "$ELECTION_BOUND" leader_among "$S1" "$S2"; then
  pass B.second-leader-kill-elects "forgejo-$(leader_among "$S1" "$S2") in ${POLL_TOOK}s (bound ${ELECTION_BOUND}s), forgejo-$L killed"
else
  flunk B.second-leader-kill-elects "no leader among forgejo-$S1 and forgejo-$S2 within ${ELECTION_BOUND}s"
fi
if sha="$(push_until "$WRITE_RESUME_BOUND" "$S1" b7-second-kill)" && poll 10 serves "$S2" "$REPO" refs/heads/b7-second-kill "$sha"; then
  pass B.second-leader-kill-writes-resume "push to forgejo-$S1 $(printf '%.1f' "$(echo "$(date +%s.%N) - $t0" | bc)")s after the kill, read on forgejo-$S2"
else
  flunk B.second-leader-kill-writes-resume "$(tail -n 2 "$WORK/push.err" | tr '\n' ' ')"
fi
start_node "$L"
if poll "$CATCH_UP_BOUND" healthy "$L" && poll "$CATCH_UP_BOUND" converged 1 2 3; then
  pass B.second-killed-leader-catches-up "forgejo-$L"
else
  flunk B.second-killed-leader-catches-up "forgejo-$L"
fi

say "B8. The lab's own end-to-end run (leader SIGKILL, Patroni failover, catch-up)"
if "$LAB_DIR/scripts/e2e.sh" >"$LAB_DIR/.build/lab-e2e.log" 2>&1; then
  pass B.lab-e2e-script "$(grep -c '^OK' "$LAB_DIR/.build/lab-e2e.log") steps, see lab-e2e.log"
else
  flunk B.lab-e2e-script "$(grep -E '^FAIL' "$LAB_DIR/.build/lab-e2e.log" | head -n 1)"
fi
cat "$LAB_DIR/.build/lab-e2e.log"

say "B9. Never two leaders in one term"
touch "$WATCH_STOP"
wait
samples="$(wc -l <"$WATCH_LOG" | tr -d ' ')"
dup="$(awk '$3 == "leader" { if (($4 in who) && who[$4] != $2) print "term " $4 ": forgejo-" who[$4] " and forgejo-" $2; who[$4] = $2 }' "$WATCH_LOG" | sort -u)"
terms="$(awk '$3 == "leader" { print $4 }' "$WATCH_LOG" | sort -un | tr '\n' ' ')"
if [ "$samples" -gt 100 ] && [ -z "$dup" ]; then
  pass B.never-two-leaders-in-a-term "$samples samples, leader terms: $terms"
else
  flunk B.never-two-leaders-in-a-term "${dup:-only $samples samples}"
fi

finish
