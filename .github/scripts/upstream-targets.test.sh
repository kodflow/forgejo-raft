#!/usr/bin/env bash
# Offline test of upstream-targets.sh: a throwaway upstream repository for the
# tags and SUPPORTED for the TXT record, one case per selection the script
# must get right. Run it from anywhere:
#   .github/scripts/upstream-targets.test.sh
set -euo pipefail

targets=$(cd "$(dirname "$0")" && pwd)/upstream-targets.sh
tmp=$(mktemp -d)
trap 'rm -rf "$tmp"' EXIT
export GIT_AUTHOR_NAME=test GIT_AUTHOR_EMAIL=test@example.invalid
export GIT_COMMITTER_NAME=test GIT_COMMITTER_EMAIL=test@example.invalid
export GIT_CONFIG_GLOBAL=/dev/null GIT_CONFIG_NOSYSTEM=1
unset TAG RAFT_MAJORS SUPPORTED GITHUB_OUTPUT

git init -q -b forgejo "$tmp/up"
git -C "$tmp/up" commit -q --allow-empty -m init
for t in v15.0.9 v15.0.10 v16.0.6 v17.0.0 v17.0.1 v18.0.0; do
	git -C "$tmp/up" tag "$t"
done
export UPSTREAM_URL=$tmp/up

failures=0
expect() { # expect <case> <want json> [VAR=value...]
	local name=$1 want=$2 got
	shift 2
	got=$(env "$@" "$targets" 2>/dev/null | sed -n 's/^tags=//p')
	if [ "$got" = "$want" ]; then
		echo "ok   $name: $got"
	else
		echo "FAIL $name: $got, want $want"
		failures=$((failures + 1))
	fi
}

expect "highest major only" '["v17.0.0"]' SUPPORTED=15.0.10,16.0.6,17.0.0
expect "order does not matter" '["v17.0.0"]' SUPPORTED=17.0.0,15.0.10,16.0.6
expect "a new major takes over" '["v18.0.0"]' SUPPORTED=16.0.6,17.0.1,18.0.0
expect "major compared as a number" '["v17.0.0"]' SUPPORTED=9.0.3,17.0.0
expect "newest of the highest major" '["v17.0.1"]' SUPPORTED=16.0.6,17.0.0,17.0.1
expect "new major not tagged yet" '["v17.0.1"]' SUPPORTED=17.0.1,19.0.0
expect "release candidate ignored" '["v17.0.0"]' SUPPORTED=17.0.0,18.0.0-rc0
expect "nothing tagged" '[]' SUPPORTED=19.0.0
expect "RAFT_MAJORS one older major" '["v16.0.6"]' SUPPORTED=15.0.10,16.0.6,17.0.0 RAFT_MAJORS=16
expect "RAFT_MAJORS several majors" '["v15.0.10","v16.0.6","v17.0.0"]' SUPPORTED=15.0.10,16.0.6,17.0.0 "RAFT_MAJORS=15, 16 17"
expect "RAFT_MAJORS none supported" '[]' SUPPORTED=15.0.10,16.0.6,17.0.0 RAFT_MAJORS=14
expect "TAG an older release" '["v15.0.9"]' TAG=v15.0.9
expect "TAG not tagged upstream" '[]' TAG=v15.0.11

out=$tmp/github_output
SUPPORTED=15.0.10,16.0.6,17.0.0 GITHUB_OUTPUT=$out "$targets" >/dev/null 2>&1
if [ "$(cat "$out")" = 'tags=["v17.0.0"]' ]; then
	echo "ok   GITHUB_OUTPUT written"
else
	echo "FAIL GITHUB_OUTPUT: $(cat "$out")"
	failures=$((failures + 1))
fi

if [ "$failures" -gt 0 ]; then
	echo "$failures case(s) failed"
	exit 1
fi
echo "all cases passed"
