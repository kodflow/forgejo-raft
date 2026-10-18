#!/usr/bin/env bash
# Offline test of raft-release-branch.sh: throwaway upstream (a development
# branch and a stable branch with its tags) and origin repositories, one case
# per mode, plus the proof that nothing of upstream's development branch
# reaches a release branch. Run it from anywhere:
#   .github/scripts/raft-release-branch.test.sh
set -euo pipefail

script=$(cd "$(dirname "$0")" && pwd)/raft-release-branch.sh
tmp=$(mktemp -d)
trap 'rm -rf "$tmp"' EXIT
export GIT_AUTHOR_NAME=test GIT_AUTHOR_EMAIL=test@example.invalid
export GIT_COMMITTER_NAME=test GIT_COMMITTER_EMAIL=test@example.invalid
export GIT_CONFIG_GLOBAL=/dev/null GIT_CONFIG_NOSYSTEM=1

failures=0
ok() { echo "ok   $1"; }
fail() {
	echo "FAIL $1"
	failures=$((failures + 1))
}
put() { # put <path> <content> <message>
	mkdir -p "$(dirname "$1")"
	printf '%s\n' "$2" >"$1"
	git add "$1"
	git commit -q -m "$3"
}
run() { # run <tag> [args...]: the script's outputs in $out, its status in $status
	local tag=$1
	shift
	status=0
	out=$(TAG=$tag UPSTREAM_URL=$tmp/upstream.git REMOTE=${REMOTE_REPO:-$tmp/origin.git} \
		NOTES_FILE=$tmp/notes.md SOURCE=forgejo "$script" "$@" 2>&1) || status=$?
}
get() { sed -n "s/^$1=//p" <<<"$out"; }
file_at() { git --git-dir="${REMOTE_REPO:-$tmp/origin.git}" show "$1:$2" 2>/dev/null || echo '<absent>'; }

# Upstream: a base, the stable branch v2.0/forgejo with v2.0.0, and the
# development branch moving on.
git init -q -b forgejo "$tmp/up"
cd "$tmp/up"
put shared.txt A 'upstream: base'
put core.txt core 'upstream: core'
git switch -q -c v2.0/forgejo
put fix.txt stable-0 'upstream: v2.0 branch point'
git tag v2.0.0
git switch -q forgejo
put dev.txt dev-only 'upstream: development, after v2.0 branched'
git clone -q --bare "$tmp/up" "$tmp/upstream.git"
git init -q --bare "$tmp/origin.git"

# This repository: upstream's development branch plus the raft delta, and
# tooling that must stay out of release branches.
git clone -q --no-tags "$tmp/upstream.git" "$tmp/work"
cd "$tmp/work"
put services/cluster/raft.go raft 'feat(cluster): raft'
put core.txt core-raft 'feat(cluster): hook in core'
put .github/workflows/ci.yml ci 'ci: workflow'

run v2.0.0 --dry-run
if [ "$(get mode)" = fresh ] && ! git --git-dir="$tmp/origin.git" rev-parse -q --verify refs/heads/release/v2.0.0 >/dev/null; then
	ok 'dry run: fresh, nothing pushed'
else fail "dry run: $out"; fi

run v2.0.0
sha=$(get sha)
if [ "$status" = 0 ] && [ "$(get mode)" = fresh ] &&
	[ "$(git --git-dir="$tmp/origin.git" rev-parse -q --verify refs/heads/release/v2.0.0)" = "$sha" ]; then
	ok 'fresh: pushed'
else fail "fresh: $out"; fi
if [ "$(file_at "$sha" services/cluster/raft.go)" = raft ] &&
	[ "$(file_at "$sha" core.txt)" = core-raft ] &&
	[ "$(file_at "$sha" fix.txt)" = stable-0 ] &&
	[ "$(file_at "$sha" dev.txt)" = '<absent>' ] &&
	[ "$(file_at "$sha" .github/workflows/ci.yml)" = '<absent>' ] &&
	[ "$(git --git-dir="$tmp/origin.git" rev-parse "$sha^")" = "$(git --git-dir="$tmp/upstream.git" rev-parse 'v2.0.0^{commit}')" ] &&
	[ "$(git --git-dir="$tmp/origin.git" show -s --format=%p "$sha" | wc -w | tr -d ' ')" = 1 ]; then
	ok 'fresh: the tag plus the raft delta only, one commit on the tag'
else fail 'fresh: content'; fi

run v2.0.0
if [ "$(get mode)" = existing ] && [ "$(get sha)" = "$sha" ]; then
	ok 'existing: picked up as it stands'
else fail "existing: $out"; fi

# Upstream: the development branch changes the line the raft delta then
# changes too, and v2.0.1 backports something else.
cd "$tmp/up"
put shared.txt B 'upstream: development changes shared'
git switch -q v2.0/forgejo
put fix.txt stable-1 'upstream: backport'
git tag v2.0.1
git switch -q forgejo
git push -q --mirror "$tmp/upstream.git"
cd "$tmp/work"
git pull -q --no-rebase --no-edit "$tmp/upstream.git" forgejo
put shared.txt B-raft 'feat(cluster): change shared'

run v2.0.1 --dry-run
sha1=$(get sha)
if [ "$(get mode)" = carry ] &&
	! git --git-dir="$tmp/origin.git" rev-parse -q --verify refs/heads/release/v2.0.1 >/dev/null; then
	ok 'carry (dry run): nothing pushed'
else fail "carry dry run: $out"; fi
run v2.0.1
sha1=$(get sha)
if [ "$(get mode)" = carry ] &&
	[ "$(file_at "$sha1" fix.txt)" = stable-1 ] &&
	[ "$(file_at "$sha1" shared.txt)" = A ] &&
	[ "$(file_at "$sha1" services/cluster/raft.go)" = raft ] &&
	[ "$(file_at "$sha1" dev.txt)" = '<absent>' ]; then
	ok 'carry: the fresh delta conflicts, release/v2.0.0 carried onto v2.0.1'
else fail "carry: $out"; fi

REMOTE_REPO=$tmp/empty.git
git init -q --bare "$REMOTE_REPO"
run v2.0.1 --dry-run
if [ "$status" = 0 ] && [ "$(get mode)" = conflict ] && [ "$(get conflicts)" = shared.txt ] && [ -z "$(get sha)" ]; then
	ok 'conflict: no earlier branch, the conflicted file named'
else fail "conflict: $out"; fi
unset REMOTE_REPO

# A fix by hand on the branch, outside the delta: reported, not refused.
git fetch -q "$tmp/origin.git" release/v2.0.0
git switch -q -c fixbranch FETCH_HEAD
put fix.txt stable-0-fixed 'fix(release): adapt to v2.0.0'
git push -q "$tmp/origin.git" HEAD:refs/heads/release/v2.0.0
run v2.0.0
if [ "$status" = 0 ] && [ "$(get mode)" = existing ] && [ "$(get outside)" = fix.txt ]; then
	ok 'existing: a file outside the delta is reported'
else fail "existing with a fix: $out"; fi

# forgejo merged into the release branch: refused.
git merge -q --no-edit -m 'merge forgejo' forgejo 2>/dev/null || {
	git checkout -q --theirs . && git add -A && git commit -q --no-edit -m 'merge forgejo'
}
git push -q "$tmp/origin.git" HEAD:refs/heads/release/v2.0.0
run v2.0.0
if [ "$status" != 0 ] && grep -q "upstream's development branch or of forgejo's history" <<<"$out"; then
	ok 'existing: a merge of forgejo is refused'
else fail "leak: $out"; fi
git switch -q forgejo

run v2.0 --dry-run
if [ "$status" != 0 ]; then ok 'a tag that is not vX.Y.Z is refused'; else fail 'bad tag'; fi

if [ "$failures" -gt 0 ]; then
	echo "$failures case(s) failed"
	exit 1
fi
echo "all cases passed"
