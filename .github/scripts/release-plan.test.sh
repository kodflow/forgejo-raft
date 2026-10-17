#!/usr/bin/env bash
# Offline test of release-plan.sh: throwaway upstream and origin repositories,
# one case per decision the plan must get right. Run it from anywhere:
#   .github/scripts/release-plan.test.sh
set -euo pipefail

plan=$(cd "$(dirname "$0")" && pwd)/release-plan.sh
tmp=$(mktemp -d)
trap 'rm -rf "$tmp"' EXIT
export GIT_AUTHOR_NAME=test GIT_AUTHOR_EMAIL=test@example.invalid
export GIT_COMMITTER_NAME=test GIT_COMMITTER_EMAIL=test@example.invalid
export GIT_CONFIG_GLOBAL=/dev/null GIT_CONFIG_NOSYSTEM=1

failures=0
commit() { # commit <path> <message>
	mkdir -p "$(dirname "$1")"
	echo "$2" >>"$1"
	git add "$1"
	git commit -q -m "$2"
}
expect() { # expect <case> <release> [version]
	local out
	out=$(UPSTREAM_URL=$tmp/upstream.git REMOTE=$tmp/origin.git NOTES_FILE=$tmp/notes.md "$plan" --dry-run)
	got=$(sed -n 's/^release=//p' <<<"$out")
	ver=$(sed -n 's/^version=//p' <<<"$out")
	if [ "$got" = "$2" ] && { [ -z "${3:-}" ] || [ "$ver" = "$3" ]; }; then
		echo "ok   $1: release=$got $ver"
	else
		echo "FAIL $1: release=$got $ver, want release=$2 ${3:-}"
		failures=$((failures + 1))
	fi
}
tag_release() { # what the workflow's release step leaves behind
	git tag "$1"
	git push -q "$tmp/origin.git" "refs/tags/$1"
	git tag -d "$1" >/dev/null
}

git init -q -b forgejo "$tmp/up"
(
	cd "$tmp/up"
	printf 'GITEA_COMPATIBILITY ?= gitea-1.22.0\n' >Makefile
	git add Makefile
	git commit -q -m 'upstream: base'
	git tag v18.0.0-dev
)
git clone -q --bare "$tmp/up" "$tmp/upstream.git"
git init -q --bare "$tmp/origin.git"
git clone -q --no-tags "$tmp/upstream.git" "$tmp/work"
cd "$tmp/work"
git tag -d v18.0.0-dev >/dev/null 2>&1 || true

commit services/cluster/raft.go 'feat(cluster): first'
expect 'first cluster commit' true v18.0.0-dev-raft.1
tag_release v18.0.0-dev-raft.1

expect 'nothing since the tag' false

# The case of v18.0.0-dev-raft.2: a merge commit whose second parent changes
# only repository tooling.
git switch -q -c ci HEAD
commit .github/workflows/x.yml 'ci: workflow'
git switch -q forgejo
git merge -q --no-ff -m 'Merge pull request: ci' ci
expect 'merge of a .github-only branch' false

git switch -q ci
commit .coderabbit.yaml 'ci: review bot'
commit contrib/cluster-lab/compose.yaml 'test(cluster): lab comment'
git switch -q forgejo
git merge -q --no-ff -m 'Merge pull request: tooling' ci
expect 'merge of a tooling-only branch' false

(cd "$tmp/up" && commit modules/git/git.go 'upstream: change')
git fetch -q "$tmp/upstream.git" forgejo
git merge -q --no-ff -m 'Merge upstream' FETCH_HEAD
expect 'upstream sync merge' false

git switch -q -c fix HEAD
commit services/cluster/raft.go 'fix(cluster): second'
git switch -q forgejo
git merge -q --no-ff -m 'Merge pull request: fix' fix
expect 'merge of a cluster change' true v18.0.0-dev-raft.2

if [ "$failures" -gt 0 ]; then
	echo "$failures case(s) failed"
	exit 1
fi
echo "all cases passed"
