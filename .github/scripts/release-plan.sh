#!/usr/bin/env bash
# Decides whether the commit at HEAD gets a release, and under which version.
# Run by .github/workflows/release.yml; runnable locally with --dry-run, which
# prints the outputs instead of writing them to $GITHUB_OUTPUT.
#
# Version: v<upstream>-raft.<N>
#   <upstream>  the nearest upstream Forgejo tag reachable from HEAD, found the
#               way Forgejo's Makefile finds it (git describe, `-test` tags
#               excluded), without the leading v. Upstream's `forgejo` branch
#               carries the development tag of the next major (v18.0.0-dev),
#               so on this branch <upstream> is e.g. 18.0.0-dev.
#   <N>         1 + the highest N already tagged for that <upstream>, read
#               from REMOTE. A new upstream version starts again at 1.
# This repository holds no upstream tags: they are read from Codeberg with
# ls-remote, and only those whose commit is already in the local history are
# created, locally, for git describe. Nothing is pushed.
#
# Release or skip: a release is cut when HEAD brings at least one non-merge
# commit that changes a released path and is neither in the previous release
# nor in upstream's branch. An upstream sync (Codeberg's commits plus the sync
# workflow's merge commit) therefore cuts nothing. Released paths are all but
# NOT_RELEASED: this repository's tooling, which neither the binary nor the
# image carries (CI and its scripts, the review bot's configuration, the
# Docker lab).
#
# Environment:
#   UPSTREAM_URL     default https://codeberg.org/forgejo/forgejo.git
#   UPSTREAM_BRANCH  default forgejo
#   REMOTE           where release tags live, default origin
#   HEAD_REV         default HEAD
#   NOTES_FILE       release notes are written there, default release-notes.md
set -euo pipefail

dry_run=false
if [ "${1:-}" = --dry-run ]; then dry_run=true; fi

UPSTREAM_URL=${UPSTREAM_URL:-https://codeberg.org/forgejo/forgejo.git}
UPSTREAM_BRANCH=${UPSTREAM_BRANCH:-forgejo}
REMOTE=${REMOTE:-origin}
HEAD_REV=$(git rev-parse --verify "${HEAD_REV:-HEAD}^{commit}")
NOTES_FILE=${NOTES_FILE:-release-notes.md}
upstream_ref=refs/remotes/release-upstream/$UPSTREAM_BRANCH

git fetch --quiet --no-tags "$UPSTREAM_URL" "+refs/heads/$UPSTREAM_BRANCH:$upstream_ref"

# Upstream tags whose commit this history already holds. An annotated tag is
# listed twice by ls-remote; its peeled line (^{}) names the commit.
while read -r sha ref; do
	name=${ref#refs/tags/}
	name=${name%^\{\}}
	if git cat-file -e "$sha^{commit}" 2>/dev/null &&
		! git rev-parse --quiet --verify "refs/tags/$name" >/dev/null; then
		git update-ref "refs/tags/$name" "$sha"
	fi
done < <(git ls-remote --tags "$UPSTREAM_URL" 'refs/tags/v*' | grep -v -- '-raft\.')

upstream_tag=$(git describe --tags --abbrev=0 --match 'v[0-9]*' \
	--exclude '*-test' --exclude '*-raft.*' "$HEAD_REV") || {
	echo "::error::no upstream Forgejo tag reachable from $HEAD_REV" >&2
	exit 1
}
upstream_version=${upstream_tag#v}

# Our release tags, from the remote: the authority on which N are taken.
git fetch --quiet --no-tags "$REMOTE" '+refs/tags/*:refs/tags/*' || true
last_n=0
while read -r tag; do
	n=${tag##*-raft.}
	if [[ $n =~ ^[0-9]+$ ]] && [ "$n" -gt "$last_n" ]; then last_n=$n; fi
done < <(git ls-remote --tags --refs "$REMOTE" "refs/tags/v$upstream_version-raft.*" | sed 's|.*refs/tags/||')
version=v$upstream_version-raft.$((last_n + 1))

previous=$(git describe --tags --abbrev=0 --match 'v*-raft.*' "$HEAD_REV" 2>/dev/null || true)
range=("$HEAD_REV")
if [ -n "$previous" ]; then range+=("^$previous"); fi

NOT_RELEASED=(.github .coderabbit.yaml contrib/cluster-lab)
pathspec=(.)
for p in "${NOT_RELEASED[@]}"; do pathspec+=(":(exclude)$p"); done
changes=$(git rev-list --count --no-merges "${range[@]}" --not "$upstream_ref" -- "${pathspec[@]}")
release=false
if [ "$changes" -gt 0 ]; then release=true; fi

# What the Makefile derives from git describe once the release tag exists:
# the tag without its v, plus the Gitea compatibility suffix.
compat=$(sed -n 's/^GITEA_COMPATIBILITY ?= //p' Makefile)
file_version=${version#v}${compat:++$compat}

{
	echo "Cluster commits since ${previous:-the upstream base}, upstream Forgejo excluded."
	echo
	git log --no-merges --format='- %s (%h)' "${range[@]}" --not "$upstream_ref"
	echo
	echo "Upstream: Forgejo $upstream_version, $UPSTREAM_BRANCH at $(git rev-parse --short=12 "$(git merge-base "$HEAD_REV" "$upstream_ref")")."
} >"$NOTES_FILE"

outputs=$(printf '%s\n' \
	"release=$release" \
	"version=$version" \
	"file_version=$file_version" \
	"upstream_version=$upstream_version" \
	"previous=$previous" \
	"changes=$changes" \
	"sha=$HEAD_REV")

if [ "$dry_run" = true ] || [ -z "${GITHUB_OUTPUT:-}" ]; then
	echo "$outputs"
	echo "--- $NOTES_FILE"
	cat "$NOTES_FILE"
else
	echo "$outputs" | tee -a "$GITHUB_OUTPUT"
fi
