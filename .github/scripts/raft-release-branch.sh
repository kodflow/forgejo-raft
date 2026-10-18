#!/usr/bin/env bash
# Builds, or picks up, the branch release/<tag>: an official Forgejo release
# plus the raft delta, ready to be tested and released as <tag>-raft.<N>.
# Run by .github/workflows/raft-release.yml; runnable locally with --dry-run,
# which pushes nothing and prints the outputs.
#
# The raft delta is what `forgejo` changes relative to upstream: the diff from
# the upstream commit `forgejo` last merged (the merge base of `forgejo` and
# Codeberg's `forgejo` branch) to `forgejo` itself, minus this repository's
# tooling (.github/, .coderabbit.yaml), which the release branch does not
# carry: its checks run from `forgejo`'s workflows. It is applied onto the
# tag by a three-way merge (base: that merge base, ours: the tag, theirs:
# `forgejo`), and the result is committed with the tag as its only parent.
# Merging `forgejo` into the tag would instead bring every development commit
# made upstream since the release branched off; a three-way merge with that
# base brings only what `forgejo` changed relative to it.
#
# Modes, in order:
#   existing  release/<tag> exists on REMOTE: it is used as it stands, so a
#             fix pushed there by hand is what gets tested and released.
#             REFRESH=true rebuilds it instead (a new commit on top of it).
#   fresh     the three-way merge above is clean.
#   carry     it conflicts, and a release branch of an earlier tag of the
#             same major exists (release/v17.0.0 for v17.0.1, the earlier tag
#             an ancestor of this one): that branch's delta, already resolved
#             and released, is carried onto the new tag the same way (base:
#             the earlier tag). Upstream's own backports are all that differs.
#   conflict  neither merges: nothing is pushed, CONFLICTS lists the files.
#
# Proof, every mode: the tag is an ancestor of the branch; no commit after
# the tag comes from upstream's development branch or from `forgejo`'s
# history; and every file the branch changes relative to the tag is a file
# the raft delta changes. A built branch (fresh, carry) that breaks any of
# these stops the run; an existing one fails on the first two and has the
# files outside the delta reported (OUTSIDE), since a fix may have to touch
# an upstream file.
#
# Environment:
#   TAG              the official release, vX.Y.Z (required)
#   UPSTREAM_URL     default https://codeberg.org/forgejo/forgejo.git
#   UPSTREAM_BRANCH  upstream's development branch, default forgejo
#   REMOTE           where release branches live, default origin
#   SOURCE           the raft source, default HEAD (a checkout of `forgejo`)
#   REFRESH          true: rebuild an existing branch, default false
#   NOTES_FILE       the delta summary is written there, default raft-notes.md
set -euo pipefail

dry_run=false
if [ "${1:-}" = --dry-run ]; then dry_run=true; fi

TAG=${TAG:?TAG=vX.Y.Z is required}
UPSTREAM_URL=${UPSTREAM_URL:-https://codeberg.org/forgejo/forgejo.git}
UPSTREAM_BRANCH=${UPSTREAM_BRANCH:-forgejo}
REMOTE=${REMOTE:-origin}
REFRESH=${REFRESH:-false}
NOTES_FILE=${NOTES_FILE:-raft-notes.md}
EXCLUDED=(.github .coderabbit.yaml)

if ! [[ $TAG =~ ^v([0-9]+)\.([0-9]+)\.[0-9]+$ ]]; then
	echo "::error::TAG must be a stable release, vX.Y.Z, not '$TAG'" >&2
	exit 1
fi
major=${BASH_REMATCH[1]}
stable_branch=v${BASH_REMATCH[1]}.${BASH_REMATCH[2]}/forgejo
branch=release/$TAG
source=$(git rev-parse --verify "${SOURCE:-HEAD}^{commit}")

git fetch --quiet --no-tags "$UPSTREAM_URL" \
	"+refs/tags/$TAG:refs/raft/upstream-tag" \
	"+refs/heads/$UPSTREAM_BRANCH:refs/raft/upstream-dev"
tag_sha=$(git rev-parse --verify 'refs/raft/upstream-tag^{commit}')
dev=refs/raft/upstream-dev
base=$(git merge-base "$source" "$dev")

existing=
if git fetch --quiet --no-tags "$REMOTE" "+refs/heads/$branch:refs/raft/branch" 2>/dev/null; then
	existing=$(git rev-parse refs/raft/branch)
fi

# Files of a delta: those changed between two commits, tooling excluded.
pathspec=(.)
for p in "${EXCLUDED[@]}"; do pathspec+=(":(exclude)$p"); done
delta_files() { git diff --name-only --no-renames "$1" "$2" -- "${pathspec[@]}"; }

# theirs: `forgejo` with the tooling put back as it was at the base, so the
# delta does not carry it. A commit object in the runner only.
theirs_commit() {
	local idx
	idx=$(mktemp)
	GIT_INDEX_FILE=$idx git read-tree "$source"
	for p in "${EXCLUDED[@]}"; do
		GIT_INDEX_FILE=$idx git rm -r -q --cached --ignore-unmatch -- "$p" >/dev/null
		if git cat-file -e "$base:$p" 2>/dev/null; then
			if [ "$(git cat-file -t "$base:$p")" = tree ]; then
				GIT_INDEX_FILE=$idx git read-tree --prefix="$p/" "$base:$p"
			else
				GIT_INDEX_FILE=$idx git update-index --add --cacheinfo \
					"$(git ls-tree "$base" -- "$p" | awk '{print $1}')" "$(git rev-parse "$base:$p")" "$p"
			fi
		fi
	done
	local tree
	tree=$(GIT_INDEX_FILE=$idx git write-tree)
	rm -f "$idx"
	git commit-tree "$tree" -p "$base" -m "raft delta of ${source:0:12}, tooling excluded"
}

# merge3 <base> <ours> <theirs>: sets $tree to the merged tree, or returns 1
# with the conflicted files in $conflicts.
tree=
conflicts=
merge3() {
	local out
	if out=$(git merge-tree --write-tree --name-only --no-messages --merge-base="$1" "$2" "$3"); then
		tree=$(head -1 <<<"$out")
		return 0
	fi
	conflicts=$(tail -n +2 <<<"$out" | sed '/^$/d' | sort -u | paste -sd, -)
	return 1
}

mode=
head=
allowed=$(mktemp)
trap 'rm -f "$allowed"' EXIT
fresh_conflicts=
theirs=$(theirs_commit)
delta_files "$base" "$theirs" >"$allowed"
carried_from=

if [ -n "$existing" ] && [ "$REFRESH" != true ]; then
	mode=existing
	head=$existing
else
	parent=$tag_sha
	if [ -n "$existing" ]; then parent=$existing; fi
	if merge3 "$base" "$tag_sha" "$theirs"; then
		mode=fresh
		msg="feat(release): raft delta onto $TAG

Three-way merge onto the official $TAG of the raft delta: forgejo-raft
forgejo@${source:0:12} relative to forgejo/forgejo@${base:0:12}, the upstream
commit it last merged. Tooling (.github, .coderabbit.yaml) excluded."
	else
		fresh_conflicts=$conflicts
		# The release branch of the nearest earlier tag of this major whose
		# tag is an ancestor of this one.
		prev=
		while read -r name; do
			[ "$name" = "$TAG" ] && continue
			git fetch --quiet --no-tags "$UPSTREAM_URL" "+refs/tags/$name:refs/raft/prev-tag" 2>/dev/null || continue
			if git merge-base --is-ancestor 'refs/raft/prev-tag^{commit}' "$tag_sha"; then
				prev=$name
				break
			fi
		done < <(git ls-remote --heads "$REMOTE" "refs/heads/release/v$major.*" |
			sed 's|.*refs/heads/release/||' | grep -E '^v[0-9]+\.[0-9]+\.[0-9]+$' | sort -rV)
		if [ -n "$prev" ]; then
			git fetch --quiet --no-tags "$UPSTREAM_URL" "+refs/tags/$prev:refs/raft/prev-tag"
			git fetch --quiet --no-tags "$REMOTE" "+refs/heads/release/$prev:refs/raft/prev-branch"
			prev_tag=$(git rev-parse 'refs/raft/prev-tag^{commit}')
			if merge3 "$prev_tag" "$tag_sha" refs/raft/prev-branch; then
				mode=carry
				carried_from=release/$prev
				delta_files "$prev_tag" refs/raft/prev-branch >>"$allowed"
				msg="feat(release): raft delta onto $TAG, carried from release/$prev

The raft delta onto $TAG conflicts ($fresh_conflicts). The delta of
release/$prev, already resolved, is carried onto $TAG instead, by a
three-way merge based on $prev."
			fi
		fi
		if [ -z "$mode" ]; then
			mode=conflict
			conflicts=$fresh_conflicts
		fi
	fi
	if [ "$mode" != conflict ]; then
		head=$(git commit-tree "$tree" -p "$parent" -m "$msg")
	fi
fi

# The proof.
outside=
if [ -n "$head" ]; then
	if ! git merge-base --is-ancestor "$tag_sha" "$head"; then
		echo "::error::$branch does not contain $TAG" >&2
		exit 1
	fi
	after=$(git rev-list --count "$head" "^$tag_sha")
	own=$(git rev-list --count "$head" "^$tag_sha" "^$dev" "^$source")
	if [ "$after" != "$own" ]; then
		echo "::error::$branch holds $((after - own)) commit(s) of upstream's development branch or of forgejo's history after $TAG: something was merged into it" >&2
		exit 1
	fi
	outside=$(git diff --name-only --no-renames "$tag_sha" "$head" |
		grep -vxF -f "$allowed" | paste -sd, - || true)
	if [ -n "$outside" ] && [ "$mode" != existing ]; then
		echo "::error::$branch changes files outside the raft delta: $outside" >&2
		exit 1
	fi
fi

{
	echo "forgejo-raft built on the official Forgejo ${TAG#v}."
	echo
	echo "- Official release notes: https://codeberg.org/forgejo/forgejo/releases/tag/$TAG"
	echo "- Raft delta: forgejo-raft \`forgejo\`@${source:0:12} relative to forgejo/forgejo@${base:0:12}, the upstream commit it last merged, applied onto $TAG by a three-way merge on \`$branch\`."
	if [ "$mode" = carry ]; then
		echo "- Carried from \`$carried_from\`: the fresh delta conflicted in $fresh_conflicts."
	fi
	if [ -n "$head" ]; then
		echo "- Changes relative to $TAG:$(git diff --shortstat "$tag_sha" "$head")"
		echo
		echo "| Area | Files |"
		echo "| --- | --- |"
		git diff --name-only --no-renames "$tag_sha" "$head" |
			awk -F/ '{ if (NF > 2) print $1 "/" $2; else print $1 }' |
			sort | uniq -c | sort -rn | awk '{ printf "| `%s` | %s |\n", $2, $1 }'
	fi
	if [ -n "$outside" ]; then
		echo
		echo "Changed outside the raft delta, by a fix on \`$branch\`: $outside"
	fi
} >"$NOTES_FILE"

if [ "$mode" = existing ]; then
	attempt=branch:$head
else
	attempt=delta:$tag_sha:$(git rev-parse "$theirs^{tree}")
fi

if [ "$dry_run" != true ] && { [ "$mode" = fresh ] || [ "$mode" = carry ]; }; then
	git push --quiet "$REMOTE" "$head:refs/heads/$branch"
fi

outputs=$(printf '%s\n' \
	"mode=$mode" \
	"branch=$branch" \
	"sha=$head" \
	"tag_sha=$tag_sha" \
	"base=$base" \
	"source=$source" \
	"stable_branch=$stable_branch" \
	"conflicts=$conflicts" \
	"outside=$outside" \
	"attempt=$attempt")

if [ "$dry_run" = true ] || [ -z "${GITHUB_OUTPUT:-}" ]; then
	echo "$outputs"
	echo "--- $NOTES_FILE"
	cat "$NOTES_FILE"
else
	echo "$outputs" | tee -a "$GITHUB_OUTPUT"
fi
