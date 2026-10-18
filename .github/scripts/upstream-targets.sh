#!/usr/bin/env bash
# Lists the official Forgejo releases that should have a raft release, as a
# JSON array of tags (["v17.0.0"]).
#
# Policy (owner decision of 2026-10-18): a raft release is built only for the
# newest Forgejo major. Of the versions Forgejo itself declares supported, in
# the TXT record of release.forgejo.org that Forgejo's own update checker
# reads (forgejo_versions=15.0.10,16.0.6,17.0.0), only the newest stable
# release of the highest major is kept: v17.0.0 today. Nothing is pinned: the
# day 18.0.0 is announced and tagged upstream, 18 becomes the only target on
# its own and 17 is no longer followed. Run daily, this catches every new
# release of the newest line the day it is announced. No backfill: an older
# release, or an older major, is built only on request (TAG, or RAFT_MAJORS).
# Release candidates are left out: Forgejo's are short-lived, and the
# development line already has its raft prereleases (release.yml).
#
# Environment:
#   TAG           one tag, vX.Y.Z, instead of the selection
#   RAFT_MAJORS   explicit override, majors separated by spaces or commas:
#                 the supported release of each of these majors, instead of
#                 the highest major alone
#   SUPPORTED     the versions, instead of asking DNS (tests)
#   UPSTREAM_URL  default https://codeberg.org/forgejo/forgejo.git
set -euo pipefail

UPSTREAM_URL=${UPSTREAM_URL:-https://codeberg.org/forgejo/forgejo.git}
DOMAIN=release.forgejo.org

txt() {
	if command -v dig >/dev/null; then
		dig +short TXT "$DOMAIN" | tr -d '"'
	else
		curl -fsS -H 'accept: application/dns-json' \
			"https://cloudflare-dns.com/dns-query?name=$DOMAIN&type=TXT" |
			jq -r '.Answer[]?.data' | tr -d '"'
	fi
}

if [ -n "${TAG:-}" ]; then
	versions=${TAG#v}
elif [ -n "${SUPPORTED:-}" ]; then
	versions=$SUPPORTED
else
	versions=$(txt | sed -n 's/^forgejo_versions=//p' | paste -sd, -)
	if [ -z "$versions" ]; then
		echo "::error::no forgejo_versions in the TXT record of $DOMAIN" >&2
		exit 1
	fi
fi

upstream_tags=$(git ls-remote --tags --refs "$UPSTREAM_URL" 'refs/tags/v*' | sed 's|.*refs/tags/||')
majors=$(tr -s ', ' '\n' <<<"${RAFT_MAJORS:-}" | sed '/^$/d')

# Stable releases announced and tagged upstream, as "<major> <tag>" lines.
candidates=()
while IFS= read -r v; do
	[ -z "$v" ] && continue
	tag=v$v
	if ! [[ $tag =~ ^v([0-9]+)\.[0-9]+\.[0-9]+$ ]]; then
		echo "::warning::$v is not a stable release (X.Y.Z), skipped" >&2
		continue
	fi
	major=${BASH_REMATCH[1]}
	if [ -n "$majors" ] && ! grep -qxF "$major" <<<"$majors"; then
		echo "$tag: major not in RAFT_MAJORS, skipped" >&2
		continue
	fi
	if ! grep -qxF "$tag" <<<"$upstream_tags"; then
		echo "::warning::$tag is announced but not tagged on $UPSTREAM_URL yet, skipped" >&2
		continue
	fi
	candidates+=("$major $tag")
done < <(tr ',' '\n' <<<"$versions" | tr -d ' ')

tags=()
if [ ${#candidates[@]} -gt 0 ]; then
	if [ -n "${TAG:-}" ] || [ -n "$majors" ]; then
		# An explicit choice: every candidate it lets through.
		for c in "${candidates[@]}"; do tags+=("${c#* }"); done
	else
		# The default: the newest stable release of the highest major only.
		newest=$(printf '%s\n' "${candidates[@]}" | sort -k1,1n -k2,2V | tail -n1)
		tags=("${newest#* }")
		for c in "${candidates[@]}"; do
			if [ "$c" != "$newest" ]; then
				echo "${c#* }: not the newest release of the highest major, skipped" >&2
			fi
		done
	fi
fi

json=$(printf '%s\n' ${tags[@]+"${tags[@]}"} | sed '/^$/d' | sort -uV | jq -Rnc '[inputs]')
echo "tags=$json"
if [ -n "${GITHUB_OUTPUT:-}" ]; then echo "tags=$json" >>"$GITHUB_OUTPUT"; fi
