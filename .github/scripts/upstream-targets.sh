#!/usr/bin/env bash
# Lists the official Forgejo releases that should each have a raft release,
# as a JSON array of tags (["v15.0.10","v16.0.6","v17.0.0"]).
#
# Policy: the versions Forgejo itself declares supported, in the TXT record of
# release.forgejo.org that Forgejo's own update checker reads
# (forgejo_versions=15.0.10,16.0.6,17.0.0): the newest release of every
# supported major, LTS included. Run daily, this catches every new release of
# a supported line the day it is announced. No backfill: an older release of
# a line, or an unsupported line, is built only on request (TAG).
# Release candidates are left out: Forgejo's are short-lived, and the
# development line already has its raft prereleases (release.yml).
#
# Environment:
#   TAG           one tag, vX.Y.Z, instead of the supported list
#   RAFT_MAJORS   optional filter, majors separated by spaces or commas
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

tags=()
while IFS= read -r v; do
	[ -z "$v" ] && continue
	tag=v$v
	if ! [[ $tag =~ ^v([0-9]+)\.[0-9]+\.[0-9]+$ ]]; then
		echo "::warning::$v is not a stable release (X.Y.Z), skipped" >&2
		continue
	fi
	if [ -n "$majors" ] && ! grep -qxF "${BASH_REMATCH[1]}" <<<"$majors"; then
		echo "$tag: major not in RAFT_MAJORS, skipped" >&2
		continue
	fi
	if ! grep -qxF "$tag" <<<"$upstream_tags"; then
		echo "::warning::$tag is announced but not tagged on $UPSTREAM_URL yet, skipped" >&2
		continue
	fi
	tags+=("$tag")
done < <(tr ',' '\n' <<<"$versions" | tr -d ' ')

json=$(printf '%s\n' ${tags[@]+"${tags[@]}"} | sed '/^$/d' | sort -uV | jq -Rnc '[inputs]')
echo "tags=$json"
if [ -n "${GITHUB_OUTPUT:-}" ]; then echo "tags=$json" >>"$GITHUB_OUTPUT"; fi
