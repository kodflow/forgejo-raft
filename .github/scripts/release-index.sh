#!/usr/bin/env bash
# Prints the index of forgejo-raft releases that a raft build's update checker
# reads ([cron.update_checker] RAFT_ENDPOINT), from the GitHub releases of
# REPO. Published releases only, drafts left out, in a stable order (the checker does not rely on it). No date in
# it, so an unchanged list gives the same bytes and nothing to commit.
#
#   {"schema": 1, "releases": [{"version": "17.0.1-raft.1",
#     "upstream": "17.0.1", "url": "https://github.com/...", "prerelease": false}]}
#
# Environment: REPO (owner/name, required), GH_TOKEN for gh.
set -euo pipefail
REPO=${REPO:?REPO=owner/name is required}
gh release list -R "$REPO" --limit 1000 --exclude-drafts --json tagName,isPrerelease |
	jq --arg repo "$REPO" '
		[ .[]
		  | select(.tagName | test("^v[0-9]+\\.[0-9]+\\.[0-9]+(-[0-9A-Za-z.-]+)?-raft\\.[0-9]+$"))
		  | (.tagName | ltrimstr("v")) as $v
		  | { version: $v,
		      upstream: ($v | sub("-raft\\.[0-9]+$"; "")),
		      url: ("https://github.com/" + $repo + "/releases/tag/" + .tagName),
		      prerelease: .isPrerelease } ]
		| sort_by(.version)
		| { schema: 1, releases: . }'
