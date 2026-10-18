#!/bin/sh
# Writes .build/Dockerfile.forgejo and its own .dockerignore from the Forgejo
# tree (BuildKit reads <Dockerfile>.dockerignore beside a Dockerfile first).
#
# - The ignore file adds node_modules: a developer tree holds host binaries
#   (esbuild for darwin-arm64), and `make frontend` must install its own.
# - A git worktree's .git is a pointer the build cannot follow, so the
#   version Forgejo would read from `git describe` is written to VERSION.
# - With a third argument "sdk", the SDK (build context "sdk") is joined by a
#   go.work right after the tree is copied, to build against unpublished SDK
#   changes.
set -eu
src="$1" out="$2" sdk="${3:-}"
anchor='COPY . ${GOPATH}/src/forgejo.org'
grep -qxF "$anchor" "$src/Dockerfile" || { echo "dockerfile: '$anchor' not found in $src/Dockerfile" >&2; exit 1; }
version="$(git -C "$src" describe --exclude '*-test' --tags --always 2>/dev/null | sed -e 's/^v//' -e 's/-g/-/')"
[ -n "$version" ] || { echo "dockerfile: no version from git describe in $src" >&2; exit 1; }
mkdir -p "$(dirname "$out")"
awk -v anchor="$anchor" -v sdk="$sdk" -v version="$version" '
  { print }
  $0 == anchor {
    print "RUN [ -s ${GOPATH}/src/forgejo.org/VERSION ] || echo \"" version "\" > ${GOPATH}/src/forgejo.org/VERSION"
  }
  $0 == anchor && sdk == "sdk" {
    print "COPY --from=sdk . ${GOPATH}/src/forgejo.org/.sdk"
    print "RUN cd ${GOPATH}/src/forgejo.org && go work init . ./.sdk"
  }' "$src/Dockerfile" >"$out"
{
  [ -f "$src/.dockerignore" ] && cat "$src/.dockerignore"
  printf '\n# cluster-lab: never ship host-built dependencies or outputs\nnode_modules\n**/node_modules\n/gitea\n/forgejo\n/data\n/.go-cache\n'
} >"$out.dockerignore"
