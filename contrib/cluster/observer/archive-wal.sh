#!/bin/sh
# PostgreSQL's archive_command on a host without the Forgejo binary, the
# same contract as `forgejo cluster wal-archive` (contrib/cluster/observer.md):
#   archive_command = '/usr/local/bin/archive-wal.sh /srv/forgejo/wal-archive %p %f'
# Idempotent: a file already archived with the same bytes is success, other
# bytes under the same name are refused and never overwritten. The copy is
# synced before it takes its name.
set -eu
dir="$1" src="$2" name="$3"
# A promoted standby archives the old timeline's last segment as .partial;
# refusing it would stop PostgreSQL's archiving for good.
echo "$name" | grep -Eq '^([0-9A-F]{24}(\.[0-9A-F]{8}\.backup|\.partial)?|[0-9A-F]{8}\.history)$' || {
  echo "archive-wal: $name is not a WAL file name" >&2
  exit 2
}
dst="$dir/$name"
# Success is answered only once the name is durable, as wal-archive does: a
# first attempt killed between its ln and its sync left it in the page cache,
# and PostgreSQL recycles the segment on success.
if [ -e "$dst" ]; then
  if cmp -s "$src" "$dst"; then
    sync "$dir" 2>/dev/null || sync
    exit 0
  fi
  echo "archive-wal: $name is already archived with other bytes; refusing to overwrite it" >&2
  exit 1
fi
tmp="$dir/.$name.$$.partial"
trap 'rm -f "$tmp"' EXIT
cp "$src" "$tmp"
sync "$tmp" 2>/dev/null || sync
# ln fails if another archiver got there first: compare again.
if ! ln "$tmp" "$dst" 2>/dev/null; then
  if cmp -s "$src" "$dst"; then
    sync "$dir" 2>/dev/null || sync
    exit 0
  fi
  echo "archive-wal: $name appeared with other bytes" >&2
  exit 1
fi
sync "$dir" 2>/dev/null || sync
